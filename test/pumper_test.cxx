/*
 * Copyright (C) 2024-2026 David C. Manuelda (StormBytePP)
 *
 * This file is part of StormByte-Buffer.
 *
 * StormByte-Buffer original source is dual-licensed:
 *
 * 1. GNU Lesser General Public License v3.0 (or later)
 *    You may redistribute and/or modify this file under the terms of the
 *    GNU Lesser General Public License as published by the Free Software
 *    Foundation, either version 3 of the License, or (at your option)
 *    any later version.
 *
 * 2. Commercial license
 *    Alternatively, this file may be used under the terms of a commercial
 *    license agreement with the copyright holder
 *    (David C. Manuelda <StormByte@gmail.com>).
 *
 * Both licenses apply only to original StormByte-Buffer source in this
 * repository. They do not cover other StormByte modules or any third-party
 * material shipped with this repository (including everything under
 * thirdparty/, and in particular the bundled StormByte-Logger tree and
 * the rest of the StormByte suite it vendors), which remains under its own
 * license.
 *
 * Neither license grants any patent rights. Any patent licenses required
 * to use this software or third-party components must be obtained separately
 * from the patent holders.
 *
 * StormByte-Buffer is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
 * GNU Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public License
 * version 3 along with StormByte-Buffer. If not, see
 * <https://www.gnu.org/licenses/lgpl-3.0.html>.
 *
 * SPDX-License-Identifier: LGPL-3.0-or-later OR LicenseRef-StormByte-Commercial
 */

#include <StormByte/buffer/bridge.hxx>
#include <StormByte/buffer/consumer.hxx>
#include <StormByte/buffer/fifo.hxx>
#include <StormByte/buffer/io/buffered_file_reader.hxx>
#include <StormByte/buffer/io/buffered_file_writer.hxx>
#include <StormByte/buffer/io/buffered_reader.hxx>
#include <StormByte/buffer/io/telemetry.hxx>
#include <StormByte/buffer/producer.hxx>
#include <StormByte/buffer/pumper.hxx>
#include <StormByte/byte_size.hxx>
#include <StormByte/safe/binary.hxx>
#include <StormByte/safe/string.hxx>
#include <StormByte/test_handlers.h>

#include <atomic>
#include <chrono>
#include <condition_variable>
#include <filesystem>
#include <fstream>
#include <iostream>
#include <iterator>
#include <mutex>
#include <string>
#include <string_view>
#include <thread>

using StormByte::ByteSize;
using StormByte::Buffer::Bridge;
using StormByte::Buffer::Chunk;
using StormByte::Buffer::Consumer;
using StormByte::Buffer::FIFO;
using StormByte::Buffer::HighWater;
using StormByte::Buffer::Producer;
using StormByte::Buffer::Pumper;
using StormByte::Buffer::IO::BufferedFileReader;
using StormByte::Buffer::IO::BufferedFileWriter;
using StormByte::Buffer::IO::BufferedReader;
using StormByte::Buffer::IO::MaxMemory;
using StormByte::Buffer::IO::ReadAhead;
using StormByte::Buffer::IO::WriteChunk;
using StormByte::Safe::Binary;

namespace {
	class TransferGate {
		public:
			bool WaitForEntry(const std::chrono::milliseconds timeout) {
				std::unique_lock lock(m_mutex);
				return m_cv.wait_for(lock, timeout, [this] { return m_entered; });
			}

			void Release() {
				std::lock_guard lock(m_mutex);
				m_released = true;
				m_cv.notify_all();
			}

			void BlockFirst() {
				std::unique_lock lock(m_mutex);
				if (!m_entered) {
					m_entered = true;
					m_cv.notify_all();
					m_cv.wait(lock, [this] { return m_released; });
				}
			}

		private:
			std::mutex m_mutex;
			std::condition_variable m_cv;
			bool m_entered {false};
			bool m_released {false};
	};

	class GatedFIFO final: public FIFO {
		public:
			bool WaitForWrite(const std::chrono::milliseconds timeout) {
				return m_gate.WaitForEntry(timeout);
			}

			void ReleaseWrite() {
				m_gate.Release();
			}

			bool Write(const ByteSize& count, Binary&& data) noexcept override {
				m_gate.BlockFirst();
				return FIFO::Write(count, std::move(data));
			}

		private:
			TransferGate m_gate;
	};

	class GatedEofFIFO final: public FIFO {
		public:
			explicit GatedEofFIFO(const std::size_t probe): m_gate_at(probe) {}

			bool EoF() const noexcept override {
				{
					std::unique_lock lock(m_mutex);
					if (++m_probes == m_gate_at) {
						m_entered = true;
						m_cv.notify_all();
						m_cv.wait(lock, [this] { return m_released; });
					}
				}
				return FIFO::EoF();
			}

			bool WaitForProbe(const std::chrono::milliseconds timeout) const {
				std::unique_lock lock(m_mutex);
				return m_cv.wait_for(lock, timeout, [this] { return m_entered; });
			}

			void ReleaseProbe() {
				std::lock_guard lock(m_mutex);
				m_released = true;
				m_cv.notify_all();
			}

		private:
			const std::size_t m_gate_at;
			mutable std::mutex m_mutex;
			mutable std::condition_variable m_cv;
			mutable std::size_t m_probes {0};
			mutable bool m_entered {false};
			bool m_released {false};
	};

	class BlockingReader final: public BufferedReader {
		public:
			explicit BlockingReader(TransferGate& gate):
				BufferedReader(StormByte::Safe::String{"test://blocking"},
					StormByte::Buffer::IO::Location::Remote,
					Parameters{StormByte::Buffer::IO::ReadAhead{ByteSize{0}},
						StormByte::Buffer::IO::MaxMemory{ByteSize{0}}}),
				m_gate(gate) {}

			BlockingReader(BlockingReader&& other) noexcept:
				BufferedReader(std::move(other)),
				m_gate(other.m_gate),
				m_sent(other.m_sent) {}

			~BlockingReader() noexcept override {
				static_cast<void>(Close());
			}

		protected:
			StormByte::Buffer::IO::Result OriginOpen() override {
				SetState(StormByte::Buffer::IO::State::Idle);
				return { StormByte::Buffer::IO::Status::Ok, 0 };
			}

			StormByte::Buffer::IO::Result OriginClose() override {
				SetState(StormByte::Buffer::IO::State::Unavailable);
				return { StormByte::Buffer::IO::Status::Ok, 0 };
			}

			StormByte::Buffer::IO::Result OriginPull(ByteSize, FIFO& dest) override {
				if (m_sent)
					return { StormByte::Buffer::IO::Status::End, 0 };
				m_gate.BlockFirst();
				if (!dest.Write("Z"))
					return { StormByte::Buffer::IO::Status::Error, 0 };
				m_sent = true;
				return { StormByte::Buffer::IO::Status::Ok, 1 };
			}

			bool OriginCanSeek() const noexcept override { return false; }

			StormByte::Buffer::IO::Result OriginSeek(std::ptrdiff_t, StormByte::Buffer::Position) override {
				return { StormByte::Buffer::IO::Status::Failed, 0 };
			}

			bool OriginHasSize() const noexcept override { return false; }

			StormByte::Safe::Optional<ByteSize> OriginSize() const noexcept override { return {}; }

		private:
			TransferGate& m_gate;
			bool m_sent {false};
	};

	StormByte::Safe::String Loc(const std::filesystem::path& path) {
#ifdef WINDOWS
		return StormByte::Safe::String(StormByte::Safe::WString(std::wstring_view(path.wstring())));
#else
		return StormByte::Safe::String(std::string_view(path.string()));
#endif
	}

	void DumpText(const std::filesystem::path& path, const std::string& text) {
		std::ofstream out(path, std::ios::binary | std::ios::trunc);
		out.write(text.data(), static_cast<std::streamsize>(text.size()));
	}

	std::string Slurp(const std::filesystem::path& path) {
		std::ifstream in(path, std::ios::binary);
		return std::string(std::istreambuf_iterator<char>(in), std::istreambuf_iterator<char>());
	}

	std::string BytesToText(const Binary& data) {
		if (data.empty())
			return {};
		return std::string(reinterpret_cast<const char*>(data.data()), static_cast<std::size_t>(data.size()));
	}

	bool WaitFinish(Pumper& pump, const std::chrono::milliseconds budget) {
		const auto start = std::chrono::steady_clock::now();
		while (!pump.Failed() && !pump.Canceled() && !pump.EoF()) {
			if (std::chrono::steady_clock::now() - start > budget)
				return false;
			std::this_thread::sleep_for(std::chrono::milliseconds(1));
		}
		return pump.EoF() && !pump.Failed() && !pump.Canceled();
	}

	bool WaitDelivered(Pumper& pump, const ByteSize need, const std::chrono::milliseconds budget) {
		const auto start = std::chrono::steady_clock::now();
		while (!pump.Failed() && !pump.Canceled()) {
			const auto tel = pump.ReadTelemetry();
			if (tel && tel->Delivered() >= need)
				return true;
			if (std::chrono::steady_clock::now() - start > budget)
				return false;
			std::this_thread::sleep_for(std::chrono::milliseconds(1));
		}
		return false;
	}
}

// -------------------
// Cancel
// -------------------

int test_pumper_cancel_before_close_does_not_drain() {
	Producer src;
	FIFO out;
	Consumer in = src.Consumer();
	src.Write("ABCDEFGH");
	Pumper pump(Bridge(in, out), { Chunk{2} });
	ASSERT_TRUE(WaitDelivered(pump, ByteSize{1}, std::chrono::seconds(2)));
	pump.Cancel();
	src.Write("XXXXXXXX");
	src.Close();
	std::this_thread::sleep_for(std::chrono::milliseconds(50));
	ASSERT_TRUE(pump.Canceled());
	ASSERT_FALSE(pump.Failed());
	const auto tel = pump.ReadTelemetry();
	ASSERT_TRUE(static_cast<bool>(tel));
	ASSERT_TRUE(tel->Delivered() < ByteSize{16});
	RETURN_TEST(0);
}

int test_pumper_cancel_is_terminal() {
	Producer src;
	FIFO out;
	Consumer in = src.Consumer();
	Pumper pump(Bridge(in, out), { Chunk{64} });
	ASSERT_FALSE(pump.Failed());
	ASSERT_FALSE(pump.Canceled());
	pump.Cancel();
	ASSERT_TRUE(pump.Canceled());
	ASSERT_FALSE(pump.Failed());
	pump.Toggle();
	ASSERT_TRUE(pump.Canceled());
	ASSERT_FALSE(pump.Failed());
	src.Write("late");
	src.Close();
	std::this_thread::sleep_for(std::chrono::milliseconds(50));
	ASSERT_TRUE(pump.Canceled());
	ASSERT_FALSE(pump.Failed());
	ASSERT_FALSE(WaitFinish(pump, std::chrono::milliseconds(100)));
	RETURN_TEST(0);
}

int test_pumper_cancel_telemetry_survives() {
	Producer src;
	FIFO out;
	Consumer in = src.Consumer();
	src.Write("abc");
	Pumper pump(Bridge(in, out), { Chunk{1} });
	ASSERT_TRUE(WaitDelivered(pump, ByteSize{1}, std::chrono::seconds(2)));
	const auto kept = pump.ReadTelemetry();
	pump.Cancel();
	ASSERT_TRUE(static_cast<bool>(kept));
	ASSERT_TRUE(kept == pump.ReadTelemetry());
	ASSERT_TRUE(kept->Delivered() >= ByteSize{1});
	src.Close();
	RETURN_TEST(0);
}

int test_pumper_cancel_while_paused_is_terminal() {
	Producer src;
	FIFO out;
	Consumer in = src.Consumer();
	Pumper pump(Bridge(in, out), { Chunk{1} });
	pump.Toggle();
	const bool wrote = src.Write("must-not-copy");
	pump.Cancel();
	pump.Toggle();
	src.Close();
	ASSERT_TRUE(wrote);
	ASSERT_TRUE(pump.Canceled());
	ASSERT_FALSE(pump.Failed());
	ASSERT_EQUAL(ByteSize{0}, pump.ReadTelemetry()->Delivered());
	ASSERT_EQUAL(ByteSize{0}, out.Size());
	RETURN_TEST(0);
}

// -------------------
// Failed
// -------------------

int test_bridge_source_failure_during_eof_probe() {
	GatedEofFIFO in(1);
	FIFO out;
	Bridge bridge(in, out);
	ByteSize moved{0};
	std::thread transfer([&] {
		moved = bridge.Passthrough(1, Bridge::Operation::NonBlocking);
	});
	const bool entered = in.WaitForProbe(std::chrono::seconds(2));
	if (entered)
		in.SetError();
	in.ReleaseProbe();
	transfer.join();
	ASSERT_TRUE(entered);
	ASSERT_EQUAL(ByteSize{0}, moved);
	ASSERT_TRUE(bridge.Failed());
	ASSERT_TRUE(bridge.State() == Bridge::State::Failed);
	RETURN_TEST(0);
}

int test_bridge_source_failure_after_transfer() {
	GatedEofFIFO in(1);
	FIFO out;
	ASSERT_TRUE(in.Write("Z"));
	Bridge bridge(in, out);
	ByteSize moved{0};
	std::thread transfer([&] {
		moved = bridge.Passthrough(1, Bridge::Operation::NonBlocking);
	});
	const bool entered = in.WaitForProbe(std::chrono::seconds(2));
	if (entered)
		in.SetError();
	in.ReleaseProbe();
	transfer.join();
	Binary got;
	const bool extracted = out.Extract(0, got);
	ASSERT_TRUE(entered);
	ASSERT_EQUAL(ByteSize{1}, moved);
	ASSERT_TRUE(bridge.Failed());
	ASSERT_EQUAL(ByteSize{1}, bridge.ReadTelemetry()->Delivered());
	ASSERT_EQUAL(ByteSize{1}, bridge.WriteTelemetry()->Accepted());
	ASSERT_TRUE(extracted);
	ASSERT_EQUAL(std::string("Z"), BytesToText(got));
	RETURN_TEST(0);
}

int test_pumper_source_failure_during_eof_probe() {
	GatedEofFIFO in(2);
	FIFO out;
	Pumper pump(Bridge(in, out), { Chunk{1} });
	const bool entered = in.WaitForProbe(std::chrono::seconds(2));
	if (entered)
		in.SetError();
	in.ReleaseProbe();
	const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(2);
	while (entered && !pump.Failed() && std::chrono::steady_clock::now() < deadline)
		std::this_thread::sleep_for(std::chrono::milliseconds(1));
	const bool failed = pump.Failed();
	const bool canceled = pump.Canceled();
	if (!failed)
		pump.Cancel();
	ASSERT_TRUE(entered);
	ASSERT_TRUE(failed);
	ASSERT_FALSE(canceled);
	RETURN_TEST(0);
}

int test_pumper_failed_is_not_canceled() {
	Producer src;
	FIFO out;
	Consumer in = src.Consumer();
	Pumper pump(Bridge(in, out), { Chunk{64} });
	src.SetError();
	const auto start = std::chrono::steady_clock::now();
	while (!pump.Failed() && !pump.Canceled()) {
		if (std::chrono::steady_clock::now() - start > std::chrono::seconds(2))
			break;
		std::this_thread::sleep_for(std::chrono::milliseconds(1));
	}
	ASSERT_TRUE(pump.Failed());
	ASSERT_FALSE(pump.Canceled());
	RETURN_TEST(0);
}

// -------------------
// Finish
// -------------------

int test_pumper_auto_chunk_copies_all() {
	FIFO in;
	FIFO out;
	const std::string payload(64 * 1024, 'A');
	in.Write(payload);
	in.Close();
	Pumper pump(Bridge(in, out));
	ASSERT_TRUE(WaitFinish(pump, std::chrono::seconds(5)));
	Binary got;
	ASSERT_TRUE(out.Extract(0, got));
	ASSERT_EQUAL(ByteSize{payload.size()}, got.size());
	ASSERT_EQUAL(payload, BytesToText(got));
	RETURN_TEST(0);
}

int test_pumper_chunked_copies_all() {
	FIFO in;
	FIFO out;
	in.Write("0123456789abcdef");
	in.Close();
	Pumper pump(Bridge(in, out), { Chunk{3} });
	ASSERT_TRUE(WaitFinish(pump, std::chrono::seconds(2)));
	Binary got;
	ASSERT_TRUE(out.Extract(0, got));
	ASSERT_EQUAL(ByteSize{16}, got.size());
	const auto read = pump.ReadTelemetry();
	const auto write = pump.WriteTelemetry();
	ASSERT_TRUE(static_cast<bool>(read));
	ASSERT_TRUE(static_cast<bool>(write));
	ASSERT_EQUAL(ByteSize{16}, read->Delivered());
	ASSERT_EQUAL(ByteSize{16}, write->Accepted());
	RETURN_TEST(0);
}

int test_pumper_dtor_finishes_closed_source() {
	FIFO in;
	FIFO out;
	in.Write("done-by-dtor");
	in.Close();
	{
		Pumper pump(Bridge(in, out), { Chunk{4} });
		(void)pump;
	}
	Binary got;
	ASSERT_TRUE(out.Extract(0, got));
	ASSERT_EQUAL(std::string("done-by-dtor"), BytesToText(got));
	RETURN_TEST(0);
}

int test_pumper_empty_closed_source_is_eof() {
	FIFO in;
	FIFO out;
	in.Close();
	Pumper pump(Bridge(in, out), { Chunk{32} });
	ASSERT_TRUE(WaitFinish(pump, std::chrono::seconds(2)));
	ASSERT_TRUE(pump.EoF());
	ASSERT_FALSE(pump.Failed());
	ASSERT_FALSE(pump.Canceled());
	const auto read = pump.ReadTelemetry();
	ASSERT_TRUE(static_cast<bool>(read));
	ASSERT_EQUAL(ByteSize{0}, read->Delivered());
	RETURN_TEST(0);
}

int test_pumper_live_producer_then_close() {
	Producer src;
	FIFO out;
	Consumer in = src.Consumer();
	Pumper pump(Bridge(in, out), { Chunk{5} });
	src.Write("hello");
	ASSERT_TRUE(WaitDelivered(pump, ByteSize{5}, std::chrono::seconds(2)));
	src.Write(" world");
	src.Close();
	ASSERT_TRUE(WaitFinish(pump, std::chrono::seconds(2)));
	Binary got;
	ASSERT_TRUE(out.Extract(0, got));
	ASSERT_EQUAL(std::string("hello world"), BytesToText(got));
	RETURN_TEST(0);
}

// -------------------
// HighWater
// -------------------

int test_pumper_highwater_small_still_copies() {
	FIFO in;
	FIFO out;
	in.Write("0123456789");
	in.Close();
	Pumper pump(Bridge(in, out), { Chunk{8}, HighWater{ByteSize{2}} });
	ASSERT_TRUE(WaitFinish(pump, std::chrono::seconds(2)));
	Binary got;
	ASSERT_TRUE(out.Extract(0, got));
	ASSERT_EQUAL(ByteSize{10}, got.size());
	RETURN_TEST(0);
}

int test_pumper_highwater_zero_still_copies() {
	FIFO in;
	FIFO out;
	in.Write("0123456789");
	in.Close();
	Pumper pump(Bridge(in, out), { Chunk{3}, HighWater{ByteSize{0}} });
	ASSERT_TRUE(WaitFinish(pump, std::chrono::seconds(2)));
	Binary got;
	ASSERT_TRUE(out.Extract(0, got));
	ASSERT_EQUAL(ByteSize{10}, got.size());
	RETURN_TEST(0);
}

// -------------------
// IO
// -------------------

int test_pumper_basic_telemetry_during_transfer() {
	Producer src;
	FIFO out;
	Consumer in = src.Consumer();
	Bridge bridge(in, out);
	const auto read = bridge.ReadTelemetry();
	const auto write = bridge.WriteTelemetry();
	ASSERT_TRUE(static_cast<bool>(read));
	ASSERT_TRUE(static_cast<bool>(write));

	std::atomic<bool> ready{false};
	std::atomic<bool> stop{false};
	bool valid_snapshots = true;
	std::size_t snapshots = 0;
	std::thread observer([&] {
		ByteSize previous_read{0};
		ByteSize previous_write{0};
		while (!stop.load(std::memory_order_acquire)) {
			const ByteSize current_read = read->Delivered();
			const ByteSize current_write = write->Accepted();
			if (current_read < previous_read || current_write < previous_write
					|| static_cast<std::string>(*read).empty()
					|| static_cast<std::string>(*write).empty())
				valid_snapshots = false;
			previous_read = current_read;
			previous_write = current_write;
			++snapshots;
			if (snapshots == 1)
				ready.store(true, std::memory_order_release);
		}
	});
	while (!ready.load(std::memory_order_acquire))
		std::this_thread::yield();
	Pumper pump(std::move(bridge), { Chunk{4} });
	const std::string payload(4096, 'z');
	const bool wrote = src.Write(std::string_view(payload));
	src.Close();
	const bool finished = WaitFinish(pump, std::chrono::seconds(10));
	if (!finished)
		pump.Cancel();
	stop.store(true, std::memory_order_release);
	observer.join();
	Binary got;
	const bool extracted = out.Extract(0, got);

	ASSERT_TRUE(wrote);
	ASSERT_TRUE(finished);
	ASSERT_TRUE(valid_snapshots);
	ASSERT_TRUE(snapshots > 0);
	ASSERT_EQUAL(ByteSize{payload.size()}, read->Delivered());
	ASSERT_EQUAL(ByteSize{payload.size()}, write->Accepted());
	ASSERT_TRUE(extracted);
	ASSERT_EQUAL(payload, BytesToText(got));
	RETURN_TEST(0);
}

int test_pumper_io_to_io_then_remove() {
	const auto in_path = std::filesystem::temp_directory_path() / "sbb_pump_in.tmp";
	const auto out_path = std::filesystem::temp_directory_path() / "sbb_pump_out.tmp";
	std::filesystem::remove(in_path);
	std::filesystem::remove(out_path);
	DumpText(in_path, "FILEPUMP");
	{
		BufferedFileReader in(Loc(in_path), { ReadAhead{ByteSize{0}}, MaxMemory{ByteSize{0}} });
		BufferedFileWriter out(Loc(out_path), { WriteChunk{ByteSize{0}}, MaxMemory{ByteSize{0}} });
		ASSERT_TRUE(in.Open());
		ASSERT_TRUE(out.Open());
		Pumper pump(Bridge(std::move(in), std::move(out)), { Chunk{4} });
		ASSERT_TRUE(WaitFinish(pump, std::chrono::seconds(2)));
		pump.Cancel();
	}
	ASSERT_EQUAL(std::string("FILEPUMP"), Slurp(out_path));
	ASSERT_TRUE(std::filesystem::remove(in_path));
	ASSERT_TRUE(std::filesystem::remove(out_path));
	RETURN_TEST(0);
}

int test_pumper_io_telemetry_during_transfer() {
	const auto in_path = std::filesystem::temp_directory_path() / "sbb_pump_telemetry_in.tmp";
	const auto out_path = std::filesystem::temp_directory_path() / "sbb_pump_telemetry_out.tmp";
	std::filesystem::remove(in_path);
	std::filesystem::remove(out_path);
	const std::string payload(16 * 1024, 'x');
	DumpText(in_path, payload);
	bool finished = false;
	bool valid_snapshots = true;
	std::size_t snapshots = 0;
	ByteSize delivered{0};
	ByteSize accepted{0};
	{
		BufferedFileReader in(Loc(in_path), { ReadAhead{ByteSize{0}}, MaxMemory{ByteSize{0}} });
		BufferedFileWriter out(Loc(out_path), { WriteChunk{ByteSize{0}}, MaxMemory{ByteSize{0}} });
		ASSERT_TRUE(in.Open());
		ASSERT_TRUE(out.Open());
		const auto read = in.Telemetry();
		const auto write = out.Telemetry();
		const auto* io_read = dynamic_cast<const StormByte::Buffer::IO::ReadTelemetry*>(read.get());
		const auto* io_write = dynamic_cast<const StormByte::Buffer::IO::WriteTelemetry*>(write.get());
		ASSERT_TRUE(io_read != nullptr);
		ASSERT_TRUE(io_write != nullptr);

		std::atomic<bool> ready{false};
		std::atomic<bool> stop{false};
		std::thread observer([&] {
			ByteSize previous_read{0};
			ByteSize previous_write{0};
			while (!stop.load(std::memory_order_acquire)) {
				const ByteSize current_read = io_read->Delivered();
				const ByteSize current_write = io_write->Accepted();
				if (current_read < previous_read || current_write < previous_write
						|| static_cast<std::string>(*io_read).empty()
						|| static_cast<std::string>(*io_write).empty())
					valid_snapshots = false;
				previous_read = current_read;
				previous_write = current_write;
				++snapshots;
				if (snapshots == 1)
					ready.store(true, std::memory_order_release);
			}
		});
		while (!ready.load(std::memory_order_acquire))
			std::this_thread::yield();
		Pumper pump(Bridge(std::move(in), std::move(out)), { Chunk{16} });
		finished = WaitFinish(pump, std::chrono::seconds(10));
		if (!finished)
			pump.Cancel();
		stop.store(true, std::memory_order_release);
		observer.join();
		delivered = io_read->Delivered();
		accepted = io_write->Accepted();
	}
	const std::string contents = Slurp(out_path);
	const bool removed_in = std::filesystem::remove(in_path);
	const bool removed_out = std::filesystem::remove(out_path);
	ASSERT_TRUE(finished);
	ASSERT_TRUE(valid_snapshots);
	ASSERT_TRUE(snapshots > 0);
	ASSERT_EQUAL(ByteSize{payload.size()}, delivered);
	ASSERT_EQUAL(ByteSize{payload.size()}, accepted);
	ASSERT_EQUAL(payload, contents);
	ASSERT_TRUE(removed_in);
	ASSERT_TRUE(removed_out);
	RETURN_TEST(0);
}

// -------------------
// Move
// -------------------

int test_pumper_move_leaves_source_failed() {
	FIFO in;
	FIFO out;
	in.Write("xyz");
	in.Close();
	Pumper pump(Bridge(in, out), { Chunk{1} });
	Pumper taken(std::move(pump));
	ASSERT_FALSE(pump.Failed());
	ASSERT_FALSE(pump.Canceled());
	ASSERT_TRUE(pump.EoF());
	ASSERT_TRUE(WaitFinish(taken, std::chrono::seconds(2)));
	Binary got;
	ASSERT_TRUE(out.Extract(0, got));
	ASSERT_EQUAL(ByteSize{3}, got.size());
	RETURN_TEST(0);
}

// -------------------
// Toggle
// -------------------

int test_pumper_toggle_pauses_and_resumes() {
	Producer src;
	FIFO out;
	Consumer in = src.Consumer();
	Pumper pump(Bridge(in, out), { Chunk{1} });
	src.Write("ABCD");
	const bool delivered = WaitDelivered(pump, ByteSize{1}, std::chrono::seconds(2));
	if (!delivered)
		pump.Cancel();
	ASSERT_TRUE(delivered);
	pump.Toggle();
	const auto paused = pump.ReadTelemetry();
	if (!paused)
		pump.Cancel();
	ASSERT_TRUE(static_cast<bool>(paused));
	const ByteSize at_pause = paused->Delivered();
	src.Write("EFGH");
	std::this_thread::sleep_for(std::chrono::milliseconds(80));
	const auto still = pump.ReadTelemetry();
	if (!still)
		pump.Cancel();
	ASSERT_TRUE(static_cast<bool>(still));
	const ByteSize during_pause = still->Delivered();
	if (at_pause != during_pause)
		pump.Cancel();
	ASSERT_EQUAL(at_pause, during_pause);
	pump.Toggle();
	src.Close();
	const bool finished = WaitFinish(pump, std::chrono::seconds(2));
	if (!finished)
		pump.Cancel();
	ASSERT_TRUE(finished);
	Binary got;
	ASSERT_TRUE(out.Extract(0, got));
	ASSERT_EQUAL(std::string("ABCDEFGH"), BytesToText(got));
	RETURN_TEST(0);
}

int test_pumper_toggle_waits_for_inflight_transfer() {
	Producer src;
	GatedFIFO out;
	Consumer in = src.Consumer();
	src.Write("AB");
	Pumper pump(Bridge(in, out), { Chunk{1} });
	const bool entered = out.WaitForWrite(std::chrono::seconds(2));
	if (!entered) {
		out.ReleaseWrite();
		pump.Cancel();
		src.Close();
	}
	ASSERT_TRUE(entered);

	std::atomic<bool> started{false};
	std::atomic<bool> returned{false};
	std::thread toggler([&] {
		started.store(true, std::memory_order_release);
		pump.Toggle();
		returned.store(true, std::memory_order_release);
	});
	while (!started.load(std::memory_order_acquire))
		std::this_thread::yield();
	std::this_thread::sleep_for(std::chrono::milliseconds(30));
	const bool returned_while_blocked = returned.load(std::memory_order_acquire);
	out.ReleaseWrite();
	toggler.join();

	const auto read = pump.ReadTelemetry();
	const ByteSize at_pause = read->Delivered();
	src.Write("C");
	std::this_thread::sleep_for(std::chrono::milliseconds(30));
	const ByteSize during_pause = read->Delivered();
	const bool paused = at_pause == during_pause;
	if (paused)
		pump.Toggle();
	else
		pump.Cancel();
	src.Close();
	const bool finished = paused && WaitFinish(pump, std::chrono::seconds(5));
	if (!finished)
		pump.Cancel();
	Binary got;
	const bool extracted = out.Extract(0, got);

	ASSERT_FALSE(returned_while_blocked);
	ASSERT_TRUE(paused);
	ASSERT_TRUE(finished);
	ASSERT_TRUE(extracted);
	ASSERT_EQUAL(std::string("ABC"), BytesToText(got));
	RETURN_TEST(0);
}

int test_pumper_toggle_waits_for_blocking_reader() {
	TransferGate gate;
	BlockingReader in(gate);
	FIFO out;
	ASSERT_TRUE(in.Open());
	Bridge bridge(std::move(in), out);
	ASSERT_TRUE(bridge.InputPullBlocking());
	Pumper pump(std::move(bridge), { Chunk{1} });
	const bool entered = gate.WaitForEntry(std::chrono::seconds(2));
	if (!entered) {
		gate.Release();
		pump.Cancel();
	}
	ASSERT_TRUE(entered);

	std::atomic<bool> started{false};
	std::atomic<bool> returned{false};
	std::thread toggler([&] {
		started.store(true, std::memory_order_release);
		pump.Toggle();
		returned.store(true, std::memory_order_release);
	});
	while (!started.load(std::memory_order_acquire))
		std::this_thread::yield();
	std::this_thread::sleep_for(std::chrono::milliseconds(30));
	const bool returned_while_blocked = returned.load(std::memory_order_acquire);
	gate.Release();
	toggler.join();

	const auto read = pump.ReadTelemetry();
	const ByteSize at_pause = read->Delivered();
	std::this_thread::sleep_for(std::chrono::milliseconds(30));
	const bool paused = at_pause == read->Delivered();
	if (paused)
		pump.Toggle();
	else
		pump.Cancel();
	const bool finished = paused && WaitFinish(pump, std::chrono::seconds(5));
	if (!finished)
		pump.Cancel();
	Binary got;
	const bool extracted = out.Extract(0, got);
	ASSERT_FALSE(returned_while_blocked);
	ASSERT_EQUAL(ByteSize{1}, at_pause);
	ASSERT_TRUE(paused);
	ASSERT_TRUE(finished);
	ASSERT_TRUE(extracted);
	ASSERT_EQUAL(std::string("Z"), BytesToText(got));
	RETURN_TEST(0);
}

int test_pumper_closed_source_while_paused() {
	Producer src;
	FIFO out;
	Consumer in = src.Consumer();
	Pumper pump(Bridge(in, out), { Chunk{1} });
	pump.Toggle();
	const bool wrote = src.Write("last");
	src.Close();
	std::this_thread::sleep_for(std::chrono::milliseconds(20));
	const ByteSize while_paused = pump.ReadTelemetry()->Delivered();
	if (while_paused == ByteSize{0})
		pump.Toggle();
	else
		pump.Cancel();
	const bool finished = while_paused == ByteSize{0} && WaitFinish(pump, std::chrono::seconds(5));
	if (!finished)
		pump.Cancel();
	Binary got;
	const bool extracted = out.Extract(0, got);
	ASSERT_TRUE(wrote);
	ASSERT_EQUAL(ByteSize{0}, while_paused);
	ASSERT_TRUE(finished);
	ASSERT_TRUE(extracted);
	ASSERT_EQUAL(std::string("last"), BytesToText(got));
	RETURN_TEST(0);
}

int test_pumper_repeated_pause_is_stable() {
	Producer src;
	FIFO out;
	Consumer in = src.Consumer();
	Pumper pump(Bridge(in, out), { Chunk{1} });
	const auto read = pump.ReadTelemetry();
	std::string expected;
	bool stable = true;
	bool wrote = true;
	for (int i = 0; i < 128; ++i) {
		pump.Toggle();
		const ByteSize at_pause = read->Delivered();
		wrote = src.Write("ABCD");
		expected += "ABCD";
		std::this_thread::sleep_for(std::chrono::milliseconds(1));
		stable = at_pause == read->Delivered();
		if (!stable || !wrote)
			break;
		pump.Toggle();
	}
	if (!stable || !wrote)
		pump.Cancel();
	src.Close();
	const bool finished = stable && wrote && WaitFinish(pump, std::chrono::seconds(10));
	if (!finished)
		pump.Cancel();
	Binary got;
	const bool extracted = out.Extract(0, got);
	ASSERT_TRUE(stable);
	ASSERT_TRUE(wrote);
	ASSERT_TRUE(finished);
	ASSERT_TRUE(extracted);
	ASSERT_EQUAL(expected, BytesToText(got));
	RETURN_TEST(0);
}

int main() {
	int result = 0;

	// -------------------
	// Cancel
	// -------------------
	result += test_pumper_cancel_before_close_does_not_drain();
	result += test_pumper_cancel_is_terminal();
	result += test_pumper_cancel_telemetry_survives();
	result += test_pumper_cancel_while_paused_is_terminal();

	// -------------------
	// Failed
	// -------------------
	result += test_bridge_source_failure_during_eof_probe();
	result += test_bridge_source_failure_after_transfer();
	result += test_pumper_source_failure_during_eof_probe();
	result += test_pumper_failed_is_not_canceled();

	// -------------------
	// Finish
	// -------------------
	result += test_pumper_auto_chunk_copies_all();
	result += test_pumper_chunked_copies_all();
	result += test_pumper_dtor_finishes_closed_source();
	result += test_pumper_empty_closed_source_is_eof();
	result += test_pumper_live_producer_then_close();

	// -------------------
	// HighWater
	// -------------------
	result += test_pumper_highwater_small_still_copies();
	result += test_pumper_highwater_zero_still_copies();

	// -------------------
	// IO
	// -------------------
	result += test_pumper_basic_telemetry_during_transfer();
	result += test_pumper_io_to_io_then_remove();
	result += test_pumper_io_telemetry_during_transfer();

	// -------------------
	// Move
	// -------------------
	result += test_pumper_move_leaves_source_failed();

	// -------------------
	// Toggle
	// -------------------
	result += test_pumper_toggle_pauses_and_resumes();
	result += test_pumper_toggle_waits_for_inflight_transfer();
	result += test_pumper_toggle_waits_for_blocking_reader();
	result += test_pumper_closed_source_while_paused();
	result += test_pumper_repeated_pause_is_stable();

	if (result == 0)
		std::cout << "All tests passed!" << std::endl;
	else
		std::cout << result << " tests failed." << std::endl;
	return result;
}
