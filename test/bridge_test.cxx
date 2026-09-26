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
#include <StormByte/buffer/io/buffered_file_reader.hxx>
#include <StormByte/buffer/io/buffered_file_writer.hxx>
#include <StormByte/buffer/producer.hxx>
#include <StormByte/buffer/shared_fifo.hxx>
#include <StormByte/test_handlers.h>

#include <algorithm>
#include <atomic>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <filesystem>
#include <fstream>
#include <iostream>
#include <string>
#include <thread>

using StormByte::Buffer::Bridge;
using StormByte::Buffer::Consumer;
using StormByte::BinaryData;
using StormByte::Buffer::ExternalBufferReader;
using StormByte::Buffer::ExternalBufferWriter;
using StormByte::Buffer::ExternalWriter;
using StormByte::Buffer::FIFO;
using StormByte::Buffer::Producer;
using StormByte::Buffer::SharedFIFO;
using StormByte::Buffer::IO::BufferedFileReader;
using StormByte::Buffer::IO::BufferedFileWriter;
using StormByte::Buffer::IO::State;
using StormByte::Buffer::IO::ToString;
using StormByte::Buffer::IO::Drainer::ToString;
using StormByte::Buffer::IO::Drainer::Operation;
using StormByte::Buffer::IO::Drainer::Status;

namespace {
	std::string BytesToText(const BinaryData& data) {
		if (data.empty())
			return {};
		return std::string(reinterpret_cast<const char*>(data.data()),
			static_cast<std::size_t>(data.size()));
	}

	std::filesystem::path File(const char* name) {
		return CurrentFileDirectory / "files" / name;
	}

	std::filesystem::path Scratch(const char* tag) {
		static std::uint64_t n = 0;
		++n;
		return std::filesystem::temp_directory_path() / (std::string("sbr_") + tag + std::to_string(n) + ".tmp");
	}

	std::string Slurp(const std::filesystem::path& path) {
		std::ifstream in(path, std::ios::in | std::ios::binary);
		if (!in)
			return {};
		return std::string(std::istreambuf_iterator<char>(in), std::istreambuf_iterator<char>());
	}

	FIFO FromText(const std::string& text) {
		FIFO fifo;
		BinaryData data(StormByte::ByteSize{text.size()});
		for (std::size_t i = 0; i < text.size(); ++i)
			data[i] = static_cast<std::byte>(text[i]);
		static_cast<void>(fifo.Write(data.size(), std::move(data)));
		return fifo;
	}

	BinaryData Pattern(const std::size_t n) {
		BinaryData data(StormByte::ByteSize{n});
		for (std::size_t i = 0; i < n; ++i)
			data[i] = static_cast<std::byte>(i & 0xFF);
		return data;
	}

	std::string FifoText(const FIFO& fifo) {
		return BytesToText(fifo.Data());
	}

	bool WaitFifoSize(const FIFO& fifo, const std::size_t n) {
		const StormByte::ByteSize want{n};
		for (int i = 0; i < 500; ++i) {
			if (fifo.Size() >= want)
				return true;
			std::this_thread::sleep_for(std::chrono::milliseconds(10));
		}
		return fifo.Size() >= want;
	}

	bool WaitSize(const StormByte::Buffer::Generic& buf, const std::size_t n) {
		const StormByte::ByteSize want{n};
		for (int i = 0; i < 500; ++i) {
			if (buf.Size() >= want)
				return true;
			std::this_thread::sleep_for(std::chrono::milliseconds(10));
		}
		return buf.Size() >= want;
	}

	bool WaitFile(const std::filesystem::path& path, const std::size_t n) {
		for (int i = 0; i < 500; ++i) {
			if (Slurp(path).size() >= n)
				return true;
			std::this_thread::sleep_for(std::chrono::milliseconds(10));
		}
		return Slurp(path).size() >= n;
	}

	bool WaitDirtyZero(BufferedFileWriter& out) {
		for (int i = 0; i < 200; ++i) {
			if (out.Dirty() == StormByte::ByteSize{0})
				return true;
			std::this_thread::sleep_for(std::chrono::milliseconds(10));
		}
		return out.Dirty() == StormByte::ByteSize{0};
	}

	bool WaitConsumerEof(const Consumer& consumer) {
		for (int i = 0; i < 500; ++i) {
			if (consumer.EoF())
				return true;
			std::this_thread::sleep_for(std::chrono::milliseconds(10));
		}
		return consumer.EoF();
	}

	class FailingWriter final : public ExternalWriter {
		public:
			FailingWriter(FIFO& to, std::size_t succeed_calls) noexcept
				: m_target(to), m_succeed(succeed_calls), m_calls(0), m_closed(false), m_error(false) {}

			bool IsWritable() const noexcept override {
				return !m_closed && !m_error;
			}

			StormByte::ByteSize Occupied() const noexcept override {
				return m_target.Size();
			}

			bool Write(const BinaryData& data) noexcept override {
				BinaryData copy = data;
				return Write(std::move(copy));
			}

			bool Write(BinaryData&& in) noexcept override {
				if (m_closed || m_error)
					return false;
				if (m_calls < m_succeed) {
					++m_calls;
					return m_target.Write(std::move(in));
				}
				return false;
			}

			bool Write(const StormByte::ByteSize& count, const BinaryData& data) noexcept override {
				if (count == StormByte::ByteSize{0})
					return Write(data);
				const StormByte::ByteSize n = std::min(count, data.size());
				BinaryData tmp(data.data(), n);
				return Write(std::move(tmp));
			}

			bool Write(const StormByte::ByteSize& count, BinaryData&& data) noexcept override {
				if (count == StormByte::ByteSize{0})
					return Write(std::move(data));
				if (count < data.size())
					data.resize(count);
				return Write(std::move(data));
			}

			void Close() noexcept override {
				m_closed = true;
			}

			void SetError() noexcept override {
				m_error = true;
			}

			PointerType Clone() const noexcept override {
				return MakePointer<FailingWriter>(m_target, m_succeed);
			}

			PointerType Move() noexcept override {
				return MakePointer<FailingWriter>(m_target, m_succeed);
			}

		private:
			FIFO& m_target;
			std::size_t m_succeed;
			std::size_t m_calls;
			bool m_closed;
			bool m_error;
	};
}

// -------------------
// Buffer to file
// -------------------

int test_buf_to_io() {
	const std::string fn = "test_buf_to_io";
	const auto out_path = Scratch("b2io");
	std::filesystem::remove(out_path);
	FIFO src = FromText("HELLO");
	src.Close();
	ExternalBufferReader in(src);
	BufferedFileWriter out(out_path, 0, 0);
	ASSERT_TRUE(fn, out.Open());
	Bridge bridge(in, out, 16);
	ASSERT_TRUE(fn, bridge.Flush());
	ASSERT_TRUE(fn, WaitDirtyZero(out));
	out.Close();
	ASSERT_TRUE(fn, WaitFile(out_path, 5));
	ASSERT_EQUAL(fn, std::string("HELLO"), Slurp(out_path));
	std::filesystem::remove(out_path);
	RETURN_TEST(fn, 0);
}

int test_buf_to_io_uncapped_ctor() {
	const std::string fn = "test_buf_to_io_uncapped_ctor";
	const auto out_path = Scratch("b2ioun");
	std::filesystem::remove(out_path);
	FIFO src = FromText("HELLO");
	src.Close();
	ExternalBufferReader in(src);
	BufferedFileWriter out(out_path, 0, 0);
	ASSERT_TRUE(fn, out.Open());
	Bridge bridge(in, out);
	ASSERT_EQUAL(fn, ToString(Status::Started), ToString(bridge.Drainer()));
	ASSERT_EQUAL(fn, StormByte::ByteSize{0}, bridge.HighWater());
	ASSERT_TRUE(fn, bridge.Flush());
	ASSERT_TRUE(fn, WaitDirtyZero(out));
	out.Close();
	ASSERT_TRUE(fn, WaitFile(out_path, 5));
	ASSERT_EQUAL(fn, std::string("HELLO"), Slurp(out_path));
	std::filesystem::remove(out_path);
	RETURN_TEST(fn, 0);
}

int test_buf_to_io_writer_not_open() {
	const std::string fn = "test_buf_to_io_writer_not_open";
	const auto out_path = Scratch("b2ion");
	std::filesystem::remove(out_path);
	FIFO src = FromText("NOPE");
	ExternalBufferReader in(src);
	BufferedFileWriter out(out_path, 0, 0);
	Bridge bridge(in, out, 16);
	std::this_thread::sleep_for(std::chrono::milliseconds(50));
	ASSERT_EQUAL(fn, StormByte::ByteSize{4}, src.Available());
	std::filesystem::remove(out_path);
	RETURN_TEST(fn, 0);
}

// -------------------
// Drainer
// -------------------

int test_backpressure_shared_fifo() {
	const std::string fn = "test_backpressure_shared_fifo";
	const std::string text = "ABCDEFGHIJKLMNOP";
	FIFO src = FromText(text);
	src.Close();
	SharedFIFO dst;
	ExternalBufferReader in(src);
	ExternalBufferWriter out(dst);
	Bridge bridge(in, out, 4);

	std::atomic<bool> stop {false};
	BinaryData collected;
	std::thread consumer([&] {
		while (!stop.load() || dst.Available() > StormByte::ByteSize{0}) {
			if (dst.Available() == StormByte::ByteSize{0}) {
				std::this_thread::yield();
				continue;
			}
			BinaryData chunk;
			const StormByte::ByteSize n = std::min(StormByte::ByteSize{2}, dst.Available());
			if (dst.Extract(n, chunk))
				collected.insert(collected.end(), chunk.begin(), chunk.end());
		}
	});

	for (int i = 0; i < 500 && collected.size() < StormByte::ByteSize{text.size()}; ++i)
		std::this_thread::sleep_for(std::chrono::milliseconds(10));
	stop.store(true);
	consumer.join();
	ASSERT_EQUAL(fn, text, BytesToText(collected));
	RETURN_TEST(fn, 0);
}

int test_bridge_flush_is_barrier() {
	const std::string fn = "test_bridge_flush_is_barrier";
	FIFO src = FromText("FLUSHME");
	src.Close();
	FIFO dst;
	ExternalBufferReader in(src);
	ExternalBufferWriter out(dst);
	Bridge bridge(in, out, 32);
	ASSERT_TRUE(fn, bridge.Flush());
	ASSERT_EQUAL(fn, std::string("FLUSHME"), FifoText(dst));
	RETURN_TEST(fn, 0);
}

int test_drainer_flush_hurry_on_buffer() {
	const std::string fn = "test_drainer_flush_hurry_on_buffer";
	FIFO src = FromText("ABCDEFGH");
	src.Close();
	FIFO dst;
	ExternalBufferReader in(src);
	ExternalBufferWriter out(dst);
	Bridge bridge(in, out, 64);
	ASSERT_TRUE(fn, WaitFifoSize(dst, 8));
	ASSERT_TRUE(fn, bridge.Drainer(Operation::Flush));
	ASSERT_EQUAL(fn, std::string("ABCDEFGH"), FifoText(dst));
	ASSERT_EQUAL(fn, ToString(Status::Started), ToString(bridge.Drainer()));
	RETURN_TEST(fn, 0);
}

int test_drainer_ops_on_moved_from() {
	const std::string fn = "test_drainer_ops_on_moved_from";
	FIFO src = FromText("Z");
	src.Close();
	FIFO dst;
	ExternalBufferReader in(src);
	ExternalBufferWriter out(dst);
	Bridge first(in, out, 8);
	Bridge second(std::move(first));
	ASSERT_EQUAL(fn, ToString(Status::Stopped), ToString(first.Drainer()));
	ASSERT_FALSE(fn, first.Drainer(Operation::Toggle));
	ASSERT_FALSE(fn, first.Drainer(Operation::Flush));
	ASSERT_FALSE(fn, first.Flush());
	RETURN_TEST(fn, 0);
}

int test_high_water_setter_does_not_toggle() {
	const std::string fn = "test_high_water_setter_does_not_toggle";
	FIFO src = FromText("XYZ");
	FIFO dst;
	ExternalBufferReader in(src);
	ExternalBufferWriter out(dst);
	Bridge bridge(in, out, 16);
	ASSERT_TRUE(fn, WaitFifoSize(dst, 3));
	ASSERT_TRUE(fn, bridge.Drainer(Operation::Toggle));
	ASSERT_EQUAL(fn, ToString(Status::Paused), ToString(bridge.Drainer()));
	bridge.HighWater(0);
	ASSERT_EQUAL(fn, StormByte::ByteSize{0}, bridge.HighWater());
	ASSERT_EQUAL(fn, ToString(Status::Paused), ToString(bridge.Drainer()));
	ASSERT_TRUE(fn, src.Write("!!!"));
	std::this_thread::sleep_for(std::chrono::milliseconds(50));
	ASSERT_EQUAL(fn, std::string("XYZ"), FifoText(dst));
	RETURN_TEST(fn, 0);
}

int test_toggle_pauses_and_resumes_buffer() {
	const std::string fn = "test_toggle_pauses_and_resumes_buffer";
	FIFO src = FromText("HELLO");
	FIFO dst;
	ExternalBufferReader in(src);
	ExternalBufferWriter out(dst);
	Bridge bridge(in, out, 64);
	ASSERT_TRUE(fn, WaitFifoSize(dst, 5));
	ASSERT_EQUAL(fn, std::string("HELLO"), FifoText(dst));
	ASSERT_TRUE(fn, bridge.Drainer(Operation::Toggle));
	ASSERT_EQUAL(fn, ToString(Status::Paused), ToString(bridge.Drainer()));
	ASSERT_TRUE(fn, src.Write("WORLD"));
	std::this_thread::sleep_for(std::chrono::milliseconds(80));
	ASSERT_EQUAL(fn, std::string("HELLO"), FifoText(dst));
	ASSERT_TRUE(fn, bridge.Drainer(Operation::Toggle));
	ASSERT_EQUAL(fn, ToString(Status::Started), ToString(bridge.Drainer()));
	ASSERT_TRUE(fn, WaitFifoSize(dst, 10));
	ASSERT_EQUAL(fn, std::string("HELLOWORLD"), FifoText(dst));
	RETURN_TEST(fn, 0);
}

int test_toggle_pauses_with_high_water_zero() {
	const std::string fn = "test_toggle_pauses_with_high_water_zero";
	FIFO src = FromText("ABC");
	FIFO dst;
	ExternalBufferReader in(src);
	ExternalBufferWriter out(dst);
	Bridge bridge(in, out, 0);
	ASSERT_EQUAL(fn, ToString(Status::Started), ToString(bridge.Drainer()));
	ASSERT_TRUE(fn, WaitFifoSize(dst, 3));
	ASSERT_TRUE(fn, bridge.Drainer(Operation::Toggle));
	ASSERT_EQUAL(fn, ToString(Status::Paused), ToString(bridge.Drainer()));
	ASSERT_TRUE(fn, src.Write("DEF"));
	std::this_thread::sleep_for(std::chrono::milliseconds(50));
	ASSERT_EQUAL(fn, std::string("ABC"), FifoText(dst));
	ASSERT_TRUE(fn, bridge.Drainer(Operation::Toggle));
	ASSERT_TRUE(fn, WaitFifoSize(dst, 6));
	ASSERT_EQUAL(fn, std::string("ABCDEF"), FifoText(dst));
	RETURN_TEST(fn, 0);
}

// -------------------
// External
// -------------------

int test_close_source_while_started() {
	const std::string fn = "test_close_source_while_started";
	FIFO src = FromText("HELLO");
	FIFO dst;
	ExternalBufferReader in(src);
	ExternalBufferWriter out(dst);
	Bridge bridge(in, out, 64);
	ASSERT_TRUE(fn, WaitFifoSize(dst, 5));
	ASSERT_TRUE(fn, src.Write("WORLD"));
	src.Close();
	ASSERT_TRUE(fn, WaitFifoSize(dst, 10));
	ASSERT_TRUE(fn, bridge.Flush());
	ASSERT_EQUAL(fn, std::string("HELLOWORLD"), FifoText(dst));
	ASSERT_TRUE(fn, bridge.EoF());
	RETURN_TEST(fn, 0);
}

int test_ext_drains_all() {
	const std::string fn = "test_ext_drains_all";
	const std::string text = "The quick brown fox jumps over the lazy dog.";
	FIFO src = FromText(text);
	src.Close();
	FIFO dst;
	ExternalBufferReader in(src);
	ExternalBufferWriter out(dst);
	Bridge bridge(in, out, 64);
	ASSERT_EQUAL(fn, ToString(Status::Started), ToString(bridge.Drainer()));
	ASSERT_TRUE(fn, WaitFifoSize(dst, text.size()));
	ASSERT_TRUE(fn, bridge.Flush());
	ASSERT_EQUAL(fn, text, FifoText(dst));
	ASSERT_TRUE(fn, bridge.EoF());
	RETURN_TEST(fn, 0);
}

int test_ext_eof_after_close_source() {
	const std::string fn = "test_ext_eof_after_close_source";
	FIFO src = FromText("X");
	src.Close();
	FIFO dst;
	ExternalBufferReader in(src);
	ExternalBufferWriter out(dst);
	Bridge bridge(in, out, 8);
	ASSERT_TRUE(fn, WaitFifoSize(dst, 1));
	ASSERT_TRUE(fn, bridge.EoF());
	RETURN_TEST(fn, 0);
}

int test_ext_flush_and_close() {
	const std::string fn = "test_ext_flush_and_close";
	FIFO src = FromText("CLOSEME");
	src.Close();
	FIFO dst;
	ExternalBufferReader in(src);
	ExternalBufferWriter out(dst);
	Bridge bridge(in, out, 16);
	ASSERT_TRUE(fn, WaitFifoSize(dst, 7));
	ASSERT_TRUE(fn, bridge.FlushAndClose());
	ASSERT_FALSE(fn, out.IsWritable());
	RETURN_TEST(fn, 0);
}

int test_ext_high_water_zero_starts_running() {
	const std::string fn = "test_ext_high_water_zero_starts_running";
	FIFO src = FromText("ABCDE");
	src.Close();
	FIFO dst;
	ExternalBufferReader in(src);
	ExternalBufferWriter out(dst);
	Bridge bridge(in, out, 0);
	ASSERT_EQUAL(fn, ToString(Status::Started), ToString(bridge.Drainer()));
	ASSERT_TRUE(fn, WaitFifoSize(dst, 5));
	ASSERT_EQUAL(fn, std::string("ABCDE"), FifoText(dst));
	RETURN_TEST(fn, 0);
}

int test_ext_move_assign_bridge() {
	const std::string fn = "test_ext_move_assign_bridge";
	FIFO src_a = FromText("AAAA");
	src_a.Close();
	FIFO dst_a;
	ExternalBufferReader in_a(src_a);
	ExternalBufferWriter out_a(dst_a);
	Bridge a(in_a, out_a, 16);
	FIFO src_b = FromText("BBBB");
	src_b.Close();
	FIFO dst_b;
	ExternalBufferReader in_b(src_b);
	ExternalBufferWriter out_b(dst_b);
	Bridge b(in_b, out_b, 16);
	b = std::move(a);
	ASSERT_EQUAL(fn, ToString(Status::Stopped), ToString(a.Drainer()));
	ASSERT_TRUE(fn, WaitFifoSize(dst_a, 4));
	ASSERT_EQUAL(fn, std::string("AAAA"), FifoText(dst_a));
	RETURN_TEST(fn, 0);
}

int test_ext_move_bridge() {
	const std::string fn = "test_ext_move_bridge";
	FIFO src = FromText("MOVEOK");
	src.Close();
	FIFO dst;
	ExternalBufferReader in(src);
	ExternalBufferWriter out(dst);
	Bridge first(in, out, 16);
	Bridge second(std::move(first));
	ASSERT_EQUAL(fn, ToString(Status::Stopped), ToString(first.Drainer()));
	ASSERT_FALSE(fn, first.Drainer(Operation::Toggle));
	ASSERT_TRUE(fn, WaitFifoSize(dst, 6));
	ASSERT_EQUAL(fn, std::string("MOVEOK"), FifoText(dst));
	RETURN_TEST(fn, 0);
}

int test_ext_set_error() {
	const std::string fn = "test_ext_set_error";
	FIFO src = FromText("ERR");
	FIFO dst;
	ExternalBufferReader in(src);
	ExternalBufferWriter out(dst);
	Bridge bridge(in, out, 16);
	bridge.SetError();
	ASSERT_FALSE(fn, bridge.IsWritable());
	RETURN_TEST(fn, 0);
}

int test_ext_writer_failure() {
	const std::string fn = "test_ext_writer_failure";
	FIFO src = FromText("NOPE");
	src.Close();
	FIFO dst;
	ExternalBufferReader in(src);
	FailingWriter out(dst, 0);
	Bridge bridge(in, out, 16);
	std::this_thread::sleep_for(std::chrono::milliseconds(50));
	ASSERT_FALSE(fn, bridge.Flush());
	ASSERT_EQUAL(fn, StormByte::ByteSize{0}, dst.Size());
	RETURN_TEST(fn, 0);
}

// -------------------
// File to buffer
// -------------------

int test_io_to_buf() {
	const std::string fn = "test_io_to_buf";
	FIFO dst;
	BufferedFileReader in(File("five.bin"));
	ExternalBufferWriter out(dst);
	ASSERT_TRUE(fn, in.Open());
	Bridge bridge(in, out, 16);
	ASSERT_TRUE(fn, WaitFifoSize(dst, 5));
	ASSERT_EQUAL(fn, std::string("ABCDE"), FifoText(dst));
	ASSERT_TRUE(fn, in.EoF());
	in.Close();
	RETURN_TEST(fn, 0);
}

int test_io_to_buf_nul_and_binary() {
	const std::string fn = "test_io_to_buf_nul_and_binary";
	FIFO dst;
	BufferedFileReader in(File("with_nuls.bin"));
	ExternalBufferWriter out(dst);
	ASSERT_TRUE(fn, in.Open());
	Bridge bridge(in, out, 16);
	ASSERT_TRUE(fn, WaitFifoSize(dst, 5));
	ASSERT_EQUAL(fn, Slurp(File("with_nuls.bin")), FifoText(dst));
	in.Close();
	RETURN_TEST(fn, 0);
}

int test_io_to_buf_pattern() {
	const std::string fn = "test_io_to_buf_pattern";
	FIFO dst;
	BufferedFileReader in(File("pattern_256.bin"));
	ExternalBufferWriter out(dst);
	ASSERT_TRUE(fn, in.Open());
	Bridge bridge(in, out, 512);
	ASSERT_TRUE(fn, WaitFifoSize(dst, 256));
	ASSERT_EQUAL(fn, Slurp(File("pattern_256.bin")), FifoText(dst));
	in.Close();
	RETURN_TEST(fn, 0);
}

int test_io_to_buf_reader_not_open() {
	const std::string fn = "test_io_to_buf_reader_not_open";
	FIFO dst;
	BufferedFileReader in(File("five.bin"));
	ExternalBufferWriter out(dst);
	Bridge bridge(in, out, 16);
	std::this_thread::sleep_for(std::chrono::milliseconds(50));
	ASSERT_EQUAL(fn, StormByte::ByteSize{0}, dst.Size());
	RETURN_TEST(fn, 0);
}

// -------------------
// File to file
// -------------------

int test_io_empty_file() {
	const std::string fn = "test_io_empty_file";
	const auto out_path = Scratch("empty");
	std::filesystem::remove(out_path);
	BufferedFileReader in(File("empty.bin"));
	BufferedFileWriter out(out_path, 0, 0);
	ASSERT_TRUE(fn, in.Open());
	ASSERT_TRUE(fn, out.Open());
	Bridge bridge(in, out, 16);
	ASSERT_TRUE(fn, bridge.Flush());
	in.Close();
	out.Close();
	ASSERT_EQUAL(fn, std::string(""), Slurp(out_path));
	std::filesystem::remove(out_path);
	RETURN_TEST(fn, 0);
}

int test_io_file_to_file() {
	const std::string fn = "test_io_file_to_file";
	const auto out_path = Scratch("io2io");
	std::filesystem::remove(out_path);
	BufferedFileReader in(File("five.bin"));
	BufferedFileWriter out(out_path, 0, 0);
	ASSERT_TRUE(fn, in.Open());
	ASSERT_TRUE(fn, out.Open());
	Bridge bridge(in, out, 16);
	ASSERT_TRUE(fn, bridge.Flush());
	in.Close();
	out.Close();
	ASSERT_TRUE(fn, WaitFile(out_path, 5));
	ASSERT_EQUAL(fn, std::string("ABCDE"), Slurp(out_path));
	std::filesystem::remove(out_path);
	RETURN_TEST(fn, 0);
}

int test_io_flush_and_close_does_not_close_file() {
	const std::string fn = "test_io_flush_and_close_does_not_close_file";
	const auto out_path = Scratch("noclose");
	std::filesystem::remove(out_path);
	BufferedFileReader in(File("five.bin"));
	BufferedFileWriter out(out_path, 0, 0);
	ASSERT_TRUE(fn, in.Open());
	ASSERT_TRUE(fn, out.Open());
	Bridge bridge(in, out, 16);
	ASSERT_TRUE(fn, bridge.FlushAndClose());
	ASSERT_TRUE(fn, static_cast<bool>(out));
	ASSERT_EQUAL(fn, ToString(State::Idle), ToString(out.State()));
	in.Close();
	out.Close();
	ASSERT_TRUE(fn, WaitFile(out_path, 5));
	std::filesystem::remove(out_path);
	RETURN_TEST(fn, 0);
}

int test_io_high_water_zero_pumps() {
	const std::string fn = "test_io_high_water_zero_pumps";
	const auto out_path = Scratch("iohw0");
	std::filesystem::remove(out_path);
	BufferedFileReader in(File("five.bin"));
	BufferedFileWriter out(out_path, 0, 0);
	ASSERT_TRUE(fn, in.Open());
	ASSERT_TRUE(fn, out.Open());
	Bridge bridge(in, out, 0);
	ASSERT_EQUAL(fn, ToString(Status::Started), ToString(bridge.Drainer()));
	ASSERT_TRUE(fn, bridge.Flush());
	in.Close();
	out.Close();
	ASSERT_TRUE(fn, WaitFile(out_path, 5));
	ASSERT_EQUAL(fn, std::string("ABCDE"), Slurp(out_path));
	std::filesystem::remove(out_path);
	RETURN_TEST(fn, 0);
}

int test_io_pattern_256() {
	const std::string fn = "test_io_pattern_256";
	const auto out_path = Scratch("pat");
	std::filesystem::remove(out_path);
	BufferedFileReader in(File("pattern_256.bin"));
	BufferedFileWriter out(out_path, 0, 0);
	ASSERT_TRUE(fn, in.Open());
	ASSERT_TRUE(fn, out.Open());
	Bridge bridge(in, out, 512);
	ASSERT_TRUE(fn, bridge.Flush());
	in.Close();
	out.Close();
	ASSERT_TRUE(fn, WaitFile(out_path, 256));
	ASSERT_EQUAL(fn, Slurp(File("pattern_256.bin")), Slurp(out_path));
	std::filesystem::remove(out_path);
	RETURN_TEST(fn, 0);
}

int test_io_set_error_noop() {
	const std::string fn = "test_io_set_error_noop";
	const auto out_path = Scratch("seterr");
	std::filesystem::remove(out_path);
	BufferedFileReader in(File("five.bin"));
	BufferedFileWriter out(out_path, 0, 0);
	ASSERT_TRUE(fn, in.Open());
	ASSERT_TRUE(fn, out.Open());
	Bridge bridge(in, out, 16);
	bridge.SetError();
	ASSERT_TRUE(fn, bridge.Flush());
	ASSERT_TRUE(fn, bridge.IsWritable());
	in.Close();
	out.Close();
	ASSERT_TRUE(fn, WaitFile(out_path, 5));
	std::filesystem::remove(out_path);
	RETURN_TEST(fn, 0);
}

int test_io_uncapped_ctor() {
	const std::string fn = "test_io_uncapped_ctor";
	const auto out_path = Scratch("iouncap");
	std::filesystem::remove(out_path);
	BufferedFileReader in(File("five.bin"));
	BufferedFileWriter out(out_path, 0, 0);
	ASSERT_TRUE(fn, in.Open());
	ASSERT_TRUE(fn, out.Open());
	Bridge bridge(in, out);
	ASSERT_EQUAL(fn, ToString(Status::Started), ToString(bridge.Drainer()));
	ASSERT_EQUAL(fn, StormByte::ByteSize{0}, bridge.HighWater());
	ASSERT_TRUE(fn, bridge.Flush());
	in.Close();
	out.Close();
	ASSERT_TRUE(fn, WaitFile(out_path, 5));
	ASSERT_EQUAL(fn, std::string("ABCDE"), Slurp(out_path));
	std::filesystem::remove(out_path);
	RETURN_TEST(fn, 0);
}

int test_io_unopened_does_not_write() {
	const std::string fn = "test_io_unopened_does_not_write";
	const auto out_path = Scratch("unopen");
	std::filesystem::remove(out_path);
	BufferedFileReader in(File("five.bin"));
	BufferedFileWriter out(out_path, 0, 0);
	Bridge bridge(in, out, 16);
	std::this_thread::sleep_for(std::chrono::milliseconds(50));
	ASSERT_FALSE(fn, bridge.IsReadable());
	ASSERT_FALSE(fn, bridge.IsWritable());
	ASSERT_EQUAL(fn, std::string(""), Slurp(out_path));
	std::filesystem::remove(out_path);
	RETURN_TEST(fn, 0);
}

// -------------------
// Muxer / demuxer
// -------------------

int test_demuxer_file_to_producer() {
	const std::string fn = "test_demuxer_file_to_producer";
	Producer producer;
	Consumer consumer = producer.Consumer();
	BufferedFileReader in(File("pattern_256.bin"));
	ExternalBufferWriter out(producer);
	ASSERT_TRUE(fn, in.Open());
	Bridge bridge(in, out, 512);
	ASSERT_TRUE(fn, WaitSize(consumer, 256));
	ASSERT_TRUE(fn, bridge.Flush());
	BinaryData got;
	ASSERT_TRUE(fn, consumer.Extract(StormByte::ByteSize{256}, got));
	ASSERT_EQUAL(fn, Slurp(File("pattern_256.bin")), BytesToText(got));
	ASSERT_TRUE(fn, in.EoF());
	in.Close();
	producer.Close();
	RETURN_TEST(fn, 0);
}

int test_demuxer_file_to_producer_high_water() {
	const std::string fn = "test_demuxer_file_to_producer_high_water";
	const std::size_t total = 1024 * 1024;
	const BinaryData payload = Pattern(total);
	const auto src_path = Scratch("demux_src");
	std::filesystem::remove(src_path);
	{
		std::ofstream seed(src_path, std::ios::out | std::ios::binary | std::ios::trunc);
		seed.write(reinterpret_cast<const char*>(payload.data()),
			static_cast<std::streamsize>(static_cast<std::size_t>(payload.size())));
	}

	Producer producer;
	Consumer consumer = producer.Consumer();
	BufferedFileReader in(src_path);
	ExternalBufferWriter out(producer);
	ASSERT_TRUE(fn, in.Open());
	Bridge bridge(in, out, 4096);

	BinaryData collected;
	collected.reserve(StormByte::ByteSize{total});
	for (int i = 0; i < 4000 && collected.size() < StormByte::ByteSize{total}; ++i) {
		if (consumer.Size() == StormByte::ByteSize{0}) {
			if (in.EoF())
				break;
			std::this_thread::sleep_for(std::chrono::milliseconds(5));
			continue;
		}
		BinaryData chunk;
		const StormByte::ByteSize n = std::min(StormByte::ByteSize{4096}, consumer.Size());
		if (consumer.Extract(n, chunk))
			collected.insert(collected.end(), chunk.begin(), chunk.end());
	}
	ASSERT_TRUE(fn, bridge.Flush());
	while (consumer.Size() > StormByte::ByteSize{0}) {
		BinaryData chunk;
		if (!consumer.Extract(consumer.Size(), chunk))
			break;
		collected.insert(collected.end(), chunk.begin(), chunk.end());
	}
	ASSERT_EQUAL(fn, StormByte::ByteSize{total}, collected.size());
	ASSERT_EQUAL(fn, BytesToText(payload), BytesToText(collected));
	in.Close();
	producer.Close();
	std::filesystem::remove(src_path);
	RETURN_TEST(fn, 0);
}

int test_muxer_producer_to_file_high_water() {
	const std::string fn = "test_muxer_producer_to_file_high_water";
	const std::size_t total = 1024 * 1024;
	const auto out_path = Scratch("mux");
	std::filesystem::remove(out_path);

	Producer producer;
	Consumer consumer = producer.Consumer();
	const BinaryData payload = Pattern(total);
	ASSERT_TRUE(fn, producer.Write(payload));
	producer.Close();

	ExternalBufferReader in(consumer);
	BufferedFileWriter out(out_path, 0, 0);
	ASSERT_TRUE(fn, out.Open());
	Bridge bridge(in, out, 4096);
	ASSERT_TRUE(fn, WaitConsumerEof(consumer));
	ASSERT_TRUE(fn, bridge.Flush());
	ASSERT_TRUE(fn, WaitDirtyZero(out));
	out.Close();
	ASSERT_TRUE(fn, WaitFile(out_path, total));

	const std::string disk = Slurp(out_path);
	ASSERT_EQUAL(fn, total, disk.size());
	ASSERT_EQUAL(fn, BytesToText(payload), disk);
	std::filesystem::remove(out_path);
	RETURN_TEST(fn, 0);
}

int test_muxer_then_demuxer_roundtrip() {
	const std::string fn = "test_muxer_then_demuxer_roundtrip";
	const std::string text = "roundtrip-bridge-mux-demux";
	const auto path = Scratch("round");
	std::filesystem::remove(path);

	{
		Producer producer;
		Consumer consumer = producer.Consumer();
		ASSERT_TRUE(fn, producer.Write(text));
		producer.Close();
		ExternalBufferReader in(consumer);
		BufferedFileWriter out(path, 0, 0);
		ASSERT_TRUE(fn, out.Open());
		Bridge mux(in, out, 8);
		ASSERT_TRUE(fn, WaitConsumerEof(consumer));
		ASSERT_TRUE(fn, mux.Flush());
		ASSERT_TRUE(fn, WaitDirtyZero(out));
		out.Close();
		ASSERT_TRUE(fn, WaitFile(path, text.size()));
	}

	{
		Producer producer;
		Consumer consumer = producer.Consumer();
		BufferedFileReader in(path);
		ExternalBufferWriter out(producer);
		ASSERT_TRUE(fn, in.Open());
		Bridge demux(in, out, 4096);
		ASSERT_TRUE(fn, WaitSize(consumer, text.size()));
		ASSERT_TRUE(fn, demux.Flush());
		BinaryData got;
		ASSERT_TRUE(fn, consumer.Extract(StormByte::ByteSize{text.size()}, got));
		ASSERT_EQUAL(fn, text, BytesToText(got));
		in.Close();
		producer.Close();
	}

	std::filesystem::remove(path);
	RETURN_TEST(fn, 0);
}

// -------------------
// Producer
// -------------------

int test_producer_close_empty() {
	const std::string fn = "test_producer_close_empty";
	Producer producer;
	Consumer consumer = producer.Consumer();
	FIFO dst;
	ExternalBufferReader in(consumer);
	ExternalBufferWriter out(dst);
	Bridge bridge(in, out, 16);
	producer.Close();
	ASSERT_TRUE(fn, bridge.Flush());
	ASSERT_EQUAL(fn, StormByte::ByteSize{0}, dst.Size());
	ASSERT_TRUE(fn, consumer.EoF());
	ASSERT_TRUE(fn, bridge.EoF());
	RETURN_TEST(fn, 0);
}

int test_producer_close_while_started_wakes_worker() {
	const std::string fn = "test_producer_close_while_started_wakes_worker";
	Producer producer;
	Consumer consumer = producer.Consumer();
	FIFO dst;
	ExternalBufferReader in(consumer);
	ExternalBufferWriter out(dst);
	Bridge bridge(in, out, 64);
	ASSERT_TRUE(fn, producer.Write("ABC"));
	ASSERT_TRUE(fn, WaitFifoSize(dst, 3));
	ASSERT_TRUE(fn, producer.Write("DEF"));
	producer.Close();
	ASSERT_TRUE(fn, WaitFifoSize(dst, 6));
	ASSERT_TRUE(fn, bridge.Flush());
	ASSERT_EQUAL(fn, std::string("ABCDEF"), FifoText(dst));
	ASSERT_TRUE(fn, consumer.EoF());
	ASSERT_TRUE(fn, bridge.EoF());
	RETURN_TEST(fn, 0);
}

int main() {
	int result = 0;

	// -------------------
	// Buffer to file
	// -------------------
	result += test_buf_to_io();
	result += test_buf_to_io_uncapped_ctor();
	result += test_buf_to_io_writer_not_open();

	// -------------------
	// Drainer
	// -------------------
	result += test_backpressure_shared_fifo();
	result += test_bridge_flush_is_barrier();
	result += test_drainer_flush_hurry_on_buffer();
	result += test_drainer_ops_on_moved_from();
	result += test_high_water_setter_does_not_toggle();
	result += test_toggle_pauses_and_resumes_buffer();
	result += test_toggle_pauses_with_high_water_zero();

	// -------------------
	// External
	// -------------------
	result += test_close_source_while_started();
	result += test_ext_drains_all();
	result += test_ext_eof_after_close_source();
	result += test_ext_flush_and_close();
	result += test_ext_high_water_zero_starts_running();
	result += test_ext_move_assign_bridge();
	result += test_ext_move_bridge();
	result += test_ext_set_error();
	result += test_ext_writer_failure();

	// -------------------
	// File to buffer
	// -------------------
	result += test_io_to_buf();
	result += test_io_to_buf_nul_and_binary();
	result += test_io_to_buf_pattern();
	result += test_io_to_buf_reader_not_open();

	// -------------------
	// File to file
	// -------------------
	result += test_io_empty_file();
	result += test_io_file_to_file();
	result += test_io_flush_and_close_does_not_close_file();
	result += test_io_high_water_zero_pumps();
	result += test_io_pattern_256();
	result += test_io_set_error_noop();
	result += test_io_uncapped_ctor();
	result += test_io_unopened_does_not_write();

	// -------------------
	// Muxer / demuxer
	// -------------------
	result += test_demuxer_file_to_producer();
	result += test_demuxer_file_to_producer_high_water();
	result += test_muxer_producer_to_file_high_water();
	result += test_muxer_then_demuxer_roundtrip();

	// -------------------
	// Producer
	// -------------------
	result += test_producer_close_empty();
	result += test_producer_close_while_started_wakes_worker();

	if (result == 0)
		std::cout << "All tests passed!" << std::endl;
	else
		std::cout << result << " tests failed." << std::endl;
	return result;
}
