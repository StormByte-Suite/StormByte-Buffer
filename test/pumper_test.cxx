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
#include <StormByte/buffer/producer.hxx>
#include <StormByte/buffer/pumper.hxx>
#include <StormByte/byte_size.hxx>
#include <StormByte/safe/binary.hxx>
#include <StormByte/safe/string.hxx>
#include <StormByte/test_handlers.h>

#include <chrono>
#include <filesystem>
#include <fstream>
#include <iostream>
#include <iterator>
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
using StormByte::Buffer::IO::MaxMemory;
using StormByte::Buffer::IO::ReadAhead;
using StormByte::Buffer::IO::WriteChunk;
using StormByte::Safe::Binary;

namespace {
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

// -------------------
// Failed
// -------------------

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
	ASSERT_TRUE(WaitDelivered(pump, ByteSize{1}, std::chrono::seconds(2)));
	pump.Toggle();
	const auto paused = pump.ReadTelemetry();
	ASSERT_TRUE(static_cast<bool>(paused));
	const ByteSize at_pause = paused->Delivered();
	src.Write("EFGH");
	std::this_thread::sleep_for(std::chrono::milliseconds(80));
	const auto still = pump.ReadTelemetry();
	ASSERT_TRUE(static_cast<bool>(still));
	ASSERT_EQUAL(at_pause, still->Delivered());
	pump.Toggle();
	src.Close();
	ASSERT_TRUE(WaitFinish(pump, std::chrono::seconds(2)));
	Binary got;
	ASSERT_TRUE(out.Extract(0, got));
	ASSERT_EQUAL(std::string("ABCDEFGH"), BytesToText(got));
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

	// -------------------
	// Failed
	// -------------------
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
	result += test_pumper_io_to_io_then_remove();

	// -------------------
	// Move
	// -------------------
	result += test_pumper_move_leaves_source_failed();

	// -------------------
	// Toggle
	// -------------------
	result += test_pumper_toggle_pauses_and_resumes();

	if (result == 0)
		std::cout << "All tests passed!" << std::endl;
	else
		std::cout << result << " tests failed." << std::endl;
	return result;
}
