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

#include <StormByte/binary_data.hxx>
#include <StormByte/buffer/bridge.hxx>
#include <StormByte/buffer/consumer.hxx>
#include <StormByte/buffer/fifo.hxx>
#include <StormByte/buffer/io/buffered_file_reader.hxx>
#include <StormByte/buffer/io/buffered_file_writer.hxx>
#include <StormByte/buffer/producer.hxx>
#include <StormByte/buffer/pumper.hxx>
#include <StormByte/byte_size.hxx>
#include <StormByte/string/string.hxx>
#include <StormByte/test_handlers.h>

#include <chrono>
#include <filesystem>
#include <fstream>
#include <iostream>
#include <iterator>
#include <string>
#include <string_view>
#include <thread>

using StormByte::BinaryData;
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

namespace {
	StormByte::String::String Loc(const std::filesystem::path& path) {
#ifdef WINDOWS
		return StormByte::String::String(StormByte::String::WString(std::wstring_view(path.wstring())));
#else
		return StormByte::String::String(std::string_view(path.string()));
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
	constexpr auto fn = "test_pumper_cancel_before_close_does_not_drain";
	int result = 0;
	Producer src;
	FIFO out;
	Consumer in = src.Consumer();
	src.Write("ABCDEFGH");
	Pumper pump(Bridge(in, out), { Chunk{2} });
	ASSERT_TRUE(fn, WaitDelivered(pump, ByteSize{1}, std::chrono::seconds(2)));
	pump.Cancel();
	src.Write("XXXXXXXX");
	src.Close();
	std::this_thread::sleep_for(std::chrono::milliseconds(50));
	ASSERT_TRUE(fn, pump.Canceled());
	ASSERT_TRUE(fn, !pump.Failed());
	const auto tel = pump.ReadTelemetry();
	ASSERT_TRUE(fn, static_cast<bool>(tel));
	ASSERT_TRUE(fn, tel->Delivered() < ByteSize{16});
	RETURN_TEST(fn, result);
}

int test_pumper_cancel_is_terminal() {
	constexpr auto fn = "test_pumper_cancel_is_terminal";
	int result = 0;
	Producer src;
	FIFO out;
	Consumer in = src.Consumer();
	Pumper pump(Bridge(in, out), { Chunk{64} });
	ASSERT_TRUE(fn, !pump.Failed());
	ASSERT_TRUE(fn, !pump.Canceled());
	pump.Cancel();
	ASSERT_TRUE(fn, pump.Canceled());
	ASSERT_TRUE(fn, !pump.Failed());
	pump.Toggle();
	ASSERT_TRUE(fn, pump.Canceled());
	ASSERT_TRUE(fn, !pump.Failed());
	src.Write("late");
	src.Close();
	std::this_thread::sleep_for(std::chrono::milliseconds(50));
	ASSERT_TRUE(fn, pump.Canceled());
	ASSERT_TRUE(fn, !pump.Failed());
	ASSERT_TRUE(fn, !WaitFinish(pump, std::chrono::milliseconds(100)));
	RETURN_TEST(fn, result);
}

int test_pumper_cancel_telemetry_survives() {
	constexpr auto fn = "test_pumper_cancel_telemetry_survives";
	int result = 0;
	Producer src;
	FIFO out;
	Consumer in = src.Consumer();
	src.Write("abc");
	Pumper pump(Bridge(in, out), { Chunk{1} });
	ASSERT_TRUE(fn, WaitDelivered(pump, ByteSize{1}, std::chrono::seconds(2)));
	const auto kept = pump.ReadTelemetry();
	pump.Cancel();
	ASSERT_TRUE(fn, static_cast<bool>(kept));
	ASSERT_TRUE(fn, kept == pump.ReadTelemetry());
	ASSERT_TRUE(fn, kept->Delivered() >= ByteSize{1});
	src.Close();
	RETURN_TEST(fn, result);
}

// -------------------
// Failed
// -------------------

int test_pumper_failed_is_not_canceled() {
	constexpr auto fn = "test_pumper_failed_is_not_canceled";
	int result = 0;
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
	ASSERT_TRUE(fn, pump.Failed());
	ASSERT_TRUE(fn, !pump.Canceled());
	RETURN_TEST(fn, result);
}

// -------------------
// Finish
// -------------------

int test_pumper_auto_chunk_copies_all() {
	constexpr auto fn = "test_pumper_auto_chunk_copies_all";
	int result = 0;
	FIFO in;
	FIFO out;
	const std::string payload(64 * 1024, 'A');
	in.Write(payload);
	in.Close();
	Pumper pump(Bridge(in, out));
	ASSERT_TRUE(fn, WaitFinish(pump, std::chrono::seconds(5)));
	BinaryData got;
	ASSERT_TRUE(fn, out.Extract(0, got));
	ASSERT_EQUAL(fn, payload.size(), got.size());
	ASSERT_TRUE(fn, std::string(reinterpret_cast<const char*>(got.data()), got.size()) == payload);
	RETURN_TEST(fn, result);
}

int test_pumper_chunked_copies_all() {
	constexpr auto fn = "test_pumper_chunked_copies_all";
	int result = 0;
	FIFO in;
	FIFO out;
	in.Write("0123456789abcdef");
	in.Close();
	Pumper pump(Bridge(in, out), { Chunk{3} });
	ASSERT_TRUE(fn, WaitFinish(pump, std::chrono::seconds(2)));
	BinaryData got;
	ASSERT_TRUE(fn, out.Extract(0, got));
	ASSERT_EQUAL(fn, ByteSize{16}, ByteSize{got.size()});
	const auto read = pump.ReadTelemetry();
	const auto write = pump.WriteTelemetry();
	ASSERT_TRUE(fn, static_cast<bool>(read));
	ASSERT_TRUE(fn, static_cast<bool>(write));
	ASSERT_EQUAL(fn, ByteSize{16}, read->Delivered());
	ASSERT_EQUAL(fn, ByteSize{16}, write->Accepted());
	RETURN_TEST(fn, result);
}

int test_pumper_dtor_finishes_closed_source() {
	constexpr auto fn = "test_pumper_dtor_finishes_closed_source";
	int result = 0;
	FIFO in;
	FIFO out;
	in.Write("done-by-dtor");
	in.Close();
	{
		Pumper pump(Bridge(in, out), { Chunk{4} });
		(void)pump;
	}
	BinaryData got;
	ASSERT_TRUE(fn, out.Extract(0, got));
	ASSERT_EQUAL(fn, std::string("done-by-dtor"),
		std::string(reinterpret_cast<const char*>(got.data()), got.size()));
	RETURN_TEST(fn, result);
}

int test_pumper_empty_closed_source_is_eof() {
	constexpr auto fn = "test_pumper_empty_closed_source_is_eof";
	int result = 0;
	FIFO in;
	FIFO out;
	in.Close();
	Pumper pump(Bridge(in, out), { Chunk{32} });
	ASSERT_TRUE(fn, WaitFinish(pump, std::chrono::seconds(2)));
	ASSERT_TRUE(fn, pump.EoF());
	ASSERT_TRUE(fn, !pump.Failed());
	ASSERT_TRUE(fn, !pump.Canceled());
	const auto read = pump.ReadTelemetry();
	ASSERT_TRUE(fn, static_cast<bool>(read));
	ASSERT_EQUAL(fn, ByteSize{0}, read->Delivered());
	RETURN_TEST(fn, result);
}

int test_pumper_live_producer_then_close() {
	constexpr auto fn = "test_pumper_live_producer_then_close";
	int result = 0;
	Producer src;
	FIFO out;
	Consumer in = src.Consumer();
	Pumper pump(Bridge(in, out), { Chunk{5} });
	src.Write("hello");
	ASSERT_TRUE(fn, WaitDelivered(pump, ByteSize{5}, std::chrono::seconds(2)));
	src.Write(" world");
	src.Close();
	ASSERT_TRUE(fn, WaitFinish(pump, std::chrono::seconds(2)));
	BinaryData got;
	ASSERT_TRUE(fn, out.Extract(0, got));
	ASSERT_EQUAL(fn, std::string("hello world"),
		std::string(reinterpret_cast<const char*>(got.data()), got.size()));
	RETURN_TEST(fn, result);
}

// -------------------
// HighWater
// -------------------

int test_pumper_highwater_small_still_copies() {
	constexpr auto fn = "test_pumper_highwater_small_still_copies";
	int result = 0;
	FIFO in;
	FIFO out;
	in.Write("0123456789");
	in.Close();
	Pumper pump(Bridge(in, out), { Chunk{8}, HighWater{ByteSize{2}} });
	ASSERT_TRUE(fn, WaitFinish(pump, std::chrono::seconds(2)));
	BinaryData got;
	ASSERT_TRUE(fn, out.Extract(0, got));
	ASSERT_EQUAL(fn, ByteSize{10}, ByteSize{got.size()});
	RETURN_TEST(fn, result);
}

int test_pumper_highwater_zero_still_copies() {
	constexpr auto fn = "test_pumper_highwater_zero_still_copies";
	int result = 0;
	FIFO in;
	FIFO out;
	in.Write("0123456789");
	in.Close();
	Pumper pump(Bridge(in, out), { Chunk{3}, HighWater{ByteSize{0}} });
	ASSERT_TRUE(fn, WaitFinish(pump, std::chrono::seconds(2)));
	BinaryData got;
	ASSERT_TRUE(fn, out.Extract(0, got));
	ASSERT_EQUAL(fn, ByteSize{10}, ByteSize{got.size()});
	RETURN_TEST(fn, result);
}

// -------------------
// IO
// -------------------

int test_pumper_io_to_io_then_remove() {
	constexpr auto fn = "test_pumper_io_to_io_then_remove";
	int result = 0;
	const auto in_path = std::filesystem::temp_directory_path() / "sbb_pump_in.tmp";
	const auto out_path = std::filesystem::temp_directory_path() / "sbb_pump_out.tmp";
	std::filesystem::remove(in_path);
	std::filesystem::remove(out_path);
	DumpText(in_path, "FILEPUMP");
	{
		BufferedFileReader in(Loc(in_path), { ReadAhead{ByteSize{0}}, MaxMemory{ByteSize{0}} });
		BufferedFileWriter out(Loc(out_path), { WriteChunk{ByteSize{0}}, MaxMemory{ByteSize{0}} });
		ASSERT_TRUE(fn, in.Open());
		ASSERT_TRUE(fn, out.Open());
		Pumper pump(Bridge(std::move(in), std::move(out)), { Chunk{4} });
		ASSERT_TRUE(fn, WaitFinish(pump, std::chrono::seconds(2)));
		pump.Cancel();
	}
	ASSERT_EQUAL(fn, std::string("FILEPUMP"), Slurp(out_path));
	ASSERT_TRUE(fn, std::filesystem::remove(in_path));
	ASSERT_TRUE(fn, std::filesystem::remove(out_path));
	RETURN_TEST(fn, result);
}

// -------------------
// Move
// -------------------

int test_pumper_move_leaves_source_failed() {
	constexpr auto fn = "test_pumper_move_leaves_source_failed";
	int result = 0;
	FIFO in;
	FIFO out;
	in.Write("xyz");
	in.Close();
	Pumper pump(Bridge(in, out), { Chunk{1} });
	Pumper taken(std::move(pump));
	ASSERT_TRUE(fn, !pump.Failed());
	ASSERT_TRUE(fn, !pump.Canceled());
	ASSERT_TRUE(fn, pump.EoF());
	ASSERT_TRUE(fn, WaitFinish(taken, std::chrono::seconds(2)));
	BinaryData got;
	ASSERT_TRUE(fn, out.Extract(0, got));
	ASSERT_EQUAL(fn, ByteSize{3}, ByteSize{got.size()});
	RETURN_TEST(fn, result);
}

// -------------------
// Toggle
// -------------------

int test_pumper_toggle_pauses_and_resumes() {
	constexpr auto fn = "test_pumper_toggle_pauses_and_resumes";
	int result = 0;
	Producer src;
	FIFO out;
	Consumer in = src.Consumer();
	Pumper pump(Bridge(in, out), { Chunk{1} });
	src.Write("ABCD");
	ASSERT_TRUE(fn, WaitDelivered(pump, ByteSize{1}, std::chrono::seconds(2)));
	pump.Toggle();
	const auto paused = pump.ReadTelemetry();
	ASSERT_TRUE(fn, static_cast<bool>(paused));
	const ByteSize at_pause = paused->Delivered();
	src.Write("EFGH");
	std::this_thread::sleep_for(std::chrono::milliseconds(80));
	const auto still = pump.ReadTelemetry();
	ASSERT_TRUE(fn, static_cast<bool>(still));
	ASSERT_EQUAL(fn, at_pause, still->Delivered());
	pump.Toggle();
	src.Close();
	ASSERT_TRUE(fn, WaitFinish(pump, std::chrono::seconds(2)));
	BinaryData got;
	ASSERT_TRUE(fn, out.Extract(0, got));
	ASSERT_EQUAL(fn, std::string("ABCDEFGH"),
		std::string(reinterpret_cast<const char*>(got.data()), got.size()));
	RETURN_TEST(fn, result);
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
