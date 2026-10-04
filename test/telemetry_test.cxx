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
#include <StormByte/buffer/fifo.hxx>
#include <StormByte/buffer/io/buffered_file_reader.hxx>
#include <StormByte/buffer/io/telemetry.hxx>
#include <StormByte/buffer/telemetry.hxx>
#include <StormByte/byte_size.hxx>
#include <StormByte/safe/string.hxx>
#include <StormByte/telemetry.hxx>
#include <StormByte/test_handlers.h>

#include <filesystem>
#include <fstream>
#include <iostream>
#include <string>
#include <string_view>

using StormByte::ByteSize;
using StormByte::Buffer::Bridge;
using StormByte::Buffer::FIFO;
using StormByte::Buffer::IO::BufferedFileReader;
using StormByte::Buffer::IO::MaxMemory;
using StormByte::Buffer::IO::ReadAhead;

static_assert(StormByte::Type::MaybeSafe<StormByte::Buffer::Telemetry::OperationSample>);
static_assert(!StormByte::Type::IsSafe<StormByte::Buffer::Telemetry::OperationSample>::value);

namespace {
	class SampleTelemetry final: public StormByte::Telemetry {
		public:
			operator StormByte::Safe::String() const override {
				return StormByte::Safe::String(std::string_view{});
			}

			std::uint64_t OperationCount() const {
				return Clock("Buffer.Operation").Count();
			}
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
}

// -------------------
// Close
// -------------------

int test_telemetry_survives_bridge_close() {
	constexpr auto fn = "test_telemetry_survives_bridge_close";
	int result = 0;
	FIFO in;
	FIFO out;
	in.Write("payload");
	in.Close();
	Bridge bridge(in, out);
	ASSERT_EQUAL(fn, ByteSize{7}, bridge.Passthrough(7));
	const auto read = bridge.ReadTelemetry();
	const auto write = bridge.WriteTelemetry();
	bridge.Close();
	ASSERT_TRUE(fn, static_cast<bool>(read));
	ASSERT_TRUE(fn, static_cast<bool>(write));
	ASSERT_EQUAL(fn, ByteSize{7}, read->Delivered());
	ASSERT_EQUAL(fn, ByteSize{7}, write->Accepted());
	RETURN_TEST(fn, result);
}

int test_telemetry_survives_bridge_dtor() {
	constexpr auto fn = "test_telemetry_survives_bridge_dtor";
	int result = 0;
	StormByte::Safe::Shared<StormByte::Buffer::ReadTelemetry> kept;
	{
		FIFO in;
		FIFO out;
		in.Write("xyz");
		in.Close();
		Bridge bridge(in, out);
		ASSERT_EQUAL(fn, ByteSize{3}, bridge.Passthrough(3));
		kept = bridge.ReadTelemetry();
	}
	ASSERT_TRUE(fn, static_cast<bool>(kept));
	ASSERT_EQUAL(fn, ByteSize{3}, kept->Delivered());
	RETURN_TEST(fn, result);
}

// -------------------
// Flatten
// -------------------

int test_telemetry_flatten_after_transfer() {
	constexpr auto fn = "test_telemetry_flatten_after_transfer";
	int result = 0;
	FIFO in;
	FIFO out;
	in.Write("payload");
	in.Close();
	Bridge bridge(in, out);
	ASSERT_EQUAL(fn, ByteSize{7}, bridge.Passthrough(7));
	const auto read = bridge.ReadTelemetry();
	const auto write = bridge.WriteTelemetry();
	ASSERT_TRUE(fn, static_cast<bool>(read));
	ASSERT_TRUE(fn, static_cast<bool>(write));
	ASSERT_TRUE(fn, !static_cast<std::string>(*read).empty());
	ASSERT_TRUE(fn, !static_cast<std::string>(*write).empty());
	RETURN_TEST(fn, result);
}

// -------------------
// Handle
// -------------------

int test_telemetry_same_handle() {
	constexpr auto fn = "test_telemetry_same_handle";
	int result = 0;
	FIFO in;
	FIFO out;
	in.Write("payload");
	in.Close();
	Bridge bridge(in, out);
	const auto first = bridge.ReadTelemetry();
	ASSERT_EQUAL(fn, ByteSize{7}, bridge.Passthrough(7));
	const auto second = bridge.ReadTelemetry();
	ASSERT_TRUE(fn, first == second);
	ASSERT_EQUAL(fn, ByteSize{7}, first->Delivered());
	ASSERT_EQUAL(fn, ByteSize{7}, second->Delivered());
	RETURN_TEST(fn, result);
}

// -------------------
// IO
// -------------------

int test_telemetry_io_leaf_same_pointer() {
	constexpr auto fn = "test_telemetry_io_leaf_same_pointer";
	int result = 0;
	const auto path = std::filesystem::temp_directory_path() / "sbb_tel_io.tmp";
	std::filesystem::remove(path);
	DumpText(path, "ABCDEFGH");
	BufferedFileReader in(Loc(path), { ReadAhead{ByteSize{0}}, MaxMemory{ByteSize{0}} });
	ASSERT_TRUE(fn, in.Open());
	const auto leaf = in.Telemetry();
	FIFO out;
	Bridge bridge(std::move(in), out);
	ASSERT_TRUE(fn, bridge.InputIsIO());
	const auto bridged = bridge.ReadTelemetry();
	ASSERT_TRUE(fn, static_cast<bool>(leaf));
	ASSERT_TRUE(fn, static_cast<bool>(bridged));
	ASSERT_TRUE(fn, leaf == bridged);
	ASSERT_EQUAL(fn, ByteSize{8}, bridge.Passthrough(8));
	ASSERT_EQUAL(fn, ByteSize{8}, bridged->Delivered());
	ASSERT_EQUAL(fn, ByteSize{8}, leaf->Delivered());
	bridge.Close();
	std::filesystem::remove(path);
	RETURN_TEST(fn, result);
}

int test_telemetry_io_not_double_counted() {
	constexpr auto fn = "test_telemetry_io_not_double_counted";
	int result = 0;
	const auto path = std::filesystem::temp_directory_path() / "sbb_tel_once.tmp";
	std::filesystem::remove(path);
	DumpText(path, "1234");
	BufferedFileReader in(Loc(path), { ReadAhead{ByteSize{0}}, MaxMemory{ByteSize{0}} });
	ASSERT_TRUE(fn, in.Open());
	FIFO out;
	Bridge bridge(std::move(in), out);
	ASSERT_EQUAL(fn, ByteSize{4}, bridge.Passthrough(4));
	const auto read = bridge.ReadTelemetry();
	ASSERT_TRUE(fn, static_cast<bool>(read));
	ASSERT_EQUAL(fn, ByteSize{4}, read->Delivered());
	bridge.Close();
	std::filesystem::remove(path);
	RETURN_TEST(fn, result);
}

// -------------------
// Start
// -------------------

int test_telemetry_empty_before_passthrough() {
	constexpr auto fn = "test_telemetry_empty_before_passthrough";
	int result = 0;
	FIFO in;
	FIFO out;
	in.Write("payload");
	in.Close();
	Bridge bridge(in, out);
	const auto read = bridge.ReadTelemetry();
	const auto write = bridge.WriteTelemetry();
	ASSERT_TRUE(fn, static_cast<bool>(read));
	ASSERT_TRUE(fn, static_cast<bool>(write));
	ASSERT_EQUAL(fn, ByteSize{0}, read->Delivered());
	ASSERT_EQUAL(fn, ByteSize{0}, write->Accepted());
	RETURN_TEST(fn, result);
}

// -------------------
// Track
// -------------------

int test_telemetry_tracks_delivered_and_accepted() {
	constexpr auto fn = "test_telemetry_tracks_delivered_and_accepted";
	int result = 0;
	FIFO in;
	FIFO out;
	in.Write("payload");
	in.Close();
	Bridge bridge(in, out);
	ASSERT_EQUAL(fn, ByteSize{7}, bridge.Passthrough(7));
	const auto read = bridge.ReadTelemetry();
	const auto write = bridge.WriteTelemetry();
	ASSERT_TRUE(fn, static_cast<bool>(read));
	ASSERT_TRUE(fn, static_cast<bool>(write));
	ASSERT_EQUAL(fn, ByteSize{7}, read->Delivered());
	ASSERT_EQUAL(fn, ByteSize{7}, write->Accepted());
	RETURN_TEST(fn, result);
}

int test_telemetry_uses_base_clock() {
	constexpr auto fn = "test_telemetry_uses_base_clock";
	int result = 0;
	const std::string payload(1024 * 1024, 'x');
	FIFO in;
	FIFO out;
	in.Write(std::string_view(payload));
	in.Close();
	Bridge bridge(in, out);
	ASSERT_EQUAL(fn, ByteSize{payload.size()}, bridge.Passthrough(ByteSize{payload.size()}));
	const auto read = bridge.ReadTelemetry();
	ASSERT_TRUE(fn, static_cast<bool>(read));
	const StormByte::Telemetry& base = *read;
	ASSERT_TRUE(fn, !static_cast<std::string>(base).empty());
	ASSERT_TRUE(fn, read->MeanRate() > ByteSize{0});
	RETURN_TEST(fn, result);
}

int test_telemetry_records_independent_samples() {
	constexpr auto fn = "test_telemetry_records_independent_samples";
	int result = 0;
	SampleTelemetry telemetry;
	{
		auto outer = telemetry.MeasureOperation();
		auto nested = telemetry.MeasureOperation();
		nested.Commit(ByteSize{2});
		outer.Commit(ByteSize{1});
	}
	auto transferred = telemetry.MeasureOperation();
	std::thread worker([sample = std::move(transferred)]() mutable {
		sample.Commit(ByteSize{3});
	});
	worker.join();
	ASSERT_EQUAL(fn, std::uint64_t{3}, telemetry.OperationCount());
	RETURN_TEST(fn, result);
}

int main() {
	int result = 0;

	// -------------------
	// Close
	// -------------------
	result += test_telemetry_survives_bridge_close();
	result += test_telemetry_survives_bridge_dtor();

	// -------------------
	// Flatten
	// -------------------
	result += test_telemetry_flatten_after_transfer();

	// -------------------
	// Handle
	// -------------------
	result += test_telemetry_same_handle();

	// -------------------
	// IO
	// -------------------
	result += test_telemetry_io_leaf_same_pointer();
	result += test_telemetry_io_not_double_counted();

	// -------------------
	// Start
	// -------------------
	result += test_telemetry_empty_before_passthrough();

	// -------------------
	// Track
	// -------------------
	result += test_telemetry_tracks_delivered_and_accepted();
	result += test_telemetry_uses_base_clock();
	result += test_telemetry_records_independent_samples();

	if (result == 0)
		std::cout << "All tests passed!" << std::endl;
	else
		std::cout << result << " tests failed." << std::endl;
	return result;
}
