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

#include <StormByte/buffer/fifo.hxx>
#include <StormByte/buffer/io/buffered_file_reader.hxx>
#include <StormByte/buffer/io/telemetry.hxx>
#include <StormByte/safe/binary.hxx>
#include <StormByte/safe/wstring.hxx>
#include <StormByte/test_handlers.h>

#include <array>
#include <chrono>
#include <cstddef>
#include <filesystem>
#include <fstream>
#include <iostream>
#include <span>
#include <string>
#include <string_view>
#include <thread>
#include <vector>

using StormByte::Buffer::FIFO;
using StormByte::Buffer::Position;
using StormByte::Buffer::IO::BufferedFileReader;
using StormByte::Buffer::IO::MaxMemory;
using StormByte::Buffer::IO::ReadAhead;
using StormByte::Buffer::IO::State;
using StormByte::Buffer::IO::Status;
using StormByte::Buffer::IO::ToString;
using StormByte::Safe::Binary;

namespace {
	constexpr char kHex[] = "0123456789abcdef";
	constexpr std::size_t kHexLen = 16;
	constexpr std::size_t kHexFile = 4 * 1024 * 1024;

	StormByte::Safe::String Loc(const std::filesystem::path& path) {
#ifdef WINDOWS
		return StormByte::Safe::String(StormByte::Safe::WString(std::wstring_view(path.wstring())));
#else
		return StormByte::Safe::String(std::string_view(path.string()));
#endif
	}

	std::filesystem::path File(const char* name) {
		return CurrentFileDirectory / "files" / name;
	}

	BufferedFileReader::Parameters P(const std::size_t read_ahead, const std::size_t max_memory) {
		return { ReadAhead(StormByte::ByteSize{read_ahead}), MaxMemory(StormByte::ByteSize{max_memory}) };
	}

	std::string Text(FIFO& fifo) {
		Binary data;
		static_cast<void>(fifo.Peek(0, data));
		return std::string(reinterpret_cast<const char*>(data.data()),
			static_cast<std::size_t>(data.size()));
	}

	Binary Bytes(FIFO& fifo) {
		Binary data;
		static_cast<void>(fifo.Peek(0, data));
		return data;
	}

	void Fill(std::span<std::byte> dest, const std::byte value) {
		for (auto& b : dest)
			b = value;
	}

	unsigned char HexAt(const std::size_t offset) {
		return static_cast<unsigned char>(kHex[offset % kHexLen]);
	}

	std::filesystem::path HexPath() {
		return std::filesystem::temp_directory_path() / "stormbyte_buffer_hex_4m.bin";
	}

	bool WriteHexFile(const std::filesystem::path& path, const std::size_t size = kHexFile) {
		std::ofstream out(path, std::ios::binary | std::ios::trunc);
		if (!out)
			return false;
		std::vector<char> block(64 * 1024);
		std::size_t off = 0;
		while (off < size) {
			const std::size_t n = (size - off) < block.size() ? (size - off) : block.size();
			for (std::size_t i = 0; i < n; ++i)
				block[i] = static_cast<char>(HexAt(off + i));
			out.write(block.data(), static_cast<std::streamsize>(n));
			off += n;
		}
		return static_cast<bool>(out);
	}

	int CheckTell(const BufferedFileReader& in, const std::size_t expect) {
		ASSERT_EQUAL(StormByte::ByteSize{expect}, in.Tell());
		return 0;
	}

	int ReadExpect(BufferedFileReader& in, const std::size_t from, const std::size_t n) {
		if (CheckTell(in, from) != 0)
			return 1;
		FIFO dest;
		const auto got = in.Read(StormByte::ByteSize{n}, dest);
		if (got.status != Status::Ok && got.status != Status::End)
			return 1;
		ASSERT_EQUAL(StormByte::ByteSize{n}, got.count);
		if (CheckTell(in, from + n) != 0)
			return 1;
		const auto bytes = Bytes(dest);
		ASSERT_EQUAL(n, static_cast<std::size_t>(bytes.size()));
		for (std::size_t i = 0; i < n; ++i)
			ASSERT_EQUAL(static_cast<unsigned>(HexAt(from + i)),
				static_cast<unsigned>(bytes[i]));
		return 0;
	}

	int SeekExpectTell(BufferedFileReader& in, const std::size_t target) {
		ASSERT_EQUAL(ToString(Status::Ok),
			ToString(in.Seek(static_cast<std::ptrdiff_t>(target), Position::Absolute).status));
		return CheckTell(in, target);
	}

	void DumpTelemetry(const char* tag, const BufferedFileReader& in) {
		const auto tel = in.Telemetry();
		if (!tel) {
			std::cout << "[telemetry " << tag << "] empty" << std::endl;
			return;
		}
		std::cout << "[telemetry " << tag << "] " << static_cast<std::string>(*tel) << std::endl;
	}

	const StormByte::Buffer::IO::ReadTelemetry* IoTel(const BufferedFileReader& in) {
		const auto tel = in.Telemetry();
		if (!tel)
			return nullptr;
		return dynamic_cast<const StormByte::Buffer::IO::ReadTelemetry*>(tel.get());
	}

	/** Read sequentially in @p chunk steps until End, checking every byte. Returns bytes read or -1. */
	long long DrainHex(BufferedFileReader& in, const std::size_t chunk) {
		std::size_t total = static_cast<std::size_t>(in.Tell());
		const std::size_t first = total;
		for (;;) {
			FIFO dest;
			const auto got = in.Read(StormByte::ByteSize{chunk}, dest);
			if (got.status != Status::Ok && got.status != Status::End)
				return -1;
			const auto bytes = Bytes(dest);
			if (static_cast<std::size_t>(bytes.size()) != static_cast<std::size_t>(got.count))
				return -1;
			for (std::size_t i = 0; i < static_cast<std::size_t>(bytes.size()); ++i) {
				if (static_cast<unsigned char>(bytes[i]) != HexAt(total + i))
					return -1;
			}
			total += static_cast<std::size_t>(got.count);
			if (in.Tell() != StormByte::ByteSize{total})
				return -1;
			if (got.status == Status::End || got.count < StormByte::ByteSize{chunk})
				break;
		}
		return static_cast<long long>(total - first);
	}
}

// -------------------
// Fixtures
// -------------------

int test_lines_are_octets() {
	BufferedFileReader in(Loc(File("lines.txt")));
	ASSERT_TRUE(in.Open());
	FIFO dest;
	const auto read = in.Read(18, dest);
	ASSERT_EQUAL(ToString(Status::Ok), ToString(read.status));
	ASSERT_EQUAL(std::string("line1\nline2\nline3\n"), Text(dest));
	ASSERT_EQUAL(StormByte::ByteSize{18}, in.Tell());
	RETURN_TEST(0);
}

int test_no_nl_and_nul() {
	BufferedFileReader text(Loc(File("no_nl.txt")));
	ASSERT_TRUE(text.Open());
	FIFO t;
	ASSERT_EQUAL(ToString(Status::Ok), ToString(text.Read(17, t).status));
	ASSERT_EQUAL(std::string("no newline at end"), Text(t));
	ASSERT_EQUAL(StormByte::ByteSize{17}, text.Tell());
	BufferedFileReader raw(Loc(File("with_nuls.bin")));
	ASSERT_TRUE(raw.Open());
	FIFO n;
	ASSERT_EQUAL(ToString(Status::Ok), ToString(raw.Read(5, n).status));
	const auto bytes = Bytes(n);
	ASSERT_EQUAL(static_cast<std::size_t>(5), static_cast<std::size_t>(bytes.size()));
	ASSERT_EQUAL(std::byte{'A'}, bytes[0]);
	ASSERT_EQUAL(std::byte{0}, bytes[1]);
	RETURN_TEST(0);
}

int test_pattern_256() {
	BufferedFileReader in(Loc(File("pattern_256.bin")));
	ASSERT_TRUE(in.Open());
	FIFO dest;
	const auto read = in.Read(256, dest);
	ASSERT_EQUAL(ToString(Status::Ok), ToString(read.status));
	const auto bytes = Bytes(dest);
	ASSERT_EQUAL(static_cast<std::size_t>(256), static_cast<std::size_t>(bytes.size()));
	for (std::size_t i = 0; i < 256; ++i)
		ASSERT_EQUAL(static_cast<std::byte>(i), bytes[i]);
	ASSERT_EQUAL(StormByte::ByteSize{256}, in.Tell());
	RETURN_TEST(0);
}

// -------------------
// Hex
// -------------------

int test_hex_fake_seek_in_page() {
	const auto path = HexPath();
	ASSERT_TRUE(WriteHexFile(path));
	BufferedFileReader in(Loc(path), P(32 * 1024, 256 * 1024));
	ASSERT_TRUE(in.Open());
	if (ReadExpect(in, 0, 64 * 1024) != 0)
		return 1;
	DumpTelemetry("hex-fake-after-fill", in);
	if (SeekExpectTell(in, 8 * 1024) != 0)
		return 1;
	DumpTelemetry("hex-fake-seek-8k", in);
	if (ReadExpect(in, 8 * 1024, 16) != 0)
		return 1;
	DumpTelemetry("hex-fake-read-16", in);
	if (SeekExpectTell(in, 16 * 1024) != 0)
		return 1;
	if (ReadExpect(in, 16 * 1024, 32) != 0)
		return 1;
	if (SeekExpectTell(in, 0) != 0)
		return 1;
	if (ReadExpect(in, 0, 64) != 0)
		return 1;
	DumpTelemetry("hex-fake-seek0-done", in);
	RETURN_TEST(0);
}

int test_hex_fake_seek_peek_does_not_move_tell() {
	const auto path = HexPath();
	ASSERT_TRUE(WriteHexFile(path));
	BufferedFileReader in(Loc(path), P(16 * 1024, 64 * 1024));
	ASSERT_TRUE(in.Open());
	if (ReadExpect(in, 0, 32 * 1024) != 0)
		return 1;
	if (SeekExpectTell(in, 1024) != 0)
		return 1;
	FIFO peek;
	ASSERT_EQUAL(ToString(Status::Ok), ToString(in.Peek(8, peek).status));
	if (CheckTell(in, 1024) != 0)
		return 1;
	const auto bytes = Bytes(peek);
	ASSERT_EQUAL(static_cast<std::size_t>(8), static_cast<std::size_t>(bytes.size()));
	for (std::size_t i = 0; i < 8; ++i)
		ASSERT_EQUAL(static_cast<unsigned>(HexAt(1024 + i)),
			static_cast<unsigned>(bytes[i]));
	if (ReadExpect(in, 1024, 8) != 0)
		return 1;
	DumpTelemetry("peek-then-read-fake-seek", in);
	RETURN_TEST(0);
}

int test_hex_fake_seek_small_reads_then_catchup() {
	const auto path = HexPath();
	ASSERT_TRUE(WriteHexFile(path));
	BufferedFileReader in(Loc(path), P(16 * 1024, 256 * 1024));
	ASSERT_TRUE(in.Open());
	if (ReadExpect(in, 0, 48 * 1024) != 0)
		return 1;
	if (SeekExpectTell(in, 40 * 1024) != 0)
		return 1;
	DumpTelemetry("hex-catchup-seek-40k", in);
	if (ReadExpect(in, 40 * 1024, 1024) != 0)
		return 1;
	if (ReadExpect(in, 41 * 1024, 1024) != 0)
		return 1;
	if (ReadExpect(in, 42 * 1024, 6 * 1024) != 0)
		return 1;
	if (ReadExpect(in, 48 * 1024, 1024) != 0)
		return 1;
	DumpTelemetry("hex-catchup-past-old-tell", in);
	RETURN_TEST(0);
}

int test_hex_fake_seek_tell_before_read() {
	const auto path = HexPath();
	ASSERT_TRUE(WriteHexFile(path));
	BufferedFileReader in(Loc(path), P(32 * 1024, 256 * 1024));
	ASSERT_TRUE(in.Open());
	if (ReadExpect(in, 0, 64 * 1024) != 0)
		return 1;
	DumpTelemetry("after-fill-64k", in);
	if (SeekExpectTell(in, 8 * 1024) != 0)
		return 1;
	DumpTelemetry("fake-seek-8k-before-read", in);
	if (ReadExpect(in, 8 * 1024, 16) != 0)
		return 1;
	DumpTelemetry("fake-seek-8k-after-read16", in);
	if (SeekExpectTell(in, 16 * 1024) != 0)
		return 1;
	if (ReadExpect(in, 16 * 1024, 1) != 0)
		return 1;
	if (ReadExpect(in, 16 * 1024 + 1, 15) != 0)
		return 1;
	if (ReadExpect(in, 16 * 1024 + 16, 32) != 0)
		return 1;
	DumpTelemetry("fake-seek-16k-three-small-reads", in);
	if (SeekExpectTell(in, 0) != 0)
		return 1;
	if (ReadExpect(in, 0, 32) != 0)
		return 1;
	DumpTelemetry("fake-seek-0-after-read32", in);
	RETURN_TEST(0);
}

int test_hex_relative_fake_seek() {
	const auto path = HexPath();
	ASSERT_TRUE(WriteHexFile(path));
	BufferedFileReader in(Loc(path), P(16 * 1024, 64 * 1024));
	ASSERT_TRUE(in.Open());
	if (ReadExpect(in, 0, 20 * 1024) != 0)
		return 1;
	ASSERT_EQUAL(ToString(Status::Ok),
		ToString(in.Seek(-4 * 1024, Position::Relative).status));
	if (CheckTell(in, 16 * 1024) != 0)
		return 1;
	if (ReadExpect(in, 16 * 1024, 64) != 0)
		return 1;
	DumpTelemetry("relative-fake-back", in);
	RETURN_TEST(0);
}

int test_hex_seek_cold_tell_then_bytes() {
	const auto path = HexPath();
	ASSERT_TRUE(WriteHexFile(path));
	BufferedFileReader in(Loc(path), P(8 * 1024, 32 * 1024));
	ASSERT_TRUE(in.Open());
	if (ReadExpect(in, 0, 4096) != 0)
		return 1;
	if (SeekExpectTell(in, 2 * 1024 * 1024) != 0)
		return 1;
	DumpTelemetry("cold-2m-before-read", in);
	if (ReadExpect(in, 2 * 1024 * 1024, 64) != 0)
		return 1;
	if (SeekExpectTell(in, 3 * 1024 * 1024 + 7) != 0)
		return 1;
	if (ReadExpect(in, 3 * 1024 * 1024 + 7, 33) != 0)
		return 1;
	DumpTelemetry("cold-3m-after-read", in);
	RETURN_TEST(0);
}

int test_hex_seek_miss_cold() {
	const auto path = HexPath();
	ASSERT_TRUE(WriteHexFile(path));
	BufferedFileReader in(Loc(path), P(8 * 1024, 32 * 1024));
	ASSERT_TRUE(in.Open());
	if (ReadExpect(in, 0, 4096) != 0)
		return 1;
	if (SeekExpectTell(in, 2 * 1024 * 1024) != 0)
		return 1;
	DumpTelemetry("hex-cold-seek-2m", in);
	if (ReadExpect(in, 2 * 1024 * 1024, 64) != 0)
		return 1;
	if (SeekExpectTell(in, 3 * 1024 * 1024 + 7) != 0)
		return 1;
	if (ReadExpect(in, 3 * 1024 * 1024 + 7, 33) != 0)
		return 1;
	DumpTelemetry("hex-cold-done", in);
	RETURN_TEST(0);
}

int test_hex_seek_partial_hole() {
	const auto path = HexPath();
	ASSERT_TRUE(WriteHexFile(path));
	BufferedFileReader in(Loc(path), P(4 * 1024, 16 * 1024));
	ASSERT_TRUE(in.Open());
	if (ReadExpect(in, 0, 8192) != 0)
		return 1;
	if (SeekExpectTell(in, 4096) != 0)
		return 1;
	DumpTelemetry("hex-partial-seek-4k", in);
	if (ReadExpect(in, 4096, 32 * 1024) != 0)
		return 1;
	DumpTelemetry("hex-partial-after-hole", in);
	RETURN_TEST(0);
}

int test_hex_seek_partial_hole_tell() {
	const auto path = HexPath();
	ASSERT_TRUE(WriteHexFile(path));
	BufferedFileReader in(Loc(path), P(4 * 1024, 16 * 1024));
	ASSERT_TRUE(in.Open());
	if (ReadExpect(in, 0, 8192) != 0)
		return 1;
	if (SeekExpectTell(in, 4096) != 0)
		return 1;
	DumpTelemetry("partial-before-overread", in);
	if (ReadExpect(in, 4096, 32 * 1024) != 0)
		return 1;
	DumpTelemetry("partial-after-overread", in);
	RETURN_TEST(0);
}

int test_hex_seek_saved_partial_disjoint_spans() {
	const auto path = HexPath();
	ASSERT_TRUE(WriteHexFile(path));
	BufferedFileReader in(Loc(path), P(4 * 1024, 256 * 1024));
	ASSERT_TRUE(in.Open());
	if (ReadExpect(in, 0, 8192) != 0)
		return 1;
	if (SeekExpectTell(in, 1024 * 1024) != 0)
		return 1;
	if (ReadExpect(in, 1024 * 1024, 8192) != 0)
		return 1;
	if (SeekExpectTell(in, 1024) != 0)
		return 1;
	DumpTelemetry("partial-island-before-overread", in);
	if (ReadExpect(in, 1024, 32 * 1024) != 0)
		return 1;
	DumpTelemetry("partial-island-after-overread", in);
	ASSERT_EQUAL(ToString(Status::Ok),
		ToString(in.Seek(0, Position::Absolute).status));
	DumpTelemetry("partial-island-epoch-closed", in);
	const auto tel = in.Telemetry();
	ASSERT_TRUE(static_cast<bool>(tel));
	const auto* io = IoTel(in);
	ASSERT_TRUE(io != nullptr);
	ASSERT_EQUAL(tel->Delivered(), io->HitAhead() + io->HitBack() + io->Miss());
	ASSERT_TRUE(io->SeekOrigin() > 0);
	ASSERT_TRUE(io->SeekSavedPartial() > 0);
	RETURN_TEST(0);
}

int test_hex_seek_stress_fixed_offsets() {
	const auto path = HexPath();
	ASSERT_TRUE(WriteHexFile(path));
	BufferedFileReader in(Loc(path), P(16 * 1024, 128 * 1024));
	ASSERT_TRUE(in.Open());
	const std::size_t offs[] = {
		0, 1, 15, 16, 17,
		4095, 4096, 4097,
		64 * 1024 - 3, 64 * 1024, 64 * 1024 + 3,
		100000, 1234567, 2000000, 3000001,
		kHexFile - 32, kHexFile - 16
	};
	const std::size_t lens[] = { 1, 2, 16, 17, 256, 4096 };
	for (const std::size_t off : offs) {
		for (const std::size_t n : lens) {
			if (off + n > kHexFile)
				continue;
			if (SeekExpectTell(in, off) != 0)
				return 1;
			if (ReadExpect(in, off, n) != 0)
				return 1;
		}
	}
	DumpTelemetry("hex-stress-done", in);
	RETURN_TEST(0);
}

int test_hex_seq_matches_pattern() {
	const auto path = HexPath();
	ASSERT_TRUE(WriteHexFile(path));
	BufferedFileReader in(Loc(path), P(64 * 1024, 256 * 1024));
	ASSERT_TRUE(in.Open());
	if (CheckTell(in, 0) != 0)
		return 1;
	constexpr std::size_t chunk = 4096;
	std::size_t off = 0;
	while (off < kHexFile) {
		const std::size_t n = (kHexFile - off) < chunk ? (kHexFile - off) : chunk;
		if (ReadExpect(in, off, n) != 0)
			return 1;
		off += n;
	}
	RETURN_TEST(0);
}

// -------------------
// Move
// -------------------

int test_move_transfers_session() {
	BufferedFileReader in(Loc(File("five.bin")));
	ASSERT_TRUE(in.Open());
	FIFO first;
	ASSERT_EQUAL(ToString(Status::Ok), ToString(in.Read(2, first).status));
	ASSERT_EQUAL(StormByte::ByteSize{2}, in.Tell());
	BufferedFileReader moved(std::move(in));
	ASSERT_TRUE(moved.IsOpen());
	ASSERT_EQUAL(StormByte::ByteSize{2}, moved.Tell());
	FIFO rest;
	ASSERT_EQUAL(ToString(Status::Ok), ToString(moved.Read(3, rest).status));
	ASSERT_EQUAL(std::string("CDE"), Text(rest));
	ASSERT_EQUAL(StormByte::ByteSize{5}, moved.Tell());
	RETURN_TEST(0);
}

// -------------------
// Setup
// -------------------

int test_explicit_readahead_survives_setup() {
	BufferedFileReader in(Loc(File("ahead.bin")), P(25, 1024));
	ASSERT_TRUE(in.Open());
	ASSERT_EQUAL(StormByte::ByteSize{25}, in.ReadAhead());
	ASSERT_EQUAL(StormByte::ByteSize{1024}, in.MaxMemory());
	FIFO dest;
	ASSERT_EQUAL(ToString(Status::Ok), ToString(in.Read(5, dest).status));
	ASSERT_EQUAL(std::string("01234"), Text(dest));
	RETURN_TEST(0);
}

int test_explicit_zero_survives_open() {
	BufferedFileReader in(Loc(File("five.bin")), P(0, 0));
	ASSERT_TRUE(in.Open());
	ASSERT_EQUAL(StormByte::ByteSize{0}, in.ReadAhead());
	ASSERT_EQUAL(StormByte::ByteSize{0}, in.MaxMemory());
	FIFO dest;
	ASSERT_EQUAL(ToString(Status::Ok), ToString(in.Read(5, dest).status));
	ASSERT_EQUAL(std::string("ABCDE"), Text(dest));
	RETURN_TEST(0);
}

int test_path_only_setup_sets_readahead() {
	BufferedFileReader in(Loc(File("five.bin")));
	ASSERT_TRUE(in.Open());
	ASSERT_TRUE(in.ReadAhead() > 0);
	FIFO dest;
	ASSERT_EQUAL(ToString(Status::Ok), ToString(in.Read(5, dest).status));
	ASSERT_EQUAL(std::string("ABCDE"), Text(dest));
	RETURN_TEST(0);
}

// -------------------
// Volume
// -------------------

int test_block_4k_chunked() {
	BufferedFileReader in(Loc(File("block_4k.bin")), P(512, 2048));
	ASSERT_TRUE(in.Open());
	StormByte::ByteSize total{0};
	while (!in.EoF()) {
		FIFO dest;
		const auto read = in.Read(1000, dest);
		if (read.status == Status::Failed || read.status == Status::Error)
			return 1;
		total += read.count;
		ASSERT_EQUAL(total, in.Tell());
		if (read.status == Status::End)
			break;
	}
	ASSERT_EQUAL(StormByte::ByteSize{4096}, total);
	RETURN_TEST(0);
}

int test_max_memory_zero_span_reads() {
	BufferedFileReader in(Loc(File("five.bin")), P(0, 0));
	ASSERT_TRUE(in.Open());
	std::array<std::byte, 5> raw {};
	const auto got = in.Read(std::span<std::byte>(raw));
	ASSERT_EQUAL(ToString(Status::Ok), ToString(got.status));
	ASSERT_EQUAL(std::string("ABCDE"),
		std::string(reinterpret_cast<const char*>(raw.data()), 5));
	ASSERT_EQUAL(StormByte::ByteSize{5}, in.Tell());
	RETURN_TEST(0);
}

int test_max_memory_zero_still_reads() {
	BufferedFileReader in(Loc(File("block_256.bin")), P(0, 0));
	ASSERT_TRUE(in.Open());
	FIFO dest;
	const auto read = in.Read(256, dest);
	ASSERT_EQUAL(ToString(Status::Ok), ToString(read.status));
	ASSERT_EQUAL(StormByte::ByteSize{256}, read.count);
	ASSERT_EQUAL(StormByte::ByteSize{256}, in.Tell());
	RETURN_TEST(0);
}

int test_readahead_knobs() {
	BufferedFileReader in(Loc(File("ahead.bin")), P(25, 1024));
	ASSERT_EQUAL(StormByte::ByteSize{25}, in.ReadAhead());
	ASSERT_EQUAL(StormByte::ByteSize{1024}, in.MaxMemory());
	RETURN_TEST(0);
}

int test_sequential_capped_window_slides() {
	const auto path = HexPath();
	ASSERT_TRUE(WriteHexFile(path));
	constexpr std::size_t cap = 64 * 1024;
	BufferedFileReader in(Loc(path), P(16 * 1024, cap));
	ASSERT_TRUE(in.Open());
	ASSERT_EQUAL(static_cast<long long>(kHexFile), DrainHex(in, 3000));
	const auto* io = IoTel(in);
	ASSERT_TRUE(io != nullptr);
	ASSERT_EQUAL(StormByte::ByteSize{kHexFile}, io->Delivered());
	ASSERT_EQUAL(io->Delivered(), io->HitAhead() + io->HitBack() + io->Miss());
	ASSERT_EQUAL(StormByte::ByteSize{kHexFile}, io->Origin());
	ASSERT_TRUE(io->CachedPeak() <= StormByte::ByteSize{cap});
	ASSERT_TRUE(io->Cached() <= StormByte::ByteSize{cap});
	ASSERT_TRUE(io->Evicted() > 0);
	const std::size_t back = 8 * 1024;
	if (SeekExpectTell(in, kHexFile - back) != 0)
		return 1;
	if (ReadExpect(in, kHexFile - back, back) != 0)
		return 1;
	ASSERT_EQUAL(StormByte::ByteSize{kHexFile}, io->Origin());
	RETURN_TEST(0);
}

int test_sequential_contiguous_blocks_single_span() {
	const auto path = HexPath();
	ASSERT_TRUE(WriteHexFile(path));
	BufferedFileReader in(Loc(path), P(64 * 1024, 8 * 1024 * 1024));
	ASSERT_TRUE(in.Open());
	ASSERT_EQUAL(static_cast<long long>(kHexFile), DrainHex(in, 1000));
	const auto* io = IoTel(in);
	ASSERT_TRUE(io != nullptr);
	ASSERT_EQUAL(StormByte::ByteSize{kHexFile}, io->Delivered());
	ASSERT_EQUAL(io->Delivered(), io->HitAhead() + io->HitBack() + io->Miss());
	ASSERT_EQUAL(StormByte::ByteSize{kHexFile}, io->Origin());
	ASSERT_EQUAL(StormByte::ByteSize{kHexFile}, io->Cached());
	ASSERT_EQUAL(static_cast<std::size_t>(0), io->Evicted());
	const std::size_t seek_origin = io->SeekOrigin();
	if (SeekExpectTell(in, 0) != 0)
		return 1;
	ASSERT_EQUAL(StormByte::ByteSize{kHexFile}, in.Available());
	for (std::size_t at = 0; at < kHexFile; at += 4096) {
		if (ReadExpect(in, at, 4096) != 0)
			return 1;
	}
	ASSERT_EQUAL(StormByte::ByteSize{2 * kHexFile}, io->Delivered());
	ASSERT_EQUAL(StormByte::ByteSize{kHexFile}, io->HitBack());
	ASSERT_EQUAL(StormByte::ByteSize{kHexFile}, io->Origin());
	ASSERT_EQUAL(seek_origin, io->SeekOrigin());
	if (SeekExpectTell(in, 1234567) != 0)
		return 1;
	if (ReadExpect(in, 1234567, 70000) != 0)
		return 1;
	ASSERT_EQUAL(StormByte::ByteSize{kHexFile}, io->Origin());
	RETURN_TEST(0);
}

int test_sequential_overlapping_pulls_merge() {
	const auto path = HexPath();
	ASSERT_TRUE(WriteHexFile(path));
	BufferedFileReader in(Loc(path), P(0, 1024 * 1024));
	ASSERT_TRUE(in.Open());
	for (const std::size_t at : {std::size_t{20000}, std::size_t{50000}, std::size_t{90000}}) {
		if (SeekExpectTell(in, at) != 0)
			return 1;
		if (ReadExpect(in, at, 5000) != 0)
			return 1;
	}
	if (SeekExpectTell(in, 0) != 0)
		return 1;
	for (std::size_t at = 0; at + 777 <= 128 * 1024; at += 777) {
		if (ReadExpect(in, at, 777) != 0)
			return 1;
	}
	if (SeekExpectTell(in, 0) != 0)
		return 1;
	const auto* io = IoTel(in);
	ASSERT_TRUE(io != nullptr);
	const auto origin = io->Origin();
	ASSERT_TRUE(in.Available() >= StormByte::ByteSize{120 * 1024});
	if (ReadExpect(in, 0, 120 * 1024) != 0)
		return 1;
	ASSERT_EQUAL(origin, io->Origin());
	ASSERT_EQUAL(io->Delivered(), io->HitAhead() + io->HitBack() + io->Miss());
	RETURN_TEST(0);
}

int test_sequential_large_benchmark() {
	constexpr std::size_t size = 64 * 1024 * 1024;
	const auto path = std::filesystem::temp_directory_path() / "stormbyte_buffer_hex_64m.bin";
	ASSERT_TRUE(WriteHexFile(path, size));

	struct Config {
		const char* name;
		std::size_t read_ahead;
		std::size_t max_memory;
	};
	constexpr std::array<Config, 4> configs {{
		{ "no-cache", 0, 0 },
		{ "cache-only", 0, 2 * size },
		{ "window", 256 * 1024, 4 * 1024 * 1024 },
		{ "full", 1024 * 1024, 2 * size },
	}};
	std::array<double, configs.size()> seconds {};
	for (std::size_t c = 0; c < configs.size(); ++c) {
		BufferedFileReader in(Loc(path), P(configs[c].read_ahead, configs[c].max_memory));
		ASSERT_TRUE(in.Open());
		const auto started = std::chrono::steady_clock::now();
		ASSERT_EQUAL(static_cast<long long>(size), DrainHex(in, 32 * 1024));
		seconds[c] = std::chrono::duration<double>(std::chrono::steady_clock::now() - started).count();
		const auto* io = IoTel(in);
		ASSERT_TRUE(io != nullptr);
		ASSERT_EQUAL(StormByte::ByteSize{size}, io->Origin());
		std::cout << "[benchmark " << configs[c].name << "] " << seconds[c] << " s, "
			<< (static_cast<double>(size) / (1024.0 * 1024.0)) / seconds[c] << " MiB/s" << std::endl;
	}
	std::filesystem::remove(path);
	for (std::size_t c = 1; c < configs.size(); ++c)
		ASSERT_TRUE(seconds[c] <= seconds[0] * 20.0 + 2.0);
	RETURN_TEST(0);
}

// -------------------
// Read
// -------------------

int test_empty_file() {
	BufferedFileReader in(Loc(File("empty.bin")));
	static_assert(StormByte::Type::SameAs<decltype(in.Size()),
		StormByte::Safe::Optional<StormByte::ByteSize>>);
	ASSERT_FALSE(in.Size().has_value());
	ASSERT_TRUE(in.Open());
	const auto size = in.Size();
	ASSERT_TRUE(size.has_value());
	ASSERT_EQUAL(StormByte::ByteSize{0}, *size);
	ASSERT_EQUAL(StormByte::ByteSize{0}, in.Tell());
	FIFO dest("KEEP");
	const auto read = in.Read(1, dest);
	ASSERT_EQUAL(ToString(Status::End), ToString(read.status));
	ASSERT_EQUAL(StormByte::ByteSize{0}, read.count);
	ASSERT_EQUAL(std::string("KEEP"), Text(dest));
	ASSERT_EQUAL(StormByte::ByteSize{0}, in.Tell());
	ASSERT_TRUE(in.EoF());
	ASSERT_FALSE(static_cast<bool>(in));
	ASSERT_EQUAL(Status::Ok, in.Close().status);
	ASSERT_FALSE(in.Size().has_value());
	ASSERT_TRUE(size.has_value());
	ASSERT_EQUAL(StormByte::ByteSize{0}, *size);
	RETURN_TEST(0);
}

int test_open_five_read_exact() {
	BufferedFileReader in(Loc(File("five.bin")));
	ASSERT_TRUE(in.Open());
	ASSERT_TRUE(in.IsOpen());
	ASSERT_TRUE(static_cast<bool>(in));
	ASSERT_EQUAL(ToString(State::Idle), ToString(in.State()));
	ASSERT_TRUE(in.IsSeekable());
	ASSERT_TRUE(in.IsSized());
	const auto size = in.Size();
	ASSERT_TRUE(size.has_value());
	ASSERT_EQUAL(StormByte::ByteSize{5}, *size);
	ASSERT_EQUAL(StormByte::ByteSize{0}, in.Tell());
	ASSERT_EQUAL(Loc(File("five.bin")), in.Path());
	FIFO dest;
	const auto read = in.Read(5, dest);
	ASSERT_EQUAL(ToString(Status::Ok), ToString(read.status));
	ASSERT_EQUAL(StormByte::ByteSize{5}, read.count);
	ASSERT_EQUAL(std::string("ABCDE"), Text(dest));
	ASSERT_EQUAL(StormByte::ByteSize{5}, in.Tell());
	RETURN_TEST(0);
}

int test_peek_does_not_consume() {
	BufferedFileReader in(Loc(File("five.bin")));
	ASSERT_TRUE(in.Open());
	FIFO peek;
	const auto peeked = in.Peek(3, peek);
	ASSERT_EQUAL(ToString(Status::Ok), ToString(peeked.status));
	ASSERT_EQUAL(std::string("ABC"), Text(peek));
	ASSERT_EQUAL(StormByte::ByteSize{0}, in.Tell());
	FIFO dest;
	ASSERT_EQUAL(ToString(Status::Ok), ToString(in.Read(3, dest).status));
	ASSERT_EQUAL(std::string("ABC"), Text(dest));
	ASSERT_EQUAL(StormByte::ByteSize{3}, in.Tell());
	RETURN_TEST(0);
}

int test_peek_span_does_not_consume() {
	BufferedFileReader in(Loc(File("five.bin")));
	ASSERT_TRUE(in.Open());
	std::array<std::byte, 3> raw {};
	const auto peeked = in.Peek(std::span<std::byte>(raw));
	ASSERT_EQUAL(ToString(Status::Ok), ToString(peeked.status));
	ASSERT_EQUAL(std::string("ABC"),
		std::string(reinterpret_cast<const char*>(raw.data()), 3));
	ASSERT_EQUAL(StormByte::ByteSize{0}, in.Tell());
	FIFO dest;
	ASSERT_EQUAL(ToString(Status::Ok), ToString(in.Read(3, dest).status));
	ASSERT_EQUAL(std::string("ABC"), Text(dest));
	ASSERT_EQUAL(StormByte::ByteSize{3}, in.Tell());
	RETURN_TEST(0);
}

int test_read_overwrites_dest() {
	BufferedFileReader in(Loc(File("five.bin")));
	ASSERT_TRUE(in.Open());
	FIFO dest("OLD");
	const auto read = in.Read(5, dest);
	ASSERT_EQUAL(ToString(Status::Ok), ToString(read.status));
	ASSERT_EQUAL(std::string("ABCDE"), Text(dest));
	RETURN_TEST(0);
}

int test_read_past_end_is_short() {
	BufferedFileReader in(Loc(File("five.bin")));
	ASSERT_TRUE(in.Open());
	FIFO dest;
	const auto read = in.Read(100, dest);
	ASSERT_EQUAL(ToString(Status::End), ToString(read.status));
	ASSERT_EQUAL(StormByte::ByteSize{5}, read.count);
	ASSERT_EQUAL(std::string("ABCDE"), Text(dest));
	ASSERT_EQUAL(StormByte::ByteSize{5}, in.Tell());
	ASSERT_TRUE(in.EoF());
	ASSERT_FALSE(static_cast<bool>(in));
	ASSERT_EQUAL(ToString(State::Idle), ToString(in.State()));
	RETURN_TEST(0);
}

int test_read_span_before_open_leaves_dest() {
	BufferedFileReader in(Loc(File("five.bin")));
	std::array<std::byte, 2> raw {};
	Fill(raw, std::byte{0x5A});
	const auto got = in.Read(std::span<std::byte>(raw));
	ASSERT_EQUAL(ToString(Status::Failed), ToString(got.status));
	ASSERT_EQUAL(StormByte::ByteSize{0}, got.count);
	ASSERT_EQUAL(static_cast<unsigned>(0x5A), static_cast<unsigned>(raw[0]));
	ASSERT_EQUAL(StormByte::ByteSize{0}, in.Tell());
	RETURN_TEST(0);
}

int test_read_span_empty_ok() {
	BufferedFileReader in(Loc(File("five.bin")));
	ASSERT_TRUE(in.Open());
	const auto got = in.Read(std::span<std::byte>{});
	ASSERT_EQUAL(ToString(Status::Ok), ToString(got.status));
	ASSERT_EQUAL(StormByte::ByteSize{0}, got.count);
	ASSERT_EQUAL(StormByte::ByteSize{0}, in.Tell());
	FIFO dest;
	ASSERT_EQUAL(ToString(Status::Ok), ToString(in.Read(2, dest).status));
	ASSERT_EQUAL(std::string("AB"), Text(dest));
	ASSERT_EQUAL(StormByte::ByteSize{2}, in.Tell());
	RETURN_TEST(0);
}

int test_read_span_exact() {
	BufferedFileReader in(Loc(File("five.bin")));
	ASSERT_TRUE(in.Open());
	std::array<std::byte, 5> raw {};
	Fill(raw, std::byte{0x5A});
	const auto got = in.Read(std::span<std::byte>(raw));
	ASSERT_EQUAL(ToString(Status::Ok), ToString(got.status));
	ASSERT_EQUAL(StormByte::ByteSize{5}, got.count);
	ASSERT_EQUAL(std::string("ABCDE"),
		std::string(reinterpret_cast<const char*>(raw.data()), raw.size()));
	ASSERT_EQUAL(StormByte::ByteSize{5}, in.Tell());
	RETURN_TEST(0);
}

int test_read_span_short_leaves_tail() {
	BufferedFileReader in(Loc(File("five.bin")));
	ASSERT_TRUE(in.Open());
	std::array<std::byte, 8> raw {};
	Fill(raw, std::byte{0x5A});
	const auto got = in.Read(std::span<std::byte>(raw));
	ASSERT_EQUAL(ToString(Status::End), ToString(got.status));
	ASSERT_EQUAL(StormByte::ByteSize{5}, got.count);
	ASSERT_EQUAL(std::string("ABCDE"),
		std::string(reinterpret_cast<const char*>(raw.data()), 5));
	ASSERT_EQUAL(static_cast<unsigned>(0x5A), static_cast<unsigned>(raw[5]));
	ASSERT_EQUAL(StormByte::ByteSize{5}, in.Tell());
	RETURN_TEST(0);
}

int test_read_zero_serves_window() {
	BufferedFileReader in(Loc(File("five.bin")), P(0, 0));
	ASSERT_TRUE(in.Open());
	FIFO empty;
	const auto first = in.Read(0, empty);
	ASSERT_EQUAL(ToString(Status::Ok), ToString(first.status));
	ASSERT_EQUAL(StormByte::ByteSize{0}, first.count);
	ASSERT_EQUAL(StormByte::ByteSize{0}, in.Tell());
	FIFO chunk;
	ASSERT_EQUAL(ToString(Status::Ok), ToString(in.Read(2, chunk).status));
	ASSERT_EQUAL(std::string("AB"), Text(chunk));
	ASSERT_EQUAL(StormByte::ByteSize{2}, in.Tell());
	RETURN_TEST(0);
}

int test_second_read_after_end() {
	BufferedFileReader in(Loc(File("one.bin")));
	ASSERT_TRUE(in.Open());
	FIFO dest;
	ASSERT_EQUAL(ToString(Status::Ok), ToString(in.Read(1, dest).status));
	ASSERT_EQUAL(std::string("A"), Text(dest));
	ASSERT_EQUAL(StormByte::ByteSize{1}, in.Tell());
	FIFO again("KEEP");
	const auto end = in.Read(1, again);
	ASSERT_EQUAL(ToString(Status::End), ToString(end.status));
	ASSERT_EQUAL(StormByte::ByteSize{0}, end.count);
	ASSERT_EQUAL(std::string("KEEP"), Text(again));
	ASSERT_EQUAL(StormByte::ByteSize{1}, in.Tell());
	ASSERT_FALSE(static_cast<bool>(in));
	RETURN_TEST(0);
}

int test_sequential_splits() {
	BufferedFileReader in(Loc(File("five.bin")));
	ASSERT_TRUE(in.Open());
	FIFO a, b;
	ASSERT_EQUAL(ToString(Status::Ok), ToString(in.Read(3, a).status));
	ASSERT_EQUAL(StormByte::ByteSize{3}, in.Tell());
	ASSERT_EQUAL(ToString(Status::Ok), ToString(in.Read(2, b).status));
	ASSERT_EQUAL(std::string("ABC"), Text(a));
	ASSERT_EQUAL(std::string("DE"), Text(b));
	ASSERT_EQUAL(StormByte::ByteSize{5}, in.Tell());
	RETURN_TEST(0);
}

// -------------------
// Seek
// -------------------

int test_peek_window_survives_seek() {
	BufferedFileReader in(Loc(File("ahead.bin")), P(16, 64));
	ASSERT_TRUE(in.Open());
	FIFO window;
	ASSERT_EQUAL(ToString(Status::Ok), ToString(in.Peek(10, window).status));
	ASSERT_EQUAL(std::string("0123456789"), Text(window));
	ASSERT_EQUAL(StormByte::ByteSize{0}, in.Tell());
	ASSERT_EQUAL(ToString(Status::Ok), ToString(in.Seek(4, Position::Absolute).status));
	ASSERT_EQUAL(StormByte::ByteSize{4}, in.Tell());
	ASSERT_FALSE(in.EoF());
	FIFO mid;
	ASSERT_EQUAL(ToString(Status::Ok), ToString(in.Peek(4, mid).status));
	ASSERT_EQUAL(std::string("4567"), Text(mid));
	ASSERT_EQUAL(StormByte::ByteSize{4}, in.Tell());
	ASSERT_EQUAL(ToString(Status::Ok), ToString(in.Seek(0, Position::Absolute).status));
	ASSERT_EQUAL(StormByte::ByteSize{0}, in.Tell());
	ASSERT_FALSE(in.EoF());
	FIFO again;
	ASSERT_EQUAL(ToString(Status::Ok), ToString(in.Peek(6, again).status));
	ASSERT_EQUAL(std::string("012345"), Text(again));
	FIFO consumed;
	ASSERT_EQUAL(ToString(Status::Ok), ToString(in.Read(6, consumed).status));
	ASSERT_EQUAL(std::string("012345"), Text(consumed));
	ASSERT_EQUAL(StormByte::ByteSize{6}, in.Tell());
	RETURN_TEST(0);
}

int test_seek_absolute_and_relative() {
	BufferedFileReader in(Loc(File("seek.bin")));
	ASSERT_TRUE(in.Open());
	FIFO dest;
	ASSERT_EQUAL(ToString(Status::Ok), ToString(in.Seek(4, Position::Absolute).status));
	ASSERT_EQUAL(StormByte::ByteSize{4}, in.Tell());
	ASSERT_EQUAL(ToString(Status::Ok), ToString(in.Read(2, dest).status));
	ASSERT_EQUAL(std::string("45"), Text(dest));
	ASSERT_EQUAL(StormByte::ByteSize{6}, in.Tell());
	ASSERT_EQUAL(ToString(Status::Ok), ToString(in.Seek(-2, Position::Relative).status));
	ASSERT_EQUAL(StormByte::ByteSize{4}, in.Tell());
	FIFO again;
	ASSERT_EQUAL(ToString(Status::Ok), ToString(in.Read(3, again).status));
	ASSERT_EQUAL(std::string("456"), Text(again));
	ASSERT_EQUAL(StormByte::ByteSize{7}, in.Tell());
	RETURN_TEST(0);
}

int test_seek_end_via_size() {
	BufferedFileReader in(Loc(File("seek.bin")));
	ASSERT_TRUE(in.Open());
	ASSERT_TRUE(in.IsSized());
	const auto size = in.Size();
	ASSERT_TRUE(size.has_value());
	ASSERT_EQUAL(ToString(Status::Ok),
		ToString(in.Seek(static_cast<std::ptrdiff_t>(*size), Position::Absolute).status));
	ASSERT_EQUAL(*size, in.Tell());
	FIFO dest("KEEP");
	const auto end = in.Read(1, dest);
	ASSERT_EQUAL(ToString(Status::End), ToString(end.status));
	ASSERT_EQUAL(StormByte::ByteSize{0}, end.count);
	ASSERT_EQUAL(std::string("KEEP"), Text(dest));
	ASSERT_EQUAL(*size, in.Tell());
	ASSERT_TRUE(in.EoF());
	ASSERT_EQUAL(ToString(Status::Ok),
		ToString(in.Seek(static_cast<std::ptrdiff_t>(*size) - 2, Position::Absolute).status));
	ASSERT_EQUAL(*size - 2, in.Tell());
	ASSERT_FALSE(in.EoF());
	ASSERT_TRUE(static_cast<bool>(in));
	FIFO tail;
	ASSERT_EQUAL(ToString(Status::Ok), ToString(in.Read(2, tail).status));
	ASSERT_EQUAL(StormByte::ByteSize{2}, tail.Available());
	ASSERT_EQUAL(*size, in.Tell());
	RETURN_TEST(0);
}

int test_seek_hits_readahead_then_realigns() {
	BufferedFileReader in(Loc(File("ahead.bin")), P(16, 64));
	ASSERT_TRUE(in.Open());
	FIFO first;
	ASSERT_EQUAL(ToString(Status::Ok), ToString(in.Read(2, first).status));
	ASSERT_EQUAL(StormByte::ByteSize{2}, in.Tell());
	std::this_thread::sleep_for(std::chrono::milliseconds(50));
	ASSERT_EQUAL(ToString(Status::Ok), ToString(in.Seek(6, Position::Absolute).status));
	ASSERT_EQUAL(StormByte::ByteSize{6}, in.Tell());
	ASSERT_FALSE(in.EoF());
	FIFO mid;
	ASSERT_EQUAL(ToString(Status::Ok), ToString(in.Read(4, mid).status));
	ASSERT_EQUAL(std::string("6789"), Text(mid));
	ASSERT_EQUAL(StormByte::ByteSize{10}, in.Tell());
	RETURN_TEST(0);
}

int test_seek_negative_absolute_fails() {
	BufferedFileReader in(Loc(File("seek.bin")));
	ASSERT_TRUE(in.Open());
	ASSERT_EQUAL(ToString(Status::Failed), ToString(in.Seek(-1, Position::Absolute).status));
	ASSERT_EQUAL(StormByte::ByteSize{0}, in.Tell());
	RETURN_TEST(0);
}

int test_seek_past_size_then_read() {
	BufferedFileReader in(Loc(File("five.bin")));
	ASSERT_TRUE(in.Open());
	const auto size = in.Size();
	ASSERT_TRUE(size.has_value());
	ASSERT_EQUAL(ToString(Status::Ok),
		ToString(in.Seek(static_cast<std::ptrdiff_t>(*size + 8), Position::Absolute).status));
	ASSERT_EQUAL(*size + 8, in.Tell());
	FIFO dest("KEEP");
	const auto got = in.Read(4, dest);
	ASSERT_EQUAL(ToString(Status::End), ToString(got.status));
	ASSERT_EQUAL(StormByte::ByteSize{0}, got.count);
	ASSERT_EQUAL(std::string("KEEP"), Text(dest));
	ASSERT_EQUAL(*size + 8, in.Tell());
	RETURN_TEST(0);
}

int test_seek_relative_before_zero_fails() {
	BufferedFileReader in(Loc(File("seek.bin")));
	ASSERT_TRUE(in.Open());
	ASSERT_EQUAL(ToString(Status::Failed), ToString(in.Seek(-1, Position::Relative).status));
	ASSERT_EQUAL(StormByte::ByteSize{0}, in.Tell());
	RETURN_TEST(0);
}

int test_seek_without_open_fails() {
	BufferedFileReader in(Loc(File("seek.bin")));
	ASSERT_EQUAL(ToString(Status::Failed), ToString(in.Seek(0, Position::Absolute).status));
	ASSERT_EQUAL(StormByte::ByteSize{0}, in.Tell());
	RETURN_TEST(0);
}

int test_seek_zero_after_end_clears_eof() {
	BufferedFileReader in(Loc(File("five.bin")));
	ASSERT_TRUE(in.Open());
	FIFO first;
	const auto drain = in.Read(16, first);
	ASSERT_EQUAL(ToString(Status::End), ToString(drain.status));
	ASSERT_EQUAL(StormByte::ByteSize{5}, drain.count);
	ASSERT_EQUAL(std::string("ABCDE"), Text(first));
	ASSERT_TRUE(in.EoF());
	ASSERT_FALSE(static_cast<bool>(in));
	ASSERT_EQUAL(ToString(State::Idle), ToString(in.State()));
	ASSERT_EQUAL(ToString(Status::Ok), ToString(in.Seek(0, Position::Absolute).status));
	ASSERT_EQUAL(StormByte::ByteSize{0}, in.Tell());
	ASSERT_FALSE(in.EoF());
	ASSERT_TRUE(static_cast<bool>(in));
	ASSERT_TRUE(in.IsReadable());
	ASSERT_EQUAL(ToString(State::Idle), ToString(in.State()));
	FIFO second;
	const auto again = in.Read(16, second);
	ASSERT_EQUAL(ToString(Status::End), ToString(again.status));
	ASSERT_EQUAL(StormByte::ByteSize{5}, again.count);
	ASSERT_EQUAL(std::string("ABCDE"), Text(second));
	ASSERT_TRUE(in.EoF());
	RETURN_TEST(0);
}

int test_seek_zero_after_end_twice() {
	BufferedFileReader in(Loc(File("five.bin")));
	ASSERT_TRUE(in.Open());
	for (int pass = 0; pass < 2; ++pass) {
		FIFO dest;
		const auto got = in.Read(8, dest);
		ASSERT_EQUAL(ToString(Status::End), ToString(got.status));
		ASSERT_EQUAL(StormByte::ByteSize{5}, got.count);
		ASSERT_EQUAL(std::string("ABCDE"), Text(dest));
		ASSERT_TRUE(in.EoF());
		ASSERT_EQUAL(ToString(Status::Ok), ToString(in.Seek(0, Position::Absolute).status));
		ASSERT_FALSE(in.EoF());
		ASSERT_EQUAL(StormByte::ByteSize{0}, in.Tell());
	}
	FIFO last;
	ASSERT_EQUAL(ToString(Status::Ok), ToString(in.Read(5, last).status));
	ASSERT_EQUAL(std::string("ABCDE"), Text(last));
	RETURN_TEST(0);
}

int test_seek_zero_on_fresh_open() {
	BufferedFileReader in(Loc(File("five.bin")));
	ASSERT_TRUE(in.Open());
	ASSERT_EQUAL(ToString(Status::Ok), ToString(in.Seek(0, Position::Absolute).status));
	ASSERT_EQUAL(StormByte::ByteSize{0}, in.Tell());
	ASSERT_FALSE(in.EoF());
	ASSERT_TRUE(static_cast<bool>(in));
	ASSERT_EQUAL(ToString(State::Idle), ToString(in.State()));
	FIFO dest;
	ASSERT_EQUAL(ToString(Status::Ok), ToString(in.Read(5, dest).status));
	ASSERT_EQUAL(std::string("ABCDE"), Text(dest));
	RETURN_TEST(0);
}

int test_seek_zero_rereads() {
	BufferedFileReader in(Loc(File("seek.bin")));
	ASSERT_TRUE(in.Open());
	FIFO first;
	ASSERT_EQUAL(ToString(Status::Ok), ToString(in.Read(4, first).status));
	ASSERT_EQUAL(StormByte::ByteSize{4}, in.Tell());
	ASSERT_EQUAL(ToString(Status::Ok), ToString(in.Seek(0, Position::Absolute).status));
	ASSERT_EQUAL(StormByte::ByteSize{0}, in.Tell());
	ASSERT_FALSE(in.EoF());
	ASSERT_TRUE(static_cast<bool>(in));
	FIFO again;
	ASSERT_EQUAL(ToString(Status::Ok), ToString(in.Read(4, again).status));
	ASSERT_EQUAL(Text(first), Text(again));
	ASSERT_EQUAL(StormByte::ByteSize{4}, in.Tell());
	RETURN_TEST(0);
}

// -------------------
// Session
// -------------------

int test_close_then_reopen() {
	BufferedFileReader in(Loc(File("five.bin")));
	ASSERT_TRUE(in.Open());
	ASSERT_EQUAL(ToString(Status::Ok), ToString(in.Close().status));
	ASSERT_EQUAL(ToString(State::Unavailable), ToString(in.State()));
	ASSERT_FALSE(static_cast<bool>(in));
	ASSERT_EQUAL(ToString(Status::Ok), ToString(in.Close().status));
	FIFO dest("KEEP");
	ASSERT_EQUAL(ToString(Status::Failed), ToString(in.Read(1, dest).status));
	ASSERT_EQUAL(std::string("KEEP"), Text(dest));
	ASSERT_FALSE(in.Rewind());
	ASSERT_TRUE(in.Open());
	ASSERT_EQUAL(ToString(State::Idle), ToString(in.State()));
	ASSERT_TRUE(static_cast<bool>(in));
	ASSERT_EQUAL(StormByte::ByteSize{0}, in.Tell());
	FIFO again;
	ASSERT_EQUAL(ToString(Status::Ok), ToString(in.Read(5, again).status));
	ASSERT_EQUAL(std::string("ABCDE"), Text(again));
	RETURN_TEST(0);
}

int test_ctor_unavailable_bool_false() {
	BufferedFileReader in(Loc(File("five.bin")));
	ASSERT_EQUAL(ToString(State::Unavailable), ToString(in.State()));
	ASSERT_FALSE(static_cast<bool>(in));
	ASSERT_FALSE(in.IsOpen());
	ASSERT_FALSE(in.IsReadable());
	ASSERT_EQUAL(StormByte::ByteSize{0}, in.Tell());
	ASSERT_EQUAL(StormByte::ByteSize{0}, in.ReadAhead());
	ASSERT_EQUAL(Loc(File("five.bin")), in.Path());
	RETURN_TEST(0);
}

int test_open_directory() {
	BufferedFileReader in(Loc(CurrentFileDirectory / "files"));
	ASSERT_FALSE(in.Open());
	ASSERT_EQUAL(ToString(State::Directory), ToString(in.State()));
	ASSERT_FALSE(static_cast<bool>(in));
	RETURN_TEST(0);
}

int test_open_missing() {
	BufferedFileReader in(Loc(File("does-not-exist.bin")));
	ASSERT_FALSE(in.Open());
	ASSERT_EQUAL(ToString(State::Missing), ToString(in.State()));
	ASSERT_FALSE(static_cast<bool>(in));
	ASSERT_FALSE(in.IsOpen());
	RETURN_TEST(0);
}

int test_open_not_idempotent() {
	BufferedFileReader in(Loc(File("five.bin")));
	ASSERT_TRUE(in.Open());
	ASSERT_EQUAL(ToString(State::Idle), ToString(in.State()));
	ASSERT_TRUE(static_cast<bool>(in));
	ASSERT_FALSE(in.Open());
	ASSERT_EQUAL(ToString(State::Idle), ToString(in.State()));
	ASSERT_TRUE(static_cast<bool>(in));
	FIFO dest;
	ASSERT_EQUAL(ToString(Status::Ok), ToString(in.Read(5, dest).status));
	ASSERT_EQUAL(std::string("ABCDE"), Text(dest));
	RETURN_TEST(0);
}

int test_read_before_open_leaves_dest() {
	BufferedFileReader in(Loc(File("five.bin")));
	FIFO dest("KEEP");
	const auto read = in.Read(1, dest);
	ASSERT_EQUAL(ToString(Status::Failed), ToString(read.status));
	ASSERT_EQUAL(StormByte::ByteSize{0}, read.count);
	ASSERT_EQUAL(std::string("KEEP"), Text(dest));
	ASSERT_EQUAL(StormByte::ByteSize{0}, in.Tell());
	RETURN_TEST(0);
}

int test_rewind_rereads() {
	BufferedFileReader in(Loc(File("five.bin")));
	ASSERT_TRUE(in.Open());
	FIFO first;
	ASSERT_EQUAL(ToString(Status::Ok), ToString(in.Read(5, first).status));
	ASSERT_EQUAL(StormByte::ByteSize{5}, in.Tell());
	ASSERT_TRUE(in.Rewind());
	ASSERT_EQUAL(ToString(State::Idle), ToString(in.State()));
	ASSERT_EQUAL(StormByte::ByteSize{0}, in.Tell());
	FIFO second;
	ASSERT_EQUAL(ToString(Status::Ok), ToString(in.Read(5, second).status));
	ASSERT_EQUAL(std::string("ABCDE"), Text(second));
	ASSERT_EQUAL(StormByte::ByteSize{5}, in.Tell());
	RETURN_TEST(0);
}

int test_rewind_without_open_fails() {
	BufferedFileReader in(Loc(File("five.bin")));
	ASSERT_FALSE(in.Rewind());
	ASSERT_EQUAL(ToString(State::Unavailable), ToString(in.State()));
	RETURN_TEST(0);
}

// -------------------
// Telemetry
// -------------------

int test_telemetry_cold_seek_epoch() {
	const auto path = HexPath();
	ASSERT_TRUE(WriteHexFile(path));
	BufferedFileReader in(Loc(path), P(8 * 1024, 32 * 1024));
	ASSERT_TRUE(in.Open());
	if (ReadExpect(in, 0, 4096) != 0)
		return 1;
	DumpTelemetry("tel-cold-warm", in);
	if (SeekExpectTell(in, 2 * 1024 * 1024) != 0)
		return 1;
	if (ReadExpect(in, 2 * 1024 * 1024, 4096) != 0)
		return 1;
	if (SeekExpectTell(in, 0) != 0)
		return 1;
	DumpTelemetry("tel-cold-epoch-closed", in);
	const auto tel = in.Telemetry();
	ASSERT_TRUE(static_cast<bool>(tel));
	const auto* io = IoTel(in);
	ASSERT_TRUE(io != nullptr);
	ASSERT_EQUAL(tel->Delivered(), io->HitAhead() + io->HitBack() + io->Miss());
	RETURN_TEST(0);
}

int test_telemetry_ctor_is_zero() {
	BufferedFileReader in(Loc(File("five.bin")), P(0, 0));
	DumpTelemetry("ctor", in);
	const auto tel = in.Telemetry();
	ASSERT_TRUE(static_cast<bool>(tel));
	const auto* io = IoTel(in);
	ASSERT_TRUE(io != nullptr);
	ASSERT_EQUAL(StormByte::ByteSize{0}, tel->Delivered());
	ASSERT_EQUAL(StormByte::ByteSize{0}, io->HitAhead() + io->HitBack());
	ASSERT_EQUAL(StormByte::ByteSize{0}, io->Miss());
	ASSERT_EQUAL(static_cast<std::size_t>(0), io->SeekLogical());
	RETURN_TEST(0);
}

int test_telemetry_fake_seek_epoch() {
	const auto path = HexPath();
	ASSERT_TRUE(WriteHexFile(path));
	BufferedFileReader in(Loc(path), P(16 * 1024, 256 * 1024));
	ASSERT_TRUE(in.Open());
	DumpTelemetry("tel-fake-open", in);
	if (ReadExpect(in, 0, 64 * 1024) != 0)
		return 1;
	DumpTelemetry("tel-fake-filled", in);
	if (SeekExpectTell(in, 8 * 1024) != 0)
		return 1;
	DumpTelemetry("tel-fake-seek-open-epoch", in);
	if (ReadExpect(in, 8 * 1024, 1024) != 0)
		return 1;
	if (ReadExpect(in, 9 * 1024, 1024) != 0)
		return 1;
	DumpTelemetry("tel-fake-small-reads", in);
	if (SeekExpectTell(in, 0) != 0)
		return 1;
	DumpTelemetry("tel-fake-epoch-closed", in);
	const auto tel = in.Telemetry();
	ASSERT_TRUE(static_cast<bool>(tel));
	const auto* io = IoTel(in);
	ASSERT_TRUE(io != nullptr);
	ASSERT_EQUAL(tel->Delivered(), io->HitAhead() + io->HitBack() + io->Miss());
	RETURN_TEST(0);
}

int test_telemetry_hex_pressure() {
	const auto path = HexPath();
	ASSERT_TRUE(WriteHexFile(path));
	BufferedFileReader in(Loc(path), P(16 * 1024, 64 * 1024));
	ASSERT_TRUE(in.Open());
	DumpTelemetry("open", in);
	if (ReadExpect(in, 0, 512 * 1024) != 0)
		return 1;
	DumpTelemetry("seq-512k", in);
	if (SeekExpectTell(in, 8 * 1024) != 0)
		return 1;
	if (ReadExpect(in, 8 * 1024, 8192) != 0)
		return 1;
	DumpTelemetry("seek-back-8k", in);
	if (SeekExpectTell(in, 2 * 1024 * 1024) != 0)
		return 1;
	if (ReadExpect(in, 2 * 1024 * 1024, 8192) != 0)
		return 1;
	DumpTelemetry("seek-cold-2m", in);
	ASSERT_EQUAL(ToString(Status::Ok), ToString(in.Close().status));
	DumpTelemetry("after-close", in);
	const auto tel = in.Telemetry();
	ASSERT_TRUE(static_cast<bool>(tel));
	const auto* io = IoTel(in);
	ASSERT_TRUE(io != nullptr);
	ASSERT_EQUAL(tel->Delivered(), io->HitAhead() + io->HitBack() + io->Miss());
	RETURN_TEST(0);
}

int test_telemetry_saved_partial_disjoint() {
	const auto path = HexPath();
	ASSERT_TRUE(WriteHexFile(path));
	BufferedFileReader in(Loc(path), P(4 * 1024, 256 * 1024));
	ASSERT_TRUE(in.Open());
	if (ReadExpect(in, 0, 8192) != 0)
		return 1;
	if (SeekExpectTell(in, 1024 * 1024) != 0)
		return 1;
	if (ReadExpect(in, 1024 * 1024, 8192) != 0)
		return 1;
	if (SeekExpectTell(in, 1024) != 0)
		return 1;
	if (ReadExpect(in, 1024, 32 * 1024) != 0)
		return 1;
	ASSERT_EQUAL(ToString(Status::Ok),
		ToString(in.Seek(0, Position::Absolute).status));
	const auto tel = in.Telemetry();
	ASSERT_TRUE(static_cast<bool>(tel));
	const auto* io = IoTel(in);
	ASSERT_TRUE(io != nullptr);
	ASSERT_EQUAL(tel->Delivered(), io->HitAhead() + io->HitBack() + io->Miss());
	ASSERT_TRUE(io->SeekOrigin() > 0);
	ASSERT_TRUE(io->SeekSavedPartial() > 0);
	RETURN_TEST(0);
}

// -------------------
// Tell
// -------------------

int test_tell_after_span_read_and_seek_zero() {
	BufferedFileReader in(Loc(File("five.bin")));
	ASSERT_TRUE(in.Open());
	std::array<std::byte, 3> raw {};
	ASSERT_EQUAL(StormByte::ByteSize{3}, in.Read(std::span<std::byte>(raw)).count);
	ASSERT_EQUAL(StormByte::ByteSize{3}, in.Tell());
	ASSERT_EQUAL(ToString(Status::Ok), ToString(in.Seek(0, Position::Absolute).status));
	ASSERT_EQUAL(StormByte::ByteSize{0}, in.Tell());
	ASSERT_FALSE(in.EoF());
	FIFO dest;
	ASSERT_EQUAL(ToString(Status::Ok), ToString(in.Read(5, dest).status));
	ASSERT_EQUAL(std::string("ABCDE"), Text(dest));
	ASSERT_EQUAL(StormByte::ByteSize{5}, in.Tell());
	RETURN_TEST(0);
}

int test_tell_at_size_and_past_size() {
	BufferedFileReader in(Loc(File("five.bin")));
	ASSERT_TRUE(in.Open());
	const auto size = in.Size();
	ASSERT_TRUE(size.has_value());
	ASSERT_EQUAL(ToString(Status::Ok),
		ToString(in.Seek(static_cast<std::ptrdiff_t>(*size), Position::Absolute).status));
	ASSERT_EQUAL(*size, in.Tell());
	FIFO dest("KEEP");
	ASSERT_EQUAL(StormByte::ByteSize{0}, in.Read(3, dest).count);
	ASSERT_EQUAL(*size, in.Tell());
	ASSERT_EQUAL(ToString(Status::Ok),
		ToString(in.Seek(static_cast<std::ptrdiff_t>(*size + 6), Position::Absolute).status));
	ASSERT_EQUAL(*size + 6, in.Tell());
	ASSERT_EQUAL(StormByte::ByteSize{0}, in.Read(1, dest).count);
	ASSERT_EQUAL(*size + 6, in.Tell());
	RETURN_TEST(0);
}

int test_tell_matches_seek_and_failed_seek_stays() {
	BufferedFileReader in(Loc(File("seek.bin")));
	ASSERT_TRUE(in.Open());
	ASSERT_EQUAL(ToString(Status::Ok), ToString(in.Seek(7, Position::Absolute).status));
	ASSERT_EQUAL(StormByte::ByteSize{7}, in.Tell());
	ASSERT_EQUAL(ToString(Status::Ok), ToString(in.Seek(-3, Position::Relative).status));
	ASSERT_EQUAL(StormByte::ByteSize{4}, in.Tell());
	ASSERT_EQUAL(ToString(Status::Failed), ToString(in.Seek(-10, Position::Relative).status));
	ASSERT_EQUAL(StormByte::ByteSize{4}, in.Tell());
	ASSERT_EQUAL(ToString(Status::Failed), ToString(in.Seek(-1, Position::Absolute).status));
	ASSERT_EQUAL(StormByte::ByteSize{4}, in.Tell());
	RETURN_TEST(0);
}

int test_tell_max_memory_zero_seek_read() {
	BufferedFileReader in(Loc(File("seek.bin")), P(0, 0));
	ASSERT_TRUE(in.Open());
	ASSERT_EQUAL(StormByte::ByteSize{0}, in.Tell());
	ASSERT_EQUAL(ToString(Status::Ok), ToString(in.Seek(4, Position::Absolute).status));
	ASSERT_EQUAL(StormByte::ByteSize{4}, in.Tell());
	FIFO dest;
	ASSERT_EQUAL(StormByte::ByteSize{2}, in.Read(2, dest).count);
	ASSERT_EQUAL(std::string("45"), Text(dest));
	ASSERT_EQUAL(StormByte::ByteSize{6}, in.Tell());
	RETURN_TEST(0);
}

int test_tell_open_is_zero() {
	BufferedFileReader in(Loc(File("five.bin")));
	ASSERT_EQUAL(StormByte::ByteSize{0}, in.Tell());
	ASSERT_TRUE(in.Open());
	ASSERT_EQUAL(StormByte::ByteSize{0}, in.Tell());
	RETURN_TEST(0);
}

int test_tell_tracks_each_read() {
	BufferedFileReader in(Loc(File("five.bin")));
	ASSERT_TRUE(in.Open());
	FIFO a, b, c;
	ASSERT_EQUAL(StormByte::ByteSize{1}, in.Read(1, a).count);
	ASSERT_EQUAL(StormByte::ByteSize{1}, in.Tell());
	ASSERT_EQUAL(StormByte::ByteSize{2}, in.Read(2, b).count);
	ASSERT_EQUAL(StormByte::ByteSize{3}, in.Tell());
	ASSERT_EQUAL(StormByte::ByteSize{2}, in.Read(2, c).count);
	ASSERT_EQUAL(StormByte::ByteSize{5}, in.Tell());
	RETURN_TEST(0);
}

int test_tell_unchanged_on_peek_and_empty_span() {
	BufferedFileReader in(Loc(File("five.bin")));
	ASSERT_TRUE(in.Open());
	FIFO dest;
	ASSERT_EQUAL(ToString(Status::Ok), ToString(in.Read(2, dest).status));
	ASSERT_EQUAL(StormByte::ByteSize{2}, in.Tell());
	FIFO peek;
	ASSERT_EQUAL(ToString(Status::Ok), ToString(in.Peek(2, peek).status));
	ASSERT_EQUAL(StormByte::ByteSize{2}, in.Tell());
	std::array<std::byte, 2> raw {};
	ASSERT_EQUAL(ToString(Status::Ok), ToString(in.Peek(std::span<std::byte>(raw)).status));
	ASSERT_EQUAL(StormByte::ByteSize{2}, in.Tell());
	ASSERT_EQUAL(ToString(Status::Ok), ToString(in.Read(std::span<std::byte>{}).status));
	ASSERT_EQUAL(StormByte::ByteSize{2}, in.Tell());
	RETURN_TEST(0);
}

int main() {
	int result = 0;

	// -------------------
	// Fixtures
	// -------------------
	result += test_lines_are_octets();
	result += test_no_nl_and_nul();
	result += test_pattern_256();

	// -------------------
	// Hex
	// -------------------
	result += test_hex_fake_seek_in_page();
	result += test_hex_fake_seek_peek_does_not_move_tell();
	result += test_hex_fake_seek_small_reads_then_catchup();
	result += test_hex_fake_seek_tell_before_read();
	result += test_hex_relative_fake_seek();
	result += test_hex_seek_cold_tell_then_bytes();
	result += test_hex_seek_miss_cold();
	result += test_hex_seek_partial_hole();
	result += test_hex_seek_partial_hole_tell();
	result += test_hex_seek_saved_partial_disjoint_spans();
	result += test_hex_seek_stress_fixed_offsets();
	result += test_hex_seq_matches_pattern();

	// -------------------
	// Move
	// -------------------
	result += test_move_transfers_session();

	// -------------------
	// Setup
	// -------------------
	result += test_explicit_readahead_survives_setup();
	result += test_explicit_zero_survives_open();
	result += test_path_only_setup_sets_readahead();

	// -------------------
	// Volume
	// -------------------
	result += test_block_4k_chunked();
	result += test_max_memory_zero_span_reads();
	result += test_max_memory_zero_still_reads();
	result += test_readahead_knobs();
	result += test_sequential_capped_window_slides();
	result += test_sequential_contiguous_blocks_single_span();
	result += test_sequential_overlapping_pulls_merge();
	result += test_sequential_large_benchmark();

	// -------------------
	// Read
	// -------------------
	result += test_empty_file();
	result += test_open_five_read_exact();
	result += test_peek_does_not_consume();
	result += test_peek_span_does_not_consume();
	result += test_read_overwrites_dest();
	result += test_read_past_end_is_short();
	result += test_read_span_before_open_leaves_dest();
	result += test_read_span_empty_ok();
	result += test_read_span_exact();
	result += test_read_span_short_leaves_tail();
	result += test_read_zero_serves_window();
	result += test_second_read_after_end();
	result += test_sequential_splits();

	// -------------------
	// Seek
	// -------------------
	result += test_peek_window_survives_seek();
	result += test_seek_absolute_and_relative();
	result += test_seek_end_via_size();
	result += test_seek_hits_readahead_then_realigns();
	result += test_seek_negative_absolute_fails();
	result += test_seek_past_size_then_read();
	result += test_seek_relative_before_zero_fails();
	result += test_seek_without_open_fails();
	result += test_seek_zero_after_end_clears_eof();
	result += test_seek_zero_after_end_twice();
	result += test_seek_zero_on_fresh_open();
	result += test_seek_zero_rereads();

	// -------------------
	// Session
	// -------------------
	result += test_close_then_reopen();
	result += test_ctor_unavailable_bool_false();
	result += test_open_directory();
	result += test_open_missing();
	result += test_open_not_idempotent();
	result += test_read_before_open_leaves_dest();
	result += test_rewind_rereads();
	result += test_rewind_without_open_fails();

	// -------------------
	// Telemetry
	// -------------------
	result += test_telemetry_cold_seek_epoch();
	result += test_telemetry_ctor_is_zero();
	result += test_telemetry_fake_seek_epoch();
	result += test_telemetry_hex_pressure();
	result += test_telemetry_saved_partial_disjoint();

	// -------------------
	// Tell
	// -------------------
	result += test_tell_after_span_read_and_seek_zero();
	result += test_tell_at_size_and_past_size();
	result += test_tell_matches_seek_and_failed_seek_stays();
	result += test_tell_max_memory_zero_seek_read();
	result += test_tell_open_is_zero();
	result += test_tell_tracks_each_read();
	result += test_tell_unchanged_on_peek_and_empty_span();

	if (result == 0)
		std::cout << "All tests passed!" << std::endl;
	else
		std::cout << result << " tests failed." << std::endl;
	return result;
}
