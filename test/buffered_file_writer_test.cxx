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
#include <StormByte/buffer/io/buffered_file_writer.hxx>
#include <StormByte/buffer/io/telemetry.hxx>
#include <StormByte/safe/binary.hxx>
#include <StormByte/safe/wstring.hxx>
#include <StormByte/system/file.hxx>
#include <StormByte/test_handlers.h>

#include <chrono>
#include <cstddef>
#include <filesystem>
#include <fstream>
#include <iostream>
#include <iterator>
#include <span>
#include <string>
#include <string_view>
#include <thread>

using StormByte::Buffer::FIFO;
using StormByte::Buffer::Position;
using StormByte::Buffer::IO::BackPressure;
using StormByte::Buffer::IO::BufferedFileWriter;
using StormByte::Buffer::IO::MaxMemory;
using StormByte::Buffer::IO::State;
using StormByte::Buffer::IO::Status;
using StormByte::Buffer::IO::ToString;
using StormByte::Buffer::IO::WriteChunk;
using StormByte::Safe::Binary;

namespace {
	constexpr char kHex[] = "0123456789abcdef";

	struct WriterKnob {
		StormByte::ByteSize chunk;
		StormByte::ByteSize memory;
		std::size_t pressure;
		const char* tag;
	};

	const WriterKnob kKnobs[] = {
		{ StormByte::ByteSize{0}, StormByte::ByteSize{0}, 0, "direct" },
		{ StormByte::ByteSize{4096}, StormByte::ByteSize{0}, 4, "ring-only" },
		{ StormByte::ByteSize{1024}, StormByte::ByteSize{4096}, 2, "pages-small" },
		{ StormByte::ByteSize{4096}, StormByte::ByteSize{256ull * 1024ull}, 4, "pages-large" },
		{ StormByte::ByteSize{4096}, StormByte::ByteSize{65536}, 8, "pages-ring" },
	};

	StormByte::Safe::String Loc(const std::filesystem::path& path) {
#ifdef WINDOWS
		return StormByte::Safe::String(StormByte::Safe::WString(std::wstring_view(path.wstring())));
#else
		return StormByte::Safe::String(std::string_view(path.string()));
#endif
	}

	std::filesystem::path Scratch(const char* tag) {
		StormByte::Safe::String path;
		if (!StormByte::System::File::Temporary(path, tag))
			return {};
		return std::filesystem::path(std::string(path));
	}

	std::string Slurp(const std::filesystem::path& path) {
		std::ifstream in(path, std::ios::in | std::ios::binary);
		if (!in)
			return {};
		return std::string(std::istreambuf_iterator<char>(in), std::istreambuf_iterator<char>());
	}

	char HexAt(const std::size_t i) {
		return kHex[i % 16];
	}

	FIFO FromText(const std::string& text) {
		FIFO fifo;
		Binary data(StormByte::ByteSize{text.size()});
		for (std::size_t i = 0; i < text.size(); ++i)
			data[i] = static_cast<std::byte>(text[i]);
		static_cast<void>(fifo.Write(data.size(), std::move(data)));
		return fifo;
	}

	std::string HexSlice(const std::size_t from, const std::size_t n) {
		std::string s;
		s.resize(n);
		for (std::size_t i = 0; i < n; ++i)
			s[i] = HexAt(from + i);
		return s;
	}

	BufferedFileWriter::Parameters P(const StormByte::ByteSize chunk, const std::size_t pressure,
			const StormByte::ByteSize memory = StormByte::ByteSize{0}) {
		return { WriteChunk(chunk), BackPressure(pressure), MaxMemory(memory) };
	}

	bool WaitDirtyZero(BufferedFileWriter& out) {
		for (int i = 0; i < 80; ++i) {
			if (out.Dirty() == StormByte::ByteSize{0})
				return true;
			std::this_thread::sleep_for(std::chrono::milliseconds(10));
		}
		return out.Dirty() == StormByte::ByteSize{0};
	}

	int CheckTell(BufferedFileWriter& out, const std::size_t expect) {
		ASSERT_EQUAL(StormByte::ByteSize{expect}, out.Tell());
		return 0;
	}

	int WriteExpect(BufferedFileWriter& out, const std::string& text) {
		FIFO src = FromText(text);
		const auto written = out.Write(src);
		ASSERT_EQUAL(ToString(Status::Ok), ToString(written.status));
		ASSERT_EQUAL(StormByte::ByteSize{text.size()}, written.count);
		return 0;
	}

	int SeekExpectTell(BufferedFileWriter& out, const std::ptrdiff_t offset, const Position mode, const std::size_t tell) {
		ASSERT_EQUAL(ToString(Status::Ok), ToString(out.Seek(offset, mode).status));
		return CheckTell(out, tell);
	}

	void DumpTelemetry(const char* tag, const BufferedFileWriter& out) {
		const auto tel = out.Telemetry();
		if (!tel) {
			std::cout << "[telemetry " << tag << "] empty" << std::endl;
			return;
		}
		std::cout << "[telemetry " << tag << "] " << static_cast<std::string>(*tel) << std::endl;
	}

	const StormByte::Buffer::IO::WriteTelemetry* IoW(const BufferedFileWriter& out) {
		const auto tel = out.Telemetry();
		if (!tel)
			return nullptr;
		return dynamic_cast<const StormByte::Buffer::IO::WriteTelemetry*>(tel.get());
	}

	void DumpMismatch(const char* tag, const std::string& expect, const std::string& got) {
		std::cout << "[mismatch " << tag << "]"
			<< " expect_size=" << expect.size()
			<< " got_size=" << got.size();
		const std::size_t n = expect.size() < got.size() ? expect.size() : got.size();
		std::size_t i = 0;
		for (; i < n; ++i) {
			if (expect[i] != got[i])
				break;
		}
		if (i == n && expect.size() == got.size()) {
			std::cout << " identical" << std::endl;
			return;
		}
		std::cout << " first=" << i;
		if (i < expect.size())
			std::cout << " expect_byte="
				<< static_cast<unsigned>(static_cast<unsigned char>(expect[i]));
		if (i < got.size())
			std::cout << " got_byte="
				<< static_cast<unsigned>(static_cast<unsigned char>(got[i]));
		std::cout << std::endl;
	}

	int WriteAll(BufferedFileWriter& out, const std::string& body) {
		std::size_t off = 0;
		while (off < body.size()) {
			const std::size_t n = (body.size() - off) < 3072u ? (body.size() - off) : 3072u;
			FIFO piece = FromText(body.substr(off, n));
			auto written = out.Write(piece);
			if (written.status == Status::TryAgain) {
				ASSERT_EQUAL(ToString(Status::Ok), ToString(out.Flush().status));
				written = out.Write(piece);
			}
			ASSERT_EQUAL(ToString(Status::Ok), ToString(written.status));
			off += n;
		}
		return 0;
	}

	BufferedFileWriter MakeWriter(const std::filesystem::path& path, const WriterKnob& k) {
		return BufferedFileWriter(Loc(path), P(k.chunk, k.pressure, k.memory));
	}
}

// -------------------
// Direct
// -------------------

int test_idle_direct_writer_shutdown() {
	for (std::size_t i = 0; i < 128; ++i)
		BufferedFileWriter out(Loc(Scratch("idle")), P(StormByte::ByteSize{0}, 0));
	RETURN_TEST(0);
}

int test_direct_span_write() {
	const auto path = Scratch("span");
	std::filesystem::remove(path);
	BufferedFileWriter out(Loc(path), P(StormByte::ByteSize{0}, 0));
	ASSERT_TRUE(out.Open());
	const char raw[] = { 'Z', 'Y', 'X' };
	const auto written = out.Write(std::span<const std::byte>(
		reinterpret_cast<const std::byte*>(raw), 3));
	ASSERT_EQUAL(ToString(Status::Ok), ToString(written.status));
	ASSERT_EQUAL(StormByte::ByteSize{3}, out.Tell());
	ASSERT_EQUAL(StormByte::ByteSize{0}, out.Dirty());
	ASSERT_EQUAL(std::string("ZYX"), Slurp(path));
	ASSERT_TRUE(out.Close());
	std::filesystem::remove(path);
	RETURN_TEST(0);
}

int test_direct_write_hits_disk() {
	const auto path = Scratch("direct");
	std::filesystem::remove(path);
	BufferedFileWriter out(Loc(path), P(StormByte::ByteSize{0}, 0));
	ASSERT_TRUE(out.Open());
	ASSERT_EQUAL(StormByte::ByteSize{0}, out.WriteChunk());
	ASSERT_EQUAL(static_cast<std::size_t>(0), out.BackPressure());
	FIFO src = FromText("HELLO");
	const auto written = out.Write(src);
	ASSERT_EQUAL(ToString(Status::Ok), ToString(written.status));
	ASSERT_EQUAL(StormByte::ByteSize{5}, written.count);
	ASSERT_EQUAL(StormByte::ByteSize{0}, src.Available());
	ASSERT_EQUAL(StormByte::ByteSize{5}, out.Tell());
	ASSERT_EQUAL(StormByte::ByteSize{0}, out.Dirty());
	ASSERT_EQUAL(std::string("HELLO"), Slurp(path));
	ASSERT_TRUE(out.Close());
	std::filesystem::remove(path);
	RETURN_TEST(0);
}

int test_empty_write_ok() {
	const auto path = Scratch("empty");
	std::filesystem::remove(path);
	BufferedFileWriter out(Loc(path), P(StormByte::ByteSize{0}, 0));
	ASSERT_TRUE(out.Open());
	FIFO empty;
	const auto written = out.Write(empty);
	ASSERT_EQUAL(ToString(Status::Ok), ToString(written.status));
	ASSERT_EQUAL(StormByte::ByteSize{0}, written.count);
	ASSERT_EQUAL(StormByte::ByteSize{0}, out.Tell());
	ASSERT_EQUAL(StormByte::ByteSize{0}, out.Dirty());
	ASSERT_TRUE(out.Close());
	std::filesystem::remove(path);
	RETURN_TEST(0);
}

int test_explicit_zero_survives_open() {
	const auto path = Scratch("zero");
	std::filesystem::remove(path);
	BufferedFileWriter out(Loc(path), P(StormByte::ByteSize{0}, 0));
	ASSERT_TRUE(out.Open());
	ASSERT_EQUAL(StormByte::ByteSize{0}, out.WriteChunk());
	ASSERT_EQUAL(static_cast<std::size_t>(0), out.BackPressure());
	ASSERT_TRUE(out.Close());
	std::filesystem::remove(path);
	RETURN_TEST(0);
}

// -------------------
// Hex
// -------------------

int test_hex_fake_seek_patch() {
	const auto path = Scratch("hexfake");
	std::filesystem::remove(path);
	BufferedFileWriter out(Loc(path), P(StormByte::ByteSize{4096}, 4, StormByte::ByteSize{256ull * 1024ull}));
	ASSERT_TRUE(out.Open());
	ASSERT_EQUAL(0, WriteExpect(out, HexSlice(0, 65536)));
	DumpTelemetry("hex-fake-before-seek", out);
	ASSERT_EQUAL(0, SeekExpectTell(out, 16, Position::Absolute, 16));
	DumpTelemetry("hex-fake-after-seek", out);
	ASSERT_EQUAL(0, WriteExpect(out, std::string("ZZZZ")));
	ASSERT_EQUAL(0, CheckTell(out, 20));
	DumpTelemetry("hex-fake-after-patch", out);
	ASSERT_TRUE(out.Close());
	std::string expect = HexSlice(0, 65536);
	expect.replace(16, 4, "ZZZZ");
	ASSERT_EQUAL(expect, Slurp(path));
	const auto* io = IoW(out);
	ASSERT_TRUE(io != nullptr);
	ASSERT_EQUAL(StormByte::ByteSize{65536}, io->HighWater());
	ASSERT_EQUAL(io->Materialized(), io->HighWater());
	std::filesystem::remove(path);
	RETURN_TEST(0);
}

int test_hex_fake_seek_small_then_front() {
	const auto path = Scratch("hexfront");
	std::filesystem::remove(path);
	BufferedFileWriter out(Loc(path), P(StormByte::ByteSize{4096}, 4, StormByte::ByteSize{256ull * 1024ull}));
	ASSERT_TRUE(out.Open());
	ASSERT_EQUAL(0, WriteExpect(out, HexSlice(0, 65536)));
	ASSERT_EQUAL(0, SeekExpectTell(out, 8, Position::Absolute, 8));
	ASSERT_EQUAL(0, WriteExpect(out, std::string("aa")));
	ASSERT_EQUAL(0, SeekExpectTell(out, 10, Position::Absolute, 10));
	ASSERT_EQUAL(0, WriteExpect(out, std::string("bb")));
	ASSERT_EQUAL(0, SeekExpectTell(out, 12, Position::Absolute, 12));
	ASSERT_EQUAL(0, WriteExpect(out, std::string("cc")));
	ASSERT_EQUAL(0, SeekExpectTell(out, 65536, Position::Absolute, 65536));
	DumpTelemetry("hex-fake-back-at-front", out);
	ASSERT_TRUE(out.Close());
	std::string expect = HexSlice(0, 65536);
	expect.replace(8, 2, "aa");
	expect.replace(10, 2, "bb");
	expect.replace(12, 2, "cc");
	ASSERT_EQUAL(expect, Slurp(path));
	std::filesystem::remove(path);
	RETURN_TEST(0);
}

int test_hex_island_1m_zeros() {
	const auto path = Scratch("island");
	std::filesystem::remove(path);
	BufferedFileWriter out(Loc(path), P(StormByte::ByteSize{4096}, 4, StormByte::ByteSize{256ull * 1024ull}));
	ASSERT_TRUE(out.Open());
	ASSERT_EQUAL(0, WriteExpect(out, std::string("HEADHEAD")));
	ASSERT_EQUAL(0, SeekExpectTell(out, 1024 * 1024, Position::Absolute, 1024 * 1024));
	ASSERT_EQUAL(0, WriteExpect(out, std::string("TAILTAIL")));
	DumpTelemetry("island-before-close", out);
	ASSERT_TRUE(out.Close());
	const std::string got = Slurp(path);
	ASSERT_EQUAL(static_cast<std::size_t>(1024 * 1024 + 8), got.size());
	ASSERT_EQUAL(std::string("HEADHEAD"), got.substr(0, 8));
	ASSERT_EQUAL(std::string("TAILTAIL"), got.substr(1024 * 1024, 8));
	for (std::size_t i = 8; i < 1024u * 1024u; ++i) {
		if (got[i] != '\0') {
			ASSERT_EQUAL(static_cast<int>(0), static_cast<int>(static_cast<unsigned char>(got[i])));
			break;
		}
	}
	const auto* io = IoW(out);
	ASSERT_TRUE(io != nullptr);
	ASSERT_EQUAL(StormByte::ByteSize{1024ull * 1024ull + 8ull}, io->HighWater());
	ASSERT_EQUAL(io->Materialized(), io->HighWater());
	std::filesystem::remove(path);
	RETURN_TEST(0);
}

int test_hex_seek_past_eof_hole() {
	const auto path = Scratch("hole");
	std::filesystem::remove(path);
	BufferedFileWriter out(Loc(path), P(StormByte::ByteSize{8}, 4, StormByte::ByteSize{64}));
	ASSERT_TRUE(out.Open());
	ASSERT_EQUAL(0, WriteExpect(out, std::string("AB")));
	ASSERT_EQUAL(0, SeekExpectTell(out, 8, Position::Absolute, 8));
	ASSERT_EQUAL(0, WriteExpect(out, std::string("CD")));
	DumpTelemetry("hole-before-close", out);
	ASSERT_TRUE(out.Close());
	const std::string got = Slurp(path);
	ASSERT_EQUAL(static_cast<std::size_t>(10), got.size());
	ASSERT_EQUAL(std::string("AB"), got.substr(0, 2));
	ASSERT_EQUAL(std::string(6, '\0'), got.substr(2, 6));
	ASSERT_EQUAL(std::string("CD"), got.substr(8, 2));
	std::filesystem::remove(path);
	RETURN_TEST(0);
}

int test_hex_seq_matches_pattern() {
	const auto path = Scratch("hexseq");
	std::filesystem::remove(path);
	BufferedFileWriter out(Loc(path), P(StormByte::ByteSize{4096}, 4, StormByte::ByteSize{256ull * 1024ull}));
	ASSERT_TRUE(out.Open());
	const std::string body = HexSlice(0, 65536);
	ASSERT_EQUAL(0, WriteExpect(out, body));
	ASSERT_EQUAL(0, CheckTell(out, 65536));
	DumpTelemetry("hex-seq-before-close", out);
	ASSERT_TRUE(out.Close());
	ASSERT_EQUAL(body, Slurp(path));
	const auto* io = IoW(out);
	ASSERT_TRUE(io != nullptr);
	ASSERT_EQUAL(StormByte::ByteSize{65536}, io->HighWater());
	ASSERT_EQUAL(io->Materialized(), io->HighWater());
	std::filesystem::remove(path);
	RETURN_TEST(0);
}

int test_hex_evict_keeps_pattern() {
	const auto path = Scratch("evict");
	std::filesystem::remove(path);
	BufferedFileWriter out(Loc(path), P(StormByte::ByteSize{1024}, 2, StormByte::ByteSize{4096}));
	ASSERT_TRUE(out.Open());
	const std::string body = HexSlice(0, 16384);
	ASSERT_EQUAL(0, WriteExpect(out, body));
	DumpTelemetry("evict-after-fill", out);
	const auto* io_fill = IoW(out);
	ASSERT_TRUE(io_fill != nullptr);
	ASSERT_TRUE(io_fill->Evicted() > 0);
	ASSERT_EQUAL(0, SeekExpectTell(out, 32, Position::Absolute, 32));
	ASSERT_EQUAL(0, WriteExpect(out, std::string("EVICTOK!")));
	DumpTelemetry("evict-after-patch", out);
	ASSERT_TRUE(out.Close());
	DumpTelemetry("evict-closed", out);
	std::string expect = body;
	expect.replace(32, 8, "EVICTOK!");
	ASSERT_EQUAL(expect, Slurp(path));
	const auto* io = IoW(out);
	ASSERT_TRUE(io != nullptr);
	ASSERT_EQUAL(StormByte::ByteSize{16384}, io->HighWater());
	ASSERT_EQUAL(io->Materialized(), io->HighWater());
	std::filesystem::remove(path);
	RETURN_TEST(0);
}

// -------------------
// Move
// -------------------

int test_move_transfers_session() {
	const auto path = Scratch("move");
	std::filesystem::remove(path);
	BufferedFileWriter out(Loc(path), P(StormByte::ByteSize{0}, 0));
	ASSERT_TRUE(out.Open());
	FIFO first = FromText("AB");
	ASSERT_EQUAL(ToString(Status::Ok), ToString(out.Write(first).status));
	BufferedFileWriter moved(std::move(out));
	ASSERT_FALSE(static_cast<bool>(out));
	ASSERT_EQUAL(ToString(State::Unavailable), ToString(out.State()));
	ASSERT_EQUAL(StormByte::ByteSize{2}, moved.Tell());
	FIFO rest = FromText("C");
	ASSERT_EQUAL(ToString(Status::Ok), ToString(moved.Write(rest).status));
	ASSERT_EQUAL(StormByte::ByteSize{3}, moved.Tell());
	ASSERT_TRUE(moved.Close());
	ASSERT_EQUAL(std::string("ABC"), Slurp(path));
	std::filesystem::remove(path);
	RETURN_TEST(0);
}

// -------------------
// Setup
// -------------------

int test_explicit_chunk_survives_setup() {
	const auto path = Scratch("keep");
	std::filesystem::remove(path);
	BufferedFileWriter out(Loc(path), P(StormByte::ByteSize{8}, 2));
	ASSERT_TRUE(out.Open());
	ASSERT_EQUAL(StormByte::ByteSize{8}, out.WriteChunk());
	ASSERT_EQUAL(static_cast<std::size_t>(2), out.BackPressure());
	ASSERT_EQUAL(StormByte::ByteSize{0}, out.MaxMemory());
	ASSERT_TRUE(out.Close());
	std::filesystem::remove(path);
	RETURN_TEST(0);
}

int test_path_only_setup_sets_device_knobs() {
	const auto path = Scratch("probe");
	std::filesystem::remove(path);
	BufferedFileWriter out(Loc(path));
	ASSERT_EQUAL(StormByte::ByteSize{0}, out.WriteChunk());
	ASSERT_EQUAL(static_cast<std::size_t>(0), out.BackPressure());
	ASSERT_TRUE(out.Open());
	ASSERT_TRUE(out.WriteChunk() >= StormByte::ByteSize{16ull * 1024ull});
	ASSERT_TRUE(out.WriteChunk() <= StormByte::ByteSize{1024ull * 1024ull});
	ASSERT_EQUAL(static_cast<std::size_t>(4), out.BackPressure());
	ASSERT_EQUAL(StormByte::ByteSize{1024ull * 1024ull}, out.MaxMemory());
	ASSERT_TRUE(out.Close());
	std::filesystem::remove(path);
	RETURN_TEST(0);
}

int test_path_only_short_write_holds_until_flush() {
	const auto path = Scratch("phold");
	std::filesystem::remove(path);
	BufferedFileWriter out(Loc(path));
	ASSERT_TRUE(out.Open());
	FIFO src = FromText("ZYX");
	ASSERT_EQUAL(ToString(Status::Ok), ToString(out.Write(src).status));
	ASSERT_EQUAL(StormByte::ByteSize{3}, out.Tell());
	ASSERT_EQUAL(StormByte::ByteSize{3}, out.Dirty());
	ASSERT_EQUAL(std::string(""), Slurp(path));
	ASSERT_EQUAL(ToString(Status::Ok), ToString(out.Flush().status));
	ASSERT_EQUAL(StormByte::ByteSize{0}, out.Dirty());
	ASSERT_EQUAL(std::string("ZYX"), Slurp(path));
	ASSERT_TRUE(out.Close());
	std::filesystem::remove(path);
	RETURN_TEST(0);
}

// -------------------
// Ring
// -------------------

int test_backpressure_tryagain_atomic() {
	const auto path = Scratch("bp");
	std::filesystem::remove(path);
	BufferedFileWriter out(Loc(path), P(StormByte::ByteSize{4}, 1));
	ASSERT_TRUE(out.Open());
	FIFO first = FromText("AB");
	ASSERT_EQUAL(ToString(Status::Ok), ToString(out.Write(first).status));
	ASSERT_EQUAL(StormByte::ByteSize{2}, out.Tell());
	ASSERT_EQUAL(StormByte::ByteSize{2}, out.Dirty());
	FIFO second = FromText("CDEFGH");
	const auto blocked = out.Write(second);
	ASSERT_EQUAL(ToString(Status::TryAgain), ToString(blocked.status));
	ASSERT_EQUAL(StormByte::ByteSize{0}, blocked.count);
	ASSERT_EQUAL(StormByte::ByteSize{6}, second.Available());
	ASSERT_EQUAL(StormByte::ByteSize{2}, out.Tell());
	ASSERT_EQUAL(StormByte::ByteSize{2}, out.Dirty());
	ASSERT_TRUE(out.Close());
	std::filesystem::remove(path);
	RETURN_TEST(0);
}

int test_chunk_holds_until_flush() {
	const auto path = Scratch("hold");
	std::filesystem::remove(path);
	BufferedFileWriter out(Loc(path), P(StormByte::ByteSize{8}, 4));
	ASSERT_TRUE(out.Open());
	FIFO src = FromText("ABC");
	ASSERT_EQUAL(ToString(Status::Ok), ToString(out.Write(src).status));
	ASSERT_EQUAL(StormByte::ByteSize{3}, out.Dirty());
	ASSERT_EQUAL(StormByte::ByteSize{3}, out.Tell());
	ASSERT_EQUAL(std::string(""), Slurp(path));
	ASSERT_EQUAL(ToString(Status::Ok), ToString(out.Flush().status));
	ASSERT_EQUAL(StormByte::ByteSize{0}, out.Dirty());
	ASSERT_EQUAL(StormByte::ByteSize{3}, out.Tell());
	ASSERT_EQUAL(std::string("ABC"), Slurp(path));
	ASSERT_TRUE(out.Close());
	std::filesystem::remove(path);
	RETURN_TEST(0);
}

int test_close_flushes_dirty() {
	const auto path = Scratch("cflush");
	std::filesystem::remove(path);
	{
		BufferedFileWriter out(Loc(path), P(StormByte::ByteSize{8}, 4));
		ASSERT_TRUE(out.Open());
		FIFO src = FromText("XY");
		ASSERT_EQUAL(ToString(Status::Ok), ToString(out.Write(src).status));
		ASSERT_EQUAL(StormByte::ByteSize{2}, out.Dirty());
		ASSERT_TRUE(out.Close());
	}
	ASSERT_EQUAL(std::string("XY"), Slurp(path));
	std::filesystem::remove(path);
	RETURN_TEST(0);
}

int test_full_chunk_clears_dirty() {
	const auto path = Scratch("drain");
	std::filesystem::remove(path);
	BufferedFileWriter out(Loc(path), P(StormByte::ByteSize{4}, 4));
	ASSERT_TRUE(out.Open());
	FIFO src = FromText("ABCD");
	ASSERT_EQUAL(ToString(Status::Ok), ToString(out.Write(src).status));
	ASSERT_EQUAL(StormByte::ByteSize{4}, out.Tell());
	ASSERT_TRUE(WaitDirtyZero(out));
	ASSERT_EQUAL(StormByte::ByteSize{0}, out.Dirty());
	ASSERT_EQUAL(ToString(Status::Ok), ToString(out.Flush().status));
	ASSERT_EQUAL(std::string("ABCD"), Slurp(path));
	ASSERT_TRUE(out.Close());
	std::filesystem::remove(path);
	RETURN_TEST(0);
}

int test_truncate_drops_dirty() {
	const auto path = Scratch("tdrop");
	std::filesystem::remove(path);
	BufferedFileWriter out(Loc(path), P(StormByte::ByteSize{8}, 4));
	ASSERT_TRUE(out.Open());
	FIFO src = FromText("NOPE");
	ASSERT_EQUAL(ToString(Status::Ok), ToString(out.Write(src).status));
	ASSERT_EQUAL(StormByte::ByteSize{4}, out.Dirty());
	ASSERT_EQUAL(ToString(Status::Ok), ToString(out.Truncate().status));
	ASSERT_EQUAL(StormByte::ByteSize{0}, out.Dirty());
	ASSERT_EQUAL(StormByte::ByteSize{0}, out.Tell());
	ASSERT_TRUE(out.Close());
	ASSERT_EQUAL(std::string(""), Slurp(path));
	std::filesystem::remove(path);
	RETURN_TEST(0);
}

int test_truncate_zeros_file_and_tell() {
	const auto path = Scratch("trunc");
	std::filesystem::remove(path);
	BufferedFileWriter out(Loc(path), P(StormByte::ByteSize{0}, 0));
	ASSERT_TRUE(out.Open());
	FIFO src = FromText("XYZ");
	ASSERT_EQUAL(ToString(Status::Ok), ToString(out.Write(src).status));
	ASSERT_EQUAL(StormByte::ByteSize{3}, out.Tell());
	ASSERT_EQUAL(ToString(Status::Ok), ToString(out.Truncate().status));
	ASSERT_EQUAL(StormByte::ByteSize{0}, out.Tell());
	ASSERT_EQUAL(StormByte::ByteSize{0}, out.Dirty());
	ASSERT_EQUAL(std::string(""), Slurp(path));
	FIFO again = FromText("OK");
	ASSERT_EQUAL(ToString(Status::Ok), ToString(out.Write(again).status));
	ASSERT_EQUAL(StormByte::ByteSize{2}, out.Tell());
	ASSERT_TRUE(out.Close());
	ASSERT_EQUAL(std::string("OK"), Slurp(path));
	std::filesystem::remove(path);
	RETURN_TEST(0);
}

int test_two_chunks_then_flush() {
	const auto path = Scratch("two");
	std::filesystem::remove(path);
	BufferedFileWriter out(Loc(path), P(StormByte::ByteSize{2}, 4));
	ASSERT_TRUE(out.Open());
	FIFO a = FromText("AB");
	FIFO b = FromText("CD");
	ASSERT_EQUAL(ToString(Status::Ok), ToString(out.Write(a).status));
	ASSERT_EQUAL(StormByte::ByteSize{2}, out.Tell());
	ASSERT_TRUE(WaitDirtyZero(out));
	ASSERT_EQUAL(ToString(Status::Ok), ToString(out.Write(b).status));
	ASSERT_EQUAL(StormByte::ByteSize{4}, out.Tell());
	ASSERT_TRUE(WaitDirtyZero(out));
	ASSERT_EQUAL(ToString(Status::Ok), ToString(out.Flush().status));
	ASSERT_EQUAL(std::string("ABCD"), Slurp(path));
	ASSERT_TRUE(out.Close());
	std::filesystem::remove(path);
	RETURN_TEST(0);
}

// -------------------
// Seek
// -------------------

int test_seek_before_start_fails() {
	const auto path = Scratch("neg");
	std::filesystem::remove(path);
	BufferedFileWriter out(Loc(path), P(StormByte::ByteSize{0}, 0));
	ASSERT_TRUE(out.Open());
	FIFO src = FromText("HI");
	ASSERT_EQUAL(ToString(Status::Ok), ToString(out.Write(src).status));
	ASSERT_EQUAL(ToString(Status::Failed), ToString(out.Seek(-1, Position::Absolute).status));
	ASSERT_EQUAL(ToString(Status::Failed), ToString(out.Seek(-3, Position::Relative).status));
	ASSERT_EQUAL(StormByte::ByteSize{2}, out.Tell());
	ASSERT_TRUE(out.Close());
	ASSERT_EQUAL(std::string("HI"), Slurp(path));
	std::filesystem::remove(path);
	RETURN_TEST(0);
}

int test_seek_keeps_dirty_then_patches() {
	const auto path = Scratch("sdirty");
	std::filesystem::remove(path);
	BufferedFileWriter out(Loc(path), P(StormByte::ByteSize{8}, 4, StormByte::ByteSize{64}));
	ASSERT_TRUE(out.Open());
	FIFO fill = FromText("XXXX");
	ASSERT_EQUAL(ToString(Status::Ok), ToString(out.Write(fill).status));
	ASSERT_EQUAL(StormByte::ByteSize{4}, out.Dirty());
	ASSERT_EQUAL(StormByte::ByteSize{4}, out.Size());
	ASSERT_EQUAL(ToString(Status::Ok), ToString(out.Seek(1, Position::Absolute).status));
	ASSERT_EQUAL(StormByte::ByteSize{4}, out.Dirty());
	ASSERT_EQUAL(StormByte::ByteSize{1}, out.Tell());
	ASSERT_EQUAL(std::string(""), Slurp(path));
	FIFO mid = FromText("YZ");
	ASSERT_EQUAL(ToString(Status::Ok), ToString(out.Write(mid).status));
	ASSERT_TRUE(out.Close());
	ASSERT_EQUAL(std::string("XYZX"), Slurp(path));
	std::filesystem::remove(path);
	RETURN_TEST(0);
}

int test_seek_patch_direct() {
	const auto path = Scratch("patch");
	std::filesystem::remove(path);
	BufferedFileWriter out(Loc(path), P(StormByte::ByteSize{0}, 0));
	ASSERT_TRUE(out.Open());
	FIFO fill = FromText("XXXXYYYY");
	ASSERT_EQUAL(ToString(Status::Ok), ToString(out.Write(fill).status));
	ASSERT_EQUAL(StormByte::ByteSize{8}, out.Tell());
	ASSERT_EQUAL(StormByte::ByteSize{0}, out.Dirty());
	ASSERT_EQUAL(StormByte::ByteSize{8}, out.Size());
	ASSERT_EQUAL(ToString(Status::Ok), ToString(out.Seek(0, Position::Absolute).status));
	ASSERT_EQUAL(StormByte::ByteSize{0}, out.Tell());
	FIFO ab = FromText("AB");
	ASSERT_EQUAL(ToString(Status::Ok), ToString(out.Write(ab).status));
	ASSERT_EQUAL(StormByte::ByteSize{2}, out.Tell());
	ASSERT_EQUAL(ToString(Status::Ok), ToString(out.Seek(4, Position::Absolute).status));
	ASSERT_EQUAL(StormByte::ByteSize{4}, out.Tell());
	FIFO cd = FromText("CD");
	ASSERT_EQUAL(ToString(Status::Ok), ToString(out.Write(cd).status));
	ASSERT_EQUAL(StormByte::ByteSize{6}, out.Tell());
	ASSERT_EQUAL(StormByte::ByteSize{8}, out.Size());
	ASSERT_TRUE(out.Close());
	ASSERT_EQUAL(std::string("ABXXCDYY"), Slurp(path));
	std::filesystem::remove(path);
	RETURN_TEST(0);
}

int test_seek_relative() {
	const auto path = Scratch("rel");
	std::filesystem::remove(path);
	BufferedFileWriter out(Loc(path), P(StormByte::ByteSize{0}, 0));
	ASSERT_TRUE(out.Open());
	FIFO fill = FromText("01234567");
	ASSERT_EQUAL(ToString(Status::Ok), ToString(out.Write(fill).status));
	ASSERT_EQUAL(ToString(Status::Ok), ToString(out.Seek(-4, Position::Relative).status));
	ASSERT_EQUAL(StormByte::ByteSize{4}, out.Tell());
	FIFO mid = FromText("AB");
	ASSERT_EQUAL(ToString(Status::Ok), ToString(out.Write(mid).status));
	ASSERT_TRUE(out.Close());
	ASSERT_EQUAL(std::string("0123AB67"), Slurp(path));
	std::filesystem::remove(path);
	RETURN_TEST(0);
}

int test_seek_then_extend() {
	const auto path = Scratch("ext");
	std::filesystem::remove(path);
	BufferedFileWriter out(Loc(path), P(StormByte::ByteSize{0}, 0));
	ASSERT_TRUE(out.Open());
	FIFO head = FromText("AB");
	ASSERT_EQUAL(ToString(Status::Ok), ToString(out.Write(head).status));
	ASSERT_EQUAL(ToString(Status::Ok), ToString(out.Seek(2, Position::Absolute).status));
	FIFO tail = FromText("CDEF");
	ASSERT_EQUAL(ToString(Status::Ok), ToString(out.Write(tail).status));
	ASSERT_EQUAL(StormByte::ByteSize{6}, out.Tell());
	ASSERT_EQUAL(StormByte::ByteSize{6}, out.Size());
	ASSERT_TRUE(out.Close());
	ASSERT_EQUAL(std::string("ABCDEF"), Slurp(path));
	std::filesystem::remove(path);
	RETURN_TEST(0);
}

int test_seek_without_open_fails() {
	BufferedFileWriter out(Loc(Scratch("closed")), P(StormByte::ByteSize{0}, 0));
	ASSERT_EQUAL(ToString(Status::Failed), ToString(out.Seek(0, Position::Absolute).status));
	RETURN_TEST(0);
}

int test_size_counts_dirty() {
	const auto path = Scratch("szdirty");
	std::filesystem::remove(path);
	BufferedFileWriter out(Loc(path), P(StormByte::ByteSize{8}, 4));
	ASSERT_TRUE(out.Open());
	FIFO src = FromText("ABC");
	ASSERT_EQUAL(ToString(Status::Ok), ToString(out.Write(src).status));
	ASSERT_EQUAL(StormByte::ByteSize{3}, out.Tell());
	ASSERT_EQUAL(StormByte::ByteSize{3}, out.Dirty());
	ASSERT_EQUAL(StormByte::ByteSize{3}, out.Size());
	ASSERT_EQUAL(std::string(""), Slurp(path));
	ASSERT_EQUAL(ToString(Status::Ok), ToString(out.Flush().status));
	ASSERT_EQUAL(StormByte::ByteSize{0}, out.Dirty());
	ASSERT_EQUAL(StormByte::ByteSize{3}, out.Tell());
	ASSERT_EQUAL(StormByte::ByteSize{3}, out.Size());
	ASSERT_EQUAL(std::string("ABC"), Slurp(path));
	ASSERT_TRUE(out.Close());
	std::filesystem::remove(path);
	RETURN_TEST(0);
}

// -------------------
// Session
// -------------------

int test_close_then_reopen_appends() {
	const auto path = Scratch("reopen");
	std::filesystem::remove(path);
	{
		BufferedFileWriter out(Loc(path), P(StormByte::ByteSize{0}, 0));
		ASSERT_TRUE(out.Open());
		FIFO a = FromText("AB");
		ASSERT_EQUAL(ToString(Status::Ok), ToString(out.Write(a).status));
		ASSERT_EQUAL(StormByte::ByteSize{2}, out.Tell());
		ASSERT_TRUE(out.Close());
		ASSERT_EQUAL(ToString(State::Unavailable), ToString(out.State()));
		ASSERT_EQUAL(StormByte::ByteSize{0}, out.Dirty());
		ASSERT_TRUE(out.Close());
		ASSERT_TRUE(out.Open());
		ASSERT_EQUAL(StormByte::ByteSize{0}, out.Tell());
		ASSERT_EQUAL(ToString(Status::Ok),
			ToString(out.Seek(static_cast<std::ptrdiff_t>(out.Size()), Position::Absolute).status));
		FIFO b = FromText("CD");
		ASSERT_EQUAL(ToString(Status::Ok), ToString(out.Write(b).status));
		ASSERT_EQUAL(StormByte::ByteSize{4}, out.Tell());
		ASSERT_TRUE(out.Close());
	}
	ASSERT_EQUAL(std::string("ABCD"), Slurp(path));
	std::filesystem::remove(path);
	RETURN_TEST(0);
}

int test_ctor_unavailable() {
	const auto path = Scratch("ctor");
	BufferedFileWriter out(Loc(path));
	ASSERT_EQUAL(ToString(State::Unavailable), ToString(out.State()));
	ASSERT_FALSE(static_cast<bool>(out));
	ASSERT_FALSE(out.IsOpen());
	ASSERT_EQUAL(StormByte::ByteSize{0}, out.Tell());
	ASSERT_EQUAL(StormByte::ByteSize{0}, out.Dirty());
	ASSERT_EQUAL(StormByte::ByteSize{0}, out.WriteChunk());
	ASSERT_EQUAL(static_cast<std::size_t>(0), out.BackPressure());
	ASSERT_EQUAL(Loc(path), out.Path());
	RETURN_TEST(0);
}

int test_open_creates_and_not_idempotent() {
	const auto path = Scratch("create");
	std::filesystem::remove(path);
	BufferedFileWriter out(Loc(path));
	ASSERT_TRUE(out.Open());
	ASSERT_EQUAL(ToString(State::Idle), ToString(out.State()));
	ASSERT_TRUE(static_cast<bool>(out));
	ASSERT_TRUE(std::filesystem::exists(path));
	ASSERT_EQUAL(StormByte::ByteSize{0}, out.Tell());
	ASSERT_FALSE(out.Open());
	ASSERT_EQUAL(ToString(State::Idle), ToString(out.State()));
	ASSERT_TRUE(out.Close());
	std::filesystem::remove(path);
	RETURN_TEST(0);
}

int test_open_directory() {
	BufferedFileWriter out(Loc(CurrentFileDirectory / "files"));
	ASSERT_FALSE(out.Open());
	ASSERT_EQUAL(ToString(State::Directory), ToString(out.State()));
	RETURN_TEST(0);
}

int test_open_missing_parent() {
	BufferedFileWriter out(Loc(std::filesystem::path("no_such_sbw_dir") / "x.bin"));
	ASSERT_FALSE(out.Open());
	ASSERT_EQUAL(ToString(State::Missing), ToString(out.State()));
	ASSERT_FALSE(static_cast<bool>(out));
	RETURN_TEST(0);
}

int test_rewind_without_open_fails() {
	BufferedFileWriter out(Loc(Scratch("rew")));
	ASSERT_FALSE(out.Rewind());
	ASSERT_EQUAL(ToString(State::Unavailable), ToString(out.State()));
	RETURN_TEST(0);
}

int test_write_before_open_leaves_src() {
	BufferedFileWriter out(Loc(Scratch("before")));
	FIFO src = FromText("KEEP");
	const auto written = out.Write(src);
	ASSERT_EQUAL(ToString(Status::Failed), ToString(written.status));
	ASSERT_EQUAL(StormByte::ByteSize{0}, written.count);
	ASSERT_EQUAL(StormByte::ByteSize{4}, src.Available());
	ASSERT_EQUAL(StormByte::ByteSize{0}, out.Tell());
	RETURN_TEST(0);
}

// -------------------
// Stress
// -------------------

int test_stress_close_after_evict_seek() {
	for (const auto& k : kKnobs) {
		const auto path = Scratch((std::string("clsev-") + k.tag).c_str());
		std::filesystem::remove(path);
		BufferedFileWriter out = MakeWriter(path, k);
		ASSERT_TRUE(out.Open());
		ASSERT_EQUAL(0, WriteAll(out, HexSlice(0, 8192)));
		ASSERT_EQUAL(0, SeekExpectTell(out, 100, Position::Absolute, 100));
		ASSERT_EQUAL(0, WriteAll(out, std::string("ZZ")));
		DumpTelemetry((std::string("clsev-") + k.tag).c_str(), out);
		ASSERT_TRUE(out.Close());
		const auto* io = IoW(out);
		ASSERT_TRUE(io != nullptr);
		ASSERT_EQUAL(StormByte::ByteSize{0}, io->Dirty());
		std::string expect = HexSlice(0, 8192);
		expect.replace(100, 2, "ZZ");
		ASSERT_EQUAL(expect, Slurp(path));
		ASSERT_EQUAL(StormByte::ByteSize{8192}, io->HighWater());
		ASSERT_EQUAL(io->Materialized(), io->HighWater());
		std::filesystem::remove(path);
	}
	RETURN_TEST(0);
}

int test_stress_double_flush_seek_back() {
	for (const auto& k : kKnobs) {
		const auto path = Scratch((std::string("dflush-") + k.tag).c_str());
		std::filesystem::remove(path);
		BufferedFileWriter out = MakeWriter(path, k);
		ASSERT_TRUE(out.Open());
		const std::string body = HexSlice(0, 8192);
		ASSERT_EQUAL(0, WriteAll(out, body));
		ASSERT_EQUAL(ToString(Status::Ok), ToString(out.Flush().status));
		ASSERT_EQUAL(0, SeekExpectTell(out, 16, Position::Absolute, 16));
		ASSERT_EQUAL(0, WriteAll(out, std::string("XXXX")));
		ASSERT_EQUAL(ToString(Status::Ok), ToString(out.Flush().status));
		DumpTelemetry((std::string("dflush-") + k.tag).c_str(), out);
		const auto* io = IoW(out);
		ASSERT_TRUE(io != nullptr);
		ASSERT_EQUAL(StormByte::ByteSize{0}, io->Dirty());
		ASSERT_EQUAL(StormByte::ByteSize{8192}, io->HighWater());
		ASSERT_TRUE(out.Close());
		std::string expect = body;
		expect.replace(16, 4, "XXXX");
		ASSERT_EQUAL(expect, Slurp(path));
		std::filesystem::remove(path);
	}
	RETURN_TEST(0);
}

int test_stress_evict_multi_page() {
	for (const auto& k : kKnobs) {
		const auto path = Scratch((std::string("evictm-") + k.tag).c_str());
		std::filesystem::remove(path);
		BufferedFileWriter out = MakeWriter(path, k);
		ASSERT_TRUE(out.Open());
		ASSERT_EQUAL(0, WriteAll(out, std::string("AAAAAAAA")));
		ASSERT_EQUAL(0, SeekExpectTell(out, 4096, Position::Absolute, 4096));
		ASSERT_EQUAL(0, WriteAll(out, std::string("BBBBBBBB")));
		ASSERT_EQUAL(0, SeekExpectTell(out, 8192, Position::Absolute, 8192));
		ASSERT_EQUAL(0, WriteAll(out, std::string("CCCCCCCC")));
		ASSERT_EQUAL(0, SeekExpectTell(out, 16384, Position::Absolute, 16384));
		ASSERT_EQUAL(0, WriteAll(out, std::string("DDDDDDDD")));
		DumpTelemetry((std::string("evictm-") + k.tag).c_str(), out);
		ASSERT_TRUE(out.Close());
		const auto* io = IoW(out);
		ASSERT_TRUE(io != nullptr);
		ASSERT_EQUAL(StormByte::ByteSize{0}, io->Dirty());
		ASSERT_EQUAL(io->Materialized(), io->HighWater());
		const std::string got = Slurp(path);
		ASSERT_EQUAL(static_cast<std::size_t>(16392), got.size());
		ASSERT_EQUAL(std::string("AAAAAAAA"), got.substr(0, 8));
		ASSERT_EQUAL(std::string("BBBBBBBB"), got.substr(4096, 8));
		ASSERT_EQUAL(std::string("CCCCCCCC"), got.substr(8192, 8));
		ASSERT_EQUAL(std::string("DDDDDDDD"), got.substr(16384, 8));
		std::filesystem::remove(path);
	}
	RETURN_TEST(0);
}

int test_stress_far_future_hole() {
	constexpr std::size_t kGap = 8u * 1024u * 1024u;
	for (const auto& k : kKnobs) {
		const auto path = Scratch((std::string("far-") + k.tag).c_str());
		std::filesystem::remove(path);
		BufferedFileWriter out = MakeWriter(path, k);
		ASSERT_TRUE(out.Open());
		ASSERT_EQUAL(0, WriteAll(out, std::string("HEADHEAD")));
		ASSERT_EQUAL(0, SeekExpectTell(out, static_cast<std::ptrdiff_t>(kGap), Position::Absolute, kGap));
		ASSERT_EQUAL(0, WriteAll(out, std::string("TAILTAIL")));
		ASSERT_EQUAL(0, SeekExpectTell(out, 0, Position::Absolute, 0));
		ASSERT_EQUAL(0, WriteAll(out, std::string("NOSE")));
		DumpTelemetry((std::string("far-") + k.tag).c_str(), out);
		ASSERT_TRUE(out.Close());
		const std::string got = Slurp(path);
		ASSERT_EQUAL(kGap + 8, got.size());
		ASSERT_EQUAL(std::string("NOSEHEAD"), got.substr(0, 8));
		ASSERT_EQUAL(std::string("TAILTAIL"), got.substr(kGap, 8));
		ASSERT_EQUAL('\0', got[8]);
		ASSERT_EQUAL('\0', got[kGap - 1]);
		const auto* io = IoW(out);
		ASSERT_TRUE(io != nullptr);
		ASSERT_EQUAL(StormByte::ByteSize{kGap + 8}, io->HighWater());
		ASSERT_EQUAL(io->Materialized(), io->HighWater());
		std::filesystem::remove(path);
	}
	RETURN_TEST(0);
}

int test_stress_patch_evicted_and_dirty() {
	for (const auto& k : kKnobs) {
		const auto path = Scratch((std::string("patev-") + k.tag).c_str());
		std::filesystem::remove(path);
		BufferedFileWriter out = MakeWriter(path, k);
		ASSERT_TRUE(out.Open());
		const std::string body = HexSlice(0, 16384);
		ASSERT_EQUAL(0, WriteAll(out, body));
		ASSERT_EQUAL(ToString(Status::Ok), ToString(out.Flush().status));
		ASSERT_EQUAL(0, SeekExpectTell(out, 32, Position::Absolute, 32));
		ASSERT_EQUAL(0, WriteAll(out, std::string("EVICTED!")));
		ASSERT_EQUAL(0, SeekExpectTell(out, 12000, Position::Absolute, 12000));
		ASSERT_EQUAL(0, WriteAll(out, std::string("STILDIRT")));
		DumpTelemetry((std::string("patev-") + k.tag).c_str(), out);
		ASSERT_TRUE(out.Close());
		std::string expect = body;
		expect.replace(32, 8, "EVICTED!");
		expect.replace(12000, 8, "STILDIRT");
		const std::string got = Slurp(path);
		DumpMismatch((std::string("patev-") + k.tag).c_str(), expect, got);
		ASSERT_EQUAL(expect, got);
		const auto* io = IoW(out);
		ASSERT_TRUE(io != nullptr);
		ASSERT_EQUAL(StormByte::ByteSize{16384}, io->HighWater());
		ASSERT_EQUAL(io->Materialized(), io->HighWater());
		std::filesystem::remove(path);
	}
	RETURN_TEST(0);
}

int test_stress_reopen_second_session() {
	for (const auto& k : kKnobs) {
		const auto path = Scratch((std::string("reopen-") + k.tag).c_str());
		std::filesystem::remove(path);
		{
			BufferedFileWriter out = MakeWriter(path, k);
			ASSERT_TRUE(out.Open());
			ASSERT_EQUAL(0, WriteAll(out, std::string("AB")));
			ASSERT_TRUE(out.Close());
			ASSERT_EQUAL(std::string("AB"), Slurp(path));
		}
		{
			BufferedFileWriter out = MakeWriter(path, k);
			ASSERT_TRUE(out.Open());
			ASSERT_EQUAL(StormByte::ByteSize{0}, out.Tell());
			ASSERT_EQUAL(ToString(Status::Ok),
				ToString(out.Seek(static_cast<std::ptrdiff_t>(out.Size()), Position::Absolute).status));
			ASSERT_EQUAL(0, WriteAll(out, std::string("CD")));
			DumpTelemetry((std::string("reopen-") + k.tag).c_str(), out);
			ASSERT_TRUE(out.Close());
			ASSERT_EQUAL(std::string("ABCD"), Slurp(path));
		}
		std::filesystem::remove(path);
	}
	RETURN_TEST(0);
}

int test_stress_ring_seek_no_flush() {
	const WriterKnob rings[] = {
		{ StormByte::ByteSize{8}, StormByte::ByteSize{0}, 16, "ring" },
		{ StormByte::ByteSize{8}, StormByte::ByteSize{64}, 16, "ring-pages" },
		{ StormByte::ByteSize{0}, StormByte::ByteSize{0}, 0, "direct" },
	};
	for (const auto& k : rings) {
		const auto path = Scratch((std::string("rseek-") + k.tag).c_str());
		std::filesystem::remove(path);
		BufferedFileWriter out = MakeWriter(path, k);
		ASSERT_TRUE(out.Open());
		ASSERT_EQUAL(0, WriteAll(out, std::string("0123456789ABCDEF")));
		ASSERT_EQUAL(0, SeekExpectTell(out, 4, Position::Absolute, 4));
		ASSERT_EQUAL(0, WriteAll(out, std::string("xxxx")));
		ASSERT_EQUAL(0, WriteAll(out, std::string("yyyy")));
		DumpTelemetry((std::string("rseek-") + k.tag).c_str(), out);
		ASSERT_TRUE(out.Close());
		ASSERT_EQUAL(std::string("0123xxxxyyyyCDEF"), Slurp(path));
		const auto* io = IoW(out);
		ASSERT_TRUE(io != nullptr);
		ASSERT_EQUAL(StormByte::ByteSize{16}, io->HighWater());
		ASSERT_EQUAL(io->Materialized(), io->HighWater());
		std::filesystem::remove(path);
	}
	RETURN_TEST(0);
}

// -------------------
// Telemetry
// -------------------

int test_telemetry_ctor_is_zero() {
	const auto path = Scratch("tzero");
	std::filesystem::remove(path);
	BufferedFileWriter out(Loc(path), P(StormByte::ByteSize{0}, 0));
	DumpTelemetry("ctor", out);
	const auto tel = out.Telemetry();
	const auto* io = IoW(out);
	ASSERT_TRUE(static_cast<bool>(tel));
	ASSERT_TRUE(io != nullptr);
	ASSERT_EQUAL(StormByte::ByteSize{0}, tel->Accepted());
	ASSERT_EQUAL(StormByte::ByteSize{0}, io->Materialized());
	ASSERT_EQUAL(StormByte::ByteSize{0}, io->HighWater());
	ASSERT_EQUAL(static_cast<std::size_t>(0), io->SeekLogical());
	std::filesystem::remove(path);
	RETURN_TEST(0);
}

int test_telemetry_direct_write_counts_accepted() {
	const auto path = Scratch("tdir");
	std::filesystem::remove(path);
	BufferedFileWriter out(Loc(path), P(StormByte::ByteSize{0}, 0));
	ASSERT_TRUE(out.Open());
	FIFO src = FromText("HELLO");
	ASSERT_EQUAL(ToString(Status::Ok), ToString(out.Write(src).status));
	DumpTelemetry("direct", out);
	const auto tel = out.Telemetry();
	const auto* io = IoW(out);
	ASSERT_TRUE(static_cast<bool>(tel));
	ASSERT_TRUE(io != nullptr);
	ASSERT_EQUAL(StormByte::ByteSize{5}, tel->Accepted());
	ASSERT_EQUAL(StormByte::ByteSize{5}, io->Direct());
	ASSERT_TRUE(out.Close());
	ASSERT_EQUAL(StormByte::ByteSize{5}, io->Materialized());
	ASSERT_EQUAL(StormByte::ByteSize{5}, io->HighWater());
	ASSERT_EQUAL(std::string("HELLO"), Slurp(path));
	std::filesystem::remove(path);
	RETURN_TEST(0);
}

int test_telemetry_fake_seek_epoch() {
	const auto path = Scratch("tepoch");
	std::filesystem::remove(path);
	BufferedFileWriter out(Loc(path), P(StormByte::ByteSize{4096}, 4, StormByte::ByteSize{256ull * 1024ull}));
	ASSERT_TRUE(out.Open());
	ASSERT_EQUAL(0, WriteExpect(out, HexSlice(0, 8192)));
	DumpTelemetry("tel-fill", out);
	ASSERT_EQUAL(0, SeekExpectTell(out, 16, Position::Absolute, 16));
	DumpTelemetry("tel-seek", out);
	ASSERT_EQUAL(0, WriteExpect(out, std::string("WWWW")));
	DumpTelemetry("tel-patch", out);
	ASSERT_EQUAL(0, SeekExpectTell(out, 8192, Position::Absolute, 8192));
	DumpTelemetry("tel-front", out);
	ASSERT_TRUE(out.Close());
	DumpTelemetry("tel-closed", out);
	const auto tel = out.Telemetry();
	const auto* io = IoW(out);
	ASSERT_TRUE(static_cast<bool>(tel));
	ASSERT_TRUE(io != nullptr);
	ASSERT_EQUAL(StormByte::ByteSize{0}, io->Dirty());
	ASSERT_EQUAL(tel->Accepted(), io->Behind() + io->Direct());
	ASSERT_EQUAL(StormByte::ByteSize{8192}, io->HighWater());
	ASSERT_EQUAL(io->Materialized(), io->HighWater());
	std::string expect = HexSlice(0, 8192);
	expect.replace(16, 4, "WWWW");
	ASSERT_EQUAL(expect, Slurp(path));
	std::filesystem::remove(path);
	RETURN_TEST(0);
}

int test_telemetry_island_and_hole() {
	const auto path = Scratch("tisland");
	std::filesystem::remove(path);
	BufferedFileWriter out(Loc(path), P(StormByte::ByteSize{4096}, 4, StormByte::ByteSize{256ull * 1024ull}));
	ASSERT_TRUE(out.Open());
	ASSERT_EQUAL(0, WriteExpect(out, std::string("AA")));
	ASSERT_EQUAL(0, SeekExpectTell(out, 4096, Position::Absolute, 4096));
	ASSERT_EQUAL(0, WriteExpect(out, std::string("BB")));
	DumpTelemetry("tel-island", out);
	ASSERT_TRUE(out.Close());
	DumpTelemetry("tel-island-closed", out);
	const std::string got = Slurp(path);
	ASSERT_EQUAL(static_cast<std::size_t>(4098), got.size());
	ASSERT_EQUAL(std::string("AA"), got.substr(0, 2));
	ASSERT_EQUAL(std::string("BB"), got.substr(4096, 2));
	const auto* io = IoW(out);
	ASSERT_TRUE(io != nullptr);
	ASSERT_EQUAL(StormByte::ByteSize{4098}, io->HighWater());
	ASSERT_EQUAL(io->Materialized(), io->HighWater());
	std::filesystem::remove(path);
	RETURN_TEST(0);
}

int test_telemetry_pressure_mixed_seeks() {
	const auto path = Scratch("tmix");
	std::filesystem::remove(path);
	BufferedFileWriter out(Loc(path), P(StormByte::ByteSize{4096}, 8));
	ASSERT_TRUE(out.Open());

	constexpr std::size_t kBytes = 512u * 1024u;
	std::string blob(kBytes, '\0');
	for (std::size_t i = 0; i < kBytes; ++i)
		blob[i] = static_cast<char>(i & 0xFF);

	std::size_t off = 0;
	while (off < kBytes) {
		const std::size_t n = (kBytes - off) < 3072u ? (kBytes - off) : 3072u;
		FIFO piece = FromText(blob.substr(off, n));
		auto written = out.Write(piece);
		if (written.status == Status::TryAgain) {
			ASSERT_EQUAL(ToString(Status::Ok), ToString(out.Flush().status));
			written = out.Write(piece);
		}
		ASSERT_EQUAL(ToString(Status::Ok), ToString(written.status));
		off += n;
	}
	DumpTelemetry("seq-512k", out);

	ASSERT_EQUAL(ToString(Status::Ok), ToString(out.Seek(0, Position::Absolute).status));
	FIFO head = FromText(std::string(8192, 'H'));
	{
		auto written = out.Write(head);
		if (written.status == Status::TryAgain) {
			ASSERT_EQUAL(ToString(Status::Ok), ToString(out.Flush().status));
			written = out.Write(head);
		}
		ASSERT_EQUAL(ToString(Status::Ok), ToString(written.status));
	}
	DumpTelemetry("seek0-8k", out);

	ASSERT_EQUAL(ToString(Status::Ok),
		ToString(out.Seek(static_cast<std::ptrdiff_t>(kBytes / 2), Position::Absolute).status));
	FIFO mid = FromText(std::string(8192, 'M'));
	{
		auto written = out.Write(mid);
		if (written.status == Status::TryAgain) {
			ASSERT_EQUAL(ToString(Status::Ok), ToString(out.Flush().status));
			written = out.Write(mid);
		}
		ASSERT_EQUAL(ToString(Status::Ok), ToString(written.status));
	}
	DumpTelemetry("seek-mid-8k", out);

	ASSERT_EQUAL(ToString(Status::Ok),
		ToString(out.Seek(-4096, Position::Relative).status));
	FIFO back = FromText(std::string(4096, 'B'));
	{
		auto written = out.Write(back);
		if (written.status == Status::TryAgain) {
			ASSERT_EQUAL(ToString(Status::Ok), ToString(out.Flush().status));
			written = out.Write(back);
		}
		ASSERT_EQUAL(ToString(Status::Ok), ToString(written.status));
	}
	DumpTelemetry("seek-rel-4k", out);

	ASSERT_EQUAL(ToString(Status::Ok), ToString(out.Flush().status));
	DumpTelemetry("after-flush", out);

	const auto tel = out.Telemetry();
	const auto* io = IoW(out);
	ASSERT_TRUE(static_cast<bool>(tel));
	ASSERT_TRUE(io != nullptr);
	ASSERT_EQUAL(tel->Accepted(), io->Behind() + io->Direct());
	ASSERT_TRUE(tel->Accepted() >= StormByte::ByteSize{kBytes});
	ASSERT_EQUAL(StormByte::ByteSize{4096u * 8u}, io->Cap());
	ASSERT_EQUAL(StormByte::ByteSize{kBytes}, io->HighWater());
	ASSERT_EQUAL(io->Materialized(), io->HighWater());
	ASSERT_TRUE(out.Close());

	std::string expect = blob;
	expect.replace(0, 8192, std::string(8192, 'H'));
	expect.replace(kBytes / 2, 4096, std::string(4096, 'M'));
	expect.replace(kBytes / 2 + 4096, 4096, std::string(4096, 'B'));
	ASSERT_EQUAL(expect, Slurp(path));
	std::filesystem::remove(path);
	RETURN_TEST(0);
}

int test_telemetry_ring_counts_behind_and_tryagain() {
	const auto path = Scratch("tring");
	std::filesystem::remove(path);
	BufferedFileWriter out(Loc(path), P(StormByte::ByteSize{4}, 1));
	ASSERT_TRUE(out.Open());
	FIFO first = FromText("AB");
	ASSERT_EQUAL(ToString(Status::Ok), ToString(out.Write(first).status));
	FIFO second = FromText("CDEFGH");
	ASSERT_EQUAL(ToString(Status::TryAgain), ToString(out.Write(second).status));
	DumpTelemetry("ring-bp", out);
	const auto tel = out.Telemetry();
	const auto* io = IoW(out);
	ASSERT_TRUE(static_cast<bool>(tel));
	ASSERT_TRUE(io != nullptr);
	ASSERT_EQUAL(StormByte::ByteSize{2}, tel->Accepted());
	ASSERT_EQUAL(static_cast<std::size_t>(1), io->TryAgain());
	ASSERT_TRUE(out.Close());
	ASSERT_EQUAL(std::string("AB"), Slurp(path));
	std::filesystem::remove(path);
	RETURN_TEST(0);
}

int test_telemetry_survives_close_and_truncate() {
	const auto path = Scratch("tkeep");
	std::filesystem::remove(path);
	BufferedFileWriter out(Loc(path), P(StormByte::ByteSize{0}, 0));
	ASSERT_TRUE(out.Open());
	FIFO src = FromText("XYZ");
	ASSERT_EQUAL(ToString(Status::Ok), ToString(out.Write(src).status));
	ASSERT_EQUAL(ToString(Status::Ok), ToString(out.Truncate().status));
	DumpTelemetry("after-truncate", out);
	const auto tel = out.Telemetry();
	const auto* io = IoW(out);
	ASSERT_TRUE(static_cast<bool>(tel));
	ASSERT_TRUE(io != nullptr);
	ASSERT_EQUAL(StormByte::ByteSize{3}, tel->Accepted());
	ASSERT_EQUAL(StormByte::ByteSize{0}, io->HighWater());
	ASSERT_EQUAL(StormByte::ByteSize{0}, io->Materialized());
	ASSERT_TRUE(out.Close());
	DumpTelemetry("after-close", out);
	ASSERT_EQUAL(StormByte::ByteSize{3}, tel->Accepted());
	ASSERT_EQUAL(std::string(""), Slurp(path));
	std::filesystem::remove(path);
	RETURN_TEST(0);
}

int main() {
	int result = 0;

	// -------------------
	// Direct
	// -------------------
	result += test_idle_direct_writer_shutdown();
	result += test_direct_span_write();
	result += test_direct_write_hits_disk();
	result += test_empty_write_ok();
	result += test_explicit_zero_survives_open();

	// -------------------
	// Hex
	// -------------------
	result += test_hex_fake_seek_patch();
	result += test_hex_fake_seek_small_then_front();
	result += test_hex_island_1m_zeros();
	result += test_hex_seek_past_eof_hole();
	result += test_hex_seq_matches_pattern();
	result += test_hex_evict_keeps_pattern();

	// -------------------
	// Move
	// -------------------
	result += test_move_transfers_session();

	// -------------------
	// Setup
	// -------------------
	result += test_explicit_chunk_survives_setup();
	result += test_path_only_setup_sets_device_knobs();
	result += test_path_only_short_write_holds_until_flush();

	// -------------------
	// Ring
	// -------------------
	result += test_backpressure_tryagain_atomic();
	result += test_chunk_holds_until_flush();
	result += test_close_flushes_dirty();
	result += test_full_chunk_clears_dirty();
	result += test_truncate_drops_dirty();
	result += test_truncate_zeros_file_and_tell();
	result += test_two_chunks_then_flush();

	// -------------------
	// Seek
	// -------------------
	result += test_seek_before_start_fails();
	result += test_seek_keeps_dirty_then_patches();
	result += test_seek_patch_direct();
	result += test_seek_relative();
	result += test_seek_then_extend();
	result += test_seek_without_open_fails();
	result += test_size_counts_dirty();

	// -------------------
	// Session
	// -------------------
	result += test_close_then_reopen_appends();
	result += test_ctor_unavailable();
	result += test_open_creates_and_not_idempotent();
	result += test_open_directory();
	result += test_open_missing_parent();
	result += test_rewind_without_open_fails();
	result += test_write_before_open_leaves_src();

	// -------------------
	// Stress
	// -------------------
	result += test_stress_close_after_evict_seek();
	result += test_stress_double_flush_seek_back();
	result += test_stress_evict_multi_page();
	result += test_stress_far_future_hole();
	result += test_stress_patch_evicted_and_dirty();
	result += test_stress_reopen_second_session();
	result += test_stress_ring_seek_no_flush();

	// -------------------
	// Telemetry
	// -------------------
	result += test_telemetry_ctor_is_zero();
	result += test_telemetry_direct_write_counts_accepted();
	result += test_telemetry_fake_seek_epoch();
	result += test_telemetry_island_and_hole();
	result += test_telemetry_pressure_mixed_seeks();
	result += test_telemetry_ring_counts_behind_and_tryagain();
	result += test_telemetry_survives_close_and_truncate();

	if (result == 0)
		std::cout << "All tests passed!" << std::endl;
	else
		std::cout << result << " tests failed." << std::endl;
	return result;
}
