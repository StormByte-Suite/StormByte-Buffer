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
#include <StormByte/safe/binary.hxx>
#include <StormByte/test_handlers.h>

#include <cstddef>
#include <cstring>
#include <iostream>
#include <memory>
#include <random>
#include <string>
#include <string_view>
#include <vector>

using StormByte::Buffer::FIFO;
using StormByte::Buffer::Position;
using StormByte::Buffer::ReadOnly;
using StormByte::Buffer::ReadWrite;
using StormByte::Buffer::WriteOnly;
using StormByte::Safe::Binary;

namespace {
	std::string BytesToText(const Binary& data) {
		if (data.empty())
			return {};
		return std::string(reinterpret_cast<const char*>(data.data()),
			static_cast<std::size_t>(data.size()));
	}

	Binary TextToBytes(std::string_view text) {
		Binary out(StormByte::ByteSize{text.size()});
		if (!text.empty())
			std::memcpy(out.data(), text.data(), text.size());
		return out;
	}

	std::string MakePattern(std::size_t n) {
		std::string s;
		s.reserve(n);
		for (std::size_t i = 0; i < n; ++i)
			s.push_back(static_cast<char>('A' + (i % 26)));
		return s;
	}

	/**
	 * @brief Consumer-defined FIFO with a DLL-safe formatting hook.
	 */
	class HeaderFIFO final: public FIFO {
		protected:
			/**
			 * @brief Return consumer-owned header text without exporting a stream.
			 * @return DLL-safe custom header, including an embedded NUL.
			 */
			StormByte::Safe::String HexDumpHeader() const noexcept override {
				return StormByte::Safe::String{std::string_view("fifo\0header", 11)};
			}
	};
}

// -------------------
// Available
// -------------------

int test_fifo_available_bytes() {
	FIFO fifo;
	ASSERT_EQUAL(fifo.Available(), StormByte::ByteSize{0});
	(void)fifo.Write("ABCDEFGHIJ");
	ASSERT_EQUAL(fifo.Available(), StormByte::ByteSize{10});
	Binary r1;
	(void)fifo.Read(3, r1);
	ASSERT_EQUAL(fifo.Available(), StormByte::ByteSize{7});
	Binary r2;
	(void)fifo.Read(2, r2);
	ASSERT_EQUAL(fifo.Available(), StormByte::ByteSize{5});
	fifo.Seek(0, Position::Absolute);
	ASSERT_EQUAL(fifo.Available(), StormByte::ByteSize{10});
	fifo.Seek(4, Position::Absolute);
	ASSERT_EQUAL(fifo.Available(), StormByte::ByteSize{6});
	Binary e1;
	(void)fifo.Extract(3, e1);
	ASSERT_EQUAL(fifo.Available(), StormByte::ByteSize{3});
	Binary r3;
	(void)fifo.Read(0, r3);
	ASSERT_EQUAL(fifo.Available(), StormByte::ByteSize{0});
	fifo.Seek(0, Position::Absolute);
	Binary e2;
	(void)fifo.Extract(0, e2);
	ASSERT_EQUAL(fifo.Available(), StormByte::ByteSize{0});
	ASSERT_TRUE(fifo.Empty());
	RETURN_TEST(0);
}

int test_fifo_available_bytes_after_ops() {
	FIFO fifo;
	(void)fifo.Write("ABCDEFGH");
	ASSERT_EQUAL(fifo.Available(), StormByte::ByteSize{8});
	Binary r1;
	(void)fifo.Read(3, r1);
	ASSERT_EQUAL(fifo.Available(), StormByte::ByteSize{5});
	Binary e1;
	(void)fifo.Extract(4, e1);
	ASSERT_EQUAL(fifo.Available(), StormByte::ByteSize{1});
	(void)fifo.Write("1234");
	ASSERT_EQUAL(fifo.Available(), StormByte::ByteSize{5});
	Binary r2;
	(void)fifo.Read(5, r2);
	ASSERT_EQUAL(fifo.Available(), StormByte::ByteSize{0});
	RETURN_TEST(0);
}

// -------------------
// Close
// -------------------

int test_fifo_close_eof_when_empty() {
	FIFO fifo;
	fifo.Close();
	ASSERT_TRUE(fifo.EoF());
	ASSERT_FALSE(fifo.IsWritable());
	ASSERT_TRUE(fifo.IsReadable());
	Binary out;
	ASSERT_FALSE(fifo.Read(0, out));
	RETURN_TEST(0);
}

int test_fifo_close_preserves_copy_state() {
	FIFO a;
	(void)a.Write("HI");
	a.Close();
	FIFO b(a);
	ASSERT_FALSE(b.IsWritable());
	ASSERT_TRUE(b.IsReadable());
	Binary out;
	ASSERT_TRUE(b.Extract(0, out));
	ASSERT_EQUAL(BytesToText(out), std::string("HI"));
	RETURN_TEST(0);
}

int test_fifo_close_rejects_writes() {
	FIFO fifo;
	(void)fifo.Write("ABC");
	ASSERT_TRUE(fifo.IsWritable());
	ASSERT_FALSE(fifo.EoF());
	fifo.Close();
	ASSERT_FALSE(fifo.IsWritable());
	ASSERT_TRUE(fifo.IsReadable());
	ASSERT_FALSE(fifo.Write("X"));
	ASSERT_EQUAL(fifo.Size(), StormByte::ByteSize{3});
	Binary out;
	ASSERT_TRUE(fifo.Extract(0, out));
	ASSERT_EQUAL(BytesToText(out), std::string("ABC"));
	ASSERT_TRUE(fifo.EoF());
	RETURN_TEST(0);
}

int test_fifo_seterror_blocks_all() {
	FIFO fifo;
	(void)fifo.Write("DATA");
	fifo.SetError();
	ASSERT_FALSE(fifo.IsWritable());
	ASSERT_FALSE(fifo.IsReadable());
	ASSERT_TRUE(fifo.EoF());
	ASSERT_FALSE(fifo.Write("X"));
	Binary out;
	ASSERT_FALSE(fifo.Read(0, out));
	ASSERT_FALSE(fifo.Extract(0, out));
	RETURN_TEST(0);
}

// -------------------
// Construct
// -------------------

int test_fifo_copy_ctor_assign() {
	FIFO a;
	(void)a.Write(std::string("AB"));
	FIFO b(a);
	ASSERT_EQUAL(a.Size(), b.Size());
	Binary out1, out2;
	(void)b.Extract(2, out1);
	ASSERT_EQUAL(std::string("AB"), BytesToText(out1));
	FIFO c;
	c = a;
	ASSERT_EQUAL(a.Size(), c.Size());
	(void)c.Extract(2, out2);
	ASSERT_EQUAL(std::string("AB"), BytesToText(out2));
	RETURN_TEST(0);
}

int test_fifo_default_ctor() {
	FIFO fifo;
	ASSERT_TRUE(fifo.Empty());
	ASSERT_EQUAL(StormByte::ByteSize{0}, fifo.Size());
	RETURN_TEST(0);
}

int test_fifo_equality() {
	FIFO a;
	FIFO b;
	(void)a.Write("ABC");
	(void)b.Write("ABC");
	ASSERT_TRUE(a == b);
	ASSERT_FALSE(a != b);
	Binary tmp;
	(void)a.Read(1, tmp);
	ASSERT_FALSE(a == b);
	(void)b.Read(1, tmp);
	ASSERT_TRUE(a == b);
	(void)b.Write("D");
	ASSERT_FALSE(a == b);
	RETURN_TEST(0);
}

int test_fifo_move_ctor_assign() {
	FIFO a;
	(void)a.Write(std::string("XY"));
	FIFO b(std::move(a));
	ASSERT_EQUAL(StormByte::ByteSize{2}, b.Size());
	ASSERT_TRUE(a.Empty());
	FIFO c;
	c = std::move(b);
	ASSERT_EQUAL(StormByte::ByteSize{2}, c.Size());
	ASSERT_TRUE(b.Empty());
	RETURN_TEST(0);
}

int test_fifo_polymorphic_interface_abi() {
	std::unique_ptr<ReadWrite> fifo = std::make_unique<FIFO>();
	ReadOnly& reader = *fifo;
	WriteOnly& writer = *fifo;
	Binary copy {std::byte{'A'}};
	Binary moved {std::byte{'B'}};
	FIFO source;
	ASSERT_TRUE(source.Write("CD"));
	ASSERT_TRUE(writer.Write(0, copy));
	ASSERT_TRUE(writer.Write(0, std::move(moved)));
	ASSERT_TRUE(writer.Write(0, static_cast<const ReadOnly&>(source)));
	source.Seek(0, Position::Absolute);
	ASSERT_TRUE(writer.Write(0, std::move(source)));
	ASSERT_TRUE(writer.IsWritable());
	ASSERT_EQUAL(reader.Size(), StormByte::ByteSize{6});
	ASSERT_EQUAL(reader.Available(), StormByte::ByteSize{6});
	ASSERT_FALSE(reader.Empty());
	ASSERT_TRUE(reader.IsReadable());
	Binary peek;
	ASSERT_TRUE(reader.Peek(1, peek));
	Binary read;
	ASSERT_TRUE(reader.Read(1, read));
	reader.Seek(0, Position::Absolute);
	ASSERT_TRUE(reader.Drop(1));
	Binary extracted;
	ASSERT_TRUE(reader.Extract(1, extracted));
	reader.Clean();
	reader.Clear();
	writer.Close();
	ASSERT_TRUE(reader.EoF());
	Binary until_eof;
	reader.ReadUntilEoF(until_eof);
	reader.ExtractUntilEoF(until_eof);
	fifo.reset();
	RETURN_TEST(0);
}

// -------------------
// HexDump
// -------------------

int test_fifo_hexdump() {
	FIFO fifo;
	(void)fifo.Write("0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcd");
	const std::string dump = std::string(fifo.HexDump(8, 0));
	std::string expected;
	expected += "Size: 40 bytes\n";
	expected += "Read Position: 0\n";
	expected += "Status: open / ok\n";
	expected += "00000000: 30 31 32 33 34 35 36 37   01234567\n";
	expected += "00000008: 38 39 41 42 43 44 45 46   89ABCDEF\n";
	expected += "00000010: 47 48 49 4A 4B 4C 4D 4E   GHIJKLMN\n";
	expected += "00000018: 4F 50 51 52 53 54 55 56   OPQRSTUV\n";
	expected += "00000020: 57 58 59 5A 61 62 63 64   WXYZabcd";
	ASSERT_EQUAL(expected, dump);
	RETURN_TEST(0);
}

int test_fifo_hexdump_mixed() {
	FIFO fifo;
	std::vector<std::byte> v;
	v.push_back(std::byte{0x41});
	v.push_back(std::byte{0x00});
	v.push_back(std::byte{0x1F});
	v.push_back(std::byte{0x20});
	v.push_back(std::byte{0x41});
	v.push_back(std::byte{0x7E});
	v.push_back(std::byte{0x7F});
	v.push_back(std::byte{0x80});
	v.push_back(std::byte{0xFF});
	v.push_back(std::byte{0x30});
	(void)fifo.Write(std::move(v));
	const std::string dump = std::string(fifo.HexDump(8, 0));
	std::string expected;
	expected += "Size: 10 bytes\n";
	expected += "Read Position: 0\n";
	expected += "Status: open / ok\n";
	expected += "00000000: 41 00 1F 20 41 7E 7F 80   A.. A~..\n";
	expected += std::string("00000008: FF 30") + std::string(21, ' ') + ".0";
	ASSERT_EQUAL(expected, dump);
	RETURN_TEST(0);
}

int test_fifo_hexdump_offset() {
	FIFO fifo;
	(void)fifo.Write("0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcd");
	fifo.Seek(5, Position::Absolute);
	const std::string dump = std::string(fifo.HexDump(8, 0));
	std::string expected;
	expected += "Size: 40 bytes\n";
	expected += "Read Position: 5\n";
	expected += "Status: open / ok\n";
	expected += "00000005: 35 36 37 38 39 41 42 43   56789ABC\n";
	expected += "0000000D: 44 45 46 47 48 49 4A 4B   DEFGHIJK\n";
	expected += "00000015: 4C 4D 4E 4F 50 51 52 53   LMNOPQRS\n";
	expected += "0000001D: 54 55 56 57 58 59 5A 61   TUVWXYZa\n";
	expected += "00000025: 62 63 64                  bcd";
	ASSERT_EQUAL(expected, dump);
	RETURN_TEST(0);
}

int test_fifo_hexdump_status_closed() {
	FIFO fifo;
	(void)fifo.Write("AB");
	fifo.Close();
	const std::string dump = std::string(fifo.HexDump(8, 0));
	ASSERT_CONTAINS(dump, std::string_view("closed"));
	ASSERT_CONTAINS(dump, std::string_view("ok"));
	RETURN_TEST(0);
}

int test_fifo_hexdump_status_error() {
	FIFO fifo;
	(void)fifo.Write("AB");
	fifo.SetError();
	const std::string dump = std::string(fifo.HexDump(8, 0));
	ASSERT_CONTAINS(dump, std::string_view("error"));
	RETURN_TEST(0);
}

int test_fifo_safe_header_hook() {
	HeaderFIFO fifo;
	ASSERT_TRUE(fifo.Write("A"));
	const FIFO& base = fifo;
	const auto dump = base.HexDump();
	ASSERT_TRUE(std::string_view(dump).starts_with(std::string_view("fifo\0header\n", 12)));
	ASSERT_TRUE(dump.contains("41"));
	ASSERT_EQUAL(fifo.Available(), StormByte::ByteSize{1});
	RETURN_TEST(0);
}

// -------------------
// Peek
// -------------------

int test_fifo_peek_after_seek() {
	FIFO fifo;
	(void)fifo.Write(std::string("0123456789"));
	fifo.Seek(5, Position::Absolute);
	Binary peek;
	ASSERT_TRUE(fifo.Peek(3, peek));
	ASSERT_EQUAL(BytesToText(peek), std::string("567"));
	Binary read;
	ASSERT_TRUE(fifo.Read(3, read));
	ASSERT_EQUAL(BytesToText(read), std::string("567"));
	RETURN_TEST(0);
}

int test_fifo_peek_all_available() {
	FIFO fifo;
	(void)fifo.Write(std::string("WORLD"));
	Binary peek_all;
	ASSERT_TRUE(fifo.Peek(0, peek_all));
	ASSERT_EQUAL(BytesToText(peek_all), std::string("WORLD"));
	Binary read1;
	ASSERT_TRUE(fifo.Read(2, read1));
	Binary peek_remaining;
	ASSERT_TRUE(fifo.Peek(0, peek_remaining));
	ASSERT_EQUAL(BytesToText(peek_remaining), std::string("RLD"));
	RETURN_TEST(0);
}

int test_fifo_peek_basic() {
	FIFO fifo;
	(void)fifo.Write(std::string("HELLO"));
	Binary peek1;
	ASSERT_TRUE(fifo.Peek(3, peek1));
	ASSERT_EQUAL(BytesToText(peek1), std::string("HEL"));
	Binary peek2;
	ASSERT_TRUE(fifo.Peek(3, peek2));
	ASSERT_EQUAL(BytesToText(peek2), std::string("HEL"));
	Binary read1;
	ASSERT_TRUE(fifo.Read(3, read1));
	ASSERT_EQUAL(BytesToText(read1), std::string("HEL"));
	Binary peek3;
	ASSERT_TRUE(fifo.Peek(2, peek3));
	ASSERT_EQUAL(BytesToText(peek3), std::string("LO"));
	RETURN_TEST(0);
}

int test_fifo_peek_insufficient_data() {
	FIFO fifo;
	(void)fifo.Write(std::string("ABC"));
	Binary peek;
	ASSERT_FALSE(fifo.Peek(10, peek));
	RETURN_TEST(0);
}

int test_fifo_skip_basic() {
	FIFO fifo;
	(void)fifo.Write(std::string("ABCDEFG"));
	(void)fifo.Drop(3);
	ASSERT_EQUAL(fifo.Size(), StormByte::ByteSize{4});
	Binary out;
	(void)fifo.Extract(0, out);
	ASSERT_EQUAL(BytesToText(out), std::string("DEFG"));
	RETURN_TEST(0);
}

int test_fifo_skip_with_readpos() {
	FIFO fifo;
	(void)fifo.Write(std::string("0123456789"));
	Binary r;
	ASSERT_TRUE(fifo.Read(3, r));
	(void)fifo.Drop(4);
	ASSERT_EQUAL(fifo.Size(), StormByte::ByteSize{3});
	Binary out;
	(void)fifo.Extract(0, out);
	ASSERT_EQUAL(BytesToText(out), std::string("789"));
	RETURN_TEST(0);
}

// -------------------
// Read
// -------------------

int test_fifo_buffer_stress() {
	FIFO fifo;
	std::mt19937_64 rng(12345);
	std::uniform_int_distribution<int> small(1, 256);
	std::uniform_int_distribution<int> large(512, 4096);
	std::string expected;
	expected.reserve(200000);
	for (int i = 0; i < 1000; ++i) {
		int len = small(rng);
		std::string chunk = MakePattern(static_cast<std::size_t>(len));
		(void)fifo.Write(chunk);
		expected.append(chunk);
		if (i % 10 == 0) {
			Binary out;
			(void)fifo.Extract(len / 2, out);
			std::string got = BytesToText(out);
			std::string exp = expected.substr(0, static_cast<std::size_t>(out.size()));
			ASSERT_EQUAL(exp, got);
			expected.erase(0, static_cast<std::size_t>(out.size()));
		}
	}
	for (int i = 0; i < 200; ++i) {
		int len = large(rng);
		std::string chunk = MakePattern(static_cast<std::size_t>(len));
		(void)fifo.Write(chunk);
		expected.append(chunk);
		if (i % 5 == 0) {
			Binary out;
			(void)fifo.Extract(len, out);
			std::string got = BytesToText(out);
			std::string exp = expected.substr(0, static_cast<std::size_t>(out.size()));
			ASSERT_EQUAL(exp, got);
			expected.erase(0, static_cast<std::size_t>(out.size()));
		}
	}
	Binary out;
	(void)fifo.Extract(0, out);
	ASSERT_EQUAL(expected, BytesToText(out));
	ASSERT_TRUE(fifo.Empty());
	RETURN_TEST(0);
}

int test_fifo_clear() {
	FIFO fifo;
	(void)fifo.Write(std::string(100, 'A'));
	fifo.Clear();
	ASSERT_TRUE(fifo.Empty());
	ASSERT_EQUAL(StormByte::ByteSize{0}, fifo.Size());
	RETURN_TEST(0);
}

int test_fifo_clear_with_data() {
	FIFO fifo;
	(void)fifo.Write(TextToBytes("X"));
	ASSERT_FALSE(fifo.Empty());
	fifo.Clear();
	ASSERT_TRUE(fifo.Empty());
	ASSERT_EQUAL(fifo.Size(), StormByte::ByteSize{0});
	RETURN_TEST(0);
}

int test_fifo_extract_adjusts_read_position() {
	FIFO fifo;
	(void)fifo.Write(std::string("0123456789"));
	Binary r1;
	(void)fifo.Read(5, r1);
	ASSERT_EQUAL(BytesToText(r1), std::string("01234"));
	Binary e1;
	(void)fifo.Extract(3, e1);
	ASSERT_EQUAL(BytesToText(e1), std::string("567"));
	ASSERT_EQUAL(fifo.Size(), StormByte::ByteSize{7});
	Binary r2;
	(void)fifo.Read(2, r2);
	ASSERT_EQUAL(BytesToText(r2), std::string("89"));
	RETURN_TEST(0);
}

int test_fifo_extract_insufficient_data_error() {
	FIFO fifo;
	(void)fifo.Write(std::string("HELLO"));
	Binary result;
	ASSERT_FALSE(fifo.Extract(20, result));
	Binary result2;
	ASSERT_TRUE(fifo.Extract(0, result2));
	ASSERT_EQUAL(result2.size(), StormByte::ByteSize{5});
	ASSERT_TRUE(fifo.Empty());
	RETURN_TEST(0);
}

int test_fifo_read_after_position_beyond_size() {
	FIFO fifo;
	(void)fifo.Write(std::string("1234"));
	Binary r1;
	ASSERT_TRUE(fifo.Read(4, r1));
	ASSERT_EQUAL(BytesToText(r1), std::string("1234"));
	Binary result;
	ASSERT_FALSE(fifo.Read(1, result));
	Binary result2;
	ASSERT_FALSE(fifo.Read(0, result2));
	RETURN_TEST(0);
}

int test_fifo_read_all_nondestructive() {
	FIFO fifo;
	(void)fifo.Write(std::string("HELLO"));
	Binary out1;
	(void)fifo.Read(0, out1);
	ASSERT_EQUAL(BytesToText(out1), std::string("HELLO"));
	ASSERT_EQUAL(fifo.Size(), StormByte::ByteSize{5});
	ASSERT_FALSE(fifo.Empty());
	Binary tmp;
	ASSERT_FALSE(fifo.Read(0, tmp));
	RETURN_TEST(0);
}

int test_fifo_read_default_all() {
	FIFO fifo;
	(void)fifo.Write(std::string("DATA"));
	Binary out;
	(void)fifo.Extract(0, out);
	ASSERT_EQUAL(BytesToText(out), std::string("DATA"));
	ASSERT_TRUE(fifo.Empty());
	RETURN_TEST(0);
}

int test_fifo_read_insufficient_data_error() {
	FIFO fifo;
	(void)fifo.Write(std::string("ABC"));
	Binary result;
	ASSERT_FALSE(fifo.Read(10, result));
	Binary result2;
	ASSERT_TRUE(fifo.Read(0, result2));
	ASSERT_EQUAL(result2.size(), StormByte::ByteSize{3});
	RETURN_TEST(0);
}

int test_fifo_read_nondestructive() {
	FIFO fifo;
	(void)fifo.Write(std::string("ABCDEF"));
	Binary out1;
	(void)fifo.Read(3, out1);
	ASSERT_EQUAL(BytesToText(out1), std::string("ABC"));
	ASSERT_EQUAL(fifo.Size(), StormByte::ByteSize{6});
	Binary out2;
	(void)fifo.Read(3, out2);
	ASSERT_EQUAL(BytesToText(out2), std::string("DEF"));
	ASSERT_EQUAL(fifo.Size(), StormByte::ByteSize{6});
	Binary out3;
	ASSERT_FALSE(fifo.Read(0, out3));
	RETURN_TEST(0);
}

int test_fifo_read_span_all_available() {
	FIFO fifo;
	(void)fifo.Write("HelloWorld");
	Binary out;
	ASSERT_TRUE(fifo.Read(0, out));
	ASSERT_EQUAL(out.size(), StormByte::ByteSize{10});
	ASSERT_EQUAL(BytesToText(out), std::string("HelloWorld"));
	ASSERT_EQUAL(fifo.Available(), StormByte::ByteSize{0});
	RETURN_TEST(0);
}

int test_fifo_read_span_basic() {
	FIFO fifo;
	(void)fifo.Write("ABCDEF");
	Binary out;
	ASSERT_TRUE(fifo.Read(3, out));
	ASSERT_EQUAL(out.size(), StormByte::ByteSize{3});
	ASSERT_EQUAL(static_cast<char>(out[0]), 'A');
	ASSERT_EQUAL(static_cast<char>(out[1]), 'B');
	ASSERT_EQUAL(static_cast<char>(out[2]), 'C');
	Binary read;
	ASSERT_TRUE(fifo.Read(3, read));
	ASSERT_EQUAL(BytesToText(read), std::string("DEF"));
	RETURN_TEST(0);
}

int test_fifo_read_span_insufficient_data() {
	FIFO fifo;
	(void)fifo.Write("ABC");
	Binary span;
	ASSERT_FALSE(fifo.Read(10, span));
	Binary read;
	ASSERT_TRUE(fifo.Read(3, read));
	ASSERT_EQUAL(BytesToText(read), std::string("ABC"));
	RETURN_TEST(0);
}

int test_fifo_read_span_vs_read() {
	FIFO fifo1, fifo2;
	const std::string data = "ComparisonTest";
	(void)fifo1.Write(data);
	(void)fifo2.Write(data);
	Binary r1, r2;
	ASSERT_TRUE(fifo1.Read(4, r1));
	ASSERT_TRUE(fifo2.Read(4, r2));
	ASSERT_EQUAL(BytesToText(r1), BytesToText(r2));
	ASSERT_EQUAL(BytesToText(r1), std::string("Comp"));
	ASSERT_EQUAL(fifo1.Available(), fifo2.Available());
	RETURN_TEST(0);
}

int test_fifo_read_vs_extract() {
	FIFO fifo;
	(void)fifo.Write(std::string("123456"));
	Binary r1;
	(void)fifo.Read(2, r1);
	ASSERT_EQUAL(BytesToText(r1), std::string("12"));
	ASSERT_EQUAL(fifo.Size(), StormByte::ByteSize{6});
	Binary e1;
	(void)fifo.Extract(2, e1);
	ASSERT_EQUAL(BytesToText(e1), std::string("34"));
	ASSERT_EQUAL(fifo.Size(), StormByte::ByteSize{4});
	Binary r2;
	(void)fifo.Read(2, r2);
	ASSERT_EQUAL(BytesToText(r2), std::string("56"));
	RETURN_TEST(0);
}

int test_fifo_read_with_wrap() {
	FIFO fifo;
	(void)fifo.Write("ABCDE");
	Binary temp;
	(void)fifo.Extract(2, temp);
	(void)fifo.Write("12");
	Binary out;
	(void)fifo.Read(0, out);
	ASSERT_EQUAL(BytesToText(out), std::string("CDE12"));
	ASSERT_EQUAL(fifo.Size(), StormByte::ByteSize{5});
	RETURN_TEST(0);
}

int test_fifo_write_read_vector() {
	FIFO fifo;
	const std::string s = "Hello";
	(void)fifo.Write(s);
	Binary data;
	(void)fifo.Extract(StormByte::ByteSize{s.size()}, data);
	ASSERT_EQUAL(s, BytesToText(data));
	ASSERT_TRUE(fifo.Empty());
	RETURN_TEST(0);
}

int test_fifo_wrap_around() {
	FIFO fifo;
	(void)fifo.Write("ABCDE");
	Binary r1, all;
	(void)fifo.Extract(2, r1);
	ASSERT_EQUAL(std::string("AB"), BytesToText(r1));
	(void)fifo.Write("1234");
	(void)fifo.Extract(7, all);
	ASSERT_EQUAL(StormByte::ByteSize{7}, all.size());
	ASSERT_EQUAL(std::string("CDE1234"), BytesToText(all));
	ASSERT_TRUE(fifo.Empty());
	RETURN_TEST(0);
}

// -------------------
// Seek
// -------------------

int test_fifo_seek_absolute() {
	FIFO fifo;
	(void)fifo.Write(std::string("ABCDEFGHIJ"));
	Binary r1;
	fifo.Seek(3, Position::Absolute);
	(void)fifo.Read(3, r1);
	ASSERT_EQUAL(BytesToText(r1), std::string("DEF"));
	fifo.Seek(0, Position::Absolute);
	Binary r2;
	(void)fifo.Read(2, r2);
	ASSERT_EQUAL(BytesToText(r2), std::string("AB"));
	fifo.Seek(7, Position::Absolute);
	Binary r3;
	(void)fifo.Read(3, r3);
	ASSERT_EQUAL(BytesToText(r3), std::string("HIJ"));
	fifo.Seek(100, Position::Absolute);
	Binary tmp;
	ASSERT_FALSE(fifo.Read(0, tmp));
	RETURN_TEST(0);
}

int test_fifo_seek_after_extract() {
	FIFO fifo;
	(void)fifo.Write(std::string("ABCDEFGHIJKLMNO"));
	Binary r1;
	(void)fifo.Read(5, r1);
	ASSERT_EQUAL(BytesToText(r1), std::string("ABCDE"));
	Binary e1;
	(void)fifo.Extract(3, e1);
	ASSERT_EQUAL(BytesToText(e1), std::string("FGH"));
	ASSERT_EQUAL(fifo.Size(), StormByte::ByteSize{12});
	fifo.Seek(0, Position::Absolute);
	Binary r2;
	(void)fifo.Read(3, r2);
	ASSERT_EQUAL(BytesToText(r2), std::string("ABC"));
	fifo.Seek(5, Position::Absolute);
	Binary r3;
	(void)fifo.Read(3, r3);
	ASSERT_EQUAL(BytesToText(r3), std::string("IJK"));
	RETURN_TEST(0);
}

int test_fifo_seek_relative() {
	FIFO fifo;
	(void)fifo.Write(std::string("0123456789"));
	Binary r1;
	(void)fifo.Read(2, r1);
	ASSERT_EQUAL(BytesToText(r1), std::string("01"));
	fifo.Seek(3, Position::Relative);
	Binary r2;
	(void)fifo.Read(2, r2);
	ASSERT_EQUAL(BytesToText(r2), std::string("56"));
	fifo.Seek(2, Position::Relative);
	Binary r3;
	(void)fifo.Read(1, r3);
	ASSERT_EQUAL(BytesToText(r3), std::string("9"));
	fifo.Seek(100, Position::Relative);
	Binary tmp;
	ASSERT_FALSE(fifo.Read(0, tmp));
	RETURN_TEST(0);
}

int test_fifo_seek_relative_from_current() {
	FIFO fifo;
	(void)fifo.Write(std::string("ABCDEFGHIJ"));
	Binary r1;
	ASSERT_TRUE(fifo.Read(2, r1));
	ASSERT_EQUAL(BytesToText(r1), std::string("AB"));
	fifo.Seek(0, Position::Relative);
	Binary r2;
	ASSERT_TRUE(fifo.Read(2, r2));
	ASSERT_EQUAL(BytesToText(r2), std::string("CD"));
	fifo.Seek(1, Position::Absolute);
	Binary r3;
	ASSERT_TRUE(fifo.Read(3, r3));
	ASSERT_EQUAL(BytesToText(r3), std::string("BCD"));
	RETURN_TEST(0);
}

int test_fifo_seek_with_wrap() {
	FIFO fifo;
	(void)fifo.Write("ABCDEFGHIJ");
	Binary e1;
	(void)fifo.Extract(5, e1);
	ASSERT_EQUAL(fifo.Size(), StormByte::ByteSize{5});
	(void)fifo.Write("12345");
	ASSERT_EQUAL(fifo.Size(), StormByte::ByteSize{10});
	fifo.Seek(0, Position::Absolute);
	Binary r1;
	(void)fifo.Read(5, r1);
	ASSERT_EQUAL(BytesToText(r1), std::string("FGHIJ"));
	fifo.Seek(5, Position::Absolute);
	Binary r2;
	(void)fifo.Read(5, r2);
	ASSERT_EQUAL(BytesToText(r2), std::string("12345"));
	RETURN_TEST(0);
}

// -------------------
// Write
// -------------------

int test_fifo_adopt_storage_move_write() {
	FIFO fifo;
	auto v = TextToBytes("MOVE");
	(void)fifo.Write(std::move(v));
	ASSERT_EQUAL(fifo.Size(), StormByte::ByteSize{4});
	Binary out;
	(void)fifo.Extract(4, out);
	ASSERT_EQUAL(BytesToText(out), std::string("MOVE"));
	ASSERT_TRUE(fifo.Empty());
	RETURN_TEST(0);
}

int test_fifo_move_steal_preserves_read_position() {
	FIFO src;
	(void)src.Write(std::string("ABCDE"));
	Binary r;
	ASSERT_TRUE(src.Read(2, r));
	FIFO dst;
	ASSERT_TRUE(dst.Write(std::move(src)));
	Binary out;
	ASSERT_TRUE(dst.Read(0, out));
	ASSERT_EQUAL(BytesToText(out), std::string("CDE"));
	RETURN_TEST(0);
}

int test_fifo_write_basic() {
	FIFO fifo;
	(void)fifo.Write(std::string("1234"));
	ASSERT_EQUAL(StormByte::ByteSize{4}, fifo.Size());
	RETURN_TEST(0);
}

int test_fifo_write_full_telling_zero() {
	FIFO fifo;
	Binary data(StormByte::ByteSize{10}, std::byte{0xFF});
	ASSERT_TRUE(fifo.Write(0, data));
	ASSERT_EQUAL(fifo.Size(), StormByte::ByteSize{10});
	RETURN_TEST(0);
}

int test_fifo_write_multiple() {
	FIFO fifo;
	(void)fifo.Write(std::string(10, 'Z'));
	ASSERT_EQUAL(StormByte::ByteSize{10}, fifo.Size());
	(void)fifo.Write(std::string(5, 'Y'));
	ASSERT_EQUAL(StormByte::ByteSize{15}, fifo.Size());
	RETURN_TEST(0);
}

int test_fifo_write_partial_count() {
	FIFO fifo;
	auto data = TextToBytes("PARTIAL");
	ASSERT_TRUE(fifo.Write(3, data));
	ASSERT_EQUAL(StormByte::ByteSize{3}, fifo.Size());
	Binary out;
	(void)fifo.Extract(0, out);
	ASSERT_EQUAL(std::string("PAR"), BytesToText(out));
	RETURN_TEST(0);
}

int test_fifo_write_remaining_fifo() {
	FIFO src;
	(void)src.Write(std::string("HELLO"));
	Binary r;
	(void)src.Read(2, r);
	FIFO dst;
	(void)dst.Write(std::string("START"));
	const auto before = dst.Size();
	const auto src_remaining = src.Available();
	ASSERT_TRUE(dst.Write(src));
	ASSERT_EQUAL(dst.Size(), before + src_remaining);
	Binary all;
	(void)dst.Extract(0, all);
	ASSERT_EQUAL(BytesToText(all), std::string("STARTLLO"));
	FIFO src2;
	(void)src2.Write(std::string("WORLD"));
	ASSERT_TRUE(dst.Write(std::move(src2)));
	Binary tail;
	(void)dst.Extract(0, tail);
	ASSERT_EQUAL(BytesToText(tail), std::string("WORLD"));
	RETURN_TEST(0);
}

int test_fifo_write_vector_and_rvalue() {
	FIFO fifo;
	std::vector<std::byte> v(3);
	v[0] = std::byte{'A'};
	v[1] = std::byte{'B'};
	v[2] = std::byte{'C'};
	(void)fifo.Write(v);
	std::vector<std::byte> w(3);
	w[0] = std::byte{'D'};
	w[1] = std::byte{'E'};
	w[2] = std::byte{'F'};
	(void)fifo.Write(std::move(w));
	Binary out;
	(void)fifo.Extract(6, out);
	ASSERT_EQUAL(std::string("ABCDEF"), BytesToText(out));
	RETURN_TEST(0);
}

int main() {
	int result = 0;

	// -------------------
	// Available
	// -------------------
	result += test_fifo_available_bytes();
	result += test_fifo_available_bytes_after_ops();

	// -------------------
	// Close
	// -------------------
	result += test_fifo_close_eof_when_empty();
	result += test_fifo_close_preserves_copy_state();
	result += test_fifo_close_rejects_writes();
	result += test_fifo_seterror_blocks_all();

	// -------------------
	// Construct
	// -------------------
	result += test_fifo_copy_ctor_assign();
	result += test_fifo_default_ctor();
	result += test_fifo_equality();
	result += test_fifo_move_ctor_assign();
	result += test_fifo_polymorphic_interface_abi();

	// -------------------
	// HexDump
	// -------------------
	result += test_fifo_hexdump();
	result += test_fifo_hexdump_mixed();
	result += test_fifo_hexdump_offset();
	result += test_fifo_hexdump_status_closed();
	result += test_fifo_hexdump_status_error();
	result += test_fifo_safe_header_hook();

	// -------------------
	// Peek
	// -------------------
	result += test_fifo_peek_after_seek();
	result += test_fifo_peek_all_available();
	result += test_fifo_peek_basic();
	result += test_fifo_peek_insufficient_data();
	result += test_fifo_skip_basic();
	result += test_fifo_skip_with_readpos();

	// -------------------
	// Read
	// -------------------
	result += test_fifo_buffer_stress();
	result += test_fifo_clear();
	result += test_fifo_clear_with_data();
	result += test_fifo_extract_adjusts_read_position();
	result += test_fifo_extract_insufficient_data_error();
	result += test_fifo_read_after_position_beyond_size();
	result += test_fifo_read_all_nondestructive();
	result += test_fifo_read_default_all();
	result += test_fifo_read_insufficient_data_error();
	result += test_fifo_read_nondestructive();
	result += test_fifo_read_span_all_available();
	result += test_fifo_read_span_basic();
	result += test_fifo_read_span_insufficient_data();
	result += test_fifo_read_span_vs_read();
	result += test_fifo_read_vs_extract();
	result += test_fifo_read_with_wrap();
	result += test_fifo_write_read_vector();
	result += test_fifo_wrap_around();

	// -------------------
	// Seek
	// -------------------
	result += test_fifo_seek_absolute();
	result += test_fifo_seek_after_extract();
	result += test_fifo_seek_relative();
	result += test_fifo_seek_relative_from_current();
	result += test_fifo_seek_with_wrap();

	// -------------------
	// Write
	// -------------------
	result += test_fifo_adopt_storage_move_write();
	result += test_fifo_move_steal_preserves_read_position();
	result += test_fifo_write_basic();
	result += test_fifo_write_full_telling_zero();
	result += test_fifo_write_multiple();
	result += test_fifo_write_partial_count();
	result += test_fifo_write_remaining_fifo();
	result += test_fifo_write_vector_and_rvalue();

	if (result == 0)
		std::cout << "All tests passed!" << std::endl;
	else
		std::cout << result << " tests failed." << std::endl;
	return result;
}
