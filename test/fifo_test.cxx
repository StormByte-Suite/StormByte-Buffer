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
#include <StormByte/test_handlers.h>

#include <cstddef>
#include <cstring>
#include <iostream>
#include <memory>
#include <random>
#include <string>
#include <vector>

using StormByte::BinaryData;
using StormByte::Buffer::FIFO;
using StormByte::Buffer::Position;
using StormByte::Buffer::ReadOnly;
using StormByte::Buffer::ReadWrite;
using StormByte::Buffer::WriteOnly;

namespace {
	std::string BytesToText(const BinaryData& data) {
		if (data.empty())
			return {};
		return std::string(reinterpret_cast<const char*>(data.data()),
			static_cast<std::size_t>(data.size()));
	}

	BinaryData TextToBytes(std::string_view text) {
		BinaryData out(StormByte::ByteSize{text.size()});
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
}

// -------------------
// Available
// -------------------

int test_fifo_available_bytes() {
	FIFO fifo;
	ASSERT_EQUAL("empty available", fifo.Available(), StormByte::ByteSize{0});
	(void)fifo.Write("ABCDEFGHIJ");
	ASSERT_EQUAL("after write available", fifo.Available(), StormByte::ByteSize{10});
	BinaryData r1;
	(void)fifo.Read(3, r1);
	ASSERT_EQUAL("after read 3", fifo.Available(), StormByte::ByteSize{7});
	BinaryData r2;
	(void)fifo.Read(2, r2);
	ASSERT_EQUAL("after read 2 more", fifo.Available(), StormByte::ByteSize{5});
	fifo.Seek(0, Position::Absolute);
	ASSERT_EQUAL("after seek to 0", fifo.Available(), StormByte::ByteSize{10});
	fifo.Seek(4, Position::Absolute);
	ASSERT_EQUAL("after seek to 4", fifo.Available(), StormByte::ByteSize{6});
	BinaryData e1;
	(void)fifo.Extract(3, e1);
	ASSERT_EQUAL("after extract 3", fifo.Available(), StormByte::ByteSize{3});
	BinaryData r3;
	(void)fifo.Read(0, r3);
	ASSERT_EQUAL("after reading all remaining", fifo.Available(), StormByte::ByteSize{0});
	fifo.Seek(0, Position::Absolute);
	BinaryData e2;
	(void)fifo.Extract(0, e2);
	ASSERT_EQUAL("after extract all", fifo.Available(), StormByte::ByteSize{0});
	ASSERT_TRUE("buffer empty", fifo.Empty());
	RETURN_TEST("test_fifo_available_bytes", 0);
}

int test_fifo_available_bytes_after_ops() {
	FIFO fifo;
	(void)fifo.Write("ABCDEFGH");
	ASSERT_EQUAL("initial available", fifo.Available(), StormByte::ByteSize{8});
	BinaryData r1;
	(void)fifo.Read(3, r1);
	ASSERT_EQUAL("after read 3", fifo.Available(), StormByte::ByteSize{5});
	BinaryData e1;
	(void)fifo.Extract(4, e1);
	ASSERT_EQUAL("after extract 4", fifo.Available(), StormByte::ByteSize{1});
	(void)fifo.Write("1234");
	ASSERT_EQUAL("after wrap write", fifo.Available(), StormByte::ByteSize{5});
	BinaryData r2;
	(void)fifo.Read(5, r2);
	ASSERT_EQUAL("after read 5", fifo.Available(), StormByte::ByteSize{0});
	RETURN_TEST("test_fifo_available_bytes_after_ops", 0);
}

// -------------------
// Close / error
// -------------------

int test_fifo_close_eof_when_empty() {
	FIFO fifo;
	fifo.Close();
	ASSERT_TRUE("eof when closed and empty", fifo.EoF());
	ASSERT_FALSE("not writable", fifo.IsWritable());
	ASSERT_TRUE("still readable (no error)", fifo.IsReadable());
	BinaryData out;
	ASSERT_FALSE("read on closed empty fails", fifo.Read(0, out));
	RETURN_TEST("test_fifo_close_eof_when_empty", 0);
}

int test_fifo_close_preserves_copy_state() {
	FIFO a;
	(void)a.Write("HI");
	a.Close();
	FIFO b(a);
	ASSERT_FALSE("copy not writable", b.IsWritable());
	ASSERT_TRUE("copy still readable", b.IsReadable());
	BinaryData out;
	ASSERT_TRUE("copy extract ok", b.Extract(0, out));
	ASSERT_EQUAL("copy content", BytesToText(out), std::string("HI"));
	RETURN_TEST("test_fifo_close_preserves_copy_state", 0);
}

int test_fifo_close_rejects_writes() {
	FIFO fifo;
	(void)fifo.Write("ABC");
	ASSERT_TRUE("writable before close", fifo.IsWritable());
	ASSERT_FALSE("not eof before close with data", fifo.EoF());
	fifo.Close();
	ASSERT_FALSE("not writable after close", fifo.IsWritable());
	ASSERT_TRUE("still readable after close", fifo.IsReadable());
	ASSERT_FALSE("write after close fails", fifo.Write("X"));
	ASSERT_EQUAL("size unchanged after failed write", fifo.Size(), StormByte::ByteSize{3});
	BinaryData out;
	ASSERT_TRUE("extract after close ok", fifo.Extract(0, out));
	ASSERT_EQUAL("content after close", BytesToText(out), std::string("ABC"));
	ASSERT_TRUE("eof after drain", fifo.EoF());
	RETURN_TEST("test_fifo_close_rejects_writes", 0);
}

int test_fifo_seterror_blocks_all() {
	FIFO fifo;
	(void)fifo.Write("DATA");
	fifo.SetError();
	ASSERT_FALSE("not writable on error", fifo.IsWritable());
	ASSERT_FALSE("not readable on error", fifo.IsReadable());
	ASSERT_TRUE("eof on error", fifo.EoF());
	ASSERT_FALSE("write fails on error", fifo.Write("X"));
	BinaryData out;
	ASSERT_FALSE("read fails on error", fifo.Read(0, out));
	ASSERT_FALSE("extract fails on error", fifo.Extract(0, out));
	RETURN_TEST("test_fifo_seterror_blocks_all", 0);
}

// -------------------
// Construct
// -------------------

int test_fifo_copy_ctor_assign() {
	FIFO a;
	(void)a.Write(std::string("AB"));
	FIFO b(a);
	ASSERT_EQUAL("copy ctor size", a.Size(), b.Size());
	BinaryData out1, out2;
	(void)b.Extract(2, out1);
	ASSERT_EQUAL("copy ctor content", std::string("AB"), BytesToText(out1));
	FIFO c;
	c = a;
	ASSERT_EQUAL("copy assign size", a.Size(), c.Size());
	(void)c.Extract(2, out2);
	ASSERT_EQUAL("copy assign content", std::string("AB"), BytesToText(out2));
	RETURN_TEST("test_fifo_copy_ctor_assign", 0);
}

int test_fifo_default_ctor() {
	FIFO fifo;
	ASSERT_TRUE("default ctor empty", fifo.Empty());
	ASSERT_EQUAL("default ctor size", StormByte::ByteSize{0}, fifo.Size());
	RETURN_TEST("test_fifo_default_ctor", 0);
}

int test_fifo_equality() {
	FIFO a;
	FIFO b;
	(void)a.Write("ABC");
	(void)b.Write("ABC");
	ASSERT_TRUE("fifo equal same content", a == b);
	ASSERT_FALSE("fifo unequal same content", a != b);
	BinaryData tmp;
	(void)a.Read(1, tmp);
	ASSERT_FALSE("fifo unequal after read changes position", a == b);
	(void)b.Read(1, tmp);
	ASSERT_TRUE("fifo equal after syncing read position", a == b);
	(void)b.Write("D");
	ASSERT_FALSE("fifo not equal after different content", a == b);
	RETURN_TEST("test_fifo_equality", 0);
}

int test_fifo_move_ctor_assign() {
	FIFO a;
	(void)a.Write(std::string("XY"));
	FIFO b(std::move(a));
	ASSERT_EQUAL("move ctor size", StormByte::ByteSize{2}, b.Size());
	ASSERT_TRUE("move ctor a empty", a.Empty());
	FIFO c;
	c = std::move(b);
	ASSERT_EQUAL("move assign size", StormByte::ByteSize{2}, c.Size());
	ASSERT_TRUE("move assign b empty", b.Empty());
	RETURN_TEST("test_fifo_move_ctor_assign", 0);
}

int test_fifo_polymorphic_interface_abi() {
	std::unique_ptr<ReadWrite> fifo = std::make_unique<FIFO>();
	ReadOnly& reader = *fifo;
	WriteOnly& writer = *fifo;
	BinaryData copy {std::byte{'A'}};
	BinaryData moved {std::byte{'B'}};
	FIFO source;
	ASSERT_TRUE("write source", source.Write("CD"));
	ASSERT_TRUE("write copy", writer.Write(0, copy));
	ASSERT_TRUE("write move", writer.Write(0, std::move(moved)));
	ASSERT_TRUE("write readonly copy", writer.Write(0, static_cast<const ReadOnly&>(source)));
	source.Seek(0, Position::Absolute);
	ASSERT_TRUE("write readonly move", writer.Write(0, std::move(source)));
	ASSERT_TRUE("writable", writer.IsWritable());
	ASSERT_EQUAL("size", reader.Size(), StormByte::ByteSize{6});
	ASSERT_EQUAL("available", reader.Available(), StormByte::ByteSize{6});
	ASSERT_FALSE("empty", reader.Empty());
	ASSERT_TRUE("readable", reader.IsReadable());
	BinaryData peek;
	ASSERT_TRUE("peek", reader.Peek(1, peek));
	BinaryData read;
	ASSERT_TRUE("read", reader.Read(1, read));
	reader.Seek(0, Position::Absolute);
	ASSERT_TRUE("drop", reader.Drop(1));
	BinaryData extracted;
	ASSERT_TRUE("extract", reader.Extract(1, extracted));
	reader.Clean();
	reader.Clear();
	writer.Close();
	ASSERT_TRUE("eof after close", reader.EoF());
	BinaryData until_eof;
	reader.ReadUntilEoF(until_eof);
	reader.ExtractUntilEoF(until_eof);
	fifo.reset();
	RETURN_TEST("test_fifo_polymorphic_interface_abi", 0);
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
	ASSERT_EQUAL("test_fifo_hexdump exact match", expected, dump);
	RETURN_TEST("test_fifo_hexdump", 0);
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
	ASSERT_EQUAL("test_fifo_hexdump_mixed exact match", expected, dump);
	RETURN_TEST("test_fifo_hexdump_mixed", 0);
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
	ASSERT_EQUAL("test_fifo_hexdump_offset exact match", expected, dump);
	RETURN_TEST("test_fifo_hexdump_offset", 0);
}

int test_fifo_hexdump_status_closed() {
	FIFO fifo;
	(void)fifo.Write("AB");
	fifo.Close();
	const std::string dump = std::string(fifo.HexDump(8, 0));
	ASSERT_TRUE("hexdump mentions closed", dump.find("closed") != std::string::npos);
	ASSERT_TRUE("hexdump mentions ok (no error)", dump.find("ok") != std::string::npos);
	RETURN_TEST("test_fifo_hexdump_status_closed", 0);
}

int test_fifo_hexdump_status_error() {
	FIFO fifo;
	(void)fifo.Write("AB");
	fifo.SetError();
	const std::string dump = std::string(fifo.HexDump(8, 0));
	ASSERT_TRUE("hexdump mentions error", dump.find("error") != std::string::npos);
	RETURN_TEST("test_fifo_hexdump_status_error", 0);
}

// -------------------
// Peek / drop
// -------------------

int test_fifo_peek_after_seek() {
	FIFO fifo;
	(void)fifo.Write(std::string("0123456789"));
	fifo.Seek(5, Position::Absolute);
	BinaryData peek;
	ASSERT_TRUE("peek after seek returned", fifo.Peek(3, peek));
	ASSERT_EQUAL("peek after seek content", BytesToText(peek), std::string("567"));
	BinaryData read;
	ASSERT_TRUE("read after peek returned", fifo.Read(3, read));
	ASSERT_EQUAL("read after peek content", BytesToText(read), std::string("567"));
	RETURN_TEST("test_fifo_peek_after_seek", 0);
}

int test_fifo_peek_all_available() {
	FIFO fifo;
	(void)fifo.Write(std::string("WORLD"));
	BinaryData peek_all;
	ASSERT_TRUE("peek all returned", fifo.Peek(0, peek_all));
	ASSERT_EQUAL("peek all content", BytesToText(peek_all), std::string("WORLD"));
	BinaryData read1;
	ASSERT_TRUE("read returned", fifo.Read(2, read1));
	BinaryData peek_remaining;
	ASSERT_TRUE("peek remaining returned", fifo.Peek(0, peek_remaining));
	ASSERT_EQUAL("peek remaining content", BytesToText(peek_remaining), std::string("RLD"));
	RETURN_TEST("test_fifo_peek_all_available", 0);
}

int test_fifo_peek_basic() {
	FIFO fifo;
	(void)fifo.Write(std::string("HELLO"));
	BinaryData peek1;
	ASSERT_TRUE("peek returned", fifo.Peek(3, peek1));
	ASSERT_EQUAL("peek content", BytesToText(peek1), std::string("HEL"));
	BinaryData peek2;
	ASSERT_TRUE("peek2 returned", fifo.Peek(3, peek2));
	ASSERT_EQUAL("peek2 content", BytesToText(peek2), std::string("HEL"));
	BinaryData read1;
	ASSERT_TRUE("read returned", fifo.Read(3, read1));
	ASSERT_EQUAL("read content matches peek", BytesToText(read1), std::string("HEL"));
	BinaryData peek3;
	ASSERT_TRUE("peek3 returned", fifo.Peek(2, peek3));
	ASSERT_EQUAL("peek3 content", BytesToText(peek3), std::string("LO"));
	RETURN_TEST("test_fifo_peek_basic", 0);
}

int test_fifo_peek_insufficient_data() {
	FIFO fifo;
	(void)fifo.Write(std::string("ABC"));
	BinaryData peek;
	ASSERT_FALSE("peek insufficient returned error", fifo.Peek(10, peek));
	RETURN_TEST("test_fifo_peek_insufficient_data", 0);
}

int test_fifo_skip_basic() {
	FIFO fifo;
	(void)fifo.Write(std::string("ABCDEFG"));
	(void)fifo.Drop(3);
	ASSERT_EQUAL("skip basic size", fifo.Size(), StormByte::ByteSize{4});
	BinaryData out;
	(void)fifo.Extract(0, out);
	ASSERT_EQUAL("extract after skip content", BytesToText(out), std::string("DEFG"));
	RETURN_TEST("test_fifo_skip_basic", 0);
}

int test_fifo_skip_with_readpos() {
	FIFO fifo;
	(void)fifo.Write(std::string("0123456789"));
	BinaryData r;
	ASSERT_TRUE("read before skip", fifo.Read(3, r));
	(void)fifo.Drop(4);
	ASSERT_EQUAL("size after skip with readpos", fifo.Size(), StormByte::ByteSize{3});
	BinaryData out;
	(void)fifo.Extract(0, out);
	ASSERT_EQUAL("content after skip with readpos", BytesToText(out), std::string("789"));
	RETURN_TEST("test_fifo_skip_with_readpos", 0);
}

// -------------------
// Read / extract
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
			BinaryData out;
			(void)fifo.Extract(len / 2, out);
			std::string got = BytesToText(out);
			std::string exp = expected.substr(0, static_cast<std::size_t>(out.size()));
			ASSERT_EQUAL("stress phase1", exp, got);
			expected.erase(0, static_cast<std::size_t>(out.size()));
		}
	}
	for (int i = 0; i < 200; ++i) {
		int len = large(rng);
		std::string chunk = MakePattern(static_cast<std::size_t>(len));
		(void)fifo.Write(chunk);
		expected.append(chunk);
		if (i % 5 == 0) {
			BinaryData out;
			(void)fifo.Extract(len, out);
			std::string got = BytesToText(out);
			std::string exp = expected.substr(0, static_cast<std::size_t>(out.size()));
			ASSERT_EQUAL("stress phase2", exp, got);
			expected.erase(0, static_cast<std::size_t>(out.size()));
		}
	}
	BinaryData out;
	(void)fifo.Extract(0, out);
	ASSERT_EQUAL("stress final drain", expected, BytesToText(out));
	ASSERT_TRUE("stress empty", fifo.Empty());
	RETURN_TEST("test_fifo_buffer_stress", 0);
}

int test_fifo_clear() {
	FIFO fifo;
	(void)fifo.Write(std::string(100, 'A'));
	fifo.Clear();
	ASSERT_TRUE("clear empty", fifo.Empty());
	ASSERT_EQUAL("clear size", StormByte::ByteSize{0}, fifo.Size());
	RETURN_TEST("test_fifo_clear", 0);
}

int test_fifo_clear_with_data() {
	FIFO fifo;
	(void)fifo.Write(TextToBytes("X"));
	ASSERT_FALSE("has data before clear", fifo.Empty());
	fifo.Clear();
	ASSERT_TRUE("empty after clear", fifo.Empty());
	ASSERT_EQUAL("size is zero", fifo.Size(), StormByte::ByteSize{0});
	RETURN_TEST("test_fifo_clear_with_data", 0);
}

int test_fifo_extract_adjusts_read_position() {
	FIFO fifo;
	(void)fifo.Write(std::string("0123456789"));
	BinaryData r1;
	(void)fifo.Read(5, r1);
	ASSERT_EQUAL("read 5", BytesToText(r1), std::string("01234"));
	BinaryData e1;
	(void)fifo.Extract(3, e1);
	ASSERT_EQUAL("extract 3", BytesToText(e1), std::string("567"));
	ASSERT_EQUAL("size after extract", fifo.Size(), StormByte::ByteSize{7});
	BinaryData r2;
	(void)fifo.Read(2, r2);
	ASSERT_EQUAL("read after extract", BytesToText(r2), std::string("89"));
	RETURN_TEST("test_fifo_extract_adjusts_read_position", 0);
}

int test_fifo_extract_insufficient_data_error() {
	FIFO fifo;
	(void)fifo.Write(std::string("HELLO"));
	BinaryData result;
	ASSERT_FALSE("extract insufficient returns error", fifo.Extract(20, result));
	BinaryData result2;
	ASSERT_TRUE("extract with 0 succeeds", fifo.Extract(0, result2));
	ASSERT_EQUAL("extract returns available", result2.size(), StormByte::ByteSize{5});
	ASSERT_TRUE("buffer empty after extract all", fifo.Empty());
	RETURN_TEST("test_fifo_extract_insufficient_data_error", 0);
}

int test_fifo_read_after_position_beyond_size() {
	FIFO fifo;
	(void)fifo.Write(std::string("1234"));
	BinaryData r1;
	ASSERT_TRUE("read all data", fifo.Read(4, r1));
	ASSERT_EQUAL("read all data", BytesToText(r1), std::string("1234"));
	BinaryData result;
	ASSERT_FALSE("read beyond position returns error", fifo.Read(1, result));
	BinaryData result2;
	ASSERT_FALSE("read 0 at end returns error", fifo.Read(0, result2));
	RETURN_TEST("test_fifo_read_after_position_beyond_size", 0);
}

int test_fifo_read_all_nondestructive() {
	FIFO fifo;
	(void)fifo.Write(std::string("HELLO"));
	BinaryData out1;
	(void)fifo.Read(0, out1);
	ASSERT_EQUAL("read all content", BytesToText(out1), std::string("HELLO"));
	ASSERT_EQUAL("size unchanged", fifo.Size(), StormByte::ByteSize{5});
	ASSERT_FALSE("not empty after read", fifo.Empty());
	BinaryData tmp;
	ASSERT_FALSE("second read all empty", fifo.Read(0, tmp));
	RETURN_TEST("test_fifo_read_all_nondestructive", 0);
}

int test_fifo_read_default_all() {
	FIFO fifo;
	(void)fifo.Write(std::string("DATA"));
	BinaryData out;
	(void)fifo.Extract(0, out);
	ASSERT_EQUAL("read default all", BytesToText(out), std::string("DATA"));
	ASSERT_TRUE("read default all empty", fifo.Empty());
	RETURN_TEST("test_fifo_read_default_all", 0);
}

int test_fifo_read_insufficient_data_error() {
	FIFO fifo;
	(void)fifo.Write(std::string("ABC"));
	BinaryData result;
	ASSERT_FALSE("read insufficient returns error", fifo.Read(10, result));
	BinaryData result2;
	ASSERT_TRUE("read with 0 succeeds", fifo.Read(0, result2));
	ASSERT_EQUAL("read returns available", result2.size(), StormByte::ByteSize{3});
	RETURN_TEST("test_fifo_read_insufficient_data_error", 0);
}

int test_fifo_read_nondestructive() {
	FIFO fifo;
	(void)fifo.Write(std::string("ABCDEF"));
	BinaryData out1;
	(void)fifo.Read(3, out1);
	ASSERT_EQUAL("first read content", BytesToText(out1), std::string("ABC"));
	ASSERT_EQUAL("size unchanged after read", fifo.Size(), StormByte::ByteSize{6});
	BinaryData out2;
	(void)fifo.Read(3, out2);
	ASSERT_EQUAL("second read content", BytesToText(out2), std::string("DEF"));
	ASSERT_EQUAL("size still unchanged", fifo.Size(), StormByte::ByteSize{6});
	BinaryData out3;
	ASSERT_FALSE("third read error", fifo.Read(0, out3));
	RETURN_TEST("test_fifo_read_nondestructive", 0);
}

int test_fifo_read_span_all_available() {
	FIFO fifo;
	(void)fifo.Write("HelloWorld");
	BinaryData out;
	ASSERT_TRUE("read_span_all returned", fifo.Read(0, out));
	ASSERT_EQUAL("read_span_all size", out.size(), StormByte::ByteSize{10});
	ASSERT_EQUAL("read_span_all content", BytesToText(out), std::string("HelloWorld"));
	ASSERT_EQUAL("read_span_all no available", fifo.Available(), StormByte::ByteSize{0});
	RETURN_TEST("test_fifo_read_span_all_available", 0);
}

int test_fifo_read_span_basic() {
	FIFO fifo;
	(void)fifo.Write("ABCDEF");
	BinaryData out;
	ASSERT_TRUE("read_span returned", fifo.Read(3, out));
	ASSERT_EQUAL("read_span size", out.size(), StormByte::ByteSize{3});
	ASSERT_EQUAL("read_span first byte", static_cast<char>(out[0]), 'A');
	ASSERT_EQUAL("read_span second byte", static_cast<char>(out[1]), 'B');
	ASSERT_EQUAL("read_span third byte", static_cast<char>(out[2]), 'C');
	BinaryData read;
	ASSERT_TRUE("read after span returned", fifo.Read(3, read));
	ASSERT_EQUAL("read after span content", BytesToText(read), std::string("DEF"));
	RETURN_TEST("test_fifo_read_span_basic", 0);
}

int test_fifo_read_span_insufficient_data() {
	FIFO fifo;
	(void)fifo.Write("ABC");
	BinaryData span;
	ASSERT_FALSE("read_span_insufficient error", fifo.Read(10, span));
	BinaryData read;
	ASSERT_TRUE("read after failed span returned", fifo.Read(3, read));
	ASSERT_EQUAL("read after failed span", BytesToText(read), std::string("ABC"));
	RETURN_TEST("test_fifo_read_span_insufficient_data", 0);
}

int test_fifo_read_span_vs_read() {
	FIFO fifo1, fifo2;
	const std::string data = "ComparisonTest";
	(void)fifo1.Write(data);
	(void)fifo2.Write(data);
	BinaryData r1, r2;
	ASSERT_TRUE("span_vs_read first read ok", fifo1.Read(4, r1));
	ASSERT_TRUE("span_vs_read second read ok", fifo2.Read(4, r2));
	ASSERT_EQUAL("span_vs_read content", BytesToText(r1), BytesToText(r2));
	ASSERT_EQUAL("span_vs_read expected", BytesToText(r1), std::string("Comp"));
	ASSERT_EQUAL("span_vs_read fifo1 available", fifo1.Available(), fifo2.Available());
	RETURN_TEST("test_fifo_read_span_vs_read", 0);
}

int test_fifo_read_vs_extract() {
	FIFO fifo;
	(void)fifo.Write(std::string("123456"));
	BinaryData r1;
	(void)fifo.Read(2, r1);
	ASSERT_EQUAL("read content", BytesToText(r1), std::string("12"));
	ASSERT_EQUAL("size after read", fifo.Size(), StormByte::ByteSize{6});
	BinaryData e1;
	(void)fifo.Extract(2, e1);
	ASSERT_EQUAL("extract content", BytesToText(e1), std::string("34"));
	ASSERT_EQUAL("size after extract", fifo.Size(), StormByte::ByteSize{4});
	BinaryData r2;
	(void)fifo.Read(2, r2);
	ASSERT_EQUAL("read after extract", BytesToText(r2), std::string("56"));
	RETURN_TEST("test_fifo_read_vs_extract", 0);
}

int test_fifo_read_with_wrap() {
	FIFO fifo;
	(void)fifo.Write("ABCDE");
	BinaryData temp;
	(void)fifo.Extract(2, temp);
	(void)fifo.Write("12");
	BinaryData out;
	(void)fifo.Read(0, out);
	ASSERT_EQUAL("read wrap content", BytesToText(out), std::string("CDE12"));
	ASSERT_EQUAL("size unchanged wrap", fifo.Size(), StormByte::ByteSize{5});
	RETURN_TEST("test_fifo_read_with_wrap", 0);
}

int test_fifo_write_read_vector() {
	FIFO fifo;
	const std::string s = "Hello";
	(void)fifo.Write(s);
	BinaryData data;
	(void)fifo.Extract(StormByte::ByteSize{s.size()}, data);
	ASSERT_EQUAL("test_fifo_write_read_vector", s, BytesToText(data));
	ASSERT_TRUE("test_fifo_write_read_vector", fifo.Empty());
	RETURN_TEST("test_fifo_write_read_vector", 0);
}

int test_fifo_wrap_around() {
	FIFO fifo;
	(void)fifo.Write("ABCDE");
	BinaryData r1, all;
	(void)fifo.Extract(2, r1);
	ASSERT_EQUAL("test_fifo_wrap_around r1", std::string("AB"), BytesToText(r1));
	(void)fifo.Write("1234");
	(void)fifo.Extract(7, all);
	ASSERT_EQUAL("test_fifo_wrap_around size", StormByte::ByteSize{7}, all.size());
	ASSERT_EQUAL("test_fifo_wrap_around content", std::string("CDE1234"), BytesToText(all));
	ASSERT_TRUE("test_fifo_wrap_around empty", fifo.Empty());
	RETURN_TEST("test_fifo_wrap_around", 0);
}

// -------------------
// Seek
// -------------------

int test_fifo_seek_absolute() {
	FIFO fifo;
	(void)fifo.Write(std::string("ABCDEFGHIJ"));
	BinaryData r1;
	fifo.Seek(3, Position::Absolute);
	(void)fifo.Read(3, r1);
	ASSERT_EQUAL("seek absolute 3", BytesToText(r1), std::string("DEF"));
	fifo.Seek(0, Position::Absolute);
	BinaryData r2;
	(void)fifo.Read(2, r2);
	ASSERT_EQUAL("seek absolute 0", BytesToText(r2), std::string("AB"));
	fifo.Seek(7, Position::Absolute);
	BinaryData r3;
	(void)fifo.Read(3, r3);
	ASSERT_EQUAL("seek absolute 7", BytesToText(r3), std::string("HIJ"));
	fifo.Seek(100, Position::Absolute);
	BinaryData tmp;
	ASSERT_FALSE("seek beyond size", fifo.Read(0, tmp));
	RETURN_TEST("test_fifo_seek_absolute", 0);
}

int test_fifo_seek_after_extract() {
	FIFO fifo;
	(void)fifo.Write(std::string("ABCDEFGHIJKLMNO"));
	BinaryData r1;
	(void)fifo.Read(5, r1);
	ASSERT_EQUAL("read before extract", BytesToText(r1), std::string("ABCDE"));
	BinaryData e1;
	(void)fifo.Extract(3, e1);
	ASSERT_EQUAL("extract 3", BytesToText(e1), std::string("FGH"));
	ASSERT_EQUAL("size after extract", fifo.Size(), StormByte::ByteSize{12});
	fifo.Seek(0, Position::Absolute);
	BinaryData r2;
	(void)fifo.Read(3, r2);
	ASSERT_EQUAL("seek absolute after extract", BytesToText(r2), std::string("ABC"));
	fifo.Seek(5, Position::Absolute);
	BinaryData r3;
	(void)fifo.Read(3, r3);
	ASSERT_EQUAL("seek to middle after extract", BytesToText(r3), std::string("IJK"));
	RETURN_TEST("test_fifo_seek_after_extract", 0);
}

int test_fifo_seek_relative() {
	FIFO fifo;
	(void)fifo.Write(std::string("0123456789"));
	BinaryData r1;
	(void)fifo.Read(2, r1);
	ASSERT_EQUAL("initial read", BytesToText(r1), std::string("01"));
	fifo.Seek(3, Position::Relative);
	BinaryData r2;
	(void)fifo.Read(2, r2);
	ASSERT_EQUAL("seek relative +3", BytesToText(r2), std::string("56"));
	fifo.Seek(2, Position::Relative);
	BinaryData r3;
	(void)fifo.Read(1, r3);
	ASSERT_EQUAL("seek relative +2", BytesToText(r3), std::string("9"));
	fifo.Seek(100, Position::Relative);
	BinaryData tmp;
	ASSERT_FALSE("seek relative beyond", fifo.Read(0, tmp));
	RETURN_TEST("test_fifo_seek_relative", 0);
}

int test_fifo_seek_relative_from_current() {
	FIFO fifo;
	(void)fifo.Write(std::string("ABCDEFGHIJ"));
	BinaryData r1;
	ASSERT_TRUE("initial read", fifo.Read(2, r1));
	ASSERT_EQUAL("initial read", BytesToText(r1), std::string("AB"));
	fifo.Seek(0, Position::Relative);
	BinaryData r2;
	ASSERT_TRUE("seek relative 0", fifo.Read(2, r2));
	ASSERT_EQUAL("seek relative 0", BytesToText(r2), std::string("CD"));
	fifo.Seek(1, Position::Absolute);
	BinaryData r3;
	ASSERT_TRUE("seek back to 1", fifo.Read(3, r3));
	ASSERT_EQUAL("seek back to 1", BytesToText(r3), std::string("BCD"));
	RETURN_TEST("test_fifo_seek_relative_from_current", 0);
}

int test_fifo_seek_with_wrap() {
	FIFO fifo;
	(void)fifo.Write("ABCDEFGHIJ");
	BinaryData e1;
	(void)fifo.Extract(5, e1);
	ASSERT_EQUAL("size after first extract", fifo.Size(), StormByte::ByteSize{5});
	(void)fifo.Write("12345");
	ASSERT_EQUAL("size after wrap write", fifo.Size(), StormByte::ByteSize{10});
	fifo.Seek(0, Position::Absolute);
	BinaryData r1;
	(void)fifo.Read(5, r1);
	ASSERT_EQUAL("seek 0 after wrap", BytesToText(r1), std::string("FGHIJ"));
	fifo.Seek(5, Position::Absolute);
	BinaryData r2;
	(void)fifo.Read(5, r2);
	ASSERT_EQUAL("seek 5 after wrap", BytesToText(r2), std::string("12345"));
	RETURN_TEST("test_fifo_seek_with_wrap", 0);
}

// -------------------
// Write
// -------------------

int test_fifo_adopt_storage_move_write() {
	FIFO fifo;
	auto v = TextToBytes("MOVE");
	(void)fifo.Write(std::move(v));
	ASSERT_EQUAL("test_fifo_adopt_storage_move_write size", fifo.Size(), StormByte::ByteSize{4});
	BinaryData out;
	(void)fifo.Extract(4, out);
	ASSERT_EQUAL("test_fifo_adopt_storage_move_write content", BytesToText(out), std::string("MOVE"));
	ASSERT_TRUE("test_fifo_adopt_storage_move_write empty", fifo.Empty());
	RETURN_TEST("test_fifo_adopt_storage_move_write", 0);
}

int test_fifo_move_steal_preserves_read_position() {
	FIFO src;
	(void)src.Write(std::string("ABCDE"));
	BinaryData r;
	ASSERT_TRUE("advance read returned", src.Read(2, r));
	FIFO dst;
	ASSERT_TRUE("move write returned true", dst.Write(std::move(src)));
	BinaryData out;
	ASSERT_TRUE("dst read returned", dst.Read(0, out));
	ASSERT_EQUAL("dst remaining after move preserves position", BytesToText(out), std::string("CDE"));
	RETURN_TEST("test_fifo_move_steal_preserves_read_position", 0);
}

int test_fifo_write_basic() {
	FIFO fifo;
	(void)fifo.Write(std::string("1234"));
	ASSERT_EQUAL("write size", StormByte::ByteSize{4}, fifo.Size());
	RETURN_TEST("test_fifo_write_basic", 0);
}

int test_fifo_write_full_telling_zero() {
	FIFO fifo;
	BinaryData data(StormByte::ByteSize{10}, std::byte{0xFF});
	ASSERT_TRUE("write zero bytes returned", fifo.Write(0, data));
	ASSERT_EQUAL("size after write zero", fifo.Size(), StormByte::ByteSize{10});
	RETURN_TEST("test_fifo_write_full_telling_zero", 0);
}

int test_fifo_write_multiple() {
	FIFO fifo;
	(void)fifo.Write(std::string(10, 'Z'));
	ASSERT_EQUAL("write size", StormByte::ByteSize{10}, fifo.Size());
	(void)fifo.Write(std::string(5, 'Y'));
	ASSERT_EQUAL("size after second write", StormByte::ByteSize{15}, fifo.Size());
	RETURN_TEST("test_fifo_write_multiple", 0);
}

int test_fifo_write_partial_count() {
	FIFO fifo;
	auto data = TextToBytes("PARTIAL");
	ASSERT_TRUE("partial write ok", fifo.Write(3, data));
	ASSERT_EQUAL("partial write size", StormByte::ByteSize{3}, fifo.Size());
	BinaryData out;
	(void)fifo.Extract(0, out);
	ASSERT_EQUAL("partial write content", std::string("PAR"), BytesToText(out));
	RETURN_TEST("test_fifo_write_partial_count", 0);
}

int test_fifo_write_remaining_fifo() {
	FIFO src;
	(void)src.Write(std::string("HELLO"));
	BinaryData r;
	(void)src.Read(2, r);
	FIFO dst;
	(void)dst.Write(std::string("START"));
	const auto before = dst.Size();
	const auto src_remaining = src.Available();
	ASSERT_TRUE("fifo write whole returned", dst.Write(src));
	ASSERT_EQUAL("fifo write remaining size", dst.Size(), before + src_remaining);
	BinaryData all;
	(void)dst.Extract(0, all);
	ASSERT_EQUAL("fifo write remaining content", BytesToText(all), std::string("STARTLLO"));
	FIFO src2;
	(void)src2.Write(std::string("WORLD"));
	ASSERT_TRUE("fifo write whole rvalue returned", dst.Write(std::move(src2)));
	BinaryData tail;
	(void)dst.Extract(0, tail);
	ASSERT_EQUAL("fifo write whole rvalue content", BytesToText(tail), std::string("WORLD"));
	RETURN_TEST("test_fifo_write_remaining_fifo", 0);
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
	BinaryData out;
	(void)fifo.Extract(6, out);
	ASSERT_EQUAL("write vector+rvalue", std::string("ABCDEF"), BytesToText(out));
	RETURN_TEST("test_fifo_write_vector_and_rvalue", 0);
}

int main() {
	int result = 0;

	// -------------------
	// Available
	// -------------------
	result += test_fifo_available_bytes();
	result += test_fifo_available_bytes_after_ops();

	// -------------------
	// Close / error
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

	// -------------------
	// Peek / drop
	// -------------------
	result += test_fifo_peek_after_seek();
	result += test_fifo_peek_all_available();
	result += test_fifo_peek_basic();
	result += test_fifo_peek_insufficient_data();
	result += test_fifo_skip_basic();
	result += test_fifo_skip_with_readpos();

	// -------------------
	// Read / extract
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
