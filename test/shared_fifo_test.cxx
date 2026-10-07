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

#include <StormByte/buffer/shared_fifo.hxx>
#include <StormByte/safe/binary.hxx>
#include <StormByte/test_handlers.h>

#include <atomic>
#include <chrono>
#include <cstddef>
#include <iostream>
#include <memory>
#include <string>
#include <string_view>
#include <thread>
#include <vector>

using StormByte::Buffer::FIFO;
using StormByte::Buffer::Position;
using StormByte::Buffer::ReadOnly;
using StormByte::Buffer::ReadWrite;
using StormByte::Buffer::SharedFIFO;
using StormByte::Buffer::WriteOnly;
using StormByte::Safe::Binary;

namespace {
	std::string BytesToText(const Binary& data) {
		if (data.empty())
			return {};
		return std::string(reinterpret_cast<const char*>(data.data()),
			static_cast<std::size_t>(data.size()));
	}

	/**
	 * @brief Consumer-defined SharedFIFO with a DLL-safe formatting hook.
	 */
	class HeaderSharedFIFO final: public SharedFIFO {
		protected:
			/**
			 * @brief Return consumer-owned header text without exporting a stream.
			 * @return DLL-safe custom header, including an embedded NUL.
			 */
			StormByte::Safe::String HexDumpHeader() const noexcept override {
				return StormByte::Safe::String{std::string_view("shared\0header", 13)};
			}
	};
}

// -------------------
// Available
// -------------------

int test_shared_fifo_available_bytes_basic() {
	SharedFIFO fifo;
	ASSERT_EQUAL(fifo.Available(), StormByte::ByteSize{0});
	(void)fifo.Write("HELLO WORLD");
	ASSERT_EQUAL(fifo.Available(), StormByte::ByteSize{11});
	Binary r1;
	(void)fifo.Read(5, r1);
	ASSERT_EQUAL(fifo.Available(), StormByte::ByteSize{6});
	fifo.Seek(2, Position::Absolute);
	ASSERT_EQUAL(fifo.Available(), StormByte::ByteSize{9});
	Binary e1;
	(void)fifo.Extract(3, e1);
	ASSERT_EQUAL(fifo.Available(), StormByte::ByteSize{6});
	RETURN_TEST(0);
}

int test_shared_fifo_available_bytes_concurrent() {
	SharedFIFO fifo;
	std::atomic<std::size_t> available_checks{0};
	std::atomic<bool> done{false};
	std::thread writer([&] {
		for (int i = 0; i < 10; ++i) {
			(void)fifo.Write("DATA");
			std::this_thread::sleep_for(std::chrono::milliseconds(5));
		}
		done.store(true);
		fifo.Close();
	});
	std::thread reader([&] {
		while (!done.load() || !fifo.Empty()) {
			if (fifo.Available() > 0) {
				Binary data;
				(void)fifo.Extract(0, data);
				available_checks.fetch_add(1);
			}
			std::this_thread::sleep_for(std::chrono::milliseconds(3));
		}
	});
	writer.join();
	reader.join();
	ASSERT_TRUE(available_checks.load() > 0);
	ASSERT_TRUE(fifo.Empty());
	ASSERT_EQUAL(fifo.Available(), StormByte::ByteSize{0});
	RETURN_TEST(0);
}

// -------------------
// Close
// -------------------

int test_shared_fifo_blocking_read_insufficient_not_closed() {
	SharedFIFO fifo;
	(void)fifo.Write("12");
	std::atomic<bool> read_started{false};
	std::atomic<bool> read_got_error{false};
	std::atomic<bool> read_finished{false};
	std::thread reader([&] {
		read_started.store(true);
		Binary out;
		const auto result = fifo.Read(10, out);
		read_finished.store(true);
		read_got_error.store(!result);
	});
	std::this_thread::sleep_for(std::chrono::milliseconds(10));
	ASSERT_TRUE(read_started.load());
	ASSERT_FALSE(read_finished.load());
	fifo.Close();
	reader.join();
	ASSERT_TRUE(read_finished.load());
	ASSERT_TRUE(read_got_error.load());
	RETURN_TEST(0);
}

int test_shared_fifo_close_suppresses_writes() {
	SharedFIFO fifo;
	(void)fifo.Write(std::string("ABC"));
	ASSERT_EQUAL(fifo.Size(), StormByte::ByteSize{3});
	fifo.Close();
	(void)fifo.Write(std::string("DEF"));
	ASSERT_EQUAL(fifo.Size(), StormByte::ByteSize{3});
	Binary out;
	ASSERT_TRUE(fifo.Extract(0, out));
	ASSERT_EQUAL(BytesToText(out), std::string("ABC"));
	RETURN_TEST(0);
}

int test_shared_fifo_equality() {
	SharedFIFO sa;
	SharedFIFO sb;
	(void)sa.Write("HELLO");
	(void)sb.Write("HELLO");
	ASSERT_TRUE(sa == sb);
	ASSERT_FALSE(sa != sb);
	sa.Close();
	ASSERT_FALSE(sa == sb);
	sb.Close();
	ASSERT_TRUE(sa == sb);
	RETURN_TEST(0);
}

int test_shared_fifo_extract_closed_no_data_nonblocking() {
	SharedFIFO fifo;
	fifo.Close();
	ASSERT_FALSE(fifo.IsWritable());
	ASSERT_EQUAL(fifo.Size(), StormByte::ByteSize{0});
	Binary out;
	ASSERT_FALSE(fifo.Extract(10, out));
	RETURN_TEST(0);
}

int test_shared_fifo_extract_insufficient_closed_returns_available() {
	SharedFIFO fifo;
	(void)fifo.Write("HELLO");
	fifo.Close();
	Binary out;
	ASSERT_FALSE(fifo.Extract(100, out));
	ASSERT_EQUAL(fifo.Size(), StormByte::ByteSize{5});
	Binary all;
	ASSERT_TRUE(fifo.Read(0, all));
	ASSERT_EQUAL(BytesToText(all), std::string("HELLO"));
	RETURN_TEST(0);
}

int test_shared_fifo_read_closed_no_data_nonblocking() {
	SharedFIFO fifo;
	fifo.Close();
	ASSERT_FALSE(fifo.IsWritable());
	ASSERT_EQUAL(fifo.Size(), StormByte::ByteSize{0});
	Binary out;
	ASSERT_FALSE(fifo.Read(10, out));
	RETURN_TEST(0);
}

int test_shared_fifo_read_insufficient_closed_returns_available() {
	SharedFIFO fifo;
	(void)fifo.Write("ABC");
	fifo.Close();
	Binary out;
	ASSERT_FALSE(fifo.Read(10, out));
	ASSERT_EQUAL(fifo.Available(), StormByte::ByteSize{3});
	RETURN_TEST(0);
}

// -------------------
// HexDump
// -------------------

int test_shared_fifo_hexdump() {
	SharedFIFO sf;
	(void)sf.Write("0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcd");
	std::string dump = static_cast<std::string>(sf.HexDump(8, 0));
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

int test_shared_fifo_hexdump_mixed() {
	SharedFIFO sf;
	Binary v;
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
	(void)sf.Write(std::move(v));
	std::string dump = static_cast<std::string>(sf.HexDump(8, 0));
	std::string expected;
	expected += "Size: 10 bytes\n";
	expected += "Read Position: 0\n";
	expected += "Status: open / ok\n";
	expected += "00000000: 41 00 1F 20 41 7E 7F 80   A.. A~..\n";
	expected += std::string("00000008: FF 30") + std::string(21, ' ') + ".0";
	ASSERT_EQUAL(expected, dump);
	RETURN_TEST(0);
}

int test_shared_fifo_hexdump_offset() {
	SharedFIFO sf;
	(void)sf.Write("0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcd");
	sf.Seek(5, Position::Absolute);
	std::string dump = static_cast<std::string>(sf.HexDump(8, 0));
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

int test_shared_fifo_safe_header_hook() {
	HeaderSharedFIFO fifo;
	ASSERT_TRUE(fifo.Write("A"));
	const FIFO& base = fifo;
	const auto dump = base.HexDump();
	ASSERT_TRUE(std::string_view(dump).starts_with(std::string_view("shared\0header\n", 14)));
	ASSERT_TRUE(dump.contains("41"));
	ASSERT_EQUAL(fifo.Available(), StormByte::ByteSize{1});
	RETURN_TEST(0);
}

// -------------------
// Peek
// -------------------

int test_shared_fifo_peek_all_available() {
	SharedFIFO fifo;
	(void)fifo.Write(std::string("WORLD"));
	Binary peek_all;
	ASSERT_TRUE(fifo.Peek(0, peek_all));
	ASSERT_EQUAL(BytesToText(peek_all), std::string("WORLD"));
	Binary read_all;
	ASSERT_TRUE(fifo.Read(0, read_all));
	ASSERT_EQUAL(BytesToText(read_all), std::string("WORLD"));
	RETURN_TEST(0);
}

int test_shared_fifo_peek_basic() {
	SharedFIFO fifo;
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
	RETURN_TEST(0);
}

int test_shared_fifo_peek_concurrent() {
	SharedFIFO fifo;
	(void)fifo.Write(std::string("DATA"));
	Binary peek;
	ASSERT_TRUE(fifo.Peek(4, peek));
	ASSERT_EQUAL(BytesToText(peek), std::string("DATA"));
	Binary read;
	ASSERT_TRUE(fifo.Read(4, read));
	ASSERT_EQUAL(BytesToText(read), std::string("DATA"));
	RETURN_TEST(0);
}

int test_shared_fifo_skip_basic() {
	SharedFIFO sf;
	(void)sf.Write(std::string("ABCDEFG"));
	(void)sf.Drop(3);
	ASSERT_EQUAL(sf.Size(), StormByte::ByteSize{4});
	Binary out;
	ASSERT_TRUE(sf.Extract(0, out));
	ASSERT_EQUAL(BytesToText(out), std::string("DEFG"));
	RETURN_TEST(0);
}

int test_shared_fifo_skip_with_readpos() {
	SharedFIFO sf;
	(void)sf.Write(std::string("0123456789"));
	Binary r;
	ASSERT_TRUE(sf.Read(3, r));
	(void)sf.Drop(4);
	ASSERT_EQUAL(sf.Size(), StormByte::ByteSize{3});
	Binary out;
	ASSERT_TRUE(sf.Extract(0, out));
	ASSERT_EQUAL(BytesToText(out), std::string("789"));
	RETURN_TEST(0);
}

// -------------------
// Threading
// -------------------

int test_shared_fifo_concurrent_seek_and_read() {
	SharedFIFO fifo;
	(void)fifo.Write(std::string("0123456789"));
	std::atomic<bool> seeker_done{false};
	std::string read_a, read_b;
	std::atomic<bool> reader_failed{false};
	std::thread seeker([&] {
		fifo.Seek(5, Position::Absolute);
		std::this_thread::sleep_for(std::chrono::milliseconds(2));
		fifo.Seek(2, Position::Relative);
		std::this_thread::sleep_for(std::chrono::milliseconds(2));
		fifo.Seek(1, Position::Absolute);
		fifo.Close();
		seeker_done.store(true);
	});
	std::thread reader([&] {
		Binary r1, r2;
		if (!fifo.Read(2, r1)) {
			reader_failed.store(true);
			return;
		}
		read_a = BytesToText(r1);
		std::this_thread::sleep_for(std::chrono::milliseconds(3));
		if (!fifo.Read(3, r2)) {
			reader_failed.store(true);
			return;
		}
		read_b = BytesToText(r2);
	});
	seeker.join();
	reader.join();
	ASSERT_FALSE(reader_failed.load());
	ASSERT_TRUE(seeker_done.load());
	ASSERT_TRUE(read_a.size() <= 2);
	ASSERT_TRUE(read_b.size() <= 3);
	auto within_digits = [](const std::string& s) {
		for (char c : s)
			if (c < '0' || c > '9')
				return false;
		return true;
	};
	ASSERT_TRUE(within_digits(read_a));
	ASSERT_TRUE(within_digits(read_b));
	RETURN_TEST(0);
}

int test_shared_fifo_extract_adjusts_read_position_concurrency() {
	SharedFIFO fifo;
	(void)fifo.Write(std::string("ABCDEFGH"));
	std::string r_before, r_after;
	std::atomic<bool> reader_failed{false};
	std::atomic<bool> first_read_done{false};
	std::thread reader([&] {
		Binary before, after;
		if (!fifo.Read(3, before)) {
			reader_failed.store(true);
			return;
		}
		r_before = BytesToText(before);
		first_read_done.store(true);
		std::this_thread::sleep_for(std::chrono::milliseconds(5));
		if (!fifo.Read(2, after)) {
			reader_failed.store(true);
			return;
		}
		r_after = BytesToText(after);
	});
	std::thread extractor([&] {
		while (!first_read_done.load())
			std::this_thread::sleep_for(std::chrono::microseconds(100));
		Binary e;
		(void)fifo.Extract(2, e);
	});
	reader.join();
	extractor.join();
	ASSERT_FALSE(reader_failed.load());
	ASSERT_EQUAL(r_before, std::string("ABC"));
	ASSERT_EQUAL(r_after, std::string("FG"));
	RETURN_TEST(0);
}

int test_shared_fifo_extract_blocking_and_close() {
	SharedFIFO fifo;
	std::atomic<bool> woke{false};
	std::atomic<bool> saw_writable{false};
	std::size_t extracted_size = 1234;
	std::thread t([&] {
		Binary out;
		const auto res = fifo.Extract(1, out);
		woke.store(true);
		saw_writable.store(fifo.IsWritable());
		extracted_size = res ? static_cast<std::size_t>(out.size()) : 0;
	});
	std::this_thread::sleep_for(std::chrono::milliseconds(5));
	fifo.Close();
	t.join();
	ASSERT_TRUE(woke.load());
	ASSERT_FALSE(saw_writable.load());
	ASSERT_EQUAL(extracted_size, static_cast<std::size_t>(0));
	RETURN_TEST(0);
}

int test_shared_fifo_growth_under_contention() {
	SharedFIFO fifo;
	const int iters = 100;
	std::thread producer([&] {
		for (int i = 0; i < iters; ++i)
			(void)fifo.Write(std::string(100 + (i % 50), 'Z'));
		fifo.Close();
	});
	std::size_t consumed = 0;
	std::thread consumer([&] {
		while (true) {
			Binary part;
			if (!fifo.Extract(128, part)) {
				if (fifo.Available() > 0) {
					Binary rem;
					if (fifo.Extract(0, rem) && !rem.empty())
						consumed += static_cast<std::size_t>(rem.size());
				}
				break;
			}
			if (part.empty() && fifo.EoF())
				break;
			consumed += static_cast<std::size_t>(part.size());
		}
	});
	producer.join();
	consumer.join();
	std::size_t expected = 0;
	for (int i = 0; i < iters; ++i)
		expected += static_cast<std::size_t>(100 + (i % 50));
	ASSERT_EQUAL(consumed, expected);
	RETURN_TEST(0);
}

int test_shared_fifo_multi_producer_single_consumer_counts() {
	SharedFIFO fifo;
	const int chunks = 200;
	std::atomic<bool> p1_done{false}, p2_done{false};
	std::thread producerA([&] {
		for (int i = 0; i < chunks; ++i)
			(void)fifo.Write(std::string("A"));
		p1_done.store(true);
	});
	std::thread producerB([&] {
		for (int i = 0; i < chunks; ++i)
			(void)fifo.Write(std::string("B"));
		p2_done.store(true);
	});
	std::string collected;
	std::thread consumer([&] {
		while (true) {
			Binary part;
			const auto res = fifo.Extract(1, part);
			if (!res || (part.empty() && fifo.EoF()))
				break;
			collected.append(BytesToText(part));
		}
	});
	producerA.join();
	producerB.join();
	fifo.Close();
	consumer.join();
	ASSERT_TRUE(p1_done.load() && p2_done.load());
	std::size_t countA = 0, countB = 0;
	for (char c : collected) {
		if (c == 'A')
			++countA;
		else if (c == 'B')
			++countB;
	}
	ASSERT_EQUAL(countA, static_cast<std::size_t>(chunks));
	ASSERT_EQUAL(countB, static_cast<std::size_t>(chunks));
	ASSERT_EQUAL(collected.size(), static_cast<std::size_t>(chunks * 2));
	RETURN_TEST(0);
}

int test_shared_fifo_multiple_consumers_total_coverage() {
	SharedFIFO fifo;
	const int total = 1000;
	std::thread producer([&] {
		(void)fifo.Write(std::string(total, 'X'));
		fifo.Close();
	});
	std::atomic<std::size_t> c1{0}, c2{0};
	std::thread consumer1([&] {
		std::size_t local = 0;
		while (true) {
			Binary part;
			const auto res = fifo.Extract(1, part);
			if (!res || (part.empty() && fifo.EoF()))
				break;
			local += static_cast<std::size_t>(part.size());
		}
		c1.store(local);
	});
	std::thread consumer2([&] {
		std::size_t local = 0;
		while (true) {
			Binary part;
			const auto res = fifo.Extract(1, part);
			if (!res || (part.empty() && fifo.EoF()))
				break;
			local += static_cast<std::size_t>(part.size());
		}
		c2.store(local);
	});
	producer.join();
	consumer1.join();
	consumer2.join();
	ASSERT_EQUAL(c1.load() + c2.load(), static_cast<std::size_t>(total));
	RETURN_TEST(0);
}

int test_shared_fifo_producer_consumer_blocking() {
	SharedFIFO fifo;
	std::atomic<bool> done{false};
	const std::string payload = "ABCDEFGHIJ";
	std::thread producer([&] {
		(void)fifo.Write(std::string(payload.begin(), payload.begin() + 4));
		std::this_thread::sleep_for(std::chrono::milliseconds(10));
		(void)fifo.Write(std::string(payload.begin() + 4, payload.end()));
		fifo.Close();
		done.store(true);
	});
	std::string collected;
	std::thread consumer([&] {
		while (true) {
			Binary part;
			if (!fifo.Read(3, part)) {
				if (fifo.Available() > 0) {
					Binary rem;
					if (fifo.Read(0, rem) && !rem.empty())
						collected.append(BytesToText(rem));
				}
				break;
			}
			if (part.empty() && fifo.EoF())
				break;
			collected.append(BytesToText(part));
			std::this_thread::sleep_for(std::chrono::milliseconds(1));
		}
	});
	producer.join();
	consumer.join();
	ASSERT_TRUE(done.load());
	ASSERT_EQUAL(collected, payload);
	RETURN_TEST(0);
}

// -------------------
// Write
// -------------------

int test_shared_fifo_multiple_spans_eof() {
	SharedFIFO fifo;
	(void)fifo.Write(std::string("ABCDEFGHIJ"));
	Binary s1, s2, s3;
	ASSERT_TRUE(fifo.Read(4, s1));
	ASSERT_EQUAL(s1.size(), StormByte::ByteSize{4});
	ASSERT_TRUE(fifo.Read(3, s2));
	ASSERT_EQUAL(s2.size(), StormByte::ByteSize{3});
	ASSERT_TRUE(fifo.Read(3, s3));
	ASSERT_EQUAL(s3.size(), StormByte::ByteSize{3});
	ASSERT_EQUAL(fifo.Available(), StormByte::ByteSize{0});
	ASSERT_FALSE(fifo.EoF());
	fifo.Close();
	ASSERT_TRUE(fifo.EoF());
	RETURN_TEST(0);
}

int test_shared_fifo_polymorphic_interface_abi() {
	std::unique_ptr<ReadWrite> fifo = std::make_unique<SharedFIFO>();
	ReadOnly& reader = *fifo;
	WriteOnly& writer = *fifo;
	Binary data {std::byte{'A'}, std::byte{'B'}};
	ASSERT_TRUE(writer.Write(0, std::move(data)));
	ASSERT_TRUE(writer.IsWritable());
	ASSERT_EQUAL(reader.Available(), StormByte::ByteSize{2});
	ASSERT_FALSE(reader.Empty());
	ASSERT_TRUE(reader.IsReadable());
	Binary peek;
	ASSERT_TRUE(reader.Peek(1, peek));
	Binary read;
	ASSERT_TRUE(reader.Read(1, read));
	reader.Seek(0, Position::Absolute);
	ASSERT_TRUE(reader.Drop(1));
	reader.Clean();
	writer.SetError();
	ASSERT_TRUE(reader.EoF());
	reader.Clear();
	Binary until_eof;
	reader.ReadUntilEoF(until_eof);
	reader.ExtractUntilEoF(until_eof);
	fifo.reset();
	RETURN_TEST(0);
}

int test_shared_fifo_wrap_boundary_blocking() {
	SharedFIFO fifo;
	(void)fifo.Write("ABCDE");
	Binary r1;
	ASSERT_TRUE(fifo.Read(3, r1));
	ASSERT_EQUAL(BytesToText(r1), std::string("ABC"));
	Binary e1;
	ASSERT_TRUE(fifo.Extract(2, e1));
	ASSERT_EQUAL(BytesToText(e1), std::string("DE"));
	(void)fifo.Write("12");
	fifo.Seek(0, Position::Absolute);
	Binary all;
	ASSERT_TRUE(fifo.Read(0, all));
	ASSERT_EQUAL(BytesToText(all).size(), static_cast<std::size_t>(5));
	RETURN_TEST(0);
}

int test_shared_fifo_write_span_basic() {
	SharedFIFO fifo;
	const char* msg = "SFPAN";
	Binary vec(reinterpret_cast<const std::byte*>(msg), StormByte::ByteSize{5});
	ASSERT_TRUE(fifo.Write(vec));
	Binary read;
	ASSERT_TRUE(fifo.Read(5, read));
	ASSERT_EQUAL(BytesToText(read), std::string("SFPAN"));
	RETURN_TEST(0);
}

int test_shared_fifo_write_whole_fifo() {
	SharedFIFO shared;
	FIFO src;
	(void)src.Write(std::string("ONE"));
	ASSERT_TRUE(shared.Write(src));
	Binary all;
	ASSERT_TRUE(shared.Extract(0, all));
	ASSERT_EQUAL(BytesToText(all), std::string("ONE"));
	FIFO src2;
	(void)src2.Write(std::string("TWO"));
	ASSERT_TRUE(shared.Write(std::move(src2)));
	Binary all2;
	ASSERT_TRUE(shared.Extract(0, all2));
	ASSERT_EQUAL(BytesToText(all2), std::string("TWO"));
	RETURN_TEST(0);
}

int main() {
	int result = 0;

	// -------------------
	// Available
	// -------------------
	result += test_shared_fifo_available_bytes_basic();
	result += test_shared_fifo_available_bytes_concurrent();

	// -------------------
	// Close
	// -------------------
	result += test_shared_fifo_blocking_read_insufficient_not_closed();
	result += test_shared_fifo_close_suppresses_writes();
	result += test_shared_fifo_equality();
	result += test_shared_fifo_extract_closed_no_data_nonblocking();
	result += test_shared_fifo_extract_insufficient_closed_returns_available();
	result += test_shared_fifo_read_closed_no_data_nonblocking();
	result += test_shared_fifo_read_insufficient_closed_returns_available();

	// -------------------
	// HexDump
	// -------------------
	result += test_shared_fifo_hexdump();
	result += test_shared_fifo_hexdump_mixed();
	result += test_shared_fifo_hexdump_offset();
	result += test_shared_fifo_safe_header_hook();

	// -------------------
	// Peek
	// -------------------
	result += test_shared_fifo_peek_all_available();
	result += test_shared_fifo_peek_basic();
	result += test_shared_fifo_peek_concurrent();
	result += test_shared_fifo_skip_basic();
	result += test_shared_fifo_skip_with_readpos();

	// -------------------
	// Threading
	// -------------------
	result += test_shared_fifo_concurrent_seek_and_read();
	result += test_shared_fifo_extract_adjusts_read_position_concurrency();
	result += test_shared_fifo_extract_blocking_and_close();
	result += test_shared_fifo_growth_under_contention();
	result += test_shared_fifo_multi_producer_single_consumer_counts();
	result += test_shared_fifo_multiple_consumers_total_coverage();
	result += test_shared_fifo_producer_consumer_blocking();

	// -------------------
	// Write
	// -------------------
	result += test_shared_fifo_multiple_spans_eof();
	result += test_shared_fifo_polymorphic_interface_abi();
	result += test_shared_fifo_wrap_boundary_blocking();
	result += test_shared_fifo_write_span_basic();
	result += test_shared_fifo_write_whole_fifo();

	if (result == 0)
		std::cout << "All tests passed!" << std::endl;
	else
		std::cout << result << " tests failed." << std::endl;
	return result;
}
