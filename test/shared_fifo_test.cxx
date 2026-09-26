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
#include <StormByte/test_handlers.h>

#include <atomic>
#include <chrono>
#include <cstddef>
#include <iostream>
#include <memory>
#include <string>
#include <thread>
#include <vector>

using StormByte::BinaryData;
using StormByte::Buffer::FIFO;
using StormByte::Buffer::Position;
using StormByte::Buffer::ReadOnly;
using StormByte::Buffer::ReadWrite;
using StormByte::Buffer::SharedFIFO;
using StormByte::Buffer::WriteOnly;

namespace {
	std::string BytesToText(const BinaryData& data) {
		if (data.empty())
			return {};
		return std::string(reinterpret_cast<const char*>(data.data()),
			static_cast<std::size_t>(data.size()));
	}
}

// -------------------
// Available
// -------------------

int test_shared_fifo_available_bytes_basic() {
	const std::string fn = "test_shared_fifo_available_bytes_basic";
	SharedFIFO fifo;
	ASSERT_EQUAL(fn, fifo.Available(), StormByte::ByteSize{0});
	(void)fifo.Write("HELLO WORLD");
	ASSERT_EQUAL(fn, fifo.Available(), StormByte::ByteSize{11});
	BinaryData r1;
	(void)fifo.Read(5, r1);
	ASSERT_EQUAL(fn, fifo.Available(), StormByte::ByteSize{6});
	fifo.Seek(2, Position::Absolute);
	ASSERT_EQUAL(fn, fifo.Available(), StormByte::ByteSize{9});
	BinaryData e1;
	(void)fifo.Extract(3, e1);
	ASSERT_EQUAL(fn, fifo.Available(), StormByte::ByteSize{6});
	RETURN_TEST(fn, 0);
}

int test_shared_fifo_available_bytes_concurrent() {
	const std::string fn = "test_shared_fifo_available_bytes_concurrent";
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
				BinaryData data;
				(void)fifo.Extract(0, data);
				available_checks.fetch_add(1);
			}
			std::this_thread::sleep_for(std::chrono::milliseconds(3));
		}
	});
	writer.join();
	reader.join();
	ASSERT_TRUE(fn, available_checks.load() > 0);
	ASSERT_TRUE(fn, fifo.Empty());
	ASSERT_EQUAL(fn, fifo.Available(), StormByte::ByteSize{0});
	RETURN_TEST(fn, 0);
}

// -------------------
// Close / error
// -------------------

int test_shared_fifo_blocking_read_insufficient_not_closed() {
	const std::string fn = "test_shared_fifo_blocking_read_insufficient_not_closed";
	SharedFIFO fifo;
	(void)fifo.Write("12");
	std::atomic<bool> read_started{false};
	std::atomic<bool> read_got_error{false};
	std::atomic<bool> read_finished{false};
	std::thread reader([&] {
		read_started.store(true);
		BinaryData out;
		const auto result = fifo.Read(10, out);
		read_finished.store(true);
		read_got_error.store(!result);
	});
	std::this_thread::sleep_for(std::chrono::milliseconds(10));
	ASSERT_TRUE(fn, read_started.load());
	ASSERT_FALSE(fn, read_finished.load());
	fifo.Close();
	reader.join();
	ASSERT_TRUE(fn, read_finished.load());
	ASSERT_TRUE(fn, read_got_error.load());
	RETURN_TEST(fn, 0);
}

int test_shared_fifo_close_suppresses_writes() {
	const std::string fn = "test_shared_fifo_close_suppresses_writes";
	SharedFIFO fifo;
	(void)fifo.Write(std::string("ABC"));
	ASSERT_EQUAL(fn, fifo.Size(), StormByte::ByteSize{3});
	fifo.Close();
	(void)fifo.Write(std::string("DEF"));
	ASSERT_EQUAL(fn, fifo.Size(), StormByte::ByteSize{3});
	BinaryData out;
	ASSERT_TRUE(fn, fifo.Extract(0, out));
	ASSERT_EQUAL(fn, BytesToText(out), std::string("ABC"));
	RETURN_TEST(fn, 0);
}

int test_shared_fifo_extract_closed_no_data_nonblocking() {
	const std::string fn = "test_shared_fifo_extract_closed_no_data_nonblocking";
	SharedFIFO fifo;
	fifo.Close();
	ASSERT_FALSE(fn, fifo.IsWritable());
	ASSERT_EQUAL(fn, fifo.Size(), StormByte::ByteSize{0});
	BinaryData out;
	ASSERT_FALSE(fn, fifo.Extract(10, out));
	RETURN_TEST(fn, 0);
}

int test_shared_fifo_extract_insufficient_closed_returns_available() {
	const std::string fn = "test_shared_fifo_extract_insufficient_closed_returns_available";
	SharedFIFO fifo;
	(void)fifo.Write("HELLO");
	fifo.Close();
	BinaryData out;
	ASSERT_FALSE(fn, fifo.Extract(100, out));
	ASSERT_EQUAL(fn, fifo.Size(), StormByte::ByteSize{5});
	BinaryData all;
	ASSERT_TRUE(fn, fifo.Read(0, all));
	ASSERT_EQUAL(fn, BytesToText(all), std::string("HELLO"));
	RETURN_TEST(fn, 0);
}

int test_shared_fifo_read_closed_no_data_nonblocking() {
	const std::string fn = "test_shared_fifo_read_closed_no_data_nonblocking";
	SharedFIFO fifo;
	fifo.Close();
	ASSERT_FALSE(fn, fifo.IsWritable());
	ASSERT_EQUAL(fn, fifo.Size(), StormByte::ByteSize{0});
	BinaryData out;
	ASSERT_FALSE(fn, fifo.Read(10, out));
	RETURN_TEST(fn, 0);
}

int test_shared_fifo_read_insufficient_closed_returns_available() {
	const std::string fn = "test_shared_fifo_read_insufficient_closed_returns_available";
	SharedFIFO fifo;
	(void)fifo.Write("ABC");
	fifo.Close();
	BinaryData out;
	ASSERT_FALSE(fn, fifo.Read(10, out));
	ASSERT_EQUAL(fn, fifo.Available(), StormByte::ByteSize{3});
	RETURN_TEST(fn, 0);
}

int test_sharedfifo_equality() {
	const std::string fn = "test_sharedfifo_equality";
	SharedFIFO sa;
	SharedFIFO sb;
	(void)sa.Write("HELLO");
	(void)sb.Write("HELLO");
	ASSERT_TRUE(fn, sa == sb);
	ASSERT_FALSE(fn, sa != sb);
	sa.Close();
	ASSERT_FALSE(fn, sa == sb);
	sb.Close();
	ASSERT_TRUE(fn, sa == sb);
	RETURN_TEST(fn, 0);
}

// -------------------
// HexDump
// -------------------

int test_hexdump1() {
	const std::string fn = "test_shared_hexdump";
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
	ASSERT_EQUAL(fn, expected, dump);
	RETURN_TEST(fn, 0);
}

int test_hexdump2() {
	const std::string fn = "test_shared_hexdump_offset";
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
	ASSERT_EQUAL(fn, expected, dump);
	RETURN_TEST(fn, 0);
}

int test_hexdump3() {
	const std::string fn = "test_shared_hexdump_mixed";
	SharedFIFO sf;
	BinaryData v;
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
	ASSERT_EQUAL(fn, expected, dump);
	RETURN_TEST(fn, 0);
}

// -------------------
// Peek / drop
// -------------------

int test_shared_fifo_peek_all_available() {
	const std::string fn = "test_shared_fifo_peek_all_available";
	SharedFIFO fifo;
	(void)fifo.Write(std::string("WORLD"));
	BinaryData peek_all;
	ASSERT_TRUE(fn, fifo.Peek(0, peek_all));
	ASSERT_EQUAL(fn, BytesToText(peek_all), std::string("WORLD"));
	BinaryData read_all;
	ASSERT_TRUE(fn, fifo.Read(0, read_all));
	ASSERT_EQUAL(fn, BytesToText(read_all), std::string("WORLD"));
	RETURN_TEST(fn, 0);
}

int test_shared_fifo_peek_basic() {
	const std::string fn = "test_shared_fifo_peek_basic";
	SharedFIFO fifo;
	(void)fifo.Write(std::string("HELLO"));
	BinaryData peek1;
	ASSERT_TRUE(fn, fifo.Peek(3, peek1));
	ASSERT_EQUAL(fn, BytesToText(peek1), std::string("HEL"));
	BinaryData peek2;
	ASSERT_TRUE(fn, fifo.Peek(3, peek2));
	ASSERT_EQUAL(fn, BytesToText(peek2), std::string("HEL"));
	BinaryData read1;
	ASSERT_TRUE(fn, fifo.Read(3, read1));
	ASSERT_EQUAL(fn, BytesToText(read1), std::string("HEL"));
	RETURN_TEST(fn, 0);
}

int test_shared_fifo_peek_concurrent() {
	const std::string fn = "test_shared_fifo_peek_concurrent";
	SharedFIFO fifo;
	(void)fifo.Write(std::string("DATA"));
	BinaryData peek;
	ASSERT_TRUE(fn, fifo.Peek(4, peek));
	ASSERT_EQUAL(fn, BytesToText(peek), std::string("DATA"));
	BinaryData read;
	ASSERT_TRUE(fn, fifo.Read(4, read));
	ASSERT_EQUAL(fn, BytesToText(read), std::string("DATA"));
	RETURN_TEST(fn, 0);
}

int test_shared_fifo_skip_basic() {
	const std::string fn = "test_shared_fifo_skip_basic";
	SharedFIFO sf;
	(void)sf.Write(std::string("ABCDEFG"));
	(void)sf.Drop(3);
	ASSERT_EQUAL(fn, sf.Size(), StormByte::ByteSize{4});
	BinaryData out;
	ASSERT_TRUE(fn, sf.Extract(0, out));
	ASSERT_EQUAL(fn, BytesToText(out), std::string("DEFG"));
	RETURN_TEST(fn, 0);
}

int test_shared_fifo_skip_with_readpos() {
	const std::string fn = "test_shared_fifo_skip_with_readpos";
	SharedFIFO sf;
	(void)sf.Write(std::string("0123456789"));
	BinaryData r;
	ASSERT_TRUE(fn, sf.Read(3, r));
	(void)sf.Drop(4);
	ASSERT_EQUAL(fn, sf.Size(), StormByte::ByteSize{3});
	BinaryData out;
	ASSERT_TRUE(fn, sf.Extract(0, out));
	ASSERT_EQUAL(fn, BytesToText(out), std::string("789"));
	RETURN_TEST(fn, 0);
}

// -------------------
// Threading
// -------------------

int test_shared_fifo_concurrent_seek_and_read() {
	const std::string fn = "test_shared_fifo_concurrent_seek_and_read";
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
		BinaryData r1, r2;
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
	ASSERT_FALSE(fn, reader_failed.load());
	ASSERT_TRUE(fn, seeker_done.load());
	ASSERT_TRUE(fn, read_a.size() <= 2);
	ASSERT_TRUE(fn, read_b.size() <= 3);
	auto within_digits = [](const std::string& s) {
		for (char c : s)
			if (c < '0' || c > '9')
				return false;
		return true;
	};
	ASSERT_TRUE(fn, within_digits(read_a));
	ASSERT_TRUE(fn, within_digits(read_b));
	RETURN_TEST(fn, 0);
}

int test_shared_fifo_extract_adjusts_read_position_concurrency() {
	const std::string fn = "test_shared_fifo_extract_adjusts_read_position_concurrency";
	SharedFIFO fifo;
	(void)fifo.Write(std::string("ABCDEFGH"));
	std::string r_before, r_after;
	std::atomic<bool> reader_failed{false};
	std::atomic<bool> first_read_done{false};
	std::thread reader([&] {
		BinaryData before, after;
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
		BinaryData e;
		(void)fifo.Extract(2, e);
	});
	reader.join();
	extractor.join();
	ASSERT_FALSE(fn, reader_failed.load());
	ASSERT_EQUAL(fn, r_before, std::string("ABC"));
	ASSERT_EQUAL(fn, r_after, std::string("FG"));
	RETURN_TEST(fn, 0);
}

int test_shared_fifo_extract_blocking_and_close() {
	const std::string fn = "test_shared_fifo_extract_blocking_and_close";
	SharedFIFO fifo;
	std::atomic<bool> woke{false};
	std::atomic<bool> saw_writable{false};
	std::size_t extracted_size = 1234;
	std::thread t([&] {
		BinaryData out;
		const auto res = fifo.Extract(1, out);
		woke.store(true);
		saw_writable.store(fifo.IsWritable());
		extracted_size = res ? static_cast<std::size_t>(out.size()) : 0;
	});
	std::this_thread::sleep_for(std::chrono::milliseconds(5));
	fifo.Close();
	t.join();
	ASSERT_TRUE(fn, woke.load());
	ASSERT_FALSE(fn, saw_writable.load());
	ASSERT_EQUAL(fn, extracted_size, static_cast<std::size_t>(0));
	RETURN_TEST(fn, 0);
}

int test_shared_fifo_growth_under_contention() {
	const std::string fn = "test_shared_fifo_growth_under_contention";
	SharedFIFO fifo;
	const int iters = 100;
	std::atomic<bool> done{false};
	std::thread producer([&] {
		for (int i = 0; i < iters; ++i)
			(void)fifo.Write(std::string(100 + (i % 50), 'Z'));
		done.store(true);
		fifo.Close();
	});
	std::size_t consumed = 0;
	std::thread consumer([&] {
		while (true) {
			BinaryData part;
			if (!fifo.Extract(128, part)) {
				if (fifo.Available() > 0) {
					BinaryData rem;
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
	ASSERT_EQUAL(fn, consumed, expected);
	RETURN_TEST(fn, 0);
}

int test_shared_fifo_multi_producer_single_consumer_counts() {
	const std::string fn = "test_shared_fifo_multi_producer_single_consumer_counts";
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
			BinaryData part;
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
	ASSERT_TRUE(fn, p1_done.load() && p2_done.load());
	std::size_t countA = 0, countB = 0;
	for (char c : collected) {
		if (c == 'A')
			++countA;
		else if (c == 'B')
			++countB;
	}
	ASSERT_EQUAL(fn, countA, static_cast<std::size_t>(chunks));
	ASSERT_EQUAL(fn, countB, static_cast<std::size_t>(chunks));
	ASSERT_EQUAL(fn, collected.size(), static_cast<std::size_t>(chunks * 2));
	RETURN_TEST(fn, 0);
}

int test_shared_fifo_multiple_consumers_total_coverage() {
	const std::string fn = "test_shared_fifo_multiple_consumers_total_coverage";
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
			BinaryData part;
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
			BinaryData part;
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
	ASSERT_EQUAL(fn, c1.load() + c2.load(), static_cast<std::size_t>(total));
	RETURN_TEST(fn, 0);
}

int test_shared_fifo_producer_consumer_blocking() {
	const std::string fn = "test_shared_fifo_producer_consumer_blocking";
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
			BinaryData part;
			if (!fifo.Read(3, part)) {
				if (fifo.Available() > 0) {
					BinaryData rem;
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
	ASSERT_TRUE(fn, done.load());
	ASSERT_EQUAL(fn, collected, payload);
	RETURN_TEST(fn, 0);
}

// -------------------
// Write / read
// -------------------

int test_shared_fifo_multiple_spans_eof() {
	const std::string fn = "test_shared_fifo_multiple_spans_eof";
	SharedFIFO fifo;
	(void)fifo.Write(std::string("ABCDEFGHIJ"));
	BinaryData s1, s2, s3;
	ASSERT_TRUE(fn, fifo.Read(4, s1));
	ASSERT_EQUAL(fn, s1.size(), StormByte::ByteSize{4});
	ASSERT_TRUE(fn, fifo.Read(3, s2));
	ASSERT_EQUAL(fn, s2.size(), StormByte::ByteSize{3});
	ASSERT_TRUE(fn, fifo.Read(3, s3));
	ASSERT_EQUAL(fn, s3.size(), StormByte::ByteSize{3});
	ASSERT_EQUAL(fn, fifo.Available(), StormByte::ByteSize{0});
	ASSERT_FALSE(fn, fifo.EoF());
	fifo.Close();
	ASSERT_TRUE(fn, fifo.EoF());
	RETURN_TEST(fn, 0);
}

int test_shared_fifo_polymorphic_interface_abi() {
	const std::string fn = "test_shared_fifo_polymorphic_interface_abi";
	std::unique_ptr<ReadWrite> fifo = std::make_unique<SharedFIFO>();
	ReadOnly& reader = *fifo;
	WriteOnly& writer = *fifo;
	BinaryData data {std::byte{'A'}, std::byte{'B'}};
	ASSERT_TRUE(fn, writer.Write(0, std::move(data)));
	ASSERT_TRUE(fn, writer.IsWritable());
	ASSERT_EQUAL(fn, reader.Available(), StormByte::ByteSize{2});
	ASSERT_FALSE(fn, reader.Empty());
	ASSERT_TRUE(fn, reader.IsReadable());
	BinaryData peek;
	ASSERT_TRUE(fn, reader.Peek(1, peek));
	BinaryData read;
	ASSERT_TRUE(fn, reader.Read(1, read));
	reader.Seek(0, Position::Absolute);
	ASSERT_TRUE(fn, reader.Drop(1));
	reader.Clean();
	writer.SetError();
	ASSERT_TRUE(fn, reader.EoF());
	reader.Clear();
	BinaryData until_eof;
	reader.ReadUntilEoF(until_eof);
	reader.ExtractUntilEoF(until_eof);
	fifo.reset();
	RETURN_TEST(fn, 0);
}

int test_shared_fifo_wrap_boundary_blocking() {
	const std::string fn = "test_shared_fifo_wrap_boundary_blocking";
	SharedFIFO fifo;
	(void)fifo.Write("ABCDE");
	BinaryData r1;
	ASSERT_TRUE(fn, fifo.Read(3, r1));
	ASSERT_EQUAL(fn, BytesToText(r1), std::string("ABC"));
	BinaryData e1;
	ASSERT_TRUE(fn, fifo.Extract(2, e1));
	ASSERT_EQUAL(fn, BytesToText(e1), std::string("DE"));
	(void)fifo.Write("12");
	fifo.Seek(0, Position::Absolute);
	BinaryData all;
	ASSERT_TRUE(fn, fifo.Read(0, all));
	ASSERT_EQUAL(fn, BytesToText(all).size(), static_cast<std::size_t>(5));
	RETURN_TEST(fn, 0);
}

int test_shared_fifo_write_span_basic() {
	const std::string fn = "test_shared_fifo_write_span_basic";
	SharedFIFO fifo;
	const char* msg = "SFPAN";
	BinaryData vec(reinterpret_cast<const std::byte*>(msg), StormByte::ByteSize{5});
	ASSERT_TRUE(fn, fifo.Write(vec));
	BinaryData read;
	ASSERT_TRUE(fn, fifo.Read(5, read));
	ASSERT_EQUAL(fn, BytesToText(read), std::string("SFPAN"));
	RETURN_TEST(fn, 0);
}

int test_shared_fifo_write_whole_fifo() {
	const std::string fn = "test_shared_fifo_write_whole_fifo";
	SharedFIFO shared;
	FIFO src;
	(void)src.Write(std::string("ONE"));
	ASSERT_TRUE(fn, shared.Write(src));
	BinaryData all;
	ASSERT_TRUE(fn, shared.Extract(0, all));
	ASSERT_EQUAL(fn, BytesToText(all), std::string("ONE"));
	FIFO src2;
	(void)src2.Write(std::string("TWO"));
	ASSERT_TRUE(fn, shared.Write(std::move(src2)));
	BinaryData all2;
	ASSERT_TRUE(fn, shared.Extract(0, all2));
	ASSERT_EQUAL(fn, BytesToText(all2), std::string("TWO"));
	RETURN_TEST(fn, 0);
}

int main() {
	int result = 0;

	// -------------------
	// Available
	// -------------------
	result += test_shared_fifo_available_bytes_basic();
	result += test_shared_fifo_available_bytes_concurrent();

	// -------------------
	// Close / error
	// -------------------
	result += test_shared_fifo_blocking_read_insufficient_not_closed();
	result += test_shared_fifo_close_suppresses_writes();
	result += test_shared_fifo_extract_closed_no_data_nonblocking();
	result += test_shared_fifo_extract_insufficient_closed_returns_available();
	result += test_shared_fifo_read_closed_no_data_nonblocking();
	result += test_shared_fifo_read_insufficient_closed_returns_available();
	result += test_sharedfifo_equality();

	// -------------------
	// HexDump
	// -------------------
	result += test_hexdump1();
	result += test_hexdump2();
	result += test_hexdump3();

	// -------------------
	// Peek / drop
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
	// Write / read
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
