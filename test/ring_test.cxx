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

#include <StormByte/buffer/ring.hxx>
#include <StormByte/test_handlers.h>

#include <algorithm>
#include <atomic>
#include <chrono>
#include <cstddef>
#include <iostream>
#include <memory>
#include <string>
#include <thread>
#include <vector>

using StormByte::BinaryData;
using StormByte::Buffer::Position;
using StormByte::Buffer::ReadOnly;
using StormByte::Buffer::ReadWrite;
using StormByte::Buffer::Ring;
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
// Basic
// -------------------

int test_ring_basic_write_read() {
	const std::string fn = "test_ring_basic_write_read";
	Ring ring;
	const std::string msg = "Hello, Ring!";
	ASSERT_TRUE(fn, ring.Write(msg));
	ring.Close();
	ASSERT_EQUAL(fn, ring.Size(), StormByte::ByteSize{msg.size()});
	ASSERT_FALSE(fn, ring.Empty());
	ASSERT_EQUAL(fn, ring.Available(), StormByte::ByteSize{msg.size()});
	BinaryData data;
	ASSERT_TRUE(fn, ring.Read(StormByte::ByteSize{msg.size()}, data));
	ASSERT_EQUAL(fn, BytesToText(data), msg);
	ASSERT_EQUAL(fn, ring.Available(), StormByte::ByteSize{0});
	RETURN_TEST(fn, 0);
}

int test_ring_clear() {
	const std::string fn = "test_ring_clear";
	Ring ring;
	ASSERT_TRUE(fn, ring.Write("Some data to clear"));
	ASSERT_FALSE(fn, ring.Empty());
	ring.Clear();
	ASSERT_TRUE(fn, ring.Empty());
	ASSERT_EQUAL(fn, ring.Size(), StormByte::ByteSize{0});
	ASSERT_EQUAL(fn, ring.Available(), StormByte::ByteSize{0});
	ASSERT_TRUE(fn, ring.Write("New data"));
	ASSERT_EQUAL(fn, ring.Size(), StormByte::ByteSize{8});
	RETURN_TEST(fn, 0);
}

int test_ring_clean_after_seek() {
	const std::string fn = "test_ring_clean_after_seek";
	Ring ring;
	ASSERT_TRUE(fn, ring.Write("ABCDEFGH"));
	ring.Seek(3, Position::Absolute);
	ASSERT_EQUAL(fn, ring.Available(), StormByte::ByteSize{5});
	ring.Clean();
	ASSERT_EQUAL(fn, ring.Size(), StormByte::ByteSize{5});
	ASSERT_EQUAL(fn, ring.Available(), StormByte::ByteSize{5});
	BinaryData data;
	ASSERT_TRUE(fn, ring.Read(0, data));
	ASSERT_EQUAL(fn, BytesToText(data), std::string("DEFGH"));
	RETURN_TEST(fn, 0);
}

int test_ring_close_mechanism() {
	const std::string fn = "test_ring_close_mechanism";
	Ring ring;
	ASSERT_TRUE(fn, ring.Write("Data"));
	ring.Close();
	ASSERT_FALSE(fn, ring.Write("More"));
	ASSERT_EQUAL(fn, ring.Size(), StormByte::ByteSize{4});
	BinaryData d;
	(void)ring.Extract(0, d);
	ASSERT_TRUE(fn, ring.EoF());
	RETURN_TEST(fn, 0);
}

int test_ring_drop() {
	const std::string fn = "test_ring_drop";
	Ring ring;
	ASSERT_TRUE(fn, ring.Write("0123456789"));
	ASSERT_TRUE(fn, ring.Drop(4));
	ASSERT_EQUAL(fn, ring.Size(), StormByte::ByteSize{6});
	BinaryData data;
	ASSERT_TRUE(fn, ring.Read(0, data));
	ASSERT_EQUAL(fn, BytesToText(data), std::string("456789"));
	RETURN_TEST(fn, 0);
}

int test_ring_empty_read_failure() {
	const std::string fn = "test_ring_empty_read_failure";
	Ring ring;
	ring.Close();
	BinaryData data;
	ASSERT_FALSE(fn, ring.Read(0, data));
	ASSERT_FALSE(fn, ring.Extract(0, data));
	ASSERT_TRUE(fn, ring.EoF());
	RETURN_TEST(fn, 0);
}

int test_ring_extract_destructive() {
	const std::string fn = "test_ring_extract_destructive";
	Ring ring;
	ASSERT_TRUE(fn, ring.Write("ABCDEFGH"));
	ASSERT_EQUAL(fn, ring.Size(), StormByte::ByteSize{8});
	BinaryData first, rest;
	ASSERT_TRUE(fn, ring.Extract(3, first));
	ASSERT_EQUAL(fn, BytesToText(first), std::string("ABC"));
	ASSERT_EQUAL(fn, ring.Size(), StormByte::ByteSize{5});
	ASSERT_EQUAL(fn, ring.Available(), StormByte::ByteSize{5});
	ASSERT_TRUE(fn, ring.Extract(0, rest));
	ASSERT_EQUAL(fn, BytesToText(rest), std::string("DEFGH"));
	ASSERT_TRUE(fn, ring.Empty());
	ASSERT_EQUAL(fn, ring.Size(), StormByte::ByteSize{0});
	RETURN_TEST(fn, 0);
}

int test_ring_move_semantics() {
	const std::string fn = "test_ring_move_semantics";
	Ring r1;
	ASSERT_TRUE(fn, r1.Write("Data"));
	ASSERT_TRUE(fn, r1.Write("More"));
	Ring r2 = std::move(r1);
	ASSERT_EQUAL(fn, r2.Size(), StormByte::ByteSize{8});
	BinaryData data;
	ASSERT_TRUE(fn, r2.Read(0, data));
	ASSERT_EQUAL(fn, BytesToText(data), std::string("DataMore"));
	RETURN_TEST(fn, 0);
}

int test_ring_multiple_writes() {
	const std::string fn = "test_ring_multiple_writes";
	Ring ring;
	ASSERT_TRUE(fn, ring.Write("First"));
	ASSERT_TRUE(fn, ring.Write("Second"));
	ASSERT_TRUE(fn, ring.Write("Third"));
	ring.Close();
	BinaryData all;
	ASSERT_TRUE(fn, ring.Read(0, all));
	ASSERT_EQUAL(fn, BytesToText(all), std::string("FirstSecondThird"));
	RETURN_TEST(fn, 0);
}

int test_ring_peek_does_not_consume() {
	const std::string fn = "test_ring_peek_does_not_consume";
	Ring ring;
	ASSERT_TRUE(fn, ring.Write("ABCDEFGH"));
	ring.Close();
	BinaryData p1, p2, r1, r2;
	ASSERT_TRUE(fn, ring.Peek(4, p1));
	ASSERT_EQUAL(fn, BytesToText(p1), std::string("ABCD"));
	ASSERT_TRUE(fn, ring.Peek(4, p2));
	ASSERT_EQUAL(fn, BytesToText(p2), std::string("ABCD"));
	ASSERT_EQUAL(fn, ring.Available(), StormByte::ByteSize{8});
	ASSERT_TRUE(fn, ring.Read(4, r1));
	ASSERT_EQUAL(fn, BytesToText(r1), std::string("ABCD"));
	ASSERT_EQUAL(fn, ring.Available(), StormByte::ByteSize{4});
	ASSERT_TRUE(fn, ring.Read(4, r2));
	ASSERT_EQUAL(fn, BytesToText(r2), std::string("EFGH"));
	ASSERT_EQUAL(fn, ring.Available(), StormByte::ByteSize{0});
	RETURN_TEST(fn, 0);
}

int test_ring_seek_operations() {
	const std::string fn = "test_ring_seek_operations";
	Ring ring;
	ASSERT_TRUE(fn, ring.Write("0123456789"));
	ring.Close();
	BinaryData from5, fromStart, from7;
	ASSERT_TRUE(fn, ring.Read(5, from5));
	ASSERT_EQUAL(fn, BytesToText(from5), std::string("01234"));
	ring.Seek(-5, Position::Relative);
	ASSERT_TRUE(fn, ring.Read(0, fromStart));
	ASSERT_EQUAL(fn, BytesToText(fromStart), std::string("0123456789"));
	ring.Seek(7, Position::Absolute);
	ASSERT_TRUE(fn, ring.Read(3, from7));
	ASSERT_EQUAL(fn, BytesToText(from7), std::string("789"));
	RETURN_TEST(fn, 0);
}

// -------------------
// Position
// -------------------

int test_ring_extract_then_seek() {
	const std::string fn = "test_ring_extract_then_seek";
	Ring ring;
	ASSERT_TRUE(fn, ring.Write("ABCDEFGHIJ"));
	BinaryData part;
	ASSERT_TRUE(fn, ring.Extract(4, part));
	ASSERT_EQUAL(fn, BytesToText(part), std::string("ABCD"));
	ASSERT_EQUAL(fn, ring.Size(), StormByte::ByteSize{6});
	ring.Seek(2, Position::Absolute);
	BinaryData mid;
	ASSERT_TRUE(fn, ring.Read(3, mid));
	ASSERT_EQUAL(fn, BytesToText(mid), std::string("GHI"));
	RETURN_TEST(fn, 0);
}

int test_ring_extract_until_eof_waits_for_producer() {
	const std::string fn = "test_ring_extract_until_eof_waits_for_producer";
	Ring ring;
	std::thread producer([&] {
		std::this_thread::sleep_for(std::chrono::milliseconds(30));
		(void)ring.Write("Hello");
		std::this_thread::sleep_for(std::chrono::milliseconds(20));
		(void)ring.Write("World");
		ring.Close();
	});
	BinaryData all;
	ring.ExtractUntilEoF(all);
	producer.join();
	ASSERT_EQUAL(fn, BytesToText(all), std::string("HelloWorld"));
	ASSERT_TRUE(fn, ring.EoF());
	RETURN_TEST(fn, 0);
}

int test_ring_extract_zero_returns_all() {
	const std::string fn = "test_ring_extract_zero_returns_all";
	Ring ring;
	ASSERT_TRUE(fn, ring.Write("TestData"));
	ring.Close();
	BinaryData data;
	ASSERT_TRUE(fn, ring.Extract(0, data));
	ASSERT_EQUAL(fn, data.size(), StormByte::ByteSize{8});
	ASSERT_TRUE(fn, ring.Empty());
	RETURN_TEST(fn, 0);
}

int test_ring_partial_extract_leaves_valid_position() {
	const std::string fn = "test_ring_partial_extract_leaves_valid_position";
	Ring ring;
	ASSERT_TRUE(fn, ring.Write("0123456789"));
	ring.Seek(3, Position::Absolute);
	BinaryData mid;
	ASSERT_TRUE(fn, ring.Extract(4, mid));
	ASSERT_EQUAL(fn, BytesToText(mid), std::string("3456"));
	ASSERT_EQUAL(fn, ring.Size(), StormByte::ByteSize{6});
	ASSERT_EQUAL(fn, ring.Available(), StormByte::ByteSize{3});
	BinaryData rest;
	ASSERT_TRUE(fn, ring.Extract(0, rest));
	ASSERT_EQUAL(fn, rest.size(), StormByte::ByteSize{3});
	ASSERT_EQUAL(fn, BytesToText(rest), std::string("789"));
	ASSERT_EQUAL(fn, ring.Size(), StormByte::ByteSize{3});
	ASSERT_EQUAL(fn, ring.Available(), StormByte::ByteSize{0});
	ring.Clean();
	ASSERT_TRUE(fn, ring.Empty());
	RETURN_TEST(fn, 0);
}

int test_ring_read_zero_on_closed_empty_fails() {
	const std::string fn = "test_ring_read_zero_on_closed_empty_fails";
	Ring ring;
	ring.Close();
	BinaryData data;
	ASSERT_FALSE(fn, ring.Read(0, data));
	ASSERT_FALSE(fn, ring.Extract(0, data));
	RETURN_TEST(fn, 0);
}

int test_ring_read_zero_on_open_empty_waits_until_close() {
	const std::string fn = "test_ring_read_zero_on_open_empty_waits_until_close";
	Ring ring;
	std::atomic<bool> finished{false};
	bool read_ok = true;
	std::thread consumer([&] {
		BinaryData data;
		read_ok = ring.Read(0, data);
		finished = true;
	});
	std::this_thread::sleep_for(std::chrono::milliseconds(30));
	ASSERT_FALSE(fn, finished.load());
	ring.Close();
	consumer.join();
	ASSERT_TRUE(fn, finished.load());
	ASSERT_FALSE(fn, read_ok);
	ASSERT_TRUE(fn, ring.EoF());
	RETURN_TEST(fn, 0);
}

// -------------------
// Stress
// -------------------

int test_ring_alternating_small_large() {
	const std::string fn = "test_ring_alternating_small_large";
	Ring ring;
	std::atomic<std::size_t> total{0};
	std::thread writer([&] {
		for (int i = 0; i < 20; ++i) {
			if (i % 2 == 0)
				(void)ring.Write("X");
			else
				(void)ring.Write(std::string(1000, 'Y'));
			std::this_thread::sleep_for(std::chrono::microseconds(50));
		}
		ring.Close();
	});
	std::thread reader([&] {
		BinaryData all;
		ring.ExtractUntilEoF(all);
		total = static_cast<std::size_t>(all.size());
	});
	writer.join();
	reader.join();
	ASSERT_EQUAL(fn, total.load(), static_cast<std::size_t>(10 * 1 + 10 * 1000));
	RETURN_TEST(fn, 0);
}

int test_ring_available_bytes_consistency() {
	const std::string fn = "test_ring_available_bytes_consistency";
	Ring ring;
	ASSERT_EQUAL(fn, ring.Available(), StormByte::ByteSize{0});
	ASSERT_TRUE(fn, ring.Write("TEST DATA"));
	ASSERT_EQUAL(fn, ring.Available(), StormByte::ByteSize{9});
	BinaryData r1;
	ASSERT_TRUE(fn, ring.Read(4, r1));
	ASSERT_EQUAL(fn, ring.Available(), StormByte::ByteSize{5});
	ring.Seek(0, Position::Absolute);
	ASSERT_EQUAL(fn, ring.Available(), StormByte::ByteSize{9});
	BinaryData e1;
	ASSERT_TRUE(fn, ring.Extract(3, e1));
	ASSERT_EQUAL(fn, ring.Available(), StormByte::ByteSize{6});
	ASSERT_TRUE(fn, ring.Write("MORE"));
	ASSERT_EQUAL(fn, ring.Available(), StormByte::ByteSize{10});
	ring.Seek(0, Position::Absolute);
	BinaryData all;
	ASSERT_TRUE(fn, ring.Read(0, all));
	ASSERT_EQUAL(fn, ring.Available(), StormByte::ByteSize{0});
	RETURN_TEST(fn, 0);
}

int test_ring_burst_then_drain() {
	const std::string fn = "test_ring_burst_then_drain";
	Ring ring;
	std::atomic<std::size_t> total{0};
	std::thread writer([&] {
		for (int i = 0; i < 1000; ++i)
			(void)ring.Write("0123456789");
		ring.Close();
	});
	std::thread reader([&] {
		BinaryData all;
		ring.ExtractUntilEoF(all);
		total = static_cast<std::size_t>(all.size());
	});
	writer.join();
	reader.join();
	ASSERT_EQUAL(fn, total.load(), static_cast<std::size_t>(10000));
	RETURN_TEST(fn, 0);
}

int test_ring_clear_while_producing() {
	const std::string fn = "test_ring_clear_while_producing";
	Ring ring;
	ASSERT_TRUE(fn, ring.Write("InitialData"));
	ASSERT_TRUE(fn, ring.Size() > 0);
	ring.Clear();
	ASSERT_TRUE(fn, ring.Empty());
	ASSERT_TRUE(fn, ring.Write("AfterClear"));
	ring.Close();
	BinaryData data;
	ASSERT_TRUE(fn, ring.Extract(0, data));
	ASSERT_EQUAL(fn, BytesToText(data), std::string("AfterClear"));
	RETURN_TEST(fn, 0);
}

int test_ring_interleaved_read_extract() {
	const std::string fn = "test_ring_interleaved_read_extract";
	Ring ring;
	ASSERT_TRUE(fn, ring.Write("ABCDEFGH"));
	ring.Close();
	BinaryData r1, e1;
	ASSERT_TRUE(fn, ring.Read(5, r1));
	ASSERT_EQUAL(fn, BytesToText(r1), std::string("ABCDE"));
	ASSERT_EQUAL(fn, ring.Available(), StormByte::ByteSize{3});
	ASSERT_TRUE(fn, ring.Extract(3, e1));
	ASSERT_EQUAL(fn, BytesToText(e1), std::string("FGH"));
	ASSERT_EQUAL(fn, ring.Size(), StormByte::ByteSize{5});
	ASSERT_EQUAL(fn, ring.Available(), StormByte::ByteSize{0});
	ASSERT_FALSE(fn, ring.Empty());
	ring.Clean();
	ASSERT_TRUE(fn, ring.Empty());
	RETURN_TEST(fn, 0);
}

int test_ring_partial_read_on_closed() {
	const std::string fn = "test_ring_partial_read_on_closed";
	Ring ring;
	const std::string msg(30, 'Z');
	ASSERT_TRUE(fn, ring.Write(msg));
	ring.Close();
	BinaryData data;
	ASSERT_FALSE(fn, ring.Read(50, data));
	BinaryData rem;
	ASSERT_TRUE(fn, ring.Read(0, rem));
	ASSERT_EQUAL(fn, rem.size(), StormByte::ByteSize{30});
	ASSERT_TRUE(fn, ring.EoF());
	RETURN_TEST(fn, 0);
}

int test_ring_polymorphic_interface_abi() {
	const std::string fn = "test_ring_polymorphic_interface_abi";
	std::unique_ptr<ReadWrite> ring = std::make_unique<Ring>();
	ReadOnly& reader = *ring;
	WriteOnly& writer = *ring;
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
	reader.Clear();
	writer.Close();
	ASSERT_TRUE(fn, reader.EoF());
	BinaryData until_eof;
	reader.ReadUntilEoF(until_eof);
	reader.ExtractUntilEoF(until_eof);
	ring.reset();
	RETURN_TEST(fn, 0);
}

int test_ring_stress_rapid_small_writes() {
	const std::string fn = "test_ring_stress_rapid_small_writes";
	Ring ring;
	std::atomic<std::size_t> written{0}, read{0};
	std::thread writer([&] {
		for (int i = 0; i < 1000; ++i) {
			(void)ring.Write("X");
			written.fetch_add(1);
		}
		ring.Close();
	});
	std::thread reader([&] {
		BinaryData all;
		ring.ExtractUntilEoF(all);
		read = static_cast<std::size_t>(all.size());
	});
	writer.join();
	reader.join();
	ASSERT_EQUAL(fn, written.load(), static_cast<std::size_t>(1000));
	ASSERT_EQUAL(fn, read.load(), written.load());
	ASSERT_TRUE(fn, ring.Empty());
	RETURN_TEST(fn, 0);
}

int test_ring_very_large_transfer() {
	const std::string fn = "test_ring_very_large_transfer";
	Ring ring;
	const std::size_t large = 1u << 20;
	std::atomic<std::size_t> received{0};
	std::thread writer([&] {
		const std::size_t chunk = 8192;
		for (std::size_t i = 0; i < large; i += chunk) {
			std::string block(chunk, static_cast<char>('A' + (i / chunk) % 26));
			(void)ring.Write(block);
		}
		ring.Close();
	});
	std::thread reader([&] {
		BinaryData all;
		ring.ExtractUntilEoF(all);
		received = static_cast<std::size_t>(all.size());
	});
	writer.join();
	reader.join();
	ASSERT_EQUAL(fn, received.load(), large);
	RETURN_TEST(fn, 0);
}

// -------------------
// Threading
// -------------------

int test_ring_blocking_read_waits_for_data() {
	const std::string fn = "test_ring_blocking_read_waits_for_data";
	Ring ring;
	std::atomic<bool> started{false}, finished{false};
	std::string result;
	std::thread consumer([&] {
		started = true;
		BinaryData data;
		if (ring.Read(10, data))
			result = BytesToText(data);
		finished = true;
	});
	std::this_thread::sleep_for(std::chrono::milliseconds(20));
	ASSERT_TRUE(fn, started.load());
	ASSERT_FALSE(fn, finished.load());
	(void)ring.Write("AB");
	std::this_thread::sleep_for(std::chrono::milliseconds(20));
	ASSERT_FALSE(fn, finished.load());
	(void)ring.Write("CDEFGHIJ");
	ring.Close();
	consumer.join();
	ASSERT_TRUE(fn, finished.load());
	ASSERT_EQUAL(fn, result, std::string("ABCDEFGHIJ"));
	RETURN_TEST(fn, 0);
}

int test_ring_close_unblocks_waiter() {
	const std::string fn = "test_ring_close_unblocks_waiter";
	Ring ring;
	std::atomic<bool> completed{false};
	std::string result;
	std::thread consumer([&] {
		BinaryData data;
		if (ring.Read(100, data))
			result = BytesToText(data);
		else if (ring.Available() > 0) {
			BinaryData rem;
			if (ring.Read(0, rem))
				result = BytesToText(rem);
		}
		completed = true;
	});
	std::this_thread::sleep_for(std::chrono::milliseconds(15));
	(void)ring.Write("Short");
	ring.Close();
	consumer.join();
	ASSERT_TRUE(fn, completed.load());
	ASSERT_EQUAL(fn, result, std::string("Short"));
	ASSERT_TRUE(fn, result.size() < 100);
	RETURN_TEST(fn, 0);
}

int test_ring_multiple_writers_single_reader() {
	const std::string fn = "test_ring_multiple_writers_single_reader";
	Ring ring;
	const int chunks_per = 50;
	std::atomic<int> finished{0};
	std::string collected;
	auto writer_fn = [&](char id) {
		for (int i = 0; i < chunks_per; ++i)
			(void)ring.Write(std::string(1, id));
		finished.fetch_add(1);
	};
	std::thread w1(writer_fn, 'A');
	std::thread w2(writer_fn, 'B');
	std::thread w3(writer_fn, 'C');
	std::thread closer([&] {
		while (finished.load() < 3)
			std::this_thread::sleep_for(std::chrono::milliseconds(1));
		ring.Close();
	});
	std::thread reader([&] {
		BinaryData all;
		ring.ExtractUntilEoF(all);
		collected = BytesToText(all);
	});
	w1.join();
	w2.join();
	w3.join();
	closer.join();
	reader.join();
	ASSERT_EQUAL(fn, static_cast<std::size_t>(std::count(collected.begin(), collected.end(), 'A')),
		static_cast<std::size_t>(chunks_per));
	ASSERT_EQUAL(fn, static_cast<std::size_t>(std::count(collected.begin(), collected.end(), 'B')),
		static_cast<std::size_t>(chunks_per));
	ASSERT_EQUAL(fn, static_cast<std::size_t>(std::count(collected.begin(), collected.end(), 'C')),
		static_cast<std::size_t>(chunks_per));
	ASSERT_EQUAL(fn, collected.size(), static_cast<std::size_t>(chunks_per * 3));
	RETURN_TEST(fn, 0);
}

int test_ring_peek_blocking() {
	const std::string fn = "test_ring_peek_blocking";
	Ring ring;
	std::thread writer([&] {
		std::this_thread::sleep_for(std::chrono::milliseconds(25));
		(void)ring.Write("0123456789");
		ring.Close();
	});
	BinaryData data;
	ASSERT_TRUE(fn, ring.Peek(10, data));
	ASSERT_EQUAL(fn, BytesToText(data), std::string("0123456789"));
	ASSERT_EQUAL(fn, ring.Available(), StormByte::ByteSize{10});
	writer.join();
	RETURN_TEST(fn, 0);
}

int test_ring_set_error_unblocks_and_fails() {
	const std::string fn = "test_ring_set_error_unblocks_and_fails";
	Ring ring;
	std::atomic<bool> completed{false};
	bool read_ok = true;
	std::thread consumer([&] {
		BinaryData data;
		read_ok = ring.Read(50, data);
		completed = true;
	});
	std::this_thread::sleep_for(std::chrono::milliseconds(15));
	ring.SetError();
	consumer.join();
	ASSERT_TRUE(fn, completed.load());
	ASSERT_FALSE(fn, read_ok);
	ASSERT_TRUE(fn, ring.HasError());
	ASSERT_FALSE(fn, ring.IsReadable());
	ASSERT_FALSE(fn, ring.IsWritable());
	RETURN_TEST(fn, 0);
}

int test_ring_single_writer_multiple_readers() {
	const std::string fn = "test_ring_single_writer_multiple_readers";
	Ring ring;
	const int total = 200;
	std::atomic<std::size_t> c1{0}, c2{0}, c3{0};
	std::thread writer([&] {
		for (int i = 0; i < total; ++i)
			(void)ring.Write("X");
		ring.Close();
	});
	auto reader_fn = [&](std::atomic<std::size_t>& counter) {
		while (!ring.EoF()) {
			BinaryData part;
			if (ring.Extract(5, part) && !part.empty())
				counter += static_cast<std::size_t>(part.size());
			else if (ring.EoF())
				break;
			else
				std::this_thread::yield();
		}
		BinaryData rem;
		if (ring.Extract(0, rem) && !rem.empty())
			counter += static_cast<std::size_t>(rem.size());
	};
	std::thread r1(reader_fn, std::ref(c1));
	std::thread r2(reader_fn, std::ref(c2));
	std::thread r3(reader_fn, std::ref(c3));
	writer.join();
	r1.join();
	r2.join();
	r3.join();
	ASSERT_EQUAL(fn, c1.load() + c2.load() + c3.load(), static_cast<std::size_t>(total));
	ASSERT_TRUE(fn, (c1.load() > 0) || (c2.load() > 0) || (c3.load() > 0));
	RETURN_TEST(fn, 0);
}

int test_ring_single_writer_single_reader() {
	const std::string fn = "test_ring_single_writer_single_reader";
	Ring ring;
	const int messages = 100;
	std::atomic<bool> writer_done{false};
	std::string collected;
	std::thread writer([&] {
		for (int i = 0; i < messages; ++i)
			(void)ring.Write(std::to_string(i) + ",");
		ring.Close();
		writer_done = true;
	});
	std::thread reader([&] {
		BinaryData all;
		ring.ExtractUntilEoF(all);
		collected = BytesToText(all);
	});
	writer.join();
	reader.join();
	ASSERT_TRUE(fn, writer_done.load());
	ASSERT_FALSE(fn, collected.empty());
	ASSERT_TRUE(fn, ring.EoF());
	RETURN_TEST(fn, 0);
}

int main() {
	int result = 0;

	// -------------------
	// Basic
	// -------------------
	result += test_ring_basic_write_read();
	result += test_ring_clear();
	result += test_ring_clean_after_seek();
	result += test_ring_close_mechanism();
	result += test_ring_drop();
	result += test_ring_empty_read_failure();
	result += test_ring_extract_destructive();
	result += test_ring_move_semantics();
	result += test_ring_multiple_writes();
	result += test_ring_peek_does_not_consume();
	result += test_ring_seek_operations();

	// -------------------
	// Position
	// -------------------
	result += test_ring_extract_then_seek();
	result += test_ring_extract_until_eof_waits_for_producer();
	result += test_ring_extract_zero_returns_all();
	result += test_ring_partial_extract_leaves_valid_position();
	result += test_ring_read_zero_on_closed_empty_fails();
	result += test_ring_read_zero_on_open_empty_waits_until_close();

	// -------------------
	// Stress
	// -------------------
	result += test_ring_alternating_small_large();
	result += test_ring_available_bytes_consistency();
	result += test_ring_burst_then_drain();
	result += test_ring_clear_while_producing();
	result += test_ring_interleaved_read_extract();
	result += test_ring_partial_read_on_closed();
	result += test_ring_polymorphic_interface_abi();
	result += test_ring_stress_rapid_small_writes();
	result += test_ring_very_large_transfer();

	// -------------------
	// Threading
	// -------------------
	result += test_ring_blocking_read_waits_for_data();
	result += test_ring_close_unblocks_waiter();
	result += test_ring_multiple_writers_single_reader();
	result += test_ring_peek_blocking();
	result += test_ring_set_error_unblocks_and_fails();
	result += test_ring_single_writer_multiple_readers();
	result += test_ring_single_writer_single_reader();

	if (result == 0)
		std::cout << "All tests passed!" << std::endl;
	else
		std::cout << result << " tests failed." << std::endl;
	return result;
}
