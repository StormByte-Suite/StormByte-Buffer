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

#include <StormByte/buffer/consumer.hxx>
#include <StormByte/buffer/external.hxx>
#include <StormByte/buffer/producer.hxx>
#include <StormByte/test_handlers.h>

#include <algorithm>
#include <atomic>
#include <cctype>
#include <chrono>
#include <cstddef>
#include <iostream>
#include <string>
#include <thread>
#include <vector>

using StormByte::Buffer::Consumer;
using StormByte::BinaryData;
using StormByte::Buffer::ExternalBufferWriter;
using StormByte::Buffer::ExternalWriter;
using StormByte::Buffer::Position;
using StormByte::Buffer::Producer;

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

int test_consumer_producer_shares_ring() {
	const std::string fn = "test_consumer_producer_shares_ring";
	Producer origin;
	auto consumer = origin.Consumer();
	Producer tip = consumer.Producer();
	ASSERT_TRUE(fn, origin == tip);
	ASSERT_TRUE(fn, tip.Write("RING"));
	BinaryData data;
	ASSERT_TRUE(fn, consumer.Extract(4, data));
	ASSERT_EQUAL(fn, std::string("RING"), BytesToText(data));
	tip.Close();
	ASSERT_FALSE(fn, origin.IsWritable());
	ASSERT_TRUE(fn, consumer.EoF());
	RETURN_TEST(fn, 0);
}

int test_producer_consumer_basic_write_read() {
	const std::string fn = "test_producer_consumer_basic_write_read";
	Producer producer;
	auto consumer = producer.Consumer();
	const std::string message = "Hello, World!";
	ASSERT_TRUE(fn, producer.Write(message));
	producer.Close();
	ASSERT_EQUAL(fn, StormByte::ByteSize{message.size()}, consumer.Size());
	ASSERT_FALSE(fn, consumer.Empty());
	BinaryData data;
	ASSERT_TRUE(fn, consumer.Read(StormByte::ByteSize{message.size()}, data));
	ASSERT_EQUAL(fn, message, BytesToText(data));
	RETURN_TEST(fn, 0);
}

int test_producer_consumer_byte_vector_write() {
	const std::string fn = "test_producer_consumer_byte_vector_write";
	Producer producer;
	auto consumer = producer.Consumer();
	const std::string bytes = "Binary data";
	ASSERT_TRUE(fn, producer.Write(bytes));
	BinaryData read_data;
	ASSERT_TRUE(fn, consumer.Read(0, read_data));
	ASSERT_EQUAL(fn, StormByte::ByteSize{bytes.size()}, read_data.size());
	ASSERT_EQUAL(fn, std::string("Binary data"), BytesToText(read_data));
	RETURN_TEST(fn, 0);
}

int test_producer_consumer_clear_operation() {
	const std::string fn = "test_producer_consumer_clear_operation";
	Producer producer;
	auto consumer = producer.Consumer();
	ASSERT_TRUE(fn, producer.Write("Some data to clear"));
	ASSERT_FALSE(fn, consumer.Empty());
	consumer.Clear();
	ASSERT_TRUE(fn, consumer.Empty());
	ASSERT_EQUAL(fn, StormByte::ByteSize{0}, consumer.Size());
	ASSERT_TRUE(fn, producer.Write("New data"));
	ASSERT_EQUAL(fn, StormByte::ByteSize{8}, consumer.Size());
	RETURN_TEST(fn, 0);
}

int test_producer_consumer_close_mechanism() {
	const std::string fn = "test_producer_consumer_close_mechanism";
	Producer producer;
	auto consumer = producer.Consumer();
	ASSERT_TRUE(fn, producer.Write("Data"));
	producer.Close();
	ASSERT_FALSE(fn, producer.Write("MoreData"));
	ASSERT_EQUAL(fn, StormByte::ByteSize{4}, consumer.Size());
	RETURN_TEST(fn, 0);
}

int test_producer_consumer_copy_semantics() {
	const std::string fn = "test_producer_consumer_copy_semantics";
	Producer producer1;
	ASSERT_TRUE(fn, producer1.Write("Original"));
	Producer producer2 = producer1;
	ASSERT_TRUE(fn, producer2.Write("Added"));
	auto consumer = producer1.Consumer();
	BinaryData all;
	ASSERT_TRUE(fn, consumer.Read(0, all));
	ASSERT_EQUAL(fn, std::string("OriginalAdded"), BytesToText(all));
	auto consumer2 = consumer;
	ASSERT_EQUAL(fn, consumer.Size(), consumer2.Size());
	RETURN_TEST(fn, 0);
}

int test_producer_consumer_extract() {
	const std::string fn = "test_producer_consumer_extract";
	Producer producer;
	auto consumer = producer.Consumer();
	ASSERT_TRUE(fn, producer.Write("ABCDEFGH"));
	ASSERT_EQUAL(fn, StormByte::ByteSize{8}, consumer.Size());
	BinaryData first, rest;
	ASSERT_TRUE(fn, consumer.Extract(3, first));
	ASSERT_EQUAL(fn, std::string("ABC"), BytesToText(first));
	ASSERT_EQUAL(fn, StormByte::ByteSize{5}, consumer.Size());
	ASSERT_TRUE(fn, consumer.Extract(0, rest));
	ASSERT_EQUAL(fn, std::string("DEFGH"), BytesToText(rest));
	ASSERT_TRUE(fn, consumer.Empty());
	ASSERT_EQUAL(fn, StormByte::ByteSize{0}, consumer.Size());
	RETURN_TEST(fn, 0);
}

int test_producer_consumer_interleaved_operations() {
	const std::string fn = "test_producer_consumer_interleaved_operations";
	Producer producer;
	auto consumer = producer.Consumer();
	ASSERT_TRUE(fn, producer.Write("Part1"));
	producer.Close();
	BinaryData r1, r2;
	ASSERT_TRUE(fn, consumer.Extract(3, r1));
	ASSERT_EQUAL(fn, std::string("Par"), BytesToText(r1));
	ASSERT_TRUE(fn, consumer.Read(0, r2));
	ASSERT_EQUAL(fn, std::string("t1"), BytesToText(r2));
	RETURN_TEST(fn, 0);
}

int test_producer_consumer_move_semantics() {
	const std::string fn = "test_producer_consumer_move_semantics";
	Producer producer1;
	ASSERT_TRUE(fn, producer1.Write("Data"));
	Producer producer2 = std::move(producer1);
	ASSERT_TRUE(fn, producer2.Write("More"));
	auto consumer = producer2.Consumer();
	ASSERT_EQUAL(fn, StormByte::ByteSize{8}, consumer.Size());
	auto consumer2 = std::move(consumer);
	BinaryData data;
	ASSERT_TRUE(fn, consumer2.Read(0, data));
	ASSERT_EQUAL(fn, std::string("DataMore"), BytesToText(data));
	RETURN_TEST(fn, 0);
}

int test_producer_consumer_multiple_writes() {
	const std::string fn = "test_producer_consumer_multiple_writes";
	Producer producer;
	auto consumer = producer.Consumer();
	ASSERT_TRUE(fn, producer.Write("First"));
	ASSERT_TRUE(fn, producer.Write("Second"));
	ASSERT_TRUE(fn, producer.Write("Third"));
	BinaryData all;
	ASSERT_TRUE(fn, consumer.Read(0, all));
	ASSERT_EQUAL(fn, std::string("FirstSecondThird"), BytesToText(all));
	RETURN_TEST(fn, 0);
}

int test_producer_consumer_seek_operations() {
	const std::string fn = "test_producer_consumer_seek_operations";
	Producer producer;
	auto consumer = producer.Consumer();
	ASSERT_TRUE(fn, producer.Write("0123456789"));
	producer.Close();
	BinaryData from5, fromStart, from7;
	ASSERT_TRUE(fn, consumer.Read(5, from5));
	ASSERT_EQUAL(fn, std::string("01234"), BytesToText(from5));
	consumer.Seek(-5, Position::Relative);
	ASSERT_TRUE(fn, consumer.Read(0, fromStart));
	ASSERT_EQUAL(fn, std::string("0123456789"), BytesToText(fromStart));
	consumer.Seek(7, Position::Absolute);
	ASSERT_TRUE(fn, consumer.Read(3, from7));
	ASSERT_EQUAL(fn, std::string("789"), BytesToText(from7));
	RETURN_TEST(fn, 0);
}

int test_producer_consumer_span_until_eof() {
	const std::string fn = "test_producer_consumer_span_until_eof";
	Producer producer;
	auto consumer = producer.Consumer();
	ASSERT_TRUE(fn, producer.Write("ABCDEFGH"));
	BinaryData s1, s2, s3;
	ASSERT_TRUE(fn, consumer.Read(3, s1));
	ASSERT_EQUAL(fn, StormByte::ByteSize{3}, s1.size());
	ASSERT_TRUE(fn, consumer.Read(3, s2));
	ASSERT_EQUAL(fn, StormByte::ByteSize{3}, s2.size());
	ASSERT_TRUE(fn, consumer.Read(2, s3));
	ASSERT_EQUAL(fn, StormByte::ByteSize{2}, s3.size());
	ASSERT_EQUAL(fn, StormByte::ByteSize{0}, consumer.Available());
	ASSERT_FALSE(fn, consumer.EoF());
	producer.Close();
	ASSERT_TRUE(fn, consumer.EoF());
	RETURN_TEST(fn, 0);
}

int test_producer_consumer_with_reserve() {
	const std::string fn = "test_producer_consumer_with_reserve";
	Producer producer;
	auto consumer = producer.Consumer();
	const std::string large_message(500, 'Z');
	ASSERT_TRUE(fn, producer.Write(large_message));
	ASSERT_TRUE(fn, producer.Write(large_message));
	ASSERT_EQUAL(fn, StormByte::ByteSize{1000}, consumer.Size());
	BinaryData data;
	ASSERT_TRUE(fn, consumer.Extract(0, data));
	ASSERT_EQUAL(fn, StormByte::ByteSize{1000}, data.size());
	ASSERT_EQUAL(fn, StormByte::ByteSize{0}, consumer.Size());
	RETURN_TEST(fn, 0);
}

int test_producer_write_span_consumer_read() {
	const std::string fn = "test_producer_write_span_consumer_read";
	Producer producer;
	auto consumer = producer.Consumer();
	ASSERT_TRUE(fn, producer.Write("PCSPAN"));
	BinaryData r;
	ASSERT_TRUE(fn, consumer.Read(6, r));
	ASSERT_EQUAL(fn, std::string("PCSPAN"), BytesToText(r));
	RETURN_TEST(fn, 0);
}

// -------------------
// Blocking / reliability
// -------------------

int test_alternating_small_large_writes() {
	const std::string fn = "test_alternating_small_large_writes";
	Producer producer;
	auto consumer = producer.Consumer();
	ASSERT_TRUE(fn, producer.Write("A"));
	ASSERT_TRUE(fn, producer.Write(std::string(100, 'B')));
	ASSERT_TRUE(fn, producer.Write("C"));
	producer.Close();
	BinaryData data;
	ASSERT_TRUE(fn, consumer.Extract(0, data));
	ASSERT_EQUAL(fn, StormByte::ByteSize{102}, data.size());
	RETURN_TEST(fn, 0);
}

int test_burst_writes_with_reserve() {
	const std::string fn = "test_burst_writes_with_reserve";
	Producer producer;
	auto consumer = producer.Consumer();
	for (int i = 0; i < 32; ++i)
		ASSERT_TRUE(fn, producer.Write("Z"));
	producer.Close();
	ASSERT_EQUAL(fn, StormByte::ByteSize{32}, consumer.Size());
	BinaryData data;
	ASSERT_TRUE(fn, consumer.Extract(0, data));
	ASSERT_EQUAL(fn, StormByte::ByteSize{32}, data.size());
	RETURN_TEST(fn, 0);
}

int test_consumer_clear_during_production() {
	const std::string fn = "test_consumer_clear_during_production";
	Producer producer;
	auto consumer = producer.Consumer();
	ASSERT_TRUE(fn, producer.Write("XXXX"));
	consumer.Clear();
	ASSERT_EQUAL(fn, StormByte::ByteSize{0}, consumer.Size());
	ASSERT_TRUE(fn, producer.Write("YY"));
	ASSERT_EQUAL(fn, StormByte::ByteSize{2}, consumer.Size());
	RETURN_TEST(fn, 0);
}

int test_consumer_extract_until_eof() {
	const std::string fn = "test_consumer_extract_until_eof";
	Producer producer;
	auto consumer = producer.Consumer();
	ASSERT_TRUE(fn, producer.Write("GONE"));
	producer.Close();
	BinaryData data;
	consumer.ExtractUntilEoF(data);
	ASSERT_EQUAL(fn, std::string("GONE"), BytesToText(data));
	ASSERT_EQUAL(fn, StormByte::ByteSize{0}, consumer.Size());
	RETURN_TEST(fn, 0);
}

int test_consumer_peek_basic() {
	const std::string fn = "test_consumer_peek_basic";
	Producer producer;
	auto consumer = producer.Consumer();
	ASSERT_TRUE(fn, producer.Write("PEEK"));
	BinaryData peeked, read;
	ASSERT_TRUE(fn, consumer.Peek(2, peeked));
	ASSERT_EQUAL(fn, std::string("PE"), BytesToText(peeked));
	ASSERT_EQUAL(fn, StormByte::ByteSize{4}, consumer.Available());
	ASSERT_TRUE(fn, consumer.Read(2, read));
	ASSERT_EQUAL(fn, std::string("PE"), BytesToText(read));
	RETURN_TEST(fn, 0);
}

int test_consumer_peek_blocking() {
	const std::string fn = "test_consumer_peek_blocking";
	Producer producer;
	auto consumer = producer.Consumer();
	std::string got;
	std::thread waiter([&] {
		BinaryData data;
		if (consumer.Peek(4, data))
			got = BytesToText(data);
	});
	std::this_thread::sleep_for(std::chrono::milliseconds(10));
	ASSERT_TRUE(fn, producer.Write("ABCD"));
	waiter.join();
	ASSERT_EQUAL(fn, std::string("ABCD"), got);
	ASSERT_EQUAL(fn, StormByte::ByteSize{4}, consumer.Available());
	RETURN_TEST(fn, 0);
}

int test_consumer_read_until_eof() {
	const std::string fn = "test_consumer_read_until_eof";
	Producer producer;
	auto consumer = producer.Consumer();
	ASSERT_TRUE(fn, producer.Write("UNTILEOF"));
	producer.Close();
	BinaryData data;
	consumer.ReadUntilEoF(data);
	ASSERT_EQUAL(fn, std::string("UNTILEOF"), BytesToText(data));
	RETURN_TEST(fn, 0);
}

int test_consumer_waits_for_insufficient_data() {
	const std::string fn = "test_consumer_waits_for_insufficient_data";
	Producer producer;
	auto consumer = producer.Consumer();
	std::atomic<bool> read_started {false};
	std::atomic<bool> read_completed {false};
	std::string result;
	std::thread consumer_thread([&] {
		read_started.store(true);
		BinaryData data;
		if (consumer.Read(20, data))
			result = BytesToText(data);
		else if (consumer.Available() > 0) {
			BinaryData rem;
			if (consumer.Read(0, rem))
				result = BytesToText(rem);
		}
		read_completed.store(true);
	});
	std::this_thread::sleep_for(std::chrono::milliseconds(10));
	ASSERT_TRUE(fn, read_started.load());
	ASSERT_FALSE(fn, read_completed.load());
	ASSERT_TRUE(fn, producer.Write("0123456789"));
	std::this_thread::sleep_for(std::chrono::milliseconds(10));
	ASSERT_FALSE(fn, read_completed.load());
	producer.Close();
	consumer_thread.join();
	ASSERT_TRUE(fn, read_completed.load());
	ASSERT_EQUAL(fn, std::string("0123456789"), result);
	ASSERT_EQUAL(fn, static_cast<std::size_t>(10), result.size());
	RETURN_TEST(fn, 0);
}

int test_empty_read_failure() {
	const std::string fn = "test_empty_read_failure";
	Producer producer;
	auto consumer = producer.Consumer();
	producer.Close();
	BinaryData data;
	ASSERT_FALSE(fn, consumer.Read(4, data));
	ASSERT_TRUE(fn, consumer.EoF());
	RETURN_TEST(fn, 0);
}

int test_extract_zero_bytes_behavior() {
	const std::string fn = "test_extract_zero_bytes_behavior";
	Producer producer;
	auto consumer = producer.Consumer();
	ASSERT_TRUE(fn, producer.Write("TestData"));
	producer.Close();
	BinaryData data;
	ASSERT_TRUE(fn, consumer.Extract(0, data));
	ASSERT_EQUAL(fn, std::string("TestData"), BytesToText(data));
	ASSERT_TRUE(fn, consumer.Empty());
	RETURN_TEST(fn, 0);
}

int test_interleaved_read_extract_with_blocking() {
	const std::string fn = "test_interleaved_read_extract_with_blocking";
	Producer producer;
	auto consumer = producer.Consumer();
	ASSERT_TRUE(fn, producer.Write("ABCDEFGH"));
	producer.Close();
	BinaryData r1, e1;
	ASSERT_TRUE(fn, consumer.Read(5, r1));
	ASSERT_TRUE(fn, consumer.Extract(3, e1));
	ASSERT_EQUAL(fn, StormByte::ByteSize{5}, r1.size());
	ASSERT_EQUAL(fn, StormByte::ByteSize{3}, e1.size());
	RETURN_TEST(fn, 0);
}

int test_multiple_consumers_with_partial_data() {
	const std::string fn = "test_multiple_consumers_with_partial_data";
	Producer producer;
	auto consumer = producer.Consumer();
	std::atomic<std::size_t> reads_completed {0};
	std::vector<std::string> results(3);
	auto consumer_func = [&](int id) {
		Consumer cons = consumer;
		BinaryData data;
		if (cons.Read(5, data))
			results[static_cast<std::size_t>(id)] = BytesToText(data);
		reads_completed.fetch_add(1);
	};
	std::thread cons1(consumer_func, 0);
	std::thread cons2(consumer_func, 1);
	std::thread cons3(consumer_func, 2);
	std::this_thread::sleep_for(std::chrono::milliseconds(20));
	ASSERT_EQUAL(fn, static_cast<std::size_t>(0), reads_completed.load());
	ASSERT_TRUE(fn, producer.Write("ABCDE"));
	std::this_thread::sleep_for(std::chrono::milliseconds(20));
	ASSERT_EQUAL(fn, static_cast<std::size_t>(1), reads_completed.load());
	ASSERT_TRUE(fn, producer.Write("FGHIJ"));
	std::this_thread::sleep_for(std::chrono::milliseconds(20));
	ASSERT_EQUAL(fn, static_cast<std::size_t>(2), reads_completed.load());
	ASSERT_TRUE(fn, producer.Write("KLM"));
	producer.Close();
	cons1.join();
	cons2.join();
	cons3.join();
	ASSERT_EQUAL(fn, static_cast<std::size_t>(3), reads_completed.load());
	std::size_t total_received = 0;
	std::size_t with_data = 0;
	for (const auto& res : results) {
		total_received += res.size();
		if (!res.empty())
			++with_data;
	}
	ASSERT_TRUE(fn, with_data >= 1);
	ASSERT_TRUE(fn, total_received > 0 && total_received <= 13);
	RETURN_TEST(fn, 0);
}

int test_multiple_sequential_read_blocks() {
	const std::string fn = "test_multiple_sequential_read_blocks";
	Producer producer;
	auto consumer = producer.Consumer();
	ASSERT_TRUE(fn, producer.Write("ABCDEFGHIJ"));
	producer.Close();
	BinaryData a, b;
	ASSERT_TRUE(fn, consumer.Read(4, a));
	ASSERT_TRUE(fn, consumer.Read(6, b));
	ASSERT_EQUAL(fn, std::string("ABCD"), BytesToText(a));
	ASSERT_EQUAL(fn, std::string("EFGHIJ"), BytesToText(b));
	RETURN_TEST(fn, 0);
}

int test_out_of_sync_partial_writes() {
	const std::string fn = "test_out_of_sync_partial_writes";
	Producer producer;
	auto consumer = producer.Consumer();
	std::atomic<bool> consumer_done {false};
	std::string result;
	std::thread consumer_thread([&] {
		BinaryData data;
		if (consumer.Read(10, data))
			result = BytesToText(data);
		consumer_done.store(true);
	});
	std::thread producer_thread([&] {
		static_cast<void>(producer.Write("AB"));
		std::this_thread::sleep_for(std::chrono::milliseconds(50));
		static_cast<void>(producer.Write("CDEFGH"));
		std::this_thread::sleep_for(std::chrono::milliseconds(50));
		static_cast<void>(producer.Write("IJ"));
		producer.Close();
	});
	producer_thread.join();
	consumer_thread.join();
	ASSERT_TRUE(fn, consumer_done.load());
	ASSERT_EQUAL(fn, std::string("ABCDEFGHIJ"), result);
	RETURN_TEST(fn, 0);
}

int test_producer_close_during_consumer_wait() {
	const std::string fn = "test_producer_close_during_consumer_wait";
	Producer producer;
	auto consumer = producer.Consumer();
	std::atomic<bool> completed {false};
	std::string result;
	std::thread consumer_thread([&] {
		BinaryData data;
		if (consumer.Read(100, data))
			result = BytesToText(data);
		else if (consumer.Available() > 0) {
			BinaryData rem;
			if (consumer.Read(0, rem))
				result = BytesToText(rem);
		}
		completed.store(true);
	});
	std::this_thread::sleep_for(std::chrono::milliseconds(10));
	ASSERT_TRUE(fn, producer.Write("Short"));
	producer.Close();
	consumer_thread.join();
	ASSERT_TRUE(fn, completed.load());
	ASSERT_EQUAL(fn, std::string("Short"), result);
	ASSERT_TRUE(fn, result.size() < 100);
	RETURN_TEST(fn, 0);
}

int test_producer_consumer_available_bytes() {
	const std::string fn = "test_producer_consumer_available_bytes";
	Producer producer;
	auto consumer = producer.Consumer();
	ASSERT_EQUAL(fn, StormByte::ByteSize{0}, consumer.Available());
	ASSERT_TRUE(fn, producer.Write("ABCD"));
	ASSERT_EQUAL(fn, StormByte::ByteSize{4}, consumer.Available());
	BinaryData data;
	ASSERT_TRUE(fn, consumer.Read(2, data));
	ASSERT_EQUAL(fn, StormByte::ByteSize{2}, consumer.Available());
	RETURN_TEST(fn, 0);
}

int test_producer_consumer_available_bytes_threaded() {
	const std::string fn = "test_producer_consumer_available_bytes_threaded";
	Producer producer;
	auto consumer = producer.Consumer();
	std::thread writer([&] {
		static_cast<void>(producer.Write("HELLO"));
		producer.Close();
	});
	writer.join();
	ASSERT_EQUAL(fn, StormByte::ByteSize{5}, consumer.Available());
	RETURN_TEST(fn, 0);
}

int test_producer_consumer_partial_read_eof() {
	const std::string fn = "test_producer_consumer_partial_read_eof";
	Producer producer;
	auto consumer = producer.Consumer();
	ASSERT_TRUE(fn, producer.Write("XY"));
	producer.Close();
	BinaryData data;
	static_cast<void>(consumer.Read(8, data));
	if (data.empty()) {
		BinaryData rem;
		static_cast<void>(consumer.Read(0, rem));
		data = std::move(rem);
	}
	ASSERT_EQUAL(fn, std::string("XY"), BytesToText(data));
	ASSERT_TRUE(fn, consumer.EoF());
	RETURN_TEST(fn, 0);
}

int test_producer_consumer_polymorphic_interface_abi() {
	const std::string fn = "test_producer_consumer_polymorphic_interface_abi";
	Producer producer;
	auto consumer = producer.Consumer();
	ASSERT_TRUE(fn, producer.IsWritable());
	ASSERT_TRUE(fn, consumer.IsReadable());
	ASSERT_TRUE(fn, producer.Write("ABI"));
	ASSERT_EQUAL(fn, StormByte::ByteSize{3}, producer.Size());
	ASSERT_EQUAL(fn, StormByte::ByteSize{3}, consumer.Size());
	RETURN_TEST(fn, 0);
}

int test_rapid_write_close_with_slow_consumer() {
	const std::string fn = "test_rapid_write_close_with_slow_consumer";
	Producer producer;
	auto consumer = producer.Consumer();
	std::atomic<bool> producer_done {false};
	std::atomic<std::size_t> total_consumed {0};
	std::thread producer_thread([&] {
		for (int i = 0; i < 100; ++i)
			static_cast<void>(producer.Write("X"));
		producer.Close();
		producer_done.store(true);
	});
	std::thread consumer_thread([&] {
		for (;;) {
			std::this_thread::sleep_for(std::chrono::milliseconds(5));
			BinaryData part;
			if (!consumer.Extract(5, part)) {
				if (consumer.Available() > 0) {
					BinaryData rem;
					if (consumer.Extract(0, rem))
						total_consumed.fetch_add(static_cast<std::size_t>(rem.size()));
				}
				break;
			}
			if (part.empty() && consumer.EoF())
				break;
			total_consumed.fetch_add(static_cast<std::size_t>(part.size()));
		}
	});
	producer_thread.join();
	consumer_thread.join();
	ASSERT_TRUE(fn, producer_done.load());
	ASSERT_EQUAL(fn, static_cast<std::size_t>(100), total_consumed.load());
	RETURN_TEST(fn, 0);
}

int test_seek_during_blocked_read() {
	const std::string fn = "test_seek_during_blocked_read";
	Producer producer;
	auto consumer = producer.Consumer();
	ASSERT_TRUE(fn, producer.Write("0123456789"));
	producer.Close();
	consumer.Seek(4, Position::Absolute);
	BinaryData data;
	ASSERT_TRUE(fn, consumer.Read(0, data));
	ASSERT_EQUAL(fn, std::string("456789"), BytesToText(data));
	RETURN_TEST(fn, 0);
}

int test_very_large_data_transfer() {
	const std::string fn = "test_very_large_data_transfer";
	Producer producer;
	auto consumer = producer.Consumer();
	const std::string payload(64 * 1024, 'A');
	ASSERT_TRUE(fn, producer.Write(payload));
	producer.Close();
	BinaryData data;
	ASSERT_TRUE(fn, consumer.Extract(0, data));
	ASSERT_EQUAL(fn, StormByte::ByteSize{payload.size()}, data.size());
	ASSERT_EQUAL(fn, StormByte::ByteSize{0}, consumer.Size());
	RETURN_TEST(fn, 0);
}

// -------------------
// Occupied / live Size
// -------------------

int test_occupied_drops_after_consumer_extract() {
	const std::string fn = "test_occupied_drops_after_consumer_extract";
	Producer producer;
	auto consumer = producer.Consumer();
	ExternalBufferWriter adapter(producer);
	ASSERT_TRUE(fn, adapter.Write("ABCDEFGH"));
	ASSERT_EQUAL(fn, StormByte::ByteSize{8}, adapter.Occupied());
	ASSERT_EQUAL(fn, producer.Size(), adapter.Occupied());
	ASSERT_EQUAL(fn, consumer.Size(), adapter.Occupied());
	BinaryData first;
	ASSERT_TRUE(fn, consumer.Extract(3, first));
	ASSERT_EQUAL(fn, std::string("ABC"), BytesToText(first));
	ASSERT_EQUAL(fn, StormByte::ByteSize{5}, consumer.Size());
	ASSERT_EQUAL(fn, StormByte::ByteSize{5}, producer.Size());
	ASSERT_EQUAL(fn, StormByte::ByteSize{5}, adapter.Occupied());
	BinaryData rest;
	ASSERT_TRUE(fn, consumer.Extract(0, rest));
	ASSERT_EQUAL(fn, std::string("DEFGH"), BytesToText(rest));
	ASSERT_EQUAL(fn, StormByte::ByteSize{0}, consumer.Size());
	ASSERT_EQUAL(fn, StormByte::ByteSize{0}, producer.Size());
	ASSERT_EQUAL(fn, StormByte::ByteSize{0}, adapter.Occupied());
	ASSERT_TRUE(fn, consumer.Empty());
	RETURN_TEST(fn, 0);
}

int test_occupied_drops_under_concurrent_extract() {
	const std::string fn = "test_occupied_drops_under_concurrent_extract";
	Producer producer;
	auto consumer = producer.Consumer();
	ExternalBufferWriter adapter(producer);
	const std::string payload(256, 'Z');
	ASSERT_TRUE(fn, adapter.Write(payload));
	ASSERT_EQUAL(fn, StormByte::ByteSize{payload.size()}, adapter.Occupied());
	std::atomic<std::size_t> taken {0};
	std::thread cons([&] {
		while (taken.load() < payload.size()) {
			BinaryData chunk;
			const std::size_t left = payload.size() - taken.load();
			const std::size_t n = std::min<std::size_t>(16, left);
			if (!consumer.Extract(n, chunk))
				break;
			taken.fetch_add(static_cast<std::size_t>(chunk.size()));
		}
	});
	cons.join();
	ASSERT_EQUAL(fn, payload.size(), taken.load());
	ASSERT_EQUAL(fn, StormByte::ByteSize{0}, adapter.Occupied());
	ASSERT_EQUAL(fn, StormByte::ByteSize{0}, producer.Size());
	ASSERT_EQUAL(fn, StormByte::ByteSize{0}, consumer.Size());
	RETURN_TEST(fn, 0);
}

int test_occupied_read_does_not_drop() {
	const std::string fn = "test_occupied_read_does_not_drop";
	Producer producer;
	auto consumer = producer.Consumer();
	ExternalBufferWriter adapter(producer);
	ASSERT_TRUE(fn, adapter.Write("ABCDEF"));
	BinaryData peek;
	ASSERT_TRUE(fn, consumer.Read(2, peek));
	ASSERT_EQUAL(fn, StormByte::ByteSize{6}, adapter.Occupied());
	ASSERT_EQUAL(fn, StormByte::ByteSize{6}, producer.Size());
	ASSERT_EQUAL(fn, StormByte::ByteSize{4}, consumer.Available());
	RETURN_TEST(fn, 0);
}

int test_occupied_tracks_producer_size() {
	const std::string fn = "test_occupied_tracks_producer_size";
	Producer producer;
	ExternalBufferWriter adapter(producer);
	ExternalWriter& writer = adapter;
	ASSERT_EQUAL(fn, StormByte::ByteSize{0}, writer.Occupied());
	ASSERT_EQUAL(fn, producer.Size(), writer.Occupied());
	ASSERT_TRUE(fn, writer.Write("HELLO"));
	ASSERT_EQUAL(fn, StormByte::ByteSize{5}, writer.Occupied());
	ASSERT_EQUAL(fn, producer.Size(), writer.Occupied());
	RETURN_TEST(fn, 0);
}

// -------------------
// Threading
// -------------------

int test_multiple_producers_multiple_consumers() {
	const std::string fn = "test_multiple_producers_multiple_consumers";
	Producer producer;
	auto consumer = producer.Consumer();
	const int producers_count = 3;
	const int consumers_count = 2;
	const int messages_per_producer = 30;
	std::atomic<int> completed_producers {0};
	std::atomic<std::size_t> total_consumed {0};
	std::vector<std::thread> producers;
	for (int p = 0; p < producers_count; ++p) {
		producers.emplace_back([&, p] {
			Producer prod_copy = producer;
			for (int i = 0; i < messages_per_producer; ++i)
				static_cast<void>(prod_copy.Write(std::string(1, static_cast<char>('A' + p))));
			completed_producers.fetch_add(1);
		});
	}
	std::vector<std::thread> consumers;
	for (int c = 0; c < consumers_count; ++c) {
		consumers.emplace_back([&] {
			Consumer cons_copy = consumer;
			std::size_t local_consumed = 0;
			for (;;) {
				BinaryData part;
				if (!cons_copy.Extract(10, part)) {
					if (cons_copy.Available() > 0) {
						BinaryData rem;
						if (cons_copy.Extract(0, rem))
							local_consumed += static_cast<std::size_t>(rem.size());
					}
					break;
				}
				if (part.empty() && cons_copy.EoF())
					break;
				local_consumed += static_cast<std::size_t>(part.size());
			}
			total_consumed.fetch_add(local_consumed);
		});
	}
	for (auto& t : producers)
		t.join();
	producer.Close();
	for (auto& t : consumers)
		t.join();
	ASSERT_EQUAL(fn, producers_count, completed_producers.load());
	ASSERT_EQUAL(fn, static_cast<std::size_t>(producers_count * messages_per_producer), total_consumed.load());
	RETURN_TEST(fn, 0);
}

int test_multiple_producers_single_consumer() {
	const std::string fn = "test_multiple_producers_single_consumer";
	Producer producer;
	auto consumer = producer.Consumer();
	const int chunks_per_producer = 50;
	std::atomic<int> completed_producers {0};
	auto producer_func = [&](char id) {
		Producer prod_copy = producer;
		for (int i = 0; i < chunks_per_producer; ++i)
			static_cast<void>(prod_copy.Write(std::string(1, id)));
		completed_producers.fetch_add(1);
	};
	std::thread prod1(producer_func, 'A');
	std::thread prod2(producer_func, 'B');
	std::thread prod3(producer_func, 'C');
	std::string collected;
	std::thread cons_thread([&] {
		std::this_thread::sleep_for(std::chrono::milliseconds(10));
		while (completed_producers.load() < 3) {
			BinaryData part;
			if (!consumer.Extract(10, part)) {
				if (consumer.Available() > 0) {
					BinaryData rem;
					if (consumer.Extract(0, rem) && !rem.empty())
						collected.append(BytesToText(rem));
				}
				break;
			}
			if (!part.empty())
				collected.append(BytesToText(part));
			std::this_thread::sleep_for(std::chrono::milliseconds(1));
		}
		while (!consumer.Empty()) {
			BinaryData data;
			if (consumer.Extract(0, data) && !data.empty())
				collected.append(BytesToText(data));
		}
	});
	prod1.join();
	prod2.join();
	prod3.join();
	cons_thread.join();
	ASSERT_EQUAL(fn, 3, completed_producers.load());
	ASSERT_EQUAL(fn, static_cast<std::size_t>(chunks_per_producer),
		static_cast<std::size_t>(std::count(collected.begin(), collected.end(), 'A')));
	ASSERT_EQUAL(fn, static_cast<std::size_t>(chunks_per_producer),
		static_cast<std::size_t>(std::count(collected.begin(), collected.end(), 'B')));
	ASSERT_EQUAL(fn, static_cast<std::size_t>(chunks_per_producer),
		static_cast<std::size_t>(std::count(collected.begin(), collected.end(), 'C')));
	ASSERT_EQUAL(fn, static_cast<std::size_t>(chunks_per_producer * 3), collected.size());
	RETURN_TEST(fn, 0);
}

int test_producer_consumer_pipeline_pattern() {
	const std::string fn = "test_producer_consumer_pipeline_pattern";
	Producer stage1_producer;
	auto stage1_consumer = stage1_producer.Consumer();
	Producer stage2_producer;
	auto stage2_consumer = stage2_producer.Consumer();
	std::atomic<bool> done {false};
	std::thread stage1([&] {
		for (int i = 0; i < 100; ++i)
			static_cast<void>(stage1_producer.Write(std::to_string(i) + ","));
		stage1_producer.Close();
	});
	std::thread stage2([&] {
		for (;;) {
			BinaryData part;
			if (!stage1_consumer.Extract(10, part)) {
				if (stage1_consumer.Available() > 0) {
					BinaryData rem;
					if (stage1_consumer.Extract(0, rem) && !rem.empty()) {
						std::string remstr = BytesToText(rem);
						std::transform(remstr.begin(), remstr.end(), remstr.begin(),
							[](unsigned char c) { return static_cast<char>(std::toupper(c)); });
						static_cast<void>(stage2_producer.Write(remstr));
					}
				}
				break;
			}
			if (part.empty() && stage1_consumer.EoF())
				break;
			std::string str = BytesToText(part);
			std::transform(str.begin(), str.end(), str.begin(),
				[](unsigned char c) { return static_cast<char>(std::toupper(c)); });
			static_cast<void>(stage2_producer.Write(str));
		}
		stage2_producer.Close();
	});
	std::string final_result;
	std::thread stage3([&] {
		for (;;) {
			BinaryData part;
			if (!stage2_consumer.Extract(10, part)) {
				if (stage2_consumer.Available() > 0) {
					BinaryData rem;
					if (stage2_consumer.Extract(0, rem) && !rem.empty())
						final_result.append(BytesToText(rem));
				}
				break;
			}
			if (part.empty() && stage2_consumer.EoF())
				break;
			final_result.append(BytesToText(part));
		}
		done.store(true);
	});
	stage1.join();
	stage2.join();
	stage3.join();
	ASSERT_TRUE(fn, done.load());
	ASSERT_FALSE(fn, final_result.empty());
	ASSERT_TRUE(fn, final_result.find('0') != std::string::npos);
	RETURN_TEST(fn, 0);
}

int test_producer_consumer_stress_rapid_operations() {
	const std::string fn = "test_producer_consumer_stress_rapid_operations";
	Producer producer;
	auto consumer = producer.Consumer();
	std::atomic<std::size_t> write_count {0};
	std::atomic<std::size_t> read_count {0};
	std::thread writer([&] {
		for (int i = 0; i < 500; ++i) {
			static_cast<void>(producer.Write("X"));
			write_count.fetch_add(1);
		}
		producer.Close();
	});
	std::thread reader([&] {
		for (;;) {
			BinaryData part;
			if (!consumer.Extract(10, part)) {
				if (consumer.Available() > 0) {
					BinaryData rem;
					if (consumer.Extract(0, rem))
						read_count.fetch_add(static_cast<std::size_t>(rem.size()));
				}
				break;
			}
			if (part.empty() && consumer.EoF())
				break;
			read_count.fetch_add(static_cast<std::size_t>(part.size()));
		}
	});
	writer.join();
	reader.join();
	ASSERT_EQUAL(fn, static_cast<std::size_t>(500), write_count.load());
	ASSERT_EQUAL(fn, write_count.load(), read_count.load());
	ASSERT_TRUE(fn, consumer.Empty());
	RETURN_TEST(fn, 0);
}

int test_single_producer_multiple_consumers() {
	const std::string fn = "test_single_producer_multiple_consumers";
	Producer producer;
	auto consumer1 = producer.Consumer();
	auto consumer2 = consumer1;
	auto consumer3 = consumer1;
	const int total_bytes = 200;
	std::atomic<std::size_t> consumed1 {0}, consumed2 {0}, consumed3 {0};
	std::thread prod_thread([&] {
		for (int i = 0; i < total_bytes; ++i)
			static_cast<void>(producer.Write("X"));
		producer.Close();
	});
	auto consumer_func = [&](Consumer& cons, std::atomic<std::size_t>& counter) {
		for (;;) {
			BinaryData part;
			if (!cons.Extract(5, part)) {
				if (cons.Available() > 0) {
					BinaryData rem;
					if (cons.Extract(0, rem))
						counter.fetch_add(static_cast<std::size_t>(rem.size()));
				}
				break;
			}
			if (part.empty() && cons.EoF())
				break;
			counter.fetch_add(static_cast<std::size_t>(part.size()));
		}
	};
	std::thread cons1_thread(consumer_func, std::ref(consumer1), std::ref(consumed1));
	std::thread cons2_thread(consumer_func, std::ref(consumer2), std::ref(consumed2));
	std::thread cons3_thread(consumer_func, std::ref(consumer3), std::ref(consumed3));
	prod_thread.join();
	cons1_thread.join();
	cons2_thread.join();
	cons3_thread.join();
	ASSERT_EQUAL(fn, static_cast<std::size_t>(total_bytes),
		consumed1.load() + consumed2.load() + consumed3.load());
	const std::size_t with_data =
		(consumed1.load() > 0 ? 1u : 0u) +
		(consumed2.load() > 0 ? 1u : 0u) +
		(consumed3.load() > 0 ? 1u : 0u);
	ASSERT_TRUE(fn, with_data >= 1);
	RETURN_TEST(fn, 0);
}

int test_single_producer_single_consumer_threaded() {
	const std::string fn = "test_single_producer_single_consumer_threaded";
	Producer producer;
	auto consumer = producer.Consumer();
	const int messages = 100;
	std::atomic<bool> producer_done {false};
	std::string collected;
	std::thread prod_thread([&] {
		for (int i = 0; i < messages; ++i)
			static_cast<void>(producer.Write(std::to_string(i) + ","));
		producer.Close();
		producer_done.store(true);
	});
	std::thread cons_thread([&] {
		for (;;) {
			BinaryData part;
			if (!consumer.Extract(10, part)) {
				if (consumer.Available() > 0) {
					BinaryData rem;
					if (consumer.Extract(0, rem) && !rem.empty())
						collected.append(BytesToText(rem));
				}
				break;
			}
			if (part.empty() && consumer.EoF())
				break;
			collected.append(BytesToText(part));
		}
	});
	prod_thread.join();
	cons_thread.join();
	ASSERT_TRUE(fn, producer_done.load());
	ASSERT_FALSE(fn, collected.empty());
	ASSERT_TRUE(fn, consumer.EoF());
	RETURN_TEST(fn, 0);
}

int main() {
	int result = 0;

	// -------------------
	// Basic
	// -------------------
	result += test_consumer_producer_shares_ring();
	result += test_producer_consumer_basic_write_read();
	result += test_producer_consumer_byte_vector_write();
	result += test_producer_consumer_clear_operation();
	result += test_producer_consumer_close_mechanism();
	result += test_producer_consumer_copy_semantics();
	result += test_producer_consumer_extract();
	result += test_producer_consumer_interleaved_operations();
	result += test_producer_consumer_move_semantics();
	result += test_producer_consumer_multiple_writes();
	result += test_producer_consumer_seek_operations();
	result += test_producer_consumer_span_until_eof();
	result += test_producer_consumer_with_reserve();
	result += test_producer_write_span_consumer_read();

	// -------------------
	// Blocking / reliability
	// -------------------
	result += test_alternating_small_large_writes();
	result += test_burst_writes_with_reserve();
	result += test_consumer_clear_during_production();
	result += test_consumer_extract_until_eof();
	result += test_consumer_peek_basic();
	result += test_consumer_peek_blocking();
	result += test_consumer_read_until_eof();
	result += test_consumer_waits_for_insufficient_data();
	result += test_empty_read_failure();
	result += test_extract_zero_bytes_behavior();
	result += test_interleaved_read_extract_with_blocking();
	result += test_multiple_consumers_with_partial_data();
	result += test_multiple_sequential_read_blocks();
	result += test_out_of_sync_partial_writes();
	result += test_producer_close_during_consumer_wait();
	result += test_producer_consumer_available_bytes();
	result += test_producer_consumer_available_bytes_threaded();
	result += test_producer_consumer_partial_read_eof();
	result += test_producer_consumer_polymorphic_interface_abi();
	result += test_rapid_write_close_with_slow_consumer();
	result += test_seek_during_blocked_read();
	result += test_very_large_data_transfer();

	// -------------------
	// Occupied / live Size
	// -------------------
	result += test_occupied_drops_after_consumer_extract();
	result += test_occupied_drops_under_concurrent_extract();
	result += test_occupied_read_does_not_drop();
	result += test_occupied_tracks_producer_size();

	// -------------------
	// Threading
	// -------------------
	result += test_multiple_producers_multiple_consumers();
	result += test_multiple_producers_single_consumer();
	result += test_producer_consumer_pipeline_pattern();
	result += test_producer_consumer_stress_rapid_operations();
	result += test_single_producer_multiple_consumers();
	result += test_single_producer_single_consumer_threaded();

	if (result == 0)
		std::cout << "All tests passed!" << std::endl;
	else
		std::cout << result << " tests failed." << std::endl;
	return result;
}
