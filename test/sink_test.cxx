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

#include <StormByte/buffer/sink.hxx>
#include <StormByte/test_handlers.h>

#include <algorithm>
#include <atomic>
#include <chrono>
#include <condition_variable>
#include <iostream>
#include <memory>
#include <mutex>
#include <string>
#include <thread>
#include <vector>

using StormByte::Buffer::Sink;

class NonNullableSmartPointer {
    public:
        NonNullableSmartPointer() noexcept = default;
        explicit NonNullableSmartPointer(int value) noexcept : m_value(value) {}
        NonNullableSmartPointer(const NonNullableSmartPointer&) noexcept = default;
        NonNullableSmartPointer(NonNullableSmartPointer&&) noexcept = default;
        ~NonNullableSmartPointer() noexcept = default;
        NonNullableSmartPointer& operator=(const NonNullableSmartPointer&) noexcept = default;
        NonNullableSmartPointer& operator=(NonNullableSmartPointer&&) noexcept = default;

        int* get() noexcept { return &m_value; }
        const int* get() const noexcept { return &m_value; }
        int& operator*() noexcept { return m_value; }
        const int& operator*() const noexcept { return m_value; }
        int* operator->() noexcept { return &m_value; }
        const int* operator->() const noexcept { return &m_value; }

    private:
        int m_value = 0;
};

static_assert(StormByte::Type::SmartPointer<NonNullableSmartPointer>);
static_assert(!StormByte::Type::NullablePointer<NonNullableSmartPointer>);

/* -------------------------------------------------------------------------- */
/* Concurrency                                                                */
/* -------------------------------------------------------------------------- */

/**
 * @brief Concurrent To(key) >> consumer loop versus Eof.
 * @return 0 on success.
 */
int test_sink_concurrent_wire_and_eof() {
	Sink<int> producer;
	Sink<int> consumer;

	std::atomic<bool> stop_binding{false};
	std::atomic<int> max_key{100};
	std::thread bind_thread([&]() {
		int key = 100;
		while (!stop_binding.load(std::memory_order_acquire)) {
			producer.To(key) >> consumer;
			max_key.store(key, std::memory_order_release);
			++key;
			std::this_thread::yield();
		}
	});

	std::this_thread::sleep_for(std::chrono::milliseconds(5));
	producer.Eof();
	stop_binding.store(true, std::memory_order_release);
	bind_thread.join();

	ASSERT_TRUE("test_sink_concurrent_wire_and_eof consumer eof", consumer.EoF());

	const int last_key = max_key.load(std::memory_order_acquire);
	for (int k = 100; k <= last_key + 10; ++k) {
		producer.Push(k, 9999);
		ASSERT_EQUAL("test_sink_concurrent_wire_and_eof size after push post eof", StormByte::Size{0}, consumer.Size(k));
	}

	RETURN_TEST("test_sink_concurrent_wire_and_eof", 0);
}

/**
 * @brief Concurrent To(key) >> consumer versus Notify + Push.
 * @return 0 on success.
 */
int test_sink_concurrent_wire_and_notify() {
	Sink<int> producer;
	Sink<int> consumer;

	std::condition_variable cv;
	std::mutex m;
	std::atomic<bool> stop_binding{false};
	std::atomic<int> max_key{200};

	std::thread bind_thread([&]() {
		int key = 200;
		while (!stop_binding.load(std::memory_order_acquire)) {
			producer.To(key) >> consumer;
			max_key.store(key, std::memory_order_release);
			++key;
			std::this_thread::yield();
		}
	});

	std::this_thread::sleep_for(std::chrono::milliseconds(5));
	consumer.Notify(cv);
	stop_binding.store(true, std::memory_order_release);
	bind_thread.join();

	std::atomic<int> received_val{0};
	std::atomic<bool> woken{false};
	std::thread wait_thread([&]() {
		std::unique_lock<std::mutex> lock(m);
		bool ok = cv.wait_for(lock, std::chrono::seconds(1), [&]() {
			int val = consumer.Pop();
			if (val != 0) {
				received_val.store(val, std::memory_order_release);
				return true;
			}
			return false;
		});
		woken.store(ok, std::memory_order_release);
	});

	std::this_thread::sleep_for(std::chrono::milliseconds(20));
	producer.Push(200, 7777);

	wait_thread.join();
	ASSERT_TRUE("test_sink_concurrent_wire_and_notify woken by push", woken.load(std::memory_order_acquire));
	ASSERT_EQUAL("test_sink_concurrent_wire_and_notify received item", 7777, received_val.load(std::memory_order_acquire));

	producer.Eof();

	RETURN_TEST("test_sink_concurrent_wire_and_notify", 0);
}

/* -------------------------------------------------------------------------- */
/* Construction                                                               */
/* -------------------------------------------------------------------------- */

/**
 * @brief Tests default construction of Sink (zero buckets, EoF false, Ready false).
 * @return 0 on success.
 */
int test_sink_default_constructor() {
	Sink<int> sink;
	ASSERT_FALSE("test_sink_default_constructor eof", sink.EoF());
	ASSERT_FALSE("test_sink_default_constructor ready", sink.Ready());
	ASSERT_FALSE("test_sink_default_constructor draining", sink.Draining());
	ASSERT_EQUAL("test_sink_default_constructor missing key capacity", StormByte::Size{0}, sink.Capacity(42));
	ASSERT_EQUAL("test_sink_default_constructor missing key size", StormByte::Size{0}, sink.Size(42));
	ASSERT_FALSE("test_sink_default_constructor missing key full", sink.Full(42));

	RETURN_TEST("test_sink_default_constructor", 0);
}

/* -------------------------------------------------------------------------- */
/* Item types / Drain / Pop                                                   */
/* -------------------------------------------------------------------------- */

/**
 * @brief Drain: Push to an un-wired key discards.
 * @return 0 on success.
 */
int test_sink_drain_mode() {
	Sink<int> producer;
	producer.Drain();

	ASSERT_TRUE("test_sink_drain_mode draining", producer.Draining());

	producer.Push(100, 9999);
	ASSERT_EQUAL("test_sink_drain_mode size 0 for un-bound key", StormByte::Size{0}, producer.Size(100));

	Sink<int> consumer;
	producer.To(100) >> consumer;

	producer.Push(100, 8888);
	ASSERT_EQUAL("test_sink_drain_mode size 1 for bound key", StormByte::Size{1}, consumer.Size(100));
	ASSERT_EQUAL("test_sink_drain_mode pop value", 8888, consumer.Pop());

	RETURN_TEST("test_sink_drain_mode", 0);
}

/**
 * @brief Eof unblocks threads waiting in Push and Pop.
 * @return 0 on success.
 */
int test_sink_eof_unblocks_waiters() {
	Sink<int> producer;
	Sink<int> consumer;

	std::atomic<bool> push_done{false};
	std::atomic<bool> pop_done{false};

	std::thread push_thread([&]() {
		producer.Push(10, 100);
		push_done.store(true, std::memory_order_release);
	});

	std::thread pop_thread([&]() {
		(void)consumer.Pop();
		pop_done.store(true, std::memory_order_release);
	});

	std::this_thread::sleep_for(std::chrono::milliseconds(30));
	ASSERT_FALSE("test_sink_eof_unblocks_waiters push blocked", push_done.load(std::memory_order_acquire));
	ASSERT_FALSE("test_sink_eof_unblocks_waiters pop blocked", pop_done.load(std::memory_order_acquire));

	producer.Eof();
	consumer.Eof();

	push_thread.join();
	pop_thread.join();

	ASSERT_TRUE("test_sink_eof_unblocks_waiters push completed on eof", push_done.load(std::memory_order_acquire));
	ASSERT_TRUE("test_sink_eof_unblocks_waiters pop completed on eof", pop_done.load(std::memory_order_acquire));

	RETURN_TEST("test_sink_eof_unblocks_waiters", 0);
}

/**
 * @brief Smart pointer-like values without nullability pass through Sink.
 * @return 0 on success.
 */
int test_sink_non_nullable_smart_pointer() {
	Sink<NonNullableSmartPointer> producer;
	Sink<NonNullableSmartPointer> consumer;

	producer.To(1) >> consumer;
	producer.Push(1, NonNullableSmartPointer(789));

	ASSERT_EQUAL("test_sink_non_nullable_smart_pointer size", StormByte::Size{1}, consumer.Size(1));
	auto popped = consumer.Pop();
	ASSERT_EQUAL("test_sink_non_nullable_smart_pointer value", 789, *popped);
	ASSERT_FALSE("test_sink_non_nullable_smart_pointer ready", consumer.Ready());

	RETURN_TEST("test_sink_non_nullable_smart_pointer", 0);
}

/**
 * @brief Pop with custom Select chooser.
 * @return 0 on success.
 */
int test_sink_pop_custom_select() {
	Sink<int> producer;
	Sink<int> consumer;

	producer.To(1) >> consumer;
	producer.To(2) >> consumer;

	producer.Push(1, 111);
	producer.Push(2, 222);

	auto select_second = [](StormByte::Size count) -> StormByte::Size {
		return count > 1 ? StormByte::Size{1} : StormByte::Size{0};
	};

	int val = consumer.Pop(select_second);
	ASSERT_EQUAL("test_sink_pop_custom_select selected bucket index 1", 222, val);

	int val_rem = consumer.Pop();
	ASSERT_EQUAL("test_sink_pop_custom_select remaining bucket index 0", 111, val_rem);

	RETURN_TEST("test_sink_pop_custom_select", 0);
}

/**
 * @brief Push waits until the key is wired or Eof.
 * @return 0 on success.
 */
int test_sink_push_waiting_for_wire() {
	Sink<int> producer;
	Sink<int> consumer;

	std::atomic<bool> push_done{false};

	std::thread push_thread([&]() {
		producer.Push(7, 777);
		push_done.store(true, std::memory_order_release);
	});

	std::this_thread::sleep_for(std::chrono::milliseconds(30));
	ASSERT_FALSE("test_sink_push_waiting_for_wire push waiting", push_done.load(std::memory_order_acquire));

	producer.To(7) >> consumer;
	push_thread.join();

	ASSERT_TRUE("test_sink_push_waiting_for_wire push resumed", push_done.load(std::memory_order_acquire));
	ASSERT_EQUAL("test_sink_push_waiting_for_wire popped val", 777, consumer.Pop());

	RETURN_TEST("test_sink_push_waiting_for_wire", 0);
}

/* -------------------------------------------------------------------------- */
/* Notify / Unnotify                                                          */
/* -------------------------------------------------------------------------- */

/**
 * @brief Notify wakes the consumer CV on Push.
 * @return 0 on success.
 */
int test_sink_notify_condition_variable() {
	Sink<int> producer;
	Sink<int> consumer;
	producer.To(1) >> consumer;

	std::condition_variable cv;
	std::mutex m;

	consumer.Notify(cv);

	std::atomic<int> read_val{-1};
	std::thread consumer_thread([&]() {
		std::unique_lock<std::mutex> lock(m);
		cv.wait(lock, [&]() { return consumer.Ready(); });
		read_val.store(consumer.Pop(), std::memory_order_release);
	});

	std::this_thread::sleep_for(std::chrono::milliseconds(20));
	producer.Push(1, 555);
	consumer_thread.join();

	ASSERT_EQUAL("test_sink_notify_condition_variable read value", 555, read_val.load(std::memory_order_acquire));

	RETURN_TEST("test_sink_notify_condition_variable", 0);
}

/**
 * @brief Consumer Unnotify then destroy its CV; producer Eof must not signal it.
 * @return 0 on success.
 */
int test_sink_unnotify_before_cv_dies() {
	Sink<int> producer;
	auto consumer = std::make_unique<Sink<int>>();
	auto wake = std::make_unique<std::condition_variable>();

	producer.To(0) >> *consumer;
	consumer->Notify(*wake);
	producer.Push(0, 42);
	ASSERT_EQUAL("test_sink_unnotify_before_cv_dies queued",
		StormByte::Size{1}, consumer->Size(0));
	ASSERT_EQUAL("test_sink_unnotify_before_cv_dies pop", 42, consumer->Pop());

	consumer->Eof();
	consumer->Unnotify();
	wake.reset();
	consumer.reset();

	producer.Eof();
	ASSERT_TRUE("test_sink_unnotify_before_cv_dies producer eof", producer.EoF());

	RETURN_TEST("test_sink_unnotify_before_cv_dies", 0);
}

/* -------------------------------------------------------------------------- */
/* Query / keyed Pop                                                          */
/* -------------------------------------------------------------------------- */

/**
 * @brief Pop(int) reads only that key and returns default T when dry.
 * @return 0 on success.
 */
int test_sink_pop_key() {
	Sink<int> producer;
	Sink<int> consumer;

	producer.To(1) >> consumer;
	producer.To(2) >> consumer;

	producer.Push(1, 111);
	producer.Push(1, 112);
	producer.Push(2, 222);

	ASSERT_EQUAL("test_sink_pop_key first 1", 111, consumer.Pop(1));
	ASSERT_EQUAL("test_sink_pop_key size 1 left", StormByte::Size{1}, consumer.Size(1));
	ASSERT_EQUAL("test_sink_pop_key size 2 untouched", StormByte::Size{1}, consumer.Size(2));
	ASSERT_EQUAL("test_sink_pop_key key 2", 222, consumer.Pop(2));
	ASSERT_EQUAL("test_sink_pop_key dry 2", 0, consumer.Pop(2));
	ASSERT_EQUAL("test_sink_pop_key second 1", 112, consumer.Pop(1));
	ASSERT_TRUE("test_sink_pop_key empty 1", consumer.Empty(1));

	producer.Eof();
	Sink<int> closed;
	closed.Eof();
	ASSERT_EQUAL("test_sink_pop_key missing after eof", 0, closed.Pop(99));

	RETURN_TEST("test_sink_pop_key", 0);
}

/**
 * @brief Keys, Buckets, Contains, Empty/EoF/Ready per key on an empty Sink.
 * @return 0 on success.
 */
int test_sink_query_unwired() {
	Sink<int> sink;
	ASSERT_EQUAL("test_sink_query_unwired buckets", StormByte::Size{0}, sink.Buckets());
	ASSERT_TRUE("test_sink_query_unwired keys empty", sink.Keys().empty());
	ASSERT_FALSE("test_sink_query_unwired contains", sink.Contains(42));
	ASSERT_TRUE("test_sink_query_unwired empty missing", sink.Empty(42));
	ASSERT_FALSE("test_sink_query_unwired eof missing", sink.EoF(42));
	ASSERT_FALSE("test_sink_query_unwired ready missing", sink.Ready(42));

	RETURN_TEST("test_sink_query_unwired", 0);
}

/**
 * @brief Query after wiring two keys and pushing one item.
 * @return 0 on success.
 */
int test_sink_query_wired() {
	Sink<int> producer;
	Sink<int> consumer;

	producer.To(10) >> consumer;
	producer.To(20) >> consumer;

	ASSERT_EQUAL("test_sink_query_wired buckets", StormByte::Size{2}, consumer.Buckets());
	ASSERT_TRUE("test_sink_query_wired contains 10", consumer.Contains(10));
	ASSERT_TRUE("test_sink_query_wired contains 20", consumer.Contains(20));
	ASSERT_FALSE("test_sink_query_wired contains 30", consumer.Contains(30));
	ASSERT_TRUE("test_sink_query_wired empty 10", consumer.Empty(10));
	ASSERT_FALSE("test_sink_query_wired ready 10 empty", consumer.Ready(10));
	ASSERT_FALSE("test_sink_query_wired eof 10", consumer.EoF(10));

	const auto keys = consumer.Keys();
	ASSERT_EQUAL("test_sink_query_wired keys size", static_cast<std::size_t>(2), keys.size());
	ASSERT_EQUAL("test_sink_query_wired keys 0", 10, keys[0]);
	ASSERT_EQUAL("test_sink_query_wired keys 1", 20, keys[1]);

	producer.Push(10, 100);
	ASSERT_FALSE("test_sink_query_wired empty after push", consumer.Empty(10));
	ASSERT_TRUE("test_sink_query_wired ready 10", consumer.Ready(10));
	ASSERT_TRUE("test_sink_query_wired empty 20", consumer.Empty(20));
	ASSERT_FALSE("test_sink_query_wired ready 20", consumer.Ready(20));
	ASSERT_EQUAL("test_sink_query_wired size 10", StormByte::Size{1}, consumer.Size(10));

	producer.Eof();
	ASSERT_TRUE("test_sink_query_wired eof 10", consumer.EoF(10));
	ASSERT_TRUE("test_sink_query_wired eof 20", consumer.EoF(20));
	ASSERT_TRUE("test_sink_query_wired ready 10 after eof", consumer.Ready(10));
	ASSERT_TRUE("test_sink_query_wired ready 20 after eof", consumer.Ready(20));

	RETURN_TEST("test_sink_query_wired", 0);
}

/* -------------------------------------------------------------------------- */
/* Wiring (To / >> / <<)                                                      */
/* -------------------------------------------------------------------------- */

/**
 * @brief Extra writer: first Eof does not close; last writer does.
 * @return 0 on success.
 */
int test_sink_extra_writer_eof() {
	Sink<int> src;
	Sink<int> dest;
	Sink<int> extra;

	src.To(0) >> dest;
	dest.To(0) >> extra;

	src.Push(0, 1);
	extra.Push(0, 2);
	ASSERT_EQUAL("test_sink_extra_writer_eof queued", StormByte::Size{2}, dest.Size(0));

	src.Eof();
	ASSERT_FALSE("test_sink_extra_writer_eof dest open after first writer", dest.EoF());
	extra.Push(0, 3);
	ASSERT_EQUAL("test_sink_extra_writer_eof extra push after first eof", StormByte::Size{3}, dest.Size(0));

	ASSERT_EQUAL("test_sink_extra_writer_eof pop 1", 1, dest.Pop());
	ASSERT_EQUAL("test_sink_extra_writer_eof pop 2", 2, dest.Pop());
	ASSERT_EQUAL("test_sink_extra_writer_eof pop 3", 3, dest.Pop());

	extra.Eof();
	ASSERT_TRUE("test_sink_extra_writer_eof dest eof after last writer", dest.EoF());

	RETURN_TEST("test_sink_extra_writer_eof", 0);
}

/**
 * @brief Re-wire of the same producer must not leave a phantom writer.
 * @return 0 on success.
 */
int test_sink_rewire_same_writer_eof() {
	Sink<int> src;
	Sink<int> dest;

	src.To(0) >> dest;
	dest.To(0) >> src;

	std::condition_variable cv;
	std::mutex m;
	dest.Notify(cv);

	src.Push(0, 1);
	ASSERT_EQUAL("test_sink_rewire_same_writer_eof pop", 1, dest.Pop());

	std::atomic<bool> eof{false};
	std::thread waiter([&]() {
		std::unique_lock<std::mutex> lock(m);
		eof.store(cv.wait_for(lock, std::chrono::seconds(1), [&]() {
			return dest.EoF();
		}), std::memory_order_release);
	});

	src.Eof();
	waiter.join();

	ASSERT_TRUE("test_sink_rewire_same_writer_eof dest eof within 1s (would hang remuxer)",
		eof.load(std::memory_order_acquire));
	ASSERT_TRUE("test_sink_rewire_same_writer_eof dest eof", dest.EoF());

	RETURN_TEST("test_sink_rewire_same_writer_eof", 0);
}

/**
 * @brief consumer << producer and consumer << producer.To(key).
 * @return 0 on success.
 */
int test_sink_stream_operators() {
	Sink<int> producer;
	Sink<int> consumer;
	Sink<int> extra;

	producer.To(1) >> consumer;
	consumer << producer.To(2);
	producer.Push(1, 10);
	producer.Push(2, 20);
	ASSERT_EQUAL("test_sink_stream_operators size 1", StormByte::Size{1}, consumer.Size(1));
	ASSERT_EQUAL("test_sink_stream_operators size 2", StormByte::Size{1}, consumer.Size(2));

	producer.To(1) >> extra;
	extra.Push(1, 11);
	ASSERT_EQUAL("test_sink_stream_operators co-writer queued", StormByte::Size{2}, consumer.Size(1));

	producer.Eof();
	ASSERT_FALSE("test_sink_stream_operators open while extra writer", consumer.EoF());
	extra.Eof();

	std::vector<int> got;
	got.push_back(consumer.Pop());
	got.push_back(consumer.Pop());
	got.push_back(consumer.Pop());
	std::sort(got.begin(), got.end());
	ASSERT_EQUAL("test_sink_stream_operators pop a", 10, got[0]);
	ASSERT_EQUAL("test_sink_stream_operators pop b", 11, got[1]);
	ASSERT_EQUAL("test_sink_stream_operators pop c", 20, got[2]);
	ASSERT_TRUE("test_sink_stream_operators eof after last writer", consumer.EoF());

	Sink<int> left;
	Sink<int> dummy;
	left.To(3) >> dummy;
	left.Push(3, 30);
	Sink<int> all;
	all << left;
	ASSERT_EQUAL("test_sink_stream_operators bind-all size", StormByte::Size{1}, all.Size(3));
	ASSERT_EQUAL("test_sink_stream_operators bind-all pop", 30, all.Pop());

	RETURN_TEST("test_sink_stream_operators", 0);
}

/**
 * @brief Wire after Eof: new hoppers are born with EoF.
 * @return 0 on success.
 */
int test_sink_wire_after_eof() {
	Sink<int> producer;
	Sink<int> consumer;

	producer.Eof();
	ASSERT_TRUE("test_sink_wire_after_eof producer eof with zero buckets", producer.EoF());

	producer.To(50) >> consumer;
	ASSERT_TRUE("test_sink_wire_after_eof consumer born eof", consumer.EoF());
	ASSERT_TRUE("test_sink_wire_after_eof consumer ready on eof", consumer.Ready());

	RETURN_TEST("test_sink_wire_after_eof", 0);
}

/**
 * @brief producer >> consumer shares every existing hopper.
 * @return 0 on success.
 */
int test_sink_wire_all_hoppers() {
	Sink<int> producer;
	Sink<int> consumer;
	Sink<int> dummy;

	producer.To(1) >> dummy;
	producer.To(2) >> dummy;
	producer >> consumer;

	producer.Push(1, 100);
	producer.Push(2, 200);

	ASSERT_EQUAL("test_sink_wire_all_hoppers consumer size key 1", StormByte::Size{1}, consumer.Size(1));
	ASSERT_EQUAL("test_sink_wire_all_hoppers consumer size key 2", StormByte::Size{1}, consumer.Size(2));

	int val1 = consumer.Pop();
	int val2 = consumer.Pop();
	bool correct_set = (val1 == 100 && val2 == 200) || (val1 == 200 && val2 == 100);
	ASSERT_TRUE("test_sink_wire_all_hoppers popped values", correct_set);

	RETURN_TEST("test_sink_wire_all_hoppers", 0);
}

/**
 * @brief To(key) >> consumer and Push/Pop across keys.
 * @return 0 on success.
 */
int test_sink_wire_and_push_pop() {
	Sink<std::shared_ptr<std::string>> producer;
	Sink<std::shared_ptr<std::string>> consumer;

	producer.To(10) >> consumer;
	producer.To(20) >> consumer;

	producer.Capacity(10, 5);
	ASSERT_EQUAL("test_sink_wire_and_push_pop capacity key 10", StormByte::Size{5}, producer.Capacity(10));

	producer.Push(10, std::make_shared<std::string>("String-10"));
	producer.Push(20, std::make_shared<std::string>("String-20"));

	ASSERT_EQUAL("test_sink_wire_and_push_pop size key 10", StormByte::Size{1}, consumer.Size(10));
	ASSERT_EQUAL("test_sink_wire_and_push_pop size key 20", StormByte::Size{1}, consumer.Size(20));
	ASSERT_TRUE("test_sink_wire_and_push_pop consumer ready", consumer.Ready());

	auto item1 = consumer.Pop();
	auto item2 = consumer.Pop();

	ASSERT_TRUE("test_sink_wire_and_push_pop item1 valid", static_cast<bool>(item1));
	ASSERT_TRUE("test_sink_wire_and_push_pop item2 valid", static_cast<bool>(item2));

	producer.Eof();
	ASSERT_TRUE("test_sink_wire_and_push_pop consumer eof", consumer.EoF());

	RETURN_TEST("test_sink_wire_and_push_pop", 0);
}

/**
 * @brief Main entry point for Sink tests.
 * @return 0 on all tests passing, non-zero on failure.
 */
int main() {
	int failed = 0;

	// Concurrency
	failed += test_sink_concurrent_wire_and_eof();
	failed += test_sink_concurrent_wire_and_notify();

	// Construction
	failed += test_sink_default_constructor();

	// Item types / Drain / Pop
	failed += test_sink_drain_mode();
	failed += test_sink_eof_unblocks_waiters();
	failed += test_sink_non_nullable_smart_pointer();
	failed += test_sink_pop_custom_select();
	failed += test_sink_push_waiting_for_wire();

	// Notify / Unnotify
	failed += test_sink_notify_condition_variable();
	failed += test_sink_unnotify_before_cv_dies();

	// Query / keyed Pop
	failed += test_sink_pop_key();
	failed += test_sink_query_unwired();
	failed += test_sink_query_wired();

	// Wiring (To / >> / <<)
	failed += test_sink_extra_writer_eof();
	failed += test_sink_rewire_same_writer_eof();
	failed += test_sink_stream_operators();
	failed += test_sink_wire_after_eof();
	failed += test_sink_wire_all_hoppers();
	failed += test_sink_wire_and_push_pop();

	if (failed != 0) {
		std::cerr << failed << " test(s) failed." << std::endl;
		return 1;
	}

	std::cout << "Sink tests passed!" << std::endl;
	return 0;
}
