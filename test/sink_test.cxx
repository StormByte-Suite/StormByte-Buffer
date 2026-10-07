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
#include <StormByte/exception.hxx>
#include <StormByte/safe/atomic.hxx>
#include <StormByte/safe/condition_variable.hxx>
#include <StormByte/safe/mutex.hxx>
#include <StormByte/safe/pointers.hxx>
#include <StormByte/safe/string.hxx>
#include <StormByte/safe/unique_lock.hxx>
#include <StormByte/safe/vector.hxx>
#include <StormByte/test_handlers.h>

#include <algorithm>
#include <atomic>
#include <chrono>
#include <cstdint>
#include <future>
#include <iostream>
#include <memory>
#include <new>
#include <string>
#include <thread>
#include <vector>

using StormByte::Buffer::Sink;

/**
 * @brief Exact provider-declared pointer facade storing only an integer, with no external ownership.
 */
class NonNullableSmartPointer {
	public:
		/**
		 * @brief Construct a zero-valued facade.
		 */
		NonNullableSmartPointer() noexcept = default;

		/**
		 * @brief Construct a facade with an embedded value.
		 * @param value Initial embedded value.
		 */
		explicit NonNullableSmartPointer(int value) noexcept: m_value(value) {}

		/**
		 * @brief Copy the embedded integer.
		 * @param other Source.
		 */
		NonNullableSmartPointer(const NonNullableSmartPointer& other) noexcept = default;

		/**
		 * @brief Move the embedded integer.
		 * @param other Source.
		 */
		NonNullableSmartPointer(NonNullableSmartPointer&& other) noexcept = default;

		/**
		 * @brief Destroy the facade without releasing external resources.
		 */
		~NonNullableSmartPointer() noexcept = default;

		/**
		 * @brief Copy the embedded integer.
		 * @param other Source.
		 * @return This facade.
		 */
		NonNullableSmartPointer& operator=(const NonNullableSmartPointer& other) noexcept = default;

		/**
		 * @brief Move the embedded integer.
		 * @param other Source.
		 * @return This facade.
		 */
		NonNullableSmartPointer& operator=(NonNullableSmartPointer&& other) noexcept = default;

		/**
		 * @brief Get mutable embedded storage.
		 * @return Address of the embedded integer.
		 */
		int* get() noexcept { return &m_value; }

		/**
		 * @brief Get constant embedded storage.
		 * @return Address of the embedded integer.
		 */
		const int* get() const noexcept { return &m_value; }

		/**
		 * @brief Dereference mutable embedded storage.
		 * @return Embedded integer.
		 */
		int& operator*() noexcept { return m_value; }

		/**
		 * @brief Dereference constant embedded storage.
		 * @return Embedded integer.
		 */
		const int& operator*() const noexcept { return m_value; }

		/**
		 * @brief Get mutable arrow access.
		 * @return Address of the embedded integer.
		 */
		int* operator->() noexcept { return &m_value; }

		/**
		 * @brief Get constant arrow access.
		 * @return Address of the embedded integer.
		 */
		const int* operator->() const noexcept { return &m_value; }

	private:
		int m_value = 0;	///< Integer owned directly by the facade.
};

STORMBYTE_DECLARE_MAYBE_SAFE(NonNullableSmartPointer);

/**
 * @brief Provider-declared element with unsupported extended alignment.
 */
struct alignas(alignof(std::max_align_t) * 2) OveralignedValue {
	int value = 0;	///< Embedded integer with no allocator or external lifetime.
};

STORMBYTE_DECLARE_MAYBE_SAFE(OveralignedValue);

/**
 * @brief Provider-declared element with throwing default construction.
 */
struct ThrowingDefaultValue {
	/**
	 * @brief Provide a potentially throwing default signature for admission testing.
	 */
	ThrowingDefaultValue() noexcept(false) {}
};

STORMBYTE_DECLARE_MAYBE_SAFE(ThrowingDefaultValue);

/**
 * @brief Test whether Sink admits an element without instantiating its storage.
 * @tparam Value Candidate element.
 */
template<typename Value>
concept SinkAdmits = requires { typename Sink<Value>; };

/**
 * @brief Undeclared movable payload. It does not acquire a SafeValue contract automatically.
 */
struct UndeclaredValue {
	int value = 0;	///< Embedded integer.
};

/**
 * @brief A derived queue does not inherit an exact-type MaybeSafe declaration.
 */
struct DerivedSink: Sink<int> {};

static_assert(StormByte::Type::Movable<UndeclaredValue>);
static_assert(!StormByte::Type::SafeValue<UndeclaredValue>);
static_assert(!SinkAdmits<UndeclaredValue>);
static_assert(StormByte::Type::MaybeSafe<Sink<int>>);
static_assert(!StormByte::Type::MaybeSafe<DerivedSink>);
static_assert(SinkAdmits<int>);
static_assert(SinkAdmits<StormByte::Safe::String>);
static_assert(SinkAdmits<StormByte::Safe::Shared<int>>);
static_assert(SinkAdmits<NonNullableSmartPointer>);
static_assert(StormByte::Type::SafeValue<OveralignedValue>);
static_assert(!SinkAdmits<OveralignedValue>);
static_assert(StormByte::Type::SafeValue<ThrowingDefaultValue>);
static_assert(!SinkAdmits<ThrowingDefaultValue>);
static_assert(!SinkAdmits<const int>);
static_assert(!SinkAdmits<volatile int>);
static_assert(!SinkAdmits<int&>);
static_assert(!SinkAdmits<int*>);
static_assert(!SinkAdmits<std::string>);
static_assert(!SinkAdmits<std::shared_ptr<int>>);
static_assert(!SinkAdmits<std::unique_ptr<int>>);
static_assert(!SinkAdmits<StormByte::Safe::Unique<int>>);
static_assert(StormByte::Type::SmartPointer<NonNullableSmartPointer>);
static_assert(!StormByte::Type::NullablePointer<NonNullableSmartPointer>);

// -------------------
// Concurrency
// -------------------

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
	ASSERT_TRUE(consumer.EoF());
	const int last_key = max_key.load(std::memory_order_acquire);
	for (int k = 100; k <= last_key + 10; ++k) {
		producer.Push(k, 9999);
		ASSERT_EQUAL(StormByte::Size{0}, consumer.Size(k));
	}
	RETURN_TEST(0);
}

int test_sink_concurrent_wire_and_notify() {
	Sink<int> producer;
	Sink<int> consumer;
	StormByte::Safe::ConditionVariable cv;
	StormByte::Safe::Mutex m;
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
		StormByte::Safe::UniqueLock lock(m);
		const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(1);
		bool ok = cv.wait_until(lock, deadline, [&]() {
			int val = consumer.Pop();
			if (val != 0) {
				received_val.store(val, std::memory_order_release);
				return true;
			}
			return false;
		});
		woken.store(ok && std::chrono::steady_clock::now() < deadline, std::memory_order_release);
	});
	std::this_thread::sleep_for(std::chrono::milliseconds(20));
	{
		StormByte::Safe::UniqueLock lock(m);
		producer.Push(200, 7777);
	}
	wait_thread.join();
	ASSERT_TRUE(woken.load(std::memory_order_acquire));
	ASSERT_EQUAL(7777, received_val.load(std::memory_order_acquire));
	producer.Eof();
	RETURN_TEST(0);
}

// -------------------
// Construct
// -------------------

int test_sink_default_constructor() {
	Sink<int> sink;
	ASSERT_FALSE(sink.EoF());
	ASSERT_FALSE(sink.Ready());
	ASSERT_FALSE(sink.Draining());
	ASSERT_EQUAL(StormByte::Size{0}, sink.Capacity(42));
	ASSERT_EQUAL(StormByte::Size{0}, sink.Size(42));
	ASSERT_FALSE(sink.Full(42));
	RETURN_TEST(0);
}

// -------------------
// Drain
// -------------------

int test_sink_drain_mode() {
	Sink<int> producer;
	producer.Drain();
	ASSERT_TRUE(producer.Draining());
	producer.Push(100, 9999);
	ASSERT_EQUAL(StormByte::Size{0}, producer.Size(100));
	Sink<int> consumer;
	producer.To(100) >> consumer;
	producer.Push(100, 8888);
	ASSERT_EQUAL(StormByte::Size{1}, consumer.Size(100));
	ASSERT_EQUAL(8888, consumer.Pop());
	RETURN_TEST(0);
}

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
	ASSERT_FALSE(push_done.load(std::memory_order_acquire));
	ASSERT_FALSE(pop_done.load(std::memory_order_acquire));
	producer.Eof();
	consumer.Eof();
	push_thread.join();
	pop_thread.join();
	ASSERT_TRUE(push_done.load(std::memory_order_acquire));
	ASSERT_TRUE(pop_done.load(std::memory_order_acquire));
	RETURN_TEST(0);
}

int test_sink_non_nullable_smart_pointer() {
	Sink<NonNullableSmartPointer> producer;
	Sink<NonNullableSmartPointer> consumer;
	producer.To(1) >> consumer;
	producer.Push(1, NonNullableSmartPointer(789));
	ASSERT_EQUAL(StormByte::Size{1}, consumer.Size(1));
	auto popped = consumer.Pop();
	ASSERT_EQUAL(789, *popped);
	ASSERT_FALSE(consumer.Ready());
	RETURN_TEST(0);
}

int test_sink_overaligned_payload() {
	static_assert(StormByte::Type::SafeValue<OveralignedValue>);
	static_assert(!SinkAdmits<OveralignedValue>);
	static_assert(alignof(OveralignedValue) > alignof(std::max_align_t));
	RETURN_TEST(0);
}

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
	ASSERT_EQUAL(222, val);
	int val_rem = consumer.Pop();
	ASSERT_EQUAL(111, val_rem);
	RETURN_TEST(0);
}

int test_sink_pop_selector_exception_preserves_items() {
	Sink<int> producer;
	Sink<int> consumer;
	producer.To(1) >> consumer;
	producer.To(2) >> consumer;
	producer.Push(1, 111);
	producer.Push(2, 222);
	const auto fail = [](StormByte::Size) -> StormByte::Size {
		throw StormByte::Exception("selector failure");
	};
	ASSERT_EQUAL(0, consumer.Pop(fail));
	ASSERT_EQUAL(StormByte::Size{1}, consumer.Size(1));
	ASSERT_EQUAL(StormByte::Size{1}, consumer.Size(2));
	ASSERT_EQUAL(111, consumer.Pop());
	ASSERT_EQUAL(222, consumer.Pop());
	RETURN_TEST(0);
}

int test_sink_push_waiting_for_wire() {
	Sink<int> producer;
	Sink<int> consumer;
	std::atomic<bool> push_done{false};
	std::thread push_thread([&]() {
		producer.Push(7, 777);
		push_done.store(true, std::memory_order_release);
	});
	std::this_thread::sleep_for(std::chrono::milliseconds(30));
	ASSERT_FALSE(push_done.load(std::memory_order_acquire));
	producer.To(7) >> consumer;
	push_thread.join();
	ASSERT_TRUE(push_done.load(std::memory_order_acquire));
	ASSERT_EQUAL(777, consumer.Pop());
	RETURN_TEST(0);
}

int test_sink_selector_borrowed_state() {
	Sink<int> producer;
	Sink<int> consumer;
	producer.To(1) >> consumer;
	producer.To(2) >> consumer;
	producer.Push(1, 11);
	producer.Push(2, 22);
	producer.Push(1, 33);
	producer.Push(2, 44);
	int calls = 0;
	auto selector = [state = std::make_unique<std::size_t>(0), &calls](StormByte::Size count) mutable {
		++calls;
		return StormByte::Size{(*state)++ % static_cast<std::size_t>(count)};
	};
	ASSERT_EQUAL(11, consumer.Pop(selector));
	ASSERT_EQUAL(22, consumer.Pop(selector));
	ASSERT_EQUAL(2, calls);
	ASSERT_EQUAL(33, consumer.Pop([&calls](StormByte::Size) {
		++calls;
		return StormByte::Size{20};
	}));
	ASSERT_EQUAL(44, consumer.Pop());
	ASSERT_EQUAL(3, calls);
	RETURN_TEST(0);
}

int test_sink_safe_selector_status() {
	Sink<int> producer;
	Sink<int> consumer;
	producer.To(1) >> consumer;
	producer.To(2) >> consumer;
	producer.Push(1, 11);
	producer.Push(2, 22);
	auto* status = ::new (StormByte::Safe::Heap::Allocate(sizeof(StormByte::Safe::Status)))
		StormByte::Safe::Status(StormByte::Safe::Status::Failure);
	const auto invoke = [](void* context, StormByte::Size* output, StormByte::Size) {
		*output = StormByte::Size{1};
		return *static_cast<StormByte::Safe::Status*>(context);
	};
	Sink<int>::Select selector(status, invoke,
		[](const void* context) noexcept -> void* {
			try {
				return ::new (StormByte::Safe::Heap::Allocate(sizeof(StormByte::Safe::Status)))
					StormByte::Safe::Status(*static_cast<const StormByte::Safe::Status*>(context));
			}
			catch (...) {
				return nullptr;
			}
		},
		[](void* context) noexcept {
			StormByte::Safe::Heap::ObjectDeleter{}(static_cast<StormByte::Safe::Status*>(context));
		});
	StormByte::Size output{7};
	ASSERT_EQUAL(StormByte::Safe::Status::Failure, selector.Call(output, StormByte::Size{2}));
	ASSERT_EQUAL(StormByte::Size{7}, output);
	ASSERT_EQUAL(0, consumer.Pop(selector));
	ASSERT_EQUAL(StormByte::Size{1}, consumer.Size(1));
	ASSERT_EQUAL(StormByte::Size{1}, consumer.Size(2));
	Sink<int>::Select moved(std::move(selector));
	ASSERT_EQUAL(StormByte::Safe::Status::Missing, selector.Call(output, StormByte::Size{2}));
	ASSERT_EQUAL(StormByte::Size{7}, output);
	ASSERT_EQUAL(0, consumer.Pop(selector));
	*status = StormByte::Safe::Status::Success;
	ASSERT_EQUAL(StormByte::Safe::Status::Success, moved.Call(output, StormByte::Size{2}));
	ASSERT_EQUAL(StormByte::Size{1}, output);
	ASSERT_EQUAL(22, consumer.Pop(moved));
	ASSERT_EQUAL(11, consumer.Pop());
	RETURN_TEST(0);
}

int test_sink_safe_selector_provider_release() {
	Sink<int> producer;
	Sink<int> consumer;
	producer.To(1) >> consumer;
	producer.Push(1, 42);
	int releases = 0;
	int clones = 0;
	/**
	 * @brief Provider-owned selector state with borrowed lifetime counters.
	 */
	struct Context {
		std::size_t next;	///< Independent selection counter owned by each callback.
		int* releases;		///< Borrowed release counter that outlives every callback.
		int* clones;		///< Borrowed clone counter that outlives every callback.
	};
	{
		auto* context = ::new (StormByte::Safe::Heap::Allocate(sizeof(Context))) Context{0, &releases, &clones};
		Sink<int>::Select selector(context,
			[](void* context, StormByte::Size* output, StormByte::Size count) {
				auto* state = static_cast<Context*>(context);
				*output = StormByte::Size{state->next++ % static_cast<std::size_t>(count)};
				return StormByte::Safe::Status::Success;
			},
			[](const void* context) noexcept -> void* {
				try {
					auto* copy = ::new (StormByte::Safe::Heap::Allocate(sizeof(Context)))
						Context(*static_cast<const Context*>(context));
					++*copy->clones;
					return copy;
				}
				catch (...) {
					return nullptr;
				}
			},
			[](void* context) noexcept {
				auto* state = static_cast<Context*>(context);
				++*state->releases;
				StormByte::Safe::Heap::ObjectDeleter{}(state);
			});
		ASSERT_EQUAL(42, consumer.Pop(selector));
		ASSERT_EQUAL(0, releases);
		ASSERT_EQUAL(0, clones);
		{
			Sink<int>::Select copy(selector);
			ASSERT_EQUAL(1, clones);
			StormByte::Size output{9};
			ASSERT_EQUAL(StormByte::Safe::Status::Success, copy.Call(output, StormByte::Size{3}));
			ASSERT_EQUAL(StormByte::Size{1}, output);
			ASSERT_EQUAL(StormByte::Safe::Status::Success, copy.Call(output, StormByte::Size{3}));
			ASSERT_EQUAL(StormByte::Size{2}, output);
			ASSERT_EQUAL(StormByte::Safe::Status::Success, selector.Call(output, StormByte::Size{3}));
			ASSERT_EQUAL(StormByte::Size{1}, output);
			copy = selector;
			ASSERT_EQUAL(2, clones);
			ASSERT_EQUAL(1, releases);
			ASSERT_EQUAL(StormByte::Safe::Status::Success, copy.Call(output, StormByte::Size{3}));
			ASSERT_EQUAL(StormByte::Size{2}, output);
			ASSERT_EQUAL(StormByte::Safe::Status::Success, copy.Call(output, StormByte::Size{3}));
			ASSERT_EQUAL(StormByte::Size{0}, output);
			ASSERT_EQUAL(StormByte::Safe::Status::Success, selector.Call(output, StormByte::Size{3}));
			ASSERT_EQUAL(StormByte::Size{2}, output);
		}
		ASSERT_EQUAL(2, releases);
	}
	ASSERT_EQUAL(3, releases);
	RETURN_TEST(0);
}

int test_sink_single_bucket_selector_failure() {
	Sink<int> producer;
	Sink<int> consumer;
	producer.To(1) >> consumer;
	producer.Push(1, 42);
	int calls = 0;
	const auto safe_failure = [&calls](StormByte::Size count) -> StormByte::Size {
		++calls;
		if (count == StormByte::Size{1})
			throw StormByte::Exception("selector failure");
		return StormByte::Size{0};
	};
	ASSERT_EQUAL(0, consumer.Pop(safe_failure));
	const auto foreign_failure = [](StormByte::Size) -> StormByte::Size {
		throw 7;
	};
	ASSERT_EQUAL(0, consumer.Pop(foreign_failure));
	ASSERT_EQUAL(1, calls);
	ASSERT_EQUAL(StormByte::Size{1}, consumer.Size(1));
	ASSERT_EQUAL(42, consumer.Pop());
	RETURN_TEST(0);
}

// -------------------
// Notify
// -------------------

int test_sink_closed_binding_writer_release() {
	Sink<int> producer;
	Sink<int> consumer;
	Sink<int> closed;
	producer.To(1) >> consumer;
	closed.Eof();
	closed.To(2) >> producer;
	producer.Eof();
	producer.Eof();
	ASSERT_TRUE(consumer.EoF());
	RETURN_TEST(0);
}

int test_sink_closed_cowriter_push() {
	Sink<int> producer;
	Sink<int> consumer;
	Sink<int> coworker;
	producer.To(1) >> consumer;
	producer.To(1) >> coworker;
	producer.Eof();
	producer.Push(1, 11);
	ASSERT_TRUE(consumer.Empty(1));
	ASSERT_FALSE(consumer.EoF(1));
	coworker.Push(1, 22);
	coworker.Eof();
	ASSERT_EQUAL(22, consumer.Pop(1));
	ASSERT_TRUE(consumer.EoF());
	RETURN_TEST(0);
}

int test_sink_notify_condition_variable() {
	Sink<int> producer;
	Sink<int> consumer;
	producer.To(1) >> consumer;
	StormByte::Safe::ConditionVariable cv;
	StormByte::Safe::Mutex m;
	consumer.Notify(cv);
	std::atomic<int> read_val{-1};
	std::thread consumer_thread([&]() {
		StormByte::Safe::UniqueLock lock(m);
		if (cv.wait_for(lock, std::chrono::seconds(1), [&]() { return consumer.Ready(); }))
			read_val.store(consumer.Pop(), std::memory_order_release);
	});
	std::this_thread::sleep_for(std::chrono::milliseconds(20));
	{
		StormByte::Safe::UniqueLock lock(m);
		producer.Push(1, 555);
	}
	consumer_thread.join();
	ASSERT_EQUAL(555, read_val.load(std::memory_order_acquire));
	RETURN_TEST(0);
}

int test_sink_observer_concurrent_unnotify() {
	Sink<int> producer;
	Sink<int> consumer;
	{
		StormByte::Safe::ConditionVariable wake;
		consumer.Notify(wake);
		std::thread wiring([&] {
			for (int key = 0; key < 100; ++key)
				producer.To(key) >> consumer;
		});
		std::thread closing([&] { consumer.Eof(); });
		consumer.Unnotify();
		wiring.join();
		closing.join();
	}
	producer.Eof();
	ASSERT_TRUE(consumer.EoF());
	RETURN_TEST(0);
}

int test_sink_observer_identity() {
	Sink<int> producer;
	Sink<int> newer;
	StormByte::Safe::ConditionVariable wake;
	StormByte::Safe::Mutex mutex;
	{
		StormByte::Safe::ConditionVariable old_wake;
		Sink<int> older;
		producer.To(1) >> older;
		older >> newer;
		older.Notify(old_wake);
		newer.Notify(wake);
		older.Unnotify();
		older.Unnotify();
		older.Notify(wake);
		newer.Notify(wake);
	}
	std::atomic<bool> waiting{false};
	bool notified = false;
	std::thread waiter([&] {
		StormByte::Safe::UniqueLock lock(mutex);
		waiting.store(true, std::memory_order_release);
		const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(1);
		notified = wake.wait_until(lock, deadline, [&] { return newer.Ready(); })
			&& std::chrono::steady_clock::now() < deadline;
	});
	while (!waiting.load(std::memory_order_acquire))
		std::this_thread::yield();
	{
		StormByte::Safe::UniqueLock lock(mutex);
		producer.Push(1, 42);
	}
	waiter.join();
	ASSERT_TRUE(notified);
	ASSERT_EQUAL(42, newer.Pop(1));
	newer.Unnotify();
	RETURN_TEST(0);
}

int test_sink_observer_rebind_and_destruction() {
	Sink<int> first;
	Sink<int> second;
	{
		StormByte::Safe::ConditionVariable wake;
		Sink<int> consumer;
		first.To(1) >> consumer;
		consumer.Notify(wake);
		second.To(1) >> consumer;
		second.To(2) >> consumer;
	}
	first.Push(1, 11);
	second.Push(1, 22);
	second.Push(2, 33);
	first.Eof();
	second.Eof();
	ASSERT_EQUAL(StormByte::Size{1}, first.Size(1));
	ASSERT_EQUAL(StormByte::Size{1}, second.Size(1));
	ASSERT_EQUAL(StormByte::Size{1}, second.Size(2));
	RETURN_TEST(0);
}

int test_sink_rebound_writer_eof() {
	Sink<int> original;
	Sink<int> consumer;
	Sink<int> replacement;
	original.To(1) >> consumer;
	original.Push(1, 42);
	replacement.To(1) >> original;
	original.Eof();
	ASSERT_EQUAL(42, consumer.Pop(1));
	ASSERT_TRUE(consumer.EoF());
	replacement.Eof();
	RETURN_TEST(0);
}

int test_sink_stored_concurrent_unnotify() {
	for (int iteration = 0; iteration < 50; ++iteration) {
		Sink<int> producer;
		Sink<int> consumer;
		producer.To(0) >> consumer;
		std::thread publishing;
		std::thread wiring;
		{
			StormByte::Safe::ConditionVariable wake;
			StormByte::Safe::Atomic<std::size_t> generation{0};
			consumer.Notify(wake, generation);
			publishing = std::thread([&] {
				for (int item = 1; item <= 100; ++item)
					producer.Push(0, item);
				producer.Eof();
			});
			wiring = std::thread([&] {
				for (int key = 1; key <= 10; ++key)
					producer.To(key) >> consumer;
			});
			consumer.Unnotify();
		}
		publishing.join();
		wiring.join();
		ASSERT_TRUE(consumer.EoF(0));
		ASSERT_EQUAL(StormByte::Size{100}, consumer.Size(0));
	}
	RETURN_TEST(0);
}

int test_sink_stored_handoff() {
	for (int event = 0; event < 7; ++event) {
		Sink<int> producer;
		Sink<int> consumer;
		Sink<int> staging;
		StormByte::Safe::ConditionVariable wake;
		StormByte::Safe::Atomic<std::size_t> generation{0};
		std::atomic<bool> stopped{false};
		consumer.Notify(wake, generation);
		if (event < 2)
			producer.To(0) >> consumer;
		if (event >= 4) {
			producer.To(0) >> staging;
			if (event == 4)
				producer.Push(0, 42);
			else
				producer.Eof();
		}
		std::promise<void> checked;
		std::promise<void> published;
		auto after_check = checked.get_future();
		auto after_publish = published.get_future();
		auto waiter = std::async(std::launch::async, [&] {
			bool first = true;
			for (;;) {
				const auto before = generation.load();
				const bool ready = consumer.Ready() || stopped.load(std::memory_order_acquire);
				if (first) {
					first = false;
					checked.set_value();
					after_publish.wait();
				}
				if (ready)
					break;
				generation.wait(before);
			}
		});
		after_check.wait();
		if (event == 0)
			producer.Push(0, 42);
		else if (event == 1)
			producer.Eof();
		else if (event == 2)
			consumer.Eof();
		else if (event == 3) {
			stopped.store(true, std::memory_order_release);
			generation.fetch_add(std::size_t{1});
			generation.notify_all();
		}
		else if (event == 6)
			producer.To(0) >> consumer;
		else
			producer >> consumer;
		published.set_value();
		const bool woke = waiter.wait_for(std::chrono::seconds(1)) == std::future_status::ready;
		if (!woke) {
			stopped.store(true, std::memory_order_release);
			generation.fetch_add(std::size_t{1});
			generation.notify_all();
		}
		waiter.get();
		consumer.Unnotify();
		ASSERT_TRUE(woke);
		if (event == 0 || event == 4)
			ASSERT_EQUAL(42, consumer.Pop(0));
		else if (event != 3)
			ASSERT_TRUE(consumer.EoF());
	}
	RETURN_TEST(0);
}

int test_sink_stored_observer_identity() {
	Sink<int> first;
	Sink<int> second;
	Sink<int> consumer;
	StormByte::Safe::ConditionVariable wake;
	StormByte::Safe::Atomic<std::size_t> generation{0};
	first.To(0) >> consumer;
	{
		Sink<int> older;
		StormByte::Safe::ConditionVariable old_wake;
		StormByte::Safe::Atomic<std::size_t> old_generation{0};
		first >> older;
		older.Notify(old_wake, old_generation);
		consumer.Notify(wake, generation);
		older.Unnotify();
		older.Unnotify();
		const auto before = generation.load();
		first.Push(0, 11);
		ASSERT_EQUAL(before + 1, generation.load());
		ASSERT_EQUAL(std::size_t{0}, old_generation.load());
		ASSERT_EQUAL(11, consumer.Pop(0));
	}
	second.To(0) >> consumer;
	const auto rebound = generation.load();
	first.Push(0, 22);
	ASSERT_EQUAL(rebound, generation.load());
	second.Push(0, 33);
	ASSERT_EQUAL(rebound + 1, generation.load());
	ASSERT_EQUAL(33, consumer.Pop(0));
	consumer.Notify(wake);
	const auto legacy = generation.load();
	second.Push(0, 44);
	second.Eof();
	ASSERT_EQUAL(legacy, generation.load());
	consumer.Unnotify();
	RETURN_TEST(0);
}

int test_sink_stored_wiring() {
	Sink<int> first;
	Sink<int> second;
	Sink<int> consumer;
	Sink<int> downstream;
	StormByte::Safe::ConditionVariable wake;
	StormByte::Safe::Atomic<std::size_t> generation{0};
	first.Notify(wake, generation);
	first.To(0) >> consumer;
	const auto ensured = generation.load();
	first.Push(0, 11);
	ASSERT_EQUAL(ensured + 1, generation.load());
	ASSERT_EQUAL(11, consumer.Pop(0));
	consumer.Notify(wake, generation);
	first.To(1) >> consumer;
	consumer.Capacity(1, 512);
	const auto created = generation.load();
	first.Push(1, 22);
	ASSERT_EQUAL(created + 1, generation.load());
	ASSERT_EQUAL(22, consumer.Pop(1));
	downstream.Notify(wake, generation);
	first >> downstream;
	const auto bound = generation.load();
	first.Push(0, 33);
	ASSERT_EQUAL(bound + 1, generation.load());
	ASSERT_EQUAL(33, downstream.Pop(0));
	first.To(0) >> second;
	second >> downstream;
	const auto shared = generation.load();
	second.Push(0, 44);
	ASSERT_EQUAL(shared + 1, generation.load());
	ASSERT_EQUAL(44, downstream.Pop(0));
	first.Eof();
	ASSERT_FALSE(downstream.EoF(0));
	const auto before_final = generation.load();
	second.Eof();
	ASSERT_TRUE(generation.load() > before_final);
	ASSERT_TRUE(downstream.EoF());
	ASSERT_EQUAL(StormByte::Size{512}, downstream.Capacity(1));
	first.Unnotify();
	consumer.Unnotify();
	downstream.Unnotify();
	RETURN_TEST(0);
}

int test_sink_unnotify_before_cv_dies() {
	Sink<int> producer;
	auto consumer = std::make_unique<Sink<int>>();
	auto wake = std::make_unique<StormByte::Safe::ConditionVariable>();
	producer.To(0) >> *consumer;
	consumer->Notify(*wake);
	producer.Push(0, 42);
	ASSERT_EQUAL(StormByte::Size{1}, consumer->Size(0));
	ASSERT_EQUAL(42, consumer->Pop());
	consumer->Eof();
	consumer->Unnotify();
	wake.reset();
	consumer.reset();
	producer.Eof();
	ASSERT_TRUE(producer.EoF());
	RETURN_TEST(0);
}

int test_sink_writer_destruction() {
	Sink<int> consumer;
	{
		StormByte::Safe::ConditionVariable wake;
		Sink<int> producer;
		producer.To(1) >> consumer;
		producer.Notify(wake);
		producer.Push(1, 42);
	}
	ASSERT_EQUAL(42, consumer.Pop(1));
	ASSERT_TRUE(consumer.EoF());
	RETURN_TEST(0);
}

// -------------------
// Query
// -------------------

int test_sink_pop_key() {
	Sink<int> producer;
	Sink<int> consumer;
	producer.To(1) >> consumer;
	producer.To(2) >> consumer;
	producer.Push(1, 111);
	producer.Push(1, 112);
	producer.Push(2, 222);
	ASSERT_EQUAL(111, consumer.Pop(1));
	ASSERT_EQUAL(StormByte::Size{1}, consumer.Size(1));
	ASSERT_EQUAL(StormByte::Size{1}, consumer.Size(2));
	ASSERT_EQUAL(222, consumer.Pop(2));
	ASSERT_EQUAL(0, consumer.Pop(2));
	ASSERT_EQUAL(112, consumer.Pop(1));
	ASSERT_TRUE(consumer.Empty(1));
	producer.Eof();
	Sink<int> closed;
	closed.Eof();
	ASSERT_EQUAL(0, closed.Pop(99));
	RETURN_TEST(0);
}

int test_sink_query_snapshot() {
	StormByte::Safe::Vector<int> keys;
	{
		Sink<int> producer;
		Sink<int> consumer;
		producer.To(20) >> consumer;
		producer.To(-10) >> consumer;
		producer.To(0) >> consumer;
		keys = consumer.Keys();
		producer.To(5) >> consumer;
		ASSERT_EQUAL(StormByte::Size{4}, consumer.Buckets());
		ASSERT_EQUAL(std::size_t{3}, keys.size());
		producer.Eof();
	}
	const StormByte::Safe::Vector<int> copy = keys;
	keys[0] = -20;
	const StormByte::Safe::Vector<int> moved = std::move(keys);
	ASSERT_EQUAL(-10, copy[0]);
	ASSERT_EQUAL(0, copy[1]);
	ASSERT_EQUAL(20, copy[2]);
	ASSERT_EQUAL(std::size_t{3}, moved.size());
	ASSERT_EQUAL(-20, moved[0]);
	RETURN_TEST(0);
}

int test_sink_query_unwired() {
	Sink<int> sink;
	ASSERT_EQUAL(StormByte::Size{0}, sink.Buckets());
	ASSERT_TRUE(sink.Keys().empty());
	ASSERT_FALSE(sink.Contains(42));
	ASSERT_TRUE(sink.Empty(42));
	ASSERT_FALSE(sink.EoF(42));
	ASSERT_FALSE(sink.Ready(42));
	RETURN_TEST(0);
}

int test_sink_query_wired() {
	Sink<int> producer;
	Sink<int> consumer;
	producer.To(10) >> consumer;
	producer.To(20) >> consumer;
	ASSERT_EQUAL(StormByte::Size{2}, consumer.Buckets());
	ASSERT_TRUE(consumer.Contains(10));
	ASSERT_TRUE(consumer.Contains(20));
	ASSERT_FALSE(consumer.Contains(30));
	ASSERT_TRUE(consumer.Empty(10));
	ASSERT_FALSE(consumer.Ready(10));
	ASSERT_FALSE(consumer.EoF(10));
	const StormByte::Safe::Vector<int> keys = consumer.Keys();
	ASSERT_EQUAL(static_cast<std::size_t>(2), keys.size());
	ASSERT_EQUAL(10, keys[0]);
	ASSERT_EQUAL(20, keys[1]);
	producer.Push(10, 100);
	ASSERT_FALSE(consumer.Empty(10));
	ASSERT_TRUE(consumer.Ready(10));
	ASSERT_TRUE(consumer.Empty(20));
	ASSERT_FALSE(consumer.Ready(20));
	ASSERT_EQUAL(StormByte::Size{1}, consumer.Size(10));
	producer.Eof();
	ASSERT_TRUE(consumer.EoF(10));
	ASSERT_TRUE(consumer.EoF(20));
	ASSERT_TRUE(consumer.Ready(10));
	ASSERT_TRUE(consumer.Ready(20));
	RETURN_TEST(0);
}

// -------------------
// Wire
// -------------------

int test_sink_extra_writer_eof() {
	Sink<int> src;
	Sink<int> dest;
	Sink<int> extra;
	src.To(0) >> dest;
	dest.To(0) >> extra;
	src.Push(0, 1);
	extra.Push(0, 2);
	ASSERT_EQUAL(StormByte::Size{2}, dest.Size(0));
	src.Eof();
	ASSERT_FALSE(dest.EoF());
	extra.Push(0, 3);
	ASSERT_EQUAL(StormByte::Size{3}, dest.Size(0));
	ASSERT_EQUAL(1, dest.Pop());
	ASSERT_EQUAL(2, dest.Pop());
	ASSERT_EQUAL(3, dest.Pop());
	extra.Eof();
	ASSERT_TRUE(dest.EoF());
	RETURN_TEST(0);
}

int test_sink_rewire_same_writer_eof() {
	Sink<int> src;
	Sink<int> dest;
	src.To(0) >> dest;
	dest.To(0) >> src;
	StormByte::Safe::ConditionVariable cv;
	StormByte::Safe::Mutex m;
	dest.Notify(cv);
	src.Push(0, 1);
	ASSERT_EQUAL(1, dest.Pop());
	std::atomic<bool> eof{false};
	std::thread waiter([&]() {
		StormByte::Safe::UniqueLock lock(m);
		eof.store(cv.wait_for(lock, std::chrono::seconds(1), [&]() {
			return dest.EoF();
		}), std::memory_order_release);
	});
	{
		StormByte::Safe::UniqueLock lock(m);
		src.Eof();
	}
	waiter.join();
	ASSERT_TRUE(eof.load(std::memory_order_acquire));
	ASSERT_TRUE(dest.EoF());
	RETURN_TEST(0);
}

int test_sink_shared_hopper_lifetime() {
	Sink<int> consumer;
	{
		Sink<int> producer;
		producer.To(-1) >> consumer;
		producer.Push(-1, 42);
		producer.Push(-1, 43);
		producer.Eof();
	}
	ASSERT_EQUAL(42, consumer.Pop(-1));
	ASSERT_EQUAL(43, consumer.Pop(-1));
	ASSERT_TRUE(consumer.Empty(-1));
	ASSERT_TRUE(consumer.EoF());
	RETURN_TEST(0);
}

int test_sink_stream_operators() {
	Sink<int> producer;
	Sink<int> consumer;
	Sink<int> extra;
	producer.To(1) >> consumer;
	consumer << producer.To(2);
	producer.Push(1, 10);
	producer.Push(2, 20);
	ASSERT_EQUAL(StormByte::Size{1}, consumer.Size(1));
	ASSERT_EQUAL(StormByte::Size{1}, consumer.Size(2));
	producer.To(1) >> extra;
	extra.Push(1, 11);
	ASSERT_EQUAL(StormByte::Size{2}, consumer.Size(1));
	producer.Eof();
	ASSERT_FALSE(consumer.EoF());
	extra.Eof();
	std::vector<int> got;
	got.push_back(consumer.Pop());
	got.push_back(consumer.Pop());
	got.push_back(consumer.Pop());
	std::sort(got.begin(), got.end());
	ASSERT_EQUAL(10, got[0]);
	ASSERT_EQUAL(11, got[1]);
	ASSERT_EQUAL(20, got[2]);
	ASSERT_TRUE(consumer.EoF());
	Sink<int> left;
	Sink<int> dummy;
	left.To(3) >> dummy;
	left.Push(3, 30);
	Sink<int> all;
	all << left;
	ASSERT_EQUAL(StormByte::Size{1}, all.Size(3));
	ASSERT_EQUAL(30, all.Pop());
	RETURN_TEST(0);
}

int test_sink_wire_after_eof() {
	Sink<int> producer;
	Sink<int> consumer;
	producer.Eof();
	ASSERT_TRUE(producer.EoF());
	producer.To(50) >> consumer;
	ASSERT_TRUE(consumer.EoF());
	ASSERT_TRUE(consumer.Ready());
	RETURN_TEST(0);
}

int test_sink_wire_all_hoppers() {
	Sink<int> producer;
	Sink<int> consumer;
	Sink<int> dummy;
	producer.To(1) >> dummy;
	producer.To(2) >> dummy;
	producer >> consumer;
	producer.Push(1, 100);
	producer.Push(2, 200);
	ASSERT_EQUAL(StormByte::Size{1}, consumer.Size(1));
	ASSERT_EQUAL(StormByte::Size{1}, consumer.Size(2));
	int val1 = consumer.Pop();
	int val2 = consumer.Pop();
	bool correct_set = (val1 == 100 && val2 == 200) || (val1 == 200 && val2 == 100);
	ASSERT_TRUE(correct_set);
	RETURN_TEST(0);
}

int test_sink_wire_and_push_pop() {
	Sink<StormByte::Safe::Shared<StormByte::Safe::String>> producer;
	Sink<StormByte::Safe::Shared<StormByte::Safe::String>> consumer;
	producer.To(10) >> consumer;
	producer.To(20) >> consumer;
	producer.Capacity(10, 5);
	ASSERT_EQUAL(StormByte::Size{5}, producer.Capacity(10));
	producer.Push(10, StormByte::Safe::MakeShared<StormByte::Safe::String>("String-10"));
	producer.Push(20, StormByte::Safe::MakeShared<StormByte::Safe::String>("String-20"));
	ASSERT_EQUAL(StormByte::Size{1}, consumer.Size(10));
	ASSERT_EQUAL(StormByte::Size{1}, consumer.Size(20));
	ASSERT_TRUE(consumer.Ready());
	auto item1 = consumer.Pop();
	auto item2 = consumer.Pop();
	ASSERT_TRUE(static_cast<bool>(item1));
	ASSERT_TRUE(static_cast<bool>(item2));
	producer.Eof();
	ASSERT_TRUE(consumer.EoF());
	RETURN_TEST(0);
}

int main() {
	int result = 0;

	// -------------------
	// Concurrency
	// -------------------
	result += test_sink_concurrent_wire_and_eof();
	result += test_sink_concurrent_wire_and_notify();

	// -------------------
	// Construct
	// -------------------
	result += test_sink_default_constructor();

	// -------------------
	// Drain
	// -------------------
	result += test_sink_drain_mode();
	result += test_sink_eof_unblocks_waiters();
	result += test_sink_non_nullable_smart_pointer();
	result += test_sink_overaligned_payload();
	result += test_sink_pop_custom_select();
	result += test_sink_pop_selector_exception_preserves_items();
	result += test_sink_push_waiting_for_wire();
	result += test_sink_selector_borrowed_state();
	result += test_sink_safe_selector_status();
	result += test_sink_safe_selector_provider_release();
	result += test_sink_single_bucket_selector_failure();

	// -------------------
	// Notify
	// -------------------
	result += test_sink_closed_binding_writer_release();
	result += test_sink_closed_cowriter_push();
	result += test_sink_notify_condition_variable();
	result += test_sink_observer_concurrent_unnotify();
	result += test_sink_observer_identity();
	result += test_sink_observer_rebind_and_destruction();
	result += test_sink_rebound_writer_eof();
	result += test_sink_stored_concurrent_unnotify();
	result += test_sink_stored_handoff();
	result += test_sink_stored_observer_identity();
	result += test_sink_stored_wiring();
	result += test_sink_unnotify_before_cv_dies();
	result += test_sink_writer_destruction();

	// -------------------
	// Query
	// -------------------
	result += test_sink_pop_key();
	result += test_sink_query_snapshot();
	result += test_sink_query_unwired();
	result += test_sink_query_wired();

	// -------------------
	// Wire
	// -------------------
	result += test_sink_extra_writer_eof();
	result += test_sink_rewire_same_writer_eof();
	result += test_sink_shared_hopper_lifetime();
	result += test_sink_stream_operators();
	result += test_sink_wire_after_eof();
	result += test_sink_wire_all_hoppers();
	result += test_sink_wire_and_push_pop();

	if (result == 0)
		std::cout << "All tests passed!" << std::endl;
	else
		std::cout << result << " tests failed." << std::endl;
	return result;
}
