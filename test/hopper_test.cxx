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

#include <StormByte/buffer/hopper.hxx>
#include <StormByte/safe/atomic.hxx>
#include <StormByte/safe/condition_variable.hxx>
#include <StormByte/safe/mutex.hxx>
#include <StormByte/safe/string.hxx>
#include <StormByte/safe/unique_lock.hxx>
#include <StormByte/test_handlers.h>

#include <atomic>
#include <chrono>
#include <iostream>
#include <memory>
#include <string>
#include <thread>
#include <vector>

using StormByte::Buffer::Hopper;
using StormByte::Safe::Atomic;
using StormByte::Safe::ConditionVariable;
using StormByte::Safe::MemoryOrder;
using StormByte::Safe::Mutex;
using StormByte::Safe::UniqueLock;

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
 * @brief Provider-declared value whose alignment exceeds Base's heap contract.
 */
struct alignas(alignof(std::max_align_t) * 2) OveralignedValue {
	int value = 0;	///< Embedded integer with no allocator or external lifetime.
};

STORMBYTE_DECLARE_MAYBE_SAFE(OveralignedValue);

/**
 * @brief Provider-declared value with throwing move assignment.
 */
struct ThrowingMoveValue {
	/**
	 * @brief Construct an empty value.
	 */
	ThrowingMoveValue() noexcept = default;

	/**
	 * @brief Copy an empty value.
	 * @param other Source.
	 */
	ThrowingMoveValue(const ThrowingMoveValue& other) noexcept = default;

	/**
	 * @brief Move an empty value.
	 * @param other Source.
	 */
	ThrowingMoveValue(ThrowingMoveValue&& other) noexcept = default;

	/**
	 * @brief Copy an empty value.
	 * @param other Source.
	 * @return This value.
	 */
	ThrowingMoveValue& operator=(const ThrowingMoveValue& other) noexcept = default;

	/**
	 * @brief Provide a potentially throwing move signature for admission testing.
	 * @param other Source.
	 * @return This value.
	 */
	ThrowingMoveValue& operator=(ThrowingMoveValue&& other) noexcept(false) {
		(void)other;
		return *this;
	}
};

STORMBYTE_DECLARE_MAYBE_SAFE(ThrowingMoveValue);

/**
 * @brief Test whether Hopper admits an element type without instantiating its storage.
 * @tparam Value Candidate queue element.
 */
template<typename Value>
concept HopperAdmits = requires { typename Hopper<Value>; };

/**
 * @brief Undeclared movable payload. It does not acquire a SafeValue contract automatically.
 */
struct UndeclaredValue {
	int value = 0;	///< Embedded integer.
};

/**
 * @brief A derived queue does not inherit an exact-type MaybeSafe declaration.
 */
struct DerivedHopper: Hopper<int> {};

static_assert(StormByte::Type::Movable<UndeclaredValue>);
static_assert(!StormByte::Type::SafeValue<UndeclaredValue>);
static_assert(!HopperAdmits<UndeclaredValue>);
static_assert(StormByte::Type::MaybeSafe<Hopper<int>>);
static_assert(!StormByte::Type::MaybeSafe<DerivedHopper>);
static_assert(HopperAdmits<int>);
static_assert(HopperAdmits<StormByte::Safe::String>);
static_assert(HopperAdmits<StormByte::Safe::Shared<int>>);
static_assert(HopperAdmits<NonNullableSmartPointer>);
static_assert(StormByte::Type::SafeValue<OveralignedValue>);
static_assert(!HopperAdmits<OveralignedValue>);
static_assert(StormByte::Type::SafeValue<ThrowingMoveValue>);
static_assert(!HopperAdmits<ThrowingMoveValue>);
static_assert(!HopperAdmits<const int>);
static_assert(!HopperAdmits<volatile int>);
static_assert(!HopperAdmits<int&>);
static_assert(!HopperAdmits<int*>);
static_assert(!HopperAdmits<std::string>);
static_assert(!HopperAdmits<std::shared_ptr<int>>);
static_assert(!HopperAdmits<std::unique_ptr<int>>);
static_assert(!HopperAdmits<StormByte::Safe::Unique<int>>);
static_assert(StormByte::Type::SmartPointer<NonNullableSmartPointer>);
static_assert(!StormByte::Type::NullablePointer<NonNullableSmartPointer>);

// -------------------
// Capacity
// -------------------

int test_hopper_bounded_constructor() {
	Hopper<int> hopper(5);
	ASSERT_EQUAL(StormByte::Size{5}, hopper.Capacity());
	ASSERT_EQUAL(StormByte::Size{0}, hopper.Size());
	ASSERT_TRUE(hopper.Empty());
	ASSERT_FALSE(hopper.Full());
	RETURN_TEST(0);
}

int test_hopper_default_constructor() {
	Hopper<int> hopper;
	ASSERT_EQUAL(StormByte::Size{0}, hopper.Capacity());
	ASSERT_EQUAL(StormByte::Size{0}, hopper.Size());
	ASSERT_TRUE(hopper.Empty());
	ASSERT_FALSE(hopper.Full());
	ASSERT_FALSE(hopper.EoF());
	RETURN_TEST(0);
}

int test_hopper_dynamic_capacity() {
	Hopper<int> hopper(10);
	hopper.Push(1);
	hopper.Push(2);
	hopper.Push(3);
	ASSERT_EQUAL(StormByte::Size{3}, hopper.Size());
	ASSERT_FALSE(hopper.Full());
	hopper.Capacity(3);
	ASSERT_EQUAL(StormByte::Size{3}, hopper.Capacity());
	ASSERT_TRUE(hopper.Full());
	ASSERT_EQUAL(StormByte::Size{3}, hopper.Size());
	hopper.Capacity(0);
	ASSERT_EQUAL(StormByte::Size{0}, hopper.Capacity());
	ASSERT_FALSE(hopper.Full());
	RETURN_TEST(0);
}

// -------------------
// Notify
// -------------------

int test_hopper_concurrent_unnotify() {
	Hopper<int> hopper;
	std::thread producer;
	{
		ConditionVariable wake;
		hopper.Notify(wake);
		producer = std::thread([&] {
			for (int item = 1; item <= 1000; ++item)
				hopper.Push(item);
			hopper.Eof();
		});
		hopper.Unnotify();
	}
	producer.join();
	ASSERT_EQUAL(StormByte::Size{1000}, hopper.Size());
	ASSERT_TRUE(hopper.EoF());
	RETURN_TEST(0);
}

int test_hopper_notify_after_unnotify() {
	Hopper<int> hopper;
	ConditionVariable first;
	hopper.Notify(first);
	hopper.Unnotify();
	ConditionVariable cv;
	Mutex m;
	hopper.Notify(cv);
	std::atomic<int> received{-1};
	std::thread consumer([&]() {
		UniqueLock lock(m);
		if (cv.wait_for(lock, std::chrono::seconds(1), [&]() { return hopper.Ready(); }))
			received.store(hopper.Pop(), std::memory_order_release);
	});
	std::this_thread::sleep_for(std::chrono::milliseconds(20));
	{
		UniqueLock lock(m);
		hopper.Push(7);
	}
	consumer.join();
	ASSERT_EQUAL(7, received.load(std::memory_order_acquire));
	RETURN_TEST(0);
}

int test_hopper_notify_condition_variable() {
	Hopper<int> hopper;
	ConditionVariable cv;
	Mutex m;
	hopper.Notify(cv);
	std::atomic<int> received_val{-1};
	std::thread consumer([&]() {
		UniqueLock lock(m);
		if (cv.wait_for(lock, std::chrono::seconds(1), [&]() { return hopper.Ready(); }))
			received_val.store(hopper.Pop(), std::memory_order_release);
	});
	std::this_thread::sleep_for(std::chrono::milliseconds(20));
	{
		UniqueLock lock(m);
		hopper.Push(999);
	}
	consumer.join();
	ASSERT_EQUAL(999, received_val.load(std::memory_order_acquire));
	RETURN_TEST(0);
}

int test_hopper_stored_concurrent_unnotify() {
	for (int iteration = 0; iteration < 50; ++iteration) {
		Hopper<int> hopper;
		std::thread producer;
		{
			ConditionVariable wake;
			Atomic<std::size_t> generation{0};
			hopper.Notify(wake, generation);
			producer = std::thread([&] {
				for (int item = 1; item <= 100; ++item)
					hopper.Push(item);
				hopper.Eof();
			});
			hopper.Unnotify();
		}
		producer.join();
		ASSERT_EQUAL(StormByte::Size{100}, hopper.Size());
		ASSERT_TRUE(hopper.EoF());
	}
	RETURN_TEST(0);
}

int test_hopper_stored_notifications() {
	Hopper<int> hopper;
	ConditionVariable wake;
	Atomic<std::size_t> first{0};
	Atomic<std::size_t> second{0};
	hopper.Notify(wake, first);
	const auto before = first.load(MemoryOrder::Acquire);
	ASSERT_FALSE(hopper.Ready());
	hopper.Push(42);
	ASSERT_EQUAL(before + 1, first.load(MemoryOrder::Acquire));
	first.wait(before, MemoryOrder::Acquire);
	ASSERT_EQUAL(42, hopper.Pop());
	hopper.Notify(wake, second);
	hopper.Push(43);
	hopper.Eof();
	ASSERT_EQUAL(before + 1, first.load(MemoryOrder::Acquire));
	ASSERT_EQUAL(std::size_t{2}, second.load(MemoryOrder::Acquire));
	ASSERT_EQUAL(43, hopper.Pop());
	hopper.Notify(wake);
	hopper.Eof();
	ASSERT_EQUAL(std::size_t{2}, second.load(MemoryOrder::Acquire));
	hopper.Unnotify();
	hopper.Unnotify();
	hopper.Eof();
	ASSERT_EQUAL(std::size_t{2}, second.load(MemoryOrder::Acquire));
	RETURN_TEST(0);
}

int test_hopper_unnotify_before_cv_dies() {
	Hopper<int> hopper;
	auto wake = std::make_unique<ConditionVariable>();
	hopper.Notify(*wake);
	hopper.Push(1);
	ASSERT_EQUAL(StormByte::Size{1}, hopper.Size());
	hopper.Unnotify();
	hopper.Unnotify();
	wake.reset();
	hopper.Eof();
	ASSERT_TRUE(hopper.EoF());
	ASSERT_EQUAL(1, hopper.Pop());
	ASSERT_TRUE(hopper.Empty());
	RETURN_TEST(0);
}

// -------------------
// Query
// -------------------

int test_hopper_front_peek() {
	Hopper<int> hopper;
	ASSERT_EQUAL(0, hopper.Front());
	ASSERT_TRUE(hopper.Empty());
	hopper.Push(10);
	hopper.Push(20);
	ASSERT_EQUAL(10, hopper.Front());
	ASSERT_EQUAL(10, hopper.Front());
	ASSERT_EQUAL(StormByte::Size{2}, hopper.Size());
	ASSERT_EQUAL(10, hopper.Pop());
	ASSERT_EQUAL(20, hopper.Front());
	ASSERT_EQUAL(StormByte::Size{1}, hopper.Size());
	Hopper<StormByte::Safe::Shared<StormByte::Safe::String>> shared;
	shared.Push(StormByte::Safe::MakeShared<StormByte::Safe::String>("peek"));
	auto a = shared.Front();
	auto b = shared.Front();
	ASSERT_TRUE(static_cast<bool>(a));
	ASSERT_TRUE(static_cast<bool>(b));
	ASSERT_EQUAL(StormByte::Safe::String("peek"), *a);
	ASSERT_EQUAL(a.get(), b.get());
	ASSERT_EQUAL(StormByte::Size{1}, shared.Size());
	RETURN_TEST(0);
}

int test_hopper_writers_and_ready() {
	Hopper<int> hopper;
	ASSERT_EQUAL(1u, hopper.Writers());
	ASSERT_FALSE(hopper.Ready());
	hopper.Push(1);
	ASSERT_TRUE(hopper.Ready());
	ASSERT_EQUAL(1u, hopper.Writers());
	ASSERT_EQUAL(1, hopper.Pop());
	ASSERT_FALSE(hopper.Ready());
	hopper.Eof();
	ASSERT_TRUE(hopper.Ready());
	ASSERT_TRUE(hopper.EoF());
	RETURN_TEST(0);
}

// -------------------
// Stream
// -------------------

int test_hopper_eof_behavior() {
	Hopper<int> hopper(1);
	hopper.Push(10);
	std::atomic<bool> push_unblocked{false};
	std::thread producer([&]() {
		hopper.Push(20);
		push_unblocked.store(true, std::memory_order_release);
	});
	std::this_thread::sleep_for(std::chrono::milliseconds(30));
	ASSERT_FALSE(push_unblocked.load(std::memory_order_acquire));
	hopper.Eof();
	producer.join();
	ASSERT_TRUE(push_unblocked.load(std::memory_order_acquire));
	ASSERT_TRUE(hopper.EoF());
	hopper.Push(30);
	ASSERT_EQUAL(StormByte::Size{1}, hopper.Size());
	ASSERT_EQUAL(10, hopper.Pop());
	ASSERT_TRUE(hopper.Empty());
	ASSERT_TRUE(hopper.EoF());
	RETURN_TEST(0);
}

int test_hopper_push_blocking_and_pop_unblock() {
	Hopper<int> hopper(2);
	hopper.Push(1);
	hopper.Push(2);
	std::atomic<bool> push_completed{false};
	std::thread producer([&]() {
		hopper.Push(3);
		push_completed.store(true, std::memory_order_release);
	});
	std::this_thread::sleep_for(std::chrono::milliseconds(30));
	ASSERT_FALSE(push_completed.load(std::memory_order_acquire));
	int popped = hopper.Pop();
	ASSERT_EQUAL(1, popped);
	producer.join();
	ASSERT_TRUE(push_completed.load(std::memory_order_acquire));
	ASSERT_EQUAL(StormByte::Size{2}, hopper.Size());
	RETURN_TEST(0);
}

int test_hopper_stream_item_into() {
	Hopper<int> hopper;
	int live = 11;
	live >> hopper;
	12 >> hopper;
	ASSERT_EQUAL(StormByte::Size{2}, hopper.Size());
	ASSERT_EQUAL(11, hopper.Pop());
	ASSERT_EQUAL(12, hopper.Pop());
	Hopper<StormByte::Safe::Shared<int>> ptrs;
	StormByte::Safe::Shared<int> empty;
	empty >> ptrs;
	ASSERT_TRUE(ptrs.Empty());
	StormByte::Safe::MakeShared<int>(9) >> ptrs;
	auto got = ptrs.Pop();
	ASSERT_TRUE(static_cast<bool>(got));
	ASSERT_EQUAL(9, *got);
	RETURN_TEST(0);
}

int test_hopper_stream_members() {
	Hopper<int> hopper;
	hopper << 1;
	hopper << 2;
	ASSERT_EQUAL(StormByte::Size{2}, hopper.Size());
	int a = 0;
	int b = 0;
	hopper >> a;
	hopper >> b;
	ASSERT_EQUAL(1, a);
	ASSERT_EQUAL(2, b);
	ASSERT_TRUE(hopper.Empty());
	int dry = 7;
	hopper >> dry;
	ASSERT_EQUAL(0, dry);
	RETURN_TEST(0);
}

// -------------------
// Stress
// -------------------

int test_hopper_spsc_stress() {
	constexpr int item_count = 1000;
	Hopper<int> hopper(64);
	ConditionVariable cv;
	Mutex m;
	hopper.Notify(cv);
	std::thread producer([&]() {
		for (int item = 0; item < item_count; ++item) {
			UniqueLock lock(m);
			if (!cv.wait_for(lock, std::chrono::seconds(2), [&]() { return !hopper.Full() || hopper.EoF(); })) {
				hopper.Eof();
				cv.notify_all();
				return;
			}
			if (hopper.EoF())
				return;
			hopper << item;
		}
		UniqueLock lock(m);
		hopper.Eof();
		cv.notify_all();
	});
	std::vector<int> received;
	received.reserve(item_count);
	std::thread consumer([&]() {
		while (true) {
			UniqueLock lock(m);
			if (!cv.wait_for(lock, std::chrono::seconds(2), [&]() { return hopper.Ready(); })) {
				hopper.Eof();
				cv.notify_all();
				break;
			}
			while (!hopper.Empty()) {
				int item = 0;
				hopper >> item;
				received.push_back(item);
			}
			cv.notify_all();
			if (hopper.EoF() && hopper.Empty())
				break;
		}
	});
	producer.join();
	consumer.join();
	ASSERT_EQUAL(static_cast<std::size_t>(item_count), received.size());
	for (int i = 0; i < item_count; ++i)
		ASSERT_EQUAL(i, received[static_cast<std::size_t>(i)]);
	RETURN_TEST(0);
}

// -------------------
// Types
// -------------------

int test_hopper_non_nullable_smart_pointer() {
	Hopper<NonNullableSmartPointer> hopper;
	hopper.Push(NonNullableSmartPointer(456));
	ASSERT_EQUAL(StormByte::Size{1}, hopper.Size());
	auto popped = hopper.Pop();
	ASSERT_EQUAL(456, *popped);
	ASSERT_TRUE(hopper.Empty());
	RETURN_TEST(0);
}

int test_hopper_smart_pointer_discard() {
	Hopper<StormByte::Safe::Shared<int>> unique_hopper;
	StormByte::Safe::Shared<int> null_unique;
	unique_hopper.Push(std::move(null_unique));
	ASSERT_TRUE(unique_hopper.Empty());
	ASSERT_EQUAL(StormByte::Size{0}, unique_hopper.Size());
	unique_hopper.Push(StormByte::Safe::MakeShared<int>(123));
	ASSERT_EQUAL(StormByte::Size{1}, unique_hopper.Size());
	auto popped_unique = unique_hopper.Pop();
	ASSERT_TRUE(static_cast<bool>(popped_unique));
	ASSERT_EQUAL(123, *popped_unique);
	Hopper<StormByte::Safe::Shared<StormByte::Safe::String>> shared_hopper;
	StormByte::Safe::Shared<StormByte::Safe::String> null_shared;
	shared_hopper.Push(null_shared);
	ASSERT_TRUE(shared_hopper.Empty());
	shared_hopper.Push(StormByte::Safe::MakeShared<StormByte::Safe::String>("StormByte"));
	ASSERT_EQUAL(StormByte::Size{1}, shared_hopper.Size());
	auto popped_shared = shared_hopper.Pop();
	ASSERT_EQUAL(StormByte::Safe::String("StormByte"), *popped_shared);
	RETURN_TEST(0);
}

int test_hopper_value_types() {
	Hopper<StormByte::Safe::String> hopper;
	hopper.Push(StormByte::Safe::String("Alpha"));
	hopper.Push(StormByte::Safe::String("Beta"));
	hopper.Push(StormByte::Safe::String("Gamma"));
	ASSERT_EQUAL(StormByte::Size{3}, hopper.Size());
	ASSERT_EQUAL(StormByte::Safe::String("Alpha"), hopper.Pop());
	ASSERT_EQUAL(StormByte::Safe::String("Beta"), hopper.Pop());
	ASSERT_EQUAL(StormByte::Safe::String("Gamma"), hopper.Pop());
	ASSERT_TRUE(hopper.Empty());
	RETURN_TEST(0);
}

int main() {
	int result = 0;

	// -------------------
	// Capacity
	// -------------------
	result += test_hopper_bounded_constructor();
	result += test_hopper_default_constructor();
	result += test_hopper_dynamic_capacity();

	// -------------------
	// Notify
	// -------------------
	result += test_hopper_concurrent_unnotify();
	result += test_hopper_notify_after_unnotify();
	result += test_hopper_notify_condition_variable();
	result += test_hopper_stored_concurrent_unnotify();
	result += test_hopper_stored_notifications();
	result += test_hopper_unnotify_before_cv_dies();

	// -------------------
	// Query
	// -------------------
	result += test_hopper_front_peek();
	result += test_hopper_writers_and_ready();

	// -------------------
	// Stream
	// -------------------
	result += test_hopper_eof_behavior();
	result += test_hopper_push_blocking_and_pop_unblock();
	result += test_hopper_stream_item_into();
	result += test_hopper_stream_members();

	// -------------------
	// Stress
	// -------------------
	result += test_hopper_spsc_stress();

	// -------------------
	// Types
	// -------------------
	result += test_hopper_non_nullable_smart_pointer();
	result += test_hopper_smart_pointer_discard();
	result += test_hopper_value_types();

	if (result == 0)
		std::cout << "All tests passed!" << std::endl;
	else
		std::cout << result << " tests failed." << std::endl;
	return result;
}
