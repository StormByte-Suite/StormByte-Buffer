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
#include <StormByte/safe/string.hxx>
#include <StormByte/test_handlers.h>

#include <atomic>
#include <chrono>
#include <condition_variable>
#include <iostream>
#include <memory>
#include <mutex>
#include <string>
#include <thread>
#include <vector>

using StormByte::Buffer::Hopper;

/**
 * @brief Exact provider-declared pointer facade storing only an integer, with no external ownership.
 * @note Its provider remains loaded and all participants use a compatible ABI.
 */
class NonNullableSmartPointer {
	public:
		/**
		 * @brief Constructs a zero-valued facade.
		 */
		NonNullableSmartPointer() noexcept = default;
		/**
		 * @brief Constructs a facade with an embedded value.
		 * @param value Initial embedded value.
		 */
		explicit NonNullableSmartPointer(int value) noexcept : m_value(value) {}
		/**
		 * @brief Copies the embedded integer.
		 */
		NonNullableSmartPointer(const NonNullableSmartPointer&) noexcept = default;
		/**
		 * @brief Moves the embedded integer.
		 */
		NonNullableSmartPointer(NonNullableSmartPointer&&) noexcept = default;
		/**
		 * @brief Destroys the facade without releasing external resources.
		 */
		~NonNullableSmartPointer() noexcept = default;
		/**
		 * @brief Copies the embedded integer.
		 * @return This facade.
		 */
		NonNullableSmartPointer& operator=(const NonNullableSmartPointer&) noexcept = default;
		/**
		 * @brief Moves the embedded integer.
		 * @return This facade.
		 */
		NonNullableSmartPointer& operator=(NonNullableSmartPointer&&) noexcept = default;

		/**
		 * @brief Gets mutable embedded storage.
		 * @return Address of the embedded integer.
		 */
		int* get() noexcept { return &m_value; }
		/**
		 * @brief Gets constant embedded storage.
		 * @return Address of the embedded integer.
		 */
		const int* get() const noexcept { return &m_value; }
		/**
		 * @brief Dereferences mutable embedded storage.
		 * @return Embedded integer.
		 */
		int& operator*() noexcept { return m_value; }
		/**
		 * @brief Dereferences constant embedded storage.
		 * @return Embedded integer.
		 */
		const int& operator*() const noexcept { return m_value; }
		/**
		 * @brief Gets mutable arrow access.
		 * @return Address of the embedded integer.
		 */
		int* operator->() noexcept { return &m_value; }
		/**
		 * @brief Gets constant arrow access.
		 * @return Address of the embedded integer.
		 */
		const int* operator->() const noexcept { return &m_value; }

	private:
		/**
		 * @brief Integer owned directly by the facade.
		 */
		int m_value = 0;
};

STORMBYTE_DECLARE_MAYBE_SAFE(NonNullableSmartPointer);

/**
 * @brief Provider-declared value whose alignment exceeds Base's heap contract.
 */
struct alignas(alignof(std::max_align_t) * 2) OveralignedValue {
	/**
	 * @brief Embedded integer with no allocator or external lifetime.
	 */
	int value = 0;
};
STORMBYTE_DECLARE_MAYBE_SAFE(OveralignedValue);

/**
 * @brief Provider-declared value with throwing move assignment.
 */
struct ThrowingMoveValue {
	/**
	 * @brief Constructs an empty value.
	 */
	ThrowingMoveValue() noexcept = default;
	/**
	 * @brief Copies an empty value.
	 */
	ThrowingMoveValue(const ThrowingMoveValue&) noexcept = default;
	/**
	 * @brief Moves an empty value.
	 */
	ThrowingMoveValue(ThrowingMoveValue&&) noexcept = default;
	/**
	 * @brief Copies an empty value.
	 * @return This value.
	 */
	ThrowingMoveValue& operator=(const ThrowingMoveValue&) noexcept = default;
	/**
	 * @brief Provides a potentially throwing move signature for admission testing.
	 * @return This value.
	 */
	ThrowingMoveValue& operator=(ThrowingMoveValue&&) noexcept(false) { return *this; }
};
STORMBYTE_DECLARE_MAYBE_SAFE(ThrowingMoveValue);

/**
 * @brief Tests whether Hopper admits an element type without instantiating its storage.
 * @tparam Value Candidate queue element.
 */
template<typename Value>
concept HopperAdmits = requires { typename Hopper<Value>; };

/**
 * @brief Undeclared movable payloads do not acquire a SafeValue contract automatically.
 */
struct UndeclaredValue {
	/**
	 * @brief Embedded integer.
	 */
	int value = 0;
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

/* -------------------------------------------------------------------------- */
/* Construction / capacity                                                    */
/* -------------------------------------------------------------------------- */

/**
 * @brief Tests bounded construction and capacity query.
 * @return 0 on success.
 */
int test_hopper_bounded_constructor() {
	Hopper<int> hopper(5);
	ASSERT_EQUAL("test_hopper_bounded_constructor capacity", StormByte::Size{5}, hopper.Capacity());
	ASSERT_EQUAL("test_hopper_bounded_constructor size", StormByte::Size{0}, hopper.Size());
	ASSERT_TRUE("test_hopper_bounded_constructor empty", hopper.Empty());
	ASSERT_FALSE("test_hopper_bounded_constructor full", hopper.Full());

	RETURN_TEST("test_hopper_bounded_constructor", 0);
}

/**
 * @brief Tests default construction of Hopper (unbounded capacity, empty, size 0).
 * @return 0 on success.
 */
int test_hopper_default_constructor() {
	Hopper<int> hopper;
	ASSERT_EQUAL("test_hopper_default_constructor capacity", StormByte::Size{0}, hopper.Capacity());
	ASSERT_EQUAL("test_hopper_default_constructor size", StormByte::Size{0}, hopper.Size());
	ASSERT_TRUE("test_hopper_default_constructor empty", hopper.Empty());
	ASSERT_FALSE("test_hopper_default_constructor full", hopper.Full());
	ASSERT_FALSE("test_hopper_default_constructor eof", hopper.EoF());

	RETURN_TEST("test_hopper_default_constructor", 0);
}

/**
 * @brief Tests dynamic capacity changes (lowering capacity, raising capacity).
 * @return 0 on success.
 */
int test_hopper_dynamic_capacity() {
	Hopper<int> hopper(10);
	hopper.Push(1);
	hopper.Push(2);
	hopper.Push(3);

	ASSERT_EQUAL("test_hopper_dynamic_capacity initial size", StormByte::Size{3}, hopper.Size());
	ASSERT_FALSE("test_hopper_dynamic_capacity initial full", hopper.Full());

	hopper.Capacity(3);
	ASSERT_EQUAL("test_hopper_dynamic_capacity lowered capacity", StormByte::Size{3}, hopper.Capacity());
	ASSERT_TRUE("test_hopper_dynamic_capacity full after lowering", hopper.Full());
	ASSERT_EQUAL("test_hopper_dynamic_capacity size preserved", StormByte::Size{3}, hopper.Size());

	hopper.Capacity(0);
	ASSERT_EQUAL("test_hopper_dynamic_capacity unbounded capacity", StormByte::Size{0}, hopper.Capacity());
	ASSERT_FALSE("test_hopper_dynamic_capacity not full when unbounded", hopper.Full());

	RETURN_TEST("test_hopper_dynamic_capacity", 0);
}

/* -------------------------------------------------------------------------- */
/* Item stream operators                                                      */
/* -------------------------------------------------------------------------- */

/**
 * @brief item >> hopper (lvalue and rvalue) enqueues.
 * @return 0 on success.
 */
int test_hopper_stream_item_into() {
	Hopper<int> hopper;
	int live = 11;
	live >> hopper;
	12 >> hopper;
	ASSERT_EQUAL("test_hopper_stream_item_into size", StormByte::Size{2}, hopper.Size());
	ASSERT_EQUAL("test_hopper_stream_item_into pop 1", 11, hopper.Pop());
	ASSERT_EQUAL("test_hopper_stream_item_into pop 2", 12, hopper.Pop());

	Hopper<StormByte::Safe::Shared<int>> ptrs;
	StormByte::Safe::Shared<int> empty;
	empty >> ptrs;
	ASSERT_TRUE("test_hopper_stream_item_into null discarded", ptrs.Empty());
	StormByte::Safe::Heap::MakeShared<int>(9) >> ptrs;
	auto got = ptrs.Pop();
	ASSERT_TRUE("test_hopper_stream_item_into ptr valid", static_cast<bool>(got));
	ASSERT_EQUAL("test_hopper_stream_item_into ptr value", 9, *got);

	RETURN_TEST("test_hopper_stream_item_into", 0);
}

/**
 * @brief hopper << item and hopper >> item match Push/Pop.
 * @return 0 on success.
 */
int test_hopper_stream_members() {
	Hopper<int> hopper;
	hopper << 1;
	hopper << 2;
	ASSERT_EQUAL("test_hopper_stream_members size", StormByte::Size{2}, hopper.Size());

	int a = 0;
	int b = 0;
	hopper >> a;
	hopper >> b;
	ASSERT_EQUAL("test_hopper_stream_members pop 1", 1, a);
	ASSERT_EQUAL("test_hopper_stream_members pop 2", 2, b);
	ASSERT_TRUE("test_hopper_stream_members empty", hopper.Empty());

	int dry = 7;
	hopper >> dry;
	ASSERT_EQUAL("test_hopper_stream_members dry pop", 0, dry);

	RETURN_TEST("test_hopper_stream_members", 0);
}

/* -------------------------------------------------------------------------- */
/* Item types                                                                 */
/* -------------------------------------------------------------------------- */

/**
 * @brief Tests smart pointer-like types without nullability are enqueued normally.
 * @return 0 on success.
 */
int test_hopper_non_nullable_smart_pointer() {
	Hopper<NonNullableSmartPointer> hopper;
	hopper.Push(NonNullableSmartPointer(456));

	ASSERT_EQUAL("test_hopper_non_nullable_smart_pointer size", StormByte::Size{1}, hopper.Size());
	auto popped = hopper.Pop();
	ASSERT_EQUAL("test_hopper_non_nullable_smart_pointer value", 456, *popped);
	ASSERT_TRUE("test_hopper_non_nullable_smart_pointer empty", hopper.Empty());

	RETURN_TEST("test_hopper_non_nullable_smart_pointer", 0);
}

/**
 * @brief Tests smart pointer discard semantics (null pointers are discarded without enqueuing).
 * @return 0 on success.
 */
int test_hopper_smart_pointer_discard() {
	Hopper<StormByte::Safe::Shared<int>> unique_hopper;
	StormByte::Safe::Shared<int> null_unique;
	unique_hopper.Push(std::move(null_unique));
	ASSERT_TRUE("test_hopper_smart_pointer_discard unique empty", unique_hopper.Empty());
	ASSERT_EQUAL("test_hopper_smart_pointer_discard unique size", StormByte::Size{0}, unique_hopper.Size());

	unique_hopper.Push(StormByte::Safe::Heap::MakeShared<int>(123));
	ASSERT_EQUAL("test_hopper_smart_pointer_discard unique size 1", StormByte::Size{1}, unique_hopper.Size());
	auto popped_unique = unique_hopper.Pop();
	ASSERT_TRUE("test_hopper_smart_pointer_discard popped valid", static_cast<bool>(popped_unique));
	ASSERT_EQUAL("test_hopper_smart_pointer_discard popped value", 123, *popped_unique);

	Hopper<StormByte::Safe::Shared<StormByte::Safe::String>> shared_hopper;
	StormByte::Safe::Shared<StormByte::Safe::String> null_shared;
	shared_hopper.Push(null_shared);
	ASSERT_TRUE("test_hopper_smart_pointer_discard shared empty", shared_hopper.Empty());

	shared_hopper.Push(StormByte::Safe::Heap::MakeShared<StormByte::Safe::String>("StormByte"));
	ASSERT_EQUAL("test_hopper_smart_pointer_discard shared size 1", StormByte::Size{1}, shared_hopper.Size());
	auto popped_shared = shared_hopper.Pop();
	ASSERT_EQUAL("test_hopper_smart_pointer_discard shared value", StormByte::Safe::String("StormByte"), *popped_shared);

	RETURN_TEST("test_hopper_smart_pointer_discard", 0);
}

/**
 * @brief Tests Base-owned text values are enqueued in order.
 * @return 0 on success.
 */
int test_hopper_value_types() {
	Hopper<StormByte::Safe::String> hopper;
	hopper.Push(StormByte::Safe::String("Alpha"));
	hopper.Push(StormByte::Safe::String("Beta"));
	hopper.Push(StormByte::Safe::String("Gamma"));

	ASSERT_EQUAL("test_hopper_value_types size", StormByte::Size{3}, hopper.Size());

	ASSERT_EQUAL("test_hopper_value_types pop 1", StormByte::Safe::String("Alpha"), hopper.Pop());
	ASSERT_EQUAL("test_hopper_value_types pop 2", StormByte::Safe::String("Beta"), hopper.Pop());
	ASSERT_EQUAL("test_hopper_value_types pop 3", StormByte::Safe::String("Gamma"), hopper.Pop());
	ASSERT_TRUE("test_hopper_value_types empty", hopper.Empty());

	RETURN_TEST("test_hopper_value_types", 0);
}

/* -------------------------------------------------------------------------- */
/* Notify / Unnotify                                                          */
/* -------------------------------------------------------------------------- */

/**
 * @brief Notify after Unnotify attaches a new CV.
 * @return 0 on success.
 */
int test_hopper_notify_after_unnotify() {
	Hopper<int> hopper;
	std::condition_variable first;
	hopper.Notify(first);
	hopper.Unnotify();

	std::condition_variable cv;
	std::mutex m;
	hopper.Notify(cv);

	std::atomic<int> received{-1};
	std::thread consumer([&]() {
		std::unique_lock<std::mutex> lock(m);
		if (cv.wait_for(lock, std::chrono::seconds(1), [&]() { return hopper.Ready(); }))
			received.store(hopper.Pop(), std::memory_order_release);
	});

	std::this_thread::sleep_for(std::chrono::milliseconds(20));
	{
		std::lock_guard<std::mutex> lock(m);
		hopper.Push(7);
	}
	consumer.join();

	ASSERT_EQUAL("test_hopper_notify_after_unnotify received", 7,
		received.load(std::memory_order_acquire));

	RETURN_TEST("test_hopper_notify_after_unnotify", 0);
}

/**
 * @brief Tests Notify callback mechanism waking consumer condition variables on Push and Eof.
 * @return 0 on success.
 */
int test_hopper_notify_condition_variable() {
	Hopper<int> hopper;
	std::condition_variable cv;
	std::mutex m;

	hopper.Notify(cv);

	std::atomic<int> received_val{-1};
	std::thread consumer([&]() {
		std::unique_lock<std::mutex> lock(m);
		if (cv.wait_for(lock, std::chrono::seconds(1), [&]() { return hopper.Ready(); }))
			received_val.store(hopper.Pop(), std::memory_order_release);
	});

	std::this_thread::sleep_for(std::chrono::milliseconds(20));
	{
		std::lock_guard<std::mutex> lock(m);
		hopper.Push(999);
	}
	consumer.join();

	ASSERT_EQUAL("test_hopper_notify_condition_variable received item", 999, received_val.load(std::memory_order_acquire));

	RETURN_TEST("test_hopper_notify_condition_variable", 0);
}

/**
 * @brief Unnotify drops the consumer CV so a later Eof is safe after it dies.
 *
 * Sink wiring shares the Hopper. Notify does not own the CV. Without
 * Unnotify, Eof would signal a destroyed object.
 *
 * @return 0 on success.
 */
int test_hopper_unnotify_before_cv_dies() {
	Hopper<int> hopper;
	auto wake = std::make_unique<std::condition_variable>();
	hopper.Notify(*wake);
	hopper.Push(1);
	ASSERT_EQUAL("test_hopper_unnotify_before_cv_dies queued",
		StormByte::Size{1}, hopper.Size());

	hopper.Unnotify();
	hopper.Unnotify();
	wake.reset();

	hopper.Eof();
	ASSERT_TRUE("test_hopper_unnotify_before_cv_dies eof", hopper.EoF());
	ASSERT_EQUAL("test_hopper_unnotify_before_cv_dies pop", 1, hopper.Pop());
	ASSERT_TRUE("test_hopper_unnotify_before_cv_dies empty", hopper.Empty());

	RETURN_TEST("test_hopper_unnotify_before_cv_dies", 0);
}

/**
 * @brief Removal completes before a borrowed CV dies while producer notifications continue.
 * @return Zero on success.
 */
int test_hopper_concurrent_unnotify() {
	constexpr auto name = "test_hopper_concurrent_unnotify";
	Hopper<int> hopper;
	std::thread producer;
	{
		std::condition_variable wake;
		hopper.Notify(wake);
		producer = std::thread([&] {
			for (int item = 1; item <= 1000; ++item)
				hopper.Push(item);
			hopper.Eof();
		});
		hopper.Unnotify();
	}
	producer.join();
	ASSERT_EQUAL(name, StormByte::Size{1000}, hopper.Size());
	ASSERT_TRUE(name, hopper.EoF());
	RETURN_TEST(name, 0);
}

/* -------------------------------------------------------------------------- */
/* Push / Pop / Eof                                                           */
/* -------------------------------------------------------------------------- */

/**
 * @brief Tests Eof marking, waking blocked Push threads and ignoring subsequent Push.
 * @return 0 on success.
 */
int test_hopper_eof_behavior() {
	Hopper<int> hopper(1);
	hopper.Push(10);

	std::atomic<bool> push_unblocked{false};

	std::thread producer([&]() {
		hopper.Push(20);
		push_unblocked.store(true, std::memory_order_release);
	});

	std::this_thread::sleep_for(std::chrono::milliseconds(30));
	ASSERT_FALSE("test_hopper_eof_behavior producer blocked", push_unblocked.load(std::memory_order_acquire));

	hopper.Eof();
	producer.join();

	ASSERT_TRUE("test_hopper_eof_behavior eof unblocked producer", push_unblocked.load(std::memory_order_acquire));
	ASSERT_TRUE("test_hopper_eof_behavior eof flag set", hopper.EoF());

	hopper.Push(30);
	ASSERT_EQUAL("test_hopper_eof_behavior size 1", StormByte::Size{1}, hopper.Size());
	ASSERT_EQUAL("test_hopper_eof_behavior pop remaining item", 10, hopper.Pop());
	ASSERT_TRUE("test_hopper_eof_behavior empty", hopper.Empty());
	ASSERT_TRUE("test_hopper_eof_behavior eof remains true", hopper.EoF());

	RETURN_TEST("test_hopper_eof_behavior", 0);
}

/**
 * @brief Tests Push blocking when capacity ceiling is reached, and unblocking on Pop.
 * @return 0 on success.
 */
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
	ASSERT_FALSE("test_hopper_push_blocking_and_pop_unblock blocked push", push_completed.load(std::memory_order_acquire));

	int popped = hopper.Pop();
	ASSERT_EQUAL("test_hopper_push_blocking_and_pop_unblock popped 1", 1, popped);

	producer.join();
	ASSERT_TRUE("test_hopper_push_blocking_and_pop_unblock push resumed", push_completed.load(std::memory_order_acquire));
	ASSERT_EQUAL("test_hopper_push_blocking_and_pop_unblock size 2", StormByte::Size{2}, hopper.Size());

	RETURN_TEST("test_hopper_push_blocking_and_pop_unblock", 0);
}

/* -------------------------------------------------------------------------- */
/* Query                                                                      */
/* -------------------------------------------------------------------------- */

/**
 * @brief Front copies the next item and does not dequeue.
 * @return 0 on success.
 */
int test_hopper_front_peek() {
	Hopper<int> hopper;
	ASSERT_EQUAL("test_hopper_front_peek empty", 0, hopper.Front());
	ASSERT_TRUE("test_hopper_front_peek still empty", hopper.Empty());

	hopper.Push(10);
	hopper.Push(20);
	ASSERT_EQUAL("test_hopper_front_peek first", 10, hopper.Front());
	ASSERT_EQUAL("test_hopper_front_peek first again", 10, hopper.Front());
	ASSERT_EQUAL("test_hopper_front_peek size after peek", StormByte::Size{2}, hopper.Size());

	ASSERT_EQUAL("test_hopper_front_peek pop", 10, hopper.Pop());
	ASSERT_EQUAL("test_hopper_front_peek second", 20, hopper.Front());
	ASSERT_EQUAL("test_hopper_front_peek size after pop", StormByte::Size{1}, hopper.Size());

	Hopper<StormByte::Safe::Shared<StormByte::Safe::String>> shared;
	shared.Push(StormByte::Safe::Heap::MakeShared<StormByte::Safe::String>("peek"));
	auto a = shared.Front();
	auto b = shared.Front();
	ASSERT_TRUE("test_hopper_front_peek shared a", static_cast<bool>(a));
	ASSERT_TRUE("test_hopper_front_peek shared b", static_cast<bool>(b));
	ASSERT_EQUAL("test_hopper_front_peek shared value", StormByte::Safe::String("peek"), *a);
	ASSERT_EQUAL("test_hopper_front_peek shared same ptr", a.get(), b.get());
	ASSERT_EQUAL("test_hopper_front_peek shared size", StormByte::Size{1}, shared.Size());

	RETURN_TEST("test_hopper_front_peek", 0);
}

/**
 * @brief Writers starts at 1. Ready is false when empty and not EoF.
 * @return 0 on success.
 */
int test_hopper_writers_and_ready() {
	Hopper<int> hopper;
	ASSERT_EQUAL("test_hopper_writers_and_ready writers", 1u, hopper.Writers());
	ASSERT_FALSE("test_hopper_writers_and_ready ready empty", hopper.Ready());

	hopper.Push(1);
	ASSERT_TRUE("test_hopper_writers_and_ready ready with item", hopper.Ready());
	ASSERT_EQUAL("test_hopper_writers_and_ready writers unchanged", 1u, hopper.Writers());

	ASSERT_EQUAL("test_hopper_writers_and_ready pop", 1, hopper.Pop());
	ASSERT_FALSE("test_hopper_writers_and_ready ready after drain", hopper.Ready());

	hopper.Eof();
	ASSERT_TRUE("test_hopper_writers_and_ready ready on eof", hopper.Ready());
	ASSERT_TRUE("test_hopper_writers_and_ready eof", hopper.EoF());

	RETURN_TEST("test_hopper_writers_and_ready", 0);
}

/* -------------------------------------------------------------------------- */
/* Stress                                                                     */
/* -------------------------------------------------------------------------- */

/**
 * @brief Multi-threaded SPSC stress test transferring items through a Hopper.
 * @return 0 on success.
 */
int test_hopper_spsc_stress() {
	constexpr int item_count = 1000;
	Hopper<int> hopper(64);

	std::condition_variable cv;
	std::mutex m;
	hopper.Notify(cv);

	std::thread producer([&]() {
		for (int item = 0; item < item_count; ++item) {
			std::unique_lock<std::mutex> lock(m);
			if (!cv.wait_for(lock, std::chrono::seconds(2), [&]() { return !hopper.Full() || hopper.EoF(); })) {
				hopper.Eof();
				cv.notify_all();
				return;
			}
			if (hopper.EoF())
				return;
			hopper << item;
		}
		std::lock_guard<std::mutex> lock(m);
		hopper.Eof();
		cv.notify_all();
	});

	std::vector<int> received;
	received.reserve(item_count);

	std::thread consumer([&]() {
		while (true) {
			std::unique_lock<std::mutex> lock(m);
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

	ASSERT_EQUAL("test_hopper_spsc_stress count", static_cast<std::size_t>(item_count), received.size());
	for (int i = 0; i < item_count; ++i) {
		if (received[static_cast<std::size_t>(i)] != i)
			ASSERT_EQUAL("test_hopper_spsc_stress item mismatch", i, received[static_cast<std::size_t>(i)]);
	}

	RETURN_TEST("test_hopper_spsc_stress", 0);
}

/**
 * @brief Main entry point for Hopper tests.
 * @return 0 on all tests passing, non-zero on failure.
 */
int main() {
	int failed = 0;

	// Construction / capacity
	failed += test_hopper_bounded_constructor();
	failed += test_hopper_default_constructor();
	failed += test_hopper_dynamic_capacity();

	// Item stream operators
	failed += test_hopper_stream_item_into();
	failed += test_hopper_stream_members();

	// Item types
	failed += test_hopper_non_nullable_smart_pointer();
	failed += test_hopper_smart_pointer_discard();
	failed += test_hopper_value_types();

	// Notify / Unnotify
	failed += test_hopper_notify_after_unnotify();
	failed += test_hopper_notify_condition_variable();
	failed += test_hopper_unnotify_before_cv_dies();
	failed += test_hopper_concurrent_unnotify();

	// Push / Pop / Eof
	failed += test_hopper_eof_behavior();
	failed += test_hopper_push_blocking_and_pop_unblock();

	// Query
	failed += test_hopper_front_peek();
	failed += test_hopper_writers_and_ready();

	// Stress
	failed += test_hopper_spsc_stress();

	if (failed != 0) {
		std::cerr << failed << " test(s) failed." << std::endl;
		return 1;
	}

	std::cout << "Hopper tests passed!" << std::endl;
	return 0;
}
