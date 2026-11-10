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

#include <StormByte/buffer/byte_budget.hxx>
#include <StormByte/buffer/sink.hxx>
#include <StormByte/test_handlers.h>

#include <array>
#include <atomic>
#include <chrono>
#include <limits>
#include <thread>
#include <vector>

using namespace StormByte::Buffer;
using StormByte::ByteSize;
using StormByte::Safe::Optional;
using StormByte::Size;

namespace {
	/**
	 * @brief Wait for a bounded test predicate without hanging a failed concurrency test.
	 * @tparam Predicate Test observation.
	 * @param predicate Nonblocking observation.
	 * @return True when observed before the test deadline.
	 */
	template<typename Predicate>
	bool WaitFor(Predicate predicate) {
		const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(2);
		while (!predicate()) {
			if (std::chrono::steady_clock::now() >= deadline)
				return false;
			std::this_thread::yield();
		}
		return true;
	}

	/**
	 * @brief Construct explicit optional local bounds.
	 * @param elements Item ceiling.
	 * @param bytes Logical byte ceiling.
	 * @return Both independent bounds engaged.
	 */
	QueueLimits Bounds(std::size_t elements, std::size_t bytes) {
		return {StormByte::Safe::Optional<Size>{Size{elements}},
			StormByte::Safe::Optional<ByteSize>{ByteSize{bytes}}};
	}
}

// -------------------
// Budget
// -------------------

int test_budget_cancel_close_and_timeout() {
	ByteBudget budget(ByteSize{3});
	ByteBudget::Lease held;
	ASSERT_EQUAL(QueueStatus::Ok, budget.TryReserve(3, held));
	for (const bool close : {false, true}) {
		QueueStatus result = QueueStatus::Ok;
		std::thread worker([&] {
			ByteBudget::Lease output;
			result = budget.ReserveFor(1, output, std::chrono::seconds(2));
		});
		const bool waiting = WaitFor([&] { return budget.Snapshot().Pressure > (close ? 1u : 0u); });
		if (close)
			budget.Close();
		else
			budget.CancelPending();
		worker.join();
		ASSERT_TRUE(waiting);
		ASSERT_EQUAL(close ? QueueStatus::Closed : QueueStatus::Cancelled, result);
	}
	ASSERT_EQUAL(ByteSize{3}, budget.Snapshot().Reserved);
	held.Reset();
	ASSERT_EQUAL(ByteSize{0}, budget.Snapshot().Reserved);
	ByteBudget zero(ByteSize{0});
	ByteBudget::Lease output;
	ASSERT_EQUAL(QueueStatus::TooLarge, zero.ReserveFor(1, output, std::chrono::seconds(2)));
	ASSERT_EQUAL(QueueStatus::Ok, zero.TryReserve(0, output));
	ASSERT_TRUE(static_cast<bool>(output));
	ASSERT_EQUAL(QueueStatus::Invalid, zero.ReserveFor(0, output, std::chrono::milliseconds(-1)));
	ByteBudget full(ByteSize{1});
	ASSERT_EQUAL(QueueStatus::Ok, full.TryReserve(1, held));
	ASSERT_EQUAL(QueueStatus::Timeout, full.ReserveFor(1, output, std::chrono::milliseconds(2)));
	ASSERT_TRUE(full.Snapshot().Waits > 0);
	ASSERT_TRUE(full.Snapshot().WaitNanoseconds > 0);
	RETURN_TEST(0);
}

int test_budget_lease_identity_and_lifetime() {
	ByteBudget budget(ByteSize{100});
	ByteBudget shared = budget;
	ByteBudget other(ByteSize{100});
	ByteBudget::Lease first;
	ASSERT_EQUAL(QueueStatus::Ok, budget.TryReserve(60, first));
	auto second = first;
	ASSERT_TRUE(second.BelongsTo(shared));
	ASSERT_FALSE(second.BelongsTo(other));
	ASSERT_EQUAL(ByteSize{60}, shared.Snapshot().Reserved);
	ASSERT_EQUAL(QueueStatus::WouldBlock, budget.TryReserve(50, first));
	ASSERT_EQUAL(ByteSize{60}, first.Bytes());
	first.Reset();
	ASSERT_EQUAL(ByteSize{60}, shared.Snapshot().Reserved);
	second.Reset();
	ASSERT_EQUAL(ByteSize{0}, shared.Snapshot().Reserved);
	ASSERT_EQUAL(ByteSize{60}, shared.Snapshot().Peak);
	ByteBudget::Lease survivor;
	{
		ByteBudget temporary(ByteSize{10});
		ASSERT_EQUAL(QueueStatus::Ok, temporary.TryReserve(10, survivor));
	}
	ASSERT_EQUAL(ByteSize{10}, survivor.Bytes());
	survivor.Reset();
	RETURN_TEST(0);
}

// -------------------
// Concurrent
// -------------------

int test_concurrent_shared_budget_and_writers() {
	ByteBudget budget(ByteSize{37});
	Sink<int> producer;
	Sink<int> consumer;
	producer.To(1) >> consumer;
	producer.To(2) >> consumer;
	ASSERT_EQUAL(QueueStatus::Ok, producer.Configure(1, Bounds(5, 23), budget));
	ASSERT_EQUAL(QueueStatus::Ok, producer.Configure(2, Bounds(7, 29), budget));
	constexpr std::size_t Workers = 4;
	constexpr std::size_t PerWorker = 200;
	std::array<Sink<int>, Workers> writers;
	for (auto& writer : writers) {
		producer.To(1) >> writer;
		producer.To(2) >> writer;
	}
	producer.Eof();
	std::atomic<bool> valid {true};
	std::array<std::atomic<int>, Workers * PerWorker> seen {};
	std::thread receiver([&] {
		while (!consumer.EoF()) {
			for (const int key : {1, 2}) {
				const auto item = consumer.Pop(key);
				if (item > 0 && static_cast<std::size_t>(item) <= seen.size())
					++seen[item - 1];
				else if (item != 0)
					valid.store(false);
			}
			const auto snapshot = budget.Snapshot();
			if (snapshot.Reserved > ByteSize{37})
				valid.store(false);
			std::this_thread::yield();
		}
	});
	std::array<std::thread, Workers> threads;
	for (std::size_t worker = 0; worker < Workers; ++worker) {
		threads[worker] = std::thread([&, worker] {
			for (std::size_t index = 0; index < PerWorker; ++index) {
				int item = static_cast<int>(worker * PerWorker + index + 1);
				const auto status = writers[worker].PushFor(index % 2 + 1, item, ByteSize{index % 11 + 1},
					std::chrono::seconds(3));
				if (status != QueueStatus::Ok)
					valid.store(false);
			}
			writers[worker].Eof();
		});
	}
	for (auto& thread : threads)
		thread.join();
	receiver.join();
	ASSERT_TRUE(valid.load());
	for (const auto& count : seen)
		ASSERT_EQUAL(1, count.load());
	ASSERT_EQUAL(ByteSize{0}, budget.Snapshot().Reserved);
	ASSERT_TRUE(budget.Snapshot().Peak <= ByteSize{37});
	for (const int key : {1, 2}) {
		const auto snapshot = consumer.Snapshot(key);
		ASSERT_TRUE(snapshot.PeakElements <= (key == 1 ? Size{5} : Size{7}));
		ASSERT_TRUE(snapshot.PeakBytes <= (key == 1 ? ByteSize{23} : ByteSize{29}));
	}
	RETURN_TEST(0);
}

int test_concurrent_work_and_queue() {
	ByteBudget budget(ByteSize{10});
	Hopper<int> hopper;
	ASSERT_EQUAL(QueueStatus::Ok, hopper.Configure({}, budget));
	ByteBudget::Lease work;
	ASSERT_EQUAL(QueueStatus::Ok, budget.TryReserve(8, work));
	int item = 9;
	QueueStatus status = QueueStatus::Error;
	std::thread producer([&] { status = hopper.PushFor(item, 5, std::chrono::seconds(2)); });
	const bool waiting = WaitFor([&] { return hopper.Snapshot().Pressure > 0; });
	ASSERT_EQUAL(ByteSize{8}, budget.Snapshot().Reserved);
	work.Reset();
	producer.join();
	ASSERT_TRUE(waiting);
	ASSERT_EQUAL(QueueStatus::Ok, status);
	ASSERT_EQUAL(ByteSize{5}, budget.Snapshot().Reserved);
	ASSERT_EQUAL(9, hopper.Pop());
	ASSERT_EQUAL(ByteSize{0}, budget.Snapshot().Reserved);
	RETURN_TEST(0);
}

// -------------------
// Hopper
// -------------------

int test_hopper_independent_limits_and_reduction() {
	Hopper<int> hopper;
	hopper.Limits(QueueLimits(Optional<Size>{Size{2}}, {}));
	int item = 1;
	ASSERT_EQUAL(QueueStatus::Ok, hopper.TryPush(item, 1000));
	item = 2;
	ASSERT_EQUAL(QueueStatus::Ok, hopper.TryPush(item, 2000));
	item = 3;
	ASSERT_EQUAL(QueueStatus::WouldBlock, hopper.TryPush(item, 0));
	ASSERT_EQUAL(1, hopper.Pop());
	ASSERT_EQUAL(2, hopper.Pop());

	hopper.Limits(QueueLimits({}, Optional<ByteSize>{ByteSize{5}}));
	ASSERT_EQUAL(QueueStatus::Ok, hopper.TryPush(item, 3));
	item = 4;
	ASSERT_EQUAL(QueueStatus::Ok, hopper.TryPush(item, 2));
	item = 5;
	ASSERT_EQUAL(QueueStatus::Ok, hopper.TryPush(item, 0));
	ASSERT_EQUAL(3, hopper.Pop());
	hopper.Limits(QueueLimits({}, Optional<ByteSize>{ByteSize{1}}));
	ASSERT_EQUAL(ByteSize{2}, hopper.Snapshot().Bytes);
	const auto pressure = hopper.Snapshot().Pressure;
	QueueStatus result = QueueStatus::Error;
	std::thread waiting([&] {
		int next = 6;
		result = hopper.PushFor(next, 1, std::chrono::seconds(2));
	});
	const bool pressured = WaitFor([&] { return hopper.Snapshot().Pressure > pressure; });
	ByteBudget replacement(ByteSize{10});
	const auto configured = hopper.Configure({}, replacement);
	hopper.Limits(QueueLimits({}, Optional<ByteSize>{ByteSize{5}}));
	waiting.join();
	ASSERT_TRUE(pressured);
	ASSERT_EQUAL(QueueStatus::Invalid, configured);
	ASSERT_EQUAL(QueueStatus::Ok, result);
	ASSERT_EQUAL(ByteSize{3}, hopper.Snapshot().Bytes);
	ASSERT_EQUAL(4, hopper.Pop());
	ASSERT_EQUAL(5, hopper.Pop());
	ASSERT_EQUAL(6, hopper.Pop());
	ASSERT_EQUAL(ByteSize{0}, hopper.Snapshot().Bytes);
	RETURN_TEST(0);
}

int test_hopper_limits_and_legacy_zero() {
	Hopper<int> hopper;
	hopper.Limits(Bounds(2, 10));
	int item = 1;
	ASSERT_EQUAL(QueueStatus::Ok, hopper.TryPush(item, 7));
	item = 2;
	ASSERT_EQUAL(QueueStatus::WouldBlock, hopper.TryPush(item, 4));
	ASSERT_EQUAL(2, item);
	ASSERT_EQUAL(QueueStatus::Ok, hopper.TryPush(item, 3));
	item = 3;
	ASSERT_EQUAL(QueueStatus::WouldBlock, hopper.TryPush(item, 0));
	ASSERT_EQUAL(Size{2}, hopper.Snapshot().Elements);
	ASSERT_EQUAL(ByteSize{10}, hopper.Snapshot().Bytes);
	ASSERT_EQUAL(1, hopper.Pop());
	ASSERT_EQUAL(QueueStatus::Ok, hopper.TryPush(item, 4));
	ASSERT_EQUAL(2, hopper.Pop());
	ASSERT_EQUAL(3, hopper.Pop());
	ASSERT_EQUAL(ByteSize{0}, hopper.Snapshot().Bytes);
	hopper.Limits(Bounds(0, 0));
	ASSERT_EQUAL(QueueStatus::TooLarge, hopper.TryPush(item, 0));
	hopper.Capacity(0);
	ASSERT_EQUAL(QueueStatus::Ok, hopper.TryPush(item, 0));
	ASSERT_EQUAL(QueueStatus::TooLarge, hopper.TryPush(item, 1));
	hopper.Limits({});
	for (int index = 0; index < 100; ++index)
		hopper.Push(index);
	ASSERT_EQUAL(Size{101}, hopper.Size());
	RETURN_TEST(0);
}

int test_hopper_oversize_with_full_local_queue() {
	ByteBudget budget(ByteSize{3});
	Hopper<int> hopper;
	ASSERT_EQUAL(QueueStatus::Ok, hopper.Configure(Bounds(1, 10), budget));
	int item = 1;
	ASSERT_EQUAL(QueueStatus::Ok, hopper.TryPush(item, 3));
	item = 2;
	ASSERT_EQUAL(QueueStatus::TooLarge, hopper.TryPush(item, 4));
	ASSERT_EQUAL(QueueStatus::TooLarge, hopper.PushFor(item, 4, std::chrono::seconds(1)));
	ASSERT_EQUAL(2, item);
	ASSERT_EQUAL(ByteSize{3}, budget.Snapshot().Reserved);
	ASSERT_EQUAL(Size{1}, hopper.Size());
	RETURN_TEST(0);
}

int test_hopper_shared_charge_and_destruction() {
	ByteBudget budget(ByteSize{20});
	ByteBudget::Lease lease;
	ASSERT_EQUAL(QueueStatus::Ok, budget.TryReserve(20, lease));
	{
		Hopper<int> first;
		Hopper<int> second;
		ASSERT_EQUAL(QueueStatus::Ok, first.Configure(Bounds(2, 40), budget));
		ASSERT_EQUAL(QueueStatus::Ok, second.Configure(Bounds(2, 40), budget));
		int item = 1;
		ASSERT_EQUAL(QueueStatus::Ok, first.TryPush(item, lease));
		ASSERT_EQUAL(QueueStatus::Ok, second.TryPush(item, lease));
		ASSERT_EQUAL(ByteSize{20}, budget.Snapshot().Reserved);
		ASSERT_EQUAL(ByteSize{20}, first.Snapshot().Bytes);
		ASSERT_EQUAL(ByteSize{20}, second.Snapshot().Bytes);
		lease.Reset();
		ASSERT_EQUAL(1, first.Pop());
		ASSERT_EQUAL(ByteSize{20}, budget.Snapshot().Reserved);
		second.Eof();
		ASSERT_EQUAL(ByteSize{20}, budget.Snapshot().Reserved);
	}
	ASSERT_EQUAL(ByteSize{0}, budget.Snapshot().Reserved);
	Hopper<int> wrong;
	int item = 7;
	ASSERT_EQUAL(QueueStatus::Ok, budget.TryReserve(4, lease));
	ASSERT_EQUAL(QueueStatus::Invalid, wrong.TryPush(item, lease));
	ASSERT_EQUAL(7, item);
	ASSERT_EQUAL(ByteSize{4}, budget.Snapshot().Reserved);
	RETURN_TEST(0);
}

// -------------------
// Sink
// -------------------

int test_sink_cancel_eof_abort_and_budget_close() {
	for (const int action : {0, 1, 2, 3}) {
		ByteBudget budget(ByteSize{3});
		Sink<int> producer;
		Sink<int> consumer;
		Sink<int> cowriter;
		producer.To(1) >> consumer;
		producer.To(1) >> cowriter;
		ASSERT_EQUAL(QueueStatus::Ok, producer.Configure(1, Bounds(1, 3), budget));
		int item = 1;
		ASSERT_EQUAL(QueueStatus::Ok, producer.TryPush(1, item, 3));
		QueueStatus result = QueueStatus::Error;
		std::thread waiting([&] {
			int next = 2;
			result = producer.PushFor(1, next, 1, std::chrono::seconds(2));
		});
		const bool pressured = WaitFor([&] { return producer.Snapshot(1).Pressure > 0; });
		if (action == 0)
			producer.CancelPending(1);
		else if (action == 1)
			producer.Eof();
		else if (action == 2)
			producer.Abort();
		else
			budget.Close();
		waiting.join();
		ASSERT_TRUE(pressured);
		ASSERT_EQUAL(action == 0 ? QueueStatus::Cancelled : QueueStatus::Closed, result);
		ASSERT_EQUAL(action == 2 ? ByteSize{0} : ByteSize{3}, budget.Snapshot().Reserved);
		if (action != 2)
			ASSERT_EQUAL(1, consumer.Pop(1));
		ASSERT_EQUAL(ByteSize{0}, budget.Snapshot().Reserved);
	}
	RETURN_TEST(0);
}

int test_sink_missing_limits_and_configuration() {
	Sink<int> producer;
	Sink<int> consumer;
	ByteBudget budget(ByteSize{10});
	int item = 7;
	ASSERT_EQUAL(QueueStatus::Missing, producer.TryPush(5, item, 1));
	ASSERT_FALSE(producer.Snapshot(5).Exists);
	ASSERT_EQUAL(QueueStatus::Missing, producer.Configure(5, {}, budget));
	producer.To(5) >> consumer;
	ASSERT_EQUAL(QueueStatus::Ok, producer.Configure(5, Bounds(1, 9), budget));
	ASSERT_EQUAL(QueueStatus::TooLarge, producer.PushFor(5, item, 10, std::chrono::seconds(1)));
	ASSERT_EQUAL(QueueStatus::Ok, producer.TryPush(5, item, 9));
	ASSERT_EQUAL(QueueStatus::Invalid, producer.Configure(5, Bounds(2, 10), budget));
	ASSERT_EQUAL(Size{1}, producer.Snapshot(5).Limits.Elements());
	producer.Capacity(5, 0);
	ASSERT_FALSE(producer.Snapshot(5).Limits.ElementsLimited());
	ASSERT_TRUE(producer.Snapshot(5).Limits.BytesLimited());
	producer.Eof();
	ASSERT_EQUAL(ByteSize{9}, budget.Snapshot().Reserved);
	ASSERT_EQUAL(7, consumer.Pop(5));
	ASSERT_EQUAL(ByteSize{0}, budget.Snapshot().Reserved);
	ASSERT_TRUE(consumer.EoF());
	RETURN_TEST(0);
}

int main() {
	int result = 0;

	// -------------------
	// Budget
	// -------------------
	result += test_budget_cancel_close_and_timeout();
	result += test_budget_lease_identity_and_lifetime();

	// -------------------
	// Concurrent
	// -------------------
	result += test_concurrent_shared_budget_and_writers();
	result += test_concurrent_work_and_queue();

	// -------------------
	// Hopper
	// -------------------
	result += test_hopper_independent_limits_and_reduction();
	result += test_hopper_limits_and_legacy_zero();
	result += test_hopper_oversize_with_full_local_queue();
	result += test_hopper_shared_charge_and_destruction();

	// -------------------
	// Sink
	// -------------------
	result += test_sink_cancel_eof_abort_and_budget_close();
	result += test_sink_missing_limits_and_configuration();
	return result;
}
