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

#pragma once

#include <StormByte/type_traits/safe.hxx>

#include <StormByte/safe/atomic.hxx>
#include <StormByte/safe/condition_variable.hxx>
#include <StormByte/safe/owner.hxx>
#include <StormByte/safe/pointers.hxx>
#include <StormByte/size.hxx>
#include <StormByte/type_traits.hxx>

#include <concepts>
#include <cstddef>
#include <type_traits>
#include <utility>

/**
 * @namespace StormByte
 * @brief Root namespace of the StormByte C++ suite.
 */
namespace StormByte {
	/**
	 * @namespace StormByte::Buffer
	 * @brief Buffer module of the StormByte suite.
	 */
	namespace Buffer {
		/**
		 * @namespace StormByte::Buffer::Detail
		 * @brief Private typed queue contracts.
		 */
		namespace Detail {
			/**
			 * @brief Values supported by the nonthrowing, Base-allocated typed queues.
			 * @tparam T Unqualified element with Base SafeValue lifetime guarantees.
			 * @note Extended alignment is rejected because Base's heap has no aligned allocation API.
			 */
			template<typename T>
			concept HopperValue = Type::SafeValue<T> && Type::DefaultConstructible<T> &&
				Type::Movable<T> &&
				(!Type::Const<T>) && (!std::is_volatile_v<T>) &&
				std::is_nothrow_default_constructible_v<T> && std::is_nothrow_move_constructible_v<T> &&
				std::is_nothrow_move_assignable_v<T> && std::is_nothrow_destructible_v<T> &&
				(alignof(T) <= alignof(std::max_align_t));
		}

		/**
		 * @brief Forward declaration of the keyed typed queue.
		 * @tparam T Supported Safe value.
		 */
		template<Detail::HopperValue T>
		class Sink;

		/**
		 * @class Hopper
		 * @brief Single-producer single-consumer (SPSC) typed item queue.
		 *
		 * Hopper is a typed bucket queue for passing items between a single producer
		 * and a single consumer. Unlike byte-oriented FIFOs, Hopper operates on discrete
		 * typed elements.
		 *
		 * Key characteristics:
		 * - Capacity ceiling: Optional capacity ceiling (0 = unbounded). Push waits when full.
		 * - EoF handling: Marking Eof signals end of production; queued items can still be drained via Pop.
		 * - Consumer notification: Points to a consumer condition variable via Notify to signal when
		 *   items or EoF are available.
		 * - Item flow: @c hopper << item and @c item >> hopper enqueue; @c hopper >> item dequeues.
		 * - Query: Size, Capacity, Full, Empty, EoF, Ready, Writers, Front (peek, copy).
		 * - Non-copyable, non-movable: Shared via StormByte::Safe::Shared.
		 *
		 * @tparam T SafeValue with nonthrowing default construction, move construction,
		 * move assignment and destruction, and at most fundamental alignment.
		 * @note Conditional DLL safety requires compatible C++/STL ABI, Base and all
		 * template/provider modules remaining loaded, and the element provider's SafeValue
		 * guarantees. Derived classes must preserve these guarantees; this is not a
		 * certification of arbitrary derived payloads or owners. Private state is
		 * released through its creator callback; shared handle control-block providers
		 * must remain loaded. Ordinary inline is not a provider-locality guarantee.
		 * @note Stop and join all users before destruction. Destruction is not cancellation
		 * of concurrent member calls. Unnotify must complete before a borrowed condition variable dies.
		 * Allocation failure in the nonthrowing queue operations terminates the process.
		 */
		template<Detail::HopperValue T>
		class Hopper {
			public:
				/**
				 * @name Lifecycle
				 * @{
				 */

				/**
				 * @brief Constructs an empty unbounded Hopper.
				 * @throws AllocationError The coordinator, the gate or the signal cannot be allocated.
				 */
				Hopper() noexcept(false);

				/**
				 * @brief Constructs an empty Hopper with a capacity ceiling.
				 * @param capacity Maximum number of items allowed (0 = unbounded).
				 * @throws AllocationError The coordinator, the gate or the signal cannot be allocated.
				 */
				explicit Hopper(StormByte::Size capacity);

				Hopper(const Hopper&) = delete;

				Hopper(Hopper&&) noexcept = delete;

				/**
				 * @brief Destructor. Requires all users, including blocked producers, to be joined.
				 */
				~Hopper() noexcept;

				Hopper& operator=(const Hopper&) = delete;

				Hopper& operator=(Hopper&&) noexcept = delete;

				/**
				 * @}
				 */

				/**
				 * @name Capacity
				 * @{
				 */

				/**
				 * @brief Gets the current capacity ceiling.
				 * @return Maximum items, or 0 if unbounded.
				 */
				StormByte::Size Capacity() const noexcept;

				/**
				 * @brief Sets a new capacity ceiling.
				 * @param capacity Maximum items allowed (0 = unbounded).
				 *
				 * Lowering capacity does not drop queued items; subsequent Push calls wait
				 * until Size falls below the new ceiling.
				 */
				void Capacity(StormByte::Size capacity) noexcept;

				/**
				 * @brief Gets the number of items currently waiting in the bucket.
				 * @return Item count.
				 */
				StormByte::Size Size() const noexcept;

				/**
				 * @brief Checks whether a bounded bucket cannot accept another Push without waiting.
				 * @return true if Capacity > 0 and Size >= Capacity.
				 */
				bool Full() const noexcept;

				/**
				 * @brief Live writers attached to this hopper.
				 * @return Writer count. Last @c CloseWriter sets Eof.
				 */
				unsigned Writers() const noexcept;

				/**
				 * @}
				 */

				/**
				 * @name Producer
				 * @{
				 */

				/**
				 * @brief Enqueues one item and signals the consumer if Notify was configured.
				 * @param item Item to push. Empty smart pointers are discarded.
				 *
				 * If Capacity > 0 and the bucket is full, waits until space becomes available,
				 * or Eof is called. After Eof, Push does not enqueue. Call Eof and join
				 * blocked producers before destroying the Hopper.
				 */
				void Push(T item) noexcept;

				/**
				 * @brief Write: @p item flows into this Hopper.
				 * @param item Unit. Moved. Empty smart pointers are discarded.
				 * @return *this.
				 */
				Hopper& operator<<(T item) noexcept;

				/**
				 * @brief Marks end of production and wakes waiters.
				 *
				 * Does not discard already queued items. After Eof, Push does not enqueue.
				 */
				void Eof() noexcept;

				/**
				 * @}
				 */

				/**
				 * @name Consumer
				 * @{
				 */

				/**
				 * @brief Pops one item from the bucket, or returns default-constructed T if dry.
				 * @return Next item, or default T if empty.
				 *
				 * Does not block. If empty and EoF is false, the consumer waits on its condition variable.
				 * Wakes one producer blocked in Push if space becomes available.
				 */
				T Pop() noexcept;

				/**
				 * @brief Copy of the next item without dequeuing.
				 * @return Front item, or default T if empty.
				 *
				 * Does not block and does not wake producers. Requires T copy-constructible
				 * without throwing. Not a deep copy of a shared payload.
				 */
				T Front() const noexcept requires Type::CopyConstructible<T> && std::is_nothrow_copy_constructible_v<T>;

				/**
				 * @brief Pop one unit from this Hopper into @p item.
				 * @param item Destination. Becomes default T if the bucket is dry.
				 * @return *this.
				 */
				Hopper& operator>>(T& item) noexcept;

				/**
				 * @brief Checks whether Eof was called by a producer.
				 * @return true if Eof was called. Note that queued items may still remain.
				 */
				bool EoF() const noexcept;

				/**
				 * @brief Checks whether the queue has no pending items.
				 * @return true if empty.
				 */
				bool Empty() const noexcept;

				/**
				 * @brief Whether Pop can return an item or production is finished.
				 * @return true if !Empty() or EoF().
				 */
				bool Ready() const noexcept;

				/**
				 * @}
				 */

				/**
				 * @name Notification
				 * @{
				 */

				/**
				 * @brief Registers the consumer condition variable to notify on Push or Eof.
				 * @param wake Consumer condition variable reference. Not owned.
				 *
				 * The referent must outlive this Hopper, or the owner must call
				 * @ref Unnotify before destroying @p wake. Sink wiring shares the Hopper:
				 * a producer Eof after the consumer died is the usual case.
				 * Replacement waits for an active notification; registration must not race
				 * with destruction of either condition variable.
				 * Notification is not a stored event. Callers must coordinate predicate checks,
				 * waits and producer operations with the consumer's wait mutex to avoid lost wakeups.
				 */
				void Notify(Safe::ConditionVariable& wake) noexcept;

				/**
				 * @brief Registers stored Push and Eof notifications alongside the condition variable.
				 * @param wake Borrowed consumer condition variable.
				 * @param generation Borrowed event counter, incremented with release ordering.
				 * @note Load generation with acquire ordering before checking readiness, then
				 * wait on that captured value if not ready and repeat. Control events must
				 * increment and notify the same counter. Both referents must remain alive
				 * until Unnotify returns; replacement synchronizes with active notifications.
				 */
				void Notify(Safe::ConditionVariable& wake, Safe::Atomic<std::size_t>& generation) noexcept;

				/**
				 * @brief Drops both borrowed pointers set by @ref Notify.
				 *
				 * Safe to call more than once or when nothing was registered.
				 * After this, Push and Eof do not signal a consumer condition variable; producers
				 * blocked on a full bucket still wake on the space signal.
				 * Waits for in-flight notifications. Do not concurrently register the
				 * condition variable or counter again while destroying either referent.
				 */
				void Unnotify() noexcept;

				/**
				 * @}
				 */

				/**
				 * @brief Write: lvalue @p item flows into @p hopper.
				 * @param item Unit. Moved. Empty smart pointers are discarded.
				 * @param hopper Destination hopper.
				 * @return @p hopper.
				 *
				 * Namespace declaration required by GCC next to the friend
				 * (GCC will not define a friend-only operator out of line).
				 */
				friend Hopper& operator>>(T& item, Hopper& hopper) noexcept {
					hopper << std::move(item);
					return hopper;
				}

				/**
				 * @brief Write: rvalue @p item flows into @p hopper.
				 * @param item Unit. Empty smart pointers are discarded.
				 * @param hopper Destination hopper.
				 * @return @p hopper.
				 */
				friend Hopper& operator>>(T&& item, Hopper& hopper) noexcept {
					hopper << std::move(item);
					return hopper;
				}

			private:
				/**
				 * @brief Grants Sink access to writer and observer bookkeeping.
				 */
				friend class Sink<T>;

				/**
				 * @brief Adds one live writer.
				 */
				void AddWriter() noexcept;

				/**
				 * @brief Closes one writer; the last writer signals Eof.
				 */
				void CloseWriter() noexcept;

				/**
				 * @brief Registers a borrowed observer for a distinct Sink owner.
				 * @param wake Borrowed condition variable.
				 * @param owner Registration owner identity; never dereferenced.
				 * @param generation Optional borrowed stored-event counter.
				 */
				void Notify(Safe::ConditionVariable& wake, const void* owner, Safe::Atomic<std::size_t>* generation) noexcept;

				/**
				 * @brief Removes an observer only if its Sink owner still matches.
				 * @param owner Registration owner identity; never dereferenced.
				 */
				void Unnotify(const void* owner) noexcept;

				/**
				 * @class Implementation
				 * @brief Private implementation details of Hopper.
				 */
				class Implementation;

				/**
				 * @brief Private coordinator borrowed from m_owner.
				 */
				Implementation* m_io;

				/**
				 * @brief Base-allocated coordinator released by its creator-module callback.
				 */
				StormByte::Safe::Owner m_owner;
		};
	}
}

#include <StormByte/buffer/hopper.txx>

/**
 * @namespace StormByte
 * @brief Root namespace of the StormByte C++ suite.
 */
namespace StormByte {
	/**
	 * @namespace StormByte::Type
	 * @brief Named concepts and small type utilities used across the suite.
	 */
	namespace Type {
		/**
		 * @brief Conditionally admits Hopper with the documented element, ABI and observer contracts.
		 * @tparam T Supported SafeValue; does not certify derived types.
		 * @note Base's registration macro supports exact types only; this family requires a partial specialization.
		 */
		template<Buffer::Detail::HopperValue T>
		struct IsMaybeSafe<Buffer::Hopper<T>>: std::true_type {};
	}
}

