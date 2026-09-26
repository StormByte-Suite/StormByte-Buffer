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

#include <StormByte/buffer/hopper.hxx>
#include <StormByte/size.hxx>
#include <StormByte/type_traits.hxx>

#include <condition_variable>
#include <cstddef>
#include <functional>
#include <memory>
#include <vector>

namespace StormByte::Buffer {
	/**
	 * @class Sink
	 * @brief Set of Hopper buckets keyed by an integer.
	 *
	 * Sink manages a collection of Hopper queues keyed by an integer (representing
	 * channels, tracks, sessions, etc.). Sink does not interpret the integer key.
	 *
	 * Key characteristics:
	 * - Wiring: @c producer >> consumer shares every existing hopper.
	 *   @c producer.To(key) >> consumer creates or shares one key. When this
	 *   Sink already holds that hopper, dest is a co-writer: Eof on this Sink
	 *   then only closes the hopper when the last writer closes.
	 *   @c consumer << producer and @c consumer << producer.To(key) are the
	 *   same edges.
	 * - Drain: Terminal producer flag. Push to an un-wired key discards the item
	 *   without waiting for a consumer.
	 * - Pop: Retrieves items across buckets using Round-Robin, custom Select,
	 *   or a single key (@ref Pop(int)).
	 * - Query: @ref Keys, @ref Contains, per-key Size/Capacity/Full/Empty/EoF/Ready/Front.
	 * - EoF: Closes the Sink and CloseWriter on hoppers this Sink writes.
	 *
	 * @tparam T Item type stored in the hoppers (must be MoveConstructible).
	 */
	template<Type::MoveConstructible T>
	class Sink {
		public:
			/**
			 * @brief Index chooser for Pop selection across multiple buckets.
			 *
			 * Given the count of active buckets, returns an index in range [0, count).
			 */
			using Select = std::function<StormByte::Size(StormByte::Size)>;

			/**
			 * @name Lifecycle
			 * @{
			 */

			/**
			 * @brief Constructs an empty Sink with zero buckets.
			 */
			Sink() noexcept;

			/**
			 * @brief Copy constructor is deleted (Sink is non-copyable).
			 */
			Sink(const Sink&) = delete;

			/**
			 * @brief Move constructor is deleted (Sink is non-movable).
			 */
			Sink(Sink&&) noexcept = delete;

			/**
			 * @brief Destructor. Wakes any threads blocked on wiring.
			 */
			~Sink() noexcept;

			/**
			 * @brief Copy assignment operator is deleted (Sink is non-copyable).
			 */
			Sink& operator=(const Sink&) = delete;

			/**
			 * @brief Move assignment operator is deleted (Sink is non-movable).
			 */
			Sink& operator=(Sink&&) noexcept = delete;

			/**
			 * @}
			 */

			/**
			 * @name Producer
			 * @{
			 */

			/**
			 * @brief Pushes an item into the bucket specified by key.
			 * @param key Integer bucket identifier.
			 * @param item Item to enqueue. Empty smart pointers are discarded.
			 *
			 * Waits until the bucket exists, the Sink is closed, or Drain is set.
			 * If closed or draining and no bucket exists for key, item is discarded.
			 */
			void Push(int key, T item) noexcept;

			/**
			 * @brief Closes the Sink and releases writer holds on its hoppers.
			 *
			 * Hoppers this Sink created (or attached to as a co-writer) decrement
			 * their writer count. A hopper shared by several producers Eofs when
			 * the last writer closes. Idempotent.
			 *
			 * Wakes Push and Pop waiters.
			 */
			void Eof() noexcept;

			/**
			 * @}
			 */

			/**
			 * @name Wiring & Connection
			 * @{
			 */

			/**
			 * @class Lane
			 * @brief One-key redirect: @c from.To(key) >> dest.
			 */
			class Lane {
				public:
					/**
					 * @brief Wire this key onto @p dest.
					 * @param dest Consumer (or extra writer) Sink.
					 * @return @p dest.
					 *
					 * This Sink has no hopper yet: this is the writer, dest is
					 * the reader. This Sink already has the hopper: dest is a
					 * co-writer.
					 */
					Sink& operator>>(Sink& dest) noexcept;

				private:
					friend class Sink;
					Lane(Sink& from, int key) noexcept;
					Sink* m_from;
					int m_key;
			};

			/**
			 * @brief Redirect of one hopper key.
			 * @param key Bucket key.
			 * @return Lane for @c >> dest.
			 */
			Lane To(int key) noexcept;

			/**
			 * @brief Share every existing hopper with @p dest.
			 * @param dest Consumer Sink.
			 * @return @p dest.
			 *
			 * Zero buckets: no-op. Does not create keys.
			 */
			Sink& operator>>(Sink& dest) noexcept;

			/**
			 * @brief Same as @p src >> *this (all hoppers).
			 * @param src Producer Sink.
			 * @return *this.
			 */
			Sink& operator<<(Sink& src) noexcept;

			/**
			 * @brief Same as @p lane >> *this.
			 * @param lane @ref To result from the producer.
			 * @return *this.
			 */
			Sink& operator<<(Lane lane) noexcept;

			/**
			 * @brief Marks Sink as a terminal producer (un-wired Push calls drop instead of waiting).
			 */
			void Drain() noexcept;

			/**
			 * @brief Checks whether Drain was called.
			 * @return true if Drain was set.
			 */
			bool Draining() const noexcept;

			/**
			 * @brief Registers consumer condition variable to notify on all current and future hoppers.
			 * @param consumer Consumer condition variable reference. Not owned.
			 */
			void Notify(std::condition_variable& consumer) noexcept;

			/**
			 * @brief Clears @ref Notify on this Sink and on every hopper.
			 *
			 * Call after Eof when the consumer condition variable is about
			 * to die. Later Eof from a co-writer will not signal it.
			 */
			void Unnotify() noexcept;

			/**
			 * @}
			 */

			/**
			 * @name Bucket Capacity & Query
			 * @{
			 */

			/**
			 * @brief Keys of hoppers currently wired on this Sink.
			 * @return Keys in ascending order. Empty if none.
			 *
			 * Snapshot. Does not create buckets. Sink does not interpret keys.
			 */
			std::vector<int> Keys() const noexcept;

			/**
			 * @brief Number of hoppers wired on this Sink.
			 * @return @ref Keys size.
			 */
			StormByte::Size Buckets() const noexcept;

			/**
			 * @brief Whether hopper @p key exists on this Sink.
			 * @param key Bucket key identifier.
			 * @return true if the key is wired.
			 */
			bool Contains(int key) const noexcept;

			/**
			 * @brief Gets capacity ceiling of bucket key.
			 * @param key Bucket key identifier.
			 * @return Hopper capacity, or 0 if key bucket does not exist.
			 */
			StormByte::Size Capacity(int key) const noexcept;

			/**
			 * @brief Sets capacity ceiling of bucket key.
			 * @param key Bucket key identifier.
			 * @param capacity Maximum items allowed (0 = unbounded).
			 */
			void Capacity(int key, StormByte::Size capacity) noexcept;

			/**
			 * @brief Gets pending item count in bucket key.
			 * @param key Bucket key identifier.
			 * @return Item count, or 0 if key bucket does not exist.
			 */
			StormByte::Size Size(int key) const noexcept;

			/**
			 * @brief Checks if bucket key is full.
			 * @param key Bucket key identifier.
			 * @return true if full, or false if key does not exist.
			 */
			bool Full(int key) const noexcept;

			/**
			 * @brief Whether bucket @p key has no pending items.
			 * @param key Bucket key identifier.
			 * @return true if the key is missing or the hopper is empty.
			 */
			bool Empty(int key) const noexcept;

			/**
			 * @brief Whether producers marked Eof on bucket @p key.
			 * @param key Bucket key identifier.
			 * @return Hopper EoF, or false if the key does not exist.
			 */
			bool EoF(int key) const noexcept;

			/**
			 * @brief Whether @ref Pop(int) on @p key can return an item or that hopper is finished.
			 * @param key Bucket key identifier.
			 * @return true if the hopper has an item or is EoF. false if missing.
			 */
			bool Ready(int key) const noexcept;

			/**
			 * @brief Copy of the next item in bucket @p key without dequeuing.
			 * @param key Bucket key identifier.
			 * @return Front item, or default T if the key is missing or the hopper is empty.
			 *
			 * Does not block and does not wake producers. Requires
			 * @ref Type::CopyConstructible. Not a deep copy of the payload.
			 * Does not read any other key.
			 */
			T Front(int key) const noexcept requires Type::CopyConstructible<T>;

			/**
			 * @}
			 */

			/**
			 * @name Consumer
			 * @{
			 */

			/**
			 * @brief Pops one item from the Sink using default round-robin order.
			 * @return Next item, or default T if empty/closed.
			 *
			 * Blocks while zero buckets exist and Sink is not closed.
			 */
			T Pop() noexcept;

			/**
			 * @brief Pops one item from the Sink using a custom selection function.
			 * @param select Bucket index selection callback.
			 * @return Next item, or default T if empty/closed.
			 *
			 * Blocks while zero buckets exist and Sink is not closed.
			 */
			T Pop(const Select& select) noexcept;

			/**
			 * @brief Pops one item from bucket @p key only.
			 * @param key Bucket key identifier.
			 * @return Next item of that hopper, or default T if the key is
			 *         missing, the hopper is empty, or the Sink is closed.
			 *
			 * Waits until the key exists or the Sink is closed. Does not
			 * wait for an item: an empty hopper returns default T (same as
			 * @ref Hopper::Pop). Does not read any other key. Sink does not
			 * interpret @p key.
			 */
			T Pop(int key) noexcept;

			/**
			 * @brief Checks if the Sink is finished.
			 * @return true if no items remain and no new items can arrive in the current hoppers.
			 *
			 * Contract details for EoF():
			 * - With zero hoppers: EoF() returns true only if this Sink is closed (via Eof() or destruction).
			 * - With hoppers: EoF() returns true when all hoppers are empty and Hopper::EoF() is true,
			 *   even if this Sink itself did not call Eof() (because wiring shares the Hopper and the
			 *   producer closed it from the other Sink).
			 * - Wiring a new key after EoF() returned true may cause EoF() to return false again
			 *   if new work becomes available.
			 * - EoF() does not mean "this object called Eof()", but "no items remain and none can enter current buckets".
			 */
			bool EoF() const noexcept;

			/**
			 * @brief Checks whether a Pop call can return immediately (item available or EoF).
			 * @return true if any hopper has an item or if Sink is EoF.
			 */
			bool Ready() const noexcept;

			/**
			 * @}
			 */

		private:
			/**
			 * @class Implementation
			 * @brief Private implementation class for Sink.
			 */
			class Implementation;

			std::unique_ptr<Implementation> m_io;	///< Private coordinator.
	};
}

#include <StormByte/buffer/sink.txx>
