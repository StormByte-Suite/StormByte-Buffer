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

#include <StormByte/safe/heap.hxx>
#include <StormByte/safe/map.hxx>
#include <StormByte/safe/mutex.hxx>
#include <StormByte/safe/set.hxx>
#include <StormByte/safe/unique_lock.hxx>
#include <StormByte/safe/vector.hxx>
#include <StormByte/type_traits.hxx>

#include <cstddef>
#include <functional>
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
		 * @class Sink<T>::Implementation
		 * @brief Internal implementation class for Sink.
		 *
		 * Manages the map of key-to-Hopper buckets, thread synchronization,
		 * wiring condition variables, and pop selection algorithms. The gate,
		 * the signal and the words live on Base's heap.
		 */
		template<Detail::HopperValue T>
		class Sink<T>::Implementation {
			/**
			 * @brief Base-owned shared hopper.
			 */
			using HopperOwner = StormByte::Safe::Shared<Hopper<T>>;
			/**
			 * @brief Hopper snapshots. Position is the round-robin order.
			 */
			using HopperOwners = StormByte::Safe::Vector<HopperOwner>;
			/**
			 * @brief Keyed hoppers. Iteration order is the ascending key order published by Keys.
			 */
			using HopperBuckets = StormByte::Safe::Map<int, HopperOwner>;
			/**
			 * @brief Hoppers this Sink writes, ordered by hopper identity.
			 */
			using HopperWriters = StormByte::Safe::Set<HopperOwner>;

			public:
				/**
				 * @brief Constructs the Sink Implementation instance.
				 * @throws AllocationError The gate, the signal or a word cannot be allocated.
				 */
				Implementation()
				: m_rr(std::size_t{0}), m_consumer(nullptr), m_generation(nullptr), m_closed(false), m_drain(false) {}

				/**
				 * @brief Removes borrowed observers, marks closed and wakes wiring waiters.
				 */
				~Implementation() noexcept {
					Unnotify();
					m_closed.store(true, Safe::MemoryOrder::Release);
					m_wired.notify_all();
				}

				/**
				 * @brief Enqueues an item into the hopper for key.
				 * @param key Bucket key identifier.
				 * @param item Item to push.
				 */
				void Push(int key, T item) noexcept {
					if constexpr (Type::NullablePointer<T>) {
						if (!item)
							return;
					}
					HopperOwner hopper;
					{
						Safe::UniqueLock lock(m_mutex);
						m_wired.wait(lock, [this, key] {
							return m_closed.load(Safe::MemoryOrder::Acquire)
								|| m_drain.load(Safe::MemoryOrder::Acquire)
								|| m_buckets.find(key) != m_buckets.end();
						});
						auto found = m_buckets.find(key);
						if (m_closed.load(Safe::MemoryOrder::Acquire) || found == m_buckets.end())
							return;
						hopper = (*found).second;
					}
					if (!hopper)
						return;
					hopper->Push(std::move(item));
				}

				/**
				 * @brief Closes this Sink and returns hoppers it writes, for last-writer Eof.
				 * @note Notifies the borrowed consumer while holding its registration lock.
				 * @return Writer hoppers to CloseWriter, each returned exactly once even after closed wiring.
				 */
				HopperOwners Close() noexcept {
					HopperOwners writers;
					{
						Safe::UniqueLock lock(m_mutex);
						m_closed.store(true, Safe::MemoryOrder::Release);
						writers.reserve(m_writers.size());
						for (auto it = m_writers.begin(); it != m_writers.end(); ++it)
							writers.push_back(*it);
						m_writers.clear();
						m_wired.notify_all();
						SignalConsumer();
					}
					return writers;
				}

				/**
				 * @brief Shares all existing hoppers with consumer.
				 * @param consumer Consumer Sink implementation reference.
				 */
				void Bind(Implementation& consumer) {
					if (this == &consumer)
						return;
					Safe::UniqueLock ours;
					Safe::UniqueLock theirs;
					LockPair(m_mutex, consumer.m_mutex, ours, theirs);
					const bool closed = m_closed.load(Safe::MemoryOrder::Acquire)
						|| consumer.m_closed.load(Safe::MemoryOrder::Acquire);
					Safe::ConditionVariable* cv = consumer.m_consumer;
					for (auto it = m_buckets.begin(); it != m_buckets.end(); ++it) {
						consumer.RemoveObserver((*it).first, (*it).second);
						if (closed)
							(*it).second->Eof();
						if (cv != nullptr)
							(*it).second->Notify(*cv, &consumer, consumer.m_generation);
						consumer.m_buckets[(*it).first] = (*it).second;
					}
					if (closed)
						consumer.m_closed.store(true, Safe::MemoryOrder::Release);
					consumer.RebuildOrder();
					consumer.m_wired.notify_all();
					consumer.SignalConsumer();
					m_wired.notify_all();
				}

				/**
				 * @brief Creates or shares the hopper for key with consumer.
				 * @param key Bucket key.
				 * @param consumer Consumer Sink implementation reference.
				 * @note Adds any extra writer while both Sink registration locks are held.
				 */
				void Bind(int key, Implementation& consumer) {
					if (this == &consumer)
						return;
					Safe::UniqueLock ours;
					Safe::UniqueLock theirs;
					LockPair(m_mutex, consumer.m_mutex, ours, theirs);
					const bool closed = m_closed.load(Safe::MemoryOrder::Acquire)
						|| consumer.m_closed.load(Safe::MemoryOrder::Acquire);
					Safe::ConditionVariable* cv = consumer.m_consumer;
					const bool existed = m_buckets.contains(key);
					auto hopper = Ensure(key);
					const bool already_writer = existed && consumer.m_writers.contains(hopper);
					if (existed && !already_writer && !closed) {
						consumer.m_writers.insert(hopper);
						hopper->AddWriter();
					}
					consumer.RemoveObserver(key, hopper);
					if (closed)
						hopper->Eof();
					if (cv != nullptr)
						hopper->Notify(*cv, &consumer, consumer.m_generation);
					consumer.m_buckets[key] = hopper;
					if (closed)
						consumer.m_closed.store(true, Safe::MemoryOrder::Release);
					consumer.RebuildOrder();
					consumer.m_wired.notify_all();
					consumer.SignalConsumer();
					m_wired.notify_all();
				}

				/**
				 * @brief Sets Drain mode.
				 */
				void Drain() noexcept {
					{
						Safe::UniqueLock lock(m_mutex);
						m_drain.store(true, Safe::MemoryOrder::Release);
					}
					m_wired.notify_all();
				}

				/**
				 * @brief Checks if Drain was set.
				 * @return true if draining.
				 */
				bool Draining() const noexcept {
					return m_drain.load(Safe::MemoryOrder::Acquire);
				}

				/**
				 * @brief Registers condition variable for consumer notifications.
				 * @param consumer Condition variable reference.
				 * @param generation Optional borrowed stored-event counter.
				 */
				void Notify(Safe::ConditionVariable& consumer, Safe::Atomic<std::size_t>* generation = nullptr) noexcept {
					Safe::UniqueLock lock(m_mutex);
					m_consumer = &consumer;
					m_generation = generation;
					for (auto it = m_order.begin(); it != m_order.end(); ++it)
						(*it)->Notify(consumer, this, generation);
				}

				/**
				 * @brief Drops the consumer condition variable on this Sink and its hoppers.
				 */
				void Unnotify() noexcept {
					Safe::UniqueLock lock(m_mutex);
					m_consumer = nullptr;
					m_generation = nullptr;
					for (auto it = m_order.begin(); it != m_order.end(); ++it) {
						if (*it)
							(*it)->Unnotify(this);
					}
				}

				/**
				 * @brief Snapshot of wired keys in map order.
				 * @return Keys, empty if none.
				 */
				StormByte::Safe::Vector<int> Keys() const noexcept {
					Safe::UniqueLock lock(m_mutex);
					StormByte::Safe::Vector<int> keys;
					keys.reserve(m_buckets.size());
					for (auto it = m_buckets.begin(); it != m_buckets.end(); ++it)
						keys.push_back((*it).first);
					return keys;
				}

				/**
				 * @brief Number of wired hoppers.
				 * @return Bucket count.
				 */
				StormByte::Size Buckets() const noexcept {
					Safe::UniqueLock lock(m_mutex);
					return StormByte::Size{m_buckets.size()};
				}

				/**
				 * @brief Whether key is wired.
				 * @param key Bucket key.
				 * @return true if present.
				 */
				bool Contains(int key) const noexcept {
					return static_cast<bool>(Bucket(key));
				}

				/**
				 * @brief Gets capacity of key hopper.
				 * @param key Bucket key.
				 * @return Capacity value.
				 */
				StormByte::Size Capacity(int key) const noexcept {
					const auto hopper = Bucket(key);
					if (!hopper)
						return StormByte::Size{0};
					return hopper->Capacity();
				}

				/**
				 * @brief Sets capacity of key hopper.
				 * @param key Bucket key.
				 * @param capacity New capacity.
				 */
				void Capacity(int key, StormByte::Size capacity) noexcept {
					Safe::UniqueLock lock(m_mutex);
					auto found = m_buckets.find(key);
					if (found != m_buckets.end() && (*found).second)
						(*found).second->Capacity(capacity);
				}

				/**
				 * @brief Gets pending item count of key hopper.
				 * @param key Bucket key.
				 * @return Item count.
				 */
				StormByte::Size Size(int key) const noexcept {
					const auto hopper = Bucket(key);
					if (!hopper)
						return StormByte::Size{0};
					return hopper->Size();
				}

				/**
				 * @brief Checks if key hopper is full.
				 * @param key Bucket key.
				 * @return true if full.
				 */
				bool Full(int key) const noexcept {
					const auto hopper = Bucket(key);
					if (!hopper)
						return false;
					return hopper->Full();
				}

				/**
				 * @brief Whether key hopper has no items.
				 * @param key Bucket key.
				 * @return true if missing or empty.
				 */
				bool Empty(int key) const noexcept {
					const auto hopper = Bucket(key);
					if (!hopper)
						return true;
					return hopper->Empty();
				}

				/**
				 * @brief Whether producers marked Eof on key hopper.
				 * @param key Bucket key.
				 * @return Hopper EoF, or false if missing.
				 */
				bool EoF(int key) const noexcept {
					const auto hopper = Bucket(key);
					if (!hopper)
						return false;
					return hopper->EoF();
				}

				/**
				 * @brief Whether key hopper has an item or is finished.
				 * @param key Bucket key.
				 * @return false if missing.
				 */
				bool Ready(int key) const noexcept {
					const auto hopper = Bucket(key);
					if (!hopper)
						return false;
					return !hopper->Empty() || hopper->EoF();
				}

				/**
				 * @brief Copy of front item of key hopper. Does not dequeue.
				 * @param key Bucket key.
				 * @return Front or default T.
				 */
				T Front(int key) const noexcept requires Type::CopyConstructible<T> && std::is_nothrow_copy_constructible_v<T> {
					const auto hopper = Bucket(key);
					if (!hopper)
						return T{};
					return hopper->Front();
				}

				/**
				 * @brief Pops item using default selection.
				 * @return Popped item or default T.
				 */
				T Pop() noexcept {
					return Pop(nullptr);
				}

				/**
				 * @brief Pops item using specified selection function.
				 * @param select Borrowed bucket index chooser, or null for round-robin.
				 * @return Popped item or default T.
				 */
				T Pop(const typename Sink<T>::Select* select) noexcept {
					return Pop([select](StormByte::Size& output, StormByte::Size count) {
						return select ? select->Call(output, count) : StormByte::Safe::Status::Success;
					}, select == nullptr);
				}

				/**
				 * @brief Pops using a synchronous chooser without retaining or copying its captures.
				 * @tparam Selector Callable writing a bucket index and returning a Safe status.
				 * @param select Borrowed chooser, invoked before any item is removed.
				 * @param round_robin Whether to use the default order instead of invoking the chooser.
				 * @return Popped item, or default T on failure, exception or empty buckets.
				 */
				template<typename Selector>
				T Pop(Selector&& select, bool round_robin) noexcept {
					HopperOwners hoppers;
					{
						Safe::UniqueLock lock(m_mutex);
						m_wired.wait(lock, [this] {
							return m_closed.load(Safe::MemoryOrder::Acquire) || !m_order.empty();
						});
						hoppers = m_order;
					}
					if (hoppers.empty())
						return T{};
					const std::size_t count = static_cast<std::size_t>(hoppers.size());
					std::size_t start = 0;
					if (!round_robin) {
						try {
							StormByte::Size output{0};
							if (std::invoke(select, output, StormByte::Size{count}) != StormByte::Safe::Status::Success)
								return T{};
							start = static_cast<std::size_t>(output) % count;
						}
						catch (...) {
							return T{};
						}
					}
					else {
						start = m_rr.fetch_add(std::size_t{1}, Safe::MemoryOrder::Relaxed) % count;
					}

					for (std::size_t offset = 0; offset < count; ++offset) {
						const auto& hopper = hoppers[(start + offset) % count];
						if (!hopper->Empty())
							return hopper->Pop();
					}
					return T{};
				}

				/**
				 * @brief Pops from one key only.
				 * @param key Bucket key.
				 * @return Item or default T.
				 */
				T Pop(int key) noexcept {
					HopperOwner hopper;
					{
						Safe::UniqueLock lock(m_mutex);
						m_wired.wait(lock, [this, key] {
							return m_closed.load(Safe::MemoryOrder::Acquire)
								|| m_buckets.contains(key);
						});
						auto found = m_buckets.find(key);
						if (found == m_buckets.end() || !(*found).second)
							return T{};
						hopper = (*found).second;
					}
					return hopper->Pop();
				}

				/**
				 * @brief Checks if Sink is finished.
				 * @return true if closed and all hoppers drained.
				 */
				bool EoF() const noexcept {
					const auto hoppers = Order();
					if (hoppers.empty())
						return m_closed.load(Safe::MemoryOrder::Acquire);
					for (auto it = hoppers.begin(); it != hoppers.end(); ++it) {
						if (!(*it)->EoF())
							return false;
						if (!(*it)->Empty())
							return false;
					}
					return true;
				}

				/**
				 * @brief Checks if Pop can return immediately.
				 * @return true if item is ready or EoF reached.
				 */
				bool Ready() const noexcept {
					const auto hoppers = Order();
					if (hoppers.empty())
						return m_closed.load(Safe::MemoryOrder::Acquire);
					bool drained = true;
					for (auto it = hoppers.begin(); it != hoppers.end(); ++it) {
						if (!(*it)->Empty())
							return true;
						if (!(*it)->EoF())
							drained = false;
					}
					return drained;
				}

			private:
				/**
				 * @brief Locks two gates in address order.
				 * @param left First gate.
				 * @param right Second gate.
				 * @param first Lock that owns the lower address.
				 * @param second Lock that owns the higher address.
				 */
				static void LockPair(Safe::Mutex& left, Safe::Mutex& right, Safe::UniqueLock& first, Safe::UniqueLock& second) {
					if (&left < &right) {
						first = Safe::UniqueLock(left);
						second = Safe::UniqueLock(right);
						return;
					}
					first = Safe::UniqueLock(right);
					second = Safe::UniqueLock(left);
				}

				/**
				 * @brief Publishes a consumer event after state changes. Caller holds m_mutex.
				 */
				void SignalConsumer() noexcept {
					if (m_generation != nullptr) {
						m_generation->fetch_add(std::size_t{1}, Safe::MemoryOrder::Release);
						m_generation->notify_all();
					}
					if (m_consumer != nullptr)
						m_consumer->notify_all();
				}

				/**
				 * @brief Removes this registration from a replaced hopper. Caller holds m_mutex.
				 * @param key Bucket being rebound.
				 * @param replacement New hopper whose observer must not be removed.
				 */
				void RemoveObserver(int key, const HopperOwner& replacement) noexcept {
					const auto found = m_buckets.find(key);
					if (found == m_buckets.end() || (*found).second == replacement)
						return;
					for (auto it = m_buckets.begin(); it != m_buckets.end(); ++it) {
						if ((*it).first != key && (*it).second == (*found).second)
							return;
					}
					(*found).second->Unnotify(this);
				}

				/**
				 * @brief Ensures hopper for key exists. Caller holds m_mutex.
				 * @param key Bucket key.
				 * @return Shared hopper instance.
				 */
				HopperOwner Ensure(int key) {
					auto found = m_buckets.find(key);
					if (found != m_buckets.end())
						return (*found).second;
					auto hopper = HopperOwner::template MakePointer<Hopper<T>>();
					Safe::ConditionVariable* cv = m_consumer;
					if (cv != nullptr)
						hopper->Notify(*cv, this, m_generation);
					m_buckets.emplace(key, hopper);
					m_writers.insert(hopper);
					RebuildOrder();
					return hopper;
				}

				/**
				 * @brief Rebuilds order vector from m_buckets. Caller holds m_mutex.
				 */
				void RebuildOrder() {
					m_order.clear();
					m_order.reserve(m_buckets.size());
					for (auto it = m_buckets.begin(); it != m_buckets.end(); ++it)
						m_order.push_back((*it).second);
				}

				/**
				 * @brief Returns snapshot of current hoppers in order.
				 * @return Vector of hoppers.
				 */
				HopperOwners Order() const {
					Safe::UniqueLock lock(m_mutex);
					return m_order;
				}

				/**
				 * @brief Retrieves hopper for key.
				 * @param key Bucket key.
				 * @return Hopper pointer or nullptr.
				 */
				HopperOwner Bucket(int key) const {
					Safe::UniqueLock lock(m_mutex);
					auto found = m_buckets.find(key);
					if (found == m_buckets.end())
						return nullptr;
					return (*found).second;
				}

				/**
				 * @brief Guards buckets, order, observer registration and direct notification.
				 */
				mutable Safe::Mutex m_mutex;

				/**
				 * @brief Waits for bucket binding or closure.
				 */
				Safe::ConditionVariable m_wired;

				/**
				 * @brief Base-owned map of keys to Base-owned hoppers.
				 */
				HopperBuckets m_buckets;

				/**
				 * @brief Base-owned set of hoppers this Sink writes.
				 */
				HopperWriters m_writers;

				/**
				 * @brief Base-owned hopper order for Pop.
				 */
				HopperOwners m_order;

				/**
				 * @brief Round-robin counter.
				 */
				Safe::Atomic<std::size_t> m_rr;

				/**
				 * @brief Borrowed consumer condition variable guarded by m_mutex.
				 */
				Safe::ConditionVariable* m_consumer;

				/**
				 * @brief Borrowed event counter guarded by m_mutex.
				 */
				Safe::Atomic<std::size_t>* m_generation;

				/**
				 * @brief Closed flag.
				 */
				Safe::Atomic<bool> m_closed;

				/**
				 * @brief Drain mode flag.
				 */
				Safe::Atomic<bool> m_drain;
		};

		template<Detail::HopperValue T>
		Sink<T>::Sink()
		: m_io(::new (StormByte::Safe::Heap::Allocate(sizeof(Implementation))) Implementation()),
		m_owner(m_io, nullptr, [](void* context) noexcept {
			static_cast<Implementation*>(context)->~Implementation();
			StormByte::Safe::Heap::Free(context);
		}) {
			static_assert(alignof(Implementation) <= alignof(std::max_align_t));
		}

		template<Detail::HopperValue T>
		Sink<T>::~Sink() noexcept {
			Unnotify();
			Eof();
		}

		template<Detail::HopperValue T>
		void Sink<T>::Push(int key, T item) noexcept {
			m_io->Push(key, std::move(item));
		}

		template<Detail::HopperValue T>
		void Sink<T>::Eof() noexcept {
			const auto writers = m_io->Close();
			for (auto it = writers.begin(); it != writers.end(); ++it)
				(*it)->CloseWriter();
		}

		template<Detail::HopperValue T>
		Sink<T>::Lane::Lane(Sink& from, int key) noexcept
		: m_from(&from), m_key(key) {}

		template<Detail::HopperValue T>
		typename Sink<T>::Lane Sink<T>::To(int key) noexcept {
			return Lane(*this, key);
		}

		template<Detail::HopperValue T>
		Sink<T>& Sink<T>::Lane::operator>>(Sink& dest) noexcept {
			m_from->m_io->Bind(m_key, *dest.m_io);
			return dest;
		}

		template<Detail::HopperValue T>
		Sink<T>& Sink<T>::operator>>(Sink& dest) noexcept {
			m_io->Bind(*dest.m_io);
			return dest;
		}

		template<Detail::HopperValue T>
		Sink<T>& Sink<T>::operator<<(Sink& src) noexcept {
			src >> *this;
			return *this;
		}

		template<Detail::HopperValue T>
		Sink<T>& Sink<T>::operator<<(Lane lane) noexcept {
			lane >> *this;
			return *this;
		}

		template<Detail::HopperValue T>
		void Sink<T>::Drain() noexcept {
			m_io->Drain();
		}

		template<Detail::HopperValue T>
		bool Sink<T>::Draining() const noexcept {
			return m_io->Draining();
		}

		template<Detail::HopperValue T>
		void Sink<T>::Notify(Safe::ConditionVariable& consumer) noexcept {
			m_io->Notify(consumer);
		}

		template<Detail::HopperValue T>
		void Sink<T>::Notify(Safe::ConditionVariable& consumer, Safe::Atomic<std::size_t>& generation) noexcept {
			m_io->Notify(consumer, &generation);
		}

		template<Detail::HopperValue T>
		void Sink<T>::Unnotify() noexcept {
			m_io->Unnotify();
		}

		template<Detail::HopperValue T>
		StormByte::Safe::Vector<int> Sink<T>::Keys() const noexcept {
			return m_io->Keys();
		}

		template<Detail::HopperValue T>
		StormByte::Size Sink<T>::Buckets() const noexcept {
			return m_io->Buckets();
		}

		template<Detail::HopperValue T>
		bool Sink<T>::Contains(int key) const noexcept {
			return m_io->Contains(key);
		}

		template<Detail::HopperValue T>
		StormByte::Size Sink<T>::Capacity(int key) const noexcept {
			return m_io->Capacity(key);
		}

		template<Detail::HopperValue T>
		void Sink<T>::Capacity(int key, StormByte::Size capacity) noexcept {
			m_io->Capacity(key, capacity);
		}

		template<Detail::HopperValue T>
		StormByte::Size Sink<T>::Size(int key) const noexcept {
			return m_io->Size(key);
		}

		template<Detail::HopperValue T>
		bool Sink<T>::Full(int key) const noexcept {
			return m_io->Full(key);
		}

		template<Detail::HopperValue T>
		bool Sink<T>::Empty(int key) const noexcept {
			return m_io->Empty(key);
		}

		template<Detail::HopperValue T>
		bool Sink<T>::EoF(int key) const noexcept {
			return m_io->EoF(key);
		}

		template<Detail::HopperValue T>
		bool Sink<T>::Ready(int key) const noexcept {
			return m_io->Ready(key);
		}

		template<Detail::HopperValue T>
		T Sink<T>::Front(int key) const noexcept requires Type::CopyConstructible<T> && std::is_nothrow_copy_constructible_v<T> {
			return m_io->Front(key);
		}

		template<Detail::HopperValue T>
		T Sink<T>::Pop() noexcept {
			return m_io->Pop();
		}

		template<Detail::HopperValue T>
		T Sink<T>::Pop(const Select& select) noexcept {
			return m_io->Pop(&select);
		}

		template<Detail::HopperValue T>
		template<typename Selector>
		requires (!Type::SameAs<std::remove_cvref_t<Selector>, typename Sink<T>::Select>) &&
			requires(Selector& select, StormByte::Size count) {
				{ std::invoke(select, count) } -> Type::SameAs<StormByte::Size>;
			}
		T Sink<T>::Pop(Selector&& select) noexcept {
			return m_io->Pop([&select](StormByte::Size& output, StormByte::Size count) {
					output = std::invoke(select, count);
					return StormByte::Safe::Status::Success;
				}, false);
		}

		template<Detail::HopperValue T>
		T Sink<T>::Pop(int key) noexcept {
			return m_io->Pop(key);
		}

		template<Detail::HopperValue T>
		bool Sink<T>::EoF() const noexcept {
			return m_io->EoF();
		}

		template<Detail::HopperValue T>
		bool Sink<T>::Ready() const noexcept {
			return m_io->Ready();
		}
	}
}
