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

#include <StormByte/type_traits.hxx>

#include <atomic>
#include <concepts>
#include <condition_variable>
#include <cstddef>
#include <deque>
#include <memory>
#include <mutex>
#include <queue>
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
		 * @class Hopper<T>::Implementation
		 * @brief Internal implementation of Hopper queue details.
		 *
		 * Handles mutex-protected queue operations, atomic capacity settings,
		 * condition variable notifications, and EoF flags.
		 */
		template<Detail::HopperValue T>
		class Hopper<T>::Implementation {
			/**
			 * @brief Queue storage allocated and freed exclusively on Base's heap.
			 */
			using ItemAllocator = StormByte::Safe::Heap::Allocator<T>;

			public:
				/**
				 * @brief Constructs an unbounded Implementation instance.
				 */
				STORMBYTE_FORCE_INLINE Implementation() noexcept
					: m_eof(false), m_wake(nullptr), m_observer(nullptr), m_cap(0), m_writers(1) {}

				/**
				 * @brief Constructs a bounded Implementation instance.
				 * @param capacity Maximum items allowed.
				 */
				STORMBYTE_FORCE_INLINE explicit Implementation(StormByte::Size capacity) noexcept
					: m_eof(false), m_wake(nullptr), m_observer(nullptr), m_cap(static_cast<std::size_t>(capacity)), m_writers(1) {}

				/**
				 * @brief Destructor. Marks EoF and wakes waiting producers.
				 */
				STORMBYTE_FORCE_INLINE ~Implementation() noexcept {
					m_eof.store(true, std::memory_order_release);
					m_space.notify_all();
				}

				/**
				 * @brief Gets capacity ceiling.
				 * @return Capacity value.
				 */
				StormByte::Size Capacity() const noexcept {
					return StormByte::Size{m_cap.load(std::memory_order_acquire)};
				}

				/**
				 * @brief Sets capacity ceiling.
				 * @param capacity New capacity value.
				 */
				void Capacity(StormByte::Size capacity) noexcept {
					{
						std::lock_guard<std::mutex> lock(m_mutex);
						m_cap.store(static_cast<std::size_t>(capacity), std::memory_order_release);
					}
					m_space.notify_all();
				}

				/**
				 * @brief Gets item count in queue.
				 * @return Count of items.
				 */
				StormByte::Size Size() const noexcept {
					std::lock_guard<std::mutex> lock(m_mutex);
					return StormByte::Size{m_items.size()};
				}

				/**
				 * @brief Checks if bounded bucket is full.
				 * @return true if capacity > 0 and size >= capacity.
				 */
				bool Full() const noexcept {
					const std::size_t cap = m_cap.load(std::memory_order_acquire);
					if (cap == 0)
						return false;
					return Size() >= cap;
				}

				/**
				 * @brief Live writer count.
				 * @return Writers still open.
				 */
				unsigned Writers() const noexcept {
					return m_writers.load(std::memory_order_acquire);
				}

				/**
				 * @brief Enqueues an item, waiting if full.
				 * @param item Item to enqueue.
				 */
				void Push(T item) noexcept {
					if constexpr (Type::NullablePointer<T>) {
						if (!item)
							return;
					}
					{
						std::unique_lock<std::mutex> lock(m_mutex);
						m_space.wait(lock, [this]() {
							const std::size_t cap = m_cap.load(std::memory_order_acquire);
							return cap == 0
								|| m_items.size() < cap
								|| m_eof.load(std::memory_order_acquire);
						});
						if (m_eof.load(std::memory_order_acquire))
							return;
						m_items.push(std::move(item));
					}
					SignalConsumer();
				}

				/**
				 * @brief Signals end of production.
				 */
				void Eof() noexcept {
					{
						std::lock_guard<std::mutex> lock(m_mutex);
						m_eof.store(true, std::memory_order_release);
					}
					SignalConsumer();
					m_space.notify_all();
				}

				/**
				 * @brief Registers an extra writer.
				 */
				void AddWriter() noexcept {
					m_writers.fetch_add(1, std::memory_order_acq_rel);
				}

				/**
				 * @brief Releases one writer. Last writer force-closes.
				 */
				void CloseWriter() noexcept {
					unsigned prev = m_writers.load(std::memory_order_acquire);
					while (prev > 0) {
						if (m_writers.compare_exchange_weak(prev, prev - 1,
								std::memory_order_acq_rel, std::memory_order_acquire)) {
							if (prev == 1)
								Eof();
							return;
						}
					}
				}

				/**
				 * @brief Pops next item from queue without waiting.
				 * @return Next item, or default T if empty.
				 */
				T Pop() noexcept {
					T item{};
					{
						std::lock_guard<std::mutex> lock(m_mutex);
						if (m_items.empty())
							return T{};
						item = std::move(m_items.front());
						m_items.pop();
					}
					m_space.notify_one();
					return item;
				}

				/**
				 * @brief Copy of front item. Does not dequeue.
				 * @return Front or default T.
				 */
				T Front() const noexcept requires Type::CopyConstructible<T> && std::is_nothrow_copy_constructible_v<T> {
					std::lock_guard<std::mutex> lock(m_mutex);
					if (m_items.empty())
						return T{};
					return m_items.front();
				}

				/**
				 * @brief Checks if Eof was signaled.
				 * @return true if Eof set.
				 */
				bool EoF() const noexcept {
					return m_eof.load(std::memory_order_acquire);
				}

				/**
				 * @brief Checks if queue is empty.
				 * @return true if empty.
				 */
				bool Empty() const noexcept {
					std::lock_guard<std::mutex> lock(m_mutex);
					return m_items.empty();
				}

				/**
				 * @brief Item ready or production finished.
				 * @return true if !Empty() or EoF().
				 */
				bool Ready() const noexcept {
					return !Empty() || EoF();
				}

				/**
				 * @brief Replaces the borrowed observer after any active notification finishes.
				 * @param wake Condition variable that remains alive until removal.
				 * @param owner Sink registration identity, or null for direct registration.
				 */
				void Notify(std::condition_variable& wake, const void* owner = nullptr) noexcept {
					std::lock_guard<std::mutex> lock(m_observer_mutex);
					m_wake = &wake;
					m_observer = owner;
				}

				/**
				 * @brief Removes the observer and waits for any active notification.
				 */
				void Unnotify() noexcept {
					std::lock_guard<std::mutex> lock(m_observer_mutex);
					m_wake = nullptr;
					m_observer = nullptr;
				}

				/**
				 * @brief Removes only the matching borrowed registration and waits for its notifications.
				 * @param owner Sink registration identity; never dereferenced.
				 */
				void Unnotify(const void* owner) noexcept {
					std::lock_guard<std::mutex> lock(m_observer_mutex);
					if (m_observer == owner) {
						m_wake = nullptr;
						m_observer = nullptr;
					}
				}

			private:
				/**
				 * @brief Notifies registered consumer condition variable if set.
				 */
				void SignalConsumer() noexcept {
					std::lock_guard<std::mutex> lock(m_observer_mutex);
					if (m_wake)
						m_wake->notify_one();
				}

				/**
				 * @brief Guards queue access.
				 */
				mutable std::mutex m_mutex;

				/**
				 * @brief Producer wait condition when full.
				 */
				std::condition_variable m_space;

				/**
				 * @brief Fundamentally aligned queue storage on Base's heap.
				 */
				std::queue<T, std::deque<T, ItemAllocator>> m_items;

				/**
				 * @brief End of production flag.
				 */
				std::atomic<bool> m_eof;

				/**
				 * @brief Serializes observer replacement, removal and use.
				 */
				std::mutex m_observer_mutex;

				/**
				 * @brief Borrowed consumer condition variable, guarded by m_observer_mutex.
				 */
				std::condition_variable* m_wake;

				/**
				 * @brief Sink registration identity guarded by m_observer_mutex; never dereferenced.
				 */
				const void* m_observer;

				/**
				 * @brief Capacity ceiling (0 = unbounded).
				 */
				std::atomic<std::size_t> m_cap;

				/**
				 * @brief Live writers; last CloseWriter Eofs.
				 */
				std::atomic<unsigned> m_writers;
		};

		template<Detail::HopperValue T>
		STORMBYTE_FORCE_INLINE Hopper<T>::Hopper() noexcept
			: Hopper(0) {}

		template<Detail::HopperValue T>
		STORMBYTE_FORCE_INLINE Hopper<T>::Hopper(StormByte::Size capacity) noexcept
			: m_io(::new (StormByte::Safe::Heap::Allocate(sizeof(Implementation))) Implementation(capacity)),
			m_owner(m_io, nullptr, [](void* context) noexcept {
				static_cast<Implementation*>(context)->~Implementation();
				StormByte::Safe::Heap::Free(context);
			}) {
			static_assert(alignof(Implementation) <= alignof(std::max_align_t));
		}

		template<Detail::HopperValue T>
		STORMBYTE_FORCE_INLINE Hopper<T>::~Hopper() noexcept = default;

		template<Detail::HopperValue T>
		StormByte::Size Hopper<T>::Capacity() const noexcept {
			return m_io->Capacity();
		}

		template<Detail::HopperValue T>
		void Hopper<T>::Capacity(StormByte::Size capacity) noexcept {
			m_io->Capacity(capacity);
		}

		template<Detail::HopperValue T>
		StormByte::Size Hopper<T>::Size() const noexcept {
			return m_io->Size();
		}

		template<Detail::HopperValue T>
		bool Hopper<T>::Full() const noexcept {
			return m_io->Full();
		}

		template<Detail::HopperValue T>
		unsigned Hopper<T>::Writers() const noexcept {
			return m_io->Writers();
		}

		template<Detail::HopperValue T>
		void Hopper<T>::Push(T item) noexcept {
			m_io->Push(std::move(item));
		}

		template<Detail::HopperValue T>
		Hopper<T>& Hopper<T>::operator<<(T item) noexcept {
			Push(std::move(item));
			return *this;
		}

		template<Detail::HopperValue T>
		void Hopper<T>::Eof() noexcept {
			m_io->Eof();
		}

		template<Detail::HopperValue T>
		void Hopper<T>::AddWriter() noexcept {
			m_io->AddWriter();
		}

		template<Detail::HopperValue T>
		void Hopper<T>::CloseWriter() noexcept {
			m_io->CloseWriter();
		}

		template<Detail::HopperValue T>
		T Hopper<T>::Pop() noexcept {
			return m_io->Pop();
		}

		template<Detail::HopperValue T>
		T Hopper<T>::Front() const noexcept requires Type::CopyConstructible<T> && std::is_nothrow_copy_constructible_v<T> {
			return m_io->Front();
		}

		template<Detail::HopperValue T>
		Hopper<T>& Hopper<T>::operator>>(T& item) noexcept {
			item = Pop();
			return *this;
		}

		template<Detail::HopperValue T>
		bool Hopper<T>::EoF() const noexcept {
			return m_io->EoF();
		}

		template<Detail::HopperValue T>
		bool Hopper<T>::Empty() const noexcept {
			return m_io->Empty();
		}

		template<Detail::HopperValue T>
		bool Hopper<T>::Ready() const noexcept {
			return m_io->Ready();
		}

		template<Detail::HopperValue T>
		void Hopper<T>::Notify(std::condition_variable& wake) noexcept {
			m_io->Notify(wake);
		}

		template<Detail::HopperValue T>
		void Hopper<T>::Unnotify() noexcept {
			m_io->Unnotify();
		}

		template<Detail::HopperValue T>
		void Hopper<T>::Notify(std::condition_variable& wake, const void* owner) noexcept {
			m_io->Notify(wake, owner);
		}

		template<Detail::HopperValue T>
		void Hopper<T>::Unnotify(const void* owner) noexcept {
			m_io->Unnotify(owner);
		}
	}
}
