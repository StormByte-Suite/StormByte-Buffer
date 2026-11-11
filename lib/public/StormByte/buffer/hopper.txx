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
#include <StormByte/safe/mutex.hxx>
#include <StormByte/safe/queue.hxx>
#include <StormByte/safe/unique_lock.hxx>
#include <StormByte/type_traits.hxx>

#include <algorithm>
#include <concepts>
#include <cstddef>
#include <exception>
#include <limits>
#include <type_traits>
#include <utility>

/**
 * @namespace StormByte
 * @brief Root namespace of the StormByte suite.
 */
namespace StormByte {
	/**
	 * @namespace StormByte::Type
	 * @brief Conditional Safe value declarations.
	 */
	namespace Type {
		/**
		 * @brief Certify provider-owned entries under the same payload/module contract as Hopper.
		 * @tparam T Supported nonthrowing Safe payload.
		 */
		template<Buffer::Detail::HopperValue T>
		struct IsMaybeSafe<Buffer::Detail::HopperEntry<T>>: std::true_type {};
	}
}

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
		 * condition variable notifications, and EoF flags. The gate, the signal
		 * and the words live on Base's heap.
		 */
		template<Detail::HopperValue T>
		class Hopper<T>::Implementation {
			/**
			 * @brief Provider-owned entry with logical weight and shared reservation.
			 */
			using Entry = Detail::HopperEntry<T>;

			public:
				/**
				 * @brief Constructs an unbounded Implementation instance.
				 * @throws AllocationError The gate, the signal or a word cannot be allocated.
				 */
				Implementation()
					: m_eof(false), m_wake(nullptr), m_generation(nullptr), m_observer(nullptr), m_cap(std::size_t{0}), m_writers(1u) {}

				/**
				 * @brief Constructs a bounded Implementation instance.
				 * @param capacity Maximum items allowed.
				 * @throws AllocationError The gate, the signal or a word cannot be allocated.
				 */
				explicit Implementation(StormByte::Size capacity)
					: m_eof(false), m_wake(nullptr), m_generation(nullptr), m_observer(nullptr), m_cap(static_cast<std::size_t>(capacity)), m_writers(1u) {
					if (capacity != StormByte::Size{0})
						m_limits.Elements(capacity);
				}

				Implementation(const Implementation&) = delete;

				Implementation(Implementation&&) = delete;

				/**
				 * @brief Destructor. Marks EoF and wakes waiting producers.
				 */
				~Implementation() noexcept {
					m_eof.store(true, Safe::MemoryOrder::Release);
					m_space.notify_all();
				}

				Implementation& operator=(const Implementation&) = delete;

				Implementation& operator=(Implementation&&) = delete;

				/**
				 * @brief Gets capacity ceiling.
				 * @return Capacity value.
				 */
				StormByte::Size Capacity() const noexcept {
					return StormByte::Size{m_cap.load(Safe::MemoryOrder::Acquire)};
				}

				/**
				 * @brief Sets capacity ceiling.
				 * @param capacity New capacity value.
				 */
				void Capacity(StormByte::Size capacity) noexcept {
					{
						Safe::UniqueLock lock(m_mutex);
						m_cap.store(static_cast<std::size_t>(capacity), Safe::MemoryOrder::Release);
						if (capacity == StormByte::Size{0})
							m_limits.UnlimitedElements();
						else
							m_limits.Elements(capacity);
					}
					m_space.notify_all();
					WakePending();
				}

				/**
				 * @brief Gets item count in queue.
				 * @return Count of items.
				 */
				StormByte::Size Size() const noexcept {
					Safe::UniqueLock lock(m_mutex);
					return StormByte::Size{static_cast<std::size_t>(m_items.size())};
				}

				/**
				 * @brief Check whether an active local bound has been reached.
				 * @return True at the item or byte ceiling, independently of aggregate pressure.
				 */
				bool Full() const noexcept {
					Safe::UniqueLock lock(m_mutex);
					return (m_limits.ElementsLimited() && m_items.size() >= m_limits.Elements())
						|| (m_limits.BytesLimited() && m_bytes >= m_limits.Bytes());
				}

				/**
				 * @brief Live writer count.
				 * @return Writers still open.
				 */
				unsigned Writers() const noexcept {
					return m_writers.load(Safe::MemoryOrder::Acquire);
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
						Safe::UniqueLock lock(m_mutex);
						++m_active;
						m_space.wait(lock, [this]() -> bool {
							const std::size_t cap = m_cap.load(Safe::MemoryOrder::Acquire);
							return (cap == 0 && !m_limits.ElementsLimited())
								|| (m_limits.ElementsLimited() && m_items.size() < m_limits.Elements())
								|| m_eof.load(Safe::MemoryOrder::Acquire);
						});
						--m_active;
						if (m_eof.load(Safe::MemoryOrder::Acquire))
							return;
						try {
							auto entry = Safe::Shared<Entry>::template MakePointer<Entry>();
							m_items.push(entry);
							entry->Item = std::move(item);
							NotePeak();
						}
						catch (...) {
							std::terminate();
						}
					}
					SignalConsumer();
					WakePending();
				}

				/**
				 * @brief Apply independent local bounds without dropping retained entries.
				 * @param limits New local ceilings.
				 */
				void Limits(QueueLimits limits) noexcept {
					{
						Safe::UniqueLock lock(m_mutex);
						m_limits = limits;
						m_cap.store(limits.ElementsLimited() ? static_cast<std::size_t>(limits.Elements()) : 0,
							Safe::MemoryOrder::Release);
					}
					m_space.notify_all();
					WakePending();
				}

				/**
				 * @brief Change wake/accounting domains only when no admission or entry uses the old one.
				 * @param limits New local bounds.
				 * @param budget Shared accounting.
				 * @return Ok or Invalid without partial publication.
				 */
				QueueStatus Configure(QueueLimits limits, const ByteBudget& budget) noexcept {
					{
						Safe::UniqueLock lock(m_mutex);
						if (!m_items.empty() || m_active != 0)
							return QueueStatus::Invalid;
						m_budget = budget;
						m_limits = limits;
						m_cap.store(limits.ElementsLimited() ? static_cast<std::size_t>(limits.Elements()) : 0,
							Safe::MemoryOrder::Release);
					}
					m_space.notify_all();
					WakePending();
					return QueueStatus::Ok;
				}

				/**
				 * @brief Read queue counters without waiting for admissions.
				 * @return Coherent logical accounting.
				 */
				QueueSnapshot Snapshot() const noexcept {
					Safe::UniqueLock lock(m_mutex);
					auto snapshot = m_snapshot;
					snapshot.Limits = m_limits;
					snapshot.Elements = StormByte::Size{m_items.size()};
					snapshot.Bytes = m_bytes;
					snapshot.Closed = m_eof.load(Safe::MemoryOrder::Acquire);
					snapshot.Exists = true;
					return snapshot;
				}

				/**
				 * @brief Wake shared-domain waiters after local state changes.
				 */
				void WakePending() noexcept {
					Safe::UniqueLock lock(m_mutex);
					Detail::BudgetAccess::Notify(m_budget);
				}

				/**
				 * @brief Cancel pending admissions in this hopper only.
				 */
				void CancelPending() noexcept {
					{
						Safe::UniqueLock lock(m_mutex);
						++m_cancellation;
					}
					WakePending();
				}

				/**
				 * @brief Close and move all entries out for destruction without the queue lock.
				 */
				void Abort() noexcept {
					Safe::Queue<Safe::Shared<Entry>> discarded;
					{
						Safe::UniqueLock lock(m_mutex);
						m_eof.store(true, Safe::MemoryOrder::Release);
						discarded.swap(m_items);
						m_bytes = StormByte::ByteSize{0};
					}
					m_space.notify_all();
					SignalConsumer();
					WakePending();
				}

				/**
				 * @brief Reserve and insert with a bounded wait, never retaining partial admission charges.
				 * @param item Moved only after storage and charge succeed.
				 * @param bytes Logical entry weight.
				 * @param lease Optional shared charge.
				 * @param timeout Nonnegative bounded wait.
				 * @param closed Optional borrowed Sink closure flag.
				 * @param observer Borrowed wait observer, invoked without Buffer locks.
				 * @return Explicit admission result.
				 */
				QueueStatus Admit(T& item, StormByte::ByteSize bytes, const ByteBudget::Lease* lease,
						std::chrono::milliseconds timeout, const Safe::Atomic<bool>* closed,
						AdmissionWaitObserver observer) noexcept {
					if constexpr (Type::NullablePointer<T>) {
						if (!item)
							return QueueStatus::Empty;
					}
					const auto started = std::chrono::steady_clock::now();
					if (timeout.count() < 0 || timeout > std::chrono::duration_cast<std::chrono::milliseconds>(
							std::chrono::steady_clock::time_point::max() - started))
						return QueueStatus::Invalid;
					const auto deadline = started + timeout;
					ByteBudget budget = [&] {
						Safe::UniqueLock lock(m_mutex);
						++m_active;
						return m_budget;
					}();
					const auto initial = Detail::BudgetAccess::Event(budget);
					std::size_t cancellation;
					{
						Safe::UniqueLock lock(m_mutex);
						cancellation = m_cancellation;
					}
					bool waited = false;
					QueueStatus result = QueueStatus::WouldBlock;
					for (;;) {
						const auto event = Detail::BudgetAccess::Event(budget);
						Safe::Shared<Entry> entry;
						ByteBudget::Lease charge;
						auto reason = AdmissionWaitReason::LocalCapacity;
						{
							Safe::UniqueLock lock(m_mutex);
							const auto aggregate = budget.Snapshot();
							if (m_eof.load(Safe::MemoryOrder::Acquire)
									|| (closed && closed->load(Safe::MemoryOrder::Acquire)))
								result = QueueStatus::Closed;
							else if (m_cancellation != cancellation || event.Cancellation != initial.Cancellation)
								result = QueueStatus::Cancelled;
							else if ((m_limits.ElementsLimited() && m_limits.Elements() == StormByte::Size{0})
									|| (m_limits.BytesLimited() && bytes > m_limits.Bytes()))
								result = QueueStatus::TooLarge;
							else if (lease && (!*lease || !lease->BelongsTo(budget) || lease->Bytes() != bytes))
								result = QueueStatus::Invalid;
							else if (aggregate.Closed)
								result = QueueStatus::Closed;
							else if (!lease && aggregate.Limited && bytes > aggregate.Limit)
								result = QueueStatus::TooLarge;
							else if ((m_limits.ElementsLimited() && m_items.size() >= m_limits.Elements())
									|| (m_limits.BytesLimited() && (m_bytes > m_limits.Bytes()
										|| bytes > m_limits.Bytes() - m_bytes)))
								result = QueueStatus::WouldBlock;
							else if (bytes > StormByte::ByteSize{std::numeric_limits<std::size_t>::max()} - m_bytes)
								result = QueueStatus::TooLarge;
							else {
								if (lease) {
									charge = *lease;
									result = QueueStatus::Ok;
								}
								else {
									result = budget.TryReserve(bytes, charge);
									if (result == QueueStatus::WouldBlock)
										reason = AdmissionWaitReason::SharedBudget;
								}
								if (result == QueueStatus::Ok) {
									try {
										entry = Safe::Shared<Entry>::template MakePointer<Entry>();
										m_items.push(entry);
									}
									catch (...) {
										result = QueueStatus::Error;
									}
									if (result == QueueStatus::Ok) {
										entry->Charge = std::move(charge);
										entry->Bytes = bytes;
										entry->Item = std::move(item);
										m_bytes = m_bytes + bytes;
										NotePeak();
									}
								}
							}
							if (result == QueueStatus::WouldBlock)
								++m_snapshot.Pressure;
						}
						charge.Reset();
						if (result != QueueStatus::WouldBlock || timeout.count() == 0)
							break;
						if (std::chrono::steady_clock::now() >= deadline) {
							result = QueueStatus::Timeout;
							break;
						}
						waited = true;
						if (observer.Changed)
							observer.Changed(observer.Context, reason, true);
						Detail::BudgetAccess::Wait(budget, event, std::chrono::duration_cast<std::chrono::nanoseconds>(
							deadline - std::chrono::steady_clock::now()).count());
						if (observer.Changed)
							observer.Changed(observer.Context, reason, false);
					}
					{
						Safe::UniqueLock lock(m_mutex);
						--m_active;
						if (waited) {
							++m_snapshot.Waits;
							m_snapshot.WaitNanoseconds += std::chrono::duration_cast<std::chrono::nanoseconds>(
								std::chrono::steady_clock::now() - started).count();
						}
					}
					if (result == QueueStatus::Ok)
						SignalConsumer();
					return result;
				}

				/**
				 * @brief Signals end of production.
				 */
				void Eof() noexcept {
					{
						Safe::UniqueLock lock(m_mutex);
						m_eof.store(true, Safe::MemoryOrder::Release);
					}
					SignalConsumer();
					m_space.notify_all();
					WakePending();
				}

				/**
				 * @brief Registers an extra writer.
				 */
				void AddWriter() noexcept {
					m_writers.fetch_add(1u, Safe::MemoryOrder::AcqRel);
				}

				/**
				 * @brief Releases one writer. Last writer force-closes.
				 */
				void CloseWriter() noexcept {
					unsigned prev = m_writers.load(Safe::MemoryOrder::Acquire);
					while (prev > 0) {
						if (m_writers.compare_exchange_weak(prev, static_cast<unsigned>(prev - 1),
								Safe::MemoryOrder::AcqRel, Safe::MemoryOrder::Acquire)) {
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
					Safe::Shared<Entry> entry;
					{
						Safe::UniqueLock lock(m_mutex);
						if (m_items.empty())
							return T{};
						entry = std::move(m_items.front());
						m_items.pop();
						m_bytes = m_bytes - entry->Bytes;
					}
					item = std::move(entry->Item);
					entry.reset();
					m_space.notify_all();
					WakePending();
					return item;
				}

				/**
				 * @brief Copy of front item. Does not dequeue.
				 * @return Front or default T.
				 */
				T Front() const noexcept requires Type::CopyConstructible<T> && std::is_nothrow_copy_constructible_v<T> {
					Safe::UniqueLock lock(m_mutex);
					if (m_items.empty())
						return T{};
					return m_items.front()->Item;
				}

				/**
				 * @brief Checks if Eof was signaled.
				 * @return true if Eof set.
				 */
				bool EoF() const noexcept {
					return m_eof.load(Safe::MemoryOrder::Acquire);
				}

				/**
				 * @brief Checks if queue is empty.
				 * @return true if empty.
				 */
				bool Empty() const noexcept {
					Safe::UniqueLock lock(m_mutex);
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
				 * @param generation Optional borrowed stored-event counter.
				 */
				void Notify(Safe::ConditionVariable& wake, const void* owner = nullptr, Safe::Atomic<std::size_t>* generation = nullptr) noexcept {
					Safe::UniqueLock lock(m_observer_mutex);
					m_wake = &wake;
					m_generation = generation;
					m_observer = owner;
				}

				/**
				 * @brief Removes the observer and waits for any active notification.
				 */
				void Unnotify() noexcept {
					Safe::UniqueLock lock(m_observer_mutex);
					m_wake = nullptr;
					m_generation = nullptr;
					m_observer = nullptr;
				}

				/**
				 * @brief Removes only the matching borrowed registration and waits for its notifications.
				 * @param owner Sink registration identity; never dereferenced.
				 */
				void Unnotify(const void* owner) noexcept {
					Safe::UniqueLock lock(m_observer_mutex);
					if (m_observer == owner) {
						m_wake = nullptr;
						m_generation = nullptr;
						m_observer = nullptr;
					}
				}

			private:
				/**
				 * @brief Update peaks after insertion while the queue mutex is held.
				 */
				void NotePeak() noexcept {
					m_snapshot.PeakElements = std::max(m_snapshot.PeakElements, StormByte::Size{m_items.size()});
					m_snapshot.PeakBytes = std::max(m_snapshot.PeakBytes, m_bytes);
				}

				QueueLimits m_limits;				///< Independent local limits.
				ByteBudget m_budget;				///< Shared accounting and wake domain.
				StormByte::ByteSize m_bytes {0};		///< Live logical entry weights.
				QueueSnapshot m_snapshot;			///< Lifetime peaks and pressure.
				std::size_t m_active {0};			///< Admissions retaining this wake domain.
				std::size_t m_cancellation {0};		///< Queue-local cancellation epoch.

				/**
				 * @brief Notifies registered consumer condition variable if set.
				 */
				void SignalConsumer() noexcept {
					Safe::UniqueLock lock(m_observer_mutex);
					if (m_generation != nullptr) {
						m_generation->fetch_add(std::size_t{1}, Safe::MemoryOrder::Release);
						m_generation->notify_all();
					}
					if (m_wake != nullptr)
						m_wake->notify_one();
				}

				/**
				 * @brief Guards queue access.
				 */
				mutable Safe::Mutex m_mutex;
				/**
				 * @brief Producer wait condition when full.
				 */
				Safe::ConditionVariable m_space;
				/**
				 * @brief Item storage on Base's heap.
				 */
				StormByte::Safe::Queue<Safe::Shared<Entry>> m_items;
				/**
				 * @brief End of production flag.
				 */
				Safe::Atomic<bool> m_eof;
				/**
				 * @brief Serializes observer replacement, removal and use.
				 */
				mutable Safe::Mutex m_observer_mutex;
				/**
				 * @brief Borrowed consumer condition variable, guarded by m_observer_mutex.
				 */
				Safe::ConditionVariable* m_wake;
				/**
				 * @brief Borrowed event counter guarded by m_observer_mutex.
				 */
				Safe::Atomic<std::size_t>* m_generation;
				/**
				 * @brief Sink registration identity guarded by m_observer_mutex; never dereferenced.
				 */
				const void* m_observer;
				/**
				 * @brief Capacity ceiling (0 = unbounded).
				 */
				Safe::Atomic<std::size_t> m_cap;
				/**
				 * @brief Live writers; last CloseWriter Eofs.
				 */
				Safe::Atomic<unsigned> m_writers;
		};

		template<Detail::HopperValue T>
		Hopper<T>::Hopper()
			: Hopper(0) {}

		template<Detail::HopperValue T>
		Hopper<T>::Hopper(StormByte::Size capacity)
			: m_io(::new (StormByte::Safe::Heap::Allocate(sizeof(Implementation))) Implementation(capacity)),
			m_owner(m_io, nullptr, [](void* context) noexcept {
				static_cast<Implementation*>(context)->~Implementation();
				StormByte::Safe::Heap::Free(context);
			}) {
			static_assert(alignof(Implementation) <= __STDCPP_DEFAULT_NEW_ALIGNMENT__);
		}

		template<Detail::HopperValue T>
		Hopper<T>::~Hopper() noexcept = default;

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
		QueueStatus Hopper<T>::TryPush(T& item, StormByte::ByteSize bytes) noexcept {
			return Admit(item, bytes, nullptr, std::chrono::milliseconds{0}, nullptr);
		}

		template<Detail::HopperValue T>
		QueueStatus Hopper<T>::TryPush(T& item, const ByteBudget::Lease& lease) noexcept {
			return Admit(item, lease.Bytes(), &lease, std::chrono::milliseconds{0}, nullptr);
		}

		template<Detail::HopperValue T>
		QueueStatus Hopper<T>::PushFor(T& item, StormByte::ByteSize bytes, std::chrono::milliseconds timeout,
				AdmissionWaitObserver observer) noexcept {
			return Admit(item, bytes, nullptr, timeout, nullptr, observer);
		}

		template<Detail::HopperValue T>
		QueueStatus Hopper<T>::PushFor(T& item, const ByteBudget::Lease& lease, std::chrono::milliseconds timeout,
				AdmissionWaitObserver observer) noexcept {
			return Admit(item, lease.Bytes(), &lease, timeout, nullptr, observer);
		}

		template<Detail::HopperValue T>
		QueueStatus Hopper<T>::Admit(T& item, StormByte::ByteSize bytes, const ByteBudget::Lease* lease,
				std::chrono::milliseconds timeout, const Safe::Atomic<bool>* closed, AdmissionWaitObserver observer) noexcept {
			return m_io->Admit(item, bytes, lease, timeout, closed, observer);
		}

		template<Detail::HopperValue T>
		void Hopper<T>::Limits(QueueLimits limits) noexcept {
			m_io->Limits(limits);
		}

		template<Detail::HopperValue T>
		QueueStatus Hopper<T>::Configure(QueueLimits limits, const ByteBudget& budget) noexcept {
			return m_io->Configure(limits, budget);
		}

		template<Detail::HopperValue T>
		QueueSnapshot Hopper<T>::Snapshot() const noexcept {
			return m_io->Snapshot();
		}

		template<Detail::HopperValue T>
		void Hopper<T>::WakePending() noexcept {
			m_io->WakePending();
		}

		template<Detail::HopperValue T>
		void Hopper<T>::CancelPending() noexcept {
			m_io->CancelPending();
		}

		template<Detail::HopperValue T>
		void Hopper<T>::Abort() noexcept {
			m_io->Abort();
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
		void Hopper<T>::Notify(Safe::ConditionVariable& wake) noexcept {
			m_io->Notify(wake);
		}

		template<Detail::HopperValue T>
		void Hopper<T>::Notify(Safe::ConditionVariable& wake, Safe::Atomic<std::size_t>& generation) noexcept {
			m_io->Notify(wake, nullptr, &generation);
		}

		template<Detail::HopperValue T>
		void Hopper<T>::Unnotify() noexcept {
			m_io->Unnotify();
		}

		template<Detail::HopperValue T>
		void Hopper<T>::Notify(Safe::ConditionVariable& wake, const void* owner, Safe::Atomic<std::size_t>* generation) noexcept {
			m_io->Notify(wake, owner, generation);
		}

		template<Detail::HopperValue T>
		void Hopper<T>::Unnotify(const void* owner) noexcept {
			m_io->Unnotify(owner);
		}
	}
}
