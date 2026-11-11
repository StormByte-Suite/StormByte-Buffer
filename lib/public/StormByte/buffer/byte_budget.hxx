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

#include <StormByte/buffer/visibility.h>
#include <StormByte/byte_size.hxx>
#include <StormByte/safe/optional.hxx>
#include <StormByte/safe/pointers.hxx>
#include <StormByte/size.hxx>
#include <StormByte/type_traits/safe.hxx>

#include <chrono>
#include <cstddef>
#include <cstdint>
#include <optional>

/**
 * @namespace StormByte
 * @brief Root namespace of the StormByte suite.
 */
namespace StormByte {
	/**
	 * @namespace StormByte::Buffer
	 * @brief Generic buffering and accounting mechanisms.
	 */
	namespace Buffer {
		/**
		 * @namespace StormByte::Buffer::Detail
		 * @brief Private queue implementation contracts.
		 */
		namespace Detail {
			/**
			 * @brief Access to provider-owned budget wake events.
			 */
			struct BudgetAccess;
		}

		/**
		 * @brief Admission outcomes. Failure preserves the caller's item and output lease.
		 */
		enum class QueueStatus {
			Ok,			///< Accepted.
			WouldBlock,	///< Capacity unavailable.
			TooLarge,	///< The item can never fit the configured limit.
			Closed,		///< Queue, Sink or budget closed.
			Cancelled,	///< Pending admissions cancelled.
			Timeout,	///< Bounded wait expired.
			Missing,	///< Sink key does not exist.
			Empty,		///< Nullable item was empty.
			Error,		///< Storage allocation failed.
			Invalid		///< Invalid duration, lease or configuration.
		};

		/**
		 * @brief Independent local bounds; absent means unlimited, engaged zero means zero capacity.
		 */
		class QueueLimits {
			public:
				/**
				 * @brief Resolve optional limits into a non-owning value.
				 * @param elements Optional item ceiling.
				 * @param bytes Optional logical-byte ceiling.
				 */
				STORMBYTE_FORCE_INLINE QueueLimits(
						StormByte::Safe::Optional<StormByte::Size> elements = {},
						StormByte::Safe::Optional<StormByte::ByteSize> bytes = {}) noexcept:
					m_elements_limited(elements.has_value()),
					m_bytes_limited(bytes.has_value()),
					m_elements(elements.value_or(StormByte::Size{0})),
					m_bytes(bytes.value_or(StormByte::ByteSize{0})) {}

				/**
				 * @brief Copy resolved bounds without allocation.
				 * @param other Source.
				 */
				QueueLimits(const QueueLimits& other) noexcept = default;

				/**
				 * @brief Move resolved bounds without ownership.
				 * @param other Source.
				 */
				QueueLimits(QueueLimits&& other) noexcept = default;

				/**
				 * @brief Destroy non-owning bounds.
				 */
				~QueueLimits() noexcept = default;

				/**
				 * @brief Replace resolved bounds without allocation.
				 * @param other Source.
				 * @return This value.
				 */
				QueueLimits& operator=(const QueueLimits& other) noexcept = default;

				/**
				 * @brief Move-assign resolved bounds.
				 * @param other Source.
				 * @return This value.
				 */
				QueueLimits& operator=(QueueLimits&& other) noexcept = default;

				/**
				 * @brief Whether an item ceiling is configured.
				 * @return True for an engaged element limit.
				 */
				constexpr bool ElementsLimited() const noexcept { return m_elements_limited; }

				/**
				 * @brief Whether a byte ceiling is configured.
				 * @return True for an engaged byte limit.
				 */
				constexpr bool BytesLimited() const noexcept { return m_bytes_limited; }

				/**
				 * @brief Configured item ceiling.
				 * @return Items; meaningful only when ElementsLimited is true.
				 */
				constexpr StormByte::Size Elements() const noexcept { return m_elements; }

				/**
				 * @brief Engage an item ceiling without allocating an optional.
				 * @param elements Item ceiling, including an explicit zero.
				 */
				constexpr void Elements(StormByte::Size elements) noexcept {
					m_elements = elements;
					m_elements_limited = true;
				}

				/**
				 * @brief Remove only the item ceiling without changing byte limits.
				 */
				constexpr void UnlimitedElements() noexcept {
					m_elements_limited = false;
					m_elements = StormByte::Size{0};
				}

				/**
				 * @brief Configured byte ceiling.
				 * @return Bytes; meaningful only when BytesLimited is true.
				 */
				constexpr StormByte::ByteSize Bytes() const noexcept { return m_bytes; }

			private:
				bool m_elements_limited;				///< Item limit is active.
				bool m_bytes_limited;				///< Byte limit is active.
				StormByte::Size m_elements;			///< Item ceiling.
				StormByte::ByteSize m_bytes;			///< Byte ceiling.
		};

		/**
		 * @brief Coherent queue accounting; bytes are caller-supplied weights, never RSS.
		 */
		struct QueueSnapshot {
			QueueLimits Limits;						///< Configured local bounds.
			StormByte::Size Elements {0};			///< Currently queued entries.
			StormByte::ByteSize Bytes {0};			///< Currently queued logical weights.
			StormByte::Size PeakElements {0};		///< Lifetime peak entries.
			StormByte::ByteSize PeakBytes {0};		///< Lifetime peak logical bytes.
			std::size_t Pressure {0};				///< Admissions encountering capacity pressure.
			std::size_t Waits {0};					///< Admissions that actually waited.
			std::int64_t WaitNanoseconds {0};		///< Accumulated admission wait duration.
			bool Closed {false};					///< Production has ended.
			bool Exists {false};					///< False for a missing Sink key.
		};

		/**
		 * @brief Shared reservation counters, independent of any particular queue.
		 */
		struct BudgetSnapshot {
			bool Limited {false};					///< Aggregate ceiling is active.
			StormByte::ByteSize Limit {0};			///< Configured aggregate ceiling.
			StormByte::ByteSize Reserved {0};		///< Live lease charges, including work outside queues.
			StormByte::ByteSize Peak {0};			///< Lifetime peak reservations.
			std::size_t Pressure {0};				///< Reservations refused for temporary capacity pressure.
			std::size_t Waits {0};					///< ReserveFor calls that actually waited.
			std::int64_t WaitNanoseconds {0};		///< Accumulated ReserveFor wait duration.
			bool Closed {false};					///< Further reservations are forbidden.
		};

		/**
		 * @brief Shared logical byte accounting for queues and concurrent work.
		 *
		 * Copies share provider-owned state. A lease is charged once until its
		 * final copy is released. Buffer and Base must remain loaded. Independent
		 * handles may be used concurrently; do not move/assign a handle in use.
		 */
		class STORMBYTE_BUFFER_PUBLIC ByteBudget {
			public:
				/**
				 * @brief Shared charge that can explicitly follow one payload across queues or work.
				 */
				class STORMBYTE_BUFFER_PUBLIC Lease {
					public:
						/**
						 * @brief Construct an empty charge.
						 */
						Lease() noexcept = default;

						/**
						 * @brief Share an existing charge without reserving twice.
						 * @param other Source lease.
						 */
						Lease(const Lease& other) noexcept = default;

						/**
						 * @brief Transfer a charge.
						 * @param other Source, left empty.
						 */
						Lease(Lease&& other) noexcept = default;

						/**
						 * @brief Release this reference; final release returns bytes.
						 */
						~Lease() noexcept = default;

						/**
						 * @brief Replace this reference with a shared charge.
						 * @param other Source lease.
						 * @return This lease.
						 */
						Lease& operator=(const Lease& other) noexcept = default;

						/**
						 * @brief Replace this reference with a transferred charge.
						 * @param other Source, left empty.
						 * @return This lease.
						 */
						Lease& operator=(Lease&& other) noexcept = default;

						/**
						 * @brief Whether a charge is owned, including a zero-byte charge.
						 * @return True for a live charge.
						 */
						explicit operator bool() const noexcept;

						/**
						 * @brief Size reserved once for all copies.
						 * @return Logical bytes.
						 */
						StormByte::ByteSize Bytes() const noexcept;

						/**
						 * @brief Check shared budget identity.
						 * @param budget Candidate budget.
						 * @return True when charged to that budget.
						 */
						bool BelongsTo(const ByteBudget& budget) const noexcept;

						/**
						 * @brief Release this reference.
						 */
						void Reset() noexcept;

					private:
						friend class ByteBudget;

						/**
						 * @brief Provider-owned shared reservation.
						 */
						struct Charge;

						StormByte::Safe::Shared<Charge> m_charge;	///< Provider-owned charge.
				};

				/**
				 * @brief Construct an unlimited aggregate budget.
				 */
				ByteBudget(): ByteBudget(false, StormByte::ByteSize{0}) {}

				/**
				 * @brief Explicitly construct an unlimited budget.
				 */
				STORMBYTE_FORCE_INLINE explicit ByteBudget(std::nullopt_t): ByteBudget() {}

				/**
				 * @brief Construct a fixed aggregate ceiling; zero allows only zero-byte charges.
				 * @param limit Aggregate logical bytes.
				 */
				explicit ByteBudget(StormByte::ByteSize limit): ByteBudget(true, limit) {}

				/**
				 * @brief Share budget identity and counters.
				 * @param other Source budget.
				 */
				ByteBudget(const ByteBudget& other) noexcept = default;

				/**
				 * @brief Transfer a budget handle.
				 * @param other Source, left invalid.
				 */
				ByteBudget(ByteBudget&& other) noexcept = default;

				/**
				 * @brief Release this handle without closing other handles or leases.
				 */
				~ByteBudget() noexcept = default;

				/**
				 * @brief Share another budget.
				 * @param other Source budget.
				 * @return This handle.
				 */
				ByteBudget& operator=(const ByteBudget& other) noexcept = default;

				/**
				 * @brief Transfer another budget handle.
				 * @param other Source, left invalid.
				 * @return This handle.
				 */
				ByteBudget& operator=(ByteBudget&& other) noexcept = default;

				/**
				 * @brief Reserve without waiting for capacity.
				 * @param bytes Caller-defined weight.
				 * @param output Replaced only on success.
				 * @return Explicit admission result; allocation failure rolls back the charge.
				 */
				QueueStatus TryReserve(StormByte::ByteSize bytes, Lease& output) const noexcept;

				/**
				 * @brief Reserve with a bounded wait; zero attempts immediately.
				 * @param bytes Caller-defined weight.
				 * @param output Replaced only on success.
				 * @param timeout Nonnegative wait duration.
				 * @return Admission result; oversize weights fail without waiting.
				 */
				STORMBYTE_FORCE_INLINE QueueStatus ReserveFor(StormByte::ByteSize bytes, Lease& output,
						std::chrono::milliseconds timeout) const noexcept {
					return ReserveMilliseconds(bytes, output, timeout.count());
				}

				/**
				 * @brief Reject future reservations and wake waiters, retaining existing charges.
				 */
				void Close() const noexcept;

				/**
				 * @brief Cancel currently waiting admissions without closing or invalidating leases.
				 */
				void CancelPending() const noexcept;

				/**
				 * @brief Compare shared provider state without reserving or locking.
				 *
				 * Works before and after Close. Independent budgets differ even with equal
				 * limits and counters; moved-from handles share nothing, even together.
				 * Concurrent comparison is safe unless a handle is moved or assigned.
				 * @param other Candidate handle.
				 * @return True when both valid handles share one internal state.
				 */
				bool SharesIdentity(const ByteBudget& other) const noexcept;

				/**
				 * @brief Read counters under a short state lock, without capacity waits or I/O.
				 * @return Coherent aggregate accounting.
				 */
				BudgetSnapshot Snapshot() const noexcept;

			private:
				friend struct Detail::BudgetAccess;

				/**
				 * @brief Provider-owned synchronization and reservation counters.
				 */
				struct State;

				/**
				 * @brief Wake generation captured before an admission predicate is checked.
				 */
				struct Event {
					std::size_t Revision;		///< Capacity or lifecycle event.
					std::size_t Cancellation;	///< Explicit cancellation epoch.
				};

				/**
				 * @brief Allocate provider-owned state.
				 * @param limited Whether limit is active.
				 * @param limit Aggregate ceiling.
				 */
				ByteBudget(bool limited, StormByte::ByteSize limit);

				/**
				 * @brief Capture the current event generation.
				 * @return Stored wake and cancellation epochs.
				 */
				Event CurrentEvent() const noexcept;

				/**
				 * @brief Wake admissions after a queue state change.
				 */
				void Notify() const noexcept;

				/**
				 * @brief Wait until a captured generation changes or a bounded duration expires.
				 * @param event Generation captured before testing predicates.
				 * @param nanoseconds Relative wait duration as a scalar across the DLL.
				 */
				void Wait(Event event, std::int64_t nanoseconds) const noexcept;

				/**
				 * @brief Provider-side bounded reservation using only scalar time at the DLL boundary.
				 * @param bytes Caller-defined weight.
				 * @param output Replaced only on success.
				 * @param milliseconds Relative wait duration.
				 * @return Explicit admission result.
				 */
				QueueStatus ReserveMilliseconds(StormByte::ByteSize bytes, Lease& output,
					std::int64_t milliseconds) const noexcept;

				StormByte::Safe::Shared<State> m_state;	///< Shared provider-owned accounting.
		};

		/**
		 * @namespace StormByte::Buffer::Detail
		 * @brief Private queue implementation contracts.
		 */
		namespace Detail {
			/**
			 * @brief Queue wake transport; never calls into queues under the budget mutex.
			 */
			struct BudgetAccess {
				/**
				 * @brief Capture a stored event.
				 * @param budget Shared wake domain.
				 * @return Event generation.
				 */
				static auto Event(const ByteBudget& budget) noexcept { return budget.CurrentEvent(); }

				/**
				 * @brief Publish a queue state change.
				 * @param budget Shared wake domain.
				 */
				static void Notify(const ByteBudget& budget) noexcept { budget.Notify(); }

				/**
				 * @brief Wait for state changes without holding queue locks.
				 * @param budget Shared wake domain.
				 * @param event Captured generation.
				 * @param nanoseconds Relative wait duration.
				 */
				static void Wait(const ByteBudget& budget, ByteBudget::Event event,
						std::int64_t nanoseconds) noexcept {
					budget.Wait(event, nanoseconds);
				}
			};
		}
	}
}

STORMBYTE_DECLARE_MAYBE_SAFE(StormByte::Buffer::QueueStatus);
STORMBYTE_DECLARE_MAYBE_SAFE(StormByte::Buffer::QueueLimits);
STORMBYTE_DECLARE_MAYBE_SAFE(StormByte::Buffer::QueueSnapshot);
STORMBYTE_DECLARE_MAYBE_SAFE(StormByte::Buffer::BudgetSnapshot);
STORMBYTE_DECLARE_MAYBE_SAFE(StormByte::Buffer::ByteBudget);
STORMBYTE_DECLARE_MAYBE_SAFE(StormByte::Buffer::ByteBudget::Lease);
