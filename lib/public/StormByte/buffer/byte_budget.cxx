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

#include <algorithm>
#include <condition_variable>
#include <limits>
#include <mutex>

using namespace StormByte::Buffer;
using StormByte::ByteSize;

/**
 * @brief Provider-local shared accounting. No mutex or STL owner crosses the DLL.
 */
struct ByteBudget::State {
	public:
		/**
		 * @brief Initialize aggregate bounds.
		 * @param limited Whether limit is active.
		 * @param limit Aggregate ceiling.
		 */
		State(bool limited, ByteSize limit) {
			m_snapshot.Limited = limited;
			m_snapshot.Limit = limit;
		}

		State(const State&) = delete;

		State(State&&) = delete;

		/**
		 * @brief Release synchronization after all handles and leases are gone.
		 */
		~State() noexcept = default;

		State& operator=(const State&) = delete;

		State& operator=(State&&) = delete;

		mutable std::mutex m_mutex;			///< Protects counters and wake generations.
		std::condition_variable m_changed;	///< Shared queue/work wake domain.
		BudgetSnapshot m_snapshot;			///< Aggregate counters.
		std::size_t m_revision {0};			///< Wake generation.
		std::size_t m_cancellation {0};		///< Explicit cancellation epoch.
};

/**
 * @brief Provider-owned reservation released once by the final lease.
 */
struct ByteBudget::Lease::Charge {
	public:
		/**
		 * @brief Adopt an already-accounted reservation.
		 * @param state Shared budget state.
		 * @param bytes Reserved bytes.
		 */
		Charge(StormByte::Safe::Shared<State> state, ByteSize bytes) noexcept:
			m_state(std::move(state)), m_bytes(bytes) {}

		Charge(const Charge&) = delete;

		Charge(Charge&&) = delete;

		/**
		 * @brief Return bytes and wake admissions.
		 */
		~Charge() noexcept {
			{
				std::lock_guard lock(m_state->m_mutex);
				m_state->m_snapshot.Reserved = m_state->m_snapshot.Reserved - m_bytes;
				++m_state->m_revision;
			}
			m_state->m_changed.notify_all();
		}

		Charge& operator=(const Charge&) = delete;

		Charge& operator=(Charge&&) = delete;

		StormByte::Safe::Shared<State> m_state;	///< Budget retained through final release.
		ByteSize m_bytes;						///< Reserved logical bytes.
};

ByteBudget::ByteBudget(bool limited, ByteSize limit):
	m_state(StormByte::Safe::Shared<State>::MakePointer<State>(limited, limit)) {}

ByteBudget::Lease::operator bool() const noexcept { return static_cast<bool>(m_charge); }

ByteSize ByteBudget::Lease::Bytes() const noexcept { return m_charge ? m_charge->m_bytes : ByteSize{0}; }

bool ByteBudget::Lease::BelongsTo(const ByteBudget& budget) const noexcept {
	return m_charge && m_charge->m_state == budget.m_state;
}

void ByteBudget::Lease::Reset() noexcept { m_charge.reset(); }

QueueStatus ByteBudget::TryReserve(ByteSize bytes, Lease& output) const noexcept {
	if (!m_state)
		return QueueStatus::Closed;
	{
		std::lock_guard lock(m_state->m_mutex);
		auto& snapshot = m_state->m_snapshot;
		if (snapshot.Closed)
			return QueueStatus::Closed;
		if (bytes > ByteSize{std::numeric_limits<std::size_t>::max()} - snapshot.Reserved)
			return QueueStatus::TooLarge;
		if (snapshot.Limited) {
			if (bytes > snapshot.Limit)
				return QueueStatus::TooLarge;
			if (bytes > snapshot.Limit - snapshot.Reserved) {
				++snapshot.Pressure;
				return QueueStatus::WouldBlock;
			}
		}
		snapshot.Reserved = snapshot.Reserved + bytes;
		snapshot.Peak = std::max(snapshot.Peak, snapshot.Reserved);
	}
	Lease acquired;
	try {
		acquired.m_charge = StormByte::Safe::Shared<Lease::Charge>::MakePointer<Lease::Charge>(m_state, bytes);
	}
	catch (...) {
		{
			std::lock_guard lock(m_state->m_mutex);
			m_state->m_snapshot.Reserved = m_state->m_snapshot.Reserved - bytes;
			++m_state->m_revision;
		}
		m_state->m_changed.notify_all();
		return QueueStatus::Error;
	}
	output = std::move(acquired);
	return QueueStatus::Ok;
}

QueueStatus ByteBudget::ReserveMilliseconds(ByteSize bytes, Lease& output, std::int64_t milliseconds) const noexcept {
	const auto timeout = std::chrono::milliseconds(milliseconds);
	const auto started = std::chrono::steady_clock::now();
	if (timeout.count() < 0 || timeout > std::chrono::duration_cast<std::chrono::milliseconds>(
			std::chrono::steady_clock::time_point::max() - started))
		return QueueStatus::Invalid;
	const auto deadline = started + timeout;
	const auto initial = CurrentEvent();
	bool waited = false;
	QueueStatus result;
	for (;;) {
		const auto event = CurrentEvent();
		if (event.Cancellation != initial.Cancellation) {
			result = QueueStatus::Cancelled;
			break;
		}
		result = TryReserve(bytes, output);
		if (result != QueueStatus::WouldBlock || timeout.count() == 0)
			break;
		if (std::chrono::steady_clock::now() >= deadline) {
			result = QueueStatus::Timeout;
			break;
		}
		waited = true;
		Wait(event, std::chrono::duration_cast<std::chrono::nanoseconds>(
			deadline - std::chrono::steady_clock::now()).count());
	}
	if (waited && m_state) {
		std::lock_guard lock(m_state->m_mutex);
		++m_state->m_snapshot.Waits;
		m_state->m_snapshot.WaitNanoseconds += std::chrono::duration_cast<std::chrono::nanoseconds>(
			std::chrono::steady_clock::now() - started).count();
	}
	return result;
}

bool ByteBudget::SharesIdentity(const ByteBudget& other) const noexcept {
	return m_state && other.m_state && m_state == other.m_state;
}

void ByteBudget::Close() const noexcept {
	if (!m_state)
		return;
	{
		std::lock_guard lock(m_state->m_mutex);
		m_state->m_snapshot.Closed = true;
		++m_state->m_revision;
	}
	m_state->m_changed.notify_all();
}

void ByteBudget::CancelPending() const noexcept {
	if (!m_state)
		return;
	{
		std::lock_guard lock(m_state->m_mutex);
		++m_state->m_cancellation;
		++m_state->m_revision;
	}
	m_state->m_changed.notify_all();
}

BudgetSnapshot ByteBudget::Snapshot() const noexcept {
	if (!m_state) {
		BudgetSnapshot snapshot;
		snapshot.Closed = true;
		return snapshot;
	}
	std::lock_guard lock(m_state->m_mutex);
	return m_state->m_snapshot;
}

ByteBudget::Event ByteBudget::CurrentEvent() const noexcept {
	if (!m_state)
		return {};
	std::lock_guard lock(m_state->m_mutex);
	return {m_state->m_revision, m_state->m_cancellation};
}

void ByteBudget::Notify() const noexcept {
	if (!m_state)
		return;
	{
		std::lock_guard lock(m_state->m_mutex);
		++m_state->m_revision;
	}
	m_state->m_changed.notify_all();
}

void ByteBudget::Wait(Event event, std::int64_t nanoseconds) const noexcept {
	if (!m_state)
		return;
	std::unique_lock lock(m_state->m_mutex);
	m_state->m_changed.wait_for(lock, std::chrono::nanoseconds(nanoseconds), [&] {
		return m_state->m_revision != event.Revision || m_state->m_snapshot.Closed;
	});
}
