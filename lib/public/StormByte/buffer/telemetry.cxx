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

#include <StormByte/buffer/telemetry.hxx>
#include <StormByte/byte_size.hxx>

#include <string>
#include <string_view>

using StormByte::Buffer::ReadTelemetry;
using StormByte::Buffer::Telemetry;
using StormByte::Buffer::WriteTelemetry;

namespace {
	std::size_t& OperationDepth() noexcept {
		static thread_local std::size_t depth = 0;
		return depth;
	}

	std::uint64_t OperationThreadId() noexcept {
		static std::atomic<std::uint64_t> next_id {0};
		static thread_local const std::uint64_t id = next_id.fetch_add(1, std::memory_order_relaxed);
		return id;
	}

	std::string OperationClockName(Telemetry& owner) {
		const std::size_t depth = OperationDepth()++;
		return "Buffer.Operation." + std::to_string(reinterpret_cast<std::uintptr_t>(&owner)) + "."
			+ std::to_string(OperationThreadId()) + "." + std::to_string(depth);
	}

	void Append(std::string& dest, const char* name, const StormByte::ByteSize value) {
		if (!dest.empty())
			dest.push_back(' ');
		dest += name;
		dest.push_back('=');
		dest += static_cast<std::string>(value);
	}

	void AppendRate(std::string& dest, const StormByte::ByteSize value) {
		Append(dest, "MeanRate", value);
		dest += "/s";
	}
}

Telemetry::Telemetry() noexcept:
	StormByte::Telemetry(),
	m_rate_bytes{0},
	m_op_us{0},
	m_mean_rate{0} {}

Telemetry::~Telemetry() noexcept = default;

Telemetry::OperationSample::OperationSample(Telemetry& owner):
	m_owner{owner},
	m_clock{owner.Clock(OperationClockName(owner))},
	m_before{m_clock.Time()},
	m_depth{&OperationDepth()} {
	m_clock.Start();
}

Telemetry::OperationSample::OperationSample(OperationSample&& other) noexcept:
	m_owner{other.m_owner},
	m_clock{other.m_clock},
	m_before{other.m_before},
	m_depth{std::exchange(other.m_depth, nullptr)},
	m_bytes{other.m_bytes},
	m_committed{other.m_committed} {
	other.m_committed = false;
}

Telemetry::OperationSample::~OperationSample() noexcept {
	if (!m_depth)
		return;
	m_clock.Stop();
	if (m_committed)
		m_owner.RecordOperation(m_bytes, m_clock.Time() - m_before);
	--*m_depth;
}

void Telemetry::OperationSample::Commit(const StormByte::ByteSize bytes) noexcept {
	m_bytes = bytes;
	m_committed = true;
}

StormByte::ByteSize Telemetry::MeanRate() const noexcept {
	return StormByte::ByteSize{static_cast<std::size_t>(m_mean_rate.load(std::memory_order_acquire))};
}

Telemetry::OperationSample Telemetry::MeasureOperation() {
	return OperationSample(*this);
}

void Telemetry::RecordOperation(const StormByte::ByteSize bytes,
		const std::chrono::microseconds elapsed) noexcept {
	const std::uint64_t add = static_cast<std::uint64_t>(static_cast<std::size_t>(bytes));
	const std::uint64_t us = static_cast<std::uint64_t>(elapsed.count() < 0 ? 0 : elapsed.count());
	const std::uint64_t total_bytes = m_rate_bytes.fetch_add(add, std::memory_order_relaxed) + add;
	const std::uint64_t total_us = m_op_us.fetch_add(us, std::memory_order_relaxed) + us;
	std::uint64_t rate = 0;
	if (total_us > 0)
		rate = (total_bytes * 1000000ull) / total_us;
	m_mean_rate.store(rate, std::memory_order_release);
}

ReadTelemetry::ReadTelemetry() noexcept:
	m_delivered{0} {}

ReadTelemetry::~ReadTelemetry() noexcept = default;

StormByte::ByteSize ReadTelemetry::Delivered() const noexcept {
	return m_delivered;
}

ReadTelemetry::operator StormByte::Safe::String() const {
	std::string text;
	Append(text, "Delivered", m_delivered);
	AppendRate(text, MeanRate());
	return StormByte::Safe::String(std::string_view(text));
}

WriteTelemetry::WriteTelemetry() noexcept:
	m_accepted{0} {}

WriteTelemetry::~WriteTelemetry() noexcept = default;

StormByte::ByteSize WriteTelemetry::Accepted() const noexcept {
	return m_accepted;
}

WriteTelemetry::operator StormByte::Safe::String() const {
	std::string text;
	Append(text, "Accepted", m_accepted);
	AppendRate(text, MeanRate());
	return StormByte::Safe::String(std::string_view(text));
}
