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

#include <StormByte/buffer/io/telemetry.hxx>
#include <StormByte/byte_size.hxx>

#include <string>
#include <string_view>

using StormByte::Buffer::IO::ReadTelemetry;
using StormByte::Buffer::IO::WriteTelemetry;

namespace {
	void Append(std::string& dest, const char* name, const StormByte::ByteSize value) {
		if (!dest.empty())
			dest.push_back(' ');
		dest += name;
		dest.push_back('=');
		dest += static_cast<std::string>(value);
	}

	void Append(std::string& dest, const char* name, const std::uint64_t value) {
		if (!dest.empty())
			dest.push_back(' ');
		dest += name;
		dest.push_back('=');
		dest += std::to_string(value);
	}

	void AppendNs(std::string& dest, const char* name, const std::chrono::nanoseconds value) {
		Append(dest, name, static_cast<std::uint64_t>(value.count()));
	}
}

ReadTelemetry::ReadTelemetry() noexcept:
	m_hit_ahead{0},
	m_hit_back{0},
	m_miss{0},
	m_origin{0},
	m_cached{0},
	m_cached_peak{0},
	m_cap{0},
	m_seek_logical{0},
	m_seek_origin{0},
	m_seek_saved_full{0},
	m_seek_saved_partial{0},
	m_try_again{0},
	m_saturated{0},
	m_evicted{0},
	m_wait_min{},
	m_wait_max{},
	m_wait_total{},
	m_wait_samples{0} {}

ReadTelemetry::~ReadTelemetry() noexcept = default;

StormByte::ByteSize ReadTelemetry::HitAhead() const noexcept {
	return m_hit_ahead;
}

StormByte::ByteSize ReadTelemetry::HitBack() const noexcept {
	return m_hit_back;
}

StormByte::ByteSize ReadTelemetry::Miss() const noexcept {
	return m_miss;
}

StormByte::ByteSize ReadTelemetry::Origin() const noexcept {
	return m_origin;
}

StormByte::ByteSize ReadTelemetry::Cached() const noexcept {
	return m_cached;
}

StormByte::ByteSize ReadTelemetry::CachedPeak() const noexcept {
	return m_cached_peak;
}

StormByte::ByteSize ReadTelemetry::Cap() const noexcept {
	return m_cap;
}

std::size_t ReadTelemetry::SeekLogical() const noexcept {
	return m_seek_logical;
}

std::size_t ReadTelemetry::SeekOrigin() const noexcept {
	return m_seek_origin;
}

std::size_t ReadTelemetry::SeekSavedFull() const noexcept {
	return m_seek_saved_full;
}

std::size_t ReadTelemetry::SeekSavedPartial() const noexcept {
	return m_seek_saved_partial;
}

std::size_t ReadTelemetry::TryAgain() const noexcept {
	return m_try_again;
}

std::size_t ReadTelemetry::Saturated() const noexcept {
	return m_saturated;
}

std::size_t ReadTelemetry::Evicted() const noexcept {
	return m_evicted;
}

std::chrono::nanoseconds ReadTelemetry::WaitMin() const noexcept {
	return m_wait_min;
}

std::chrono::nanoseconds ReadTelemetry::WaitMax() const noexcept {
	return m_wait_max;
}

std::chrono::nanoseconds ReadTelemetry::WaitTotal() const noexcept {
	return m_wait_total;
}

std::size_t ReadTelemetry::WaitSamples() const noexcept {
	return m_wait_samples;
}

ReadTelemetry::operator StormByte::Safe::String() const {
	std::string text = static_cast<std::string>(
		StormByte::Buffer::ReadTelemetry::operator StormByte::Safe::String());
	Append(text, "HitAhead", m_hit_ahead);
	Append(text, "HitBack", m_hit_back);
	Append(text, "Miss", m_miss);
	Append(text, "Origin", m_origin);
	Append(text, "Cached", m_cached);
	Append(text, "CachedPeak", m_cached_peak);
	Append(text, "Cap", m_cap);
	Append(text, "SeekLogical", static_cast<std::uint64_t>(m_seek_logical));
	Append(text, "SeekOrigin", static_cast<std::uint64_t>(m_seek_origin));
	Append(text, "SeekSavedFull", static_cast<std::uint64_t>(m_seek_saved_full));
	Append(text, "SeekSavedPartial", static_cast<std::uint64_t>(m_seek_saved_partial));
	Append(text, "TryAgain", static_cast<std::uint64_t>(m_try_again));
	Append(text, "Saturated", static_cast<std::uint64_t>(m_saturated));
	Append(text, "Evicted", static_cast<std::uint64_t>(m_evicted));
	AppendNs(text, "WaitMin", m_wait_min);
	AppendNs(text, "WaitMax", m_wait_max);
	AppendNs(text, "WaitTotal", m_wait_total);
	Append(text, "WaitSamples", static_cast<std::uint64_t>(m_wait_samples));
	return StormByte::Safe::String(std::string_view(text));
}

WriteTelemetry::WriteTelemetry() noexcept:
	m_behind{0},
	m_direct{0},
	m_origin{0},
	m_materialized{0},
	m_high_water{0},
	m_hit_ahead{0},
	m_hit_back{0},
	m_miss{0},
	m_dirty{0},
	m_dirty_peak{0},
	m_cap{0},
	m_seek_logical{0},
	m_seek_origin{0},
	m_seek_saved_full{0},
	m_seek_saved_partial{0},
	m_try_again{0},
	m_saturated{0},
	m_evicted{0},
	m_wait_min{},
	m_wait_max{},
	m_wait_total{},
	m_wait_samples{0} {}

WriteTelemetry::~WriteTelemetry() noexcept = default;

StormByte::ByteSize WriteTelemetry::Behind() const noexcept {
	return m_behind;
}

StormByte::ByteSize WriteTelemetry::Direct() const noexcept {
	return m_direct;
}

StormByte::ByteSize WriteTelemetry::Origin() const noexcept {
	return m_origin;
}

StormByte::ByteSize WriteTelemetry::Materialized() const noexcept {
	return m_materialized;
}

StormByte::ByteSize WriteTelemetry::HighWater() const noexcept {
	return m_high_water;
}

StormByte::ByteSize WriteTelemetry::HitAhead() const noexcept {
	return m_hit_ahead;
}

StormByte::ByteSize WriteTelemetry::HitBack() const noexcept {
	return m_hit_back;
}

StormByte::ByteSize WriteTelemetry::Miss() const noexcept {
	return m_miss;
}

StormByte::ByteSize WriteTelemetry::Dirty() const noexcept {
	return m_dirty;
}

StormByte::ByteSize WriteTelemetry::DirtyPeak() const noexcept {
	return m_dirty_peak;
}

StormByte::ByteSize WriteTelemetry::Cap() const noexcept {
	return m_cap;
}

std::size_t WriteTelemetry::SeekLogical() const noexcept {
	return m_seek_logical;
}

std::size_t WriteTelemetry::SeekOrigin() const noexcept {
	return m_seek_origin;
}

std::size_t WriteTelemetry::SeekSavedFull() const noexcept {
	return m_seek_saved_full;
}

std::size_t WriteTelemetry::SeekSavedPartial() const noexcept {
	return m_seek_saved_partial;
}

std::size_t WriteTelemetry::TryAgain() const noexcept {
	return m_try_again;
}

std::size_t WriteTelemetry::Saturated() const noexcept {
	return m_saturated;
}

std::size_t WriteTelemetry::Evicted() const noexcept {
	return m_evicted;
}

std::chrono::nanoseconds WriteTelemetry::WaitMin() const noexcept {
	return m_wait_min;
}

std::chrono::nanoseconds WriteTelemetry::WaitMax() const noexcept {
	return m_wait_max;
}

std::chrono::nanoseconds WriteTelemetry::WaitTotal() const noexcept {
	return m_wait_total;
}

std::size_t WriteTelemetry::WaitSamples() const noexcept {
	return m_wait_samples;
}

WriteTelemetry::operator StormByte::Safe::String() const {
	std::string text = static_cast<std::string>(
		StormByte::Buffer::WriteTelemetry::operator StormByte::Safe::String());
	Append(text, "Behind", m_behind);
	Append(text, "Direct", m_direct);
	Append(text, "Origin", m_origin);
	Append(text, "Materialized", m_materialized);
	Append(text, "HighWater", m_high_water);
	Append(text, "HitAhead", m_hit_ahead);
	Append(text, "HitBack", m_hit_back);
	Append(text, "Miss", m_miss);
	Append(text, "Dirty", m_dirty);
	Append(text, "DirtyPeak", m_dirty_peak);
	Append(text, "Cap", m_cap);
	Append(text, "SeekLogical", static_cast<std::uint64_t>(m_seek_logical));
	Append(text, "SeekOrigin", static_cast<std::uint64_t>(m_seek_origin));
	Append(text, "SeekSavedFull", static_cast<std::uint64_t>(m_seek_saved_full));
	Append(text, "SeekSavedPartial", static_cast<std::uint64_t>(m_seek_saved_partial));
	Append(text, "TryAgain", static_cast<std::uint64_t>(m_try_again));
	Append(text, "Saturated", static_cast<std::uint64_t>(m_saturated));
	Append(text, "Evicted", static_cast<std::uint64_t>(m_evicted));
	AppendNs(text, "WaitMin", m_wait_min);
	AppendNs(text, "WaitMax", m_wait_max);
	AppendNs(text, "WaitTotal", m_wait_total);
	Append(text, "WaitSamples", static_cast<std::uint64_t>(m_wait_samples));
	return StormByte::Safe::String(std::string_view(text));
}
