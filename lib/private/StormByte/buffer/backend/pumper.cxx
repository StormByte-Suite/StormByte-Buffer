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

#include <StormByte/buffer/backend/pumper.hxx>

using namespace StormByte::Buffer::Backend;

namespace {
	constexpr StormByte::ByteSize kAutoChunk{64 * 1024};
	constexpr StormByte::ByteSize kDefaultNonIoHighWater{1 * 1024 * 1024};
}

Pumper::Pumper(StormByte::Buffer::Bridge&& bridge, const StormByte::ByteSize chunk,
		const std::optional<StormByte::ByteSize> high_water):
	m_bridge(std::move(bridge)),
	m_chunk(chunk) {
	if (high_water.has_value())
		m_high_water = *high_water;
	else
		m_high_water = m_bridge.InputIsIO() ? StormByte::ByteSize{0} : kDefaultNonIoHighWater;
	m_worker = std::thread(&Pumper::Worker, this);
}

Pumper::~Pumper() {
	m_stop.store(true);
	m_cv.notify_all();
	if (m_worker.joinable())
		m_worker.join();
}

bool Pumper::EoF() const noexcept {
	std::lock_guard lock(m_mutex);
	return m_bridge.EoF();
}

bool Pumper::Failed() const noexcept {
	std::lock_guard lock(m_mutex);
	return m_failed || m_bridge.Failed();
}

void Pumper::Toggle() noexcept {
	std::lock_guard lock(m_mutex);
	if (m_failed || m_bridge.Failed())
		return;
	m_paused = !m_paused;
	m_cv.notify_all();
}

void Pumper::Cancel() noexcept {
	std::lock_guard lock(m_mutex);
	m_failed = true;
	m_stop.store(true);
	m_cv.notify_all();
}

const StormByte::Shared<StormByte::Buffer::ReadTelemetry> Pumper::ReadTelemetry() const noexcept {
	return m_bridge.ReadTelemetry();
}

const StormByte::Shared<StormByte::Buffer::WriteTelemetry> Pumper::WriteTelemetry() const noexcept {
	return m_bridge.WriteTelemetry();
}

StormByte::ByteSize Pumper::CycleRequest() const noexcept {
	StormByte::ByteSize want = m_chunk == StormByte::ByteSize{0} ? kAutoChunk : m_chunk;
	if (m_high_water > StormByte::ByteSize{0} && m_high_water < want)
		want = m_high_water;
	return want;
}

void Pumper::Worker() {
	for (;;) {
		{
			std::unique_lock lock(m_mutex);
			m_cv.wait(lock, [this] {
				return m_stop.load() || !m_paused || m_failed || m_bridge.Failed();
			});
			if (m_stop.load() || m_failed || m_bridge.Failed())
				return;
		}

		if (m_bridge.EoF() || m_bridge.Failed()) {
			std::lock_guard lock(m_mutex);
			if (m_bridge.Failed())
				m_failed = true;
			return;
		}

		const StormByte::ByteSize want = CycleRequest();
		const StormByte::ByteSize got = m_bridge.Passthrough(want, StormByte::Buffer::Bridge::Operation::Blocking);
		if (m_bridge.Failed()) {
			std::lock_guard lock(m_mutex);
			m_failed = true;
			return;
		}
		if (got == StormByte::ByteSize{0} && m_bridge.EoF())
			return;
	}
}
