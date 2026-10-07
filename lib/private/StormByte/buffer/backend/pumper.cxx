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
#include <StormByte/safe/unique_lock.hxx>

#include <chrono>
#include <utility>

using namespace StormByte::Buffer::Backend;

namespace {
	constexpr StormByte::ByteSize kAutoChunk{64 * 1024};
	constexpr StormByte::ByteSize kDefaultNonIoHighWater{1 * 1024 * 1024};
}

Pumper::Pumper(StormByte::Buffer::Bridge&& bridge, const StormByte::ByteSize chunk,
		const StormByte::Safe::Optional<StormByte::ByteSize> high_water):
	m_bridge(std::move(bridge)),
	m_chunk(chunk) {
	m_read = m_bridge.ReadTelemetry();
	m_write = m_bridge.WriteTelemetry();
	m_io_in_blocking = m_bridge.InputPullBlocking();
	if (high_water.has_value())
		m_high_water = *high_water;
	else
		m_high_water = m_bridge.InputIsIO() ? StormByte::ByteSize{0} : kDefaultNonIoHighWater;
	m_worker = StormByte::Safe::Thread([this] { Worker(); });
}

Pumper::~Pumper() {
	m_cv.notify_all();
	if (m_worker.joinable())
		m_worker.join();
}

void Pumper::Cancel() noexcept {
	StormByte::Safe::UniqueLock lock(m_mutex);
	if (m_canceled)
		return;
	m_canceled = true;
	m_bridge.Close();
	m_cv.notify_all();
}

bool Pumper::Canceled() const noexcept {
	StormByte::Safe::UniqueLock lock(m_mutex);
	return m_canceled;
}

bool Pumper::EoF() const noexcept {
	StormByte::Safe::UniqueLock lock(m_mutex);
	return m_bridge.EoF();
}

bool Pumper::Failed() const noexcept {
	StormByte::Safe::UniqueLock lock(m_mutex);
	return m_bridge.Failed();
}

void Pumper::Toggle() noexcept {
	StormByte::Safe::UniqueLock lock(m_mutex);
	if (m_canceled || m_bridge.Failed())
		return;
	m_paused = !m_paused;
	m_cv.notify_all();
}

const StormByte::Safe::Shared<StormByte::Buffer::ReadTelemetry> Pumper::ReadTelemetry() const noexcept {
	return m_read;
}

const StormByte::Safe::Shared<StormByte::Buffer::WriteTelemetry> Pumper::WriteTelemetry() const noexcept {
	return m_write;
}

StormByte::ByteSize Pumper::CycleRequest() const noexcept {
	StormByte::ByteSize want = m_chunk == StormByte::ByteSize{0} ? kAutoChunk : m_chunk;
	if (want == StormByte::ByteSize{0})
		want = kAutoChunk;
	if (m_high_water > StormByte::ByteSize{0} && m_high_water < want)
		want = m_high_water;
	return want;
}

void Pumper::Worker() {
	for (;;) {
		{
			StormByte::Safe::UniqueLock lock(m_mutex);
			m_cv.wait(lock, [this] {
				return m_canceled || !m_paused || m_bridge.Failed();
			});
			if (m_canceled || m_bridge.Failed())
				return;
			if (m_paused)
				continue;
		}

		if (m_canceled || m_bridge.Failed())
			return;

		const StormByte::ByteSize want = CycleRequest();
		const auto op = m_io_in_blocking
			? StormByte::Buffer::Bridge::Operation::Blocking
			: StormByte::Buffer::Bridge::Operation::NonBlocking;
		const StormByte::ByteSize got = m_bridge.Passthrough(want, op);
		if (m_canceled || m_bridge.Failed())
			return;
		if (m_bridge.EoF())
			return;
		if (got == StormByte::ByteSize{0}) {
			StormByte::Safe::UniqueLock lock(m_mutex);
			if (m_canceled || m_paused || m_bridge.Failed())
				continue;
			m_cv.wait_for(lock, std::chrono::milliseconds(1), [this] {
				return m_canceled || m_paused || m_bridge.Failed();
			});
		}
	}
}
