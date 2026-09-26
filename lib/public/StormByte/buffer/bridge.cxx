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

#include <StormByte/buffer/bridge.hxx>
#include <StormByte/buffer/io/backend/bridge.hxx>

using namespace StormByte::Buffer;

Bridge::Bridge(ExternalReader& in, ExternalWriter& out, const StormByte::ByteSize high_water) noexcept:
	m_io(std::make_unique<IO::Backend::Bridge>(in, out, high_water)) {}

Bridge::Bridge(const IO::BufferedReader& in, IO::BufferedWriter& out, const StormByte::ByteSize high_water) noexcept:
	m_io(std::make_unique<IO::Backend::Bridge>(in, out, high_water)) {}

Bridge::Bridge(const IO::BufferedReader& in, IO::BufferedWriter& out) noexcept:
	Bridge(in, out, StormByte::ByteSize{0}) {}

Bridge::Bridge(ExternalReader& in, IO::BufferedWriter& out, const StormByte::ByteSize high_water) noexcept:
	m_io(std::make_unique<IO::Backend::Bridge>(in, out, high_water)) {}

Bridge::Bridge(ExternalReader& in, IO::BufferedWriter& out) noexcept:
	Bridge(in, out, StormByte::ByteSize{0}) {}

Bridge::Bridge(const IO::BufferedReader& in, ExternalWriter& out, const StormByte::ByteSize high_water) noexcept:
	m_io(std::make_unique<IO::Backend::Bridge>(in, out, high_water)) {}

Bridge::Bridge(Bridge&& other) noexcept:
	m_io(std::move(other.m_io)) {}

Bridge::~Bridge() noexcept = default;

Bridge& Bridge::operator=(Bridge&& other) noexcept {
	if (this != &other)
		m_io = std::move(other.m_io);
	return *this;
}

bool Bridge::EoF() const noexcept {
	return m_io && m_io->EoF();
}

bool Bridge::IsReadable() const noexcept {
	return m_io && m_io->IsReadable();
}

bool Bridge::IsWritable() const noexcept {
	return m_io && m_io->IsWritable();
}

StormByte::ByteSize Bridge::HighWater() const noexcept {
	return m_io ? m_io->HighWater() : StormByte::ByteSize{0};
}

void Bridge::HighWater(const StormByte::ByteSize high_water) noexcept {
	if (m_io)
		m_io->HighWater(high_water);
}

IO::Drainer::Status Bridge::Drainer() const noexcept {
	return m_io ? m_io->Drainer() : IO::Drainer::Status::Stopped;
}

bool Bridge::Drainer(const IO::Drainer::Operation operation) noexcept {
	return m_io && m_io->Drainer(operation);
}

bool Bridge::Flush() noexcept {
	return m_io && m_io->BarrierFlush();
}

bool Bridge::FlushAndClose() noexcept {
	return m_io && m_io->FlushAndClose();
}

void Bridge::SetError() noexcept {
	if (m_io)
		m_io->SetError();
}
