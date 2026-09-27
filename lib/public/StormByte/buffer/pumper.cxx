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
#include <StormByte/buffer/pumper.hxx>

using namespace StormByte::Buffer;

Pumper::Pumper(Bridge&& bridge, const StormByte::ByteSize chunk,
		const std::optional<StormByte::ByteSize> high_water):
	m_backend(std::make_unique<Backend::Pumper>(std::move(bridge), chunk, high_water)) {}

Pumper::Pumper(Pumper&& other) noexcept:
	m_backend(std::move(other.m_backend)) {}

Pumper::~Pumper() noexcept = default;

Pumper& Pumper::operator=(Pumper&& other) noexcept {
	if (this != &other)
		m_backend = std::move(other.m_backend);
	return *this;
}

bool Pumper::EoF() const noexcept {
	return !m_backend || m_backend->EoF();
}

bool Pumper::Failed() const noexcept {
	return !m_backend || m_backend->Failed();
}

void Pumper::Toggle() noexcept {
	if (m_backend)
		m_backend->Toggle();
}

void Pumper::Cancel() noexcept {
	if (m_backend)
		m_backend->Cancel();
}

const StormByte::Shared<StormByte::Buffer::ReadTelemetry> Pumper::ReadTelemetry() const noexcept {
	if (!m_backend)
		return {};
	return m_backend->ReadTelemetry();
}

const StormByte::Shared<StormByte::Buffer::WriteTelemetry> Pumper::WriteTelemetry() const noexcept {
	if (!m_backend)
		return {};
	return m_backend->WriteTelemetry();
}
