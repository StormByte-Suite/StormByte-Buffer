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

#include <StormByte/buffer/io/buffered_location_reader.hxx>
#include <StormByte/buffer/io/backend/buffered_location_reader.hxx>

#include <utility>

using namespace StormByte::Buffer::IO;

namespace {
	const StormByte::String::String& EmptyLocation() noexcept {
		static const StormByte::String::String empty;
		return empty;
	}
}

BufferedLocationReader::BufferedLocationReader(StormByte::String::String location,
		const StormByte::ByteSize read_ahead, const StormByte::ByteSize max_memory, const bool probe):
	BufferedReader(read_ahead, max_memory),
	m_io(std::make_unique<Backend::BufferedLocationReader>(std::move(location), probe)) {}

BufferedLocationReader::BufferedLocationReader(BufferedLocationReader&& other) noexcept:
	BufferedReader(std::move(other)),
	m_io(std::move(other.m_io)) {}

BufferedLocationReader::~BufferedLocationReader() noexcept = default;

BufferedLocationReader& BufferedLocationReader::operator=(BufferedLocationReader&& other) noexcept {
	if (this != &other) {
		BufferedReader::operator=(std::move(other));
		m_io = std::move(other.m_io);
	}
	return *this;
}

const StormByte::String::String& BufferedLocationReader::Location() const noexcept {
	return m_io ? m_io->Location() : EmptyLocation();
}

StormByte::System::Device BufferedLocationReader::Device() const {
	return OriginDevice();
}

bool BufferedLocationReader::OriginCanSeek() const noexcept {
	return true;
}

bool BufferedLocationReader::OriginHasSize() const noexcept {
	return true;
}

void BufferedLocationReader::Setup() {
	if (!m_io || !m_io->Probe())
		return;
	const auto device = Device();
	if (!device) {
		ReadAhead(StormByte::ByteSize{0});
		return;
	}
	ReadAhead(device.Window().read);
}
