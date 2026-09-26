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

#include <StormByte/buffer/io/buffered_location_writer.hxx>
#include <StormByte/buffer/io/backend/buffered_location_writer.hxx>

#include <utility>

using namespace StormByte::Buffer::IO;

namespace {
	const StormByte::String::String& EmptyLocation() noexcept {
		static const StormByte::String::String empty;
		return empty;
	}

	constexpr std::size_t DefaultBackPressure = 4;
	constexpr StormByte::ByteSize DefaultMaxMemory{1024ull * 1024ull};
}

BufferedLocationWriter::BufferedLocationWriter(StormByte::String::String location,
		const StormByte::ByteSize write_chunk, const std::size_t back_pressure,
		const std::chrono::milliseconds max_wait, const StormByte::ByteSize max_memory, const bool probe):
	BufferedWriter(write_chunk, back_pressure, max_wait, max_memory),
	m_io(std::make_unique<Backend::BufferedLocationWriter>(std::move(location), probe)) {}

BufferedLocationWriter::BufferedLocationWriter(BufferedLocationWriter&& other) noexcept:
	BufferedWriter(std::move(other)),
	m_io(std::move(other.m_io)) {}

BufferedLocationWriter::~BufferedLocationWriter() noexcept = default;

BufferedLocationWriter& BufferedLocationWriter::operator=(BufferedLocationWriter&& other) noexcept {
	if (this != &other) {
		BufferedWriter::operator=(std::move(other));
		m_io = std::move(other.m_io);
	}
	return *this;
}

const StormByte::String::String& BufferedLocationWriter::Location() const noexcept {
	return m_io ? m_io->Location() : EmptyLocation();
}

const StormByte::String::String& BufferedLocationWriter::Path() const noexcept {
	return Location();
}

StormByte::System::Device BufferedLocationWriter::Device() const {
	return OriginDevice();
}

bool BufferedLocationWriter::IsSeekable() const noexcept {
	return true;
}

bool BufferedLocationWriter::IsSized() const noexcept {
	return true;
}

StormByte::ByteSize BufferedLocationWriter::Size() const noexcept {
	return OriginSize();
}

void BufferedLocationWriter::Setup() {
	if (!m_io || !m_io->Probe())
		return;
	const auto device = Device();
	if (!device)
		WriteChunk(StormByte::ByteSize{0});
	else
		WriteChunk(device.Window().write);
	BackPressure(DefaultBackPressure);
	MaxMemory(DefaultMaxMemory);
}
