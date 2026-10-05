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

#include <StormByte/buffer/producer.hxx>
#include <StormByte/buffer/ring_owner.hxx>

using namespace StormByte::Buffer;

Producer::Producer():
	m_buffer{Backend::MakeRingOwner()} {}

Producer::Producer(const StormByte::Buffer::Consumer& consumer):
	m_buffer{consumer.m_buffer} {}

Producer::Producer(const Producer& other):
	Generic(other),
	WriteOnly(other),
	m_buffer{other.m_buffer} {}

Producer::Producer(Producer&& other) noexcept = default;

Producer::~Producer() noexcept = default;

Producer& Producer::operator=(const Producer& other) {
	if (this != &other)
		m_buffer = other.m_buffer;
	return *this;
}

Producer& Producer::operator=(Producer&& other) noexcept = default;

bool Producer::operator==(const Producer& other) const noexcept {
	return Backend::GetRing(m_buffer) == Backend::GetRing(other.m_buffer);
}

Ring& Producer::Storage() const noexcept {
	return *Backend::GetRing(m_buffer);
}

StormByte::Buffer::Consumer Producer::Consumer() {
	return StormByte::Buffer::Consumer{m_buffer};
}

void Producer::Close() noexcept {
	Storage().Close();
}

void Producer::SetError() noexcept {
	Storage().SetError();
}

bool Producer::IsWritable() const noexcept {
	return Storage().IsWritable();
}

StormByte::ByteSize Producer::Size() const noexcept {
	return Storage().Size();
}

bool Producer::Write(const StormByte::ByteSize& count, const BinaryData& data) noexcept {
	return Storage().Write(count, data);
}

bool Producer::Write(const StormByte::ByteSize& count, BinaryData&& data) noexcept {
	return Storage().Write(count, std::move(data));
}

bool Producer::Write(const StormByte::ByteSize& count, const ReadOnly& data) noexcept {
	return Storage().Write(count, data);
}

bool Producer::Write(const StormByte::ByteSize& count, ReadOnly&& data) noexcept {
	return Storage().Write(count, std::move(data));
}
