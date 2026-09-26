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

#include <StormByte/buffer/consumer.hxx>
#include <StormByte/buffer/producer.hxx>

using namespace StormByte::Buffer;

Consumer::~Consumer() noexcept = default;

Producer Consumer::Producer() const noexcept {
	return StormByte::Buffer::Producer{m_buffer};
}

StormByte::ByteSize Consumer::Available() const noexcept {
	return m_buffer->Available();
}

const StormByte::BinaryData& Consumer::Data() const noexcept {
	return m_buffer->Data();
}

bool Consumer::Empty() const noexcept {
	return m_buffer->Empty();
}

bool Consumer::EoF() const noexcept {
	return m_buffer->EoF();
}

bool Consumer::IsReadable() const noexcept {
	return m_buffer->IsReadable();
}

StormByte::ByteSize Consumer::Size() const noexcept {
	return m_buffer->Size();
}

void Consumer::Clean() noexcept {
	m_buffer->Clean();
}

void Consumer::Clear() noexcept {
	m_buffer->Clear();
}

bool Consumer::Drop(const StormByte::ByteSize& count) noexcept {
	return m_buffer->Drop(count);
}

void Consumer::Seek(const std::ptrdiff_t& offset, const Position& mode) const noexcept {
	m_buffer->Seek(offset, mode);
}

bool Consumer::Extract(const StormByte::ByteSize& count, StormByte::BinaryData& out) noexcept {
	return m_buffer->Extract(count, out);
}

bool Consumer::Extract(const StormByte::ByteSize& count, WriteOnly& out) noexcept {
	return m_buffer->Extract(count, out);
}

void Consumer::ExtractUntilEoF(StormByte::BinaryData& out) noexcept {
	m_buffer->ExtractUntilEoF(out);
}

void Consumer::ExtractUntilEoF(WriteOnly& out) noexcept {
	m_buffer->ExtractUntilEoF(out);
}

bool Consumer::Read(const StormByte::ByteSize& count, StormByte::BinaryData& out) const noexcept {
	return m_buffer->Read(count, out);
}

bool Consumer::Read(const StormByte::ByteSize& count, WriteOnly& out) const noexcept {
	return m_buffer->Read(count, out);
}

void Consumer::ReadUntilEoF(StormByte::BinaryData& out) const noexcept {
	m_buffer->ReadUntilEoF(out);
}

void Consumer::ReadUntilEoF(WriteOnly& out) const noexcept {
	m_buffer->ReadUntilEoF(out);
}

bool Consumer::Peek(const StormByte::ByteSize& count, StormByte::BinaryData& out) const noexcept {
	return m_buffer->Peek(count, out);
}

bool Consumer::Peek(const StormByte::ByteSize& count, WriteOnly& out) const noexcept {
	return m_buffer->Peek(count, out);
}
