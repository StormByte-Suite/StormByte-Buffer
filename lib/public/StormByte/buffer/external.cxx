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

#include <StormByte/buffer/external.hxx>

#include <algorithm>
#include <iterator>

using namespace StormByte::Buffer;

ExternalReader::~ExternalReader() noexcept = default;

void ExternalReader::Seek(const std::ptrdiff_t offset, const Position mode) const noexcept {
	(void)offset;
	(void)mode;
}

void ExternalReader::Clean() noexcept {}

ExternalBufferReader::~ExternalBufferReader() noexcept = default;

ReadOnly& ExternalBufferReader::Store() noexcept {
	if (auto* handle = std::get_if<Consumer>(&m_store))
		return *handle;
	return std::get<std::reference_wrapper<ReadOnly>>(m_store).get();
}

const ReadOnly& ExternalBufferReader::Store() const noexcept {
	if (const auto* handle = std::get_if<Consumer>(&m_store))
		return *handle;
	return std::get<std::reference_wrapper<ReadOnly>>(m_store).get();
}

ExternalReader::PointerType ExternalBufferReader::Clone() const noexcept {
	return MakePointer<ExternalBufferReader>(*this);
}

ExternalReader::PointerType ExternalBufferReader::Move() noexcept {
	return MakePointer<ExternalBufferReader>(std::move(*this));
}

StormByte::ByteSize ExternalBufferReader::Available() const noexcept {
	return Store().Available();
}

bool ExternalBufferReader::Empty() const noexcept {
	return Store().Empty();
}

bool ExternalBufferReader::EoF() const noexcept {
	return Store().EoF();
}

bool ExternalBufferReader::IsReadable() const noexcept {
	return Store().IsReadable();
}

bool ExternalBufferReader::Read(const StormByte::ByteSize& count, BinaryData& out) const noexcept {
	return Store().Read(count, out);
}

bool ExternalBufferReader::Extract(const StormByte::ByteSize& count, BinaryData& out) noexcept {
	return Store().Extract(count, out);
}

bool ExternalBufferReader::Peek(const StormByte::ByteSize& count, BinaryData& out) const noexcept {
	return Store().Peek(count, out);
}

void ExternalBufferReader::ReadUntilEoF(BinaryData& out) const noexcept {
	Store().ReadUntilEoF(out);
}

void ExternalBufferReader::ExtractUntilEoF(BinaryData& out) noexcept {
	Store().ExtractUntilEoF(out);
}

void ExternalBufferReader::Seek(const std::ptrdiff_t offset, const Position mode) const noexcept {
	Store().Seek(offset, mode);
}

void ExternalBufferReader::Clean() noexcept {
	Store().Clean();
}

ExternalWriter::~ExternalWriter() noexcept = default;

ExternalBufferWriter::~ExternalBufferWriter() noexcept = default;

WriteOnly& ExternalBufferWriter::Store() noexcept {
	if (auto* handle = std::get_if<Producer>(&m_store))
		return *handle;
	return std::get<std::reference_wrapper<WriteOnly>>(m_store).get();
}

const WriteOnly& ExternalBufferWriter::Store() const noexcept {
	if (const auto* handle = std::get_if<Producer>(&m_store))
		return *handle;
	return std::get<std::reference_wrapper<WriteOnly>>(m_store).get();
}

ExternalWriter::PointerType ExternalBufferWriter::Clone() const noexcept {
	return MakePointer<ExternalBufferWriter>(*this);
}

ExternalWriter::PointerType ExternalBufferWriter::Move() noexcept {
	return MakePointer<ExternalBufferWriter>(std::move(*this));
}

bool ExternalWriter::Write(const std::string_view sv) noexcept {
	if (sv.empty())
		return Write(BinaryData{});
	BinaryData tmp;
	tmp.reserve(StormByte::ByteSize{sv.size()});
	std::transform(sv.begin(), sv.end(), std::back_inserter(tmp),
		[](char c) noexcept { return static_cast<std::byte>(c); });
	return Write(std::move(tmp));
}

bool ExternalWriter::Write(const char* s) noexcept {
	if (!s)
		return Write(BinaryData{});
	return Write(std::string_view(s));
}

bool ExternalWriter::Write(const StormByte::ByteSize& count, const std::string_view sv) noexcept {
	const StormByte::ByteSize to_write = (count == StormByte::ByteSize{0})
		? StormByte::ByteSize{sv.size()}
		: std::min(count, StormByte::ByteSize{sv.size()});
	BinaryData tmp;
	if (to_write > StormByte::ByteSize{0})
		tmp.reserve(to_write);
	std::transform(sv.begin(), sv.begin() + static_cast<std::ptrdiff_t>(static_cast<std::size_t>(to_write)),
		std::back_inserter(tmp),
		[](char c) noexcept { return static_cast<std::byte>(c); });
	return Write(std::move(tmp));
}

bool ExternalBufferWriter::IsWritable() const noexcept {
	return Store().IsWritable();
}

StormByte::ByteSize ExternalBufferWriter::Occupied() const noexcept {
	return Store().Size();
}

bool ExternalBufferWriter::Write(const BinaryData& data) noexcept {
	return Store().Write(StormByte::ByteSize{0}, data);
}

bool ExternalBufferWriter::Write(BinaryData&& data) noexcept {
	return Store().Write(StormByte::ByteSize{0}, std::move(data));
}

bool ExternalBufferWriter::Write(const StormByte::ByteSize& count, const BinaryData& data) noexcept {
	return Store().Write(count, data);
}

bool ExternalBufferWriter::Write(const StormByte::ByteSize& count, BinaryData&& data) noexcept {
	return Store().Write(count, std::move(data));
}

void ExternalBufferWriter::Close() noexcept {
	Store().Close();
}

void ExternalBufferWriter::SetError() noexcept {
	Store().SetError();
}
