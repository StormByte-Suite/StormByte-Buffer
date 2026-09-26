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

#include <StormByte/buffer/generic.hxx>

using namespace StormByte::Buffer;

Generic::~Generic() noexcept = default;

ReadOnly::~ReadOnly() noexcept = default;

WriteOnly::~WriteOnly() noexcept = default;

ReadWrite::~ReadWrite() noexcept = default;

namespace StormByte::Buffer {
	template BinaryData STORMBYTE_BUFFER_INSTANTIATE Generic::DataConvert<BinaryData>(const BinaryData&) noexcept;
	template BinaryData STORMBYTE_BUFFER_INSTANTIATE Generic::DataConvert<BinaryData>(BinaryData&&) noexcept;
	template BinaryData STORMBYTE_BUFFER_INSTANTIATE Generic::DataConvert<std::span<const std::byte>>(const std::span<const std::byte>&) noexcept;
	template BinaryData STORMBYTE_BUFFER_INSTANTIATE Generic::DataConvert<std::span<std::byte>>(const std::span<std::byte>&) noexcept;
	template BinaryData STORMBYTE_BUFFER_INSTANTIATE Generic::DataConvert<std::span<std::byte>>(std::span<std::byte>&&) noexcept;

	template bool STORMBYTE_BUFFER_INSTANTIATE WriteOnly::Write<BinaryData>(const BinaryData&) noexcept;
	template bool STORMBYTE_BUFFER_INSTANTIATE WriteOnly::Write<BinaryData>(BinaryData&&) noexcept;
	template bool STORMBYTE_BUFFER_INSTANTIATE WriteOnly::Write<BinaryData>(const StormByte::ByteSize&, const BinaryData&) noexcept;
	template bool STORMBYTE_BUFFER_INSTANTIATE WriteOnly::Write<BinaryData>(const StormByte::ByteSize&, BinaryData&&) noexcept;
	template bool STORMBYTE_BUFFER_INSTANTIATE WriteOnly::Write<std::span<const std::byte>>(const std::span<const std::byte>&) noexcept;
	template bool STORMBYTE_BUFFER_INSTANTIATE WriteOnly::Write<std::span<const std::byte>>(const StormByte::ByteSize&, const std::span<const std::byte>&) noexcept;
	template bool STORMBYTE_BUFFER_INSTANTIATE WriteOnly::Write<std::span<std::byte>>(const std::span<std::byte>&) noexcept;
	template bool STORMBYTE_BUFFER_INSTANTIATE WriteOnly::Write<std::span<std::byte>>(const StormByte::ByteSize&, const std::span<std::byte>&) noexcept;
	template bool STORMBYTE_BUFFER_INSTANTIATE WriteOnly::Write<BinaryData::const_iterator, BinaryData::const_iterator>(BinaryData::const_iterator, BinaryData::const_iterator) noexcept;
	template bool STORMBYTE_BUFFER_INSTANTIATE WriteOnly::Write<BinaryData::iterator, BinaryData::iterator>(BinaryData::iterator, BinaryData::iterator) noexcept;
	template bool STORMBYTE_BUFFER_INSTANTIATE WriteOnly::Write<BinaryData::const_iterator, BinaryData::const_iterator>(const StormByte::ByteSize&, BinaryData::const_iterator, BinaryData::const_iterator) noexcept;
	template bool STORMBYTE_BUFFER_INSTANTIATE WriteOnly::Write<BinaryData::iterator, BinaryData::iterator>(const StormByte::ByteSize&, BinaryData::iterator, BinaryData::iterator) noexcept;
}
