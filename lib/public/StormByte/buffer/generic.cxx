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

#include <algorithm>
#include <span>
#include <string_view>

using namespace StormByte::Buffer;

Generic::Generic() noexcept = default;
Generic::Generic(const Generic& other) noexcept = default;
Generic::Generic(Generic&& other) noexcept = default;
Generic::~Generic() noexcept = default;
Generic& Generic::operator=(const Generic& other) noexcept = default;
Generic& Generic::operator=(Generic&& other) noexcept = default;

ReadOnly::ReadOnly() noexcept = default;
ReadOnly::ReadOnly(const ReadOnly& other) noexcept = default;
ReadOnly::ReadOnly(ReadOnly&& other) noexcept = default;
ReadOnly::~ReadOnly() noexcept = default;
ReadOnly& ReadOnly::operator=(const ReadOnly& other) noexcept = default;
ReadOnly& ReadOnly::operator=(ReadOnly&& other) noexcept {
	Generic::operator=(std::move(other));
	return *this;
}

WriteOnly::WriteOnly() noexcept = default;
WriteOnly::WriteOnly(const WriteOnly& other) noexcept = default;
WriteOnly::WriteOnly(WriteOnly&& other) noexcept = default;
WriteOnly::~WriteOnly() noexcept = default;
WriteOnly& WriteOnly::operator=(const WriteOnly& other) noexcept = default;
WriteOnly& WriteOnly::operator=(WriteOnly&& other) noexcept {
	Generic::operator=(std::move(other));
	return *this;
}

ReadWrite::ReadWrite() noexcept = default;
ReadWrite::ReadWrite(const ReadWrite& other) noexcept = default;
ReadWrite::ReadWrite(ReadWrite&& other) noexcept = default;
ReadWrite::~ReadWrite() noexcept = default;
ReadWrite& ReadWrite::operator=(const ReadWrite& other) noexcept = default;
ReadWrite& ReadWrite::operator=(ReadWrite&& other) noexcept {
	Generic::operator=(std::move(other));
	return *this;
}

StormByte::Safe::Binary Generic::DataConvert(const std::string_view sv) noexcept {
	StormByte::Safe::Binary out;
	if (!sv.empty())
		out.reserve(StormByte::ByteSize{sv.size()});
	std::transform(sv.begin(), sv.end(), std::back_inserter(out),
		[](char character) noexcept { return static_cast<std::byte>(character); });
	return out;
}

StormByte::Safe::Binary Generic::DataConvert(const char* source) noexcept {
	if (!source)
		return StormByte::Safe::Binary{};
	return DataConvert(std::string_view(source));
}

bool WriteOnly::Write(const std::string_view source) noexcept {
	StormByte::Safe::Binary converted = DataConvert(source);
	return Write(converted.size(), std::move(converted));
}

bool WriteOnly::Write(const char* source) noexcept {
	if (!source)
		return Write(StormByte::Safe::Binary{});
	return Write(std::string_view(source));
}

bool WriteOnly::Write(const StormByte::ByteSize& count, const std::string_view source) noexcept {
	const StormByte::ByteSize to_write = (count == StormByte::ByteSize{0})
		? StormByte::ByteSize{source.size()}
		: std::min(count, StormByte::ByteSize{source.size()});
	StormByte::Safe::Binary converted = DataConvert(source.substr(0, static_cast<std::size_t>(to_write)));
	return Write(to_write, std::move(converted));
}

bool WriteOnly::Write(const StormByte::ByteSize& count, const char* source) noexcept {
	if (!source)
		return Write(count, StormByte::Safe::Binary{});
	return Write(count, std::string_view(source));
}

namespace StormByte::Buffer {
	template StormByte::Safe::Binary STORMBYTE_BUFFER_INSTANTIATE Generic::DataConvert<StormByte::Safe::Binary>(const StormByte::Safe::Binary&) noexcept;
	template StormByte::Safe::Binary STORMBYTE_BUFFER_INSTANTIATE Generic::DataConvert<StormByte::Safe::Binary>(StormByte::Safe::Binary&&) noexcept;
	template StormByte::Safe::Binary STORMBYTE_BUFFER_INSTANTIATE Generic::DataConvert<std::span<const std::byte>>(const std::span<const std::byte>&) noexcept;
	template StormByte::Safe::Binary STORMBYTE_BUFFER_INSTANTIATE Generic::DataConvert<std::span<std::byte>>(const std::span<std::byte>&) noexcept;
	template StormByte::Safe::Binary STORMBYTE_BUFFER_INSTANTIATE Generic::DataConvert<std::span<std::byte>>(std::span<std::byte>&&) noexcept;

	template bool STORMBYTE_BUFFER_INSTANTIATE WriteOnly::Write<StormByte::Safe::Binary>(const StormByte::Safe::Binary&) noexcept;
	template bool STORMBYTE_BUFFER_INSTANTIATE WriteOnly::Write<StormByte::Safe::Binary>(StormByte::Safe::Binary&&) noexcept;
	template bool STORMBYTE_BUFFER_INSTANTIATE WriteOnly::Write<StormByte::Safe::Binary>(const StormByte::ByteSize&, const StormByte::Safe::Binary&) noexcept;
	template bool STORMBYTE_BUFFER_INSTANTIATE WriteOnly::Write<StormByte::Safe::Binary>(const StormByte::ByteSize&, StormByte::Safe::Binary&&) noexcept;
	template bool STORMBYTE_BUFFER_INSTANTIATE WriteOnly::Write<std::span<const std::byte>>(const std::span<const std::byte>&) noexcept;
	template bool STORMBYTE_BUFFER_INSTANTIATE WriteOnly::Write<std::span<const std::byte>>(const StormByte::ByteSize&, const std::span<const std::byte>&) noexcept;
	template bool STORMBYTE_BUFFER_INSTANTIATE WriteOnly::Write<std::span<std::byte>>(const std::span<std::byte>&) noexcept;
	template bool STORMBYTE_BUFFER_INSTANTIATE WriteOnly::Write<std::span<std::byte>>(const StormByte::ByteSize&, const std::span<std::byte>&) noexcept;
	template bool STORMBYTE_BUFFER_INSTANTIATE WriteOnly::Write<StormByte::Safe::Binary::const_iterator, StormByte::Safe::Binary::const_iterator>(StormByte::Safe::Binary::const_iterator, StormByte::Safe::Binary::const_iterator) noexcept;
	template bool STORMBYTE_BUFFER_INSTANTIATE WriteOnly::Write<StormByte::Safe::Binary::iterator, StormByte::Safe::Binary::iterator>(StormByte::Safe::Binary::iterator, StormByte::Safe::Binary::iterator) noexcept;
	template bool STORMBYTE_BUFFER_INSTANTIATE WriteOnly::Write<StormByte::Safe::Binary::const_iterator, StormByte::Safe::Binary::const_iterator>(const StormByte::ByteSize&, StormByte::Safe::Binary::const_iterator, StormByte::Safe::Binary::const_iterator) noexcept;
	template bool STORMBYTE_BUFFER_INSTANTIATE WriteOnly::Write<StormByte::Safe::Binary::iterator, StormByte::Safe::Binary::iterator>(const StormByte::ByteSize&, StormByte::Safe::Binary::iterator, StormByte::Safe::Binary::iterator) noexcept;
}
