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

#include <StormByte/buffer/exception.hxx>

using namespace StormByte::Buffer;

Exception::Exception(std::string_view message)
	: Exception(Segment{}, StormByte::Safe::String(), StormByte::Safe::String(message)) {}

Exception::Exception(Segment, const StormByte::Safe::String& child, const StormByte::Safe::String& message)
	: StormByte::Exception(StormByte::Exception::Path{std::string_view(Compose(child))}, "{}", std::string_view(message)) {}

Exception::Exception(const Exception& other) = default;

Exception::Exception(Exception&& other) noexcept = default;

Exception& Exception::operator=(const Exception& other) = default;

Exception& Exception::operator=(Exception&& other) noexcept = default;

Exception::~Exception() noexcept = default;

StormByte::Safe::String Exception::Compose(const StormByte::Safe::String& child) {
	std::string text("Buffer");
	const std::string_view segment(child);
	if (!segment.empty()) {
		text.push_back('.');
		text.append(segment);
	}
	return StormByte::Safe::String(std::string_view(text));
}

Error::Error(const Error& other) = default;

Error::Error(Error&& other) noexcept = default;

Error& Error::operator=(const Error& other) = default;

Error& Error::operator=(Error&& other) noexcept = default;

Error::~Error() noexcept = default;

ReadError::ReadError(std::string_view message)
	: Error(Segment{}, StormByte::Safe::String("Read"), StormByte::Safe::String(message)) {}

ReadError::ReadError(const ReadError& other) = default;

ReadError::ReadError(ReadError&& other) noexcept = default;

ReadError& ReadError::operator=(const ReadError& other) = default;

ReadError& ReadError::operator=(ReadError&& other) noexcept = default;

ReadError::~ReadError() noexcept = default;

WriteError::WriteError(std::string_view message)
	: Error(Segment{}, StormByte::Safe::String("Write"), StormByte::Safe::String(message)) {}

WriteError::WriteError(const WriteError& other) = default;

WriteError::WriteError(WriteError&& other) noexcept = default;

WriteError& WriteError::operator=(const WriteError& other) = default;

WriteError& WriteError::operator=(WriteError&& other) noexcept = default;

WriteError::~WriteError() noexcept = default;
