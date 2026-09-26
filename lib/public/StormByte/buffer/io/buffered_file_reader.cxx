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

#include <StormByte/buffer/io/buffered_file_reader.hxx>
#include <StormByte/buffer/fifo.hxx>
#include <StormByte/string/wstring.hxx>

#include <ios>
#include <system_error>
#include <utility>

using namespace StormByte::Buffer::IO;

namespace {
	constexpr StormByte::ByteSize DefaultMaxMemory{1024ull * 1024ull};

	StormByte::String::String ToLocation(const std::filesystem::path& path) {
#ifdef WINDOWS
		return StormByte::String::String(StormByte::String::WString(std::wstring_view(path.wstring())));
#else
		return StormByte::String::String(std::string_view(path.string()));
#endif
	}

	std::filesystem::path ToPath(const StormByte::String::String& location) {
#ifdef WINDOWS
		const StormByte::String::WString wide(location);
		return std::filesystem::path(static_cast<std::wstring_view>(wide));
#else
		return std::filesystem::path(static_cast<std::string_view>(location));
#endif
	}
}

BufferedFileReader::BufferedFileReader(std::filesystem::path path):
	BufferedLocationReader(ToLocation(path), StormByte::ByteSize{0}, DefaultMaxMemory, true) {}

BufferedFileReader::BufferedFileReader(std::filesystem::path path, const StormByte::ByteSize read_ahead,
		const StormByte::ByteSize max_memory):
	BufferedLocationReader(ToLocation(path), read_ahead, max_memory, false) {}

BufferedFileReader::BufferedFileReader(BufferedFileReader&& other) noexcept:
	BufferedLocationReader(std::move(other)),
	m_file(std::move(other.m_file)),
	m_size(other.m_size) {
	other.m_size.reset();
	if (IsOpen())
		static_cast<void>(Seek(static_cast<std::ptrdiff_t>(Tell()), Position::Absolute));
}

BufferedFileReader::~BufferedFileReader() noexcept {
	static_cast<void>(Close());
}

BufferedFileReader& BufferedFileReader::operator=(BufferedFileReader&& other) noexcept {
	if (this != &other) {
		BufferedLocationReader::operator=(std::move(other));
		m_file = std::move(other.m_file);
		m_size = other.m_size;
		other.m_size.reset();
		if (IsOpen())
			static_cast<void>(Seek(static_cast<std::ptrdiff_t>(Tell()), Position::Absolute));
	}
	return *this;
}

std::filesystem::path BufferedFileReader::Path() const {
	return ToPath(Location());
}

StormByte::System::Device BufferedFileReader::OriginDevice() const {
	return StormByte::System::Device{Location()};
}

Result BufferedFileReader::OriginOpen() {
	std::lock_guard lock(m_file_mutex);
	if (m_file.is_open())
		return { IO::Status::Failed, 0 };

	const auto path = Path();
	std::error_code ec;
	const auto st = std::filesystem::status(path, ec);
	if (ec) {
		if (ec == std::errc::no_such_file_or_directory)
			SetState(State::Missing);
		else
			SetState(State::Permission);
		return { IO::Status::Failed, 0 };
	}

	if (std::filesystem::is_directory(st)) {
		SetState(State::Directory);
		return { IO::Status::Failed, 0 };
	}

	if (!std::filesystem::is_regular_file(st)) {
		SetState(State::Permission);
		return { IO::Status::Failed, 0 };
	}

	const auto size = std::filesystem::file_size(path, ec);
	if (ec) {
		SetState(State::Permission);
		return { IO::Status::Failed, 0 };
	}

	m_file.open(path, std::ios::in | std::ios::binary);
	if (!m_file) {
		SetState(State::Permission);
		return { IO::Status::Failed, 0 };
	}

	m_size = StormByte::ByteSize{static_cast<std::size_t>(size)};
	SetState(State::Idle);
	return { IO::Status::Ok, 0 };
}

Result BufferedFileReader::OriginClose() {
	std::lock_guard lock(m_file_mutex);
	if (m_file.is_open())
		m_file.close();
	m_size.reset();
	SetState(State::Unavailable);
	return { IO::Status::Ok, 0 };
}

Result BufferedFileReader::OriginPull(const StormByte::ByteSize n, FIFO& dest) {
	std::lock_guard lock(m_file_mutex);
	if (!m_file.is_open())
		return { IO::Status::Failed, 0 };
	if (n == StormByte::ByteSize{0})
		return { IO::Status::Ok, 0 };

	m_file.clear();

	BinaryData chunk;
	chunk.resize(n);
	m_file.read(reinterpret_cast<char*>(chunk.data()),
		static_cast<std::streamsize>(static_cast<std::size_t>(n)));
	const StormByte::ByteSize got{static_cast<std::size_t>(m_file.gcount())};
	if (m_file.bad()) {
		SetState(State::Fault);
		return { IO::Status::Error, 0 };
	}

	chunk.resize(got);
	if (got > StormByte::ByteSize{0} && !dest.Write(got, std::move(chunk))) {
		SetState(State::Fault);
		return { IO::Status::Error, 0 };
	}

	if (got < n || m_file.eof())
		return { IO::Status::End, got };
	return { IO::Status::Ok, got };
}

Result BufferedFileReader::OriginSeek(const std::ptrdiff_t offset, const Position mode) {
	std::lock_guard lock(m_file_mutex);
	if (!m_file.is_open())
		return { IO::Status::Failed, 0 };

	m_file.clear();
	if (mode == Position::Absolute) {
		if (offset < 0)
			return { IO::Status::Failed, 0 };
		m_file.seekg(static_cast<std::streamoff>(offset), std::ios::beg);
	}
	else {
		m_file.seekg(static_cast<std::streamoff>(offset), std::ios::cur);
	}

	if (!m_file)
		return { IO::Status::Failed, 0 };
	return { IO::Status::Ok, 0 };
}

std::optional<StormByte::ByteSize> BufferedFileReader::OriginSize() const noexcept {
	return m_size;
}
