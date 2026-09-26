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

#include <StormByte/buffer/io/buffered_file_writer.hxx>

#include <cstdint>
#include <ios>
#include <system_error>

#ifdef WINDOWS
#	include <windows.h>
#else
#	include <sys/statvfs.h>
#endif

using namespace StormByte::Buffer::IO;

namespace {
	constexpr std::size_t DefaultBackPressure = 4;
	constexpr StormByte::ByteSize DefaultMaxMemory{1024ull * 1024ull};

	std::filesystem::path SpacePath(const std::filesystem::path& path) {
		std::error_code ec;
		const auto parent = path.parent_path();
		if (!parent.empty())
			return parent;
		const auto cwd = std::filesystem::current_path(ec);
		if (!ec)
			return cwd;
		return {};
	}

	bool VolumeHas(const std::filesystem::path& path, const StormByte::ByteSize n) {
		if (n == StormByte::ByteSize{0})
			return true;
		const auto probe = SpacePath(path);
		if (probe.empty())
			return false;

#ifdef WINDOWS
		ULARGE_INTEGER avail {};
		const auto wide = probe.wstring();
		if (wide.empty())
			return false;
		if (!GetDiskFreeSpaceExW(wide.c_str(), &avail, nullptr, nullptr))
			return false;
		return avail.QuadPart >= static_cast<ULONGLONG>(static_cast<std::size_t>(n));
#else
		struct statvfs st {};
		if (statvfs(probe.c_str(), &st) != 0)
			return false;
		const std::uint64_t avail = static_cast<std::uint64_t>(st.f_bavail) *
			static_cast<std::uint64_t>(st.f_frsize);
		return avail >= static_cast<std::uint64_t>(static_cast<std::size_t>(n));
#endif
	}

	bool OpenRandomAccess(std::ofstream& file, const std::filesystem::path& path) {
		file.open(path, std::ios::in | std::ios::out | std::ios::binary);
		if (file)
			return true;
		file.clear();
		file.open(path, std::ios::out | std::ios::binary);
		if (!file)
			return false;
		file.close();
		file.open(path, std::ios::in | std::ios::out | std::ios::binary);
		return static_cast<bool>(file);
	}
}

BufferedFileWriter::BufferedFileWriter(std::filesystem::path path):
	BufferedWriter(StormByte::ByteSize{0}, 0),
	m_path(std::move(path)),
	m_probe_on_setup(true) {}

BufferedFileWriter::BufferedFileWriter(std::filesystem::path path, const StormByte::ByteSize write_chunk,
		const std::size_t back_pressure, const std::chrono::milliseconds max_wait):
	BufferedWriter(write_chunk, back_pressure, max_wait),
	m_path(std::move(path)),
	m_probe_on_setup(false) {}

BufferedFileWriter::BufferedFileWriter(std::filesystem::path path, const StormByte::ByteSize write_chunk,
		const StormByte::ByteSize max_memory, const std::size_t back_pressure,
		const std::chrono::milliseconds max_wait):
	BufferedWriter(write_chunk, back_pressure, max_wait, max_memory),
	m_path(std::move(path)),
	m_probe_on_setup(false) {}

BufferedFileWriter::BufferedFileWriter(BufferedFileWriter&& other) noexcept:
	BufferedWriter(std::move(other)),
	m_path(std::move(other.m_path)),
	m_file(std::move(other.m_file)),
	m_probe_on_setup(other.m_probe_on_setup) {
	other.m_probe_on_setup = false;
}

BufferedFileWriter::~BufferedFileWriter() noexcept {
	static_cast<void>(Close());
}

BufferedFileWriter& BufferedFileWriter::operator=(BufferedFileWriter&& other) noexcept {
	if (this != &other) {
		static_cast<void>(Close());
		BufferedWriter::operator=(std::move(other));
		m_path = std::move(other.m_path);
		m_file = std::move(other.m_file);
		m_probe_on_setup = other.m_probe_on_setup;
		other.m_probe_on_setup = false;
	}
	return *this;
}

const std::filesystem::path& BufferedFileWriter::Path() const noexcept {
	return m_path;
}

StormByte::ByteSize BufferedFileWriter::Size() const noexcept {
	std::error_code ec;
	const auto disk = std::filesystem::file_size(m_path, ec);
	const StormByte::ByteSize on_disk = ec ? StormByte::ByteSize{0} : StormByte::ByteSize{static_cast<std::size_t>(disk)};
	const StormByte::ByteSize logical = Tell();
	return on_disk > logical ? on_disk : logical;
}

std::unique_ptr<StormByte::System::Device> BufferedFileWriter::CreateDevice() const {
	return std::make_unique<StormByte::System::Device>(m_path);
}

void BufferedFileWriter::Setup() {
	if (!m_probe_on_setup)
		return;
	const auto device = CreateDevice();
	if (!device || !*device)
		WriteChunk(StormByte::ByteSize{0});
	else
		WriteChunk(device->Window().write);
	BackPressure(DefaultBackPressure);
	MaxMemory(DefaultMaxMemory);
}

bool BufferedFileWriter::WillWrite(const StormByte::ByteSize n) const {
	if (!BufferedWriter::WillWrite(n))
		return false;
	return VolumeHas(m_path, n);
}

Result BufferedFileWriter::OriginOpen() {
	std::lock_guard lock(m_file_mutex);
	if (m_file.is_open())
		return { Status::Failed, 0 };

	std::error_code ec;
	const auto parent = m_path.parent_path();
	if (!parent.empty()) {
		const auto pst = std::filesystem::status(parent, ec);
		if (ec) {
			SetState(State::Missing);
			return { Status::Failed, 0 };
		}
		if (!std::filesystem::is_directory(pst)) {
			SetState(State::Missing);
			return { Status::Failed, 0 };
		}
	}

	const auto st = std::filesystem::status(m_path, ec);
	if (ec && ec != std::errc::no_such_file_or_directory) {
		SetState(State::NotWritable);
		return { Status::Failed, 0 };
	}

	if (!ec) {
		if (std::filesystem::is_directory(st)) {
			SetState(State::Directory);
			return { Status::Failed, 0 };
		}
		if (!std::filesystem::is_regular_file(st)) {
			SetState(State::NotWritable);
			return { Status::Failed, 0 };
		}
	}

	if (!OpenRandomAccess(m_file, m_path)) {
		SetState(State::NotWritable);
		return { Status::Failed, 0 };
	}

	SetState(State::Idle);
	return { Status::Ok, 0 };
}

Result BufferedFileWriter::OriginClose() {
	std::lock_guard lock(m_file_mutex);
	if (m_file.is_open())
		m_file.close();
	SetState(State::Unavailable);
	return { Status::Ok, 0 };
}

Result BufferedFileWriter::OriginPush(const std::span<const std::byte> data) {
	std::lock_guard lock(m_file_mutex);
	if (!m_file.is_open())
		return { Status::Failed, 0 };
	if (data.empty())
		return { Status::Ok, 0 };

	m_file.write(reinterpret_cast<const char*>(data.data()),
		static_cast<std::streamsize>(data.size()));
	if (m_file.bad()) {
		SetState(State::Fault);
		return { Status::Error, 0 };
	}

	return { Status::Ok, StormByte::ByteSize{data.size()} };
}

Result BufferedFileWriter::OriginFlush() {
	std::lock_guard lock(m_file_mutex);
	if (!m_file.is_open())
		return { Status::Failed, 0 };
	m_file.flush();
	if (m_file.bad()) {
		SetState(State::Fault);
		return { Status::Error, 0 };
	}
	return { Status::Ok, 0 };
}

Result BufferedFileWriter::OriginTruncate() {
	std::lock_guard lock(m_file_mutex);
	if (!m_file.is_open())
		return { Status::Failed, 0 };

	m_file.close();
	std::error_code ec;
	std::filesystem::resize_file(m_path, 0, ec);
	if (ec) {
		SetState(State::Fault);
		return { Status::Failed, 0 };
	}

	if (!OpenRandomAccess(m_file, m_path)) {
		SetState(State::NotWritable);
		return { Status::Failed, 0 };
	}
	return { Status::Ok, 0 };
}

Result BufferedFileWriter::OriginSeek(const StormByte::ByteSize absolute) {
	std::lock_guard lock(m_file_mutex);
	if (!m_file.is_open())
		return { Status::Failed, 0 };
	m_file.clear();
	m_file.seekp(static_cast<std::streamoff>(static_cast<std::size_t>(absolute)), std::ios::beg);
	if (!m_file)
		return { Status::Failed, 0 };
	return { Status::Ok, 0 };
}
