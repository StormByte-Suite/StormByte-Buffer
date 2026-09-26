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

#include <StormByte/buffer/io/backend/buffered_reader.hxx>
#include <StormByte/buffer/io/buffered_reader.hxx>

#include <algorithm>

using namespace StormByte::Buffer::IO;

namespace {
	StormByte::Buffer::IO::Result CopyToSpan(StormByte::Buffer::FIFO& src,
			const StormByte::Buffer::IO::Result got,
			std::span<std::byte> dest) {
		if (got.count == StormByte::ByteSize{0})
			return got;
		if (got.count > StormByte::ByteSize{dest.size()})
			return { StormByte::Buffer::IO::Status::Failed, 0 };

		StormByte::BinaryData raw;
		if (!src.Extract(got.count, raw))
			return { StormByte::Buffer::IO::Status::Failed, 0 };
		std::copy_n(raw.begin(), static_cast<std::size_t>(got.count), dest.begin());
		return got;
	}

	const StormByte::String::String& EmptyPath() noexcept {
		static const StormByte::String::String empty;
		return empty;
	}
}

BufferedReader::BufferedReader(StormByte::String::String path, const enum Location location,
		const StormByte::ByteSize read_ahead, const StormByte::ByteSize max_memory,
		const std::chrono::milliseconds max_wait):
	m_io(std::make_unique<Backend::BufferedReader>(*this, std::move(path), location, read_ahead, max_memory, max_wait)) {}

BufferedReader::BufferedReader(BufferedReader&& other) noexcept:
	m_io(std::move(other.m_io)) {
	if (m_io) {
		m_io->FlushPrefetch();
		m_io->Rebind(*this);
	}
}

BufferedReader::~BufferedReader() noexcept {
	if (m_io)
		m_io->Shutdown();
}

BufferedReader& BufferedReader::operator=(BufferedReader&& other) noexcept {
	if (this != &other) {
		if (m_io)
			static_cast<void>(Close());
		m_io = std::move(other.m_io);
		if (m_io) {
			m_io->FlushPrefetch();
			m_io->Rebind(*this);
		}
	}
	return *this;
}

const StormByte::String::String& BufferedReader::Path() const noexcept {
	return m_io ? m_io->Path() : EmptyPath();
}

enum Location BufferedReader::Location() const noexcept {
	return m_io ? m_io->Location() : Location::Local;
}

BufferedReader::operator bool() const noexcept {
	return m_io && static_cast<bool>(*m_io);
}

State BufferedReader::State() const noexcept {
	return m_io ? m_io->State() : State::Unavailable;
}

void BufferedReader::SetState(const enum State state) noexcept {
	if (m_io)
		m_io->SetState(state);
}

void BufferedReader::Setup() {}

bool BufferedReader::Open() {
	if (!m_io)
		return false;
	Setup();
	return m_io->Open();
}

Result BufferedReader::Close() {
	if (!m_io)
		return { IO::Status::Ok, 0 };
	return m_io->Close();
}

bool BufferedReader::Rewind() {
	if (!m_io)
		return false;
	return m_io->Rewind();
}

bool BufferedReader::IsOpen() const noexcept {
	return m_io && m_io->IsOpen();
}

bool BufferedReader::IsReadable() const noexcept {
	return m_io && m_io->IsReadable();
}

bool BufferedReader::EoF() const noexcept {
	return !m_io || m_io->EoF();
}

Result BufferedReader::Read(const StormByte::ByteSize n, FIFO& dest) const {
	if (!m_io)
		return { IO::Status::Failed, 0 };
	return m_io->Read(n, dest);
}

Result BufferedReader::Read(const std::span<std::byte> dest) const {
	if (dest.empty())
		return { IO::Status::Ok, 0 };
	FIFO fifo;
	const Result got = Read(StormByte::ByteSize{dest.size()}, fifo);
	return CopyToSpan(fifo, got, dest);
}

Result BufferedReader::Peek(const StormByte::ByteSize n, FIFO& dest) const {
	if (!m_io)
		return { IO::Status::Failed, 0 };
	return m_io->Peek(n, dest);
}

Result BufferedReader::Peek(const std::span<std::byte> dest) const {
	if (dest.empty())
		return { IO::Status::Ok, 0 };
	FIFO fifo;
	const Result got = Peek(StormByte::ByteSize{dest.size()}, fifo);
	return CopyToSpan(fifo, got, dest);
}

Result BufferedReader::Seek(const std::ptrdiff_t offset, const Position mode) const {
	if (!m_io)
		return { IO::Status::Failed, 0 };
	return m_io->Seek(offset, mode);
}

StormByte::ByteSize BufferedReader::Tell() const noexcept {
	return m_io ? m_io->Tell() : StormByte::ByteSize{0};
}

bool BufferedReader::IsSeekable() const noexcept {
	return m_io && m_io->IsSeekable();
}

bool BufferedReader::IsSized() const noexcept {
	return m_io && m_io->IsSized();
}

std::optional<StormByte::ByteSize> BufferedReader::Size() const noexcept {
	if (!m_io)
		return std::nullopt;
	return m_io->Size();
}

const struct BufferedReader::Telemetry BufferedReader::Telemetry() const noexcept {
	if (!m_io)
		return {};
	return m_io->Telemetry();
}

StormByte::ByteSize BufferedReader::ReadAhead() const noexcept {
	return m_io ? m_io->ReadAhead() : StormByte::ByteSize{0};
}

void BufferedReader::ReadAhead(const StormByte::ByteSize bytes) {
	if (m_io)
		m_io->ReadAhead(bytes);
}

StormByte::ByteSize BufferedReader::MaxMemory() const noexcept {
	return m_io ? m_io->MaxMemory() : StormByte::ByteSize{0};
}

void BufferedReader::MaxMemory(const StormByte::ByteSize bytes) {
	if (m_io)
		m_io->MaxMemory(bytes);
}

std::chrono::milliseconds BufferedReader::MaxWait() const noexcept {
	return m_io ? m_io->MaxWait() : std::chrono::milliseconds{0};
}

void BufferedReader::MaxWait(const std::chrono::milliseconds wait) {
	if (m_io)
		m_io->MaxWait(wait);
}
