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

#include <StormByte/buffer/io/backend/buffered_writer.hxx>
#include <StormByte/buffer/io/buffered_writer.hxx>

using namespace StormByte::Buffer::IO;

BufferedWriter::BufferedWriter(const StormByte::ByteSize write_chunk, const std::size_t back_pressure,
		const std::chrono::milliseconds max_wait, const StormByte::ByteSize max_memory):
	m_io(std::make_unique<Backend::BufferedWriter>(*this, write_chunk, back_pressure, max_wait, max_memory)) {}

BufferedWriter::BufferedWriter(BufferedWriter&& other) noexcept:
	m_io(std::move(other.m_io)) {
	if (m_io)
		m_io->Rebind(*this);
}

BufferedWriter::~BufferedWriter() noexcept {
	if (m_io)
		m_io->Shutdown();
}

BufferedWriter& BufferedWriter::operator=(BufferedWriter&& other) noexcept {
	if (this != &other) {
		if (m_io)
			static_cast<void>(Close());
		m_io = std::move(other.m_io);
		if (m_io)
			m_io->Rebind(*this);
	}
	return *this;
}

BufferedWriter::operator bool() const noexcept {
	return m_io && static_cast<bool>(*m_io);
}

enum State BufferedWriter::State() const noexcept {
	return m_io ? m_io->State() : State::Unavailable;
}

void BufferedWriter::SetState(const enum State state) noexcept {
	if (m_io)
		m_io->SetState(state);
}

void BufferedWriter::SetTell(const StormByte::ByteSize offset) noexcept {
	if (m_io)
		m_io->SetTell(offset);
}

void BufferedWriter::Setup() {}

bool BufferedWriter::Open() {
	if (!m_io)
		return false;
	Setup();
	return m_io->Open();
}

bool BufferedWriter::Close() {
	if (!m_io)
		return true;
	return m_io->Close();
}

bool BufferedWriter::Rewind() {
	if (!m_io)
		return false;
	return m_io->Rewind();
}

bool BufferedWriter::IsOpen() const noexcept {
	return m_io && m_io->IsOpen();
}

Result BufferedWriter::Flush() {
	if (!m_io)
		return { Status::Failed, 0 };
	return m_io->Flush();
}

Result BufferedWriter::Truncate() {
	if (!m_io)
		return { Status::Failed, 0 };
	return m_io->Truncate();
}

Result BufferedWriter::Write(const FIFO& src) {
	if (!m_io)
		return { Status::Failed, 0 };
	return m_io->Write(src);
}

Result BufferedWriter::Write(FIFO& src) {
	if (!m_io)
		return { Status::Failed, 0 };
	return m_io->Write(src);
}

Result BufferedWriter::Write(const std::span<const std::byte> src) {
	if (!m_io)
		return { Status::Failed, 0 };
	return m_io->Write(src);
}

StormByte::ByteSize BufferedWriter::Tell() const noexcept {
	return m_io ? m_io->Tell() : StormByte::ByteSize{0};
}

StormByte::ByteSize BufferedWriter::Dirty() const noexcept {
	return m_io ? m_io->Dirty() : StormByte::ByteSize{0};
}

StormByte::ByteSize BufferedWriter::Size() const noexcept {
	return Tell();
}

Result BufferedWriter::Seek(const std::ptrdiff_t offset, const Position mode) {
	if (!m_io)
		return { Status::Failed, 0 };
	return m_io->Seek(offset, mode);
}

Result BufferedWriter::OriginSeek(const StormByte::ByteSize) {
	return { Status::Failed, 0 };
}

const struct BufferedWriter::Telemetry BufferedWriter::Telemetry() const noexcept {
	if (!m_io)
		return {};
	return m_io->Telemetry();
}

StormByte::ByteSize BufferedWriter::WriteChunk() const noexcept {
	return m_io ? m_io->WriteChunk() : StormByte::ByteSize{0};
}

void BufferedWriter::WriteChunk(const StormByte::ByteSize bytes) {
	if (m_io)
		m_io->WriteChunk(bytes);
}

std::size_t BufferedWriter::BackPressure() const noexcept {
	return m_io ? m_io->BackPressure() : 0;
}

void BufferedWriter::BackPressure(const std::size_t chunks) {
	if (m_io)
		m_io->BackPressure(chunks);
}

StormByte::ByteSize BufferedWriter::MaxMemory() const noexcept {
	return m_io ? m_io->MaxMemory() : StormByte::ByteSize{0};
}

void BufferedWriter::MaxMemory(const StormByte::ByteSize bytes) {
	if (m_io)
		m_io->MaxMemory(bytes);
}

std::chrono::milliseconds BufferedWriter::MaxWait() const noexcept {
	return m_io ? m_io->MaxWait() : std::chrono::milliseconds{0};
}

void BufferedWriter::MaxWait(const std::chrono::milliseconds wait) {
	if (m_io)
		m_io->MaxWait(wait);
}

bool BufferedWriter::WillWrite(const StormByte::ByteSize n) const {
	return m_io && m_io->WillWrite(n);
}
