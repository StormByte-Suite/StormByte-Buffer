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

#include <StormByte/buffer/shared_fifo.hxx>
#include <StormByte/safe/unique_lock.hxx>

#include <mutex>

using namespace StormByte::Buffer;

SharedFIFO::SharedFIFO() noexcept = default;

SharedFIFO::SharedFIFO(const StormByte::Safe::Binary& data): FIFO(data) {}

SharedFIFO::SharedFIFO(StormByte::Safe::Binary&& data) noexcept: FIFO(std::move(data)) {}

SharedFIFO::SharedFIFO(std::string_view sv) noexcept: FIFO(sv) {}

SharedFIFO::SharedFIFO(const char* s) noexcept: FIFO(s) {}

SharedFIFO::SharedFIFO(const FIFO& other): FIFO(other) {}

SharedFIFO::SharedFIFO(FIFO&& other) noexcept: FIFO(std::move(other)) {}

SharedFIFO::~SharedFIFO() noexcept = default;

SharedFIFO& SharedFIFO::operator=(const FIFO& other) {
	StormByte::Safe::UniqueLock lock(m_mutex);
	FIFO::operator=(other);
	m_cv.notify_all();
	return *this;
}

SharedFIFO& SharedFIFO::operator=(FIFO&& other) noexcept {
	StormByte::Safe::UniqueLock lock(m_mutex);
	FIFO::operator=(std::move(other));
	m_cv.notify_all();
	return *this;
}

bool SharedFIFO::operator==(const SharedFIFO& other) const noexcept {
	StormByte::Safe::UniqueLock lock_this(m_mutex, StormByte::Safe::defer_lock);
	StormByte::Safe::UniqueLock lock_other(other.m_mutex, StormByte::Safe::defer_lock);
	std::lock(lock_this, lock_other);
	return static_cast<const FIFO&>(*this) == static_cast<const FIFO&>(other);
}

StormByte::ByteSize SharedFIFO::Available() const noexcept {
	StormByte::Safe::UniqueLock lock(m_mutex);
	return FIFO::Available();
}

const StormByte::Safe::Binary& SharedFIFO::Data() const noexcept {
	return m_buffer;
}

bool SharedFIFO::IsReadable() const noexcept {
	StormByte::Safe::UniqueLock lock(m_mutex);
	return FIFO::IsReadable();
}

bool SharedFIFO::IsWritable() const noexcept {
	StormByte::Safe::UniqueLock lock(m_mutex);
	return FIFO::IsWritable();
}

void SharedFIFO::Clean() noexcept {
	StormByte::Safe::UniqueLock lock(m_mutex);
	FIFO::Clean();
}

void SharedFIFO::Clear() noexcept {
	{
		StormByte::Safe::UniqueLock lock(m_mutex);
		FIFO::Clear();
	}

	m_cv.notify_all();
}

void SharedFIFO::Close() noexcept {
	{
		StormByte::Safe::UniqueLock lock(m_mutex);
		FIFO::Close();
	}

	m_cv.notify_all();
}

bool SharedFIFO::Drop(const StormByte::ByteSize& count) noexcept {
	bool result;
	{
		StormByte::Safe::UniqueLock lock(m_mutex);
		if (count != StormByte::ByteSize{0} && count > FIFO::Available())
			Wait(count, lock);
		result = FIFO::Drop(count);
	}

	m_cv.notify_all();
	return result;
}

bool SharedFIFO::Empty() const noexcept {
	StormByte::Safe::UniqueLock lock(m_mutex);
	return FIFO::Empty();
}

bool SharedFIFO::EoF() const noexcept {
	StormByte::Safe::UniqueLock lock(m_mutex);
	return FIFO::EoF();
}

bool SharedFIFO::HasError() const noexcept {
	StormByte::Safe::UniqueLock lock(m_mutex);
	return !FIFO::IsReadable();
}

StormByte::Safe::String SharedFIFO::HexDump(const StormByte::ByteSize& columns,
	const StormByte::ByteSize& byte_limit) const noexcept {
	StormByte::Safe::UniqueLock lock(m_mutex);
	return FIFO::HexDump(columns, byte_limit);
}

void SharedFIFO::Seek(const std::ptrdiff_t& offset, const Position& mode) const noexcept {
	StormByte::Safe::UniqueLock lock(m_mutex);
	FIFO::Seek(offset, mode);
}

void SharedFIFO::SetError() noexcept {
	{
		StormByte::Safe::UniqueLock lock(m_mutex);
		FIFO::SetError();
	}

	m_cv.notify_all();
}

StormByte::ByteSize SharedFIFO::Size() const noexcept {
	StormByte::Safe::UniqueLock lock(m_mutex);
	return FIFO::Size();
}

StormByte::Safe::String SharedFIFO::HexDumpHeader() const noexcept {
	return FIFO::HexDumpHeader();
}

bool SharedFIFO::ReadInternal(const StormByte::ByteSize& count, StormByte::Safe::Binary& outBuffer,
	const Operation& flag) noexcept {
	StormByte::Safe::UniqueLock lock(m_mutex);
	const StormByte::ByteSize avail = AvailableInternal();
	if (m_error || (m_closed && avail == StormByte::ByteSize{0}))
		return false;
	const StormByte::ByteSize real_count = (count == StormByte::ByteSize{0}) ? avail : count;
	if (real_count > avail && !m_closed && !m_error) {
		Wait(real_count, lock);
		const StormByte::ByteSize avail2 = AvailableInternal();
		if (m_error || (m_closed && avail2 == StormByte::ByteSize{0}))
			return false;
	}

	return FIFO::ReadInternal(count, outBuffer, flag);
}

bool SharedFIFO::ReadInternal(const StormByte::ByteSize& count, WriteOnly& outBuffer,
	const Operation& flag) noexcept {
	StormByte::Safe::UniqueLock lock(m_mutex);
	StormByte::ByteSize avail = FIFO::Available();
	if (FIFO::EoF())
		return false;
	StormByte::ByteSize real_count = (count == StormByte::ByteSize{0}) ? avail : count;
	if (real_count > avail && FIFO::IsWritable()) {
		Wait(real_count, lock);
		if (FIFO::EoF())
			return false;
	}

	return FIFO::ReadInternal(count, outBuffer, flag);
}

void SharedFIFO::Wait(const StormByte::ByteSize& n, StormByte::Safe::UniqueLock& lock) const {
	if (n == StormByte::ByteSize{0})
		return;
	m_cv.wait(lock, [&] {
		if (m_error || m_closed)
			return true;
		return AvailableInternal() >= n;
	});
}

bool SharedFIFO::WriteInternal(const StormByte::ByteSize& count, const StormByte::Safe::Binary& src) noexcept {
	bool result;
	{
		StormByte::Safe::UniqueLock lock(m_mutex);
		result = FIFO::WriteInternal(count, src);
	}

	if (result)
		m_cv.notify_all();
	return result;
}

bool SharedFIFO::WriteInternal(const StormByte::ByteSize& count, StormByte::Safe::Binary&& src) noexcept {
	bool result;
	{
		StormByte::Safe::UniqueLock lock(m_mutex);
		result = FIFO::WriteInternal(count, std::move(src));
	}

	if (result)
		m_cv.notify_all();
	return result;
}
