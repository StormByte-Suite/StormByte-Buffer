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

#include <StormByte/buffer/ring.hxx>

#include <algorithm>
#include <cctype>
#include <iomanip>
#include <iterator>
#include <mutex>
#include <sstream>
#include <string>
#include <vector>

using namespace StormByte::Buffer;

Ring::Ring(Ring&& other) noexcept {
	std::unique_lock lock(other.m_mutex);
	m_buffer          = std::move(other.m_buffer);
	m_position_offset = other.m_position_offset;
	m_closed          = other.m_closed;
	m_error           = other.m_error;
	m_error_message   = std::move(other.m_error_message);
	other.m_buffer.clear();
	other.m_position_offset = StormByte::ByteSize{0};
	other.m_closed = false;
	other.m_error  = false;
}

Ring::~Ring() noexcept = default;

Ring& Ring::operator=(Ring&& other) noexcept {
	if (this != &other) {
		std::unique_lock lock_this(m_mutex, std::defer_lock);
		std::unique_lock lock_other(other.m_mutex, std::defer_lock);
		std::lock(lock_this, lock_other);
		m_buffer          = std::move(other.m_buffer);
		m_position_offset = other.m_position_offset;
		m_closed          = other.m_closed;
		m_error           = other.m_error;
		m_error_message   = std::move(other.m_error_message);
		other.m_buffer.clear();
		other.m_position_offset = StormByte::ByteSize{0};
		other.m_closed = false;
		other.m_error  = false;
	}

	return *this;
}

bool Ring::operator==(const Ring& other) const noexcept {
	std::shared_lock lock_this(m_mutex, std::defer_lock);
	std::shared_lock lock_other(other.m_mutex, std::defer_lock);
	std::lock(lock_this, lock_other);
	return m_buffer == other.m_buffer &&
		m_position_offset == other.m_position_offset &&
		m_closed == other.m_closed &&
		m_error == other.m_error;
}

StormByte::ByteSize Ring::Available() const noexcept {
	std::shared_lock lock(m_mutex);
	const StormByte::ByteSize stored{m_buffer.size()};
	return (m_position_offset <= stored) ? (stored - m_position_offset) : StormByte::ByteSize{0};
}

bool Ring::Empty() const noexcept {
	std::shared_lock lock(m_mutex);
	return m_buffer.empty();
}

bool Ring::EoF() const noexcept {
	std::shared_lock lock(m_mutex);
	return m_error || (m_closed && (StormByte::ByteSize{m_buffer.size()} <= m_position_offset));
}

bool Ring::HasError() const noexcept {
	std::shared_lock lock(m_mutex);
	return m_error;
}

bool Ring::IsReadable() const noexcept {
	std::shared_lock lock(m_mutex);
	return !m_error;
}

bool Ring::IsWritable() const noexcept {
	std::shared_lock lock(m_mutex);
	return !m_closed && !m_error;
}

StormByte::ByteSize Ring::Size() const noexcept {
	std::shared_lock lock(m_mutex);
	return StormByte::ByteSize{m_buffer.size()};
}

const StormByte::BinaryData& Ring::Data() const noexcept {
	std::unique_lock lock(m_mutex);
	m_data_cache.assign(m_buffer.begin(), m_buffer.end());
	return m_data_cache;
}

void Ring::Clean() noexcept {
	std::unique_lock lock(m_mutex);
	const StormByte::ByteSize stored{m_buffer.size()};
	if (m_position_offset > StormByte::ByteSize{0} && m_position_offset <= stored) {
		m_buffer.erase(m_buffer.begin(),
					m_buffer.begin() + static_cast<std::ptrdiff_t>(m_position_offset));
	}
	else if (m_position_offset > stored) {
		m_buffer.clear();
	}

	m_position_offset = StormByte::ByteSize{0};
}

void Ring::Clear() noexcept {
	{
		std::unique_lock lock(m_mutex);
		m_buffer.clear();
		m_position_offset = StormByte::ByteSize{0};
	}

	m_cv.notify_all();
}

void Ring::Close() noexcept {
	{
		std::unique_lock lock(m_mutex);
		m_closed = true;
	}

	m_cv.notify_all();
}

void Ring::SetError() noexcept {
	{
		std::unique_lock lock(m_mutex);
		m_error = true;
	}

	m_cv.notify_all();
}

bool Ring::Drop(const StormByte::ByteSize& count) noexcept {
	bool result = false;
	{
		std::unique_lock lock(m_mutex);
		const StormByte::ByteSize stored{m_buffer.size()};
		const StormByte::ByteSize avail = (m_position_offset <= stored)
			? (stored - m_position_offset) : StormByte::ByteSize{0};
		if (count != StormByte::ByteSize{0} && count > avail && !m_closed)
			Wait(count, lock);
		const StormByte::ByteSize stored2{m_buffer.size()};
		const StormByte::ByteSize avail2 = (m_position_offset <= stored2)
			? (stored2 - m_position_offset) : StormByte::ByteSize{0};
		if (avail2 == StormByte::ByteSize{0} || count > avail2)
			return false;
		m_position_offset = std::min(m_position_offset + count, stored2);
		if (m_position_offset > StormByte::ByteSize{0} && m_position_offset <= StormByte::ByteSize{m_buffer.size()}) {
			m_buffer.erase(m_buffer.begin(),
						m_buffer.begin() + static_cast<std::ptrdiff_t>(m_position_offset));
		}

		m_position_offset = StormByte::ByteSize{0};
		result = true;
	}

	m_cv.notify_all();
	return result;
}

void Ring::Seek(const std::ptrdiff_t& offset, const Position& mode) const noexcept {
	std::unique_lock lock(m_mutex);
	const StormByte::ByteSize stored{m_buffer.size()};
	switch (mode) {
		case Position::Absolute:
			m_position_offset = (offset < 0)
				? StormByte::ByteSize{0}
				: std::min(StormByte::ByteSize{static_cast<std::size_t>(offset)}, stored);
			break;
		case Position::Relative:
			if (offset < 0) {
				m_position_offset = StormByte::ByteSize{static_cast<std::size_t>(
					std::max<std::ptrdiff_t>(0,
						static_cast<std::ptrdiff_t>(m_position_offset) + offset))};
			}
			else {
				m_position_offset = std::min(
					m_position_offset + StormByte::ByteSize{static_cast<std::size_t>(offset)},
					stored);
			}
			break;
		default:
			break;
	}
}

StormByte::String::String Ring::HexDump(const StormByte::ByteSize& columns,
						const StormByte::ByteSize& byte_limit) const noexcept {
	std::shared_lock lock(m_mutex);
	const StormByte::ByteSize cols = (columns == StormByte::ByteSize{0}) ? StormByte::ByteSize{16} : columns;
	const StormByte::ByteSize stored{m_buffer.size()};
	const StormByte::ByteSize end = (byte_limit > StormByte::ByteSize{0})
		? std::min(stored, m_position_offset + byte_limit)
		: stored;
	std::ostringstream oss = HexDumpHeader();
	oss << '\n';
	if (end > m_position_offset) {
		StormByte::BinaryData tmp;
		tmp.assign(m_buffer.begin() + static_cast<std::ptrdiff_t>(m_position_offset),
					m_buffer.begin() + static_cast<std::ptrdiff_t>(end));
		std::span<const std::byte> view(tmp.data(), static_cast<std::size_t>(tmp.size()));
		oss << static_cast<const char*>(FormatHexLines(view, m_position_offset, cols));
	}

	return StormByte::String::String{oss.str()};
}

std::ostringstream Ring::HexDumpHeader() const noexcept {
	std::ostringstream oss;
	oss << "Size: " << m_buffer.size() << " bytes\n";
	oss << "Read Position: " << static_cast<std::size_t>(m_position_offset) << '\n';
	oss << "Status: " << (m_closed ? "closed" : "opened")
		<< " and " << (m_error ? "error" : "ready");
	return oss;
}

StormByte::String::String Ring::FormatHexLines(std::span<const std::byte> data,
								StormByte::ByteSize start_offset,
								StormByte::ByteSize columns) noexcept {
	const std::size_t cols = static_cast<std::size_t>(
		(columns == StormByte::ByteSize{0}) ? StormByte::ByteSize{16} : columns);
	const int offset_width = 8;
	std::vector<std::string> lines;
	for (std::size_t i = 0; i < data.size(); i += cols) {
		const std::size_t line_end = std::min(data.size(), i + cols);
		std::ostringstream line;
		line << std::hex << std::uppercase << std::setw(offset_width)
			<< std::setfill('0') << static_cast<std::size_t>(start_offset + StormByte::ByteSize{i}) << ": "
			<< std::dec << std::setfill(' ');
		for (std::size_t j = i; j < i + cols; ++j) {
			if (j < line_end) {
				const unsigned int val = static_cast<unsigned int>(std::to_integer<unsigned char>(data[j]));
				line << std::hex << std::setw(2) << std::setfill('0') << std::uppercase << val << ' ' << std::dec;
			}
			else {
				line << "   ";
			}
		}

		line << "  ";
		for (std::size_t j = i; j < line_end; ++j) {
			const unsigned char c = std::to_integer<unsigned char>(data[j]);
			line << (std::isprint(c) ? static_cast<char>(c) : '.');
		}

		lines.push_back(line.str());
	}

	std::ostringstream oss;
	for (std::size_t li = 0; li < lines.size(); ++li) {
		oss << lines[li];
		if (li + 1 < lines.size())
			oss << '\n';
	}

	return StormByte::String::String{oss.str()};
}

bool Ring::Peek(const StormByte::ByteSize& count, StormByte::BinaryData& outBuffer) const noexcept {
	return const_cast<Ring*>(this)->ReadInternal(count, outBuffer, Operation::Peek);
}

bool Ring::Peek(const StormByte::ByteSize& count, WriteOnly& outBuffer) const noexcept {
	return const_cast<Ring*>(this)->ReadInternal(count, outBuffer, Operation::Peek);
}

bool Ring::Read(const StormByte::ByteSize& count, StormByte::BinaryData& outBuffer) const noexcept {
	return const_cast<Ring*>(this)->ReadInternal(count, outBuffer, Operation::Read);
}

bool Ring::Read(const StormByte::ByteSize& count, WriteOnly& outBuffer) const noexcept {
	return const_cast<Ring*>(this)->ReadInternal(count, outBuffer, Operation::Read);
}

bool Ring::Extract(const StormByte::ByteSize& count, StormByte::BinaryData& outBuffer) noexcept {
	return ReadInternal(count, outBuffer, Operation::Extract);
}

bool Ring::Extract(const StormByte::ByteSize& count, WriteOnly& outBuffer) noexcept {
	return ReadInternal(count, outBuffer, Operation::Extract);
}

void Ring::ReadUntilEoF(StormByte::BinaryData& outBuffer) const noexcept {
	const_cast<Ring*>(this)->ReadUntilEoFInternal(outBuffer, Operation::Read);
}

void Ring::ReadUntilEoF(WriteOnly& outBuffer) const noexcept {
	const_cast<Ring*>(this)->ReadUntilEoFInternal(outBuffer, Operation::Read);
}

void Ring::ExtractUntilEoF(StormByte::BinaryData& outBuffer) noexcept {
	ReadUntilEoFInternal(outBuffer, Operation::Extract);
}

void Ring::ExtractUntilEoF(WriteOnly& outBuffer) noexcept {
	ReadUntilEoFInternal(outBuffer, Operation::Extract);
}

bool Ring::ReadInternal(const StormByte::ByteSize& count, StormByte::BinaryData& outBuffer, Operation flag) noexcept {
	StormByte::BinaryData local;
	{
		std::unique_lock lock(m_mutex);
		StormByte::ByteSize stored{m_buffer.size()};
		StormByte::ByteSize avail = (m_position_offset <= stored)
			? (stored - m_position_offset) : StormByte::ByteSize{0};
		if (m_error || (m_closed && avail == StormByte::ByteSize{0}))
			return false;
		if (count == StormByte::ByteSize{0} && avail == StormByte::ByteSize{0} && !m_closed) {
			Wait(StormByte::ByteSize{1}, lock);
			stored = StormByte::ByteSize{m_buffer.size()};
			avail = (m_position_offset <= stored)
				? (stored - m_position_offset) : StormByte::ByteSize{0};
			if (avail == StormByte::ByteSize{0})
				return false;
		}

		const StormByte::ByteSize real_count = (count == StormByte::ByteSize{0}) ? avail : count;
		if (real_count > avail && !m_closed)
			Wait(real_count, lock);
		stored = StormByte::ByteSize{m_buffer.size()};
		avail = (m_position_offset <= stored)
			? (stored - m_position_offset) : StormByte::ByteSize{0};
		if ((avail == StormByte::ByteSize{0} && count == StormByte::ByteSize{0}) || real_count > avail)
			return false;
		const std::size_t n = static_cast<std::size_t>(real_count);
		auto start = m_buffer.begin() + static_cast<std::ptrdiff_t>(m_position_offset);
		switch (flag) {
			case Operation::Read:
				local.insert(local.end(), start, start + static_cast<std::ptrdiff_t>(n));
				m_position_offset = m_position_offset + real_count;
				break;
			case Operation::Peek:
				local.insert(local.end(), start, start + static_cast<std::ptrdiff_t>(n));
				break;
			case Operation::Extract:
				local.insert(local.end(),
							std::make_move_iterator(start),
							std::make_move_iterator(start + static_cast<std::ptrdiff_t>(n)));
				m_buffer.erase(start, start + static_cast<std::ptrdiff_t>(n));
				stored = StormByte::ByteSize{m_buffer.size()};
				if (m_position_offset > stored)
					m_position_offset = stored;
				break;
			default:
				return false;
		}
	}

	outBuffer.insert(outBuffer.end(),
					std::make_move_iterator(local.begin()),
					std::make_move_iterator(local.end()));
	return true;
}

bool Ring::ReadInternal(const StormByte::ByteSize& count, WriteOnly& outBuffer, Operation flag) noexcept {
	StormByte::BinaryData temp;
	if (!ReadInternal(count, temp, flag))
		return false;
	return outBuffer.Write(std::move(temp));
}

void Ring::ReadUntilEoFInternal(StormByte::BinaryData& outBuffer, Operation flag) noexcept {
	while (true) {
		{
			std::unique_lock lock(m_mutex);
			if (m_error)
				return;
			m_cv.wait(lock, [&] {
				if (m_error || m_closed)
					return true;
				const StormByte::ByteSize stored{m_buffer.size()};
				const StormByte::ByteSize avail = (m_position_offset <= stored)
					? (stored - m_position_offset) : StormByte::ByteSize{0};
				return avail > StormByte::ByteSize{0};
			});
			if (m_error)
				return;
			const StormByte::ByteSize stored{m_buffer.size()};
			const StormByte::ByteSize avail = (m_position_offset <= stored)
				? (stored - m_position_offset) : StormByte::ByteSize{0};
			if (avail == StormByte::ByteSize{0} && m_closed)
				return;
		}

		StormByte::BinaryData chunk;
		bool ok = false;
		switch (flag) {
			case Operation::Read:
				ok = Read(StormByte::ByteSize{0}, chunk);
				break;
			case Operation::Extract:
				ok = Extract(StormByte::ByteSize{0}, chunk);
				break;
			default:
				return;
		}

		if (!ok || chunk.empty()) {
			if (EoF())
				return;
			continue;
		}

		outBuffer.insert(outBuffer.end(),
						std::make_move_iterator(chunk.begin()),
						std::make_move_iterator(chunk.end()));
	}
}

void Ring::ReadUntilEoFInternal(WriteOnly& outBuffer, Operation flag) noexcept {
	StormByte::BinaryData tmp;
	ReadUntilEoFInternal(tmp, flag);
	if (!tmp.empty())
		(void)outBuffer.Write(std::move(tmp));
}

bool Ring::Write(const StormByte::ByteSize& count, const StormByte::BinaryData& data) noexcept {
	return WriteInternal(count, data);
}

bool Ring::Write(const StormByte::ByteSize& count, StormByte::BinaryData&& data) noexcept {
	return WriteInternal(count, std::move(data));
}

bool Ring::Write(const StormByte::ByteSize& count, const ReadOnly& data) noexcept {
	StormByte::BinaryData tmp;
	if (!data.Read(count, tmp))
		return false;
	return WriteInternal(StormByte::ByteSize{0}, std::move(tmp));
}

bool Ring::Write(const StormByte::ByteSize& count, ReadOnly&& data) noexcept {
	StormByte::BinaryData tmp;
	if (!data.Extract(count, tmp))
		return false;
	return WriteInternal(StormByte::ByteSize{0}, std::move(tmp));
}

bool Ring::WriteInternal(const StormByte::ByteSize& count, const StormByte::BinaryData& src) noexcept {
	bool result = false;
	{
		std::unique_lock lock(m_mutex);
		if (m_closed || m_error)
			return false;
		const StormByte::ByteSize src_size = src.size();
		const StormByte::ByteSize real_count = (count == StormByte::ByteSize{0}) ? src_size : count;
		if (real_count > src_size)
			return false;
		m_buffer.insert(m_buffer.end(),
						src.begin(),
						src.begin() + static_cast<std::ptrdiff_t>(real_count));
		result = true;
	}

	m_cv.notify_all();
	return result;
}

bool Ring::WriteInternal(const StormByte::ByteSize& count, StormByte::BinaryData&& src) noexcept {
	bool result = false;
	{
		std::unique_lock lock(m_mutex);
		if (m_closed || m_error)
			return false;
		const StormByte::ByteSize src_size = src.size();
		const StormByte::ByteSize real_count = (count == StormByte::ByteSize{0}) ? src_size : count;
		if (real_count > src_size)
			return false;
		const std::size_t n = static_cast<std::size_t>(real_count);
		if (real_count == src_size) {
			m_buffer.insert(m_buffer.end(),
							std::make_move_iterator(src.begin()),
							std::make_move_iterator(src.end()));
		}
		else {
			m_buffer.insert(m_buffer.end(),
							std::make_move_iterator(src.begin()),
							std::make_move_iterator(src.begin() + static_cast<std::ptrdiff_t>(n)));
			src.erase(src.begin(), src.begin() + static_cast<std::ptrdiff_t>(n));
		}

		result = true;
	}

	m_cv.notify_all();
	return result;
}

void Ring::Wait(const StormByte::ByteSize& n, std::unique_lock<std::shared_mutex>& lock) const {
	if (n == StormByte::ByteSize{0})
		return;
	m_cv.wait(lock, [&] {
		if (m_closed || m_error)
			return true;
		const StormByte::ByteSize stored{m_buffer.size()};
		return stored >= m_position_offset + n;
	});
}
