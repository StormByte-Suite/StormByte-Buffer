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

#include <StormByte/buffer/fifo.hxx>
#include <StormByte/string/string.hxx>

#include <algorithm>
#include <cctype>
#include <cstring>
#include <iomanip>
#include <iterator>
#include <sstream>
#include <string>
#include <vector>

using namespace StormByte::Buffer;

FIFO::FIFO(const FIFO& other) noexcept
	: Generic(other), ReadWrite(other),
	m_buffer(other.m_buffer),
	m_position_offset(other.m_position_offset),
	m_closed(other.m_closed),
	m_error(other.m_error)
{}

FIFO::FIFO(FIFO&& other) noexcept
	: Generic(std::move(other)), ReadWrite(std::move(other)),
	m_buffer(std::move(other.m_buffer)),
	m_position_offset(other.m_position_offset),
	m_closed(other.m_closed),
	m_error(other.m_error)
{
	other.m_position_offset = StormByte::ByteSize{0};
	other.m_closed = false;
	other.m_error  = false;
}

FIFO::~FIFO() noexcept = default;

FIFO& FIFO::operator=(const FIFO& other) {
	if (this != &other) {
		Generic::operator=(other);
		m_buffer          = other.m_buffer;
		m_position_offset = other.m_position_offset;
		m_closed          = other.m_closed;
		m_error           = other.m_error;
	}

	return *this;
}

FIFO& FIFO::operator=(FIFO&& other) noexcept {
	if (this != &other) {
		Generic::operator=(std::move(other));
		m_buffer          = std::move(other.m_buffer);
		m_position_offset = other.m_position_offset;
		m_closed          = other.m_closed;
		m_error           = other.m_error;
		other.m_position_offset = StormByte::ByteSize{0};
		other.m_closed = false;
		other.m_error  = false;
	}

	return *this;
}

StormByte::ByteSize FIFO::Available() const noexcept {
	return AvailableInternal();
}

const StormByte::BinaryData& FIFO::Data() const noexcept {
	return m_buffer;
}

bool FIFO::Empty() const noexcept {
	return m_buffer.empty();
}

bool FIFO::EoF() const noexcept {
	return m_error || (m_closed && AvailableInternal() == StormByte::ByteSize{0});
}

bool FIFO::IsReadable() const noexcept {
	return !m_error;
}

bool FIFO::IsWritable() const noexcept {
	return !m_closed && !m_error;
}

StormByte::ByteSize FIFO::Size() const noexcept {
	return m_buffer.size();
}

void FIFO::Clean() noexcept {
	const StormByte::ByteSize stored = m_buffer.size();
	if (m_position_offset > StormByte::ByteSize{0} && m_position_offset <= stored) {
		const StormByte::ByteSize remaining = stored - m_position_offset;
		if (remaining > StormByte::ByteSize{0}) {
			const std::size_t off = static_cast<std::size_t>(m_position_offset);
			const std::size_t rest = static_cast<std::size_t>(remaining);
			std::memmove(m_buffer.data(), m_buffer.data() + off, rest);
			m_buffer.resize(remaining);
			if (m_buffer.capacity() > remaining * 4 && m_buffer.capacity() > StormByte::ByteSize{4096})
				m_buffer.shrink_to_fit();
		}
		else {
			m_buffer.clear();
			if (m_buffer.capacity() > StormByte::ByteSize{4096})
				m_buffer.shrink_to_fit();
		}
	}
	else {
		m_buffer.clear();
	}

	m_position_offset = StormByte::ByteSize{0};
}

void FIFO::Clear() noexcept {
	m_buffer.clear();
	m_position_offset = StormByte::ByteSize{0};
}

void FIFO::Close() noexcept {
	m_closed = true;
}

bool FIFO::Drop(const StormByte::ByteSize& count) noexcept {
	const StormByte::ByteSize avail = AvailableInternal();
	if (avail == StormByte::ByteSize{0} || count > avail)
		return false;
	const StormByte::ByteSize stored = m_buffer.size();
	m_position_offset = std::min(m_position_offset + count, stored);
	FIFO::Clean();
	return true;
}

void FIFO::Seek(const std::ptrdiff_t& offset, const Position& mode) const noexcept {
	const StormByte::ByteSize stored = m_buffer.size();
	switch (mode) {
		case Position::Absolute:
			if (offset < 0)
				m_position_offset = StormByte::ByteSize{0};
			else
				m_position_offset = std::min(StormByte::ByteSize{static_cast<std::size_t>(offset)}, stored);
			break;
		case Position::Relative:
			if (offset < 0)
				m_position_offset = StormByte::ByteSize{static_cast<std::size_t>(
					std::max<std::ptrdiff_t>(0, static_cast<std::ptrdiff_t>(m_position_offset) + offset))};
			else
				m_position_offset = std::min(m_position_offset + StormByte::ByteSize{static_cast<std::size_t>(offset)}, stored);
			break;
		default:
			return;
	}
}

void FIFO::SetError() noexcept {
	m_error = true;
}

StormByte::String::String FIFO::HexDump(const StormByte::ByteSize& columns, const StormByte::ByteSize& byte_limit) const noexcept {
	const StormByte::ByteSize cols = (columns == StormByte::ByteSize{0}) ? StormByte::ByteSize{16} : columns;
	const StormByte::ByteSize stored = m_buffer.size();
	const StormByte::ByteSize end = (byte_limit > StormByte::ByteSize{0})
		? std::min(stored, m_position_offset + byte_limit)
		: stored;
	std::ostringstream oss = HexDumpHeader();
	oss << '\n';
	if (end > m_position_offset) {
		const std::size_t off = static_cast<std::size_t>(m_position_offset);
		const std::size_t len = static_cast<std::size_t>(end - m_position_offset);
		std::span<const std::byte> view(m_buffer.data() + off, len);
		oss << static_cast<const char*>(FormatHexLines(view, m_position_offset, cols));
	}

	return StormByte::String::String{oss.str()};
}

StormByte::String::String FIFO::FormatHexLines(std::span<const std::byte>& data, StormByte::ByteSize start_offset, StormByte::ByteSize columns) noexcept {
	const std::size_t cols = static_cast<std::size_t>((columns == StormByte::ByteSize{0}) ? StormByte::ByteSize{16} : columns);
	const int offset_width = 8;
	std::vector<std::string> lines;
	for (std::size_t i = 0; i < data.size(); i += cols) {
		const std::size_t line_end = std::min(data.size(), i + cols);
		std::ostringstream line;
		line << std::hex << std::uppercase << std::setw(offset_width) << std::setfill('0')
			<< static_cast<std::size_t>(start_offset + StormByte::ByteSize{i}) << ": " << std::dec << std::setfill(' ');
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
			if (std::isprint(c))
				line << static_cast<char>(c);
			else
				line << '.';
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

std::ostringstream FIFO::HexDumpHeader() const noexcept {
	std::ostringstream oss;
	oss << "Size: " << static_cast<std::size_t>(m_buffer.size()) << " bytes\n";
	oss << "Read Position: " << static_cast<std::size_t>(m_position_offset) << '\n';
	oss << "Status: " << (m_closed ? "closed" : "open")
		<< " / " << (m_error ? "error" : "ok");
	return oss;
}

StormByte::ByteSize FIFO::AvailableInternal() const noexcept {
	const StormByte::ByteSize stored = m_buffer.size();
	return (m_position_offset <= stored) ? (stored - m_position_offset) : StormByte::ByteSize{0};
}

bool FIFO::Extract(const StormByte::ByteSize& count, StormByte::BinaryData& outBuffer) noexcept {
	return ReadInternal(count, outBuffer, Operation::Extract);
}

bool FIFO::Extract(const StormByte::ByteSize& count, WriteOnly& outBuffer) noexcept {
	return ReadInternal(count, outBuffer, Operation::Extract);
}

void FIFO::ExtractUntilEoF(StormByte::BinaryData& outBuffer) noexcept {
	ReadUntilEoFInternal(outBuffer, Operation::Extract);
}

void FIFO::ExtractUntilEoF(WriteOnly& outBuffer) noexcept {
	ReadUntilEoFInternal(outBuffer, Operation::Extract);
}

bool FIFO::Read(const StormByte::ByteSize& count, StormByte::BinaryData& outBuffer) const noexcept {
	return const_cast<FIFO*>(this)->ReadInternal(count, outBuffer, Operation::Read);
}

bool FIFO::Read(const StormByte::ByteSize& count, WriteOnly& outBuffer) const noexcept {
	return const_cast<FIFO*>(this)->ReadInternal(count, outBuffer, Operation::Read);
}

void FIFO::ReadUntilEoF(StormByte::BinaryData& outBuffer) const noexcept {
	const_cast<FIFO*>(this)->ReadUntilEoFInternal(outBuffer, Operation::Read);
}

void FIFO::ReadUntilEoF(WriteOnly& outBuffer) const noexcept {
	const_cast<FIFO*>(this)->ReadUntilEoFInternal(outBuffer, Operation::Read);
}

bool FIFO::Peek(const StormByte::ByteSize& count, StormByte::BinaryData& outBuffer) const noexcept {
	return const_cast<FIFO*>(this)->ReadInternal(count, outBuffer, Operation::Peek);
}

bool FIFO::Peek(const StormByte::ByteSize& count, WriteOnly& outBuffer) const noexcept {
	return const_cast<FIFO*>(this)->ReadInternal(count, outBuffer, Operation::Peek);
}

bool FIFO::Write(const StormByte::ByteSize& count, const StormByte::BinaryData& data) noexcept {
	return WriteInternal(count, data);
}

bool FIFO::Write(const StormByte::ByteSize& count, StormByte::BinaryData&& data) noexcept {
	return WriteInternal(count, std::move(data));
}

bool FIFO::Write(const StormByte::ByteSize& count, const ReadOnly& data) noexcept {
	return WriteInternal(count, data);
}

bool FIFO::Write(const StormByte::ByteSize& count, ReadOnly&& data) noexcept {
	return WriteInternal(count, std::move(data));
}

bool FIFO::ReadInternal(const StormByte::ByteSize& count, StormByte::BinaryData& outBuffer, const Operation& flag) noexcept {
	if (m_error)
		return false;
	const StormByte::ByteSize available_bytes = AvailableInternal();
	const StormByte::ByteSize real_count = (count == StormByte::ByteSize{0}) ? available_bytes : count;
	if ((available_bytes == StormByte::ByteSize{0} && count == StormByte::ByteSize{0}) || real_count > available_bytes)
		return false;
	const std::size_t n = static_cast<std::size_t>(real_count);
	outBuffer.reserve(outBuffer.size() + StormByte::ByteSize{n});
	const auto start_it = m_buffer.begin() + static_cast<std::ptrdiff_t>(m_position_offset);
	switch (flag) {
		case Operation::Read: {
			outBuffer.insert(outBuffer.end(), start_it, start_it + static_cast<std::ptrdiff_t>(n));
			m_position_offset = m_position_offset + real_count;
			break;
		}
		case Operation::Peek: {
			outBuffer.insert(outBuffer.end(), start_it, start_it + static_cast<std::ptrdiff_t>(n));
			break;
		}
		case Operation::Extract: {
			outBuffer.insert(outBuffer.end(),
				std::make_move_iterator(start_it),
				std::make_move_iterator(start_it + static_cast<std::ptrdiff_t>(n)));
			m_buffer.erase(start_it, start_it + static_cast<std::ptrdiff_t>(n));
			const StormByte::ByteSize stored = m_buffer.size();
			if (m_position_offset > stored)
				m_position_offset = stored;
			break;
		}
		default:
			return false;
	}

	return true;
}

bool FIFO::ReadInternal(const StormByte::ByteSize& count, WriteOnly& outBuffer, const Operation& flag) noexcept {
	if (m_error)
		return false;
	const StormByte::ByteSize available_bytes = AvailableInternal();
	const StormByte::ByteSize real_count = (count == StormByte::ByteSize{0}) ? available_bytes : count;
	if ((count == StormByte::ByteSize{0} && available_bytes == StormByte::ByteSize{0}) || real_count > available_bytes)
		return false;
	StormByte::BinaryData temp;
	temp.reserve(real_count);
	if (!FIFO::ReadInternal(count, temp, flag))
		return false;
	return outBuffer.Write(std::move(temp));
}

void FIFO::ReadUntilEoFInternal(StormByte::BinaryData& outBuffer, const Operation& flag) noexcept {
	while (true) {
		switch (flag) {
			case Operation::Read:
				(void)Read(StormByte::ByteSize{0}, outBuffer);
				break;
			case Operation::Extract:
				(void)Extract(StormByte::ByteSize{0}, outBuffer);
				break;
			default:
				return;
		}

		StormByte::BinaryData unused;
		if (!Peek(StormByte::ByteSize{1}, unused))
			return;
	}
}

void FIFO::ReadUntilEoFInternal(WriteOnly& outBuffer, const Operation& flag) noexcept {
	while (true) {
		switch (flag) {
			case Operation::Read:
				(void)Read(StormByte::ByteSize{0}, outBuffer);
				break;
			case Operation::Extract:
				(void)Extract(StormByte::ByteSize{0}, outBuffer);
				break;
			default:
				return;
		}

		StormByte::BinaryData unused;
		if (!Peek(StormByte::ByteSize{1}, unused))
			return;
	}
}

bool FIFO::WriteInternal(const StormByte::ByteSize& count, const StormByte::BinaryData& src) noexcept {
	if (m_closed || m_error)
		return false;
	const StormByte::ByteSize src_size = src.size();
	if (count > StormByte::ByteSize{0} && src_size < count)
		return false;
	const StormByte::ByteSize real_count = (count == StormByte::ByteSize{0}) ? src_size : count;
	if (real_count == src_size)
		m_buffer.append(src);
	else
		m_buffer.append(src.data(), real_count);
	return true;
}

bool FIFO::WriteInternal(const StormByte::ByteSize& count, StormByte::BinaryData&& src) noexcept {
	if (m_closed || m_error)
		return false;
	const StormByte::ByteSize src_size = src.size();
	if (count > StormByte::ByteSize{0} && src_size < count)
		return false;
	const StormByte::ByteSize real_count = (count == StormByte::ByteSize{0}) ? src_size : count;
	if (real_count == src_size)
		m_buffer.append(std::move(src));
	else {
		m_buffer.append(src.data(), real_count);
		src.erase(src.begin(), src.begin() + static_cast<std::ptrdiff_t>(real_count));
	}
	return true;
}

bool FIFO::WriteInternal(const StormByte::ByteSize& count, const ReadOnly& src) noexcept {
	if (m_closed || m_error)
		return false;
	return src.Read(count, m_buffer);
}

bool FIFO::WriteInternal(const StormByte::ByteSize& count, ReadOnly&& src) noexcept {
	if (m_closed || m_error)
		return false;
	return src.Extract(count, m_buffer);
}
