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

#pragma once

#include <StormByte/type_traits/safe.hxx>

#include <StormByte/buffer/generic.hxx>
#include <StormByte/buffer/typedefs.hxx>
#include <StormByte/safe/string.hxx>

#include <span>
#include <sstream>
#include <string_view>
#include <utility>

/**
 * @namespace StormByte
 * @brief Root namespace of the StormByte C++ suite.
 */
namespace StormByte {
	/**
	 * @namespace StormByte::Buffer
	 * @brief Buffer module of the StormByte suite.
	 */
	namespace Buffer {
		/**
		 * @class FIFO
		 * @brief Byte-oriented FIFO buffer with grow-on-demand and close/error support.
		 *
		 * A contiguous growable buffer implemented atop @ref StormByte::Safe::Binary
		 * that tracks a logical read position. It grows automatically to fit writes
		 * and supports non-destructive reads and destructive extracts.
		 *
		 * This class is not thread-safe. For concurrent access use @ref SharedFIFO
		 * or @ref Ring.
		 *
		 * Supports clear / clean, a movable read position for non-destructive reads,
		 * and a closed / error state to signal end-of-writes or permanent failure.
		 * Once closed, further writes fail; readers can still drain remaining data
		 * until @ref EoF().
		 *
		 * Construction from @ref StormByte::Safe::Binary, a byte range, a string view
		 * or a C string is implicit.
		 *
		 * @see SharedFIFO, Ring, ReadWrite, Producer, Consumer
		 */
		class STORMBYTE_BUFFER_PUBLIC FIFO: public ReadWrite {
			public:
				/**
				 * @name Constructors / destructor / assignment
				 * @{
				 */

				/**
				 * @brief Default construct an empty FIFO.
				 */
				FIFO() noexcept = default;

				/**
				 * @brief Construct a FIFO with initial data (copy). Implicit.
				 * @param data Initial bytes.
				 */
				inline FIFO(const StormByte::Safe::Binary& data) noexcept
					: m_buffer(data), m_position_offset(0) {}

				/**
				 * @brief Construct a FIFO with initial data (move). Implicit.
				 * @param data Initial bytes. Moved into the FIFO.
				 */
				inline FIFO(StormByte::Safe::Binary&& data) noexcept
					: m_buffer(std::move(data)), m_position_offset(0) {}

				/**
				 * @brief Construct a FIFO from an input range. Implicit.
				 * @tparam R Input range whose elements convert to @c std::byte.
				 * @param r Range to copy from.
				 * @note Disabled when @p R is already @ref StormByte::Safe::Binary.
				 */
				template<Type::ByteInputRange R>
				requires (!Type::SameAs<R, StormByte::Safe::Binary>)
				inline FIFO(const R& r) noexcept
					: m_buffer(DataConvert(r)), m_position_offset(0) {}

				/**
				 * @brief Construct a FIFO from an rvalue range. Implicit.
				 * @tparam Rr Input range type. Moved when it is @ref StormByte::Safe::Binary.
				 * @param r Range to consume.
				 */
				template<Type::ByteInputRange Rr>
				inline FIFO(Rr&& r) noexcept
					: m_buffer(DataConvert(std::forward<Rr>(r))), m_position_offset(0) {}

				/**
				 * @brief Construct a FIFO from a string view. Implicit. No trailing NUL.
				 * @param sv Source characters.
				 */
				inline FIFO(std::string_view sv) noexcept
					: m_buffer(DataConvert(sv)), m_position_offset(0) {}

				/**
				 * @brief Construct a FIFO from a C string. Implicit.
				 * @param s Source. Null yields an empty buffer.
				 */
				inline FIFO(const char* s) noexcept
					: FIFO(s ? std::string_view(s) : std::string_view()) {}

				/**
				 * @brief Copy a FIFO, including contents and state.
				 * @param other Source.
				 */
				FIFO(const FIFO& other) noexcept;

				/**
				 * @brief Take a FIFO. @p other is left empty and valid.
				 * @param other Source.
				 */
				FIFO(FIFO&& other) noexcept;

				/**
				 * @brief Destroy the FIFO.
				 */
				virtual ~FIFO() noexcept;

				/**
				 * @brief Copy-assign a FIFO, including contents and state.
				 * @param other Source.
				 * @return This FIFO.
				 */
				FIFO& operator=(const FIFO& other);

				/**
				 * @brief Move-assign a FIFO. @p other is left empty and valid.
				 * @param other Source.
				 * @return This FIFO.
				 */
				FIFO& operator=(FIFO&& other) noexcept;

				/**
				 * @}
				 */

				/**
				 * @name Comparison
				 * @{
				 */

				/**
				 * @brief Compare two FIFOs.
				 * @param other Other FIFO.
				 * @return Whether contents, read position and closed/error flags match.
				 */
				inline bool operator==(const FIFO& other) const noexcept {
					return m_buffer == other.m_buffer &&
						m_position_offset == other.m_position_offset &&
						m_closed == other.m_closed &&
						m_error == other.m_error;
				}

				/**
				 * @brief Compare two FIFOs.
				 * @param other Other FIFO.
				 * @return Whether the FIFOs differ.
				 */
				inline bool operator!=(const FIFO& other) const noexcept {
					return !(*this == other);
				}

				/**
				 * @}
				 */

				/**
				 * @name Queries
				 * @{
				 */

				/**
				 * @brief Bytes available from the current read position.
				 * @return Unread byte count.
				 * @see Size(), Read(), Extract(), Seek()
				 */
				virtual StormByte::ByteSize Available() const noexcept override;

				/**
				 * @brief Access the internal storage.
				 * @return Owned bytes.
				 */
				virtual const StormByte::Safe::Binary& Data() const noexcept override;

				/**
				 * @brief Whether the underlying storage is empty.
				 * @return Whether @c m_buffer is empty.
				 * @note With a non-zero read position, this may be false while @ref Available() is zero.
				 * @see Size(), Available()
				 */
				virtual bool Empty() const noexcept override;

				/**
				 * @brief End-of-stream condition.
				 * @return Whether the FIFO is in error, or closed with nothing left to read.
				 */
				virtual bool EoF() const noexcept override;

				/**
				 * @brief Whether the buffer can be read.
				 * @return False in a permanent error state.
				 */
				virtual bool IsReadable() const noexcept override;

				/**
				 * @brief Whether the buffer accepts writes.
				 * @return False if closed or in error.
				 */
				virtual bool IsWritable() const noexcept override;

				/**
				 * @brief Total number of bytes stored, including the already-read prefix.
				 * @return Size of the internal buffer.
				 * @see Empty(), Available()
				 */
				virtual StormByte::ByteSize Size() const noexcept override;

				/**
				 * @}
				 */

				/**
				 * @name Maintenance / lifecycle
				 * @{
				 */

				/**
				 * @brief Discard data from the start up to the current read position.
				 */
				virtual void Clean() noexcept override;

				/**
				 * @brief Clear stored bytes and reset the read position.
				 * @details Does not clear the closed or error flags.
				 * @see Size(), Empty()
				 */
				virtual void Clear() noexcept override;

				/**
				 * @brief Mark the buffer closed for further writes.
				 * @details Later Write calls fail. Readers may still drain until @ref Available() is zero.
				 */
				void Close() noexcept override;

				/**
				 * @brief Discard unread bytes and advance the read position.
				 * @param count Bytes to drop.
				 * @return False if fewer bytes were available.
				 * @see Read(), Seek()
				 */
				virtual bool Drop(const StormByte::ByteSize& count) noexcept override;

				/**
				 * @brief Move the logical read position.
				 * @param offset Offset.
				 * @param mode Absolute or relative.
				 * @details Clamped to [0, Size()]. Does not modify stored data.
				 */
				virtual void Seek(const std::ptrdiff_t& offset, const Position& mode) const noexcept override;

				/**
				 * @brief Enter the permanent error state.
				 */
				void SetError() noexcept override;

				/**
				 * @}
				 */

				/**
				 * @name Extract (destructive)
				 * @{
				 */

				/**
				 * @brief Extract bytes into owned storage.
				 * @param count Bytes to extract. 0 means all available.
				 * @param outBuffer Destination.
				 * @return False on failure.
				 */
				bool Extract(const StormByte::ByteSize& count, StormByte::Safe::Binary& outBuffer) noexcept override;

				/**
				 * @brief Extract bytes into a writer.
				 * @param count Bytes to extract. 0 means all available.
				 * @param outBuffer Destination writer.
				 * @return False on failure.
				 */
				bool Extract(const StormByte::ByteSize& count, WriteOnly& outBuffer) noexcept override;

				/**
				 * @brief Bring the ReadOnly convenience Extract overloads into scope.
				 */
				using ReadOnly::Extract;

				/**
				 * @brief Extract until EoF into owned storage.
				 * @param outBuffer Destination.
				 */
				void ExtractUntilEoF(StormByte::Safe::Binary& outBuffer) noexcept override;

				/**
				 * @brief Extract until EoF into a writer.
				 * @param outBuffer Destination writer.
				 */
				void ExtractUntilEoF(WriteOnly& outBuffer) noexcept override;

				/**
				 * @}
				 */

				/**
				 * @name Read (non-destructive)
				 * @{
				 */

				/**
				 * @brief Read into owned storage and advance the position.
				 * @param count Bytes to read. 0 means all available.
				 * @param outBuffer Destination.
				 * @return False on failure.
				 */
				bool Read(const StormByte::ByteSize& count, StormByte::Safe::Binary& outBuffer) const noexcept override;

				/**
				 * @brief Read into a writer and advance the position.
				 * @param count Bytes to read. 0 means all available.
				 * @param outBuffer Destination writer.
				 * @return False on failure.
				 */
				bool Read(const StormByte::ByteSize& count, WriteOnly& outBuffer) const noexcept override;

				/**
				 * @brief Bring the ReadOnly convenience Read overloads into scope.
				 */
				using ReadOnly::Read;

				/**
				 * @brief Read until EoF into owned storage.
				 * @param outBuffer Destination.
				 */
				void ReadUntilEoF(StormByte::Safe::Binary& outBuffer) const noexcept override;

				/**
				 * @brief Read until EoF into a writer.
				 * @param outBuffer Destination writer.
				 */
				void ReadUntilEoF(WriteOnly& outBuffer) const noexcept override;

				/**
				 * @}
				 */

				/**
				 * @name Peek (non-destructive, does not advance)
				 * @{
				 */

				/**
				 * @brief Peek into owned storage without advancing the position.
				 * @param count Bytes to peek. 0 means all available.
				 * @param outBuffer Destination.
				 * @return False on failure.
				 */
				bool Peek(const StormByte::ByteSize& count, StormByte::Safe::Binary& outBuffer) const noexcept override;

				/**
				 * @brief Peek into a writer without advancing the position.
				 * @param count Bytes to peek. 0 means all available.
				 * @param outBuffer Destination writer.
				 * @return False on failure.
				 */
				bool Peek(const StormByte::ByteSize& count, WriteOnly& outBuffer) const noexcept override;

				/**
				 * @}
				 */

				/**
				 * @name Diagnostics
				 * @{
				 */

				/**
				 * @brief Hexdump of unread contents from the current read position.
				 * @param columns Bytes per line. 0 means 16.
				 * @param byte_limit Maximum bytes to include. 0 means no limit.
				 * @return Size, position and status, then hex/ASCII lines. No trailing newline.
				 */
				virtual StormByte::Safe::String HexDump(const StormByte::ByteSize& columns = 16,
					const StormByte::ByteSize& byte_limit = 0) const noexcept;

				/**
				 * @}
				 */

				/**
				 * @name Write
				 * @{
				 */

				/**
				 * @brief Append bytes from owned storage (copy).
				 * @param count Bytes to write.
				 * @param data Source.
				 * @return False if closed or in error.
				 */
				bool Write(const StormByte::ByteSize& count, const StormByte::Safe::Binary& data) noexcept override;

				/**
				 * @brief Append bytes from owned storage (move).
				 * @param count Bytes to write.
				 * @param data Source.
				 * @return False if closed or in error.
				 */
				bool Write(const StormByte::ByteSize& count, StormByte::Safe::Binary&& data) noexcept override;

				/**
				 * @brief Append bytes from a readable buffer (copy).
				 * @param count Bytes to write.
				 * @param data Source buffer.
				 * @return False if closed or in error.
				 */
				bool Write(const StormByte::ByteSize& count, const ReadOnly& data) noexcept override;

				/**
				 * @brief Append bytes from a readable buffer (extract path).
				 * @param count Bytes to write.
				 * @param data Source buffer.
				 * @return False if closed or in error.
				 */
				bool Write(const StormByte::ByteSize& count, ReadOnly&& data) noexcept override;

				/**
				 * @brief Bring the WriteOnly convenience Write overloads into scope.
				 */
				using WriteOnly::Write;

				/**
				 * @}
				 */

			protected:
				/**
				 * @brief Return unread bytes without virtual dispatch.
				 * @return Bytes from the current read position.
				 * @note Callers that synchronize derived state must hold their own lock.
				 */
				StormByte::ByteSize AvailableInternal() const noexcept;

				StormByte::Safe::Binary m_buffer;					///< Owned contiguous byte storage.
				mutable StormByte::ByteSize m_position_offset {0};	///< Logical read offset from the start of m_buffer.
				bool m_closed {false};								///< Once true, further writes fail.
				bool m_error {false};								///< Permanent error: unreadable and unwritable.

				/**
				 * @brief Kind of internal read operation.
				 */
				enum class Operation {
					Extract,	///< Destructive read.
					Read,		///< Non-destructive read. Advances the position.
					Peek		///< Non-destructive peek. Does not advance.
				};

				/**
				 * @brief Format hex/ASCII lines for a data span.
				 * @param data Bytes to dump.
				 * @param start_offset Display offset of the first byte.
				 * @param columns Bytes per line.
				 * @return Formatted lines, without a header.
				 */
				static StormByte::Safe::String FormatHexLines(std::span<const std::byte>& data,
					StormByte::ByteSize start_offset,
					StormByte::ByteSize columns) noexcept;

				/**
				 * @brief Build the hexdump header.
				 * @return Owned header text. Any formatting stream stays in this module.
				 */
				virtual StormByte::Safe::String HexDumpHeader() const noexcept;

				/**
				 * @name Internal read / write helpers
				 * @{
				 */

				/**
				 * @brief Shared implementation for Extract, Read and Peek into owned storage.
				 * @param count Requested byte count. 0 means all available.
				 * @param outBuffer Destination.
				 * @param flag Operation kind.
				 * @return False on failure.
				 */
				virtual bool ReadInternal(const StormByte::ByteSize& count, StormByte::Safe::Binary& outBuffer,
					const Operation& flag) noexcept;

				/**
				 * @brief Shared implementation for Extract, Read and Peek into a writer.
				 * @param count Requested byte count. 0 means all available.
				 * @param outBuffer Destination writer.
				 * @param flag Operation kind.
				 * @return False on failure.
				 */
				virtual bool ReadInternal(const StormByte::ByteSize& count, WriteOnly& outBuffer,
					const Operation& flag) noexcept;

				/**
				 * @brief Drain until EoF into owned storage.
				 * @param outBuffer Destination.
				 * @param flag Extract or Read.
				 */
				virtual void ReadUntilEoFInternal(StormByte::Safe::Binary& outBuffer, const Operation& flag) noexcept;

				/**
				 * @brief Drain until EoF into a writer.
				 * @param outBuffer Destination.
				 * @param flag Extract or Read.
				 */
				virtual void ReadUntilEoFInternal(WriteOnly& outBuffer, const Operation& flag) noexcept;

				/**
				 * @brief Append from owned storage (copy).
				 * @param count Bytes to write.
				 * @param src Source.
				 * @return False if closed or in error.
				 */
				virtual bool WriteInternal(const StormByte::ByteSize& count, const StormByte::Safe::Binary& src) noexcept;

				/**
				 * @brief Append from owned storage (move).
				 * @param count Bytes to write.
				 * @param src Source.
				 * @return False if closed or in error.
				 */
				virtual bool WriteInternal(const StormByte::ByteSize& count, StormByte::Safe::Binary&& src) noexcept;

				/**
				 * @brief Append from a readable buffer (copy).
				 * @param count Bytes to write.
				 * @param src Source.
				 * @return False if closed or in error.
				 */
				virtual bool WriteInternal(const StormByte::ByteSize& count, const ReadOnly& src) noexcept;

				/**
				 * @brief Append from a readable buffer (extract path).
				 * @param count Bytes to write.
				 * @param src Source.
				 * @return False if closed or in error.
				 */
				virtual bool WriteInternal(const StormByte::ByteSize& count, ReadOnly&& src) noexcept;

				/**
				 * @}
				 */
		};
	}
}

STORMBYTE_DECLARE_MAYBE_SAFE(StormByte::Buffer::FIFO);
