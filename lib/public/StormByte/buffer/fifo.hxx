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

#include <StormByte/buffer/generic.hxx>
#include <StormByte/buffer/typedefs.hxx>
#include <StormByte/string/string.hxx>

#include <sstream>
#include <span>
#include <utility>

/**
 * @namespace StormByte::Buffer
 * @brief Buffer module of the StormByte suite.
 */
namespace StormByte::Buffer {
	/**
	 * @class FIFO
	 * @brief Byte-oriented FIFO buffer with grow-on-demand and close/error support.
	 *
	 * @par Overview
	 * A contiguous growable buffer implemented atop @ref StormByte::BinaryData
	 * that tracks a logical read position. It grows automatically to fit writes
	 * and supports efficient non-destructive reads and destructive extracts.
	 *
	 * @par Thread safety
	 * This class is **not thread-safe**. For concurrent access use @ref SharedFIFO
	 * or @ref Ring.
	 *
	 * @par Buffer behaviour
	 * Supports clear / clean, a movable read position for non-destructive reads,
	 * and a closed / error state to signal end-of-writes or permanent failure.
	 * Once closed, further writes fail; readers can still drain remaining data
	 * until @ref EoF().
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
			 * @brief Construct FIFO with initial data (copy).
			 * @param data Initial @ref StormByte::BinaryData.
			 */
			inline FIFO(const StormByte::BinaryData& data) noexcept
				: m_buffer(data), m_position_offset(0) {}

			/**
			 * @brief Construct FIFO with initial data (move).
			 * @param data Initial @ref StormByte::BinaryData (moved into the FIFO).
			 */
			inline FIFO(StormByte::BinaryData&& data) noexcept
				: m_buffer(std::move(data)), m_position_offset(0) {}

			/**
			 * @brief Construct FIFO from an input range (copy / convert).
			 * @tparam R Input range whose elements are convertible to @c std::byte.
			 * @param r Range to copy from.
			 * @note Disabled when @p R is already @ref StormByte::BinaryData to avoid overload ambiguity.
			 */
			template<Type::ByteInputRange R>
			requires (!Type::SameAs<R, StormByte::BinaryData>)
			inline FIFO(const R& r) noexcept
				: m_buffer(DataConvert(r)), m_position_offset(0) {}

			/**
			 * @brief Construct FIFO from an rvalue range (moves when @ref StormByte::BinaryData).
			 * @tparam Rr Input range type.
			 * @param r Range to consume.
			 */
			template<Type::ByteInputRange Rr>
			inline FIFO(Rr&& r) noexcept
				: m_buffer(DataConvert(std::forward<Rr>(r))), m_position_offset(0) {}

			/**
			 * @brief Construct FIFO from a string view (no trailing NUL).
			 * @param sv Source characters.
			 */
			inline FIFO(std::string_view sv) noexcept
				: m_buffer(DataConvert(sv)), m_position_offset(0) {}

			/**
			 * @brief Construct FIFO from a null-terminated C string.
			 * @param s Source string (may be null → empty buffer).
			 */
			inline FIFO(const char* s) noexcept
				: FIFO(s ? std::string_view(s) : std::string_view()) {}

			/**
			 * @brief Copy construct, preserving buffer contents and state.
			 * @param other Source FIFO.
			 */
			FIFO(const FIFO& other) noexcept;

			/**
			 * @brief Move construct.
			 * @param other Source FIFO (left empty / unspecified but valid).
			 */
			FIFO(FIFO&& other) noexcept;

			/**
			 * @brief Virtual destructor.
			 */
			virtual ~FIFO() noexcept;

			/**
			 * @brief Copy assign, preserving buffer contents and state.
			 * @param other Source FIFO.
			 * @return Reference to this FIFO.
			 */
			FIFO& operator=(const FIFO& other);

			/**
			 * @brief Move assign.
			 * @param other Source FIFO.
			 * @return Reference to this FIFO.
			 */
			FIFO& operator=(FIFO&& other) noexcept;

			/** @} */

			/**
			 * @name Comparison
			 * @{
			 */

			/**
			 * @brief Equality comparison.
			 * @param other Other FIFO.
			 * @return @c true if buffer contents, read position and closed/error flags match.
			 */
			inline bool operator==(const FIFO& other) const noexcept {
				return m_buffer == other.m_buffer &&
					m_position_offset == other.m_position_offset &&
					m_closed == other.m_closed &&
					m_error == other.m_error;
			}

			/**
			 * @brief Inequality comparison.
			 * @param other Other FIFO.
			 * @return Negation of @ref operator==.
			 */
			inline bool operator!=(const FIFO& other) const noexcept {
				return !(*this == other);
			}

			/** @} */

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
			 * @return Constant reference to the owned @ref StormByte::BinaryData.
			 */
			virtual const StormByte::BinaryData& BinaryData() const noexcept override;

			/**
			 * @brief Whether the underlying storage is empty.
			 * @return @c true if @c m_buffer is empty.
			 * @note With a non-zero read position, @ref Empty() may be @c false while
			 *       @ref Available() is zero.
			 * @see Size(), Available()
			 */
			virtual bool Empty() const noexcept override;

			/**
			 * @brief End-of-stream condition.
			 * @return @c true if in error, or closed with no remaining unread bytes.
			 */
			virtual bool EoF() const noexcept override;

			/**
			 * @brief Whether the buffer can be read.
			 * @return @c false in permanent error state.
			 */
			virtual bool IsReadable() const noexcept override;

			/**
			 * @brief Whether the buffer accepts writes.
			 * @return @c false if closed or in error.
			 */
			virtual bool IsWritable() const noexcept override;

			/**
			 * @brief Total number of bytes stored (including already-read prefix).
			 * @return Size of the internal buffer.
			 * @see Empty(), Available()
			 */
			virtual StormByte::ByteSize Size() const noexcept override;

			/** @} */

			/**
			 * @name Maintenance / lifecycle
			 * @{
			 */

			/**
			 * @brief Discard data from the start up to the current read position.
			 */
			virtual void Clean() noexcept override;

			/**
			 * @brief Clear all buffer contents and reset the read position.
			 * @details Does **not** clear the closed / error flags.
			 * @see Size(), Empty()
			 */
			virtual void Clear() noexcept override;

			/**
			 * @brief Mark the buffer closed for further writes.
			 * @details Subsequent @c Write() calls fail. Readers may still drain
			 *          remaining data until @ref Available() is zero.
			 */
			void Close() noexcept override;

			/**
			 * @brief Discard @p count unread bytes and advance the read position.
			 * @param count Number of bytes to drop.
			 * @return @c true on success, @c false if fewer bytes were available.
			 * @see Read(), Seek()
			 */
			virtual bool Drop(const StormByte::ByteSize& count) noexcept override;

			/**
			 * @brief Move the logical read position for non-destructive reads.
			 * @param offset Offset value.
			 * @param mode @ref Position::Absolute or @ref Position::Relative.
			 * @details Position is clamped to @c [0, Size()]. Does not modify stored data.
			 */
			virtual void Seek(const std::ptrdiff_t& offset, const Position& mode) const noexcept override;

			/**
			 * @brief Enter permanent error state (unreadable and unwritable).
			 */
			void SetError() noexcept override;

			/** @} */

			/**
			 * @name Extract (destructive)
			 * @{
			 */

			/**
			 * @brief Extract bytes into a @ref StormByte::BinaryData.
			 * @param count Bytes to extract; 0 = all available.
			 * @param outBuffer Destination (filled / resized as needed).
			 * @return @c true on success, @c false on failure.
			 */
			bool Extract(const StormByte::ByteSize& count, StormByte::BinaryData& outBuffer) noexcept override;

			/**
			 * @brief Extract bytes into a @ref WriteOnly.
			 * @param count Bytes to extract; 0 = all available.
			 * @param outBuffer Destination writer.
			 * @return @c true on success, @c false on failure.
			 */
			bool Extract(const StormByte::ByteSize& count, WriteOnly& outBuffer) noexcept override;

			/** @brief Bring @ref ReadOnly convenience Extract overloads into scope. */
			using ReadOnly::Extract;

			/**
			 * @brief Extract all remaining bytes until EoF into a @ref StormByte::BinaryData.
			 * @param outBuffer Destination buffer.
			 */
			void ExtractUntilEoF(StormByte::BinaryData& outBuffer) noexcept override;

			/**
			 * @brief Extract all remaining bytes until EoF into a @ref WriteOnly.
			 * @param outBuffer Destination writer.
			 */
			void ExtractUntilEoF(WriteOnly& outBuffer) noexcept override;

			/** @} */

			/**
			 * @name Read (non-destructive)
			 * @{
			 */

			/**
			 * @brief Non-destructive read into a @ref StormByte::BinaryData (advances position).
			 * @param count Bytes to read; 0 = all available.
			 * @param outBuffer Destination buffer.
			 * @return @c true on success, @c false on failure.
			 */
			bool Read(const StormByte::ByteSize& count, StormByte::BinaryData& outBuffer) const noexcept override;

			/**
			 * @brief Non-destructive read into a @ref WriteOnly (advances position).
			 * @param count Bytes to read; 0 = all available.
			 * @param outBuffer Destination writer.
			 * @return @c true on success, @c false on failure.
			 */
			bool Read(const StormByte::ByteSize& count, WriteOnly& outBuffer) const noexcept override;

			/** @brief Bring @ref ReadOnly convenience Read overloads into scope. */
			using ReadOnly::Read;

			/**
			 * @brief Read all remaining bytes until EoF into a @ref StormByte::BinaryData.
			 * @param outBuffer Destination buffer.
			 */
			void ReadUntilEoF(StormByte::BinaryData& outBuffer) const noexcept override;

			/**
			 * @brief Read all remaining bytes until EoF into a @ref WriteOnly.
			 * @param outBuffer Destination writer.
			 */
			void ReadUntilEoF(WriteOnly& outBuffer) const noexcept override;

			/** @} */

			/**
			 * @name Peek (non-destructive, does not advance)
			 * @{
			 */

			/**
			 * @brief Peek into a @ref StormByte::BinaryData without advancing the read position.
			 * @param count Bytes to peek; 0 = all available.
			 * @param outBuffer Destination buffer.
			 * @return @c true on success, @c false on failure.
			 */
			bool Peek(const StormByte::ByteSize& count, StormByte::BinaryData& outBuffer) const noexcept override;

			/**
			 * @brief Peek into a @ref WriteOnly without advancing the read position.
			 * @param count Bytes to peek; 0 = all available.
			 * @param outBuffer Destination writer.
			 * @return @c true on success, @c false on failure.
			 */
			bool Peek(const StormByte::ByteSize& count, WriteOnly& outBuffer) const noexcept override;

			/** @} */

			/**
			 * @name Diagnostics
			 * @{
			 */

			/**
			 * @brief Hexdump of unread contents from the current read position.
			 * @param columns Bytes per line (0 → default 16).
			 * @param byte_limit Max bytes to include (0 → no limit).
			 * @return Formatted string: size / position / status, then hex/ASCII lines
			 *         (no trailing newline).
			 */
			virtual StormByte::String::String HexDump(const StormByte::ByteSize& columns = 16,
										const StormByte::ByteSize& byte_limit = 0) const noexcept;

			/** @} */

			/**
			 * @name Write
			 * @{
			 */

			/**
			 * @brief Append bytes from a @ref StormByte::BinaryData (copy).
			 * @param count Number of bytes to write.
			 * @param data Source.
			 * @return @c true on success, @c false if closed / error.
			 */
			bool Write(const StormByte::ByteSize& count, const StormByte::BinaryData& data) noexcept override;

			/**
			 * @brief Append bytes from a @ref StormByte::BinaryData (move).
			 * @param count Number of bytes to write.
			 * @param data Source.
			 * @return @c true on success, @c false if closed / error.
			 */
			bool Write(const StormByte::ByteSize& count, StormByte::BinaryData&& data) noexcept override;

			/**
			 * @brief Append bytes from a @ref ReadOnly source (copy).
			 * @param count Number of bytes to write.
			 * @param data Source buffer.
			 * @return @c true on success, @c false if closed / error.
			 */
			bool Write(const StormByte::ByteSize& count, const ReadOnly& data) noexcept override;

			/**
			 * @brief Append bytes from a @ref ReadOnly source (move / extract path).
			 * @param count Number of bytes to write.
			 * @param data Source buffer.
			 * @return @c true on success, @c false if closed / error.
			 */
			bool Write(const StormByte::ByteSize& count, ReadOnly&& data) noexcept override;

			/** @brief Bring @ref WriteOnly convenience Write overloads into scope. */
			using WriteOnly::Write;

			/** @} */

		protected:
			/**
			 * @brief Return unread bytes without virtual dispatch.
			 * @return Number of bytes from the current read position.
			 * @note Callers that synchronize derived state must hold their own lock.
			 */
			StormByte::ByteSize AvailableInternal() const noexcept;

			StormByte::BinaryData m_buffer;							///< Owned contiguous byte storage
			mutable StormByte::ByteSize m_position_offset {0};	///< Logical read offset from start of m_buffer
			bool m_closed {false};						///< Once true, further writes fail
			bool m_error {false};						///< Permanent error: unreadable and unwritable

			/**
			 * @brief Kind of internal read operation.
			 */
			enum class Operation {
				Extract,	///< Destructive read
				Read,		///< Non-destructive read (advances position)
				Peek		///< Non-destructive peek (no advance)
			};

			/**
			 * @brief Format hex/ASCII lines for a data span.
			 * @param data Bytes to dump.
			 * @param start_offset Display offset of the first byte.
			 * @param columns Bytes per line.
			 * @return Formatted lines (no header).
			 */
			static StormByte::String::String FormatHexLines(std::span<const std::byte>& data,
											StormByte::ByteSize start_offset,
											StormByte::ByteSize columns) noexcept;

			/**
			 * @brief Build the hexdump header (size / position / status).
			 * @return Stream containing the header lines.
			 */
			virtual std::ostringstream HexDumpHeader() const noexcept;

			/**
			 * @name Internal read / write helpers
			 * @{
			 */

			/**
			 * @brief Shared implementation for Extract / Read / Peek into @ref StormByte::BinaryData.
			 * @param count Requested byte count (0 = all available).
			 * @param outBuffer Destination.
			 * @param flag Operation kind.
			 * @return @c true on success.
			 */
			virtual bool ReadInternal(const StormByte::ByteSize& count, StormByte::BinaryData& outBuffer,
									const Operation& flag) noexcept;

			/**
			 * @brief Shared implementation for Extract / Read / Peek into @ref WriteOnly.
			 * @param count Requested byte count (0 = all available).
			 * @param outBuffer Destination writer.
			 * @param flag Operation kind.
			 * @return @c true on success.
			 */
			virtual bool ReadInternal(const StormByte::ByteSize& count, WriteOnly& outBuffer,
									const Operation& flag) noexcept;

			/**
			 * @brief Drain until EoF into @ref StormByte::BinaryData using @p flag semantics.
			 * @param outBuffer Destination.
			 * @param flag Extract or Read.
			 */
			virtual void ReadUntilEoFInternal(StormByte::BinaryData& outBuffer, const Operation& flag) noexcept;

			/**
			 * @brief Drain until EoF into @ref WriteOnly using @p flag semantics.
			 * @param outBuffer Destination.
			 * @param flag Extract or Read.
			 */
			virtual void ReadUntilEoFInternal(WriteOnly& outBuffer, const Operation& flag) noexcept;

			/**
			 * @brief Append from @ref StormByte::BinaryData (copy).
			 * @param count Number of bytes.
			 * @param src Source.
			 * @return @c true on success, @c false if closed / error.
			 */
			virtual bool WriteInternal(const StormByte::ByteSize& count, const StormByte::BinaryData& src) noexcept;

			/**
			 * @brief Append from @ref StormByte::BinaryData (move).
			 * @param count Number of bytes.
			 * @param src Source.
			 * @return @c true on success, @c false if closed / error.
			 */
			virtual bool WriteInternal(const StormByte::ByteSize& count, StormByte::BinaryData&& src) noexcept;

			/**
			 * @brief Append from @ref ReadOnly (copy path).
			 * @param count Number of bytes.
			 * @param src Source.
			 * @return @c true on success, @c false if closed / error.
			 */
			virtual bool WriteInternal(const StormByte::ByteSize& count, const ReadOnly& src) noexcept;

			/**
			 * @brief Append from @ref ReadOnly (move / extract path).
			 * @param count Number of bytes.
			 * @param src Source.
			 * @return @c true on success, @c false if closed / error.
			 */
			virtual bool WriteInternal(const StormByte::ByteSize& count, ReadOnly&& src) noexcept;

			/** @} */
	};
}
