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

#include <condition_variable>
#include <deque>
#include <shared_mutex>
#include <span>
#include <sstream>
#include <utility>

/**
 * @namespace StormByte::Buffer
 * @brief Buffer module of the StormByte suite.
 */
namespace StormByte::Buffer {
	/**
	 * @class Ring
	 * @brief High-performance, highly-concurrent thread-safe ring buffer.
	 *
	 * @par Overview
	 * Backed by @c std::deque&lt;std::byte&gt; with a logical read position.
	 * Uses @c std::shared_mutex so multiple readers can proceed concurrently
	 * while writers and destructive operations remain exclusive.
	 * Critical sections are kept as short as possible; blocked readers wait
	 * on a @c condition_variable_any.
	 *
	 * @par Thread safety
	 * Fully thread-safe for concurrent producers and consumers (many-to-many).
	 * @ref Pipeline intermediates use a private SPSC ring; this type is the
	 * public many-to-many ring.
	 *
	 * @par Lifecycle
	 * @ref Close() stops further writes; remaining data can still be drained.
	 * @ref SetError() makes the buffer permanently unreadable and unwritable
	 * and wakes waiters.
	 *
	 * @see Producer, Consumer, ReadWrite, SharedFIFO
	 */
	class STORMBYTE_BUFFER_PUBLIC Ring: public ReadWrite {
		public:
			/**
			 * @name Constructors / destructor / assignment
			 * @{
			 */

			/**
			 * @brief Default construct an empty Ring.
			 */
			Ring() noexcept = default;

			/**
			 * @brief Construct with initial data (copy).
			 * @param data Source bytes.
			 */
			inline explicit Ring(const StormByte::BinaryData& data) noexcept
				: m_buffer(data.begin(), data.end()) {}

			/**
			 * @brief Construct with initial data (move elements from @ref StormByte::BinaryData).
			 * @param data Source bytes (moved element-wise into the deque).
			 */
			inline explicit Ring(StormByte::BinaryData&& data) noexcept
				: m_buffer(std::make_move_iterator(data.begin()),
						std::make_move_iterator(data.end())) {}

			/**
			 * @brief Construct from an input range (copy / convert).
			 * @tparam R Range whose value_type is convertible to @c std::byte.
			 * @param r Source range (disabled when already @ref StormByte::BinaryData).
			 */
			template<Type::ByteInputRange R>
			requires (!Type::SameAs<R, StormByte::BinaryData>)
			inline explicit Ring(const R& r) noexcept {
				auto converted = DataConvert(r);
				m_buffer.assign(converted.begin(), converted.end());
			}

			/**
			 * @brief Construct from an rvalue range.
			 * @tparam Rr Range type.
			 * @param r Source range.
			 */
			template<Type::ByteInputRange Rr>
			inline explicit Ring(Rr&& r) noexcept {
				auto converted = DataConvert(std::forward<Rr>(r));
				m_buffer.assign(std::make_move_iterator(converted.begin()),
								std::make_move_iterator(converted.end()));
			}

			/**
			 * @brief Construct from a string view (no trailing NUL).
			 * @param sv Source characters.
			 */
			inline explicit Ring(std::string_view sv) noexcept {
				auto converted = DataConvert(sv);
				m_buffer.assign(converted.begin(), converted.end());
			}

			/**
			 * @brief Construct from a null-terminated C string.
			 * @param s Source string (may be null → empty).
			 */
			inline explicit Ring(const char* s) noexcept
				: Ring(s ? std::string_view(s) : std::string_view{}) {}

			/**
			 * @brief Copy constructor (deleted – mutexes are not copyable).
			 */
			Ring(const Ring&) = delete;

			/**
			 * @brief Move constructor.
			 * @param other Source Ring.
			 */
			Ring(Ring&& other) noexcept;

			/**
			 * @brief Virtual destructor.
			 */
			virtual ~Ring() noexcept;

			/**
			 * @brief Copy assignment (deleted).
			 */
			Ring& operator=(const Ring&) = delete;

			/**
			 * @brief Move assignment.
			 * @param other Source Ring.
			 * @return Reference to this Ring.
			 */
			Ring& operator=(Ring&& other) noexcept;

			/** @} */

			/**
			 * @name Comparison
			 * @{
			 */

			/**
			 * @brief Equality comparison (contents and lifecycle state).
			 * @param other Other Ring.
			 * @return @c true if equal under the comparison rules.
			 */
			bool operator==(const Ring& other) const noexcept;

			/**
			 * @brief Inequality comparison.
			 * @param other Other Ring.
			 * @return Negation of @ref operator==.
			 */
			inline bool operator!=(const Ring& other) const noexcept {
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
			 */
			StormByte::ByteSize Available() const noexcept override;

			/**
			 * @brief Snapshot of stored data (may rebuild an internal cache).
			 * @return Constant reference to a @ref StormByte::BinaryData view.
			 */
			const StormByte::BinaryData& Data() const noexcept override;

			/**
			 * @brief Whether the underlying storage is empty.
			 * @return @c true if empty.
			 */
			bool Empty() const noexcept override;

			/**
			 * @brief End-of-stream condition.
			 * @return @c true if closed (or in error) and no unread bytes remain.
			 */
			bool EoF() const noexcept override;

			/**
			 * @brief Whether @ref SetError() has been called.
			 * @return @c true in permanent error state.
			 */
			bool HasError() const noexcept;

			/**
			 * @brief Whether the buffer can still be read.
			 * @return @c false in permanent error state.
			 */
			bool IsReadable() const noexcept override;

			/**
			 * @brief Whether the buffer accepts writes.
			 * @return @c false if closed or in error.
			 */
			bool IsWritable() const noexcept override;

			/**
			 * @brief Total number of bytes stored.
			 * @return Size in bytes.
			 */
			StormByte::ByteSize Size() const noexcept override;

			/** @} */

			/**
			 * @name Maintenance / lifecycle
			 * @{
			 */

			/**
			 * @brief Discard data from the start up to the current read position.
			 */
			void Clean() noexcept override;

			/**
			 * @brief Clear all contents and reset the read position.
			 * @details Does not clear closed / error flags.
			 */
			void Clear() noexcept override;

			/**
			 * @brief Close the buffer for further writes.
			 * @details Subsequent writes fail; readers may still drain data.
			 *          Waiters are notified.
			 */
			void Close() noexcept override;

			/**
			 * @brief Discard @p count unread bytes.
			 * @param count Number of bytes to drop.
			 * @return @c true on success, @c false if fewer bytes were available.
			 */
			bool Drop(const StormByte::ByteSize& count) noexcept override;

			/**
			 * @brief Move the logical read position for non-destructive reads.
			 * @param offset Offset value.
			 * @param mode @ref Position::Absolute or @ref Position::Relative.
			 */
			void Seek(const std::ptrdiff_t& offset, const Position& mode) const noexcept override;

			/**
			 * @brief Enter permanent error state (unreadable and unwritable).
			 * @details Waiters are notified.
			 */
			void SetError() noexcept override;

			/** @} */

			/**
			 * @name Diagnostics
			 * @{
			 */

			/**
			 * @brief Hexdump of unread contents from the current read position.
			 * @param columns Bytes per line (0 → default 16).
			 * @param byte_limit Max bytes to include (0 → no limit).
			 * @return Formatted diagnostic string.
			 */
			StormByte::String::String HexDump(const StormByte::ByteSize& columns = 16,
								const StormByte::ByteSize& byte_limit = 0) const noexcept;

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
			 * @name Read (non-destructive, advances position)
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
			 * @name Extract (destructive)
			 * @{
			 */

			/**
			 * @brief Extract bytes into a @ref StormByte::BinaryData.
			 * @param count Bytes to extract; 0 = all available.
			 * @param outBuffer Destination buffer.
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
			static StormByte::String::String FormatHexLines(std::span<const std::byte> data,
											StormByte::ByteSize start_offset,
											StormByte::ByteSize columns) noexcept;

			/**
			 * @brief Build the hexdump header (size / position / status).
			 * @return Stream containing the header lines.
			 */
			virtual std::ostringstream HexDumpHeader() const noexcept;

			/**
			 * @name Internal helpers
			 * @{
			 */

			/**
			 * @brief Shared implementation for Extract / Read / Peek into @ref StormByte::BinaryData.
			 * @param count Requested byte count (0 = all available).
			 * @param outBuffer Destination.
			 * @param flag Operation kind.
			 * @return @c true on success.
			 */
			virtual bool ReadInternal(const StormByte::ByteSize& count, StormByte::BinaryData& outBuffer, Operation flag) noexcept;

			/**
			 * @brief Shared implementation for Extract / Read / Peek into @ref WriteOnly.
			 * @param count Requested byte count (0 = all available).
			 * @param outBuffer Destination writer.
			 * @param flag Operation kind.
			 * @return @c true on success.
			 */
			virtual bool ReadInternal(const StormByte::ByteSize& count, WriteOnly& outBuffer, Operation flag) noexcept;

			/**
			 * @brief Drain until EoF into @ref StormByte::BinaryData using @p flag semantics.
			 * @param outBuffer Destination.
			 * @param flag Extract or Read.
			 */
			virtual void ReadUntilEoFInternal(StormByte::BinaryData& outBuffer, Operation flag) noexcept;

			/**
			 * @brief Drain until EoF into @ref WriteOnly using @p flag semantics.
			 * @param outBuffer Destination.
			 * @param flag Extract or Read.
			 */
			virtual void ReadUntilEoFInternal(WriteOnly& outBuffer, Operation flag) noexcept;

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

			/** @} */

		private:
			std::deque<std::byte> m_buffer;					///< Contiguous-ish byte storage
			mutable StormByte::ByteSize m_position_offset{0};	///< Logical read offset
			bool m_closed{false};							///< Closed-for-writes flag
			bool m_error{false};							///< Permanent error flag
			StormByte::String::String m_error_message;		///< Optional error detail

			mutable StormByte::BinaryData m_data_cache;				///< Cache for @ref ReadOnly::Data()
			mutable std::shared_mutex m_mutex;				///< Shared for readers, exclusive for writers
			mutable std::condition_variable_any m_cv;		///< Signalled on data / close / error

			/**
			 * @brief Block until at least @p n bytes are available, or closed/error.
			 * @param n Requested byte count.
			 * @param lock Unique lock already held on @c m_mutex (released while waiting).
			 */
			void Wait(const StormByte::ByteSize& n, std::unique_lock<std::shared_mutex>& lock) const;
	};
}
