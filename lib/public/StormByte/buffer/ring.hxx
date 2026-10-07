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
#include <StormByte/safe/condition_variable_any.hxx>
#include <StormByte/safe/deque.hxx>
#include <StormByte/safe/exclusive_lock.hxx>
#include <StormByte/safe/shared_mutex.hxx>
#include <StormByte/safe/string.hxx>

#include <cstddef>
#include <span>
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
		 * @class Ring
		 * @brief Highly concurrent thread-safe ring buffer.
		 *
		 * Backed by @ref StormByte::Safe::Deque of @c std::byte with a logical read position.
		 * Uses @ref StormByte::Safe::SharedMutex so several readers can proceed while writers
		 * and destructive operations stay exclusive. Blocked readers wait on a
		 * @ref StormByte::Safe::ConditionVariableAny.
		 *
		 * Fully thread-safe for concurrent producers and consumers. Pipeline
		 * intermediates use a private SPSC ring; this type is the public
		 * many-to-many ring.
		 *
		 * @ref Close() stops further writes; remaining data can still be drained.
		 * @ref SetError() makes the buffer permanently unreadable and unwritable
		 * and wakes waiters.
		 *
		 * Construction from bytes, a range, a string view or a C string is explicit.
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
				 * @brief Construct an empty Ring. Synchronization is built in the provider module.
				 */
				Ring() noexcept;

				/**
				 * @brief Copy initial data into deque storage.
				 * @param data Source bytes.
				 */
				explicit Ring(const StormByte::Safe::Binary& data) noexcept;

				/**
				 * @brief Move initial data into deque storage.
				 * @param data Source bytes. Moved element-wise into the deque.
				 */
				explicit Ring(StormByte::Safe::Binary&& data) noexcept;

				/**
				 * @brief Convert an input range and delegate construction to the provider.
				 * @tparam R Range whose value converts to @c std::byte.
				 * @param r Source range. Disabled when it is already @ref StormByte::Safe::Binary.
				 */
				template<Type::ByteInputRange R>
				requires (!Type::SameAs<R, StormByte::Safe::Binary>)
				inline explicit Ring(const R& r) noexcept
					: Ring(DataConvert(r)) {}

				/**
				 * @brief Convert an rvalue range and delegate construction to the provider.
				 * @tparam Rr Range type.
				 * @param r Source range.
				 */
				template<Type::ByteInputRange Rr>
				inline explicit Ring(Rr&& r) noexcept
					: Ring(DataConvert(std::forward<Rr>(r))) {}

				/**
				 * @brief Construct from a string view. No trailing NUL.
				 * @param sv Source characters.
				 */
				explicit Ring(std::string_view sv) noexcept;

				/**
				 * @brief Construct from a C string.
				 * @param s Source. Null yields an empty buffer.
				 */
				inline explicit Ring(const char* s) noexcept
					: Ring(s ? std::string_view(s) : std::string_view{}) {}

				/**
				 * @brief Copy constructor. Deleted: the mutex is not copyable.
				 */
				Ring(const Ring&) = delete;

				/**
				 * @brief Take a Ring.
				 * @param other Source.
				 */
				Ring(Ring&& other) noexcept;

				/**
				 * @brief Destroy the Ring.
				 */
				virtual ~Ring() noexcept;

				/**
				 * @brief Copy assignment. Deleted.
				 */
				Ring& operator=(const Ring&) = delete;

				/**
				 * @brief Move-assign a Ring.
				 * @param other Source.
				 * @return This Ring.
				 */
				Ring& operator=(Ring&& other) noexcept;

				/**
				 * @}
				 */

				/**
				 * @name Comparison
				 * @{
				 */

				/**
				 * @brief Compare contents and lifecycle state.
				 * @param other Other Ring.
				 * @return Whether the rings match.
				 */
				bool operator==(const Ring& other) const noexcept;

				/**
				 * @brief Compare two Rings.
				 * @param other Other Ring.
				 * @return Whether the rings differ.
				 */
				inline bool operator!=(const Ring& other) const noexcept {
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
				 */
				StormByte::ByteSize Available() const noexcept override;

				/**
				 * @brief Snapshot of stored data. May rebuild an internal cache.
				 * @return Owned snapshot.
				 */
				const StormByte::Safe::Binary& Data() const noexcept override;

				/**
				 * @brief Whether the underlying storage is empty.
				 * @return Whether storage is empty.
				 */
				bool Empty() const noexcept override;

				/**
				 * @brief End-of-stream condition.
				 * @return Whether the ring is closed or in error and nothing unread remains.
				 */
				bool EoF() const noexcept override;

				/**
				 * @brief Whether @ref SetError() has been called.
				 * @return Whether the buffer is in a permanent error state.
				 */
				bool HasError() const noexcept;

				/**
				 * @brief Whether the buffer can still be read.
				 * @return False in a permanent error state.
				 */
				bool IsReadable() const noexcept override;

				/**
				 * @brief Whether the buffer accepts writes.
				 * @return False if closed or in error.
				 */
				bool IsWritable() const noexcept override;

				/**
				 * @brief Total number of bytes stored.
				 * @return Size in bytes.
				 */
				StormByte::ByteSize Size() const noexcept override;

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
				void Clean() noexcept override;

				/**
				 * @brief Clear stored bytes and reset the read position.
				 * @details Does not clear closed or error flags.
				 */
				void Clear() noexcept override;

				/**
				 * @brief Close the buffer for further writes and notify waiters.
				 * @details Later writes fail. Readers may still drain.
				 */
				void Close() noexcept override;

				/**
				 * @brief Discard unread bytes.
				 * @param count Bytes to drop.
				 * @return False if fewer bytes were available.
				 */
				bool Drop(const StormByte::ByteSize& count) noexcept override;

				/**
				 * @brief Move the logical read position.
				 * @param offset Offset.
				 * @param mode Absolute or relative.
				 */
				void Seek(const std::ptrdiff_t& offset, const Position& mode) const noexcept override;

				/**
				 * @brief Enter the permanent error state and notify waiters.
				 */
				void SetError() noexcept override;

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
				 * @return Formatted diagnostic text.
				 */
				StormByte::Safe::String HexDump(const StormByte::ByteSize& columns = 16,
					const StormByte::ByteSize& byte_limit = 0) const noexcept;

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
				 * @name Read (non-destructive, advances position)
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
				static StormByte::Safe::String FormatHexLines(std::span<const std::byte> data,
					StormByte::ByteSize start_offset,
					StormByte::ByteSize columns) noexcept;

				/**
				 * @brief Build the hexdump header.
				 * @return Owned header text. Any formatting stream stays in this module.
				 */
				virtual StormByte::Safe::String HexDumpHeader() const noexcept;

				/**
				 * @name Internal helpers
				 * @{
				 */

				/**
				 * @brief Shared Extract, Read and Peek into owned storage.
				 * @param count Requested byte count. 0 means all available.
				 * @param outBuffer Destination.
				 * @param flag Operation kind.
				 * @return False on failure.
				 */
				virtual bool ReadInternal(const StormByte::ByteSize& count, StormByte::Safe::Binary& outBuffer, Operation flag) noexcept;

				/**
				 * @brief Shared Extract, Read and Peek into a writer.
				 * @param count Requested byte count. 0 means all available.
				 * @param outBuffer Destination writer.
				 * @param flag Operation kind.
				 * @return False on failure.
				 */
				virtual bool ReadInternal(const StormByte::ByteSize& count, WriteOnly& outBuffer, Operation flag) noexcept;

				/**
				 * @brief Drain until EoF into owned storage.
				 * @param outBuffer Destination.
				 * @param flag Extract or Read.
				 */
				virtual void ReadUntilEoFInternal(StormByte::Safe::Binary& outBuffer, Operation flag) noexcept;

				/**
				 * @brief Drain until EoF into a writer.
				 * @param outBuffer Destination.
				 * @param flag Extract or Read.
				 */
				virtual void ReadUntilEoFInternal(WriteOnly& outBuffer, Operation flag) noexcept;

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
				 * @}
				 */

			private:
				StormByte::Safe::Deque<std::byte> m_buffer;			///< Byte storage. Not a std::deque.
				mutable StormByte::ByteSize m_position_offset{0};	///< Logical read offset.
				bool m_closed{false};								///< Closed-for-writes flag.
				bool m_error{false};								///< Permanent error flag.
				StormByte::Safe::String m_error_message;			///< Optional error detail.

				mutable StormByte::Safe::Binary m_data_cache;		///< Cache for Data().
				mutable StormByte::Safe::SharedMutex m_mutex;		///< Shared for readers, exclusive for writers.
				mutable StormByte::Safe::ConditionVariableAny m_cv;	///< Signalled on data, close or error.

				/**
				 * @brief Block until at least @p n bytes are available, or closed/error.
				 * @param n Requested byte count.
				 * @param lock Exclusive lock already held on @c m_mutex. Released while waiting.
				 */
				void Wait(const StormByte::ByteSize& n, StormByte::Safe::ExclusiveLock& lock) const;
		};
	}
}

STORMBYTE_DECLARE_MAYBE_SAFE(StormByte::Buffer::Ring);
