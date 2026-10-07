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

#include <StormByte/buffer/fifo.hxx>
#include <StormByte/safe/condition_variable_any.hxx>
#include <StormByte/safe/mutex.hxx>
#include <StormByte/safe/unique_lock.hxx>
#include <StormByte/type_traits.hxx>

#include <string_view>

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
		 * @class SharedFIFO
		 * @brief Thread-safe FIFO built on top of @ref FIFO.
		 *
		 * SharedFIFO wraps the non-thread-safe @ref FIFO with a mutex and a
		 * condition variable. It preserves the byte-oriented FIFO semantics and
		 * adds blocking reads and extracts.
		 *
		 * Read and Extract with a count greater than zero block until that many
		 * bytes are available, or until @ref Close() or @ref SetError(). A count
		 * of zero returns immediately with whatever is available.
		 *
		 * @ref Close() marks the FIFO closed and notifies waiters. Later Write
		 * calls fail. Waiters complete with whatever is presently available.
		 *
		 * @ref SetError() puts the buffer into a permanent error state and wakes
		 * all waiters. @ref Seek() updates the non-destructive read position and
		 * notifies waiters.
		 *
		 * All public member functions are thread-safe. Closed and error flags are
		 * owned by the base @ref FIFO. SharedFIFO only protects access and notifies.
		 *
		 * Construction from @ref StormByte::Safe::Binary, a byte range, a string
		 * view, a C string or a @ref FIFO is implicit.
		 *
		 * @see FIFO, Ring, Producer, Consumer
		 */
		class STORMBYTE_BUFFER_PUBLIC SharedFIFO: public FIFO {
			public:
				/**
				 * @name Constructors / destructor / assignment
				 * @{
				 */

				/**
				 * @brief Default construct an empty SharedFIFO.
				 */
				SharedFIFO() noexcept;

				/**
				 * @brief Construct with initial data (copy). Implicit.
				 * @param data Initial bytes.
				 */
				SharedFIFO(const StormByte::Safe::Binary& data);

				/**
				 * @brief Construct with initial data (move). Implicit.
				 * @param data Initial bytes. Moved into the base FIFO.
				 */
				SharedFIFO(StormByte::Safe::Binary&& data) noexcept;

				/**
				 * @brief Construct from an input range. Implicit.
				 * @tparam R Range whose value converts to @c std::byte.
				 * @param r Source range. Disabled when it is already @ref StormByte::Safe::Binary.
				 */
				template<StormByte::Type::ByteInputRange R>
				requires (!StormByte::Type::SameAs<R, StormByte::Safe::Binary>)
				STORMBYTE_FORCE_INLINE SharedFIFO(const R& r) noexcept: SharedFIFO(DataConvert(r)) {}

				/**
				 * @brief Construct from an rvalue range. Implicit.
				 * @tparam Rr Range type. Moved when it is @ref StormByte::Safe::Binary.
				 * @param r Source range.
				 */
				template<StormByte::Type::ByteInputRange Rr>
				STORMBYTE_FORCE_INLINE SharedFIFO(Rr&& r) noexcept: SharedFIFO(DataConvert(std::forward<Rr>(r))) {}

				/**
				 * @brief Construct from a string view. Implicit. No trailing NUL.
				 * @param sv Source characters.
				 */
				SharedFIFO(std::string_view sv) noexcept;

				/**
				 * @brief Construct from a C string. Implicit.
				 * @param s Source. Null yields an empty buffer.
				 */
				SharedFIFO(const char* s) noexcept;

				/**
				 * @brief Construct by copying a plain FIFO.
				 * @param other Source FIFO.
				 */
				SharedFIFO(const FIFO& other);

				/**
				 * @brief Construct by moving a plain FIFO.
				 * @param other Source FIFO. Left empty after the move.
				 */
				SharedFIFO(FIFO&& other) noexcept;

				/**
				 * @brief Copy constructor. Deleted: the mutex is not copyable.
				 */
				SharedFIFO(const SharedFIFO&) = delete;

				/**
				 * @brief Move constructor. Deleted: the mutex is not movable.
				 */
				SharedFIFO(SharedFIFO&&) = delete;

				/**
				 * @brief Destroy the SharedFIFO.
				 */
				virtual ~SharedFIFO() noexcept;

				/**
				 * @brief Copy-assign from a plain FIFO.
				 * @param other Source FIFO.
				 * @return This SharedFIFO.
				 */
				SharedFIFO& operator=(const FIFO& other);

				/**
				 * @brief Move-assign from a plain FIFO.
				 * @param other Source FIFO.
				 * @return This SharedFIFO.
				 */
				SharedFIFO& operator=(FIFO&& other) noexcept;

				/**
				 * @brief Copy assignment. Deleted.
				 */
				SharedFIFO& operator=(const SharedFIFO&) = delete;

				/**
				 * @brief Move assignment. Deleted: the mutex is not movable.
				 */
				SharedFIFO& operator=(SharedFIFO&&) = delete;

				/**
				 * @}
				 */

				/**
				 * @name Comparison
				 * @{
				 */

				/**
				 * @brief Compare two SharedFIFOs. Both mutexes are held.
				 * @param other Other SharedFIFO.
				 * @return Whether contents and state match.
				 */
				bool operator==(const SharedFIFO& other) const noexcept;

				/**
				 * @brief Compare two SharedFIFOs.
				 * @param other Other SharedFIFO.
				 * @return Whether the SharedFIFOs differ.
				 */
				inline bool operator!=(const SharedFIFO& other) const noexcept {
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
				virtual StormByte::ByteSize Available() const noexcept override;

				/**
				 * @brief Access the internal storage.
				 * @warning Not safe under concurrent mutation without external exclusion.
				 * @return Owned bytes.
				 */
				virtual const StormByte::Safe::Binary& Data() const noexcept override;

				/**
				 * @brief Whether the underlying storage is empty.
				 * @return Whether storage is empty.
				 * @note With a non-zero read position this may be false while @ref Available() is zero.
				 * @see Size(), Available()
				 */
				virtual bool Empty() const noexcept override;

				/**
				 * @brief End-of-stream condition.
				 * @return Whether the FIFO is closed or in error and nothing unread remains.
				 */
				virtual bool EoF() const noexcept override;

				/**
				 * @brief Whether the buffer is in a permanent error state.
				 * @return Whether @ref SetError() has been called.
				 */
				bool HasError() const noexcept;

				/**
				 * @brief Whether the buffer can still be read.
				 * @return False in a permanent error state.
				 * @see SetError(), IsWritable(), Available(), EoF()
				 */
				virtual bool IsReadable() const noexcept override;

				/**
				 * @brief Whether the buffer accepts writes.
				 * @return False if closed or in error.
				 * @see Close(), SetError(), IsReadable()
				 */
				virtual bool IsWritable() const noexcept override;

				/**
				 * @brief Total number of bytes stored.
				 * @return Size in bytes.
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
				 * @brief Discard data from the start up to the read position.
				 * @see FIFO::Clean()
				 */
				void Clean() noexcept override;

				/**
				 * @brief Clear stored bytes.
				 * @details Does not clear closed or error flags.
				 * @see FIFO::Clear()
				 */
				virtual void Clear() noexcept override;

				/**
				 * @brief Close for further writes and notify waiters.
				 * @see FIFO::Close(), IsWritable()
				 */
				virtual void Close() noexcept override;

				/**
				 * @brief Discard unread bytes and notify waiters.
				 * @param count Bytes to drop.
				 * @return False on failure.
				 * @see FIFO::Drop()
				 */
				virtual bool Drop(const StormByte::ByteSize& count) noexcept override;

				/**
				 * @brief Move the logical read position and notify waiters.
				 * @param offset Offset.
				 * @param mode Absolute or relative.
				 * @details Clamped to [0, Size()].
				 * @see Read(), Position
				 */
				virtual void Seek(const std::ptrdiff_t& offset, const Position& mode) const noexcept override;

				/**
				 * @brief Enter the permanent error state and notify waiters.
				 * @see FIFO::SetError(), IsReadable(), IsWritable()
				 */
				virtual void SetError() noexcept override;

				/**
				 * @}
				 */

				/**
				 * @name Diagnostics
				 * @{
				 */

				/**
				 * @brief Hexdump of unread contents.
				 * @param columns Bytes per line. 0 means the FIFO default.
				 * @param byte_limit Maximum bytes to include. 0 means no limit.
				 * @return Size, position and status, then hex/ASCII lines. No trailing newline.
				 */
				virtual StormByte::Safe::String HexDump(const StormByte::ByteSize& columns = 0,
					const StormByte::ByteSize& byte_limit = 0) const noexcept override;

				/**
				 * @}
				 */

			private:
				mutable StormByte::Safe::Mutex m_mutex;						///< Mutex protecting internal state.
				mutable StormByte::Safe::ConditionVariableAny m_cv;		///< Condition variable for blocking reads.

				/**
				 * @brief Hexdump header.
				 * @return Owned header text. Base semantics are preserved.
				 */
				StormByte::Safe::String HexDumpHeader() const noexcept override;

				/**
				 * @brief Blocking Extract, Read or Peek into owned storage.
				 * @param count Requested bytes.
				 * @param outBuffer Destination.
				 * @param flag Operation kind.
				 * @return False on error, or when closed with insufficient data.
				 */
				virtual bool ReadInternal(const StormByte::ByteSize& count, StormByte::Safe::Binary& outBuffer,
					const Operation& flag) noexcept override;

				/**
				 * @brief Blocking Extract, Read or Peek into a writer.
				 * @param count Requested bytes.
				 * @param outBuffer Destination writer.
				 * @param flag Operation kind.
				 * @return False on error, or when closed with insufficient data.
				 */
				virtual bool ReadInternal(const StormByte::ByteSize& count, WriteOnly& outBuffer,
					const Operation& flag) noexcept override;

				/**
				 * @brief Wait until at least @p n bytes are available, or closed/error.
				 * @param n Requested byte count. Zero returns immediately.
				 * @param lock Caller-held lock on @c m_mutex. Still held on return.
				 * @note Returns when @ref Close() or @ref SetError() is called even if fewer than @p n bytes are available.
				 * @see Close(), SetError(), IsReadable()
				 */
				void Wait(const StormByte::ByteSize& n, StormByte::Safe::UniqueLock& lock) const;

				/**
				 * @brief Append from owned storage (copy), under lock, then notify.
				 * @param count Bytes to write.
				 * @param src Source.
				 * @return False if closed or in error.
				 */
				virtual bool WriteInternal(const StormByte::ByteSize& count, const StormByte::Safe::Binary& src) noexcept override;

				/**
				 * @brief Append from owned storage (move), under lock, then notify.
				 * @param count Bytes to write.
				 * @param src Source.
				 * @return False if closed or in error.
				 */
				virtual bool WriteInternal(const StormByte::ByteSize& count, StormByte::Safe::Binary&& src) noexcept override;
		};
	}
}

STORMBYTE_DECLARE_MAYBE_SAFE(StormByte::Buffer::SharedFIFO);
