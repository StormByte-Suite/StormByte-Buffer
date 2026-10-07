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
#include <StormByte/safe/atomic.hxx>
#include <StormByte/safe/condition_variable.hxx>
#include <StormByte/safe/mutex.hxx>
#include <StormByte/safe/vector.hxx>

#include <cstdint>
#include <span>

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
		 * @class LockFreeRing
		 * @brief High-performance lock-free SPSC ring buffer. Private.
		 *
		 * Designed for intermediate @ref Pipeline stages. Correct behaviour is
		 * guaranteed only under single-producer / single-consumer access.
		 *
		 * The data path is lock-free: Base-owned words and a power-of-two circular buffer.
		 * It grows by doubling when full. Blocking waits use a Safe mutex and a
		 * Safe condition variable.
		 *
		 * Sharing one instance between several producers or several consumers is
		 * undefined. This class is @c STORMBYTE_BUFFER_PRIVATE and is not installed.
		 *
		 * @see Pipeline, Ring, ReadWrite
		 */
		class STORMBYTE_BUFFER_PRIVATE LockFreeRing final: public ReadWrite {
			public:
				/**
				 * @name Constructors / destructor / assignment
				 * @{
				 */

				/**
				 * @brief Construct with an initial capacity, rounded up to a power of two.
				 * @param initial_capacity Suggested starting size. Default is 1 MiB.
				 */
				explicit LockFreeRing(StormByte::ByteSize initial_capacity = StormByte::ByteSize{1u << 20});

				/**
				 * @brief Copy constructor. Deleted.
				 */
				LockFreeRing(const LockFreeRing&) = delete;

				/**
				 * @brief Take a ring.
				 * @param other Source.
				 */
				LockFreeRing(LockFreeRing&& other) noexcept;

				/**
				 * @brief Destroy the ring.
				 */
				~LockFreeRing() noexcept override = default;

				/**
				 * @brief Copy assignment. Deleted.
				 */
				LockFreeRing& operator=(const LockFreeRing&) = delete;

				/**
				 * @brief Move-assign a ring.
				 * @param other Source.
				 * @return This ring.
				 */
				LockFreeRing& operator=(LockFreeRing&& other) noexcept;

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
				 * @brief Whether the ring holds no unread data.
				 * @return Whether the unread range is empty.
				 */
				bool Empty() const noexcept override;

				/**
				 * @brief End-of-stream condition.
				 * @return Whether the ring is closed or in error and nothing remains.
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
				 * @brief Snapshot of stored data. May rebuild an internal cache.
				 * @return Owned snapshot.
				 * @warning For diagnostics. Prefer Read or Extract on the hot path.
				 */
				const StormByte::Safe::Binary& Data() const noexcept override;

				/**
				 * @brief Longest contiguous unread span from the read cursor.
				 * @return Empty if none. Does not wrap; call again after Consume.
				 * @details The view is a snapshot taken under the wait mutex, so a
				 * concurrent Grow cannot invalidate the pointer while the consumer
				 * is still draining. Valid until the next FrontSpan call.
				 */
				std::span<const std::byte> FrontSpan() const noexcept;

				/**
				 * @}
				 */

				/**
				 * @name Maintenance / lifecycle
				 * @{
				 */

				/**
				 * @brief Discard already-consumed data up to the logical read position.
				 */
				void Clean() noexcept override;

				/**
				 * @brief Clear stored bytes and reset positions.
				 * @details Does not clear closed or error flags.
				 */
				void Clear() noexcept override;

				/**
				 * @brief Close the buffer for further writes and wake waiters.
				 * @details Remaining bytes can still be read until @ref EoF().
				 */
				void Close() noexcept override;

				/**
				 * @brief Enter a permanent error state and wake waiters.
				 */
				void SetError() noexcept override;

				/**
				 * @brief Discard unread bytes.
				 * @param count Bytes to drop.
				 * @return False if fewer bytes were available.
				 */
				bool Drop(const StormByte::ByteSize& count) noexcept override;

				/**
				 * @brief Advance the read cursor.
				 * @param n Bytes to drop from the front. Zero is success.
				 * @return False if @p n exceeds @ref Available.
				 */
				bool Consume(StormByte::ByteSize n) noexcept;

				/**
				 * @brief Move the logical read position.
				 * @param offset Offset.
				 * @param mode Absolute or relative.
				 */
				void Seek(const std::ptrdiff_t& offset, const Position& mode) const noexcept override;

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
				 * @param out Destination.
				 * @return False on insufficient data or error.
				 */
				bool Peek(const StormByte::ByteSize& count, StormByte::Safe::Binary& out) const noexcept override;

				/**
				 * @brief Peek into a writer without advancing the position.
				 * @param count Bytes to peek. 0 means all available.
				 * @param out Destination writer.
				 * @return False on insufficient data or error.
				 */
				bool Peek(const StormByte::ByteSize& count, WriteOnly& out) const noexcept override;

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
				 * @param out Destination.
				 * @return False on insufficient data or error.
				 */
				bool Read(const StormByte::ByteSize& count, StormByte::Safe::Binary& out) const noexcept override;

				/**
				 * @brief Read into a writer and advance the position.
				 * @param count Bytes to read. 0 means all available.
				 * @param out Destination writer.
				 * @return False on insufficient data or error.
				 */
				bool Read(const StormByte::ByteSize& count, WriteOnly& out) const noexcept override;

				/**
				 * @brief Read until EoF into owned storage.
				 * @param out Destination.
				 */
				void ReadUntilEoF(StormByte::Safe::Binary& out) const noexcept override;

				/**
				 * @brief Read until EoF into a writer.
				 * @param out Destination writer.
				 */
				void ReadUntilEoF(WriteOnly& out) const noexcept override;

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
				 * @param out Destination.
				 * @return False on insufficient data or error.
				 */
				bool Extract(const StormByte::ByteSize& count, StormByte::Safe::Binary& out) noexcept override;

				/**
				 * @brief Extract bytes into a writer.
				 * @param count Bytes to extract. 0 means all available.
				 * @param out Destination writer.
				 * @return False on insufficient data or error.
				 */
				bool Extract(const StormByte::ByteSize& count, WriteOnly& out) noexcept override;

				/**
				 * @brief Extract until EoF into owned storage.
				 * @param out Destination.
				 */
				void ExtractUntilEoF(StormByte::Safe::Binary& out) noexcept override;

				/**
				 * @brief Extract until EoF into a writer.
				 * @param out Destination writer.
				 */
				void ExtractUntilEoF(WriteOnly& out) noexcept override;

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
				 * @brief Append bytes from owned storage (move path).
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
				 * @brief Append a span.
				 * @param src Octets to store.
				 * @return False if closed or in error.
				 */
				bool Write(std::span<const std::byte> src) noexcept;

				/**
				 * @brief Bring the WriteOnly convenience Write overloads into scope.
				 */
				using WriteOnly::Write;

				/**
				 * @}
				 */

			private:
				/**
				 * @brief Kind of internal read operation.
				 */
				enum class Operation {
					Extract,	///< Destructive read.
					Read,		///< Non-destructive read. Advances the logical position.
					Peek		///< Non-destructive peek. Does not advance.
				};

				StormByte::Safe::Vector<std::byte> m_storage;	///< Power-of-two circular storage on Base's heap.
				std::size_t m_capacity = 0;						///< Current capacity, a power of two.
				std::size_t m_mask = 0;							///< m_capacity - 1, used as a fast modulo.

				StormByte::Safe::Atomic<std::size_t> m_head;	///< Consumer index. The word is on Base's heap.
				StormByte::Safe::Atomic<std::size_t> m_tail;	///< Producer index. The word is on Base's heap.

				mutable StormByte::Safe::Atomic<std::size_t> m_logical;	///< Logical cursor for Read and Peek.

				StormByte::Safe::Atomic<bool> m_closed;			///< Closed-for-writes flag.
				StormByte::Safe::Atomic<bool> m_error;			///< Permanent error flag.

				mutable StormByte::Safe::Mutex m_wait_mtx;		///< Mutex for blocking waits, Grow and FrontSpan.
				mutable StormByte::Safe::ConditionVariable m_cv;	///< Signalled on data, close or error.

				mutable StormByte::Safe::Binary m_data_cache;	///< Cache used by Data().
				mutable StormByte::Safe::Vector<std::byte> m_front_cache;	///< Snapshot backing FrontSpan.

				/**
				 * @brief Round a size up to the next power of two.
				 * @param v Requested size.
				 * @return Power-of-two size, at least 1.
				 */
				static std::size_t RoundUpPow2(std::size_t v) noexcept;

				/**
				 * @brief Double capacity. Producer side only. Caller holds @c m_wait_mtx.
				 */
				void Grow() noexcept;

				/**
				 * @brief Block until at least @p n bytes are available, or closed/error.
				 * @param n Requested byte count.
				 * @return False if closed or in error before @p n bytes are ready.
				 */
				bool WaitFor(StormByte::ByteSize n) const;

				/**
				 * @brief Shared Extract, Read and Peek into owned storage.
				 * @param count Requested bytes.
				 * @param out Destination.
				 * @param op Operation kind.
				 * @return False on failure.
				 */
				bool ReadInternal(StormByte::ByteSize count, StormByte::Safe::Binary& out, Operation op) noexcept;

				/**
				 * @brief Append raw bytes on the producer path.
				 * @param count Bytes to write.
				 * @param src Source pointer.
				 * @return False if closed or in error.
				 */
				bool WriteInternal(StormByte::ByteSize count, const std::byte* src) noexcept;
		};
	}
}

STORMBYTE_DECLARE_MAYBE_SAFE(StormByte::Buffer::LockFreeRing);
