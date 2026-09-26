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

#include <atomic>
#include <condition_variable>
#include <cstdint>
#include <mutex>
#include <span>
#include <vector>

/**
 * @namespace StormByte::Buffer
 * @brief Buffer module of the StormByte suite.
 */
namespace StormByte::Buffer {
	/**
	 * @class LockFreeRing
	 * @brief High-performance lock-free SPSC ring buffer (private / internal).
	 *
	 * Designed exclusively for intermediate @ref Pipeline stages.
	 * Correct behaviour is guaranteed only under Single-Producer /
	 * Single-Consumer access (the pattern used by the current Async Pipeline).
	 *
	 * - Lock-free data path (atomics + power-of-two circular buffer)
	 * - Grows automatically (doubles capacity) when full
	 * - Minimal overhead, good cache locality
	 * - Same @ref ReadWrite contract as @ref Ring for drop-in use inside Pipeline
	 *
	 * Blocking waits (when data is not yet available) use a mutex + condition
	 * variable; the data path itself stays lock-free.
	 *
	 * @warning Never share a LockFreeRing instance between multiple producers
	 *          or multiple consumers. Doing so is undefined behaviour.
	 *
	 * @note This class is marked @c STORMBYTE_BUFFER_PRIVATE and is not installed
	 *       as a public header.
	 *
	 * @see Pipeline, Ring, ReadWrite
	 */
	class STORMBYTE_BUFFER_PRIVATE LockFreeRing final : public ReadWrite {
		public:
			/**
			 * @name Constructors / destructor / assignment
			 * @{
			 */

			/**
			 * @brief Construct with an initial capacity (rounded up to power of two).
			 * @param initial_capacity Suggested starting size in bytes (default 1 MiB).
			 */
			explicit LockFreeRing(StormByte::ByteSize initial_capacity = StormByte::ByteSize{1u << 20});

			/**
			 * @brief Copy constructor (deleted).
			 */
			LockFreeRing(const LockFreeRing&) = delete;

			/**
			 * @brief Copy assignment (deleted).
			 */
			LockFreeRing& operator=(const LockFreeRing&) = delete;

			/**
			 * @brief Move constructor.
			 * @param other Source instance.
			 */
			LockFreeRing(LockFreeRing&& other) noexcept;

			/**
			 * @brief Move assignment.
			 * @param other Source instance.
			 * @return Reference to this instance.
			 */
			LockFreeRing& operator=(LockFreeRing&& other) noexcept;

			/**
			 * @brief Destructor.
			 */
			~LockFreeRing() noexcept override = default;

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
			 * @brief Whether the ring holds no unread data.
			 * @return @c true if empty.
			 */
			bool Empty() const noexcept override;

			/**
			 * @brief End-of-stream condition.
			 * @return @c true if closed (or in error) and no bytes remain.
			 */
			bool EoF() const noexcept override;

			/**
			 * @brief Whether @ref SetError() has been called.
			 * @return @c true if the buffer is in a permanent error state.
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

			/**
			 * @brief Snapshot of stored data (may rebuild an internal cache).
			 * @return Constant reference to a @ref StormByte::BinaryData view.
			 * @warning Intended for diagnostics; prefer Read / Extract on the hot path.
			 */
			const StormByte::BinaryData& BinaryData() const noexcept override;

			/**
			 * @brief Longest contiguous unread span from the read cursor.
			 * @return Empty if none. Does not wrap; call again after Consume.
			 * @details Returns a view of an internal snapshot taken under the wait
			 *          mutex so a concurrent @ref Grow cannot invalidate the pointer
			 *          while the consumer is still draining. Valid until the next
			 *          @ref FrontSpan call on this instance.
			 */
			std::span<const std::byte> FrontSpan() const noexcept;

			/** @} */

			/**
			 * @name Maintenance / lifecycle
			 * @{
			 */

			/**
			 * @brief Discard already-consumed data up to the logical read position.
			 */
			void Clean() noexcept override;

			/**
			 * @brief Clear all contents and reset positions.
			 * @details Does not clear closed / error flags.
			 */
			void Clear() noexcept override;

			/**
			 * @brief Close the buffer for further writes (SPSC-safe).
			 * @details Sets the closed flag and wakes any @c WaitFor() waiters.
			 *          Remaining bytes can still be read until @ref EoF().
			 */
			void Close() noexcept override;

			/**
			 * @brief Enter a permanent error state.
			 * @details Makes the buffer unreadable and unwritable and wakes waiters.
			 */
			void SetError() noexcept override;

			/**
			 * @brief Discard @p count unread bytes.
			 * @param count Number of bytes to drop.
			 * @return @c true on success, @c false if fewer bytes were available.
			 */
			bool Drop(const StormByte::ByteSize& count) noexcept override;

			/**
			 * @brief Advance the read cursor by @p n bytes.
			 * @param n Bytes to drop from the front. 0 is success.
			 * @return @c false if @p n exceeds @ref Available.
			 */
			bool Consume(StormByte::ByteSize n) noexcept;

			/**
			 * @brief Move the logical read position for non-destructive reads.
			 * @param offset Offset value.
			 * @param mode @ref Position::Absolute or @ref Position::Relative.
			 */
			void Seek(const std::ptrdiff_t& offset, const Position& mode) const noexcept override;

			/** @} */

			/**
			 * @name Peek (non-destructive, does not advance)
			 * @{
			 */

			/**
			 * @brief Peek into a @ref StormByte::BinaryData without advancing the read position.
			 * @param count Bytes to peek; 0 = all available.
			 * @param out Destination.
			 * @return @c true on success, @c false on insufficient data or error.
			 */
			bool Peek(const StormByte::ByteSize& count, StormByte::BinaryData& out) const noexcept override;

			/**
			 * @brief Peek into a @ref WriteOnly without advancing the read position.
			 * @param count Bytes to peek; 0 = all available.
			 * @param out Destination writer.
			 * @return @c true on success, @c false on insufficient data or error.
			 */
			bool Peek(const StormByte::ByteSize& count, WriteOnly& out) const noexcept override;

			/** @} */

			/**
			 * @name Read (non-destructive, advances position)
			 * @{
			 */

			/**
			 * @brief Non-destructive read into a @ref StormByte::BinaryData.
			 * @param count Bytes to read; 0 = all available.
			 * @param out Destination.
			 * @return @c true on success, @c false on insufficient data or error.
			 */
			bool Read(const StormByte::ByteSize& count, StormByte::BinaryData& out) const noexcept override;

			/**
			 * @brief Non-destructive read into a @ref WriteOnly.
			 * @param count Bytes to read; 0 = all available.
			 * @param out Destination writer.
			 * @return @c true on success, @c false on insufficient data or error.
			 */
			bool Read(const StormByte::ByteSize& count, WriteOnly& out) const noexcept override;

			/**
			 * @brief Read until EoF into a @ref StormByte::BinaryData.
			 * @param out Destination.
			 */
			void ReadUntilEoF(StormByte::BinaryData& out) const noexcept override;

			/**
			 * @brief Read until EoF into a @ref WriteOnly.
			 * @param out Destination writer.
			 */
			void ReadUntilEoF(WriteOnly& out) const noexcept override;

			/** @} */

			/**
			 * @name Extract (destructive)
			 * @{
			 */

			/**
			 * @brief Extract bytes into a @ref StormByte::BinaryData (consumes data).
			 * @param count Bytes to extract; 0 = all available.
			 * @param out Destination.
			 * @return @c true on success, @c false on insufficient data or error.
			 */
			bool Extract(const StormByte::ByteSize& count, StormByte::BinaryData& out) noexcept override;

			/**
			 * @brief Extract bytes into a @ref WriteOnly (consumes data).
			 * @param count Bytes to extract; 0 = all available.
			 * @param out Destination writer.
			 * @return @c true on success, @c false on insufficient data or error.
			 */
			bool Extract(const StormByte::ByteSize& count, WriteOnly& out) noexcept override;

			/**
			 * @brief Extract until EoF into a @ref StormByte::BinaryData.
			 * @param out Destination.
			 */
			void ExtractUntilEoF(StormByte::BinaryData& out) noexcept override;

			/**
			 * @brief Extract until EoF into a @ref WriteOnly.
			 * @param out Destination writer.
			 */
			void ExtractUntilEoF(WriteOnly& out) noexcept override;

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
			 * @brief Append bytes from a @ref StormByte::BinaryData (move path).
			 * @param count Number of bytes to write.
			 * @param data Source.
			 * @return @c true on success, @c false if closed / error.
			 */
			bool Write(const StormByte::ByteSize& count, StormByte::BinaryData&& data) noexcept override;

			/**
			 * @brief Append bytes from a @ref ReadOnly (copy path).
			 * @param count Number of bytes to write.
			 * @param data Source buffer.
			 * @return @c true on success, @c false if closed / error.
			 */
			bool Write(const StormByte::ByteSize& count, const ReadOnly& data) noexcept override;

			/**
			 * @brief Append bytes from a @ref ReadOnly (move / extract path).
			 * @param count Number of bytes to write.
			 * @param data Source buffer.
			 * @return @c true on success, @c false if closed / error.
			 */
			bool Write(const StormByte::ByteSize& count, ReadOnly&& data) noexcept override;

			/**
			 * @brief Append a span (copy into the ring).
			 * @param src Octets to store.
			 * @return @c false if closed or in error.
			 */
			bool Write(std::span<const std::byte> src) noexcept;

			/**
			 * @brief Bring @ref WriteOnly convenience Write overloads into scope.
			 */
			using WriteOnly::Write;

			/** @} */

		private:
			/**
			 * @brief Kind of internal read operation.
			 */
			enum class Operation {
				Extract,	///< Destructive read
				Read,		///< Non-destructive read (advances logical position)
				Peek		///< Non-destructive peek (no advance)
			};

			std::vector<std::byte> m_storage;				///< Power-of-two circular storage
			std::size_t m_capacity = 0;						///< Current capacity (power of two)
			std::size_t m_mask = 0;							///< @c m_capacity - 1 for fast modulo

			alignas(64) std::atomic<std::size_t> m_head{0};	///< Consumer index
			alignas(64) std::atomic<std::size_t> m_tail{0};	///< Producer index

			mutable std::atomic<std::size_t> m_logical{0};	///< Logical cursor for Read/Peek

			std::atomic<bool> m_closed{false};				///< Closed-for-writes flag
			std::atomic<bool> m_error{false};				///< Permanent error flag

			mutable std::mutex m_wait_mtx;					///< Mutex for blocking waits and Grow/FrontSpan
			mutable std::condition_variable m_cv;			///< Signalled on data / close / error

			mutable StormByte::BinaryData m_data_cache;				///< Cache used by @ref ReadOnly::Data()
			mutable std::vector<std::byte> m_front_cache;	///< Snapshot backing @ref FrontSpan

			/**
			 * @brief Round @p v up to the next power of two.
			 * @param v Requested size.
			 * @return Power-of-two size (at least 1).
			 */
			static std::size_t RoundUpPow2(std::size_t v) noexcept;

			/**
			 * @brief Double capacity (producer side only; caller holds @c m_wait_mtx).
			 */
			void Grow() noexcept;

			/**
			 * @brief Block until at least @p n bytes are available, or closed/error.
			 * @param n Requested byte count.
			 * @return @c false if closed or in error before @p n bytes are ready.
			 */
			bool WaitFor(StormByte::ByteSize n) const;

			/**
			 * @brief Shared Extract / Read / Peek implementation into @ref StormByte::BinaryData.
			 * @param count Requested bytes.
			 * @param out Destination.
			 * @param op Operation kind.
			 * @return @c true on success.
			 */
			bool ReadInternal(StormByte::ByteSize count, StormByte::BinaryData& out, Operation op) noexcept;

			/**
			 * @brief Append raw bytes (producer path).
			 * @param count Number of bytes.
			 * @param src Source pointer.
			 * @return @c true on success, @c false if closed / error.
			 */
			bool WriteInternal(StormByte::ByteSize count, const std::byte* src) noexcept;
	};
}
