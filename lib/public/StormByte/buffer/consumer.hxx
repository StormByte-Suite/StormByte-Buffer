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

#include <StormByte/buffer/ring.hxx>
#include <StormByte/safe/owner.hxx>
#include <StormByte/type_traits.hxx>

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
		class Producer;

		/**
		 * @class Consumer
		 * @brief Read-oriented handle over a shared @ref Ring.
		 *
		 * Several Consumer instances may share the same Ring. An empty Consumer
		 * creates its own Ring. @ref Producer() then returns a writer on that Ring.
		 * A Consumer may also come from @ref Producer::Consumer().
		 *
		 * All operations are thread-safe and delegate to the shared Ring. Ring
		 * ownership is held by @ref StormByte::Safe::Owner. Blocking semantics match
		 * @ref Ring.
		 *
		 * Consumer is a @ref ReadOnly view and also exposes @ref Close(),
		 * @ref IsWritable() and @ref HasError() so a reader can observe the shared
		 * lifecycle.
		 *
		 * @see Producer, Ring, ReadOnly
		 */
		class STORMBYTE_BUFFER_PUBLIC Consumer final: public ReadOnly {
			public:
				/**
				 * @name Constructors / destructor / assignment
				 * @{
				 */

				/**
				 * @brief Create a Consumer and a new shared Ring.
				 * @throws StormByte::Safe::AllocationError Ring ownership cannot be allocated.
				 */
				Consumer();

				/**
				 * @brief Copy a Consumer. Both share the same Ring.
				 * @param other Source.
				 * @throws StormByte::Exception The shared owner cannot be retained.
				 */
				Consumer(const Consumer& other);

				/**
				 * @brief Take a Consumer. @p other is left valid and unspecified.
				 * @param other Source.
				 */
				Consumer(Consumer&& other) noexcept;

				/**
				 * @brief Destroy the Consumer.
				 */
				~Consumer() noexcept override;

				/**
				 * @brief Copy-assign a Consumer. Both share the same Ring afterwards.
				 * @param other Source.
				 * @return This Consumer.
				 * @throws StormByte::Exception The shared owner cannot be retained.
				 */
				Consumer& operator=(const Consumer& other);

				/**
				 * @brief Move-assign a Consumer.
				 * @param other Source.
				 * @return This Consumer.
				 */
				Consumer& operator=(Consumer&& other) noexcept;

				/**
				 * @}
				 */

				/**
				 * @name Comparison
				 * @{
				 */

				/**
				 * @brief Compare two Consumers.
				 * @param other Other Consumer.
				 * @return Whether both refer to the same Ring.
				 */
				bool operator==(const Consumer& other) const noexcept;

				/**
				 * @brief Compare two Consumers.
				 * @param other Other Consumer.
				 * @return Whether the underlying Rings differ.
				 */
				inline bool operator!=(const Consumer& other) const noexcept {
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
				 * @brief Bytes available from the current position.
				 * @return Available byte count.
				 */
				StormByte::ByteSize Available() const noexcept override;

				/**
				 * @brief Snapshot of the underlying data.
				 * @return Owned snapshot.
				 * @warning Prefer Read or Extract under concurrent mutation.
				 */
				const StormByte::Safe::Binary& Data() const noexcept override;

				/**
				 * @brief Whether the shared Ring holds no stored bytes.
				 * @return Whether storage is empty.
				 * @note With a non-zero read position this may be false while @ref Available() is zero.
				 */
				bool Empty() const noexcept override;

				/**
				 * @brief End-of-stream condition.
				 * @return Whether the Ring is closed or in error and nothing remains.
				 */
				bool EoF() const noexcept override;

				/**
				 * @brief Whether the shared Ring can still be read.
				 * @return False in a permanent error state.
				 */
				bool IsReadable() const noexcept override;

				/**
				 * @brief Whether the shared Ring still accepts writes.
				 * @return False if closed or in error.
				 */
				inline bool IsWritable() const noexcept {
					return Storage().IsWritable();
				}

				/**
				 * @brief Whether the shared Ring is in a permanent error state.
				 * @return Whether any handle called SetError on this Ring.
				 */
				inline bool HasError() const noexcept {
					return Storage().HasError();
				}

				/**
				 * @brief Total number of bytes stored in the shared Ring.
				 * @return Size in bytes.
				 */
				StormByte::ByteSize Size() const noexcept override;

				/**
				 * @brief Writer on the same Ring.
				 * @return Producer that shares this store.
				 */
				class Producer Producer() const;

				/**
				 * @}
				 */

				/**
				 * @name Maintenance / lifecycle
				 * @{
				 */

				/**
				 * @brief Discard already-consumed data up to the read position.
				 */
				void Clean() noexcept override;

				/**
				 * @brief Clear stored bytes.
				 * @details Does not clear closed or error flags on the shared Ring.
				 */
				void Clear() noexcept override;

				/**
				 * @brief Close the shared Ring for further writes.
				 * @details Same effect as Producer::Close on this Ring. Readers may still drain.
				 */
				inline void Close() noexcept {
					Storage().Close();
				}

				/**
				 * @brief Discard bytes from the current read position.
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
				 * @}
				 */

				/**
				 * @name Extract (destructive read)
				 * @{
				 */

				/**
				 * @brief Extract bytes into owned storage.
				 * @param count Bytes to extract. 0 means all available.
				 * @param out Destination. Appended to.
				 * @return False on failure.
				 */
				bool Extract(const StormByte::ByteSize& count, StormByte::Safe::Binary& out) noexcept override;

				/**
				 * @brief Extract bytes into a writer.
				 * @param count Bytes to extract. 0 means all available.
				 * @param out Destination.
				 * @return False on failure.
				 */
				bool Extract(const StormByte::ByteSize& count, WriteOnly& out) noexcept override;

				/**
				 * @brief Extract until EoF into owned storage.
				 * @param out Destination.
				 */
				void ExtractUntilEoF(StormByte::Safe::Binary& out) noexcept override;

				/**
				 * @brief Extract until EoF into a writer.
				 * @param out Destination.
				 */
				void ExtractUntilEoF(WriteOnly& out) noexcept override;

				/**
				 * @}
				 */

				/**
				 * @name Read (non-destructive)
				 * @{
				 */

				/**
				 * @brief Read into owned storage and advance the cursor.
				 * @param count Bytes to read. 0 means all available.
				 * @param out Destination. Appended to.
				 * @return False on failure.
				 */
				bool Read(const StormByte::ByteSize& count, StormByte::Safe::Binary& out) const noexcept override;

				/**
				 * @brief Read into a writer and advance the cursor.
				 * @param count Bytes to read. 0 means all available.
				 * @param out Destination.
				 * @return False on failure.
				 */
				bool Read(const StormByte::ByteSize& count, WriteOnly& out) const noexcept override;

				/**
				 * @brief Read until EoF into owned storage.
				 * @param out Destination.
				 */
				void ReadUntilEoF(StormByte::Safe::Binary& out) const noexcept override;

				/**
				 * @brief Read until EoF into a writer.
				 * @param out Destination.
				 */
				void ReadUntilEoF(WriteOnly& out) const noexcept override;

				/**
				 * @}
				 */

				/**
				 * @name Peek
				 * @{
				 */

				/**
				 * @brief Peek into owned storage. Does not advance the cursor.
				 * @param count Bytes to peek. 0 means all available.
				 * @param out Destination. Appended to.
				 * @return False on failure.
				 */
				bool Peek(const StormByte::ByteSize& count, StormByte::Safe::Binary& out) const noexcept override;

				/**
				 * @brief Peek into a writer. Does not advance the cursor.
				 * @param count Bytes to peek. 0 means all available.
				 * @param out Destination.
				 * @return False on failure.
				 */
				bool Peek(const StormByte::ByteSize& count, WriteOnly& out) const noexcept override;

				/**
				 * @}
				 */

			private:
				friend class Producer;

				StormByte::Safe::Owner m_buffer;	///< Shared Ring ownership.

				/**
				 * @brief Borrow the Ring held by this handle.
				 * @return Ring reference valid for this Consumer's lifetime.
				 */
				Ring& Storage() const noexcept;

				/**
				 * @brief Construct over an existing Ring.
				 * @param buffer Shared ring. Must not be null.
				 */
				explicit Consumer(StormByte::Safe::Owner buffer) noexcept;
		};
	}
}

/**
 * @brief Consumer ownership relies on Buffer's module-local Ring callbacks.
 * @note Buffer and Base must remain loaded with a compatible ABI until all handles are released.
 *       Copies share storage. Borrowed data references must not outlive the shared Ring.
 */
STORMBYTE_DECLARE_MAYBE_SAFE(StormByte::Buffer::Consumer);
