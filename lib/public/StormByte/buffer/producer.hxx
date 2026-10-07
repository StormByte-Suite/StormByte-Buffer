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

#include <StormByte/buffer/consumer.hxx>
#include <StormByte/safe/owner.hxx>

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
		 * @class Producer
		 * @brief Write-only handle over a shared @ref Ring.
		 *
		 * Several Producer instances may share the same Ring. Writes are
		 * thread-safe. Occupancy is @ref Generic::Size of that Ring.
		 * @ref Consumer() returns a matching reader on the same store.
		 *
		 * @see Consumer, Ring, WriteOnly
		 */
		class STORMBYTE_BUFFER_PUBLIC Producer final: public WriteOnly {
			public:
				/**
				 * @name Lifecycle
				 * @{
				 */

				/**
				 * @brief Create a Producer and a new shared Ring.
				 * @throws StormByte::Safe::AllocationError Ring ownership cannot be allocated.
				 */
				Producer();

				/**
				 * @brief Share the Ring of a Consumer.
				 * @param consumer Consumer whose Ring is shared.
				 * @throws StormByte::Exception The shared owner cannot be retained.
				 */
				Producer(const Consumer& consumer);

				/**
				 * @brief Copy a Producer. Both share the same Ring.
				 * @param other Source.
				 * @throws StormByte::Exception The shared owner cannot be retained.
				 */
				Producer(const Producer& other);

				/**
				 * @brief Take a Producer.
				 * @param other Source.
				 */
				Producer(Producer&& other) noexcept;

				/**
				 * @brief Destroy the Producer.
				 */
				~Producer() noexcept override;

				/**
				 * @brief Copy-assign a Producer. Both share the same Ring afterwards.
				 * @param other Source.
				 * @return This Producer.
				 * @throws StormByte::Exception The shared owner cannot be retained.
				 */
				Producer& operator=(const Producer& other);

				/**
				 * @brief Move-assign a Producer.
				 * @param other Source.
				 * @return This Producer.
				 */
				Producer& operator=(Producer&& other) noexcept;

				/**
				 * @}
				 */

				/**
				 * @name Comparison
				 * @{
				 */

				/**
				 * @brief Compare two Producers.
				 * @param other Other Producer.
				 * @return Whether both refer to the same Ring.
				 */
				bool operator==(const Producer& other) const noexcept;

				/**
				 * @brief Compare two Producers.
				 * @param other Other Producer.
				 * @return Whether the Rings differ.
				 */
				inline bool operator!=(const Producer& other) const noexcept {
					return !(*this == other);
				}

				/**
				 * @}
				 */

				/**
				 * @name Lifecycle / queries
				 * @{
				 */

				/**
				 * @brief Close the shared Ring for further writes and notify waiters.
				 * @details Later writes fail. Readers may still drain.
				 */
				void Close() noexcept override;

				/**
				 * @brief Put the shared Ring in a permanent error state and notify waiters.
				 */
				void SetError() noexcept override;

				/**
				 * @brief Whether the shared Ring still accepts writes.
				 * @return False if closed or in error.
				 */
				bool IsWritable() const noexcept override;

				/**
				 * @brief Bytes stored in the shared Ring.
				 * @return Size in bytes. Zero if empty.
				 */
				StormByte::ByteSize Size() const noexcept override;

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
				 * @brief Append an entire owned buffer (copy).
				 * @param data Source.
				 * @return False if closed or in error.
				 */
				inline bool Write(const StormByte::Safe::Binary& data) noexcept {
					return Write(data.size(), data);
				}

				/**
				 * @brief Append bytes from owned storage (move).
				 * @param count Bytes to write.
				 * @param data Source.
				 * @return False if closed or in error.
				 */
				bool Write(const StormByte::ByteSize& count, StormByte::Safe::Binary&& data) noexcept override;

				/**
				 * @brief Append an entire owned buffer (move).
				 * @param data Source.
				 * @return False if closed or in error.
				 */
				inline bool Write(StormByte::Safe::Binary&& data) noexcept {
					return Write(data.size(), std::move(data));
				}

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

				/**
				 * @brief Consumer that shares this Producer's Ring.
				 * @return Consumer bound to the same store.
				 */
				class Consumer Consumer();

			private:
				StormByte::Safe::Owner m_buffer;	///< Shared Ring ownership.

				/**
				 * @brief Borrow the Ring held by this handle.
				 * @return Ring reference valid for this Producer's lifetime.
				 */
				Ring& Storage() const noexcept;
		};
	}
}

/**
 * @brief Producer ownership relies on Buffer's module-local Ring callbacks.
 * @note Buffer and Base must remain loaded with a compatible ABI until all handles are released.
 *       Copies share storage. They do not clone the Ring or transfer its private ownership.
 */
STORMBYTE_DECLARE_MAYBE_SAFE(StormByte::Buffer::Producer);
