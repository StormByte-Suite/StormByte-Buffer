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

#include <StormByte/buffer/consumer.hxx>

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
		class STORMBYTE_BUFFER_PUBLIC Producer final : public WriteOnly {
			public:
				/**
				 * @name Lifecycle
				 * @{
				 */

				/**
				 * @brief Construct a Producer with a new shared @ref Ring.
				 */
				inline Producer() noexcept : m_buffer(std::make_shared<Ring>()) {}

				/**
				 * @brief Share an existing Ring.
				 * @param buffer Shared ring. Must not be null.
				 */
				inline explicit Producer(std::shared_ptr<Ring> buffer) noexcept
					: m_buffer(std::move(buffer)) {}

				/**
				 * @brief Share the Ring of a @ref Consumer.
				 * @param consumer Consumer whose Ring is shared.
				 */
				inline Producer(const Consumer& consumer) noexcept
					: m_buffer(consumer.m_buffer) {}

				/**
				 * @brief Copy constructor. Shares the same Ring.
				 * @param other Source Producer.
				 */
				inline Producer(const Producer& other) noexcept : m_buffer(other.m_buffer) {}

				/**
				 * @brief Move constructor.
				 * @param other Source Producer.
				 */
				inline Producer(Producer&& other) noexcept : m_buffer(std::move(other.m_buffer)) {}

				/**
				 * @brief Destructor.
				 */
				~Producer() noexcept override;

				/**
				 * @brief Copy assignment. Shares the same Ring afterwards.
				 * @param other Source Producer.
				 * @return *this.
				 */
				inline Producer& operator=(const Producer& other) noexcept {
					if (this != &other)
						m_buffer = other.m_buffer;
					return *this;
				}

				/**
				 * @brief Move assignment.
				 * @param other Source Producer.
				 * @return *this.
				 */
				inline Producer& operator=(Producer&& other) noexcept {
					if (this != &other)
						m_buffer = std::move(other.m_buffer);
					return *this;
				}

				/**
				 * @}
				 */

				/**
				 * @name Comparison
				 * @{
				 */

				/**
				 * @brief Equality. Same underlying Ring instance.
				 * @param other Other Producer.
				 * @return @c true if both refer to the same Ring.
				 */
				inline bool operator==(const Producer& other) const noexcept {
					return m_buffer.get() == other.m_buffer.get();
				}

				/**
				 * @brief Inequality.
				 * @param other Other Producer.
				 * @return @c true if the Rings differ.
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
				 * @brief Close the shared Ring for further writes.
				 * @details Later writes fail. Readers may still drain. Waiters are notified.
				 */
				void Close() noexcept override;

				/**
				 * @brief Permanent error on the shared Ring. Notifies waiters.
				 */
				void SetError() noexcept override;

				/**
				 * @brief Whether the shared Ring still accepts writes.
				 * @return @c false if closed or in error.
				 */
				bool IsWritable() const noexcept override;

				/**
				 * @brief Bytes stored in the shared Ring right now.
				 * @return Size in bytes. 0 if empty.
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
				 * @brief Append bytes from a @ref StormByte::BinaryData (copy).
				 * @param count Bytes to write.
				 * @param data Source.
				 * @return @c false if closed or in error.
				 */
				bool Write(const StormByte::ByteSize& count, const BinaryData& data) noexcept override;

				/**
				 * @brief Append an entire @ref StormByte::BinaryData (copy).
				 * @param data Source.
				 * @return @c false if closed or in error.
				 */
				inline bool Write(const BinaryData& data) noexcept {
					return Write(data.size(), data);
				}

				/**
				 * @brief Append bytes from a @ref StormByte::BinaryData (move).
				 * @param count Bytes to write.
				 * @param data Source.
				 * @return @c false if closed or in error.
				 */
				bool Write(const StormByte::ByteSize& count, BinaryData&& data) noexcept override;

				/**
				 * @brief Append an entire @ref StormByte::BinaryData (move).
				 * @param data Source.
				 * @return @c false if closed or in error.
				 */
				inline bool Write(BinaryData&& data) noexcept {
					return Write(data.size(), std::move(data));
				}

				/**
				 * @brief Append bytes from a @ref ReadOnly (copy).
				 * @param count Bytes to write.
				 * @param data Source buffer.
				 * @return @c false if closed or in error.
				 */
				bool Write(const StormByte::ByteSize& count, const ReadOnly& data) noexcept override;

				/**
				 * @brief Append bytes from a @ref ReadOnly (extract).
				 * @param count Bytes to write.
				 * @param data Source buffer.
				 * @return @c false if closed or in error.
				 */
				bool Write(const StormByte::ByteSize& count, ReadOnly&& data) noexcept override;

				/**
				 * @brief Bring @ref WriteOnly convenience Write overloads into scope.
				 */
				using WriteOnly::Write;

				/**
				 * @}
				 */

				/**
				 * @brief Consumer that shares this Producer’s Ring.
				 * @return Consumer bound to the same store.
				 */
				inline class Consumer Consumer() {
					return StormByte::Buffer::Consumer{ m_buffer };
				}

			protected:
				std::shared_ptr<Ring> m_buffer;	///< Shared ring. Not null after construction.
		};
	}
}
