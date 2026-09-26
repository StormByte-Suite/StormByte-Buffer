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

#include <StormByte/buffer/ring.hxx>

#include <memory>

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
		 * Multiple Consumer instances may share the same underlying Ring,
		 * allowing concurrent reads in a fully thread-safe manner.
		 *
		 * An empty Consumer (`Consumer()`) creates its own Ring. @ref Producer()
		 * then returns a writer on that Ring. A Consumer may also be obtained
		 * from @ref Producer::Consumer().
		 *
		 * All operations are thread-safe and delegate to the shared Ring.
		 * Blocking semantics match @ref Ring. Read / Extract / Peek block until
		 * data is available or the buffer is closed / in error.
		 *
		 * @par Lifecycle signalling
		 * Although Consumer is primarily a @ref ReadOnly view, it also exposes:
		 * - @ref Close() — closes the shared Ring for further writes (same effect
		 *   as @ref Producer::Close on the same buffer).
		 * - @ref IsWritable() / @ref HasError() — observe the shared Ring state
		 *   (useful in wait loops, e.g. until a pipeline finishes).
		 *
		 * @see Producer, Ring, ReadOnly
		 */
		class STORMBYTE_BUFFER_PUBLIC Consumer final: public ReadOnly {
			friend class Producer;

			public:
				/**
				 * @name Constructors / destructor / assignment
				 * @{
				 */

				/**
				 * @brief Empty Consumer. Creates a new shared @ref Ring.
				 *
				 * @ref Producer() returns a writer on that Ring. Use this when
				 * the owner only reads and still needs a write tip for a @ref Bridge.
				 */
				inline Consumer() noexcept : m_buffer(std::make_shared<Ring>()) {}

				/**
				 * @brief Copy constructor.
				 * @param other Source Consumer; both share the same Ring.
				 */
				inline Consumer(const Consumer& other) noexcept : m_buffer(other.m_buffer) {}

				/**
				 * @brief Move constructor.
				 * @param other Source Consumer (left in a valid but unspecified state).
				 */
				inline Consumer(Consumer&& other) noexcept : m_buffer(std::move(other.m_buffer)) {}

				/**
				 * @brief Destructor.
				 */
				~Consumer() noexcept override;

				/**
				 * @brief Copy assignment.
				 * @param other Source Consumer; both share the same Ring afterwards.
				 * @return Reference to this Consumer.
				 */
				inline Consumer& operator=(const Consumer& other) noexcept {
					if (this != &other)
						m_buffer = other.m_buffer;
					return *this;
				}

				/**
				 * @brief Move assignment.
				 * @param other Source Consumer.
				 * @return Reference to this Consumer.
				 */
				inline Consumer& operator=(Consumer&& other) noexcept {
					if (this != &other)
						m_buffer = std::move(other.m_buffer);
					return *this;
				}

				/** @} */

				/**
				 * @name Comparison
				 * @{
				 */

				/**
				 * @brief Equality comparison.
				 * @param other Other Consumer.
				 * @return @c true if both refer to the same underlying Ring instance.
				 */
				inline bool operator==(const Consumer& other) const noexcept {
					return m_buffer.get() == other.m_buffer.get();
				}

				/**
				 * @brief Inequality comparison.
				 * @param other Other Consumer.
				 * @return @c true if the underlying Ring instances differ.
				 */
				inline bool operator!=(const Consumer& other) const noexcept {
					return !(*this == other);
				}

				/** @} */

				/**
				 * @name Queries
				 * @{
				 */

				/**
				 * @brief Number of bytes available for reading from the current position.
				 * @return Available byte count.
				 */
				StormByte::ByteSize Available() const noexcept override;

				/**
				 * @brief Access a snapshot of the underlying data (implementation-defined).
				 * @return Constant reference to the Ring’s @ref StormByte::BinaryData view.
				 * @warning Not intended for concurrent mutation; prefer Read / Extract.
				 */
				const StormByte::BinaryData& BinaryData() const noexcept override;

				/**
				 * @brief Whether the shared Ring holds no stored bytes.
				 * @return @c true if empty.
				 * @note With a non-zero read position, @ref Empty() may still be @c false
				 *       even when @ref Available() is zero.
				 */
				bool Empty() const noexcept override;

				/**
				 * @brief End-of-stream condition.
				 * @return @c true when the Ring is closed (or in error) and no bytes remain.
				 */
				bool EoF() const noexcept override;

				/**
				 * @brief Whether the shared Ring can still be read.
				 * @return @c false if the Ring is in a permanent error state.
				 */
				bool IsReadable() const noexcept override;

				/**
				 * @brief Whether the shared Ring still accepts writes.
				 * @return @c false if closed or in error.
				 * @details Observes producer-side lifecycle on the same Ring
				 *          (e.g. wait until a pipeline stage calls Close()).
				 */
				inline bool IsWritable() const noexcept {
					return m_buffer->IsWritable();
				}

				/**
				 * @brief Whether the shared Ring is in a permanent error state.
				 * @return @c true after @ref Producer::SetError on any handle to the same Ring.
				 */
				inline bool HasError() const noexcept {
					return m_buffer->HasError();
				}

				/**
				 * @brief Total number of bytes stored in the shared Ring.
				 * @return Size in bytes.
				 */
				StormByte::ByteSize Size() const noexcept override;

				/**
				 * @brief Writer on the same Ring.
				 * @return Producer that shares this Consumer’s store.
				 *
				 * Inverse of @ref Producer::Consumer. The Ring already exists
				 * (`Consumer()` or a Producer-born Consumer).
				 */
				class Producer Producer() const noexcept;

				/** @} */

				/**
				 * @name Maintenance / lifecycle
				 * @{
				 */

				/**
				 * @brief Discard already-consumed data (from start up to the read position).
				 */
				void Clean() noexcept override;

				/**
				 * @brief Clear all buffer contents.
				 * @details Does not clear closed / error flags on the shared Ring.
				 */
				void Clear() noexcept override;

				/**
				 * @brief Close the shared Ring for further writes.
				 * @details Equivalent to @ref Producer::Close on the same underlying buffer.
				 *          Readers may still drain remaining data until EoF.
				 */
				inline void Close() noexcept {
					m_buffer->Close();
				}

				/**
				 * @brief Discard @p count bytes from the current read position.
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

				/** @} */

				/**
				 * @name Extract (destructive read)
				 * @{
				 */

				/**
				 * @brief Extract bytes into a @ref StormByte::BinaryData (consumes data from the Ring).
				 * @param count Number of bytes to extract; 0 extracts all available.
				 * @param out Destination (appended to).
				 * @return @c true on success.
				 */
				bool Extract(const StormByte::ByteSize& count, StormByte::BinaryData& out) noexcept override;

				/**
				 * @brief Extract bytes into a @ref WriteOnly store.
				 * @param count Number of bytes to extract; 0 extracts all available.
				 * @param out Destination.
				 * @return @c true on success.
				 */
				bool Extract(const StormByte::ByteSize& count, WriteOnly& out) noexcept override;

				/**
				 * @brief Extract until EoF into a @ref StormByte::BinaryData.
				 * @param out Destination.
				 */
				void ExtractUntilEoF(StormByte::BinaryData& out) noexcept override;

				/**
				 * @brief Extract until EoF into a @ref WriteOnly store.
				 * @param out Destination.
				 */
				void ExtractUntilEoF(WriteOnly& out) noexcept override;

				/** @} */

				/**
				 * @name Read (non-destructive)
				 * @{
				 */

				/**
				 * @brief Read bytes into a @ref StormByte::BinaryData. Advances the cursor.
				 * @param count Number of bytes to read; 0 reads all available.
				 * @param out Destination (appended to).
				 * @return @c true on success.
				 */
				bool Read(const StormByte::ByteSize& count, StormByte::BinaryData& out) const noexcept override;

				/**
				 * @brief Read bytes into a @ref WriteOnly store. Advances the cursor.
				 * @param count Number of bytes to read; 0 reads all available.
				 * @param out Destination.
				 * @return @c true on success.
				 */
				bool Read(const StormByte::ByteSize& count, WriteOnly& out) const noexcept override;

				/**
				 * @brief Read until EoF into a @ref StormByte::BinaryData.
				 * @param out Destination.
				 */
				void ReadUntilEoF(StormByte::BinaryData& out) const noexcept override;

				/**
				 * @brief Read until EoF into a @ref WriteOnly store.
				 * @param out Destination.
				 */
				void ReadUntilEoF(WriteOnly& out) const noexcept override;

				/** @} */

				/**
				 * @name Peek
				 * @{
				 */

				/**
				 * @brief Peek bytes into a @ref StormByte::BinaryData. Does not advance the cursor.
				 * @param count Number of bytes to peek; 0 peeks all available.
				 * @param out Destination (appended to).
				 * @return @c true on success.
				 */
				bool Peek(const StormByte::ByteSize& count, StormByte::BinaryData& out) const noexcept override;

				/**
				 * @brief Peek bytes into a @ref WriteOnly store. Does not advance the cursor.
				 * @param count Number of bytes to peek; 0 peeks all available.
				 * @param out Destination.
				 * @return @c true on success.
				 */
				bool Peek(const StormByte::ByteSize& count, WriteOnly& out) const noexcept override;

				/** @} */

			private:
				std::shared_ptr<Ring> m_buffer;	///< Shared ring storage

				/**
				 * @brief Construct over an existing Ring.
				 * @param buffer Shared ring instance (must not be null).
				 *
				 * Used by @ref Producer::Consumer.
				 */
				inline explicit Consumer(std::shared_ptr<Ring> buffer) noexcept
					: m_buffer(std::move(buffer)) {}
		};
	}
}
