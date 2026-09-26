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
#include <StormByte/buffer/generic.hxx>
#include <StormByte/buffer/producer.hxx>
#include <StormByte/buffer/typedefs.hxx>
#include <StormByte/clonable.hxx>

#include <functional>
#include <string_view>
#include <variant>

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
		 * @class ExternalReader
		 * @brief Adapter over an in-memory readable byte store.
		 *
		 * Pipeline stages and @ref Bridge read through this type so they do not
		 * depend on a concrete @ref FIFO, @ref SharedFIFO, @ref Ring or
		 * @ref Consumer. Device origins (file, socket) are @ref IO::BufferedReader
		 * leaves, not ExternalReader.
		 *
		 * Lightweight: storage lives in the referenced buffer or in an owned
		 * @ref Consumer handle. Does not Open or prefetch. @c Read / @c Extract
		 * return bool; there is no TryAgain.
		 *
		 * @see ExternalBufferReader, ExternalWriter, Pipeline, Bridge
		 */
		class STORMBYTE_BUFFER_PUBLIC ExternalReader
			: public Clonable<ExternalReader, StormByte::Unique<ExternalReader>> {
			public:
				/**
				 * @brief Default constructor.
				 */
				ExternalReader() noexcept = default;

				/**
				 * @brief Copy constructor.
				 * @param other Instance to copy.
				 */
				ExternalReader(const ExternalReader& other) = default;

				/**
				 * @brief Move constructor.
				 * @param other Instance to take from.
				 */
				ExternalReader(ExternalReader&& other) noexcept = default;

				/**
				 * @brief Destructor.
				 */
				~ExternalReader() noexcept override;

				/**
				 * @brief Copy assignment.
				 * @param other Instance to copy.
				 * @return *this.
				 */
				ExternalReader& operator=(const ExternalReader& other) = default;

				/**
				 * @brief Move assignment.
				 * @param other Instance to take from.
				 * @return *this.
				 */
				ExternalReader& operator=(ExternalReader&& other) noexcept = default;

				/**
				 * @brief Bytes that can be read without waiting on a producer.
				 * @return Available octets. 0 if the store is empty.
				 */
				virtual StormByte::ByteSize Available() const noexcept = 0;

				/**
				 * @brief Whether the store holds no readable bytes.
				 * @return @c true if empty.
				 */
				virtual bool Empty() const noexcept = 0;

				/**
				 * @brief End of stream.
				 * @return @c true when closed or failed and nothing remains.
				 */
				virtual bool EoF() const noexcept = 0;

				/**
				 * @brief Whether the source can still be read.
				 * @return @c false on permanent error.
				 */
				virtual bool IsReadable() const noexcept = 0;

				/**
				 * @brief Non-destructive read. Advances the logical cursor.
				 * @param count Bytes to request. 0 = all available now.
				 * @param out Destination (appended to).
				 * @return @c false on insufficient data or error.
				 */
				virtual bool Read(const StormByte::ByteSize& count, BinaryData& out) const noexcept = 0;

				/**
				 * @brief Read all bytes available now.
				 * @param out Destination (appended to).
				 * @return @c false on insufficient data or error.
				 */
				inline bool Read(BinaryData& out) const noexcept {
					return Read(StormByte::ByteSize{0}, out);
				}

				/**
				 * @brief Destructive read. Consumes data from the store.
				 * @param count Bytes to request. 0 = all available now.
				 * @param out Destination (appended to).
				 * @return @c false on insufficient data or error.
				 */
				virtual bool Extract(const StormByte::ByteSize& count, BinaryData& out) noexcept = 0;

				/**
				 * @brief Extract all bytes available now.
				 * @param out Destination (appended to).
				 * @return @c false on insufficient data or error.
				 */
				inline bool Extract(BinaryData& out) noexcept {
					return Extract(StormByte::ByteSize{0}, out);
				}

				/**
				 * @brief Peek. Does not advance the cursor.
				 * @param count Bytes to request. 0 = all available now.
				 * @param out Destination (appended to).
				 * @return @c false on insufficient data or error.
				 */
				virtual bool Peek(const StormByte::ByteSize& count, BinaryData& out) const noexcept = 0;

				/**
				 * @brief Read until EoF without consuming the store.
				 * @param out Destination.
				 * @warning Can grow without bound. Prefer bounded @c Read.
				 */
				virtual void ReadUntilEoF(BinaryData& out) const noexcept = 0;

				/**
				 * @brief Extract until EoF.
				 * @param out Destination.
				 * @warning Can grow without bound. Prefer bounded @c Extract.
				 */
				virtual void ExtractUntilEoF(BinaryData& out) noexcept = 0;

				/**
				 * @brief Move the logical read cursor.
				 * @param offset Offset.
				 * @param mode Absolute or relative.
				 * @details Default is a no-op. Seekable stores override.
				 */
				virtual void Seek(std::ptrdiff_t offset, Position mode) const noexcept;

				/**
				 * @brief Drop already-consumed bytes from the start of the store.
				 * @details Default is a no-op.
				 */
				virtual void Clean() noexcept;
		};

		/**
		 * @class ExternalBufferReader
		 * @brief @ref ExternalReader over a @ref ReadOnly store or an owned @ref Consumer.
		 *
		 * @ref ReadOnly does not own the store; that object must outlive this
		 * adapter. @ref Consumer is taken by value and shares the @ref Ring.
		 * A @ref Consumer argument selects the handle constructor (Identity),
		 * not the @ref ReadOnly constructor.
		 *
		 * @see ExternalReader, ReadOnly, Consumer
		 */
		class STORMBYTE_BUFFER_PUBLIC ExternalBufferReader final : public ExternalReader {
			public:
				/**
				 * @brief Adapt a @ref ReadOnly store. Not owned.
				 * @param buffer Store. Must outlive this object.
				 */
				explicit ExternalBufferReader(ReadOnly& buffer) noexcept
					: m_store(std::ref(buffer)) {}

				/**
				 * @brief Adapt a @ref Consumer. Owns a copy of the handle.
				 * @param consumer Reader on a shared @ref Ring.
				 *
				 * The source @ref Consumer may die. This adapter keeps the Ring
				 * alive. Use this when a @ref Bridge holds the adapter.
				 */
				explicit ExternalBufferReader(Consumer consumer) noexcept
					: m_store(std::move(consumer)) {}

				/**
				 * @brief Copy constructor. Copies the ref or the handle.
				 * @param other Instance to copy.
				 */
				ExternalBufferReader(const ExternalBufferReader& other) = default;

				/**
				 * @brief Move constructor.
				 * @param other Instance to take from.
				 */
				ExternalBufferReader(ExternalBufferReader&& other) noexcept = default;

				/**
				 * @brief Destructor.
				 */
				~ExternalBufferReader() noexcept override;

				/**
				 * @brief Copy assignment.
				 * @param other Instance to copy.
				 * @return *this.
				 */
				ExternalBufferReader& operator=(const ExternalBufferReader& other) = default;

				/**
				 * @brief Move assignment.
				 * @param other Instance to take from.
				 * @return *this.
				 */
				ExternalBufferReader& operator=(ExternalBufferReader&& other) noexcept = default;

				/**
				 * @brief Polymorphic copy. Same store or a copy of the handle.
				 * @return New adapter.
				 */
				PointerType Clone() const noexcept override;

				/**
				 * @brief Polymorphic move.
				 * @return New adapter.
				 */
				PointerType Move() noexcept override;

				/**
				 * @brief Bytes available on the store without waiting.
				 * @return Available octets.
				 */
				StormByte::ByteSize Available() const noexcept override;

				/**
				 * @brief Whether the store holds no readable bytes.
				 * @return @c true if empty.
				 */
				bool Empty() const noexcept override;

				/**
				 * @brief End of stream of the store.
				 * @return @c true when closed or failed and nothing remains.
				 */
				bool EoF() const noexcept override;

				/**
				 * @brief Whether the store can still be read.
				 * @return @c false on permanent error.
				 */
				bool IsReadable() const noexcept override;

				/**
				 * @brief Non-destructive read on the store.
				 * @param count Bytes to request. 0 = all available now.
				 * @param out Destination (appended to).
				 * @return @c false on insufficient data or error.
				 */
				bool Read(const StormByte::ByteSize& count, BinaryData& out) const noexcept override;

				/**
				 * @brief Destructive read on the store.
				 * @param count Bytes to request. 0 = all available now.
				 * @param out Destination (appended to).
				 * @return @c false on insufficient data or error.
				 */
				bool Extract(const StormByte::ByteSize& count, BinaryData& out) noexcept override;

				/**
				 * @brief Peek on the store. Does not advance the cursor.
				 * @param count Bytes to request. 0 = all available now.
				 * @param out Destination (appended to).
				 * @return @c false on insufficient data or error.
				 */
				bool Peek(const StormByte::ByteSize& count, BinaryData& out) const noexcept override;

				/**
				 * @brief Read the store until EoF without consuming it.
				 * @param out Destination.
				 * @warning Can grow without bound.
				 */
				void ReadUntilEoF(BinaryData& out) const noexcept override;

				/**
				 * @brief Extract the store until EoF.
				 * @param out Destination.
				 * @warning Can grow without bound.
				 */
				void ExtractUntilEoF(BinaryData& out) noexcept override;

				/**
				 * @brief Move the logical read cursor of the store.
				 * @param offset Offset.
				 * @param mode Absolute or relative.
				 */
				void Seek(std::ptrdiff_t offset, Position mode) const noexcept override;

				/**
				 * @brief Drop already-consumed bytes from the start of the store.
				 */
				void Clean() noexcept override;

			private:
				std::variant<std::reference_wrapper<ReadOnly>, Consumer> m_store;	///< Ref or owned handle

				/**
				 * @brief Active store.
				 * @return ReadOnly view of the ref or the owned Consumer.
				 */
				ReadOnly& Store() noexcept;

				/**
				 * @brief Active store.
				 * @return ReadOnly view of the ref or the owned Consumer.
				 */
				const ReadOnly& Store() const noexcept;
		};

		/**
		 * @class ExternalWriter
		 * @brief Adapter over an in-memory writable byte store.
		 *
		 * Pipeline stages and @ref Bridge write through this type so they do not
		 * depend on a concrete @ref FIFO, @ref SharedFIFO, @ref Ring or
		 * @ref Producer. Device sinks are @ref IO::BufferedWriter leaves, not
		 * ExternalWriter.
		 *
		 * @c Write accepts the whole request or fails. No TryAgain and no
		 * write-behind. Limit fill from the outside with @ref Occupied
		 * (`Occupied() + want` vs a high-water mark); do not change @c Write.
		 *
		 * @see ExternalBufferWriter, ExternalReader, Pipeline, Bridge
		 */
		class STORMBYTE_BUFFER_PUBLIC ExternalWriter
			: public Clonable<ExternalWriter, StormByte::Unique<ExternalWriter>> {
			public:
				/**
				 * @brief Default constructor.
				 */
				ExternalWriter() noexcept = default;

				/**
				 * @brief Copy constructor.
				 * @param other Instance to copy.
				 */
				ExternalWriter(const ExternalWriter& other) = default;

				/**
				 * @brief Move constructor.
				 * @param other Instance to take from.
				 */
				ExternalWriter(ExternalWriter&& other) noexcept = default;

				/**
				 * @brief Destructor.
				 */
				~ExternalWriter() noexcept override;

				/**
				 * @brief Copy assignment.
				 * @param other Instance to copy.
				 * @return *this.
				 */
				ExternalWriter& operator=(const ExternalWriter& other) = default;

				/**
				 * @brief Move assignment.
				 * @param other Instance to take from.
				 * @return *this.
				 */
				ExternalWriter& operator=(ExternalWriter&& other) noexcept = default;

				/**
				 * @brief Whether the sink still accepts writes.
				 * @return @c false if closed or in error.
				 */
				virtual bool IsWritable() const noexcept = 0;

				/**
				 * @brief Bytes stored in the sink right now.
				 * @return Occupancy. 0 if the store is empty.
				 */
				virtual StormByte::ByteSize Occupied() const noexcept = 0;

				/**
				 * @brief Write a full @ref StormByte::BinaryData (copy).
				 * @param data Octets to append.
				 * @return @c false if closed or in error.
				 */
				virtual bool Write(const BinaryData& data) noexcept = 0;

				/**
				 * @brief Write a full @ref StormByte::BinaryData (move).
				 * @param data Octets to append.
				 * @return @c false if closed or in error.
				 */
				virtual bool Write(BinaryData&& data) noexcept = 0;

				/**
				 * @brief Write up to @p count bytes (copy).
				 * @param count Maximum bytes. 0 = entire payload.
				 * @param data Source.
				 * @return @c false if closed or in error.
				 */
				virtual bool Write(const StormByte::ByteSize& count, const BinaryData& data) noexcept = 0;

				/**
				 * @brief Write up to @p count bytes (move).
				 * @param count Maximum bytes. 0 = entire payload.
				 * @param data Source.
				 * @return @c false if closed or in error.
				 */
				virtual bool Write(const StormByte::ByteSize& count, BinaryData&& data) noexcept = 0;

				/**
				 * @brief Write a string view. No terminating NUL.
				 * @param sv Source.
				 * @return @c false if closed or in error.
				 */
				bool Write(std::string_view sv) noexcept;

				/**
				 * @brief Write a C string.
				 * @param s Source. Null is an empty write.
				 * @return @c false if closed or in error.
				 */
				bool Write(const char* s) noexcept;

				/**
				 * @brief Write up to @p count characters of a string view.
				 * @param count Maximum characters. 0 = entire view.
				 * @param sv Source.
				 * @return @c false if closed or in error.
				 */
				bool Write(const StormByte::ByteSize& count, std::string_view sv) noexcept;

				/**
				 * @brief Write a string literal without the trailing NUL.
				 * @tparam N Array extent (includes the NUL of a literal).
				 * @param s Literal.
				 * @return @c false if closed or in error.
				 */
				template<std::size_t N>
				bool Write(const char (&s)[N]) noexcept {
					if (N == 0)
						return Write(BinaryData{});
					return Write(std::string_view(s, N > 0 ? N - 1 : 0));
				}

				/**
				 * @brief Stop further writes. Readers may still drain the store.
				 */
				virtual void Close() noexcept = 0;

				/**
				 * @brief Permanent error. Wake waiters.
				 */
				virtual void SetError() noexcept = 0;
		};

		/**
		 * @class ExternalBufferWriter
		 * @brief @ref ExternalWriter over a @ref WriteOnly store or an owned @ref Producer.
		 *
		 * @ref WriteOnly does not own the store; that object must outlive this
		 * adapter. @ref Producer is taken by value and shares the @ref Ring.
		 * A @ref Producer argument selects the handle constructor (Identity),
		 * not the @ref WriteOnly constructor.
		 *
		 * @ref Occupied is @ref Generic::Size of the active store.
		 *
		 * @see ExternalWriter, WriteOnly, Producer, Ring
		 */
		class STORMBYTE_BUFFER_PUBLIC ExternalBufferWriter final : public ExternalWriter {
			public:
				/**
				 * @brief Adapt a @ref WriteOnly store. Not owned.
				 * @param buffer Store. Must outlive this object.
				 */
				explicit ExternalBufferWriter(WriteOnly& buffer) noexcept
					: m_store(std::ref(buffer)) {}

				/**
				 * @brief Adapt a @ref Producer. Owns a copy of the handle.
				 * @param producer Writer on a shared @ref Ring.
				 *
				 * The source @ref Producer may die. This adapter keeps the Ring
				 * alive. Use this when a @ref Bridge holds the adapter.
				 */
				explicit ExternalBufferWriter(Producer producer) noexcept
					: m_store(std::move(producer)) {}

				/**
				 * @brief Copy constructor. Copies the ref or the handle.
				 * @param other Instance to copy.
				 */
				ExternalBufferWriter(const ExternalBufferWriter& other) = default;

				/**
				 * @brief Move constructor.
				 * @param other Instance to take from.
				 */
				ExternalBufferWriter(ExternalBufferWriter&& other) noexcept = default;

				/**
				 * @brief Destructor.
				 */
				~ExternalBufferWriter() noexcept override;

				/**
				 * @brief Copy assignment.
				 * @param other Instance to copy.
				 * @return *this.
				 */
				ExternalBufferWriter& operator=(const ExternalBufferWriter& other) = default;

				/**
				 * @brief Move assignment.
				 * @param other Instance to take from.
				 * @return *this.
				 */
				ExternalBufferWriter& operator=(ExternalBufferWriter&& other) noexcept = default;

				/**
				 * @brief Polymorphic copy. Same store or a copy of the handle.
				 * @return New adapter.
				 */
				PointerType Clone() const noexcept override;

				/**
				 * @brief Polymorphic move.
				 * @return New adapter.
				 */
				PointerType Move() noexcept override;

				/**
				 * @brief Whether the store still accepts writes.
				 * @return @c false if closed or in error.
				 */
				bool IsWritable() const noexcept override;

				/**
				 * @brief Bytes stored in the sink right now.
				 * @return @ref Generic::Size of the store. 0 if empty.
				 */
				StormByte::ByteSize Occupied() const noexcept override;

				/**
				 * @brief Write a full @ref StormByte::BinaryData (copy).
				 * @param data Octets to append.
				 * @return @c false if closed or in error.
				 */
				bool Write(const BinaryData& data) noexcept override;

				/**
				 * @brief Write a full @ref StormByte::BinaryData (move).
				 * @param data Octets to append.
				 * @return @c false if closed or in error.
				 */
				bool Write(BinaryData&& data) noexcept override;

				/**
				 * @brief Write up to @p count bytes (copy).
				 * @param count Maximum bytes. 0 = entire payload.
				 * @param data Source.
				 * @return @c false if closed or in error.
				 */
				bool Write(const StormByte::ByteSize& count, const BinaryData& data) noexcept override;

				/**
				 * @brief Write up to @p count bytes (move).
				 * @param count Maximum bytes. 0 = entire payload.
				 * @param data Source.
				 * @return @c false if closed or in error.
				 */
				bool Write(const StormByte::ByteSize& count, BinaryData&& data) noexcept override;

				/**
				 * @brief Bring string and counted helpers from @ref ExternalWriter into scope.
				 */
				using ExternalWriter::Write;

				/**
				 * @brief Stop further writes on the store.
				 */
				void Close() noexcept override;

				/**
				 * @brief Permanent error on the store. Wake waiters.
				 */
				void SetError() noexcept override;

			private:
				std::variant<std::reference_wrapper<WriteOnly>, Producer> m_store;	///< Ref or owned handle

				/**
				 * @brief Active store.
				 * @return WriteOnly view of the ref or the owned Producer.
				 */
				WriteOnly& Store() noexcept;

				/**
				 * @brief Active store.
				 * @return WriteOnly view of the ref or the owned Producer.
				 */
				const WriteOnly& Store() const noexcept;
		};
	}
}
