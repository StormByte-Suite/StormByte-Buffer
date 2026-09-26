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

#include <StormByte/buffer/io/buffered_location_writer.hxx>
#include <StormByte/buffer/visibility.h>

#include <fstream>
#include <mutex>

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
		 * @namespace StormByte::Buffer::IO
		 * @brief Buffered binary sources and sinks.
		 */
		namespace IO {
			/**
			 * @class BufferedFileWriter
			 * @brief Final @ref BufferedLocationWriter over a filesystem file.
			 *
			 * Binary file, random-access. Open creates the file when
			 * missing and leaves existing content. Overwrite is
			 * @ref Truncate. Does not open in the constructor. Does
			 * not create parent directories.
			 *
			 * The device does not change after construction. Chunk,
			 * backpressure and @ref MaxMemory for the path-only constructor
			 * come from @ref BufferedLocationWriter::Setup.
			 *
			 * @par Constructors
			 * @c BufferedFileWriter(path) asks the location layer to probe.
			 * @c BufferedFileWriter(path, write_chunk, back_pressure, max_wait)
			 * stores those values and leaves @ref MaxMemory at 0.
			 * @c BufferedFileWriter(path, write_chunk, max_memory, back_pressure, max_wait)
			 * stores the page budget too. A zero chunk or backpressure disables
			 * the ring. A zero @ref MaxMemory stores no pages.
			 *
			 * This leaf only opens, writes, flushes, truncates, seeks and
			 * reports the file length. @ref OriginDevice builds a
			 * @ref StormByte::System::Device from @ref Location.
			 *
			 * @see BufferedLocationWriter, State
			 */
			class STORMBYTE_BUFFER_PUBLIC BufferedFileWriter final: public BufferedLocationWriter {
				public:
					/**
					 * @name Lifecycle
					 * @{
					 */

					/**
					 * @brief Store the path. Chunk, backpressure and MaxMemory come from @ref Setup.
					 * @param path Local filesystem path. Stored once. @ref Location is @ref Location::Local.
					 */
					explicit BufferedFileWriter(StormByte::String::String path);

					/**
					 * @brief Store the path and explicit ring knobs. Does not open.
					 * @param path Local filesystem path. Stored once. @ref Location is @ref Location::Local.
					 * @param write_chunk Initial @ref WriteChunk in bytes.
					 * @param back_pressure Initial @ref BackPressure in chunks.
					 * @param max_wait Initial @ref MaxWait.
					 *
					 * @ref MaxMemory stays 0.
					 */
					BufferedFileWriter(StormByte::String::String path,
						StormByte::ByteSize write_chunk, std::size_t back_pressure,
						std::chrono::milliseconds max_wait = std::chrono::milliseconds{0});

					/**
					 * @brief Store the path, page budget and ring knobs. Does not open.
					 * @param path Local filesystem path. Stored once. @ref Location is @ref Location::Local.
					 * @param write_chunk Initial @ref WriteChunk in bytes.
					 * @param max_memory Initial @ref MaxMemory in bytes.
					 * @param back_pressure Initial @ref BackPressure in chunks.
					 * @param max_wait Initial @ref MaxWait.
					 */
					BufferedFileWriter(StormByte::String::String path,
						StormByte::ByteSize write_chunk, StormByte::ByteSize max_memory,
						std::size_t back_pressure,
						std::chrono::milliseconds max_wait = std::chrono::milliseconds{0});

					/**
					 * @brief Copy constructor is deleted.
					 */
					BufferedFileWriter(const BufferedFileWriter&) = delete;

					/**
					 * @brief Move constructor.
					 * @param other Instance to take from.
					 */
					BufferedFileWriter(BufferedFileWriter&& other) noexcept;

					/**
					 * @brief Destructor. Calls @ref Close while the leaf vtable is live.
					 */
					~BufferedFileWriter() noexcept override;

					/**
					 * @brief Copy assignment is deleted.
					 * @return *this.
					 */
					BufferedFileWriter& operator=(const BufferedFileWriter&) = delete;

					/**
					 * @brief Move assignment.
					 * @param other Instance to take from.
					 * @return *this.
					 */
					BufferedFileWriter& operator=(BufferedFileWriter&& other) noexcept;

					/**
					 * @}
					 */

				protected:
					/**
					 * @brief Device for the location probe.
					 * @return @ref StormByte::System::Device on @ref Location.
					 */
					StormByte::System::Device OriginDevice() const override;

					/**
					 * @brief On-disk size or the write cursor, whichever is larger.
					 * @return Byte length. 0 when the path cannot be stated.
					 */
					StormByte::ByteSize OriginSize() const noexcept override;

					/**
					 * @brief Open the path for binary random-access write.
					 * @return @ref Status::Ok or @ref Status::Failed.
					 *
					 * Creates the file when missing. Does not truncate an
					 * existing file.
					 */
					Result OriginOpen() override;

					/**
					 * @brief Close the file stream.
					 * @return @ref Status::Ok.
					 */
					virtual Result OriginClose() override;

					/**
					 * @brief Write @p data to the file.
					 * @param data Contiguous octets.
					 * @return Ok with bytes written, Error or Failed.
					 */
					virtual Result OriginPush(std::span<const std::byte> data) override;

					/**
					 * @brief Make written bytes visible to later readers of the path.
					 * @return @ref Status::Ok, Error or Failed.
					 */
					virtual Result OriginFlush() override;

					/**
					 * @brief Resize the file to zero bytes.
					 * @return @ref Status::Ok or @ref Status::Failed.
					 */
					virtual Result OriginTruncate() override;

					/**
					 * @brief Seek the file stream to an absolute byte offset.
					 * @param absolute Byte offset from the start of the file.
					 * @return @ref Status::Ok or @ref Status::Failed.
					 */
					Result OriginSeek(StormByte::ByteSize absolute) override;

					/**
					 * @brief Whether the volume looks able to accept @p n more bytes.
					 * @param n Candidate write.
					 * @return false when the base writer refuses or free space is short.
					 */
					virtual bool WillWrite(StormByte::ByteSize n) const override;

				private:
					std::ofstream m_file;					///< Binary output stream.
					mutable std::mutex m_file_mutex;		///< Serialises ofstream access.
			};
		}
	}
}
