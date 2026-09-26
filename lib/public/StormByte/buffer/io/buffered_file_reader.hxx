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

#include <StormByte/buffer/io/buffered_location_reader.hxx>
#include <StormByte/buffer/visibility.h>

#include <fstream>
#include <mutex>
#include <optional>

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
			 * @class BufferedFileReader
			 * @brief Final @ref BufferedLocationReader over a filesystem file.
			 *
			 * Binary `ifstream` only. Hooks call @ref SetState.
			 * Does not open in the constructor.
			 *
			 * The device does not change after construction: @ref ReadAhead
			 * is chosen once by @ref BufferedLocationReader::Setup.
			 * Cache size (@ref MaxMemory) stays dynamic.
			 *
			 * @par Constructors
			 * @c BufferedFileReader(path) asks the location layer to probe
			 * @ref Device at @ref Setup. Initial @ref MaxMemory is 1 MiB.
			 * @c BufferedFileReader(path, read_ahead, max_memory) stores
			 * those values. @c read_ahead 0 disables prefetch.
			 *
			 * This leaf only opens, reads, seeks and reports the file length.
			 * @ref OriginDevice builds a @ref StormByte::System::Device from
			 * @ref Location.
			 *
			 * @see BufferedLocationReader, State
			 */
			class STORMBYTE_BUFFER_PUBLIC BufferedFileReader final: public BufferedLocationReader {
				public:
					/**
					 * @name Lifecycle
					 * @{
					 */

					/**
					 * @brief Store the path. @ref ReadAhead comes from @ref Setup.
					 * @param path Locator. Stored once as @ref Location.
					 */
					explicit BufferedFileReader(StormByte::String::String path);

					/**
					 * @brief Store the path and explicit knobs. Does not open.
					 * @param path Locator. Stored once as @ref Location.
					 * @param read_ahead Prefetch length. 0 disables prefetch.
					 * @param max_memory Cache cap. 0 stores no cache.
					 */
					BufferedFileReader(StormByte::String::String path,
						StormByte::ByteSize read_ahead, StormByte::ByteSize max_memory);

					/**
					 * @brief Copy constructor is deleted.
					 */
					BufferedFileReader(const BufferedFileReader&) = delete;

					/**
					 * @brief Move constructor.
					 * @param other Instance to take from.
					 */
					BufferedFileReader(BufferedFileReader&& other) noexcept;

					/**
					 * @brief Destructor. Calls @ref Close while the leaf vtable is live.
					 */
					~BufferedFileReader() noexcept override;

					/**
					 * @brief Copy assignment is deleted.
					 * @return *this.
					 */
					BufferedFileReader& operator=(const BufferedFileReader&) = delete;

					/**
					 * @brief Move assignment.
					 * @param other Instance to take from.
					 * @return *this.
					 */
					BufferedFileReader& operator=(BufferedFileReader&& other) noexcept;

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
					 * @brief Open the file as a binary input and cache its size.
					 * @return @ref IO::Status::Ok or @ref IO::Status::Failed.
					 */
					Result OriginOpen() override;

					/**
					 * @brief Close the file stream.
					 * @return @ref IO::Status::Ok.
					 */
					Result OriginClose() override;

					/**
					 * @brief Read up to @p n bytes from the file into @p dest.
					 * @param n Maximum bytes.
					 * @param dest Base-owned FIFO.
					 * @return Ok, End, Error or Failed.
					 */
					Result OriginPull(StormByte::ByteSize n, FIFO& dest) override;

					/**
					 * @brief Seek the file stream.
					 * @param offset Byte offset.
					 * @param mode Absolute from start or relative to the file cursor.
					 * @return @ref IO::Status::Ok or @ref IO::Status::Failed.
					 */
					Result OriginSeek(std::ptrdiff_t offset, Position mode) override;

					/**
					 * @brief Cached file size.
					 * @return Size in bytes, or empty if not open.
					 */
					std::optional<StormByte::ByteSize> OriginSize() const noexcept override;

				private:
					std::ifstream m_file;					///< Binary input stream.
					std::optional<StormByte::ByteSize> m_size;	///< Size after OriginOpen.
					mutable std::mutex m_file_mutex;		///< Serialises ifstream access.
			};
		}
	}
}
