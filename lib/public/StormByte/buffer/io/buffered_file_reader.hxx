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

#include <StormByte/buffer/io/buffered_reader.hxx>
#include <StormByte/buffer/visibility.h>
#include <StormByte/system/device.hxx>

#include <filesystem>
#include <fstream>
#include <memory>
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
			 * @brief @ref BufferedReader leaf over a filesystem file.
			 *
			 * Binary `ifstream` only. Hooks call @ref SetState.
			 * Does not open in the constructor. Not sealed.
			 *
			 * The device does not change after construction: @ref ReadAhead
			 * is chosen once. Cache size (@ref MaxMemory) stays dynamic.
			 *
			 * @par Constructors
			 * @c BufferedFileReader(path) defers @ref ReadAhead to
			 * @ref Setup (`CreateDevice` + @ref StormByte::System::Device::Window).
			 * Initial @ref MaxMemory is 1 MiB.
			 * @c BufferedFileReader(path, read_ahead, max_memory) stores
			 * those values. @c read_ahead 0 disables prefetch.
			 *
			 * A derived class may override any @c Origin* hook,
			 * @ref Setup and @ref CreateDevice. Override @ref CreateDevice
			 * to supply a @ref StormByte::System::Device derivative; File
			 * only reads measurement and Window. When the transport is still
			 * this file, call the File implementation and then add behaviour.
			 * When it is not, do not call these File implementations.
			 * Prefetch and Seek stay in @ref BufferedReader.
			 *
			 * The derived destructor must call @ref Close first.
			 * File @ref Close is idempotent.
			 *
			 * @see BufferedReader, State, StormByte::System::Device
			 */
			class STORMBYTE_BUFFER_PUBLIC BufferedFileReader: public BufferedReader {
				public:
					/**
					 * @name Lifecycle
					 * @{
					 */

					/**
					 * @brief Store the path. @ref ReadAhead comes from @ref Setup.
					 * @param path Filesystem path.
					 */
					explicit BufferedFileReader(std::filesystem::path path);

					/**
					 * @brief Store the path and explicit knobs. Does not open.
					 * @param path Filesystem path.
					 * @param read_ahead Prefetch length. 0 disables prefetch.
					 * @param max_memory Cache cap. 0 stores no cache.
					 */
					BufferedFileReader(std::filesystem::path path,
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

					/**
					 * @brief Path passed to the constructor.
					 * @return Stored path (not resolved).
					 */
					virtual const std::filesystem::path& Path() const noexcept;

				protected:
					/**
					 * @brief Device used for path-only @ref Setup knobs.
					 * @return Owned @ref StormByte::System::Device (or a derivative).
					 *
					 * Default is a @ref StormByte::System::Device on @c m_path.
					 * A derived reader returns its own type; File does not slice.
					 */
					virtual std::unique_ptr<StormByte::System::Device> CreateDevice() const;

					/**
					 * @brief Apply device @ref ReadAhead when constructed from path only.
					 *
					 * No-op when the three-argument constructor already set
					 * an explicit prefetch length (including 0 = off).
					 * A failed Device probe leaves @ref ReadAhead at zero.
					 */
					void Setup() override;

					/**
					 * @brief Open @c m_path as a binary input file and cache its size.
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
					 * @brief Files are seekable.
					 * @return @c true.
					 */
					bool OriginCanSeek() const noexcept override;

					/**
					 * @brief Seek the file stream.
					 * @param offset Byte offset.
					 * @param mode Absolute from start or relative to the file cursor.
					 * @return @ref IO::Status::Ok or @ref IO::Status::Failed.
					 */
					Result OriginSeek(std::ptrdiff_t offset, Position mode) override;

					/**
					 * @brief Length is known after a successful @ref OriginOpen.
					 * @return @c true when @c m_size is set.
					 */
					bool OriginHasSize() const noexcept override;

					/**
					 * @brief Cached file size.
					 * @return Size in bytes, or empty if not open.
					 */
					std::optional<StormByte::ByteSize> OriginSize() const noexcept override;

				private:
					std::filesystem::path m_path;			///< Path given at construction.
					std::ifstream m_file;					///< Binary input stream.
					std::optional<StormByte::ByteSize> m_size;	///< Size after OriginOpen.
					mutable std::mutex m_file_mutex;		///< Serialises ifstream access.
					bool m_probe_on_setup;					///< True for the path-only constructor.
			};
		}
	}
}
