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

#include <StormByte/buffer/io/buffered_writer.hxx>
#include <StormByte/buffer/visibility.h>
#include <StormByte/system/device.hxx>

#include <filesystem>
#include <fstream>
#include <memory>
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
			 * @brief @ref BufferedWriter leaf over a filesystem file.
			 *
			 * Binary file, random-access. Open creates the file when
			 * missing and leaves existing content. Overwrite is
			 * @ref Truncate. Does not open in the constructor. Does
			 * not create parent directories. Not sealed.
			 *
			 * The device does not change after construction: @ref WriteChunk,
			 * @ref BackPressure and @ref MaxMemory are chosen once.
			 * @ref MaxWait stays dynamic.
			 *
			 * @par Constructors
			 * @c BufferedFileWriter(path) defers chunk to @ref Setup
			 * (`CreateDevice` + @ref StormByte::System::Device::Window).
			 * Path-only @ref BackPressure is 4. Path-only @ref MaxMemory
			 * is 1 MiB. @c BufferedFileWriter(path, write_chunk,
			 * back_pressure, max_wait) stores those values and leaves
			 * @ref MaxMemory at 0. @c BufferedFileWriter(path,
			 * write_chunk, max_memory, back_pressure, max_wait) stores
			 * the page budget too. A zero chunk or backpressure disables
			 * the ring. A zero @ref MaxMemory stores no pages.
			 *
			 * @ref Seek is the base implementation: logical cursor only.
			 * This leaf supplies @ref OriginSeek for GC / Flush / Close.
			 *
			 * A derived class may override any @c Origin* hook, @ref Seek,
			 * @ref Size, @ref WillWrite, @ref Setup and @ref CreateDevice.
			 * Override @ref CreateDevice to supply a
			 * @ref StormByte::System::Device derivative; File only reads
			 * measurement and Window. When the transport is still this file,
			 * call the File implementation and then add behaviour. When it
			 * is not, do not call these File implementations.
			 *
			 * The derived destructor must call @ref Close first.
			 * File @ref Close is idempotent.
			 *
			 * @see BufferedWriter, State, StormByte::System::Device
			 */
			class STORMBYTE_BUFFER_PUBLIC BufferedFileWriter: public BufferedWriter {
				public:
					/**
					 * @name Lifecycle
					 * @{
					 */

					/**
					 * @brief Store the path. Chunk, backpressure and MaxMemory come from @ref Setup.
					 * @param path Filesystem path.
					 */
					explicit BufferedFileWriter(std::filesystem::path path);

					/**
					 * @brief Store the path and explicit ring knobs. Does not open.
					 * @param path Filesystem path.
					 * @param write_chunk Initial @ref WriteChunk in bytes.
					 * @param back_pressure Initial @ref BackPressure in chunks.
					 * @param max_wait Initial @ref MaxWait.
					 *
					 * @ref MaxMemory stays 0.
					 */
					BufferedFileWriter(std::filesystem::path path,
						StormByte::ByteSize write_chunk, std::size_t back_pressure,
						std::chrono::milliseconds max_wait = std::chrono::milliseconds{0});

					/**
					 * @brief Store the path, page budget and ring knobs. Does not open.
					 * @param path Filesystem path.
					 * @param write_chunk Initial @ref WriteChunk in bytes.
					 * @param max_memory Initial @ref MaxMemory in bytes.
					 * @param back_pressure Initial @ref BackPressure in chunks.
					 * @param max_wait Initial @ref MaxWait.
					 */
					BufferedFileWriter(std::filesystem::path path,
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

					/**
					 * @brief Path passed to the constructor.
					 * @return Stored path (not resolved).
					 */
					virtual const std::filesystem::path& Path() const noexcept;

					/**
					 * @brief On-disk size or the write cursor, whichever is larger.
					 * @return Byte length.
					 */
					virtual StormByte::ByteSize Size() const noexcept;

				protected:
					/**
					 * @brief Device used for path-only @ref Setup knobs.
					 * @return Owned @ref StormByte::System::Device (or a derivative).
					 *
					 * Default is a @ref StormByte::System::Device on @c m_path.
					 * A derived writer returns its own type; File does not slice.
					 */
					virtual std::unique_ptr<StormByte::System::Device> CreateDevice() const;

					/**
					 * @brief Apply device chunk, backpressure and MaxMemory for the path-only ctor.
					 *
					 * No-op when the explicit constructor already set those knobs
					 * (including 0 = ring off). A failed Device probe leaves
					 * @ref WriteChunk at zero. Path-only @ref BackPressure stays 4.
					 * Path-only @ref MaxMemory stays 1 MiB.
					 */
					virtual void Setup() override;

					/**
					 * @brief Open @c m_path for binary random-access write.
					 * @return @ref Status::Ok or @ref Status::Failed.
					 *
					 * Creates the file when missing. Does not truncate an
					 * existing file.
					 */
					virtual Result OriginOpen() override;

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
					virtual Result OriginSeek(StormByte::ByteSize absolute);

					/**
					 * @brief Whether the volume looks able to accept @p n more bytes.
					 * @param n Candidate write.
					 * @return false when the base writer refuses or free space is short.
					 */
					virtual bool WillWrite(StormByte::ByteSize n) const override;

				private:
					std::filesystem::path m_path;			///< Path given at construction.
					std::ofstream m_file;					///< Binary output stream.
					mutable std::mutex m_file_mutex;		///< Serialises ofstream access.
					bool m_probe_on_setup;					///< True for the path-only constructor.
			};
		}
	}
}
