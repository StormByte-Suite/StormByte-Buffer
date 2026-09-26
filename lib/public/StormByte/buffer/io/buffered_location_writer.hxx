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
#include <StormByte/string/string.hxx>
#include <StormByte/system/device.hxx>

#include <chrono>
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
		/**
		 * @namespace StormByte::Buffer::IO
		 * @brief Buffered binary sources and sinks.
		 */
		namespace IO {
			namespace Backend {
				class BufferedLocationWriter;
			}

			/**
			 * @class BufferedLocationWriter
			 * @brief File-like @ref BufferedWriter.
			 *
			 * A location has a name, a cursor and a length. A file and anything
			 * that acts as a file inherit this. A byte stream that cannot seek
			 * or report a length inherits @ref BufferedWriter instead.
			 *
			 * @ref IsSeekable and @ref IsSized are always true.
			 * @ref Size is @ref OriginSize. The leaf does not override @ref Size.
			 * @ref OriginSeek is pure: the base default that only fails is not enough.
			 *
			 * @ref Path and @ref Location live on @ref BufferedWriter. They are
			 * stored once and do not change. @ref Path may be a filesystem
			 * path, @c socket://… or @c http://… . A file leaf passes
			 * @ref Location::Local and its path is a local filesystem path.
			 *
			 * @ref Device is not virtual. @ref OriginDevice is pure.
			 * @ref Setup applies @ref StormByte::System::Device::Window,
			 * backpressure 4 and 1 MiB of @ref MaxMemory when the constructor
			 * did not pass windows.
			 *
			 * The leaf implements @ref OriginOpen, @ref OriginClose,
			 * @ref OriginPush, @ref OriginFlush, @ref OriginTruncate,
			 * @ref OriginSeek, @ref OriginSize and @ref OriginDevice.
			 *
			 * @see BufferedWriter, BufferedFileWriter
			 */
			class STORMBYTE_BUFFER_PUBLIC BufferedLocationWriter: public BufferedWriter {
				public:
					/**
					 * @brief Copy constructor is deleted.
					 */
					BufferedLocationWriter(const BufferedLocationWriter&) = delete;

					/**
					 * @brief Move constructor. Transfers the location. Moved-from has none.
					 * @param other Instance to take from.
					 */
					BufferedLocationWriter(BufferedLocationWriter&& other) noexcept;

					/**
					 * @brief Destructor. Releases the location in this module.
					 */
					~BufferedLocationWriter() noexcept override;

					/**
					 * @brief Copy assignment is deleted.
					 * @return *this.
					 */
					BufferedLocationWriter& operator=(const BufferedLocationWriter&) = delete;

					/**
					 * @brief Move assignment.
					 * @param other Instance to take from.
					 * @return *this.
					 */
					BufferedLocationWriter& operator=(BufferedLocationWriter&& other) noexcept;

					/**
					 * @brief Measurement of this location.
					 * @return @ref OriginDevice by value. Not a pointer.
					 */
					StormByte::System::Device Device() const;

					/**
					 * @brief This location can seek.
					 * @return @c true.
					 */
					bool IsSeekable() const noexcept;

					/**
					 * @brief This location has a length.
					 * @return @c true.
					 */
					bool IsSized() const noexcept;

					/**
					 * @brief Length from the leaf.
					 * @return @ref OriginSize.
					 */
					StormByte::ByteSize Size() const noexcept final;

				protected:
					/**
					 * @brief Forward the locator and the sink knobs.
					 * @param path Locator. Stored once on @ref BufferedWriter.
					 * @param location @ref Location::Local or @ref Location::Remote. Stored once.
					 * @param write_chunk Initial @ref WriteChunk. Ignored when @p probe is true.
					 * @param back_pressure Initial @ref BackPressure.
					 * @param max_wait Initial @ref MaxWait.
					 * @param max_memory Initial @ref MaxMemory. Ignored when @p probe is true.
					 * @param probe When true, @ref Setup replaces chunk, backpressure and MaxMemory.
					 */
					BufferedLocationWriter(StormByte::String::String path, enum Location location,
						StormByte::ByteSize write_chunk, std::size_t back_pressure,
						std::chrono::milliseconds max_wait, StormByte::ByteSize max_memory, bool probe);

					/**
					 * @brief Leaf measurement. Not a filesystem type.
					 * @return Device built by the leaf. Returned by value.
					 */
					virtual StormByte::System::Device OriginDevice() const = 0;

					/**
					 * @brief Length the leaf can answer. Not optional.
					 * @return Byte length. A missing target is 0.
					 */
					virtual StormByte::ByteSize OriginSize() const noexcept = 0;

					/**
					 * @brief Seek the origin to an absolute offset. Required.
					 * @param absolute Byte offset from the start.
					 * @return @ref Status::Ok or @ref Status::Failed.
					 */
					virtual Result OriginSeek(StormByte::ByteSize absolute) = 0;

					/**
					 * @brief Apply the device write window when the constructor asked for a probe.
					 */
					void Setup() final;

				private:
					std::unique_ptr<Backend::BufferedLocationWriter> m_io;	///< Location string and probe flag.
			};
		}
	}
}
