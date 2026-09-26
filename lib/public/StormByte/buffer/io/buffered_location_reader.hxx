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
#include <StormByte/string/string.hxx>
#include <StormByte/system/device.hxx>

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
				class BufferedLocationReader;
			}

			/**
			 * @class BufferedLocationReader
			 * @brief File-like @ref BufferedReader.
			 *
			 * A location has a name, a cursor and a length. A file and anything
			 * that acts as a file inherit this. A byte stream that cannot seek
			 * or report a length inherits @ref BufferedReader instead.
			 *
			 * @ref IsSeekable and @ref IsSized are always true. The leaf does
			 * not implement those hooks.
			 *
			 * Public @ref Size stays @c optional on @ref BufferedReader so a
			 * @c BufferedReader& is unchanged. On this type @ref IsSized is
			 * true; the value is whatever @ref OriginSize reports.
			 *
			 * @ref Location is owned here (@c String), in this module.
			 * @ref Device is not virtual. @ref OriginDevice is pure.
			 * @ref Setup applies @ref StormByte::System::Device::Window when
			 * the constructor did not pass windows.
			 *
			 * The leaf implements @ref OriginOpen, @ref OriginClose,
			 * @ref OriginPull, @ref OriginSeek, @ref OriginSize and
			 * @ref OriginDevice. It does not touch the cache.
			 *
			 * @see BufferedReader, BufferedFileReader
			 */
			class STORMBYTE_BUFFER_PUBLIC BufferedLocationReader: public BufferedReader {
				public:
					/**
					 * @brief Copy constructor is deleted.
					 */
					BufferedLocationReader(const BufferedLocationReader&) = delete;

					/**
					 * @brief Move constructor. Transfers the location. Moved-from has none.
					 * @param other Instance to take from.
					 */
					BufferedLocationReader(BufferedLocationReader&& other) noexcept;

					/**
					 * @brief Destructor. Releases the location in this module.
					 */
					~BufferedLocationReader() noexcept override;

					/**
					 * @brief Copy assignment is deleted.
					 * @return *this.
					 */
					BufferedLocationReader& operator=(const BufferedLocationReader&) = delete;

					/**
					 * @brief Move assignment.
					 * @param other Instance to take from.
					 * @return *this.
					 */
					BufferedLocationReader& operator=(BufferedLocationReader&& other) noexcept;

					/**
					 * @brief Locator passed to the constructor. Stored once.
					 * @return Owned text. Not resolved. Empty if moved-from.
					 */
					const StormByte::String::String& Location() const noexcept;

					/**
					 * @brief Same stored locator as @ref Location.
					 * @return @ref Location. Not a copy.
					 */
					const StormByte::String::String& Path() const noexcept;

					/**
					 * @brief Measurement of this location.
					 * @return @ref OriginDevice by value. Not a pointer.
					 */
					StormByte::System::Device Device() const;

				protected:
					/**
					 * @brief Store the locator and forward the cache knobs.
					 * @param location Owned locator.
					 * @param read_ahead Initial @ref ReadAhead. Ignored when @p probe is true.
					 * @param max_memory Initial @ref MaxMemory.
					 * @param probe When true, @ref Setup replaces @ref ReadAhead from @ref Device.
					 */
					BufferedLocationReader(StormByte::String::String location,
						StormByte::ByteSize read_ahead, StormByte::ByteSize max_memory, bool probe);

					/**
					 * @brief Leaf measurement. Not a filesystem type.
					 * @return Device built by the leaf. Returned by value.
					 */
					virtual StormByte::System::Device OriginDevice() const = 0;

					/**
					 * @brief A location can seek.
					 * @return @c true.
					 */
					bool OriginCanSeek() const noexcept final;

					/**
					 * @brief A location has a length.
					 * @return @c true.
					 */
					bool OriginHasSize() const noexcept final;

					/**
					 * @brief Apply the device read window when the constructor asked for a probe.
					 */
					void Setup() final;

				private:
					std::unique_ptr<Backend::BufferedLocationReader> m_io;	///< Location string and probe flag.
			};
		}
	}
}
