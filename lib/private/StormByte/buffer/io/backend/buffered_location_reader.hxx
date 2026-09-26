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

#include <StormByte/buffer/visibility.h>

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
			 * @namespace StormByte::Buffer::IO::Backend
			 * @brief PIMPL coordinators for the public IO types.
			 */
			namespace Backend {
				/**
				 * @class BufferedLocationReader
				 * @brief Private state of @ref StormByte::Buffer::IO::BufferedLocationReader.
				 *
				 * Owns the probe flag. The locator lives on @ref IO::BufferedReader.
				 */
				class STORMBYTE_BUFFER_PRIVATE BufferedLocationReader {
					public:
						/**
						 * @brief Store whether @ref Setup probes the device.
						 * @param probe True when windows were not passed to the constructor.
						 */
						explicit BufferedLocationReader(bool probe);

						/**
						 * @brief Copy constructor is deleted.
						 */
						BufferedLocationReader(const BufferedLocationReader&) = delete;

						/**
						 * @brief Move constructor is deleted. The public class moves the pointer.
						 */
						BufferedLocationReader(BufferedLocationReader&&) = delete;

						/**
						 * @brief Release the location string in this module.
						 */
						~BufferedLocationReader();

						/**
						 * @brief Copy assignment is deleted.
						 */
						BufferedLocationReader& operator=(const BufferedLocationReader&) = delete;

						/**
						 * @brief Move assignment is deleted.
						 */
						BufferedLocationReader& operator=(BufferedLocationReader&&) = delete;

						/**
						 * @brief Whether @ref Setup should apply @ref Device::Window.
						 * @return True for the locator-only constructor.
						 */
						bool Probe() const noexcept;

					private:
						bool m_probe;	///< True when Setup reads the device window.
				};
			}
		}
	}
}
