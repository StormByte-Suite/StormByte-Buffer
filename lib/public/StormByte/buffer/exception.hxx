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
#include <StormByte/exception.hxx>
#include <StormByte/safe/string.hxx>

#include <format>
#include <string>
#include <string_view>
#include <utility>

/**
 * @namespace StormByte
 * @brief Root namespace of the StormByte suite.
 */
namespace StormByte {
	/**
	 * @namespace StormByte::Buffer
	 * @brief Buffer module of the StormByte suite.
	 */
	namespace Buffer {
		/**
		 * @class Exception
		 * @brief Root exception for Buffer. `what()` is `StormByte.Buffer: message`.
		 *
		 * Formatting runs in the caller and copies into Base-owned safe text.
		 * Buffer joins child segments in its provider before passing safe text to
		 * the out-of-line Base constructor. No caller string allocation is adopted.
		 *
		 * @see Error, ReadError, WriteError
		 */
		class STORMBYTE_BUFFER_PUBLIC Exception: public StormByte::Exception {
			public:
				/**
				 * @brief Format under `StormByte.Buffer`.
				 * @tparam Args Format argument types.
				 * @param fmt Format string.
				 * @param args Format arguments.
				 */
				template <typename... Args>
				STORMBYTE_FORCE_INLINE explicit Exception(std::format_string<Args...> fmt, Args&&... args)
					: Exception(Format(fmt, std::forward<Args>(args)...)) {}

				/**
				 * @brief Copy a borrowed plain message under `StormByte.Buffer` in the provider.
				 * @param message Exception text; copied during the call and never retained.
				 */
				explicit Exception(std::string_view message);

				/**
				 * @brief Copies Base-owned text under `StormByte.Buffer`.
				 * @param message Exception text.
				 */
				explicit Exception(const StormByte::Safe::String& message);

				/**
				 * @brief Copy Base-owned message storage through its provider.
				 * @param other Exception to copy.
				 */
				Exception(const Exception& other);

				/**
				 * @brief Transfer Base-owned message storage without allocating.
				 * @param other Exception to move from.
				 */
				Exception(Exception&& other) noexcept;

				/**
				 * @brief Copy Base-owned message storage through its provider.
				 * @param other Exception to copy.
				 * @return This exception.
				 */
				Exception& operator=(const Exception& other);

				/**
				 * @brief Transfer Base-owned message storage without allocating.
				 * @param other Exception to move from.
				 * @return This exception.
				 */
				Exception& operator=(Exception&& other) noexcept;

				/**
				 * @brief Destructor. Defined in this module so `catch` matches across a DLL.
				 */
				~Exception() noexcept override;

			protected:
				/**
				 * @brief Join a child path and plain message in the Buffer provider.
				 * @param child Joined segments under `Buffer`; empty selects `Buffer`.
				 * @param message Base-owned exception text.
				 */
				explicit Exception(const StormByte::Safe::String& child, const StormByte::Safe::String& message);

				/**
				 * @brief Format in the caller and copy the result into Base-owned text.
				 * @tparam Args Format argument types.
				 * @param fmt Format string; used verbatim when there are no arguments.
				 * @param args Format arguments.
				 * @return Base-owned formatted message.
				 */
				template <typename... Args>
				static STORMBYTE_FORCE_INLINE StormByte::Safe::String Format(std::format_string<Args...> fmt, Args&&... args) {
					if constexpr (sizeof...(Args) == 0)
						return StormByte::Safe::String(fmt.get());
					else {
						const std::string message = std::format(fmt, std::forward<Args>(args)...);
						return StormByte::Safe::String(std::string_view(message));
					}
				}

			private:
				/**
				 * @brief Compose the `Buffer.child` path in the Buffer provider.
				 * @param child Joined child segments, or empty for `Buffer`.
				 * @return Base-owned path consumed synchronously by the Base constructor.
				 */
				static StormByte::Safe::String Compose(const StormByte::Safe::String& child);
		};

		/**
		 * @class Error
		 * @brief General exception for buffer errors. Same path as @ref Exception.
		 *
		 * @see ReadError, WriteError
		 */
		class STORMBYTE_BUFFER_PUBLIC Error: public Exception {
			public:
				/**
				 * @brief Inherit the Buffer message constructors without adding a segment.
				 */
				using Exception::Exception;

				/**
				 * @brief Copy the message through the Base storage provider.
				 * @param other Error to copy.
				 */
				Error(const Error& other);

				/**
				 * @brief Transfer Base-owned message storage.
				 * @param other Error to move from.
				 */
				Error(Error&& other) noexcept;

				/**
				 * @brief Copy the message through the Base storage provider.
				 * @param other Error to copy.
				 * @return This error.
				 */
				Error& operator=(const Error& other);

				/**
				 * @brief Transfer Base-owned message storage.
				 * @param other Error to move from.
				 * @return This error.
				 */
				Error& operator=(Error&& other) noexcept;

				/**
				 * @brief Destructor. Defined in this module so `catch` matches across a DLL.
				 */
				~Error() noexcept override;
		};

		/**
		 * @class ReadError
		 * @brief Read, extract or peek failed. `what()` is `StormByte.Buffer.Read: message`.
		 */
		class STORMBYTE_BUFFER_PUBLIC ReadError: public Error {
			public:
				/**
				 * @brief Format under `StormByte.Buffer.Read`.
				 * @tparam Args Format argument types.
				 * @param fmt Format string.
				 * @param args Format arguments.
				 */
				template <typename... Args>
				STORMBYTE_FORCE_INLINE explicit ReadError(std::format_string<Args...> fmt, Args&&... args)
					: ReadError(Format(fmt, std::forward<Args>(args)...)) {}

				/**
				 * @brief Copy a borrowed plain message under `StormByte.Buffer.Read` in the provider.
				 * @param message Exception text; copied during the call and never retained.
				 */
				explicit ReadError(std::string_view message);

				/**
				 * @brief Copies Base-owned text under `StormByte.Buffer.Read`.
				 * @param message Exception text.
				 */
				explicit ReadError(const StormByte::Safe::String& message);

				/**
				 * @brief Copy the message through the Base storage provider.
				 * @param other Error to copy.
				 */
				ReadError(const ReadError& other);

				/**
				 * @brief Transfer Base-owned message storage.
				 * @param other Error to move from.
				 */
				ReadError(ReadError&& other) noexcept;

				/**
				 * @brief Copy the message through the Base storage provider.
				 * @param other Error to copy.
				 * @return This error.
				 */
				ReadError& operator=(const ReadError& other);

				/**
				 * @brief Transfer Base-owned message storage.
				 * @param other Error to move from.
				 * @return This error.
				 */
				ReadError& operator=(ReadError&& other) noexcept;

				/**
				 * @brief Destructor. Defined in this module so `catch` matches across a DLL.
				 */
				~ReadError() noexcept override;
		};

		/**
		 * @class WriteError
		 * @brief Write failed. `what()` is `StormByte.Buffer.Write: message`.
		 */
		class STORMBYTE_BUFFER_PUBLIC WriteError: public Error {
			public:
				/**
				 * @brief Format under `StormByte.Buffer.Write`.
				 * @tparam Args Format argument types.
				 * @param fmt Format string.
				 * @param args Format arguments.
				 */
				template <typename... Args>
				STORMBYTE_FORCE_INLINE explicit WriteError(std::format_string<Args...> fmt, Args&&... args)
					: WriteError(Format(fmt, std::forward<Args>(args)...)) {}

				/**
				 * @brief Copy a borrowed plain message under `StormByte.Buffer.Write` in the provider.
				 * @param message Exception text; copied during the call and never retained.
				 */
				explicit WriteError(std::string_view message);

				/**
				 * @brief Copies Base-owned text under `StormByte.Buffer.Write`.
				 * @param message Exception text.
				 */
				explicit WriteError(const StormByte::Safe::String& message);

				/**
				 * @brief Copy the message through the Base storage provider.
				 * @param other Error to copy.
				 */
				WriteError(const WriteError& other);

				/**
				 * @brief Transfer Base-owned message storage.
				 * @param other Error to move from.
				 */
				WriteError(WriteError&& other) noexcept;

				/**
				 * @brief Copy the message through the Base storage provider.
				 * @param other Error to copy.
				 * @return This error.
				 */
				WriteError& operator=(const WriteError& other);

				/**
				 * @brief Transfer Base-owned message storage.
				 * @param other Error to move from.
				 * @return This error.
				 */
				WriteError& operator=(WriteError&& other) noexcept;

				/**
				 * @brief Destructor. Defined in this module so `catch` matches across a DLL.
				 */
				~WriteError() noexcept override;
		};
	}
}
