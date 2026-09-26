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
#include <StormByte/byte_size.hxx>

#include <string_view>

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
			 * @enum Status
			 * @brief Outcome of a single I/O call.
			 *
			 * @c TryAgain is backpressure or a bounded wait. It is not POSIX EAGAIN.
			 *
			 * @see Result, State
			 */
			enum class STORMBYTE_BUFFER_PUBLIC Status {
				Ok,			///< The call completed as requested (see @ref Result::count).
				End,		///< Origin exhausted; @ref Result::count may be short.
				Error,		///< Origin failed during the call; buffers untouched.
				Failed,		///< Illegal call or dead session; buffers untouched.
				TryAgain	///< Not accepted; buffers untouched. State stays Idle.
			};

			/**
			 * @enum State
			 * @brief Session state of a coordinated reader or writer.
			 *
			 * Distinct from @ref Status, which is per-call.
			 */
			enum class STORMBYTE_BUFFER_PUBLIC State {
				Idle,			///< Armed.
				Missing,		///< Last Open: path or parent does not exist.
				Directory,		///< Last Open: path is a directory.
				Permission,		///< Last Open: exists; read open denied.
				NotWritable,	///< Last Open or mid-write: cannot write.
				Fault,			///< Origin I/O error mid-transfer.
				Unavailable		///< Ctor, after Close, or origin gone.
			};

			/**
			 * @brief Enumerator name of @p status.
			 * @param status Per-call status.
			 * @return Stable name, or empty if unknown.
			 */
			[[nodiscard]] constexpr std::string_view ToString(Status status) noexcept {
				switch (status) {
					case Status::Ok:		return "Ok";
					case Status::End:		return "End";
					case Status::Error:		return "Error";
					case Status::Failed:	return "Failed";
					case Status::TryAgain:	return "TryAgain";
				}
				return {};
			}

			/**
			 * @brief Enumerator name of @p state.
			 * @param state Session state.
			 * @return Stable name, or empty if unknown.
			 */
			[[nodiscard]] constexpr std::string_view ToString(State state) noexcept {
				switch (state) {
					case State::Idle:			return "Idle";
					case State::Missing:		return "Missing";
					case State::Directory:		return "Directory";
					case State::Permission:		return "Permission";
					case State::NotWritable:	return "NotWritable";
					case State::Fault:			return "Fault";
					case State::Unavailable:	return "Unavailable";
				}
				return {};
			}

			/**
			 * @struct Result
			 * @brief Per-call status plus how many bytes this call transferred.
			 *
			 * @c count is 0 when the caller buffer was not consumed
			 * (@ref Status::Failed, @ref Status::Error, @ref Status::TryAgain,
			 * or @ref Status::End with no remaining bytes).
			 *
			 * @see Status
			 */
			struct STORMBYTE_BUFFER_PUBLIC Result {
				Status status;					///< Outcome of the call.
				StormByte::ByteSize count;			///< Bytes transferred this call.
			};

			/**
			 * @namespace StormByte::Buffer::IO::Drainer
			 * @brief Status and operations of a Bridge pump thread.
			 */
			namespace Drainer {
				/**
				 * @enum Status
				 * @brief Whether the Bridge worker is pumping.
				 *
				 * @c Stopped is moved-from or no backend. There is no operation
				 * that stops a live Bridge; the destructor joins the thread.
				 */
				enum class STORMBYTE_BUFFER_PUBLIC Status {
					Started,	///< Worker may pull when there is room.
					Paused,		///< Worker does not pull. Ctor if high_water is 0.
					Stopped		///< No backend.
				};

				/**
				 * @enum Operation
				 * @brief Verbs for @ref StormByte::Buffer::Bridge::Drainer.
				 */
				enum class STORMBYTE_BUFFER_PUBLIC Operation {
					Toggle,		///< Started ↔ Paused.
					Flush		///< Push what is already held. No destination flush.
				};

				/**
				 * @brief Enumerator name of @p status.
				 * @param status Drainer status.
				 * @return Stable name, or empty if unknown.
				 */
				[[nodiscard]] constexpr std::string_view ToString(Status status) noexcept {
					switch (status) {
						case Status::Started:	return "Started";
						case Status::Paused:	return "Paused";
						case Status::Stopped:	return "Stopped";
					}
					return {};
				}

				/**
				 * @brief Enumerator name of @p operation.
				 * @param operation Drainer operation.
				 * @return Stable name, or empty if unknown.
				 */
				[[nodiscard]] constexpr std::string_view ToString(Operation operation) noexcept {
					switch (operation) {
						case Operation::Toggle:	return "Toggle";
						case Operation::Flush:	return "Flush";
					}
					return {};
				}
			}
		}
	}
}
