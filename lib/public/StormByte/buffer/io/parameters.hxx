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

#include <StormByte/byte_size.hxx>
#include <StormByte/platform.h>

#include <chrono>
#include <cstddef>
#include <optional>
#include <utility>

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
		 *
		 * Knob types and parameter bags are header-only. Every function is
		 * @c STORMBYTE_FORCE_INLINE so the compiler cannot emit a call into
		 * the StormByte-Buffer DLL. @c std::optional and the variadic pack
		 * stay in the caller. The DLL receives only resolved @c ByteSize,
		 * @c std::size_t and @c milliseconds.
		 */
		namespace IO {
			/**
			 * @class ReadAhead
			 * @brief Prefetch length knob. 0 disables prefetch.
			 */
			class ReadAhead {
				public:
					/**
					 * @brief Store the prefetch length.
					 * @param value Bytes. 0 disables prefetch.
					 */
					STORMBYTE_FORCE_INLINE explicit ReadAhead(const StormByte::ByteSize value) noexcept:
						m_value(value) {}

					/**
					 * @brief Prefetch length.
					 * @return Bytes.
					 */
					STORMBYTE_FORCE_INLINE StormByte::ByteSize Value() const noexcept {
						return m_value;
					}

				private:
					StormByte::ByteSize m_value;	///< Prefetch length.
			};

			/**
			 * @class MaxMemory
			 * @brief Cache / dirty-page budget knob. 0 stores no pages.
			 */
			class MaxMemory {
				public:
					/**
					 * @brief Store the memory budget.
					 * @param value Bytes. 0 stores no pages.
					 */
					STORMBYTE_FORCE_INLINE explicit MaxMemory(const StormByte::ByteSize value) noexcept:
						m_value(value) {}

					/**
					 * @brief Memory budget.
					 * @return Bytes.
					 */
					STORMBYTE_FORCE_INLINE StormByte::ByteSize Value() const noexcept {
						return m_value;
					}

				private:
					StormByte::ByteSize m_value;	///< Memory budget.
			};

			/**
			 * @class MaxWait
			 * @brief Origin wait cap. 0ms waits without limit.
			 */
			class MaxWait {
				public:
					/**
					 * @brief Store the wait cap.
					 * @param value 0ms waits without limit.
					 */
					STORMBYTE_FORCE_INLINE explicit MaxWait(const std::chrono::milliseconds value) noexcept:
						m_value(value) {}

					/**
					 * @brief Wait cap.
					 * @return Duration.
					 */
					STORMBYTE_FORCE_INLINE std::chrono::milliseconds Value() const noexcept {
						return m_value;
					}

				private:
					std::chrono::milliseconds m_value;	///< Wait cap.
			};

			/**
			 * @class WriteChunk
			 * @brief Origin push unit. 0 disables the writer ring (with BackPressure 0).
			 */
			class WriteChunk {
				public:
					/**
					 * @brief Store the chunk size.
					 * @param value Bytes. 0 disables the ring.
					 */
					STORMBYTE_FORCE_INLINE explicit WriteChunk(const StormByte::ByteSize value) noexcept:
						m_value(value) {}

					/**
					 * @brief Chunk size.
					 * @return Bytes.
					 */
					STORMBYTE_FORCE_INLINE StormByte::ByteSize Value() const noexcept {
						return m_value;
					}

				private:
					StormByte::ByteSize m_value;	///< Chunk size.
			};

			/**
			 * @class BackPressure
			 * @brief Writer ring cap in WriteChunk units. 0 disables the ring.
			 */
			class BackPressure {
				public:
					/**
					 * @brief Store the cap in chunks.
					 * @param value Chunk count. 0 disables the ring.
					 */
					STORMBYTE_FORCE_INLINE explicit BackPressure(const std::size_t value) noexcept:
						m_value(value) {}

					/**
					 * @brief Cap in chunks.
					 * @return Chunk count.
					 */
					STORMBYTE_FORCE_INLINE std::size_t Value() const noexcept {
						return m_value;
					}

				private:
					std::size_t m_value;	///< Chunk count.
			};

			/**
			 * @class ReaderParameters
			 * @brief Optional reader knobs. A missing field keeps the current default.
			 *
			 * Nested @c Parameters on @ref BufferedReader and its children
			 * inherit this type. Not a DLL type.
			 */
			class ReaderParameters {
				public:
					/**
					 * @brief No knobs. Every field is absent.
					 */
					STORMBYTE_FORCE_INLINE ReaderParameters() noexcept = default;

					/**
					 * @brief Engage the listed knobs. Unknown types do not compile.
					 * @tparam Knobs @ref ReadAhead, @ref MaxMemory and/or @ref MaxWait.
					 * @param knobs Values to store.
					 */
					template<typename... Knobs>
					STORMBYTE_FORCE_INLINE ReaderParameters(Knobs... knobs) {
						(Apply(std::move(knobs)), ...);
					}

					/**
					 * @brief Prefetch length when the caller set it.
					 * @return Empty when the caller omitted @ref ReadAhead.
					 */
					STORMBYTE_FORCE_INLINE const std::optional<StormByte::ByteSize>& ReadAhead() const noexcept {
						return m_read_ahead;
					}

					/**
					 * @brief Cache cap when the caller set it.
					 * @return Empty when the caller omitted @ref MaxMemory.
					 */
					STORMBYTE_FORCE_INLINE const std::optional<StormByte::ByteSize>& MaxMemory() const noexcept {
						return m_max_memory;
					}

					/**
					 * @brief Wait cap when the caller set it.
					 * @return Empty when the caller omitted @ref MaxWait.
					 */
					STORMBYTE_FORCE_INLINE const std::optional<std::chrono::milliseconds>& MaxWait() const noexcept {
						return m_max_wait;
					}

				private:
					STORMBYTE_FORCE_INLINE void Apply(class ReadAhead knob) noexcept {
						m_read_ahead = knob.Value();
					}

					STORMBYTE_FORCE_INLINE void Apply(class MaxMemory knob) noexcept {
						m_max_memory = knob.Value();
					}

					STORMBYTE_FORCE_INLINE void Apply(class MaxWait knob) noexcept {
						m_max_wait = knob.Value();
					}

					template<typename Knob>
					void Apply(Knob&&) = delete;

					std::optional<StormByte::ByteSize> m_read_ahead;				///< Absent = default.
					std::optional<StormByte::ByteSize> m_max_memory;				///< Absent = default.
					std::optional<std::chrono::milliseconds> m_max_wait;			///< Absent = default.
			};

			/**
			 * @class WriterParameters
			 * @brief Optional writer knobs. A missing field keeps the current default.
			 *
			 * Nested @c Parameters on @ref BufferedWriter and its children
			 * inherit this type. Not a DLL type.
			 */
			class WriterParameters {
				public:
					/**
					 * @brief No knobs. Every field is absent.
					 */
					STORMBYTE_FORCE_INLINE WriterParameters() noexcept = default;

					/**
					 * @brief Engage the listed knobs. Unknown types do not compile.
					 * @tparam Knobs @ref WriteChunk, @ref BackPressure, @ref MaxMemory and/or @ref MaxWait.
					 * @param knobs Values to store.
					 */
					template<typename... Knobs>
					STORMBYTE_FORCE_INLINE WriterParameters(Knobs... knobs) {
						(Apply(std::move(knobs)), ...);
					}

					/**
					 * @brief Chunk size when the caller set it.
					 * @return Empty when the caller omitted @ref WriteChunk.
					 */
					STORMBYTE_FORCE_INLINE const std::optional<StormByte::ByteSize>& WriteChunk() const noexcept {
						return m_write_chunk;
					}

					/**
					 * @brief Backpressure when the caller set it.
					 * @return Empty when the caller omitted @ref BackPressure.
					 */
					STORMBYTE_FORCE_INLINE const std::optional<std::size_t>& BackPressure() const noexcept {
						return m_back_pressure;
					}

					/**
					 * @brief Dirty-page budget when the caller set it.
					 * @return Empty when the caller omitted @ref MaxMemory.
					 */
					STORMBYTE_FORCE_INLINE const std::optional<StormByte::ByteSize>& MaxMemory() const noexcept {
						return m_max_memory;
					}

					/**
					 * @brief Wait cap when the caller set it.
					 * @return Empty when the caller omitted @ref MaxWait.
					 */
					STORMBYTE_FORCE_INLINE const std::optional<std::chrono::milliseconds>& MaxWait() const noexcept {
						return m_max_wait;
					}

				private:
					STORMBYTE_FORCE_INLINE void Apply(class WriteChunk knob) noexcept {
						m_write_chunk = knob.Value();
					}

					STORMBYTE_FORCE_INLINE void Apply(class BackPressure knob) noexcept {
						m_back_pressure = knob.Value();
					}

					STORMBYTE_FORCE_INLINE void Apply(class MaxMemory knob) noexcept {
						m_max_memory = knob.Value();
					}

					STORMBYTE_FORCE_INLINE void Apply(class MaxWait knob) noexcept {
						m_max_wait = knob.Value();
					}

					template<typename Knob>
					void Apply(Knob&&) = delete;

					std::optional<StormByte::ByteSize> m_write_chunk;				///< Absent = default.
					std::optional<std::size_t> m_back_pressure;						///< Absent = default.
					std::optional<StormByte::ByteSize> m_max_memory;				///< Absent = default.
					std::optional<std::chrono::milliseconds> m_max_wait;			///< Absent = default.
			};
		}
	}
}
