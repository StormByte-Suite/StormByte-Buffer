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

#include <StormByte/type_traits/safe.hxx>

#include <StormByte/buffer/io/buffered_location_writer.hxx>
#include <StormByte/buffer/visibility.h>

#include <chrono>
#include <fstream>
#include <mutex>
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
			 * backpressure and @ref MaxMemory come from
			 * @ref BufferedLocationWriter::Setup when those knobs are
			 * omitted.
			 *
			 * @par Constructors
			 * @c BufferedFileWriter(path) and @c BufferedFileWriter(path, {})
			 * ask the location layer to probe.
			 * @c BufferedFileWriter(path, { WriteChunk(n), BackPressure(k) })
			 * stores those values and leaves @ref MaxMemory at 0.
			 * Adding @c MaxMemory(m) stores the page budget too.
			 * A zero chunk or backpressure disables the ring.
			 * A zero @ref MaxMemory stores no pages.
			 *
			 * The constructor resolves Safe-owned @c Parameters into byte counts,
			 * a chunk count, a millisecond duration and the probe flag.
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
					 * @class Parameters
					 * @brief File-writer knobs. Same fields as @ref BufferedLocationWriter::Parameters.
					 * @note Construction and copying may allocate and throw.
					 */
					class Parameters: public BufferedLocationWriter::Parameters {
						public:
							/**
							 * @brief Construct an empty Safe-owned parameter bag.
							 * @note Not noexcept: a bag may allocate while it is being filled.
							 */
							Parameters() {}
							/**
							 * @brief Store writer knobs in Safe-owned optional values.
							 * @tparam Knobs Supported writer knobs.
							 * @param knobs Values to store.
							 */
							template<typename... Knobs>
							Parameters(Knobs... knobs): BufferedLocationWriter::Parameters(std::move(knobs)...) {}
							/**
							 * @brief Copy Safe-owned knobs; may allocate and throw.
							 * @param other Source bag.
							 */
							Parameters(const Parameters& other) = default;
							/**
							 * @brief Transfer Safe-owned knobs.
							 * @param other Source bag.
							 */
							Parameters(Parameters&& other) noexcept = default;
							/**
							 * @brief Release knobs through provider callbacks.
							 */
							~Parameters() noexcept = default;
							/**
							 * @brief Copy Safe-owned knobs; may allocate and throw.
							 * @param other Source bag.
							 * @return This bag.
							 */
							Parameters& operator=(const Parameters& other) = default;
							/**
							 * @brief Transfer Safe-owned knobs.
							 * @param other Source bag.
							 * @return This bag.
							 */
							Parameters& operator=(Parameters&& other) noexcept = default;
					};

					/**
					 * @name Lifecycle
					 * @{
					 */

					/**
					 * @brief Store the path and optional knobs. Does not open.
					 * @param path Local filesystem path. Stored once. @ref Location is @ref Location::Local.
					 * @param parameters Omitted knobs keep the current file defaults.
					 *
					 * All of @ref WriteChunk, @ref BackPressure and @ref MaxMemory
					 * omitted → @ref Setup probes the device.
					 * Any of those set → explicit: omitted @ref MaxMemory is 0,
					 * omitted chunk / backpressure is 0.
					 * Omitted @ref MaxWait → 0 ms.
					 */
					STORMBYTE_FORCE_INLINE explicit BufferedFileWriter(StormByte::Safe::String path,
							Parameters parameters = {}):
						BufferedFileWriter(std::move(path),
							parameters.WriteChunk().value_or(StormByte::ByteSize{0}),
							parameters.BackPressure().value_or(0),
							std::chrono::milliseconds{parameters.MaxWait().value_or(0)},
							parameters.MaxMemory().value_or(StormByte::ByteSize{0}),
							!parameters.WriteChunk().has_value()
								&& !parameters.BackPressure().has_value()
								&& !parameters.MaxMemory().has_value()) {}

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
					 * @return Owner of a @ref StormByte::System::Device on @ref Path.
					 *
					 * The base type is used as is, so usability is the real path probe.
					 */
					StormByte::Safe::Shared<StormByte::System::Device> OriginDevice() const override;

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
					Result OriginClose() override;

					/**
					 * @brief Write @p data to the file.
					 * @param data Contiguous octets.
					 * @return Ok with bytes written, Error or Failed.
					 */
					Result OriginPush(std::span<const std::byte> data) override;

					/**
					 * @brief Make written bytes visible to later readers of the path.
					 * @return @ref Status::Ok, Error or Failed.
					 */
					Result OriginFlush() override;

					/**
					 * @brief Resize the file to zero bytes.
					 * @return @ref Status::Ok or @ref Status::Failed.
					 */
					Result OriginTruncate() override;

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
					bool WillWrite(StormByte::ByteSize n) const override;

				private:
					/**
					 * @brief Construct stream and synchronization state in the provider module.
					 * @param path Local filesystem path.
					 * @param write_chunk Resolved push unit.
					 * @param back_pressure Resolved ring cap in chunks.
					 * @param max_wait Resolved wait cap.
					 * @param max_memory Resolved dirty-page budget.
					 * @param probe Whether to probe the device write window.
					 */
					BufferedFileWriter(StormByte::Safe::String path, StormByte::ByteSize write_chunk,
						std::size_t back_pressure, std::chrono::milliseconds max_wait,
						StormByte::ByteSize max_memory, bool probe);

					/**
					 * @brief Binary output stream owned by the provider module.
					 */
					std::ofstream m_file;
					/**
					 * @brief Serialises output stream access.
					 */
					mutable std::mutex m_file_mutex;
			};
		}
	}
}

STORMBYTE_DECLARE_MAYBE_SAFE(StormByte::Buffer::IO::BufferedFileWriter);
