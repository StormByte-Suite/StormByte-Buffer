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
#include <StormByte/safe/pointers.hxx>
#include <StormByte/safe/string.hxx>
#include <StormByte/system/device.hxx>

#include <chrono>
#include <memory>
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
		 * @namespace StormByte::Buffer::Backend
		 * @brief PIMPL coordinators for public Buffer types that are not IO.
		 */
		namespace Backend {
			/**
			 * @namespace StormByte::Buffer::Backend::IO
			 * @brief PIMPL coordinators for the public IO types.
			 */
			namespace IO {
				/**
				 * @class BufferedLocationWriter
				 * @brief Private state of @ref StormByte::Buffer::IO::BufferedLocationWriter.
				 */
				class BufferedLocationWriter;
			}
		}

		/**
		 * @namespace StormByte::Buffer::IO
		 * @brief Buffered binary sources and sinks.
		 */
		namespace IO {
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
			 * @ref Device is not virtual. @ref OriginDevice is pure and hands
			 * out a @ref StormByte::Safe::Shared owner, so a leaf may supply a
			 * @ref StormByte::System::Device subclass and the dynamic type
			 * survives. @ref Setup applies
			 * @ref StormByte::System::Device::Window, backpressure 4 and 1 MiB
			 * of @ref MaxMemory when the caller omitted @ref WriteChunk,
			 * @ref BackPressure and @ref MaxMemory, and
			 * @ref OriginDeviceUsable accepts the device.
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
					 * @class Parameters
					 * @brief Location-writer knobs. Same fields as @ref BufferedWriter::Parameters.
					 */
					class Parameters: public BufferedWriter::Parameters {
						public:
							using BufferedWriter::Parameters::Parameters;
					};

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
					virtual ~BufferedLocationWriter() noexcept override;

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
					 * @return Owner returned by @ref OriginDevice. May be empty.
					 *
					 * The dynamic type of the leaf device is preserved and the
					 * caller may keep the object alive.
					 */
					StormByte::Safe::Shared<StormByte::System::Device> Device() const;

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
					 * @brief Store the locator and resolve knobs in the caller.
					 * @param path Locator. Stored once on @ref BufferedWriter.
					 * @param location @ref Location::Local or @ref Location::Remote. Stored once.
					 * @param parameters Omitted @ref WriteChunk, @ref BackPressure and
					 *        @ref MaxMemory → @ref Setup probes the device.
					 *        Omitted @ref MaxWait → 0 ms.
					 */
					STORMBYTE_FORCE_INLINE BufferedLocationWriter(StormByte::Safe::String path,
							enum Location location, Parameters parameters = {}):
						BufferedLocationWriter(std::move(path), location,
							parameters.WriteChunk().value_or(StormByte::ByteSize{0}),
							parameters.BackPressure().value_or(0),
							parameters.MaxWait().value_or(std::chrono::milliseconds{0}),
							parameters.MaxMemory().value_or(StormByte::ByteSize{0}),
							!parameters.WriteChunk().has_value()
								&& !parameters.BackPressure().has_value()
								&& !parameters.MaxMemory().has_value()) {}

					/**
					 * @brief Forward the locator and the sink knobs.
					 * @param path Locator. Stored once on @ref BufferedWriter.
					 * @param location @ref Location::Local or @ref Location::Remote. Stored once.
					 * @param write_chunk Initial @ref WriteChunk. Ignored when @p probe is true.
					 * @param back_pressure Initial @ref BackPressure.
					 * @param max_wait Initial @ref MaxWait.
					 * @param max_memory Initial @ref MaxMemory. Ignored when @p probe is true.
					 * @param probe When true, @ref Setup replaces chunk, backpressure and MaxMemory.
					 *
					 * DLL boundary. @c m_io is created here.
					 */
					BufferedLocationWriter(StormByte::Safe::String path, enum Location location,
						StormByte::ByteSize write_chunk, std::size_t back_pressure,
						std::chrono::milliseconds max_wait, StormByte::ByteSize max_memory, bool probe);

					/**
					 * @brief Leaf measurement. Not necessarily a filesystem type.
					 * @return Owner of the device built by the leaf. May be empty.
					 *
					 * Build it with @c StormByte::Safe::Shared<StormByte::System::Device>::MakePointer
					 * so a @ref StormByte::System::Device subclass keeps its overrides.
					 */
					virtual StormByte::Safe::Shared<StormByte::System::Device> OriginDevice() const = 0;

					/**
					 * @brief Whether @ref Setup may read windows from @p device.
					 * @param device Owner returned by @ref OriginDevice. May be empty.
					 * @return @c true when the device may be measured.
					 *
					 * The default is a non-empty owner whose
					 * @ref StormByte::System::Device::operator bool is true, which probes
					 * the stored path. A leaf whose identifier is not a filesystem path
					 * overrides this and never reaches that non-virtual probe. An empty
					 * owner is always unusable.
					 */
					virtual bool OriginDeviceUsable(const StormByte::Safe::Shared<StormByte::System::Device>& device) const noexcept;

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
					 *
					 * @ref OriginDevice is called once. When
					 * @ref OriginDeviceUsable rejects the owner, @ref WriteChunk,
					 * @ref BackPressure and @ref MaxMemory keep their defaults.
					 */
					void Setup() final;

				private:
					std::unique_ptr<StormByte::Buffer::Backend::IO::BufferedLocationWriter> m_io;	///< Location string and probe flag.
			};
		}
	}
}
