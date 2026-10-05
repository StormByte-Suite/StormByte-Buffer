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
				 * @class BufferedLocationReader
				 * @brief Private state of @ref StormByte::Buffer::IO::BufferedLocationReader.
				 */
				class BufferedLocationReader;
			}
		}

		/**
		 * @namespace StormByte::Buffer::IO
		 * @brief Buffered binary sources and sinks.
		 */
		namespace IO {
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
			 * @ref Path and @ref Location live on @ref BufferedReader. They are
			 * stored once and do not change. @ref Path may be a filesystem
			 * path, @c socket://… or @c http://… . A file leaf passes
			 * @ref Location::Local and its path is a local filesystem path.
			 *
			 * @ref Device is not virtual. @ref OriginDevice is pure and hands
			 * out a @ref StormByte::Safe::Shared owner, so a leaf may supply a
			 * @ref StormByte::System::Device subclass and the dynamic type
			 * survives. @ref Setup applies
			 * @ref StormByte::System::Device::Window when the caller omitted
			 * @ref ReadAhead and @ref OriginDeviceUsable accepts the device.
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
					 * @class Parameters
					 * @brief Location-reader knobs. Same fields as @ref BufferedReader::Parameters.
					 * @note Construction and copying may allocate and throw.
					 */
					class Parameters: public BufferedReader::Parameters {
						public:
							/**
							 * @brief Construct an empty Safe-owned parameter bag.
							 */
							Parameters() = default;
							/**
							 * @brief Store reader knobs in Safe-owned optional values.
							 * @tparam Knobs Supported reader knobs.
							 * @param knobs Values to store.
							 */
							template<typename... Knobs>
							Parameters(Knobs... knobs): BufferedReader::Parameters(std::move(knobs)...) {}
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
					virtual ~BufferedLocationReader() noexcept override;

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
					 * @brief Measurement of this location.
					 * @return Owner returned by @ref OriginDevice. May be empty.
					 *
					 * The dynamic type of the leaf device is preserved and the
					 * caller may keep the object alive.
					 */
					StormByte::Safe::Shared<StormByte::System::Device> Device() const;

				protected:
					/**
					 * @brief Store the locator and resolve knobs in the caller.
					 * @param path Locator. Forwarded. Stored once on @ref BufferedReader.
					 * @param location @ref Location::Local or @ref Location::Remote. Forwarded.
					 * @param parameters Omitted @ref ReadAhead → @ref Setup probes @ref Device.
					 *        Omitted @ref MaxMemory / @ref MaxWait → 0 / 0 ms.
					 */
					STORMBYTE_FORCE_INLINE BufferedLocationReader(StormByte::Safe::String path,
							enum Location location, Parameters parameters = {}):
						BufferedLocationReader(std::move(path), location,
							parameters.ReadAhead().value_or(StormByte::ByteSize{0}),
							parameters.MaxMemory().value_or(StormByte::ByteSize{0}),
							std::chrono::milliseconds{parameters.MaxWait().value_or(0)},
							!parameters.ReadAhead().has_value()) {}

					/**
					 * @brief Store the locator and forward the cache knobs.
					 * @param path Locator. Forwarded. Stored once on @ref BufferedReader.
					 * @param location @ref Location::Local or @ref Location::Remote. Forwarded.
					 * @param read_ahead Initial @ref ReadAhead. Ignored when @p probe is true.
					 * @param max_memory Initial @ref MaxMemory.
					 * @param max_wait Initial @ref MaxWait.
					 * @param probe When true, @ref Setup replaces @ref ReadAhead from @ref Device.
					 *
					 * DLL boundary. @c m_io is created here.
					 */
					BufferedLocationReader(StormByte::Safe::String path, enum Location location,
						StormByte::ByteSize read_ahead, StormByte::ByteSize max_memory,
						std::chrono::milliseconds max_wait, bool probe);

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
					 * @c operator bool is true, which probes
					 * the stored path. A leaf whose identifier is not a filesystem path
					 * overrides this and never reaches that non-virtual probe. An empty
					 * owner is always unusable.
					 */
					virtual bool OriginDeviceUsable(const StormByte::Safe::Shared<StormByte::System::Device>& device) const noexcept;

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
					 *
					 * @ref OriginDevice is called once. When
					 * @ref OriginDeviceUsable rejects the owner, @ref ReadAhead keeps
					 * its default.
					 */
					void Setup() final;

				private:
					/**
					 * @brief Provider-owned location state and probe flag.
					 */
					std::unique_ptr<StormByte::Buffer::Backend::IO::BufferedLocationReader> m_io;
			};
		}
	}
}

STORMBYTE_DECLARE_MAYBE_SAFE(StormByte::Buffer::IO::BufferedLocationReader);
