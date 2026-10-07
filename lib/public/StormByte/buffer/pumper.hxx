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

#include <StormByte/buffer/bridge.hxx>
#include <StormByte/buffer/telemetry.hxx>
#include <StormByte/buffer/visibility.h>
#include <StormByte/platform.h>
#include <StormByte/safe/optional.hxx>
#include <StormByte/safe/pointers.hxx>

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
			 * @class Pumper
			 * @brief Private worker for @ref StormByte::Buffer::Pumper.
			 */
			class Pumper;
		}

		/**
		 * @class Chunk
		 * @brief Pumper cycle size. 0 means automatic chunking, not "current contents".
		 */
		class Chunk {
			public:
				/**
				 * @brief Store the cycle size.
				 * @param value Bytes. 0 = automatic.
				 */
				explicit Chunk(const StormByte::ByteSize value) noexcept:
					m_value(value) {}

				/**
				 * @brief Cycle size.
				 * @return Bytes.
				 */
				StormByte::ByteSize Value() const noexcept {
					return m_value;
				}

			private:
				/**
				 * @brief Cycle size.
				 */
				StormByte::ByteSize m_value;
		};

		/**
		 * @class HighWater
		 * @brief Input occupancy cap for @ref Pumper.
		 *
		 * Applies to the read tip only. @c 0 is an explicit "no Pumper cap".
		 * Omitting the knob is not the same as @c 0: the office then picks
		 * 0 for an IO source and a non-IO default defined in the backend.
		 */
		class HighWater {
			public:
				/**
				 * @brief Store the input cap.
				 * @param value Bytes. 0 disables the Pumper cap.
				 */
				explicit HighWater(const StormByte::ByteSize value) noexcept:
					m_value(value) {}

				/**
				 * @brief Input cap.
				 * @return Bytes.
				 */
				StormByte::ByteSize Value() const noexcept {
					return m_value;
				}

			private:
				/**
				 * @brief Input cap.
				 */
				StormByte::ByteSize m_value;
		};

		/**
		 * @class Pumper
		 * @brief Owns a @ref Bridge and moves bytes until EoF, failure or Cancel.
		 *
		 * Starts the worker in the constructor. The destructor joins; it
		 * may block until the current cycle finishes. There is no Stop.
		 * The destructor does not call @c Cancel; it lets the worker finish.
		 * @ref Cancel is terminal (@ref Canceled, no restart). @ref Toggle
		 * pauses and resumes. @ref Failed is only a real @ref Bridge fault.
		 *
		 * @par HighWater
		 * Caps how much the worker will pull from the input. @c nullopt
		 * (omitted knob): IO source → 0; non-IO source → backend default
		 * (constexpr in the .cxx). Explicit @c 0: no Pumper cap. Use 0
		 * only when the source is an IO leaf that already limits itself.
		 * Non-IO sources are unbounded by design; omitting HighWater is
		 * the safe default. 0 on a non-IO source is the caller's choice.
		 *
		 * @par Chunk
		 * Bytes the worker asks @ref Bridge::Passthrough per cycle.
		 * @c 0 is automatic chunking, not Bridge's "current contents".
		 * The backend never asks for 0 bytes: a Blocking IO pull of 0
		 * would not touch the origin.
		 *
		 * @par IO pull
		 * If the owned Bridge reports @ref Bridge::InputPullBlocking
		 * (stolen reader with @ref IO::BufferedReader::ReadAhead of 0),
		 * the worker uses @ref Bridge::Operation::Blocking. Otherwise
		 * it uses @ref Bridge::Operation::NonBlocking.
		 *
		 * @par Telemetry
		 * Copies the Bridge handles at construction. Those @c Shared
		 * objects stay valid after @ref Cancel and after *this dies
		 * if the caller kept a copy. No extra counters.
		 *
		 * An IO path stays locked while this Pumper is alive. After
		 * @ref Cancel the owned Bridge is Closed.
		 */
		class STORMBYTE_BUFFER_PUBLIC Pumper {
			public:
				/**
				 * @class Parameters
				 * @brief Optional Pumper knobs. A missing field keeps the office default.
				 *
				 * Header-only bag with Safe provider-owned optional values.
				 * @note Construction, copying and knob assignment may allocate and throw.
				 */
				class Parameters {
					public:
						/**
						 * @brief No knobs. Every field is absent.
						 */
						Parameters() = default;

						/**
						 * @brief Copy provider-owned knob values; may allocate and throw.
						 * @param other Source parameters.
						 */
						Parameters(const Parameters& other) = default;

						/**
						 * @brief Transfer provider-owned knob values.
						 * @param other Source parameters.
						 */
						Parameters(Parameters&& other) noexcept = default;

						/**
						 * @brief Release knobs through their provider callbacks.
						 */
						~Parameters() noexcept = default;

						/**
						 * @brief Copy provider-owned knob values; may allocate and throw.
						 * @param other Source parameters.
						 * @return This instance.
						 */
						Parameters& operator=(const Parameters& other) = default;

						/**
						 * @brief Transfer provider-owned knob values.
						 * @param other Source parameters.
						 * @return This instance.
						 */
						Parameters& operator=(Parameters&& other) noexcept = default;

						/**
						 * @brief Engage the listed knobs. Unknown types do not compile.
						 * @tparam Knobs @ref Chunk and/or @ref HighWater.
						 * @param knobs Values to store.
						 */
						template<typename... Knobs>
						Parameters(Knobs... knobs) {
							(Apply(std::move(knobs)), ...);
						}

						/**
						 * @brief Cycle size when the caller set it.
						 * @return Empty when the caller omitted @ref Chunk.
						 */
						const StormByte::Safe::Optional<StormByte::ByteSize>& Chunk() const noexcept {
							return m_chunk;
						}

						/**
						 * @brief Input cap when the caller set it.
						 * @return Empty when the caller omitted @ref HighWater.
						 */
						const StormByte::Safe::Optional<StormByte::ByteSize>& HighWater() const noexcept {
							return m_high_water;
						}

					private:
						/**
						 * @brief Store a @ref Chunk knob.
						 * @param knob Cycle size.
						 */
						void Apply(class Chunk knob) {
							m_chunk = knob.Value();
						}

						/**
						 * @brief Store a @ref HighWater knob.
						 * @param knob Input cap.
						 */
						void Apply(class HighWater knob) {
							m_high_water = knob.Value();
						}

						/**
						 * @brief Reject unsupported knob types.
						 * @tparam Knob Unsupported type.
						 * @param knob Unsupported value.
						 */
						template<typename Knob>
						void Apply(Knob&& knob) = delete;

						/**
						 * @brief Absent means automatic chunking.
						 */
						StormByte::Safe::Optional<StormByte::ByteSize> m_chunk;
						/**
						 * @brief Absent means the office default; an engaged zero disables the cap.
						 */
						StormByte::Safe::Optional<StormByte::ByteSize> m_high_water;
				};

				/**
				 * @brief Take a Bridge and start the worker.
				 * @param bridge Owned bridge. Moved-from is empty.
				 * @param parameters Omitted knobs keep the office default.
				 */
				STORMBYTE_FORCE_INLINE explicit Pumper(Bridge&& bridge, Parameters parameters = {}):
					Pumper(std::move(bridge),
						parameters.Chunk().value_or(StormByte::ByteSize{0}),
						parameters.HighWater()) {}

				/**
				 * @brief Copying a worker is not supported.
				 * @param other Source worker.
				 */
				Pumper(const Pumper& other) = delete;

				/**
				 * @brief Move constructor. Moved-from is empty and joined.
				 * @param other Instance to take from.
				 */
				Pumper(Pumper&& other) noexcept;

				/**
				 * @brief Destructor. Joins the worker. Does not @ref Cancel.
				 */
				~Pumper() noexcept;

				/**
				 * @brief Copy assignment is not supported.
				 * @param other Source worker.
				 * @return This instance.
				 */
				Pumper& operator=(const Pumper& other) = delete;

				/**
				 * @brief Move assignment. Moved-from is empty and joined.
				 * @param other Instance to take from.
				 * @return *this.
				 */
				Pumper& operator=(Pumper&& other) noexcept;

				/**
				 * @brief Terminal stop. Sets @ref Canceled. Closes the Bridge.
				 *
				 * Idempotent. Does not set @ref Failed. The worker will not
				 * resume. Construct a new Pumper to transfer again.
				 */
				void Cancel() noexcept;

				/**
				 * @brief Whether @ref Cancel ran.
				 * @return Sticky. A canceled Pumper cannot be resumed.
				 */
				bool Canceled() const noexcept;

				/**
				 * @brief Whether the owned Bridge reports EoF.
				 * @return @c true on EoF or if moved-from.
				 */
				bool EoF() const noexcept;

				/**
				 * @brief Whether the owned Bridge failed for real.
				 * @return Sticky. Not set by @ref Cancel or move-from.
				 */
				bool Failed() const noexcept;

				/**
				 * @brief Pause or resume the worker.
				 *
				 * No-op if @ref Failed or @ref Canceled.
				 */
				void Toggle() noexcept;

				/**
				 * @brief Read counters copied from the owned Bridge.
				 * @return Const shared handle. Empty if moved-from.
				 */
				const StormByte::Safe::Shared<StormByte::Buffer::ReadTelemetry> ReadTelemetry() const noexcept;

				/**
				 * @brief Write counters copied from the owned Bridge.
				 * @return Const shared handle. Empty if moved-from.
				 */
				const StormByte::Safe::Shared<StormByte::Buffer::WriteTelemetry> WriteTelemetry() const noexcept;

			private:
				/**
				 * @brief Resolved knobs. HighWater empty means office default.
				 * @param bridge Owned bridge.
				 * @param chunk 0 = automatic.
				 * @param high_water Empty = default from the input kind.
				 */
				Pumper(Bridge&& bridge, StormByte::ByteSize chunk,
					StormByte::Safe::Optional<StormByte::ByteSize> high_water);

				/**
				 * @brief Module-owned worker, released out of line.
				 */
				std::unique_ptr<Backend::Pumper> m_backend;
		};
	}
}

STORMBYTE_DECLARE_MAYBE_SAFE(StormByte::Buffer::Chunk);
STORMBYTE_DECLARE_MAYBE_SAFE(StormByte::Buffer::HighWater);
STORMBYTE_DECLARE_MAYBE_SAFE(StormByte::Buffer::Pumper::Parameters);
STORMBYTE_DECLARE_MAYBE_SAFE(StormByte::Buffer::Pumper);
