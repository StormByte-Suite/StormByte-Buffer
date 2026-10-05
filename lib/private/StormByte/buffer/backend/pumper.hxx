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

#include <StormByte/buffer/bridge.hxx>
#include <StormByte/buffer/telemetry.hxx>
#include <StormByte/buffer/visibility.h>
#include <StormByte/safe/optional.hxx>
#include <StormByte/safe/pointers.hxx>

#include <condition_variable>
#include <mutex>
#include <thread>

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
			 * @brief Worker that drives @ref StormByte::Buffer::Bridge::Passthrough until EoF, failure or Cancel.
			 *
			 * Owns the public Bridge. Starts in the constructor. @ref Cancel
			 * is terminal and is not @ref Failed. @ref Toggle parks the worker.
			 * The destructor joins; it does not Cancel. The worker drains
			 * until EoF or Failed unless Cancel ran.
			 *
			 * HighWater applies to the read tip. 0 = no Pumper cap.
			 * Chunk 0 = automatic cycle size (constexpr in the .cxx),
			 * not Bridge "current contents".
			 *
			 * IO sources with @ref IO::BufferedReader::ReadAhead of 0 use
			 * @ref StormByte::Buffer::Bridge::Operation::Blocking and a
			 * request size greater than 0. Everything else uses
			 * @ref StormByte::Buffer::Bridge::Operation::NonBlocking.
			 *
			 * Telemetry handles are copied from the Bridge at construction
			 * and survive @ref Cancel and destruction of the Bridge session.
			 */
			class STORMBYTE_BUFFER_PRIVATE Pumper {
				public:
					/**
					 * @brief Take the Bridge and start the worker.
					 * @param bridge Owned public bridge.
					 * @param chunk 0 = automatic cycle size.
					 * @param high_water Empty = default from the input kind.
					 */
					Pumper(StormByte::Buffer::Bridge&& bridge, StormByte::ByteSize chunk,
						StormByte::Safe::Optional<StormByte::ByteSize> high_water);

					Pumper(const Pumper&) = delete;
					Pumper(Pumper&&) = delete;

					/**
					 * @brief Join the worker. Does not @ref Cancel.
					 *
					 * The worker keeps draining until EoF or Failed unless
					 * @ref Cancel already ran. May block until that cycle ends.
					 */
					~Pumper();

					Pumper& operator=(const Pumper&) = delete;
					Pumper& operator=(Pumper&&) = delete;

					/**
					 * @brief Terminal stop. Sets @ref Canceled and closes the Bridge.
					 *
					 * Does not set @ref Failed. Cannot restart.
					 */
					void Cancel() noexcept;

					/**
					 * @brief Whether @ref Cancel ran.
					 * @return Sticky.
					 */
					bool Canceled() const noexcept;

					/**
					 * @brief Whether the Bridge reports EoF.
					 * @return @c true on EoF.
					 */
					bool EoF() const noexcept;

					/**
					 * @brief Whether the Bridge failed for real.
					 * @return Sticky. Not set by @ref Cancel.
					 */
					bool Failed() const noexcept;

					/**
					 * @brief Pause or resume the worker.
					 *
					 * No-op if @ref Failed or @ref Canceled.
					 */
					void Toggle() noexcept;

					/**
					 * @brief Cached Bridge read handle.
					 * @return Shared handle. Empty if the Bridge had none.
					 */
					const StormByte::Safe::Shared<StormByte::Buffer::ReadTelemetry> ReadTelemetry() const noexcept;

					/**
					 * @brief Cached Bridge write handle.
					 * @return Shared handle. Empty if the Bridge had none.
					 */
					const StormByte::Safe::Shared<StormByte::Buffer::WriteTelemetry> WriteTelemetry() const noexcept;

				private:
					/**
					 * @brief Worker loop.
					 */
					void Worker();

					/**
					 * @brief Bytes this cycle will ask Passthrough.
					 * @return Chunk, or the automatic size, capped by HighWater when HighWater > 0.
					 *
					 * Never returns 0. A Blocking IO pull with 0 would not
					 * touch the origin.
					 */
					StormByte::ByteSize CycleRequest() const noexcept;

					StormByte::Buffer::Bridge m_bridge;				///< Owned public bridge.
					StormByte::Safe::Shared<StormByte::Buffer::ReadTelemetry> m_read;	///< Session read counters.
					StormByte::Safe::Shared<StormByte::Buffer::WriteTelemetry> m_write;	///< Session write counters.
					StormByte::ByteSize m_chunk {0};				///< 0 = automatic.
					StormByte::ByteSize m_high_water {0};			///< 0 = no Pumper cap.
					bool m_io_in_blocking {false};					///< Bridge::InputPullBlocking at take-over.
					bool m_canceled {false};						///< Cancel ran.
					bool m_paused {false};							///< Toggle park.
					mutable std::mutex m_mutex;						///< Session.
					std::condition_variable m_cv;					///< Pause / cancel.
					std::thread m_worker;							///< Pump thread.
			};
		}
	}
}
