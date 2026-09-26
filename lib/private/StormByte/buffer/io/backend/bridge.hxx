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
#include <StormByte/buffer/external.hxx>
#include <StormByte/buffer/fifo.hxx>
#include <StormByte/buffer/io/buffered_reader.hxx>
#include <StormByte/buffer/io/buffered_writer.hxx>
#include <StormByte/buffer/io/typedefs.hxx>
#include <StormByte/buffer/visibility.h>

#include <atomic>
#include <condition_variable>
#include <cstddef>
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
				 * @class Bridge
				 * @brief Private pump for @ref StormByte::Buffer::Bridge.
				 *
				 * Owns the worker thread. @c Passthrough is the atomic unit.
				 * @p high_water 0 means no occupancy cap.
				 */
				class STORMBYTE_BUFFER_PRIVATE Bridge {
					public:
						/**
						 * @brief Buffer → buffer.
						 * @param in Source.
						 * @param out Sink.
						 * @param high_water Occupancy cap. 0 means no cap.
						 */
						Bridge(ExternalReader& in, ExternalWriter& out, StormByte::ByteSize high_water) noexcept;

						/**
						 * @brief IO → IO.
						 * @param in Source.
						 * @param out Sink.
						 * @param high_water Occupancy cap. 0 means no cap.
						 */
						Bridge(const IO::BufferedReader& in, IO::BufferedWriter& out, StormByte::ByteSize high_water) noexcept;

						/**
						 * @brief Buffer → IO.
						 * @param in Source.
						 * @param out Sink.
						 * @param high_water Occupancy cap. 0 means no cap.
						 */
						Bridge(ExternalReader& in, IO::BufferedWriter& out, StormByte::ByteSize high_water) noexcept;

						/**
						 * @brief IO → buffer.
						 * @param in Source.
						 * @param out Sink.
						 * @param high_water Occupancy cap. 0 means no cap.
						 */
						Bridge(const IO::BufferedReader& in, ExternalWriter& out, StormByte::ByteSize high_water) noexcept;

						Bridge(const Bridge&) = delete;
						Bridge(Bridge&&) = delete;

						/**
						 * @brief Destructor. Stops and joins the worker.
						 */
						~Bridge() noexcept;

						Bridge& operator=(const Bridge&) = delete;
						Bridge& operator=(Bridge&&) = delete;

						/**
						 * @brief Whether the source reports end-of-stream.
						 * @return @c true on EoF.
						 */
						bool EoF() const noexcept;

						/**
						 * @brief Whether the source can still supply bytes.
						 * @return @c false on hard error. EoF is not a hard error.
						 */
						bool IsReadable() const noexcept;

						/**
						 * @brief Whether the sink accepts writes.
						 * @return @c false if closed or failed.
						 */
						bool IsWritable() const noexcept;

						/**
						 * @brief Sink occupancy cap.
						 * @return Current high_water. 0 means no cap.
						 */
						StormByte::ByteSize HighWater() const noexcept;

						/**
						 * @brief Set the sink occupancy cap. Does not toggle.
						 * @param high_water New cap. 0 means no cap.
						 */
						void HighWater(StormByte::ByteSize high_water) noexcept;

						/**
						 * @brief Worker status.
						 * @return Started or Paused while the backend lives.
						 */
						IO::Drainer::Status Drainer() const noexcept;

						/**
						 * @brief Toggle or hurry-push.
						 * @param operation Toggle or Flush.
						 * @return @c false on a dead pump or illegal op.
						 */
						bool Drainer(IO::Drainer::Operation operation) noexcept;

						/**
						 * @brief Wait for the in-flight transaction, write it, flush the sink.
						 * @return @c false on error.
						 */
						bool BarrierFlush() noexcept;

						/**
						 * @brief BarrierFlush then Close an External writer.
						 * @return @c true if the barrier succeeded.
						 */
						bool FlushAndClose() noexcept;

						/**
						 * @brief SetError on an External writer. No-op on IO.
						 */
						void SetError() noexcept;

					private:
						/**
						 * @brief Start the worker. Called from every ctor.
						 */
						void Launch() noexcept;

						/**
						 * @brief Worker loop.
						 */
						void Worker() noexcept;

						/**
						 * @brief Source is exhausted (EoF, nothing left).
						 * @return @c true when pumping should idle.
						 */
						bool SourceDone() const noexcept;

						/**
						 * @brief Bytes available on an External source now.
						 * @return 0 if IO source or empty.
						 */
						StormByte::ByteSize AvailableNow() const noexcept;

						/**
						 * @brief Occupancy of the sink right now.
						 * @return External Occupied or IO Dirty.
						 */
						StormByte::ByteSize OccupiedNow() const noexcept;

						/**
						 * @brief Flush the IO sink. External is a no-op success.
						 * @return @c false on IO flush error.
						 */
						bool DestFlush() noexcept;

						/**
						 * @brief Pull @p n into @p dest without committing the sink.
						 * @param n Byte count.
						 * @param dest Work FIFO.
						 * @return @c false on hard read failure.
						 */
						bool Pull(StormByte::ByteSize n, FIFO& dest) noexcept;

						/**
						 * @brief Push @p src to the sink. Consumed only on success.
						 * @param src Work FIFO.
						 * @return @c false on write failure.
						 */
						bool Push(FIFO& src) noexcept;

						/**
						 * @brief One transactional move of up to @p bytes.
						 * @param bytes Requested count. Must be > 0.
						 * @return @c false on hard failure.
						 */
						bool Passthrough(StormByte::ByteSize bytes) noexcept;

						ExternalReader* m_ext_in {nullptr};				///< External source.
						ExternalWriter* m_ext_out {nullptr};			///< External sink.
						const IO::BufferedReader* m_io_in {nullptr};	///< IO source.
						IO::BufferedWriter* m_io_out {nullptr};			///< IO sink.

						StormByte::ByteSize m_chunk_min {1};				///< Smallest pull.
						StormByte::ByteSize m_chunk_max {65536};			///< Largest pull.

						mutable std::mutex m_mutex;						///< Status / flags.
						mutable std::condition_variable m_cv;			///< Worker and barriers.

						std::atomic<std::size_t> m_high_water {0};		///< Occupancy cap in bytes. 0 means none.
						IO::Drainer::Status m_status {IO::Drainer::Status::Started}; ///< Pump switch.

						bool m_stop {false};							///< Worker teardown.
						bool m_barrier {false};							///< Bridge::Flush in flight.
						bool m_hurry {false};							///< Drainer(Flush) in flight.
						bool m_failed {false};							///< Sticky pump error.
						bool m_busy {false};							///< Passthrough running.

						std::thread m_worker;							///< Pump thread.
				};
			}
		}
	}
}
