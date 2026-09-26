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

#include <StormByte/buffer/external.hxx>
#include <StormByte/buffer/fifo.hxx>
#include <StormByte/buffer/io/buffered_reader.hxx>
#include <StormByte/buffer/io/buffered_writer.hxx>
#include <StormByte/buffer/io/typedefs.hxx>
#include <StormByte/buffer/visibility.h>

#include <memory>

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
				 */
				class Bridge;
			}
		}

		/**
		 * @class Bridge
		 * @brief Move bytes from a source to a sink on an owned worker thread.
		 *
		 * Pairings: ExternalReader/Writer, IO::BufferedReader/Writer, or mixed.
		 * Holds references only. Tips must outlive the Bridge. Does not Open
		 * or Seek. No local cache. @c Passthrough is private.
		 *
		 * The worker starts in the constructor, like std::thread.
		 * Status is @ref IO::Drainer::Status::Started for any @p high_water,
		 * including 0, and for constructors that omit @p high_water.
		 * Setters never start or pause. Use @ref IO::Drainer::Operation::Toggle to pause.
		 *
		 * @p high_water > 0 is an occupancy cap on the sink
		 * (@c Occupied / Dirty). The worker does not pull more than fits
		 * under that cap. @p high_water == 0, and the two-argument
		 * constructors that take a BufferedWriter, apply no Bridge cap.
		 * The only backpressure is the sink itself (@c Write TryAgain,
		 * @c BackPressure on a BufferedWriter).
		 *
		 * Omitting @p high_water is intended for BufferedFileWriter:
		 * those leaves already cap Dirty with WriteChunk and BackPressure.
		 * Toward a FIFO, SharedFIFO, Ring or Producer the same no-cap
		 * path can pull the whole source into RAM if nothing consumes
		 * the sink. Those pairings have no two-argument constructor.
		 *
		 * @ref Flush waits for the in-flight transaction, writes it, then
		 * flushes the destination. @ref IO::Drainer::Operation::Flush pushes
		 * whatever is already held, even a short chunk, and does not flush
		 * the destination. On a BufferedWriter those bytes may still sit
		 * in Dirty.
		 *
		 * The destructor joins the worker. There is no public Stop.
		 *
		 * @see ExternalReader, ExternalWriter, IO::BufferedReader, IO::BufferedWriter
		 */
		class STORMBYTE_BUFFER_PUBLIC Bridge {
			public:
				/**
				 * @brief Buffer → buffer. Tips not owned.
				 * @param in Source.
				 * @param out Sink.
				 * @param high_water Sink occupancy cap. 0 means no Bridge cap.
				 */
				Bridge(ExternalReader& in, ExternalWriter& out, StormByte::ByteSize high_water) noexcept;

				/**
				 * @brief IO → IO. Tips must already be armed. Not owned.
				 * @param in Source.
				 * @param out Sink.
				 * @param high_water Sink occupancy cap. 0 means no Bridge cap.
				 */
				Bridge(const IO::BufferedReader& in, IO::BufferedWriter& out, StormByte::ByteSize high_water) noexcept;

				/**
				 * @brief IO → IO with no Bridge occupancy cap.
				 * @param in Source. Must already be armed.
				 * @param out Sink. Must already be armed.
				 *
				 * No occupancy limit at this layer. Worker starts
				 * (@ref IO::Drainer::Status::Started). Intended when
				 * @p out already has WriteChunk / BackPressure.
				 */
				Bridge(const IO::BufferedReader& in, IO::BufferedWriter& out) noexcept;

				/**
				 * @brief Buffer → IO. Tips not owned.
				 * @param in Source.
				 * @param out Sink.
				 * @param high_water Sink occupancy cap. 0 means no Bridge cap.
				 */
				Bridge(ExternalReader& in, IO::BufferedWriter& out, StormByte::ByteSize high_water) noexcept;

				/**
				 * @brief Buffer → IO with no Bridge occupancy cap.
				 * @param in Source.
				 * @param out Sink. Must already be armed.
				 *
				 * No occupancy limit at this layer. Worker starts
				 * (@ref IO::Drainer::Status::Started). Intended when
				 * @p out already has WriteChunk / BackPressure.
				 */
				Bridge(ExternalReader& in, IO::BufferedWriter& out) noexcept;

				/**
				 * @brief IO → buffer. Tips not owned.
				 * @param in Source.
				 * @param out Sink.
				 * @param high_water Sink occupancy cap. 0 means no Bridge cap.
				 */
				Bridge(const IO::BufferedReader& in, ExternalWriter& out, StormByte::ByteSize high_water) noexcept;

				Bridge(const Bridge&) = delete;

				/**
				 * @brief Move constructor. Moved-from Drainer is Stopped.
				 * @param other Instance to take from.
				 */
				Bridge(Bridge&& other) noexcept;

				/**
				 * @brief Destructor. Joins the worker.
				 */
				~Bridge() noexcept;

				Bridge& operator=(const Bridge&) = delete;

				/**
				 * @brief Move assignment. Moved-from Drainer is Stopped.
				 * @param other Instance to take from.
				 * @return *this.
				 */
				Bridge& operator=(Bridge&& other) noexcept;

				/**
				 * @brief Whether the source reports end-of-stream.
				 * @return @c true on EoF.
				 */
				bool EoF() const noexcept;

				/**
				 * @brief Whether the source can be read.
				 * @return @c false on error or closed.
				 */
				bool IsReadable() const noexcept;

				/**
				 * @brief Whether the sink accepts writes.
				 * @return @c false if closed or failed.
				 */
				bool IsWritable() const noexcept;

				/**
				 * @brief Sink occupancy cap.
				 * @return Current high_water. 0 means no Bridge cap.
				 */
				StormByte::ByteSize HighWater() const noexcept;

				/**
				 * @brief Set the sink occupancy cap. Does not start or pause.
				 * @param high_water New cap. 0 means no Bridge cap.
				 */
				void HighWater(StormByte::ByteSize high_water) noexcept;

				/**
				 * @brief Worker status.
				 * @return @c Stopped if moved-from.
				 */
				IO::Drainer::Status Drainer() const noexcept;

				/**
				 * @brief Toggle pause or hurry-push already held bytes.
				 * @param operation @c Toggle or @c Flush.
				 * @return @c false if moved-from or the operation cannot run.
				 */
				bool Drainer(IO::Drainer::Operation operation) noexcept;

				/**
				 * @brief Finish the in-flight transaction, write it, flush the sink.
				 * @return @c false on error.
				 */
				bool Flush() noexcept;

				/**
				 * @brief @ref Flush then Close an External writer. IO: Flush only.
				 * @return @c true if Flush succeeded.
				 */
				bool FlushAndClose() noexcept;

				/**
				 * @brief @c SetError on an External writer. No-op on IO.
				 */
				void SetError() noexcept;

			private:
				std::unique_ptr<IO::Backend::Bridge> m_io;	///< Pump.
		};
	}
}
