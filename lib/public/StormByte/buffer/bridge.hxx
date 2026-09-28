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

#include <StormByte/buffer/fifo.hxx>
#include <StormByte/buffer/generic.hxx>
#include <StormByte/buffer/io/buffered_reader.hxx>
#include <StormByte/buffer/io/buffered_writer.hxx>
#include <StormByte/buffer/telemetry.hxx>
#include <StormByte/buffer/visibility.h>
#include <StormByte/safe_pointers.hxx>
#include <StormByte/type_traits.hxx>

#include <memory>
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
		class ExternalBufferReader;
		class ExternalBufferWriter;

		/**
		 * @struct ExternalReaderDeleter
		 * @brief Destroys a non-IO read adapter in this module.
		 */
		struct STORMBYTE_BUFFER_PUBLIC ExternalReaderDeleter {
			/**
			 * @brief Destroy @p ptr. No-op if null.
			 * @param ptr Adapter or null.
			 */
			void operator()(ExternalBufferReader* ptr) const noexcept;
		};

		/**
		 * @struct ExternalWriterDeleter
		 * @brief Destroys a non-IO write adapter in this module.
		 */
		struct STORMBYTE_BUFFER_PUBLIC ExternalWriterDeleter {
			/**
			 * @brief Destroy @p ptr. No-op if null.
			 * @param ptr Adapter or null.
			 */
			void operator()(ExternalBufferWriter* ptr) const noexcept;
		};

		/**
		 * @class Bridge
		 * @brief Manual bridge between two StormByte-Buffer ends.
		 *
		 * Connects a read tip to a write tip. Bytes cross only when the
		 * caller invokes @ref Passthrough. There is no worker and no
		 * occupancy cap at this layer. Continuous transfer is @ref Pumper.
		 *
		 * @par Ownership
		 * Non-IO tips are @ref ReadOnly / @ref WriteOnly references. The
		 * buffers must outlive the Bridge. Adapters over those tips are
		 * owned here and are not part of the public contract. IO tips are
		 * taken by move as the concrete leaf so a second reader or writer
		 * cannot race @ref Passthrough. Stolen leaves live in
		 * @c StormByte::Unique on Base's heap.
		 *
		 * @par Lifecycle
		 * A Bridge is one shot. @ref State::Open while tips are attached.
		 * @ref Close, a consumed source EoF and move-from go to
		 * @ref State::Closed. A real tip fault goes to @ref State::Failed.
		 * @ref Failed is only that last case. There is no rewind and no
		 * seek on the session. After @ref State::Closed or
		 * @ref State::Failed the instance cannot be re-armed; construct
		 * a new Bridge to transfer again.
		 *
		 * @ref Close releases adapters and stolen IO leaves. IO
		 * destructors close the origin, so Windows can unlink the path.
		 * In-memory tips do not lock a file. Idempotent. @ref Close does
		 * not set @ref Failed. A session that is already @ref State::Failed
		 * stays Failed and still drops the tips.
		 *
		 * @par Passthrough
		 * One call is one atomic transfer. @c TryAgain on the write tip
		 * is retried until the requested write completes or the tip
		 * fails. That is not the same as @ref Operation::Blocking:
		 * Blocking applies only to the read side.
		 *
		 * @p n == 0 is the current contents of the read tip
		 * (non-IO occupancy, or @ref IO::BufferedReader::Available).
		 * Available on IO does not touch the origin.
		 *
		 * If @ref State is not @ref State::Open, @ref Passthrough
		 * returns 0 and does nothing. A NonBlocking call that yields
		 * zero bytes is not the end of the session.
		 *
		 * @par Telemetry
		 * This Bridge always holds a @c Shared copy of the read and
		 * write counters. A non-IO tip uses a basic
		 * @ref StormByte::Buffer::ReadTelemetry /
		 * @ref StormByte::Buffer::WriteTelemetry created here. The
		 * Bridge updates those counters on each successful transfer.
		 * An IO tip donates the leaf handle at attach. The leaf updates
		 * that object; the Bridge only caches the handle so @ref Close
		 * does not drop it. Survivors that copied the @c Shared keep
		 * the last values when *this dies.
		 *
		 * @see ReadOnly, WriteOnly, IO::BufferedReader, IO::BufferedWriter
		 */
		class STORMBYTE_BUFFER_PUBLIC Bridge {
			public:
				/**
				 * @enum Operation
				 * @brief Read-side wait policy for @ref Passthrough.
				 */
				enum class Operation {
					Blocking,		///< Wait until N bytes or EoF.
					NonBlocking		///< Take what is available now, up to N.
				};

				/**
				 * @enum State
				 * @brief Session lifetime. @ref Failed is a real tip fault only.
				 */
				enum class State {
					Open,		///< Tips attached. @ref Passthrough may run.
					Closed,		///< @ref Close, consumed EoF or moved-from.
					Failed		///< A tip failed. Sticky.
				};

				/**
				 * @brief Two non-IO tips. Referenced. Adapters owned here.
				 * @param in Source. Must outlive *this.
				 * @param out Sink. Must outlive *this.
				 */
				Bridge(ReadOnly& in, WriteOnly& out) noexcept;

				/**
				 * @brief Two IO leaves. Stolen by move.
				 * @tparam In Concrete @ref IO::BufferedReader leaf.
				 * @tparam Out Concrete @ref IO::BufferedWriter leaf.
				 * @param in Source. Moved-from is empty.
				 * @param out Sink. Moved-from is empty.
				 */
				template<typename In, typename Out>
				STORMBYTE_FORCE_INLINE Bridge(In&& in, Out&& out) noexcept
					requires (Type::DerivedFrom<std::remove_cvref_t<In>, IO::BufferedReader>
						&& Type::DerivedFrom<std::remove_cvref_t<Out>, IO::BufferedWriter>) {
					AttachIoIn(std::forward<In>(in));
					AttachIoOut(std::forward<Out>(out));
				}

				/**
				 * @brief Non-IO source, IO sink stolen.
				 * @tparam Out Concrete @ref IO::BufferedWriter leaf.
				 * @param in Source. Must outlive *this.
				 * @param out Sink. Moved-from is empty.
				 */
				template<typename Out>
				STORMBYTE_FORCE_INLINE Bridge(ReadOnly& in, Out&& out) noexcept
					requires Type::DerivedFrom<std::remove_cvref_t<Out>, IO::BufferedWriter> {
					AttachIoOut(std::forward<Out>(out));
					AttachNonIoIn(in);
				}

				/**
				 * @brief IO source stolen, non-IO sink.
				 * @tparam In Concrete @ref IO::BufferedReader leaf.
				 * @param in Source. Moved-from is empty.
				 * @param out Sink. Must outlive *this.
				 */
				template<typename In>
				STORMBYTE_FORCE_INLINE Bridge(In&& in, WriteOnly& out) noexcept
					requires Type::DerivedFrom<std::remove_cvref_t<In>, IO::BufferedReader> {
					AttachIoIn(std::forward<In>(in));
					AttachNonIoOut(out);
				}

				Bridge(const Bridge&) = delete;

				/**
				 * @brief Move constructor. Moved-from is @ref State::Closed and empty.
				 * @param other Instance to take from.
				 */
				Bridge(Bridge&& other) noexcept;

				/**
				 * @brief Destructor. Releases adapters and stolen IO tips.
				 */
				~Bridge() noexcept;

				Bridge& operator=(const Bridge&) = delete;

				/**
				 * @brief Move assignment. Moved-from is @ref State::Closed and empty.
				 * @param other Instance to take from.
				 * @return *this.
				 */
				Bridge& operator=(Bridge&& other) noexcept;

				/**
				 * @brief Release owned tips. Mark @ref State::Closed if it was Open.
				 *
				 * Drops non-IO adapters and stolen IO leaves. IO
				 * destructors close the origin. Idempotent. Does not
				 * set @ref Failed. A @ref State::Failed session stays
				 * Failed and still drops the tips. After @ref State::Closed
				 * construct a new Bridge to transfer again.
				 */
				void Close() noexcept;

				/**
				 * @brief Whether the read tip reports end-of-stream.
				 * @return @c true on EoF, if there is no read tip, or if not @ref State::Open.
				 */
				bool EoF() const noexcept;

				/**
				 * @brief Whether a tip has failed for real.
				 * @return @c true only when @ref State is @ref State::Failed.
				 */
				bool Failed() const noexcept;

				/**
				 * @brief Whether the read tip is a stolen IO leaf.
				 * @return @c true if the source is IO. Used by @ref Pumper HighWater default.
				 */
				bool InputIsIO() const noexcept;

				/**
				 * @brief Move bytes from the read tip to the write tip.
				 * @param n Requested bytes. Zero means current contents.
				 * @param operation Read-side wait policy.
				 * @return Bytes actually moved. Zero if not @ref State::Open or empty.
				 */
				StormByte::ByteSize Passthrough(StormByte::ByteSize n,
					Operation operation = Operation::Blocking);

				/**
				 * @brief Session counters for the read tip.
				 * @return Const shared handle. Empty if moved-from.
				 */
				const StormByte::Shared<StormByte::Buffer::ReadTelemetry> ReadTelemetry() const noexcept;

				/**
				 * @brief Current session state.
				 * @return @ref State::Open, @ref State::Closed or @ref State::Failed.
				 */
				enum State State() const noexcept;

				/**
				 * @brief Session counters for the write tip.
				 * @return Const shared handle. Empty if moved-from.
				 */
				const StormByte::Shared<StormByte::Buffer::WriteTelemetry> WriteTelemetry() const noexcept;

			private:
				/**
				 * @brief Own an adapter over a non-IO source. Defined in this module.
				 * @param in Source. Must outlive *this.
				 */
				void AttachNonIoIn(ReadOnly& in) noexcept;

				/**
				 * @brief Own an adapter over a non-IO sink. Defined in this module.
				 * @param out Sink. Must outlive *this.
				 */
				void AttachNonIoOut(WriteOnly& out) noexcept;

				/**
				 * @brief Steal an IO source onto Base's heap.
				 * @tparam In Concrete @ref IO::BufferedReader leaf.
				 * @param in Source. Moved-from is empty.
				 */
				template<typename In>
				STORMBYTE_FORCE_INLINE void AttachIoIn(In&& in) noexcept {
					m_io_in = StormByte::Unique<IO::BufferedReader>::MakePointer<std::remove_cvref_t<In>>(
						std::forward<In>(in));
					CacheReadTelemetry();
				}

				/**
				 * @brief Steal an IO sink onto Base's heap.
				 * @tparam Out Concrete @ref IO::BufferedWriter leaf.
				 * @param out Sink. Moved-from is empty.
				 */
				template<typename Out>
				STORMBYTE_FORCE_INLINE void AttachIoOut(Out&& out) noexcept {
					m_io_out = StormByte::Unique<IO::BufferedWriter>::MakePointer<std::remove_cvref_t<Out>>(
						std::forward<Out>(out));
					CacheWriteTelemetry();
				}

				/**
				 * @brief Snapshot the stolen IO source telemetry. Defined in this module.
				 */
				void CacheReadTelemetry() noexcept;

				/**
				 * @brief Snapshot the stolen IO sink telemetry. Defined in this module.
				 */
				void CacheWriteTelemetry() noexcept;

				/**
				 * @brief Drop adapters and stolen IO leaves. Telemetry stays.
				 */
				void ReleaseTips() noexcept;

				/**
				 * @brief Whether the attached source reports EoF. Caller holds @c m_mutex.
				 */
				bool SourceEoF() const noexcept;

				/**
				 * @brief Pull up to @p n into @p dest according to @p operation.
				 */
				IO::Result Pull(StormByte::ByteSize n, FIFO& dest, Operation operation);

				/**
				 * @brief Push @p src. Retry TryAgain until Ok, End or fail.
				 */
				IO::Result Push(FIFO& src);

				std::unique_ptr<ExternalBufferReader, ExternalReaderDeleter> m_ext_in;	///< Non-IO read adapter. Owned.
				std::unique_ptr<ExternalBufferWriter, ExternalWriterDeleter> m_ext_out;	///< Non-IO write adapter. Owned.
				StormByte::Unique<IO::BufferedReader> m_io_in;							///< Stolen IO source. Base heap.
				StormByte::Unique<IO::BufferedWriter> m_io_out;							///< Stolen IO sink. Base heap.
				StormByte::Shared<StormByte::Buffer::ReadTelemetry> m_owned_read;		///< Session read counters.
				StormByte::Shared<StormByte::Buffer::WriteTelemetry> m_owned_write;		///< Session write counters.
				enum State m_state {State::Open};										///< Session lifetime.
				mutable std::mutex m_mutex;												///< Session lock.
		};
	}
}
