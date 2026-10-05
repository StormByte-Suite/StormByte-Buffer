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
#include <StormByte/safe/pointers.hxx>
#include <StormByte/type_traits.hxx>

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
		 * @class Bridge
		 * @brief Manual bridge between two StormByte-Buffer ends.
		 *
		 * Connects a read tip to a write tip. Bytes cross only when the
		 * caller invokes @ref Passthrough. There is no worker and no
		 * occupancy cap at this layer. Continuous transfer is @ref Pumper.
		 *
		 * @par Ownership
		 * Non-IO tips are @ref ReadOnly / @ref WriteOnly references. The
		 * buffers must outlive the Bridge. IO tips are
		 * taken by move as the concrete leaf so a second reader or writer
		 * cannot race @ref Passthrough. Stolen leaves live in
		 * @c StormByte::Safe::Unique on Base's heap.
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
		 * @ref Close detaches non-IO tips and releases stolen IO leaves. IO
		 * destructors close the origin, so Windows can unlink the path.
		 * In-memory tips do not lock a file. Idempotent. @ref Close does
		 * not set @ref Failed. A session that is already @ref State::Failed
		 * stays Failed and still drops the tips.
		 *
		 * @par Passthrough
		 * One call is one atomic transfer. @c TryAgain on the write tip
		 * is retried until the requested write completes or the tip
		 * fails. That is not the same as @c Operation::Blocking:
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
		 * A non-IO source that is not readable (@ref ReadOnly::IsReadable
		 * is false, including after @c SetError) is a tip fault, not EoF.
		 * A closed empty source is EoF. Those two are not the same.
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
				 * @brief Two non-IO tips. Referenced, not owned.
				 * @param in Source. Must outlive *this.
				 * @param out Sink. Must outlive *this.
				 */
				Bridge(ReadOnly& in, WriteOnly& out);

				/**
				 * @brief Two IO leaves. Stolen by move.
				 * @tparam In Concrete @ref IO::BufferedReader leaf.
				 * @tparam Out Concrete @ref IO::BufferedWriter leaf.
				 * @param in Source. Moved-from is empty.
				 * @param out Sink. Moved-from is empty.
				 */
				template<typename In, typename Out>
				STORMBYTE_FORCE_INLINE Bridge(In&& in, Out&& out)
					requires (Type::DerivedFrom<std::remove_cvref_t<In>, IO::BufferedReader>
						&& Type::DerivedFrom<std::remove_cvref_t<Out>, IO::BufferedWriter>
						&& !Type::LvalueReference<In> && !Type::Const<std::remove_reference_t<In>>
						&& !Type::LvalueReference<Out> && !Type::Const<std::remove_reference_t<Out>>): Bridge() {
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
				STORMBYTE_FORCE_INLINE Bridge(ReadOnly& in, Out&& out)
					requires (Type::DerivedFrom<std::remove_cvref_t<Out>, IO::BufferedWriter>
						&& !Type::LvalueReference<Out> && !Type::Const<std::remove_reference_t<Out>>): Bridge() {
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
				STORMBYTE_FORCE_INLINE Bridge(In&& in, WriteOnly& out)
					requires (Type::DerivedFrom<std::remove_cvref_t<In>, IO::BufferedReader>
						&& !Type::LvalueReference<In> && !Type::Const<std::remove_reference_t<In>>): Bridge() {
					AttachIoIn(std::forward<In>(in));
					AttachNonIoOut(out);
				}

				/**
				 * @brief Reject borrowed or const IO leaves instead of falling back to borrowed tips.
				 * @tparam In Source tip type.
				 * @tparam Out Sink tip type.
				 */
				template<typename In, typename Out>
				Bridge(In&&, Out&&)
					requires ((Type::DerivedFrom<std::remove_cvref_t<In>, IO::BufferedReader>
						&& (Type::LvalueReference<In> || Type::Const<std::remove_reference_t<In>>))
						|| (Type::DerivedFrom<std::remove_cvref_t<Out>, IO::BufferedWriter>
						&& (Type::LvalueReference<Out> || Type::Const<std::remove_reference_t<Out>>))) = delete;

				/**
				 * @brief Copy construction is disabled for one-shot sessions.
				 */
				Bridge(const Bridge&) = delete;

				/**
				 * @brief Move constructor. Moved-from is @ref State::Closed and empty.
				 * @param other Instance to take from.
				 */
				Bridge(Bridge&& other) noexcept;

				/**
				 * @brief Destructor. Releases stolen IO tips.
				 */
				~Bridge() noexcept;

				/**
				 * @brief Copy assignment is disabled for one-shot sessions.
				 */
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
				 * Detaches non-IO tips and drops stolen IO leaves. IO
				 * destructors close the origin. Idempotent. Does not
				 * set @ref Failed. A @ref State::Failed session stays
				 * Failed and still drops the tips. After @ref State::Closed
				 * construct a new Bridge to transfer again.
				 */
				void Close() noexcept;

				/**
				 * @brief Whether the read tip reports end-of-stream.
				 * @return @c true on EoF, if @ref State is not Open, or if there is no read tip.
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
				 * @brief Whether @ref Pumper must Blocking-pull this source.
				 * @return @c true when the stolen reader has @ref IO::BufferedReader::ReadAhead of 0.
				 *
				 * Non-IO sources return @c false. Snapshotted at attach;
				 * later @ref IO::BufferedReader::ReadAhead setters on a
				 * moved-from leaf do not change it.
				 */
				bool InputPullBlocking() const noexcept;

				/**
				 * @brief Session lifetime.
				 * @return Current @ref State.
				 */
				enum State State() const noexcept;

				/**
				 * @brief Move bytes from the read tip to the write tip.
				 * @param n Requested bytes. Zero means current contents.
				 * @param operation Read-side wait policy.
				 * @return Bytes actually moved. Zero if not @ref State::Open or empty.
				 */
				StormByte::ByteSize Passthrough(StormByte::ByteSize n,
					Operation operation = Operation::Blocking);

				/**
				 * @brief Read counters. Always the cached @c Shared handle.
				 * @return Const shared handle. Empty if moved-from.
				 */
				const StormByte::Safe::Shared<StormByte::Buffer::ReadTelemetry> ReadTelemetry() const noexcept;

				/**
				 * @brief Write counters. Always the cached @c Shared handle.
				 * @return Const shared handle. Empty if moved-from.
				 */
				const StormByte::Safe::Shared<StormByte::Buffer::WriteTelemetry> WriteTelemetry() const noexcept;

			private:
				/**
				 * @brief Initialize an unattached session in the Buffer provider.
				 */
				Bridge() noexcept;

				/**
				 * @brief Attach a non-owning non-IO source. Defined in this module.
				 * @param in Source. Must outlive *this.
				 */
				void AttachNonIoIn(ReadOnly& in);

				/**
				 * @brief Attach a non-owning non-IO sink. Defined in this module.
				 * @param out Sink. Must outlive *this.
				 */
				void AttachNonIoOut(WriteOnly& out);

				/**
				 * @brief Snapshot the stolen IO source telemetry handle.
				 *
				 * Runs in this module so the @c Shared copy does not
				 * cross the DLL boundary from an inline template.
				 */
				void CacheReadTelemetry() noexcept;

				/**
				 * @brief Snapshot the stolen IO sink telemetry handle.
				 *
				 * Runs in this module so the @c Shared copy does not
				 * cross the DLL boundary from an inline template.
				 */
				void CacheWriteTelemetry() noexcept;

				/**
				 * @brief Detach non-IO tips and drop stolen leaves. Does not change @ref State.
				 */
				void ReleaseTips() noexcept;

				/**
				 * @brief EoF of the current read tip. Caller holds @c m_mutex.
				 * @return @c true if the tip reports EoF or there is no tip.
				 */
				bool SourceEoF() const noexcept;

				/**
				 * @brief Pull up to @p n into @p dest according to @p operation.
				 * @param n Requested bytes.
				 * @param dest Scratch FIFO.
				 * @param operation Read-side wait policy.
				 * @return Status and count. @c Failed if the source is not readable.
				 */
				IO::Result Pull(StormByte::ByteSize n, FIFO& dest, Operation operation);

				/**
				 * @brief Push @p src. Retry TryAgain until Ok, End or fail.
				 * @param src Bytes to write.
				 * @return Status. @c Failed if the sink is not writable.
				 */
				IO::Result Push(FIFO& src);

				/**
				 * @brief Steal an IO source onto Base's heap.
				 * @tparam In Concrete @ref IO::BufferedReader leaf.
				 * @param in Source. Moved-from is empty.
				 */
				template<typename In>
				STORMBYTE_FORCE_INLINE void AttachIoIn(In&& in) {
					m_io_in_blocking = (in.ReadAhead() == StormByte::ByteSize{0});
					m_io_in = StormByte::Safe::Unique<IO::BufferedReader>::MakePointer<std::remove_cvref_t<In>>(
						std::forward<In>(in));
					CacheReadTelemetry();
				}

				/**
				 * @brief Steal an IO sink onto Base's heap.
				 * @tparam Out Concrete @ref IO::BufferedWriter leaf.
				 * @param out Sink. Moved-from is empty.
				 */
				template<typename Out>
				STORMBYTE_FORCE_INLINE void AttachIoOut(Out&& out) {
					m_io_out = StormByte::Safe::Unique<IO::BufferedWriter>::MakePointer<std::remove_cvref_t<Out>>(
						std::forward<Out>(out));
					CacheWriteTelemetry();
				}

				/**
				 * @brief Borrowed non-IO source.
				 */
				ReadOnly* m_ext_in {nullptr};
				/**
				 * @brief Borrowed non-IO sink.
				 */
				WriteOnly* m_ext_out {nullptr};
				/**
				 * @brief Stolen IO source on Base's heap.
				 */
				StormByte::Safe::Unique<IO::BufferedReader> m_io_in;
				/**
				 * @brief Stolen IO sink on Base's heap.
				 */
				StormByte::Safe::Unique<IO::BufferedWriter> m_io_out;
				/**
				 * @brief Cached shared read counters.
				 */
				StormByte::Safe::Shared<StormByte::Buffer::ReadTelemetry> m_owned_read;
				/**
				 * @brief Cached shared write counters.
				 */
				StormByte::Safe::Shared<StormByte::Buffer::WriteTelemetry> m_owned_write;
				/**
				 * @brief Whether the stolen reader had zero ReadAhead at attachment.
				 */
				bool m_io_in_blocking {false};
				/**
				 * @typedef SessionState
				 * @brief Unambiguous enum type for the private session state member.
				 */
				using SessionState = enum State;

				/**
				 * @brief Session lifetime.
				 */
				SessionState m_session_state {SessionState::Open};
				/**
				 * @brief Session lock.
				 */
				mutable std::mutex m_mutex;
		};
	}
}

/**
 * @brief Session ownership uses Base heap handles and Buffer-defined release operations.
 * @note Buffer, Base and all concrete IO providers must remain loaded with a compatible ABI.
 *       Borrowed tips must outlive the session. Stolen leaves must independently provide
 *       module-correct construction and virtual destruction; derived Bridge types are not classified.
 */
STORMBYTE_DECLARE_MAYBE_SAFE(StormByte::Buffer::Bridge);
