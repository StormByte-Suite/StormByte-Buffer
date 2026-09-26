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
#include <StormByte/buffer/io/typedefs.hxx>
#include <StormByte/buffer/typedefs.hxx>
#include <StormByte/buffer/visibility.h>

#include <chrono>
#include <cstddef>
#include <memory>
#include <span>

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
				 * @class BufferedWriter
				 * @brief Private implementation of @ref StormByte::Buffer::IO::BufferedWriter.
				 */
				class BufferedWriter;

				/**
				 * @class Bridge
				 * @brief Private pump for @ref StormByte::Buffer::Bridge.
				 */
				class Bridge;
			}

			/**
			 * @class BufferedWriter
			 * @brief Coordinated binary write sink with optional chunked write-behind
			 *        and a delayed-seek page map.
			 *
			 * Public base for byte destinations. Leaves implement the
			 * @c Origin* hooks and may override @ref Setup, @ref Seek,
			 * @ref Size and @ref WillWrite. They do not override
			 * @c Write, @c Flush, @c Open, @c Close, @c Rewind or
			 * @c Truncate.
			 *
			 * @par Binary only
			 * Octets only. No text mode.
			 *
			 * @par Session
			 * Construction is @ref State::Unavailable. A successful
			 * @ref Open moves to @ref State::Idle. @ref Close is
			 * idempotent, always @ref Flush then @ref OriginClose, and
			 * returns to @ref State::Unavailable on success or @ref State::Fault if
			 * Flush failed. @ref Open is not idempotent. @c Close then
			 * @c Open is a valid round-trip. Destructor of a leaf must
			 * call @ref Close while the leaf vtable is live.
			 *
			 * @c operator bool is true when @ref State is Idle.
			 *
			 * @par Write
			 * @c Write consumes the whole visible source or nothing
			 * (atomic). @c FIFO is read from the current read position
			 * (@c FIFO::Read is const; the cursor is mutable). The FIFO
			 * / span is left untouched on @ref Status::TryAgain,
			 * @ref Status::Failed and @ref Status::Error.
			 *
			 * @par Lazy write / MaxMemory
			 * Every @c Write is lazy until @ref MaxMemory.
			 * The origin is touched only when dirty pages exceed
			 * @ref MaxMemory (GC), or on @ref Flush / @ref Close.
			 * @c MaxMemory == 0 stores no pages: each @c Write goes to
			 * the origin (and to the ring when both ring knobs are on).
			 *
			 * This is not magic RAM. Local seeks and seeks into a
			 * still-dirty past stay in the map. Distant random writes
			 * need a larger @ref MaxMemory or the farthest-past island
			 * is evicted to the origin (that eviction is real I/O and
			 * may block). A jump far ahead is supported only while it
			 * fits; it is not the typical case. The typical case is a
			 * correction near the write high-water and a seek back to
			 * that front.
			 *
			 * GC evicts the farthest past first, in a contiguous run,
			 * so one @ref OriginSeek covers a cheap sequential push.
			 * Future islands are evicted only when no evictable past
			 * remains.
			 *
			 * A durable-progress ratio is
			 * @c Materialized / HighWater when @c HighWater > 0.
			 * @c HighWater is the maximum logical cursor this session.
			 * @c Tell is the cursor and may sit behind HighWater.
			 * @c Materialized is what the origin already holds.
			 * Do not use @c Accepted or @c Tell as the denominator.
			 * After a sequential session Close,
			 * @c Materialized equals HighWater.
			 *
			 * @par WriteChunk / BackPressure
			 * Either ring knob @c 0 disables the drain pipe.
			 * Both @c > 0 enable an internal SPSC ring used only to
			 * push a GC / Flush / Close run. Capacity is
			 * @c BackPressure * WriteChunk bytes. A @c Write that would
			 * exceed that cap returns @ref Status::TryAgain.
			 * The worker does not empty the page map just because the
			 * origin cursor is aligned.
			 *
			 * Setters take effect immediately. Turning the ring off or
			 * lowering the cap below @ref Dirty flushes dirty bytes
			 * first and may block. Setting @ref MaxMemory does not
			 * Flush and does not resize the ring.
			 *
			 * @par Flush / Truncate
			 * @ref Flush blocks, materialises every dirty page in
			 * offset order (with the @ref OriginSeek calls that need),
			 * drains the ring, calls @ref OriginFlush, and never
			 * returns @ref Status::TryAgain. Flush is not where seek
			 * elision happens.
			 * @ref Truncate drops the map and the ring without pushing
			 * and calls @ref OriginTruncate. @ref Tell, HighWater and
			 * Materialized become 0.
			 *
			 * @par Seek / Size
			 * @ref Seek moves only @ref Tell. It does not call
			 * @ref OriginSeek and does not Flush. Default @ref Seek
			 * fails when the leaf has no usable @ref OriginSeek
			 * (the base hook fails). A file leaf uses the base
			 * implementation.
			 *
			 * Typical correction inside resident dirty pages is O(1)
			 * with respect to the device. A @c Write / @c Seek that
			 * trips GC may block on origin I/O on purpose: random
			 * access on a slow device is more expensive than that wait.
			 *
			 * Default @ref Size is @ref Tell (includes dirty pages).
			 * A file leaf returns max(filesystem size, Tell).
			 *
			 * @par MaxWait
			 * Applies to the next @ref OriginPush (direct @c Write or worker).
			 * @c 0ms waits without limit. The ring itself is not timed.
			 * Setting @c MaxWait does not abort an in-flight push.
			 *
			 * @par WillWrite
			 * Protected probe used by @c Backend::Bridge. Default asks
			 * the ring cap. Leaves may tighten it (disk space, socket).
			 * The answer is indicative: another process, quotas or a
			 * network filesystem can still make the later @c Write fail.
			 *
			 * @par Telemetry
			 * @ref Telemetry copies counters under the coordinator lock.
			 * Accumulators start at construction and do not reset on Close.
			 * @c Materialized and @c HighWater are levels, not accumulators.
			 *
			 * @par Movable, not copyable
			 * Move transfers @c m_io. The worker is not stopped. Moved-from
			 * is Unavailable.
			 *
			 * @see Status, State, Result, FIFO, Backend::BufferedWriter
			 */
			class STORMBYTE_BUFFER_PUBLIC BufferedWriter {
				friend class Backend::BufferedWriter;
				friend class Backend::Bridge;

				public:
					/**
					 * @struct Telemetry
					 * @brief Session telemetry. One @ref Telemetry() call, one coherent copy.
					 *
					 * Byte fields are @ref StormByte::ByteSize. Event counts are
					 * @c std::size_t. Waits are @c std::chrono::nanoseconds.
					 * Accumulators start at construction and do not reset on
					 * Close / Rewind / Open / Truncate.
					 *
					 * @c Accepted == @c Behind + @c Direct.
					 * Durable progress is @c Materialized / HighWater when
					 * HighWater > 0. Mean wait is @c WaitTotal / @c WaitSamples
					 * when samples > 0.
					 *
					 * Seek elision uses the same names as
					 * @ref StormByte::Buffer::IO::BufferedReader::Telemetry.
					 * An epoch is one logical @ref Seek until the next
					 * @ref Seek or @ref Close. @ref Flush does not close
					 * an epoch.
					 */
					struct Telemetry {
						/**
						 * @brief Octets accepted by a successful @ref Write.
						 */
						StormByte::ByteSize Accepted {};

						/**
						 * @brief Of @ref Accepted, octets that did not hit the origin on the caller thread.
						 */
						StormByte::ByteSize Behind {};

						/**
						 * @brief Of @ref Accepted, octets pushed on the caller thread.
						 */
						StormByte::ByteSize Direct {};

						/**
						 * @brief Octets pushed through @ref OriginPush since construction.
						 */
						StormByte::ByteSize Origin {};

						/**
						 * @brief Durable origin length now. Not an accumulator.
						 *
						 * What the device already holds. Overwrites do not
						 * inflate this. After Flush / Close of a sequential
						 * session it equals HighWater.
						 */
						StormByte::ByteSize Materialized {};

						/**
						 * @brief Maximum logical @ref Tell since Open / Truncate.
						 *
						 * Durable progress is Materialized / HighWater when
						 * HighWater > 0. Do not divide by Tell.
						 */
						StormByte::ByteSize HighWater {};

						/**
						 * @brief Writes that landed in a resident page at or after the previous high-water.
						 */
						StormByte::ByteSize HitAhead {};

						/**
						 * @brief Writes that landed in a resident page behind the high-water.
						 */
						StormByte::ByteSize HitBack {};

						/**
						 * @brief Writes that created or extended a page (origin hole).
						 */
						StormByte::ByteSize Miss {};

						/**
						 * @brief Octets not yet on the origin (page map plus drain pipe).
						 */
						StormByte::ByteSize Dirty {};

						/**
						 * @brief Maximum @ref Dirty since construction.
						 */
						StormByte::ByteSize DirtyPeak {};

						/**
						 * @brief Ring cap in bytes at this snapshot, or 0 if the ring is off.
						 *
						 * Not @ref MaxMemory.
						 */
						StormByte::ByteSize Cap {};

						/**
						 * @brief Logical @ref Seek calls (Tell only).
						 */
						std::size_t SeekLogical {0};

						/**
						 * @brief @ref OriginSeek calls.
						 */
						std::size_t SeekOrigin {0};

						/**
						 * @brief Closed epochs with no @ref OriginSeek.
						 */
						std::size_t SeekSavedFull {0};

						/**
						 * @brief Closed epochs that hit dirty pages and later needed @ref OriginSeek.
						 */
						std::size_t SeekSavedPartial {0};

						/**
						 * @brief Times @ref Write returned TryAgain.
						 */
						std::size_t TryAgain {0};

						/**
						 * @brief Times Dirty reached @ref Cap while Cap > 0.
						 */
						std::size_t Saturated {0};

						/**
						 * @brief Times GC materialised a page because Dirty exceeded MaxMemory.
						 */
						std::size_t Evicted {0};

						/**
						 * @brief Shortest sampled Write wait. 0 if WaitSamples == 0.
						 */
						std::chrono::nanoseconds WaitMin {};

						/**
						 * @brief Longest sampled Write wait. 0 if WaitSamples == 0.
						 */
						std::chrono::nanoseconds WaitMax {};

						/**
						 * @brief Sum of sampled waits.
						 */
						std::chrono::nanoseconds WaitTotal {};

						/**
						 * @brief Sampled waits (accepted Write or timed OriginPush). Not instant TryAgain.
						 */
						std::size_t WaitSamples {0};
					};

					/**
					 * @name Lifecycle
					 * @{
					 */

					/**
					 * @brief Copy constructor is deleted.
					 */
					BufferedWriter(const BufferedWriter&) = delete;

					/**
					 * @brief Move constructor.
					 * @param other Instance to take from. Left Unavailable.
					 */
					BufferedWriter(BufferedWriter&& other) noexcept;

					/**
					 * @brief Virtual destructor. Stops the worker. Does not call Origin*.
					 *
					 * Leaves must call @ref Close in their destructor.
					 */
					virtual ~BufferedWriter() noexcept;

					/**
					 * @brief Copy assignment is deleted.
					 * @return *this.
					 */
					BufferedWriter& operator=(const BufferedWriter&) = delete;

					/**
					 * @brief Move assignment.
					 * @param other Instance to take from. Left Unavailable.
					 * @return *this.
					 */
					BufferedWriter& operator=(BufferedWriter&& other) noexcept;

					/**
					 * @}
					 */

					/**
					 * @brief Whether the sink is prepared to write.
					 * @return @c true if @ref State is @ref State::Idle.
					 */
					virtual explicit operator bool() const noexcept final;

					/**
					 * @brief Session state.
					 * @return Current @ref State.
					 */
					virtual enum State State() const noexcept final;

					/**
					 * @name Session
					 * @{
					 */

					/**
					 * @brief Arm the origin.
					 * @return @c true if @ref State is Idle afterwards.
					 *
					 * Calls @ref Setup then the backend @c Open.
					 * Not idempotent.
					 */
					virtual bool Open() final;

					/**
					 * @brief Flush then close the origin.
					 * @return @c true if @ref State is Unavailable afterwards.
					 *
					 * Idempotent on an already closed instance. Blocking.
					 * Flush failure leaves @ref State::Fault and returns false.
					 */
					virtual bool Close() final;

					/**
					 * @brief Re-arm: @ref Close then @ref Open when currently armed.
					 * @return @c true if Idle afterwards.
					 */
					virtual bool Rewind() final;

					/**
					 * @brief Whether @ref Open succeeded and @ref Close has not.
					 * @return @c true if the session is armed.
					 */
					virtual bool IsOpen() const noexcept final;

					/**
					 * @brief Materialise every dirty page, drain the ring and @ref OriginFlush.
					 * @return @ref Status::Ok, @ref Status::Error or
					 *         @ref Status::Failed. Never @ref Status::TryAgain.
					 *
					 * Blocking. @ref Tell is unchanged.
					 */
					virtual Result Flush() final;

					/**
					 * @brief Drop dirty pages and the ring, then truncate the origin.
					 * @return @ref Status::Ok or @ref Status::Failed.
					 *
					 * Does not push. Sets @ref Tell, HighWater and Materialized to 0.
					 */
					virtual Result Truncate() final;

					/**
					 * @}
					 */

					/**
					 * @name Write
					 * @{
					 */

					/**
					 * @brief Write every unread byte of @p src.
					 * @param src Source FIFO. Read from the current position.
					 * @return Status and bytes accepted. Source untouched unless Ok.
					 */
					virtual Result Write(const FIFO& src) final;

					/**
					 * @brief Write every unread byte of @p src.
					 * @param src Source FIFO. Read from the current position.
					 * @return Status and bytes accepted. Source untouched unless Ok.
					 */
					virtual Result Write(FIFO& src) final;

					/**
					 * @brief Write the whole span.
					 * @param src Octets to copy.
					 * @return Status and bytes accepted. Source untouched unless Ok.
					 */
					virtual Result Write(std::span<const std::byte> src) final;

					/**
					 * @}
					 */

					/**
					 * @name Position
					 * @{
					 */

					/**
					 * @brief Logical write offset.
					 * @return Cursor published to the caller. Includes unflushed pages.
					 */
					virtual StormByte::ByteSize Tell() const noexcept;

					/**
					 * @brief Bytes not yet on the origin.
					 * @return Page map plus drain pipe. 0 after a successful Flush.
					 */
					virtual StormByte::ByteSize Dirty() const noexcept;

					/**
					 * @brief Logical sink length in bytes.
					 * @return Length. Default is @ref Tell (includes dirty pages).
					 *
					 * Not optional. A file leaf returns
					 * max(filesystem size, Tell). A remote leaf overrides
					 * this.
					 */
					virtual StormByte::ByteSize Size() const noexcept;

					/**
					 * @brief Move the write cursor.
					 * @param offset Byte offset.
					 * @param mode @ref Position::Absolute or @ref Position::Relative.
					 * @return @ref Status::Ok or @ref Status::Failed.
					 *
					 * Updates @ref Tell only. Does not Flush. Does not call
					 * @ref OriginSeek. Fails when the leaf @ref OriginSeek
					 * cannot move the device (default hook). O(1) for a
					 * typical in-cache correction; may block later when a
					 * Write trips GC.
					 */
					virtual Result Seek(std::ptrdiff_t offset, Position mode);

					/**
					 * @}
					 */

					/**
					 * @name Telemetry
					 * @{
					 */

					/**
					 * @brief Copy current telemetry.
					 * @return Snapshot. Does not push to the origin.
					 */
					virtual const struct Telemetry Telemetry() const noexcept final;

					/**
					 * @}
					 */

					/**
					 * @name Policy
					 * @{
					 */

					/**
					 * @brief Configured origin push unit.
					 * @return Bytes. 0 disables the ring (with BackPressure 0).
					 */
					virtual StormByte::ByteSize WriteChunk() const noexcept;

					/**
					 * @brief Set origin push unit. Takes effect immediately.
					 * @param bytes Chunk size. 0 disables the ring.
					 *
					 * May block. A zero value or a smaller unit that now meets
					 * dirty bytes drains the ring through @ref Flush / the worker
					 * before the setter returns. Not deferred to the next Write.
					 */
					virtual void WriteChunk(StormByte::ByteSize bytes);

					/**
					 * @brief Configured dirty cap in WriteChunk units.
					 * @return Chunk count. 0 disables the ring.
					 */
					virtual std::size_t BackPressure() const noexcept;

					/**
					 * @brief Set dirty cap in WriteChunk units. Takes effect immediately.
					 * @param chunks 0 disables the ring. Otherwise cap is chunks * WriteChunk.
					 *
					 * May block. Zero, or a cap below @ref Dirty, flushes dirty
					 * bytes before the setter returns. Not deferred to the next Write.
					 */
					virtual void BackPressure(std::size_t chunks);

					/**
					 * @brief Byte budget for dirty pages that are not yet on the origin.
					 * @return Bytes. 0 stores no page map (eager origin writes).
					 *
					 * Independent of @ref WriteChunk / @ref BackPressure.
					 */
					virtual StormByte::ByteSize MaxMemory() const noexcept;

					/**
					 * @brief Set the dirty-page budget. Takes effect immediately.
					 * @param bytes 0 disables the page map.
					 *
					 * Does not Flush. Does not resize the ring. A value below
					 * current dirty pages trips GC and may block.
					 */
					virtual void MaxMemory(StormByte::ByteSize bytes);

					/**
					 * @brief Wait cap for OriginPush.
					 * @return @c 0ms waits without limit.
					 */
					virtual std::chrono::milliseconds MaxWait() const noexcept;

					/**
					 * @brief Set wait cap for OriginPush. Takes effect on the next push.
					 * @param wait @c 0ms = unlimited.
					 *
					 * Does not cancel an in-flight @ref OriginPush. Does not Flush.
					 */
					virtual void MaxWait(std::chrono::milliseconds wait);

					/**
					 * @}
					 */

				protected:
					/**
					 * @brief Construct an unopened coordinator (@ref State::Unavailable).
					 * @param write_chunk Initial @ref WriteChunk in bytes.
					 * @param back_pressure Initial @ref BackPressure in chunk units.
					 * @param max_wait Initial @ref MaxWait.
					 * @param max_memory Initial @ref MaxMemory in bytes.
					 */
					explicit BufferedWriter(StormByte::ByteSize write_chunk = 0, std::size_t back_pressure = 0,
						std::chrono::milliseconds max_wait = std::chrono::milliseconds{0},
						StormByte::ByteSize max_memory = 0);

					/**
					 * @brief Publish session state from a leaf hook.
					 * @param state New @ref State.
					 */
					void SetState(enum State state) noexcept;

					/**
					 * @brief Publish the logical write offset from a leaf @ref Seek.
					 * @param offset New @ref Tell.
					 */
					void SetTell(StormByte::ByteSize offset) noexcept;

					/**
					 * @brief Leaf policy hook. Called from @ref Open before the origin.
					 *
					 * Default does nothing. File uses it for the path-only ctor.
					 * The most-derived vtable is live.
					 */
					virtual void Setup();

					/**
					 * @brief Whether @p n more bytes can be accepted now.
					 * @param n Byte count to probe.
					 * @return @c true if the ring (or direct mode) can take @p n.
					 *
					 * Indicative. Another writer, quotas or the filesystem can
					 * still reject the later @c Write. Override to tighten
					 * (disk space, socket window). Used by @c Backend::Bridge.
					 */
					virtual bool WillWrite(StormByte::ByteSize n) const;

					/**
					 * @name Origin hooks
					 * @{
					 */

					/**
					 * @brief Arm the device and @ref SetState.
					 * @return @ref Status::Ok or @ref Status::Failed.
					 */
					virtual Result OriginOpen() = 0;

					/**
					 * @brief Release the device and @ref SetState Unavailable.
					 * @return @ref Status::Ok or @ref Status::Failed.
					 */
					virtual Result OriginClose() = 0;

					/**
					 * @brief Write @p data to the device. Do not buffer in the hook.
					 * @param data Contiguous octets. May be a full WriteChunk or a tail.
					 * @return Ok with count == data.size(), Ok with a short count
					 *         (backend retries), Error or Failed.
					 */
					virtual Result OriginPush(std::span<const std::byte> data) = 0;

					/**
					 * @brief Make accepted bytes visible on the device.
					 * @return @ref Status::Ok, Error or Failed.
					 *
					 * The base calls this after a completed direct Write and after
					 * Flush has drained the ring. Do not buffer here.
					 */
					virtual Result OriginFlush() = 0;

					/**
					 * @brief Discard origin contents. Network may no-op Ok.
					 * @return @ref Status::Ok or @ref Status::Failed.
					 */
					virtual Result OriginTruncate() = 0;

					/**
					 * @brief Seek the origin to @p absolute.
					 * @param absolute Byte offset from the start.
					 * @return @ref Status::Ok or @ref Status::Failed.
					 *
					 * Default fails. File and remote leaves override this.
					 * Public @ref Seek does not call this; GC / Flush / Close do.
					 */
					virtual Result OriginSeek(StormByte::ByteSize absolute);

					/**
					 * @}
					 */

				private:
					std::unique_ptr<Backend::BufferedWriter> m_io;	///< Private coordinator state.
			};
		}
	}
}
