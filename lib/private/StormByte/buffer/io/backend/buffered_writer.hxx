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

#include <StormByte/binary_data.hxx>
#include <StormByte/byte_size.hxx>
#include <StormByte/buffer/fifo.hxx>
#include <StormByte/buffer/io/buffered_writer.hxx>
#include <StormByte/buffer/io/typedefs.hxx>
#include <StormByte/buffer/visibility.h>

#include <atomic>
#include <chrono>
#include <condition_variable>
#include <cstddef>
#include <map>
#include <memory>
#include <mutex>
#include <span>
#include <thread>

namespace StormByte::Buffer {
	class LockFreeRing;
}

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
				 *
				 * Owns session flags, the optional SPSC drain ring, the
				 * dirty page map, the logical cursor and the push worker.
				 * Invokes @c Origin* hooks on @c m_owner.
				 * Origin hooks never run while @c m_mutex is held.
				 * Every Origin hook runs under @c m_origin_io so the
				 * worker and the write thread cannot share the device
				 * cursor (stdio FILE* is not thread-safe).
				 *
				 * @par Page map
				 * When @c m_max_memory > 0, @c Write lands in @c m_pages
				 * keyed by absolute offset. Overlap and abut merge.
				 * Interior writes overwrite. The worker must not drain the
				 * map just because the origin cursor is aligned.
				 *
				 * @par GC
				 * Victim is the farthest-past span (lowest offset at or
				 * behind the high-water). Contiguous following past spans
				 * go in the same OriginSeek + push. Future islands wait
				 * until no evictable past remains. Evict is Origin I/O.
				 *
				 * @par Seek epoch
				 * @c Seek updates Tell only and opens/closes epochs for
				 * SeekSavedFull / SeekSavedPartial. Prefetch analog: the
				 * worker does not chase the logical cursor.
				 *
				 * @par Origin cursor
				 * @c OriginFlush may leave the device cursor untrusted
				 * (observed on Darwin). @c EnsureOrigin then OriginSeek's
				 * even when @c m_origin_pos already equals the target.
				 * A sequential ring drain after that first realign does
				 * not OriginSeek again. Logical @c Seek still does not
				 * touch the device.
				 */
				class STORMBYTE_BUFFER_PRIVATE BufferedWriter {
					public:
						/**
						 * @name Lifecycle
						 * @{
						 */

						/**
						 * @brief Bind to the public leaf and store policy knobs.
						 * @param owner Public instance (the most-derived object).
						 * @param write_chunk Initial WriteChunk in bytes.
						 * @param back_pressure Initial BackPressure in chunks.
						 * @param max_wait Initial MaxWait.
						 * @param max_memory Initial MaxMemory in bytes.
						 *
						 * Starts the worker thread. State is @ref State::Unavailable.
						 */
						BufferedWriter(IO::BufferedWriter& owner, StormByte::ByteSize write_chunk,
							std::size_t back_pressure, std::chrono::milliseconds max_wait,
							StormByte::ByteSize max_memory);

						/**
						 * @brief Copy constructor is deleted.
						 */
						BufferedWriter(const BufferedWriter&) = delete;

						/**
						 * @brief Move constructor is deleted.
						 */
						BufferedWriter(BufferedWriter&&) = delete;

						/**
						 * @brief Stop the worker. Does not call OriginClose.
						 */
						~BufferedWriter();

						/**
						 * @brief Copy assignment is deleted.
						 * @return *this.
						 */
						BufferedWriter& operator=(const BufferedWriter&) = delete;

						/**
						 * @brief Move assignment is deleted.
						 * @return *this.
						 */
						BufferedWriter& operator=(BufferedWriter&&) = delete;

						/**
						 * @}
						 */

						/**
						 * @brief Point hooks at a new public instance after a move.
						 * @param owner Destination public object.
						 */
						void Rebind(IO::BufferedWriter& owner) noexcept;

						/**
						 * @brief Whether the sink is prepared to write.
						 * @return @c true if @ref State is @ref State::Idle.
						 */
						explicit operator bool() const noexcept;

						/**
						 * @brief Session state.
						 * @return Current @ref State.
						 */
						enum State State() const noexcept;

						/**
						 * @brief Publish session state from a leaf hook.
						 * @param state New @ref State.
						 */
						void SetState(enum State state) noexcept;

						/**
						 * @brief Publish the logical write offset from a leaf Seek.
						 * @param offset New Tell.
						 */
						void SetTell(StormByte::ByteSize offset) noexcept;

						/**
						 * @name Session
						 * @{
						 */

						/**
						 * @brief Arm the origin.
						 * @return @c true if @ref State is Idle afterwards.
						 */
						bool Open();

						/**
						 * @brief Flush then close the origin.
						 * @return @c true if Unavailable afterwards; @c false on Fault.
						 */
						bool Close();

						/**
						 * @brief Join the worker and drop the ring. Does not call Origin*.
						 */
						void Shutdown();

						/**
						 * @brief Close then Open when currently armed.
						 * @return @c true if Idle afterwards.
						 */
						bool Rewind();

						/**
						 * @brief Whether the session is armed.
						 * @return @c true after a successful Open until Close.
						 */
						bool IsOpen() const noexcept;

						/**
						 * @brief Materialise every dirty page, drain the ring, OriginFlush.
						 * @return @ref Status::Ok, @ref Status::Error or @ref Status::Failed.
						 *         Never @ref Status::TryAgain.
						 */
						Result Flush();

						/**
						 * @brief Drop the map and the ring without pushing, truncate the origin.
						 * @return @ref Status::Ok or @ref Status::Failed.
						 */
						Result Truncate();

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
						Result Write(const FIFO& src);

						/**
						 * @brief Write every unread byte of @p src.
						 * @param src Source FIFO. Read from the current position.
						 * @return Status and bytes accepted. Source untouched unless Ok.
						 */
						Result Write(FIFO& src);

						/**
						 * @brief Write the whole span.
						 * @param src Octets to copy.
						 * @return Status and bytes accepted. Source untouched unless Ok.
						 */
						Result Write(std::span<const std::byte> src);

						/**
						 * @}
						 */

						/**
						 * @brief Logical write offset.
						 * @return Cursor including unflushed pages.
						 */
						StormByte::ByteSize Tell() const noexcept;

						/**
						 * @brief Bytes not yet on the origin.
						 * @return Page map plus drain pipe.
						 */
						StormByte::ByteSize Dirty() const noexcept;

						/**
						 * @brief Move only the logical cursor.
						 * @param offset Byte offset.
						 * @param mode Absolute or Relative.
						 * @return Ok or Failed. Does not OriginSeek.
						 */
						Result Seek(std::ptrdiff_t offset, Position mode);

						/**
						 * @name Telemetry
						 * @{
						 */

						/**
						 * @brief Copy current telemetry under @c m_mutex.
						 * @return Snapshot. Does not push.
						 */
						struct IO::BufferedWriter::Telemetry Telemetry() const noexcept;

						/**
						 * @}
						 */

						/**
						 * @brief Configured origin push unit.
						 * @return Bytes. 0 disables the ring.
						 */
						StormByte::ByteSize WriteChunk() const noexcept;

						/**
						 * @brief Set origin push unit.
						 * @param bytes Chunk size. 0 disables the ring.
						 */
						void WriteChunk(StormByte::ByteSize bytes);

						/**
						 * @brief Configured dirty cap in WriteChunk units.
						 * @return Chunk count. 0 disables the ring.
						 */
						std::size_t BackPressure() const noexcept;

						/**
						 * @brief Set dirty cap in WriteChunk units.
						 * @param chunks 0 disables the ring.
						 */
						void BackPressure(std::size_t chunks);

						/**
						 * @brief Page-map budget.
						 * @return Bytes. 0 stores no pages.
						 */
						StormByte::ByteSize MaxMemory() const noexcept;

						/**
						 * @brief Set the page-map budget. May GC if below Dirty.
						 * @param bytes 0 disables the page map.
						 */
						void MaxMemory(StormByte::ByteSize bytes);

						/**
						 * @brief Wait cap for OriginPush.
						 * @return @c 0ms waits without limit.
						 */
						std::chrono::milliseconds MaxWait() const noexcept;

						/**
						 * @brief Set wait cap for OriginPush.
						 * @param wait @c 0ms = unlimited.
						 */
						void MaxWait(std::chrono::milliseconds wait);

						/**
						 * @brief Whether @p n bytes fit under the current ring cap.
						 * @param n Prospective Write size.
						 * @return @c true in direct mode, or if ring Dirty + n <= cap.
						 */
						bool WillWrite(StormByte::ByteSize n) const noexcept;

					private:
						/**
						 * @brief Dirty page: absolute start and payload.
						 */
						struct Page {
							StormByte::ByteSize offset {0};	///< First byte of this span.
							BinaryData bytes;					///< Octets not yet on the origin.
						};

						/**
						 * @brief Whether both ring knobs are on.
						 * @return @c true if WriteChunk and BackPressure are non-zero.
						 */
						bool BufferedMode() const noexcept;

						/**
						 * @brief Whether the page map is enabled.
						 * @return @c true if MaxMemory > 0.
						 */
						bool PageMode() const noexcept;

						/**
						 * @brief Ring cap in bytes.
						 * @return BackPressure * WriteChunk, or 0 if direct.
						 */
						StormByte::ByteSize PendingCap() const noexcept;

						/**
						 * @brief Occupancy of @c m_pages.
						 * @return Sum of page sizes.
						 */
						StormByte::ByteSize PageDirty() const noexcept;

						/**
						 * @brief Page map plus ring occupancy.
						 * @return Bytes not on the origin.
						 */
						StormByte::ByteSize TotalDirty() const noexcept;

						/**
						 * @brief Whether @p bytes fit under the ring cap.
						 * @param bytes Payload size of the prospective Write.
						 * @return @c true if the Write may proceed.
						 */
						bool WouldAccept(StormByte::ByteSize bytes) const noexcept;

						/**
						 * @brief Start @c m_worker if it is not joinable.
						 */
						void StartWorker();

						/**
						 * @brief Signal stop and join @c m_worker.
						 */
						void StopWorker();

						/**
						 * @brief Wake the worker to drain a materialised run.
						 */
						void RequestDrain() const;

						/**
						 * @brief Worker loop: wait, OriginPush ring spans, park.
						 */
						void Worker();

						/**
						 * @brief OriginPush @p data, retry short writes, honor MaxWait.
						 * @param data Contiguous octets.
						 * @return Ok when every byte was pushed; Error or Failed otherwise.
						 *
						 * Caller already holds @c m_origin_io.
						 */
						Result PushAll(std::span<const std::byte> data) const;

						/**
						 * @brief Shared implementation of the public Write overloads.
						 * @param src Octets to accept.
						 * @return Status and bytes accepted.
						 */
						Result WriteSpan(std::span<const std::byte> src);

						/**
						 * @brief Place @p src into the page map at @c m_tell.
						 * @param src Octets.
						 * @return Ok or Failed.
						 */
						Result StorePages(std::span<const std::byte> src);

						/**
						 * @brief Merge overlap / abut around @p offset.
						 * @param offset Page start to repair from.
						 */
						void Coalesce(StormByte::ByteSize offset);

						/**
						 * @brief Materialise pages until PageDirty <= MaxMemory.
						 * @return Ok, Error or Failed.
						 */
						Result CollectGarbage();

						/**
						 * @brief Materialise every page in offset order.
						 * @return Ok, Error or Failed.
						 */
						Result MaterializeAll();

						/**
						 * @brief Push one page to the origin.
						 * @param lock Coordinator lock held by the caller. Released during Origin*.
						 * @param it Page to start from. Invalidated on success.
						 * @return Ok, Error or Failed.
						 */
						Result MaterializeFrom(std::unique_lock<std::mutex>& lock,
							std::map<std::size_t, Page>::iterator it);

						/**
						 * @brief OriginSeek when the device cursor is untrusted or not @p absolute.
						 * @param absolute Device offset.
						 * @return Ok or Failed. Increments SeekOrigin on a real seek.
						 *
						 * Skips OriginSeek when @c m_origin_pos equals @p absolute
						 * and @c m_origin_cursor_dirty is false.
						 */
						Result EnsureOrigin(StormByte::ByteSize absolute);

						/**
						 * @brief Close the current seek epoch if one is open.
						 */
						void CloseSeekEpoch() noexcept;

						/**
						 * @brief Drop every page. Caller holds @c m_mutex.
						 */
						void ClearPages() noexcept;

						/**
						 * @brief Record a wait sample. Caller holds @c m_mutex.
						 * @param elapsed Duration of the Write that worked or waited on OriginPush.
						 */
						void NoteWait(std::chrono::nanoseconds elapsed) const noexcept;

						/**
						 * @brief Raise DirtyPeak and Saturated. Caller holds @c m_mutex.
						 */
						void NoteDirty() const noexcept;

						IO::BufferedWriter* m_owner;				///< Public leaf (hooks).

						mutable std::mutex m_mutex;					///< Session + knobs.
						mutable std::mutex m_origin_io;				///< Serialises every Origin* hook.
						mutable std::condition_variable m_cv;		///< Worker / flush waits.

						StormByte::ByteSize m_write_chunk {0};			///< Origin push unit.
						std::size_t m_back_pressure {0};			///< Cap in WriteChunk units.
						StormByte::ByteSize m_max_memory {0};			///< Page-map budget.
						std::chrono::milliseconds m_max_wait {0};	///< OriginPush wait cap.

						enum State m_state { State::Unavailable };	///< Session state.
						bool m_open {false};						///< Session armed.
						mutable bool m_failed {false};				///< Permanent failure.
						mutable StormByte::ByteSize m_tell {0};			///< Logical cursor.
						StormByte::ByteSize m_high_water {0};			///< Max Tell seen this session.
						bool m_origin_cursor_dirty {false};			///< OriginFlush may desync the fd.
						StormByte::ByteSize m_origin_pos {0};			///< Device cursor.
						StormByte::ByteSize m_materialized {0};			///< Durable origin length.

						std::map<std::size_t, Page> m_pages;		///< Dirty pages by offset.
						std::unique_ptr<LockFreeRing> m_ring;		///< Drain pipe. Null if ring off.

						bool m_epoch_open {false};					///< Logical seek pending close.
						bool m_epoch_hit {false};					///< Epoch wrote into a resident page.
						bool m_epoch_origin {false};				///< Epoch already OriginSeek'd.

						mutable StormByte::ByteSize m_accepted {0};		///< Telemetry.Accepted.
						mutable StormByte::ByteSize m_behind {0};		///< Telemetry.Behind.
						mutable StormByte::ByteSize m_direct {0};		///< Telemetry.Direct.
						mutable StormByte::ByteSize m_origin_bytes {0};	///< Telemetry.Origin.
						mutable StormByte::ByteSize m_hit_ahead {0};	///< Telemetry.HitAhead.
						mutable StormByte::ByteSize m_hit_back {0};		///< Telemetry.HitBack.
						mutable StormByte::ByteSize m_miss {0};			///< Telemetry.Miss.
						mutable StormByte::ByteSize m_dirty_peak {0};	///< Telemetry.DirtyPeak.
						mutable std::size_t m_seek_logical {0};		///< Telemetry.SeekLogical.
						mutable std::size_t m_seek_origin {0};		///< Telemetry.SeekOrigin.
						mutable std::size_t m_seek_saved_full {0};	///< Telemetry.SeekSavedFull.
						mutable std::size_t m_seek_saved_partial {0};	///< Telemetry.SeekSavedPartial.
						mutable std::size_t m_try_again {0};		///< Telemetry.TryAgain.
						mutable std::size_t m_saturated {0};		///< Telemetry.Saturated.
						mutable std::size_t m_evicted {0};			///< Telemetry.Evicted.
						mutable std::chrono::nanoseconds m_wait_min {0};	///< Telemetry.WaitMin.
						mutable std::chrono::nanoseconds m_wait_max {0};	///< Telemetry.WaitMax.
						mutable std::chrono::nanoseconds m_wait_total {0};	///< Telemetry.WaitTotal.
						mutable std::size_t m_wait_samples {0};		///< Telemetry.WaitSamples.

						mutable std::atomic<bool> m_stop {false};	///< Worker teardown.
						mutable std::atomic<bool> m_flush {false};	///< Drain entire ring.
						mutable bool m_drain_run {false};			///< Worker has work.
						std::thread m_worker;						///< Push thread.
				};
			}
		}
	}
}
