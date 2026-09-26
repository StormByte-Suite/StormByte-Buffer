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
#include <StormByte/buffer/io/buffered_reader.hxx>
#include <StormByte/buffer/io/typedefs.hxx>
#include <StormByte/buffer/typedefs.hxx>
#include <StormByte/buffer/visibility.h>

#include <atomic>
#include <chrono>
#include <condition_variable>
#include <map>
#include <mutex>
#include <optional>
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
				 * @class BufferedReader
				 * @brief Private implementation of @ref StormByte::Buffer::IO::BufferedReader.
				 *
				 * Owns session flags, @ref State, the cache map, the logical
				 * cursor and the prefetch worker. Invokes @c Origin* hooks on
				 * @c m_owner.
				 *
				 * A seekable origin keeps a map of owned spans keyed by stream
				 * offset. Seek updates @c m_tell only. @c m_origin_pos is the
				 * device. Prefetch only while those two match. PullAt at
				 * @c m_origin_pos is sequential; anywhere else is OriginSeek.
				 */
				class STORMBYTE_BUFFER_PRIVATE BufferedReader {
					public:
						/**
						 * @name Lifecycle
						 * @{
						 */

						/**
						 * @brief Bind to the public leaf and store policy knobs.
						 * @param owner Public instance (the most-derived object).
						 * @param read_ahead Initial @ref ReadAhead in bytes.
						 * @param max_memory Initial @ref MaxMemory in bytes.
						 * @param max_wait Initial @ref MaxWait. @c 0ms = unlimited.
						 *
						 * Starts the worker thread. State is @ref State::Unavailable.
						 */
						BufferedReader(IO::BufferedReader& owner, StormByte::ByteSize read_ahead,
							StormByte::ByteSize max_memory, std::chrono::milliseconds max_wait);

						/**
						 * @brief Copy constructor is deleted.
						 */
						BufferedReader(const BufferedReader&) = delete;

						/**
						 * @brief Move constructor is deleted.
						 */
						BufferedReader(BufferedReader&&) = delete;

						/**
						 * @brief Stop the worker. Does not call OriginClose.
						 */
						~BufferedReader();

						/**
						 * @brief Copy assignment is deleted.
						 */
						BufferedReader& operator=(const BufferedReader&) = delete;

						/**
						 * @brief Move assignment is deleted.
						 */
						BufferedReader& operator=(BufferedReader&&) = delete;

						/**
						 * @}
						 */

						/**
						 * @brief Point hooks at a new public instance after a move.
						 * @param owner Destination public object.
						 */
						void Rebind(IO::BufferedReader& owner) noexcept;

						/**
						 * @brief Cancel the in-flight pull and wait until the worker is idle.
						 *
						 * Call this before @ref Rebind on a move, while the leaf
						 * origin still belongs to the source object.
						 */
						void FlushPrefetch() const;

						/**
						 * @brief Whether the source is prepared to read.
						 * @return @ref IsReadable.
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
						 * @name Session
						 * @{
						 */

						/**
						 * @brief Arm the origin.
						 * @return @c true if @ref State is @ref State::Idle afterwards.
						 */
						bool Open();

						/**
						 * @brief Flush prefetch, drop caches, close the origin.
						 * @return @ref Status::Ok. Idempotent. State → Unavailable.
						 */
						Result Close();

						/**
						 * @brief Join the worker and drop caches. Does not call Origin*.
						 */
						void Shutdown();

						/**
						 * @brief @ref Close then @ref Open when currently armed.
						 * @return @c true if Idle afterwards.
						 */
						bool Rewind();

						/**
						 * @brief Whether the session is armed (not Unavailable-from-ctor/close).
						 * @return @c true after a successful Open until Close.
						 */
						bool IsOpen() const noexcept;

						/**
						 * @brief Whether a @c Read may still produce bytes.
						 * @return Idle and not @ref EoF.
						 */
						bool IsReadable() const noexcept;

						/**
						 * @brief Whether no further bytes can be produced.
						 * @return @c true when no cached byte remains at @ref Tell
						 *         and the origin is exhausted, or after @ref Close.
						 */
						bool EoF() const noexcept;

						/**
						 * @}
						 */

						/**
						 * @name Read
						 * @{
						 */

						/**
						 * @brief Consume @p n bytes into @p dest.
						 * @param n Requested count. Zero serves the current span.
						 * @param dest Caller FIFO.
						 * @return Status and bytes written to @p dest.
						 */
						Result Read(StormByte::ByteSize n, FIFO& dest) const;

						/**
						 * @brief Copy @p n bytes into @p dest without consuming.
						 * @param n Requested count. Zero copies the current span.
						 * @param dest Caller FIFO.
						 * @return Status and bytes written to @p dest.
						 */
						Result Peek(StormByte::ByteSize n, FIFO& dest) const;

						/**
						 * @}
						 */

						/**
						 * @name Position
						 * @{
						 */

						/**
						 * @brief Move @c m_tell only. Does not call OriginSeek.
						 * @param offset Byte offset.
						 * @param mode Absolute or relative.
						 * @return @ref Status::Ok or @ref Status::Failed.
						 */
						Result Seek(std::ptrdiff_t offset, Position mode) const;

						/**
						 * @brief Logical read offset in the stream.
						 * @return Bytes from the origin start.
						 */
						StormByte::ByteSize Tell() const noexcept;

						/**
						 * @brief Whether the leaf origin can seek.
						 * @return @c OriginCanSeek.
						 */
						bool IsSeekable() const noexcept;

						/**
						 * @}
						 */

						/**
						 * @name Size
						 * @{
						 */

						/**
						 * @brief Whether the leaf origin reports a length.
						 * @return @c OriginHasSize.
						 */
						bool IsSized() const noexcept;

						/**
						 * @brief Origin length when known.
						 * @return Length, or empty.
						 */
						std::optional<StormByte::ByteSize> Size() const noexcept;

						/**
						 * @}
						 */

						/**
						 * @name Telemetry
						 * @{
						 */

						/**
						 * @brief Copy current telemetry under @c m_mutex.
						 * @return Snapshot. Does not pull.
						 */
						struct IO::BufferedReader::Telemetry Telemetry() const noexcept;

						/**
						 * @}
						 */

						/**
						 * @name Policy
						 * @{
						 */

						/**
						 * @brief Configured prefetch length.
						 * @return Current ReadAhead.
						 */
						StormByte::ByteSize ReadAhead() const noexcept;

						/**
						 * @brief Set prefetch length.
						 * @param bytes New target. 0 disables prefetch.
						 */
						void ReadAhead(StormByte::ByteSize bytes);

						/**
						 * @brief Configured cache cap.
						 * @return Current MaxMemory.
						 */
						StormByte::ByteSize MaxMemory() const noexcept;

						/**
						 * @brief Set cache cap.
						 * @param bytes Approximate resident cap. 0 drops all spans.
						 */
						void MaxMemory(StormByte::ByteSize bytes);

						/**
						 * @brief Configured read wait limit.
						 * @return @c 0ms waits forever.
						 */
						std::chrono::milliseconds MaxWait() const noexcept;

						/**
						 * @brief Set read wait limit.
						 * @param wait @c 0ms = unlimited. Positive = timeout then TryAgain.
						 */
						void MaxWait(std::chrono::milliseconds wait);

						/**
						 * @}
						 */

					private:
						/**
						 * @brief Start @c m_worker if it is not joinable.
						 */
						void StartWorker();

						/**
						 * @brief Signal stop and join @c m_worker.
						 */
						void StopWorker();

						/**
						 * @brief Arm the worker only if Tell equals the device cursor.
						 */
						void RequestPrefetch() const;

						/**
						 * @brief Worker loop: wait for a target, pull, park.
						 */
						void Worker();

						/**
						 * @brief Drop every cached span.
						 */
						void DropCache() const;

						/**
						 * @brief Bytes stored across all spans.
						 * @return Sum of each span's available length.
						 */
						StormByte::ByteSize CachedBytes() const noexcept;

						/**
						 * @brief Contiguous cached bytes starting at @p pos.
						 * @param pos Stream offset.
						 * @return Length of the hit span from @p pos, or 0.
						 */
						StormByte::ByteSize CoverageFrom(StormByte::ByteSize pos) const noexcept;

						/**
						 * @brief Span that contains @p pos, or @c m_spans.end().
						 * @param pos Stream offset.
						 * @return Map iterator.
						 */
						std::map<StormByte::ByteSize, FIFO>::iterator FindSpan(StormByte::ByteSize pos) const noexcept;

						/**
						 * @brief Copy @p n bytes at @p pos from a hit span into @p dest.
						 * @param pos Stream offset of the first byte.
						 * @param n Byte count. Must fit in the hit.
						 * @param dest Destination FIFO (appends).
						 * @return @c true if the copy succeeded.
						 */
						bool CopyFromCache(StormByte::ByteSize pos, StormByte::ByteSize n, FIFO& dest) const;

						/**
						 * @brief Remove @p [from, to) from the map, splitting spans.
						 * @param from Inclusive stream offset.
						 * @param to Exclusive stream offset.
						 */
						void EraseRange(StormByte::ByteSize from, StormByte::ByteSize to) const;

						/**
						 * @brief Insert @p piece at @p start and merge overlap / abutment.
						 * @param start Stream offset of @p piece[0].
						 * @param piece Owned bytes. Ignored when @ref MaxMemory is 0.
						 */
						void CommitSpan(StormByte::ByteSize start, FIFO&& piece) const;

						/**
						 * @brief Evict spans farthest from @ref Tell until @ref MaxMemory.
						 *
						 * @c 0 drops the whole map. A span that contains @ref Tell
						 * is trimmed last, keeping bytes nearest the cursor.
						 */
						void CollectGarbage() const;

						/**
						 * @brief Place the device cursor at @p pos if needed.
						 * @param pos Desired origin offset.
						 * @return @ref Status::Ok or @ref Status::Failed.
						 *
						 * Must not run under @c m_mutex.
						 */
						Result EnsureOrigin(StormByte::ByteSize pos) const;

						/**
						 * @brief @c OriginPull at @p at into @p dest and optionally cache.
						 * @param at Stream offset to read from.
						 * @param n Maximum bytes.
						 * @param dest Receives the pulled bytes (not the user FIFO).
						 * @return Origin status and byte count.
						 *
						 * Must not run under @c m_mutex.
						 */
						Result PullAt(StormByte::ByteSize at, StormByte::ByteSize n, FIFO& dest) const;

						/**
						 * @brief Shared @c Read / @c Peek implementation.
						 * @param n Requested count.
						 * @param dest Caller FIFO.
						 * @param consume @c true for Read, @c false for Peek.
						 * @return Status and bytes written to @p dest.
						 */
						Result Serve(StormByte::ByteSize n, FIFO& dest, bool consume) const;

						/**
						 * @brief Record a wait sample. Caller holds @c m_mutex.
						 * @param elapsed Duration of the Serve that worked or timed out.
						 */
						void NoteWait(std::chrono::nanoseconds elapsed) const noexcept;

						/**
						 * @brief Raise CachedPeak and Saturated if the cap is hit. Caller holds @c m_mutex.
						 */
						void NoteResident() const noexcept;

						/**
						 * @brief Tell equals a known device cursor. Caller holds @c m_mutex.
						 * @return @c true if a pull at Tell needs no OriginSeek.
						 */
						bool DeviceSynced() const noexcept;

						/**
						 * @brief Fold the open epoch into SavedFull / SavedPartial. Caller holds @c m_mutex.
						 */
						void CloseSeekEpoch() const noexcept;

						IO::BufferedReader* m_owner;					///< Public leaf (hooks).

						mutable std::mutex m_mutex;						///< Session + map.
						mutable std::condition_variable m_cv;			///< Worker / flush waits.

						StormByte::ByteSize m_read_ahead {0};				///< Prefetch target length.
						StormByte::ByteSize m_max_memory {0};				///< Approximate cache cap.
						std::chrono::milliseconds m_max_wait {0};		///< Read wait cap. 0 = forever.

						enum State m_state { State::Unavailable };		///< Session state.
						bool m_open {false};							///< Session armed (Open until Close).
						mutable bool m_failed {false};					///< Permanent failure.
						mutable bool m_origin_exhausted {false};		///< Device EOF (not public EoF).
						mutable StormByte::ByteSize m_tell {0};				///< Logical cursor. AVIO contract.
						mutable StormByte::ByteSize m_max_tell {0};			///< High-water of consumed Tell.
						mutable StormByte::ByteSize m_origin_pos {0};		///< Device cursor. Not updated by Seek.
						mutable bool m_origin_valid {false};			///< Whether @c m_origin_pos is known.
						mutable bool m_hold_prefetch {false};			///< Fake seek: prefetch off until catch-up or OriginSeek.

						mutable std::map<StormByte::ByteSize, FIFO> m_spans;	///< [offset, offset+len) owned bytes.

						mutable StormByte::ByteSize m_delivered {0};		///< Telemetry.Delivered.
						mutable StormByte::ByteSize m_hit_ahead {0};		///< Telemetry.HitAhead.
						mutable StormByte::ByteSize m_hit_back {0};			///< Telemetry.HitBack.
						mutable StormByte::ByteSize m_miss {0};				///< Telemetry.Miss.
						mutable StormByte::ByteSize m_origin {0};			///< Telemetry.Origin.
						mutable StormByte::ByteSize m_cached_peak {0};		///< Telemetry.CachedPeak.
						mutable std::size_t m_seek_logical {0};			///< Telemetry.SeekLogical.
						mutable std::size_t m_seek_origin {0};			///< Telemetry.SeekOrigin.
						mutable std::size_t m_seek_saved_full {0};		///< Telemetry.SeekSavedFull.
						mutable std::size_t m_seek_saved_partial {0};	///< Telemetry.SeekSavedPartial.
						mutable bool m_seek_epoch {false};				///< Epoch open until next Seek/Close.
						mutable bool m_seek_had_cache {false};			///< CoverageFrom(target) at Seek.
						mutable bool m_seek_did_origin {false};			///< OriginSeek hook ran in epoch.
						mutable std::size_t m_try_again {0};			///< Telemetry.TryAgain.
						mutable std::size_t m_saturated {0};			///< Telemetry.Saturated.
						mutable std::size_t m_evicted {0};				///< Telemetry.Evicted.
						mutable std::chrono::nanoseconds m_wait_min {0};	///< Telemetry.WaitMin.
						mutable std::chrono::nanoseconds m_wait_max {0};	///< Telemetry.WaitMax.
						mutable std::chrono::nanoseconds m_wait_total {0};	///< Telemetry.WaitTotal.
						mutable std::size_t m_wait_samples {0};			///< Telemetry.WaitSamples.

						mutable std::atomic<bool> m_stop {false};		///< Worker teardown.
						mutable std::atomic<bool> m_cancel_prefetch {false}; ///< Flush in-flight pull.
						mutable bool m_prefetch_run {false};			///< Worker has an active target.
						mutable StormByte::ByteSize m_prefetch_target {0};	///< Desired coverage from Tell.
						std::thread m_worker;							///< Prefetch thread.
				};
			}
		}
	}
}
