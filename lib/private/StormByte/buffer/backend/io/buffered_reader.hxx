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
#include <StormByte/buffer/io/telemetry.hxx>
#include <StormByte/buffer/io/typedefs.hxx>
#include <StormByte/buffer/typedefs.hxx>
#include <StormByte/buffer/visibility.h>
#include <StormByte/safe/pointers.hxx>
#include <StormByte/safe/string.hxx>

#include <atomic>
#include <chrono>
#include <condition_variable>
#include <map>
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
			 * @namespace StormByte::Buffer::Backend::IO
			 * @brief PIMPL coordinators for the public IO types.
			 */
			namespace IO {
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
						 * @param path Locator. Not changed afterwards.
						 * @param location Local or remote. Not changed afterwards.
						 * @param read_ahead Initial @ref ReadAhead in bytes.
						 * @param max_memory Initial @ref MaxMemory in bytes.
						 * @param max_wait Initial @ref MaxWait. @c 0ms = unlimited.
						 *
						 * Starts the worker thread. State is @ref State::Unavailable.
						 */
						BufferedReader(StormByte::Buffer::IO::BufferedReader& owner, StormByte::Safe::String path,
							StormByte::Buffer::IO::Location location, StormByte::ByteSize read_ahead,
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
						void Rebind(StormByte::Buffer::IO::BufferedReader& owner) noexcept;

						/**
						 * @brief Locator stored at construction.
						 * @return Owned text.
						 */
						const StormByte::Safe::String& Path() const noexcept;

						/**
						 * @brief Kind stored at construction.
						 * @return Local or remote.
						 */
						StormByte::Buffer::IO::Location Location() const noexcept;

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
						StormByte::Buffer::IO::State State() const noexcept;

						/**
						 * @brief Publish session state from a leaf hook.
						 * @param state New @ref State.
						 */
						void SetState(StormByte::Buffer::IO::State state) noexcept;

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
						StormByte::Buffer::IO::Result Close();

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
						 * @brief Cached bytes readable at @ref Tell without touching the origin.
						 * @return Contiguous coverage from @c m_tell. 0 if closed.
						 */
						StormByte::ByteSize Available() const noexcept;

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
						StormByte::Buffer::IO::Result Read(StormByte::ByteSize n, FIFO& dest) const;

						/**
						 * @brief Copy @p n bytes into @p dest without consuming.
						 * @param n Requested count. Zero copies the current span.
						 * @param dest Caller FIFO.
						 * @return Status and bytes written to @p dest.
						 */
						StormByte::Buffer::IO::Result Peek(StormByte::ByteSize n, FIFO& dest) const;

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
						StormByte::Buffer::IO::Result Seek(std::ptrdiff_t offset, Position mode) const;

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
						StormByte::Safe::Optional<StormByte::ByteSize> Size() const noexcept;

						/**
						 * @}
						 */

						/**
						 * @name Telemetry
						 * @{
						 */

						/**
						 * @brief Attach the telemetry object created by the leaf.
						 * @param telemetry Shared handle. Must not be empty.
						 */
						void BindTelemetry(StormByte::Safe::Shared<StormByte::Buffer::ReadTelemetry> telemetry) noexcept;

						/**
						 * @brief Shared read counters.
						 * @return Handle. Empty until BindTelemetry.
						 */
						const StormByte::Safe::Shared<StormByte::Buffer::ReadTelemetry> Telemetry() const noexcept;

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
						StormByte::Buffer::IO::Result EnsureOrigin(StormByte::ByteSize pos) const;

						/**
						 * @brief @c OriginPull at @p at into @p dest and optionally cache.
						 * @param at Stream offset to read from.
						 * @param n Maximum bytes.
						 * @param dest Receives the pulled bytes (not the user FIFO).
						 * @return Origin status and byte count.
						 *
						 * Must not run under @c m_mutex.
						 */
						StormByte::Buffer::IO::Result PullAt(StormByte::ByteSize at, StormByte::ByteSize n, FIFO& dest) const;

						/**
						 * @brief Shared @c Read / @c Peek implementation.
						 * @param n Requested count.
						 * @param dest Caller FIFO.
						 * @param consume @c true for Read, @c false for Peek.
						 * @return Status and bytes written to @p dest.
						 */
						StormByte::Buffer::IO::Result Serve(StormByte::ByteSize n, FIFO& dest, bool consume) const;

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

						/**
						 * @brief IO telemetry when the bound object is that type.
						 * @return Pointer or null.
						 */
						StormByte::Buffer::IO::ReadTelemetry* IoTelemetry() const noexcept;

						/**
						 * @brief Public leaf supplying origin hooks.
						 */
						StormByte::Buffer::IO::BufferedReader* m_owner;
						/**
						 * @brief Immutable locator.
						 */
						StormByte::Safe::String m_path;
						/**
						 * @brief Immutable local or remote location kind.
						 */
						StormByte::Buffer::IO::Location m_location {StormByte::Buffer::IO::Location::Local};

						/**
						 * @brief Protects session state and the cache map.
						 */
						mutable std::mutex m_mutex;
						/**
						 * @brief Coordinates worker and flush waits.
						 */
						mutable std::condition_variable m_cv;

						/**
						 * @brief Prefetch target length.
						 */
						StormByte::ByteSize m_read_ahead {0};
						/**
						 * @brief Approximate cache cap.
						 */
						StormByte::ByteSize m_max_memory {0};
						/**
						 * @brief Read wait cap; zero waits forever.
						 */
						std::chrono::milliseconds m_max_wait {0};

						/**
						 * @brief Session state.
						 */
						StormByte::Buffer::IO::State m_state { StormByte::Buffer::IO::State::Unavailable };
						/**
						 * @brief Session armed from Open until Close.
						 */
						bool m_open {false};
						/**
						 * @brief Permanent failure.
						 */
						mutable bool m_failed {false};
						/**
						 * @brief Device end of file, distinct from public EoF.
						 */
						mutable bool m_origin_exhausted {false};
						/**
						 * @brief Logical cursor required by the AVIO contract.
						 */
						mutable StormByte::ByteSize m_tell {0};
						/**
						 * @brief High-water of consumed Tell.
						 */
						mutable StormByte::ByteSize m_max_tell {0};
						/**
						 * @brief Device cursor, not updated by Seek.
						 */
						mutable StormByte::ByteSize m_origin_pos {0};
						/**
						 * @brief Whether @c m_origin_pos is known.
						 */
						mutable bool m_origin_valid {false};
						/**
						 * @brief Fake seek suspends prefetch until catch-up or OriginSeek.
						 */
						mutable bool m_hold_prefetch {false};

						/**
						 * @brief Owned byte spans indexed by their origin offset.
						 */
						mutable std::map<StormByte::ByteSize, FIFO> m_spans;
						/**
						 * @brief Session counters.
						 */
						mutable StormByte::Safe::Shared<StormByte::Buffer::ReadTelemetry> m_telemetry;

						/**
						 * @brief Telemetry.Delivered.
						 */
						mutable StormByte::ByteSize m_delivered {0};
						/**
						 * @brief Telemetry.HitAhead.
						 */
						mutable StormByte::ByteSize m_hit_ahead {0};
						/**
						 * @brief Telemetry.HitBack.
						 */
						mutable StormByte::ByteSize m_hit_back {0};
						/**
						 * @brief Telemetry.Miss.
						 */
						mutable StormByte::ByteSize m_miss {0};
						/**
						 * @brief Telemetry.Origin.
						 */
						mutable StormByte::ByteSize m_origin {0};
						/**
						 * @brief Telemetry.CachedPeak.
						 */
						mutable StormByte::ByteSize m_cached_peak {0};
						/**
						 * @brief Telemetry.SeekLogical.
						 */
						mutable std::size_t m_seek_logical {0};
						/**
						 * @brief Telemetry.SeekOrigin.
						 */
						mutable std::size_t m_seek_origin {0};
						/**
						 * @brief Telemetry.SeekSavedFull.
						 */
						mutable std::size_t m_seek_saved_full {0};
						/**
						 * @brief Telemetry.SeekSavedPartial.
						 */
						mutable std::size_t m_seek_saved_partial {0};
						/**
						 * @brief Epoch open until the next Seek or Close.
						 */
						mutable bool m_seek_epoch {false};
						/**
						 * @brief CoverageFrom(target) at Seek.
						 */
						mutable bool m_seek_had_cache {false};
						/**
						 * @brief OriginSeek hook ran in this epoch.
						 */
						mutable bool m_seek_did_origin {false};
						/**
						 * @brief Telemetry.TryAgain.
						 */
						mutable std::size_t m_try_again {0};
						/**
						 * @brief Telemetry.Saturated.
						 */
						mutable std::size_t m_saturated {0};
						/**
						 * @brief Telemetry.Evicted.
						 */
						mutable std::size_t m_evicted {0};
						/**
						 * @brief Telemetry.WaitMin.
						 */
						mutable std::chrono::nanoseconds m_wait_min {0};
						/**
						 * @brief Telemetry.WaitMax.
						 */
						mutable std::chrono::nanoseconds m_wait_max {0};
						/**
						 * @brief Telemetry.WaitTotal.
						 */
						mutable std::chrono::nanoseconds m_wait_total {0};
						/**
						 * @brief Telemetry.WaitSamples.
						 */
						mutable std::size_t m_wait_samples {0};

						/**
						 * @brief Worker teardown requested.
						 */
						mutable std::atomic<bool> m_stop {false};
						/**
						 * @brief Flush the in-flight pull.
						 */
						mutable std::atomic<bool> m_cancel_prefetch {false};
						/**
						 * @brief Worker has an active target.
						 */
						mutable bool m_prefetch_run {false};
						/**
						 * @brief Desired coverage from Tell.
						 */
						mutable StormByte::ByteSize m_prefetch_target {0};
						/**
						 * @brief Prefetch thread.
						 */
						std::thread m_worker;
				};
			}
		}
	}
}
