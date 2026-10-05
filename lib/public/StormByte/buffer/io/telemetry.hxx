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

#include <StormByte/buffer/telemetry.hxx>

#include <chrono>
#include <cstddef>

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
				 * @brief Private read coordinator that updates IO telemetry.
				 */
				class BufferedReader;
				/**
				 * @brief Private write coordinator that updates IO telemetry.
				 */
				class BufferedWriter;
			}
		}

		/**
		 * @namespace StormByte::Buffer::IO
		 * @brief Buffered binary sources and sinks.
		 */
		namespace IO {
			/**
			 * @class ReadTelemetry
			 * @brief Reader counters on top of @ref StormByte::Buffer::ReadTelemetry.
			 *
			 * Same fields as @c BufferedReader::Telemetry in SHA c1d44cb.
			 * The private buffered reader coordinator writes them.
			 * MeanRate is the caller rate, not the origin rate.
			 */
			class STORMBYTE_BUFFER_PUBLIC ReadTelemetry: public StormByte::Buffer::ReadTelemetry {
				public:
					/**
					 * @brief Construct all counters at 0.
					 */
					ReadTelemetry() noexcept;

					/**
					 * @brief Copy constructor is deleted.
					 */
					ReadTelemetry(const ReadTelemetry&) = delete;

					/**
					 * @brief Move constructor is deleted.
					 */
					ReadTelemetry(ReadTelemetry&&) = delete;

					/**
					 * @brief Virtual destructor. Out-of-line for the DLL boundary.
					 */
					virtual ~ReadTelemetry() noexcept override;

					/**
					 * @brief Copy assignment is deleted.
					 * @return *this.
					 */
					ReadTelemetry& operator=(const ReadTelemetry&) = delete;

					/**
					 * @brief Move assignment is deleted.
					 * @return *this.
					 */
					ReadTelemetry& operator=(ReadTelemetry&&) = delete;

					/**
					 * @brief Of Delivered, cache octets never consumed before.
					 * @return First-touch / read-ahead hits.
					 */
					StormByte::ByteSize HitAhead() const noexcept;

					/**
					 * @brief Of Delivered, cache octets already passed by Tell.
					 * @return Page-cache replay after Seek.
					 */
					StormByte::ByteSize HitBack() const noexcept;

					/**
					 * @brief Of Delivered, octets pulled from the origin in that Read.
					 * @return Origin miss octets.
					 */
					StormByte::ByteSize Miss() const noexcept;

					/**
					 * @brief Octets transferred by OriginPull this session.
					 * @return Accumulator. Prefetch included.
					 */
					StormByte::ByteSize Origin() const noexcept;

					/**
					 * @brief Resident cache octets now.
					 * @return Current map occupancy.
					 */
					StormByte::ByteSize Cached() const noexcept;

					/**
					 * @brief Maximum Cached since construction.
					 * @return Peak occupancy.
					 */
					StormByte::ByteSize CachedPeak() const noexcept;

					/**
					 * @brief MaxMemory at this snapshot.
					 * @return Cache cap. 0 means no cache.
					 */
					StormByte::ByteSize Cap() const noexcept;

					/**
					 * @brief Successful public Seek calls.
					 * @return Count.
					 */
					std::size_t SeekLogical() const noexcept;

					/**
					 * @brief OriginSeek calls that reached the device.
					 * @return Count.
					 */
					std::size_t SeekOrigin() const noexcept;

					/**
					 * @brief Closed Seek epochs that never called OriginSeek after a cache hit.
					 * @return Count.
					 */
					std::size_t SeekSavedFull() const noexcept;

					/**
					 * @brief Closed Seek epochs that called OriginSeek after a cache hit.
					 * @return Count.
					 */
					std::size_t SeekSavedPartial() const noexcept;

					/**
					 * @brief Times Read / Peek returned TryAgain.
					 * @return Count.
					 */
					std::size_t TryAgain() const noexcept;

					/**
					 * @brief Times resident cache reached Cap while Cap > 0.
					 * @return Count.
					 */
					std::size_t Saturated() const noexcept;

					/**
					 * @brief Cache spans dropped by MaxMemory eviction.
					 * @return Count.
					 */
					std::size_t Evicted() const noexcept;

					/**
					 * @brief Shortest sampled Read/Peek wait.
					 * @return 0 if WaitSamples is 0.
					 */
					std::chrono::nanoseconds WaitMin() const noexcept;

					/**
					 * @brief Longest sampled Read/Peek wait.
					 * @return 0 if WaitSamples is 0.
					 */
					std::chrono::nanoseconds WaitMax() const noexcept;

					/**
					 * @brief Sum of sampled waits.
					 * @return Accumulator.
					 */
					std::chrono::nanoseconds WaitTotal() const noexcept;

					/**
					 * @brief Sampled waits (work or timed-out TryAgain).
					 * @return Count. Not empty no-ops.
					 */
					std::size_t WaitSamples() const noexcept;

					/**
					 * @brief Flatten base and IO read counters.
					 * @return IEC ByteSize text. MeanRate ends with /s.
					 */
					operator StormByte::Safe::String() const override;

				protected:
					friend class StormByte::Buffer::Backend::IO::BufferedReader;

					/**
					 * @brief First-touch cache octets.
					 */
					StormByte::ByteSize m_hit_ahead;
					/**
					 * @brief Replay after Seek.
					 */
					StormByte::ByteSize m_hit_back;
					/**
					 * @brief Pulled from the origin this Read.
					 */
					StormByte::ByteSize m_miss;
					/**
					 * @brief OriginPull octets this session.
					 */
					StormByte::ByteSize m_origin;
					/**
					 * @brief Resident cache now.
					 */
					StormByte::ByteSize m_cached;
					/**
					 * @brief Maximum Cached.
					 */
					StormByte::ByteSize m_cached_peak;
					/**
					 * @brief MaxMemory snapshot.
					 */
					StormByte::ByteSize m_cap;
					/**
					 * @brief Public Seek calls.
					 */
					std::size_t m_seek_logical;
					/**
					 * @brief OriginSeek calls.
					 */
					std::size_t m_seek_origin;
					/**
					 * @brief Epochs with no OriginSeek after a hit.
					 */
					std::size_t m_seek_saved_full;
					/**
					 * @brief Epochs that later needed OriginSeek.
					 */
					std::size_t m_seek_saved_partial;
					/**
					 * @brief Read/Peek TryAgain.
					 */
					std::size_t m_try_again;
					/**
					 * @brief Cache hit Cap.
					 */
					std::size_t m_saturated;
					/**
					 * @brief Spans dropped by MaxMemory.
					 */
					std::size_t m_evicted;
					/**
					 * @brief Shortest sampled wait.
					 */
					std::chrono::nanoseconds m_wait_min;
					/**
					 * @brief Longest sampled wait.
					 */
					std::chrono::nanoseconds m_wait_max;
					/**
					 * @brief Sum of sampled waits.
					 */
					std::chrono::nanoseconds m_wait_total;
					/**
					 * @brief Sampled waits.
					 */
					std::size_t m_wait_samples;
			};

			/**
			 * @class WriteTelemetry
			 * @brief Writer counters on top of @ref StormByte::Buffer::WriteTelemetry.
			 *
			 * Same fields as @c BufferedWriter::Telemetry in SHA c1d44cb.
			 * The private buffered writer coordinator writes them.
			 * MeanRate is the caller rate, not the origin rate.
			 */
			class STORMBYTE_BUFFER_PUBLIC WriteTelemetry: public StormByte::Buffer::WriteTelemetry {
				public:
					/**
					 * @brief Construct all counters at 0.
					 */
					WriteTelemetry() noexcept;

					/**
					 * @brief Copy constructor is deleted.
					 */
					WriteTelemetry(const WriteTelemetry&) = delete;

					/**
					 * @brief Move constructor is deleted.
					 */
					WriteTelemetry(WriteTelemetry&&) = delete;

					/**
					 * @brief Virtual destructor. Out-of-line for the DLL boundary.
					 */
					virtual ~WriteTelemetry() noexcept override;

					/**
					 * @brief Copy assignment is deleted.
					 * @return *this.
					 */
					WriteTelemetry& operator=(const WriteTelemetry&) = delete;

					/**
					 * @brief Move assignment is deleted.
					 * @return *this.
					 */
					WriteTelemetry& operator=(WriteTelemetry&&) = delete;

					/**
					 * @brief Of Accepted, octets that did not hit the origin on the caller thread.
					 * @return Write-behind octets.
					 */
					StormByte::ByteSize Behind() const noexcept;

					/**
					 * @brief Of Accepted, octets pushed on the caller thread.
					 * @return Direct origin octets.
					 */
					StormByte::ByteSize Direct() const noexcept;

					/**
					 * @brief Octets pushed through OriginPush since construction.
					 * @return Accumulator.
					 */
					StormByte::ByteSize Origin() const noexcept;

					/**
					 * @brief Durable origin length now.
					 * @return Device occupancy. Not an accumulator.
					 */
					StormByte::ByteSize Materialized() const noexcept;

					/**
					 * @brief Maximum logical Tell since Open / Truncate.
					 * @return High-water of the session.
					 */
					StormByte::ByteSize HighWater() const noexcept;

					/**
					 * @brief Writes that landed in a resident page at or after the previous high-water.
					 * @return Hit-ahead octets.
					 */
					StormByte::ByteSize HitAhead() const noexcept;

					/**
					 * @brief Writes that landed in a resident page behind the high-water.
					 * @return Hit-back octets.
					 */
					StormByte::ByteSize HitBack() const noexcept;

					/**
					 * @brief Writes that created or extended a page.
					 * @return Miss octets.
					 */
					StormByte::ByteSize Miss() const noexcept;

					/**
					 * @brief Octets not yet on the origin.
					 * @return Page map plus drain pipe.
					 */
					StormByte::ByteSize Dirty() const noexcept;

					/**
					 * @brief Maximum Dirty since construction.
					 * @return Peak dirty occupancy.
					 */
					StormByte::ByteSize DirtyPeak() const noexcept;

					/**
					 * @brief Ring cap in bytes at this snapshot.
					 * @return 0 if the ring is off. Not MaxMemory.
					 */
					StormByte::ByteSize Cap() const noexcept;

					/**
					 * @brief Logical Seek calls (Tell only).
					 * @return Count.
					 */
					std::size_t SeekLogical() const noexcept;

					/**
					 * @brief OriginSeek calls.
					 * @return Count.
					 */
					std::size_t SeekOrigin() const noexcept;

					/**
					 * @brief Closed epochs with no OriginSeek.
					 * @return Count.
					 */
					std::size_t SeekSavedFull() const noexcept;

					/**
					 * @brief Closed epochs that hit dirty pages and later needed OriginSeek.
					 * @return Count.
					 */
					std::size_t SeekSavedPartial() const noexcept;

					/**
					 * @brief Times Write returned TryAgain.
					 * @return Count.
					 */
					std::size_t TryAgain() const noexcept;

					/**
					 * @brief Times Dirty reached Cap while Cap > 0.
					 * @return Count.
					 */
					std::size_t Saturated() const noexcept;

					/**
					 * @brief Times GC materialised a page because Dirty exceeded MaxMemory.
					 * @return Count.
					 */
					std::size_t Evicted() const noexcept;

					/**
					 * @brief Shortest sampled Write wait.
					 * @return 0 if WaitSamples is 0.
					 */
					std::chrono::nanoseconds WaitMin() const noexcept;

					/**
					 * @brief Longest sampled Write wait.
					 * @return 0 if WaitSamples is 0.
					 */
					std::chrono::nanoseconds WaitMax() const noexcept;

					/**
					 * @brief Sum of sampled waits.
					 * @return Accumulator.
					 */
					std::chrono::nanoseconds WaitTotal() const noexcept;

					/**
					 * @brief Sampled waits (accepted Write or timed OriginPush).
					 * @return Count. Not instant TryAgain.
					 */
					std::size_t WaitSamples() const noexcept;

					/**
					 * @brief Flatten base and IO write counters.
					 * @return IEC ByteSize text. MeanRate ends with /s.
					 */
					operator StormByte::Safe::String() const override;

				protected:
					friend class StormByte::Buffer::Backend::IO::BufferedWriter;

					/**
					 * @brief Accepted not pushed on the caller thread.
					 */
					StormByte::ByteSize m_behind;
					/**
					 * @brief Accepted pushed on the caller thread.
					 */
					StormByte::ByteSize m_direct;
					/**
					 * @brief OriginPush octets.
					 */
					StormByte::ByteSize m_origin;
					/**
					 * @brief Durable origin length now.
					 */
					StormByte::ByteSize m_materialized;
					/**
					 * @brief Max logical Tell since Open/Truncate.
					 */
					StormByte::ByteSize m_high_water;
					/**
					 * @brief Writes into a page at/after high-water.
					 */
					StormByte::ByteSize m_hit_ahead;
					/**
					 * @brief Writes into a page behind high-water.
					 */
					StormByte::ByteSize m_hit_back;
					/**
					 * @brief Writes that created or extended a page.
					 */
					StormByte::ByteSize m_miss;
					/**
					 * @brief Not yet on the origin.
					 */
					StormByte::ByteSize m_dirty;
					/**
					 * @brief Maximum Dirty.
					 */
					StormByte::ByteSize m_dirty_peak;
					/**
					 * @brief Ring cap, or 0 if off.
					 */
					StormByte::ByteSize m_cap;
					/**
					 * @brief Logical Seek calls.
					 */
					std::size_t m_seek_logical;
					/**
					 * @brief OriginSeek calls.
					 */
					std::size_t m_seek_origin;
					/**
					 * @brief Epochs with no OriginSeek.
					 */
					std::size_t m_seek_saved_full;
					/**
					 * @brief Epochs that later needed OriginSeek.
					 */
					std::size_t m_seek_saved_partial;
					/**
					 * @brief Write TryAgain.
					 */
					std::size_t m_try_again;
					/**
					 * @brief Dirty reached Cap.
					 */
					std::size_t m_saturated;
					/**
					 * @brief GC materialised a page.
					 */
					std::size_t m_evicted;
					/**
					 * @brief Shortest sampled Write wait.
					 */
					std::chrono::nanoseconds m_wait_min;
					/**
					 * @brief Longest sampled Write wait.
					 */
					std::chrono::nanoseconds m_wait_max;
					/**
					 * @brief Sum of sampled waits.
					 */
					std::chrono::nanoseconds m_wait_total;
					/**
					 * @brief Sampled waits.
					 */
					std::size_t m_wait_samples;
			};
		}
	}
}

STORMBYTE_DECLARE_MAYBE_SAFE(StormByte::Buffer::IO::ReadTelemetry);
STORMBYTE_DECLARE_MAYBE_SAFE(StormByte::Buffer::IO::WriteTelemetry);
