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

#include <StormByte/type_traits/safe.hxx>

#include <StormByte/byte_size.hxx>
#include <StormByte/platform.h>
#include <StormByte/safe/optional.hxx>
#include <StormByte/type_traits.hxx>

#include <chrono>
#include <cstddef>
#include <cstdint>
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
		 * @namespace StormByte::Buffer::IO
		 * @brief Buffered binary sources and sinks.
		 *
		 * Knob types and parameter bags are header-only. Optional values use
		 * Safe provider callbacks for ownership. IO constructors resolve the
		 * knobs into byte counts, chunk counts and millisecond durations.
		 */
		namespace IO {
			/**
			 * @brief Maximum OriginPull request. Zero selects the 64 KiB default.
			 */
			class ReadChunk {
				public:
					/**
					 * @brief Store the requested transfer limit.
					 * @param value Bytes; zero selects 64 KiB.
					 */
					explicit ReadChunk(StormByte::ByteSize value) noexcept: m_value(value) {}

					/**
					 * @brief Copy the limit.
					 * @param other Source.
					 */
					ReadChunk(const ReadChunk& other) noexcept = default;

					/**
					 * @brief Move the limit.
					 * @param other Source.
					 */
					ReadChunk(ReadChunk&& other) noexcept = default;

					/**
					 * @brief Destroy the non-owning limit.
					 */
					~ReadChunk() noexcept = default;

					/**
					 * @brief Replace the limit.
					 * @param other Source.
					 * @return This knob.
					 */
					ReadChunk& operator=(const ReadChunk& other) noexcept = default;

					/**
					 * @brief Move-assign the limit.
					 * @param other Source.
					 * @return This knob.
					 */
					ReadChunk& operator=(ReadChunk&& other) noexcept = default;

					/**
					 * @brief Requested transfer maximum.
					 * @return Bytes, or zero for the default.
					 */
					StormByte::ByteSize Value() const noexcept { return m_value; }

				private:
					StormByte::ByteSize m_value;	///< Requested transfer maximum.
			};

			/**
			 * @brief Maximum OriginPush request in every mode. Zero is unlimited.
			 */
			class WriteTransfer {
				public:
					/**
					 * @brief Store the transfer maximum.
					 * @param value Bytes, or zero for unlimited.
					 */
					explicit WriteTransfer(StormByte::ByteSize value) noexcept: m_value(value) {}

					/**
					 * @brief Copy the limit.
					 * @param other Source.
					 */
					WriteTransfer(const WriteTransfer& other) noexcept = default;

					/**
					 * @brief Move the limit.
					 * @param other Source.
					 */
					WriteTransfer(WriteTransfer&& other) noexcept = default;

					/**
					 * @brief Destroy the non-owning limit.
					 */
					~WriteTransfer() noexcept = default;

					/**
					 * @brief Replace the limit.
					 * @param other Source.
					 * @return This knob.
					 */
					WriteTransfer& operator=(const WriteTransfer& other) noexcept = default;

					/**
					 * @brief Move-assign the limit.
					 * @param other Source.
					 * @return This knob.
					 */
					WriteTransfer& operator=(WriteTransfer&& other) noexcept = default;

					/**
					 * @brief Requested transfer maximum.
					 * @return Bytes, or zero for unlimited.
					 */
					StormByte::ByteSize Value() const noexcept { return m_value; }

				private:
					StormByte::ByteSize m_value;	///< Requested transfer maximum.
			};

			/**
			 * @brief Page drain high watermark. Zero drains only under memory pressure.
			 */
			class WriteThreshold {
				public:
					/**
					 * @brief Store the drain high watermark.
					 * @param value Pending bytes; zero means memory pressure only.
					 */
					explicit WriteThreshold(StormByte::ByteSize value) noexcept: m_value(value) {}

					/**
					 * @brief Copy the watermark.
					 * @param other Source.
					 */
					WriteThreshold(const WriteThreshold& other) noexcept = default;

					/**
					 * @brief Move the watermark.
					 * @param other Source.
					 */
					WriteThreshold(WriteThreshold&& other) noexcept = default;

					/**
					 * @brief Destroy the non-owning watermark.
					 */
					~WriteThreshold() noexcept = default;

					/**
					 * @brief Replace the watermark.
					 * @param other Source.
					 * @return This knob.
					 */
					WriteThreshold& operator=(const WriteThreshold& other) noexcept = default;

					/**
					 * @brief Move-assign the watermark.
					 * @param other Source.
					 * @return This knob.
					 */
					WriteThreshold& operator=(WriteThreshold&& other) noexcept = default;

					/**
					 * @brief Requested high watermark.
					 * @return Bytes, or zero for memory pressure only.
					 */
					StormByte::ByteSize Value() const noexcept { return m_value; }

				private:
					StormByte::ByteSize m_value;	///< Requested drain high watermark.
			};

			/**
			 * @brief Complete non-owning reader policy; scalar time crosses the DLL.
			 */
			struct ReaderConfiguration {
				StormByte::ByteSize ReadChunk {64 * 1024};	///< Origin request maximum; zero selects 64 KiB.
				StormByte::ByteSize ReadAhead {0};			///< Prefetch target; zero disables.
				StormByte::ByteSize MaxMemory {0};			///< Page payload budget.
				std::int64_t MaxWaitMilliseconds {0};		///< Wait cap; zero is unlimited.
			};

			/**
			 * @brief Complete non-owning writer policy; Configure applies all fields together.
			 */
			struct WriterConfiguration {
				StormByte::ByteSize WriteChunk {0};		///< Ring unit and drain trigger.
				std::size_t BackPressure {0};			///< Ring capacity in chunk units.
				std::int64_t MaxWaitMilliseconds {0};	///< Wait cap; zero is unlimited.
				StormByte::ByteSize MaxMemory {0};		///< Dirty page payload budget.
				StormByte::ByteSize WriteTransfer {0};	///< Universal push maximum; zero is unlimited.
				StormByte::ByteSize WriteThreshold {0};	///< Drain high watermark; zero is pressure only.
			};

			/**
			 * @brief Non-I/O snapshot. Counts refer to origin hooks, not public operations.
			 */
			struct TransferSnapshot {
				std::size_t Calls {0};						///< Lifetime real hook calls.
				StormByte::ByteSize Bytes {0};				///< Lifetime origin bytes.
				StormByte::ByteSize LastRequest {0};			///< Last hook request size.
				StormByte::ByteSize MaxRequest {0};			///< Lifetime maximum request.
				StormByte::ByteSize TransferLimit {0};		///< Effective maximum; zero means unlimited.
				StormByte::ByteSize PageCapacity {0};		///< Page payload budget.
				StormByte::ByteSize RingCapacity {0};		///< Actual ring allocation.
				StormByte::ByteSize QueueCapacity {0};		///< Logical ring admission capacity.
				StormByte::ByteSize Threshold {0};			///< Effective page drain watermark.
				StormByte::ByteSize Pending {0};				///< Queued/in-materialization payload.
				StormByte::ByteSize InFlight {0};			///< Bytes requested in a current hook.
				StormByte::ByteSize Retained {0};			///< Nonseekable reader replay outside pages.
				StormByte::ByteSize Reconfiguration {0};	///< Temporary replacement ring reservation.
			};

			/**
			 * @class ReadAhead
			 * @brief Prefetch length knob. 0 disables prefetch.
			 */
			class ReadAhead {
				public:
					/**
					 * @brief Store the prefetch length.
					 * @param value Bytes. 0 disables prefetch.
					 */
					explicit ReadAhead(const StormByte::ByteSize value) noexcept:
						m_value(value) {}

					/**
					 * @brief Prefetch length.
					 * @return Bytes.
					 */
					StormByte::ByteSize Value() const noexcept {
						return m_value;
					}

				private:
					/**
					 * @brief Prefetch length.
					 */
					StormByte::ByteSize m_value;
			};

			/**
			 * @class MaxMemory
			 * @brief Cache / dirty-page budget knob. 0 stores no pages.
			 */
			class MaxMemory {
				public:
					/**
					 * @brief Store the memory budget.
					 * @param value Bytes. 0 stores no pages.
					 */
					explicit MaxMemory(const StormByte::ByteSize value) noexcept:
						m_value(value) {}

					/**
					 * @brief Memory budget.
					 * @return Bytes.
					 */
					StormByte::ByteSize Value() const noexcept {
						return m_value;
					}

				private:
					/**
					 * @brief Memory budget.
					 */
					StormByte::ByteSize m_value;
			};

			/**
			 * @class MaxWait
			 * @brief Origin wait cap. 0ms waits without limit.
			 */
			class MaxWait {
				public:
					/**
					 * @brief Store the wait cap.
					 * @param value 0ms waits without limit.
					 */
					explicit MaxWait(const std::chrono::milliseconds value) noexcept:
						m_value(value) {}

					/**
					 * @brief Wait cap.
					 * @return Duration.
					 */
					std::chrono::milliseconds Value() const noexcept {
						return m_value;
					}

				private:
					/**
					 * @brief Wait cap.
					 */
					std::chrono::milliseconds m_value;
			};

			/**
			 * @class WriteChunk
			 * @brief Ring unit and drain trigger. 0 disables the writer ring.
			 */
			class WriteChunk {
				public:
					/**
					 * @brief Store the chunk size.
					 * @param value Bytes. 0 disables the ring.
					 */
					explicit WriteChunk(const StormByte::ByteSize value) noexcept:
						m_value(value) {}

					/**
					 * @brief Chunk size.
					 * @return Bytes.
					 */
					StormByte::ByteSize Value() const noexcept {
						return m_value;
					}

				private:
					/**
					 * @brief Chunk size.
					 */
					StormByte::ByteSize m_value;
			};

			/**
			 * @class BackPressure
			 * @brief Writer ring cap in WriteChunk units. 0 disables the ring.
			 */
			class BackPressure {
				public:
					/**
					 * @brief Store the cap in chunks.
					 * @param value Chunk count. 0 disables the ring.
					 */
					explicit BackPressure(const std::size_t value) noexcept:
						m_value(value) {}

					/**
					 * @brief Cap in chunks.
					 * @return Chunk count.
					 */
					std::size_t Value() const noexcept {
						return m_value;
					}

				private:
					/**
					 * @brief Chunk count.
					 */
					std::size_t m_value;
			};

			/**
			 * @class ReaderParameters
			 * @brief Optional reader knobs. A missing field keeps the current default.
			 *
			 * Nested @c Parameters on @ref BufferedReader and its children
			 * inherit this type. Safe-owned values retain provider-side lifecycle.
			 * @note Construction, copying and knob assignment may allocate and throw.
			 */
			class ReaderParameters {
				public:
					/**
					 * @brief No knobs. Every field is absent.
					 */
					ReaderParameters() = default;

					/**
					 * @brief Engage the listed knobs. Unknown types do not compile.
					 * @tparam Knobs ReadChunk, ReadAhead, MaxMemory and/or MaxWait.
					 * @param knobs Values to store.
					 */
					template<typename... Knobs>
					ReaderParameters(Knobs... knobs) {
						(Apply(std::move(knobs)), ...);
					}

					/**
					 * @brief Copy Safe-owned optional knobs; may allocate and throw.
					 * @param other Source knobs.
					 */
					ReaderParameters(const ReaderParameters& other) = default;

					/**
					 * @brief Transfer Safe-owned optional knobs.
					 * @param other Source knobs.
					 */
					ReaderParameters(ReaderParameters&& other) noexcept = default;

					/**
					 * @brief Release optional knobs through provider callbacks.
					 */
					~ReaderParameters() noexcept = default;

					/**
					 * @brief Copy Safe-owned optional knobs; may allocate and throw.
					 * @param other Source knobs.
					 * @return This parameter bag.
					 */
					ReaderParameters& operator=(const ReaderParameters& other) = default;

					/**
					 * @brief Transfer Safe-owned optional knobs.
					 * @param other Source knobs.
					 * @return This parameter bag.
					 */
					ReaderParameters& operator=(ReaderParameters&& other) noexcept = default;

					/**
					 * @brief Origin request maximum when the caller supplied it.
					 * @return Empty when the caller omitted @ref ReadChunk.
					 */
					const StormByte::Safe::Optional<StormByte::ByteSize>& ReadChunk() const noexcept {
						return m_read_chunk;
					}

					/**
					 * @brief Prefetch length when the caller set it.
					 * @return Empty when the caller omitted @ref ReadAhead.
					 */
					const StormByte::Safe::Optional<StormByte::ByteSize>& ReadAhead() const noexcept {
						return m_read_ahead;
					}

					/**
					 * @brief Cache cap when the caller set it.
					 * @return Empty when the caller omitted @ref MaxMemory.
					 */
					const StormByte::Safe::Optional<StormByte::ByteSize>& MaxMemory() const noexcept {
						return m_max_memory;
					}

					/**
					 * @brief Signed millisecond count when the caller set the wait cap.
					 * @return Empty when omitted; an engaged zero means unlimited wait.
					 */
					const StormByte::Safe::Optional<std::chrono::milliseconds::rep>& MaxWait() const noexcept {
						return m_max_wait;
					}

				private:
					/**
					 * @brief Engage the origin request maximum.
					 * @param knob Caller-provided read knob.
					 */
					void Apply(class ReadChunk knob) {
						m_read_chunk = knob.Value();
					}

					/**
					 * @brief Engage the prefetch length.
					 * @param knob Caller-provided prefetch knob.
					 */
					void Apply(class ReadAhead knob) {
						m_read_ahead = knob.Value();
					}

					/**
					 * @brief Engage the cache budget.
					 * @param knob Caller-provided memory knob.
					 */
					void Apply(class MaxMemory knob) {
						m_max_memory = knob.Value();
					}

					/**
					 * @brief Engage the wait cap.
					 * @param knob Caller-provided wait knob.
					 */
					void Apply(class MaxWait knob) {
						m_max_wait = knob.Value().count();
					}

					/**
					 * @brief Reject unsupported reader knobs.
					 * @tparam Knob Unsupported knob type.
					 */
					template<typename Knob>
					void Apply(Knob&&) = delete;

					StormByte::Safe::Optional<StormByte::ByteSize> m_read_chunk;	///< Optional origin read maximum.
					/**
					 * @brief Safe-owned prefetch length; absent keeps the default.
					 */
					StormByte::Safe::Optional<StormByte::ByteSize> m_read_ahead;
					/**
					 * @brief Safe-owned cache budget; absent keeps the default.
					 */
					StormByte::Safe::Optional<StormByte::ByteSize> m_max_memory;
					/**
					 * @brief Safe-owned signed millisecond count; absent keeps the default.
					 */
					StormByte::Safe::Optional<std::chrono::milliseconds::rep> m_max_wait;
			};

			/**
			 * @class WriterParameters
			 * @brief Optional writer knobs. A missing field keeps the current default.
			 *
			 * Nested @c Parameters on @ref BufferedWriter and its children
			 * inherit this type. Safe-owned values retain provider-side lifecycle.
			 * @note Construction, copying and knob assignment may allocate and throw.
			 */
			class WriterParameters {
				public:
					/**
					 * @brief No knobs. Every field is absent.
					 */
					WriterParameters() = default;

					/**
					 * @brief Engage the listed knobs. Unknown types do not compile.
					 * @tparam Knobs WriteChunk, BackPressure, MaxMemory, MaxWait, WriteTransfer or WriteThreshold.
					 * @param knobs Values to store.
					 */
					template<typename... Knobs>
					WriterParameters(Knobs... knobs) {
						(Apply(std::move(knobs)), ...);
					}

					/**
					 * @brief Copy Safe-owned optional knobs; may allocate and throw.
					 * @param other Source knobs.
					 */
					WriterParameters(const WriterParameters& other) = default;

					/**
					 * @brief Transfer Safe-owned optional knobs.
					 * @param other Source knobs.
					 */
					WriterParameters(WriterParameters&& other) noexcept = default;

					/**
					 * @brief Release optional knobs through provider callbacks.
					 */
					~WriterParameters() noexcept = default;

					/**
					 * @brief Copy Safe-owned optional knobs; may allocate and throw.
					 * @param other Source knobs.
					 * @return This parameter bag.
					 */
					WriterParameters& operator=(const WriterParameters& other) = default;

					/**
					 * @brief Transfer Safe-owned optional knobs.
					 * @param other Source knobs.
					 * @return This parameter bag.
					 */
					WriterParameters& operator=(WriterParameters&& other) noexcept = default;

					/**
					 * @brief Universal push maximum when the caller supplied it.
					 * @return Empty when the caller omitted @ref WriteTransfer.
					 */
					const StormByte::Safe::Optional<StormByte::ByteSize>& WriteTransfer() const noexcept {
						return m_write_transfer;
					}

					/**
					 * @brief Page-drain watermark when the caller supplied it.
					 * @return Empty when the caller omitted @ref WriteThreshold.
					 */
					const StormByte::Safe::Optional<StormByte::ByteSize>& WriteThreshold() const noexcept {
						return m_write_threshold;
					}

					/**
					 * @brief Chunk size when the caller set it.
					 * @return Empty when the caller omitted @ref WriteChunk.
					 */
					const StormByte::Safe::Optional<StormByte::ByteSize>& WriteChunk() const noexcept {
						return m_write_chunk;
					}

					/**
					 * @brief Backpressure when the caller set it.
					 * @return Empty when the caller omitted @ref BackPressure.
					 */
					const StormByte::Safe::Optional<std::size_t>& BackPressure() const noexcept {
						return m_back_pressure;
					}

					/**
					 * @brief Dirty-page budget when the caller set it.
					 * @return Empty when the caller omitted @ref MaxMemory.
					 */
					const StormByte::Safe::Optional<StormByte::ByteSize>& MaxMemory() const noexcept {
						return m_max_memory;
					}

					/**
					 * @brief Signed millisecond count when the caller set the wait cap.
					 * @return Empty when omitted; an engaged zero means unlimited wait.
					 */
					const StormByte::Safe::Optional<std::chrono::milliseconds::rep>& MaxWait() const noexcept {
						return m_max_wait;
					}

				private:
					/**
					 * @brief Engage the universal push maximum.
					 * @param knob Caller-provided transfer knob.
					 */
					void Apply(class WriteTransfer knob) {
						m_write_transfer = knob.Value();
					}

					/**
					 * @brief Engage the page-drain watermark.
					 * @param knob Caller-provided threshold knob.
					 */
					void Apply(class WriteThreshold knob) {
						m_write_threshold = knob.Value();
					}

					/**
					 * @brief Engage the push unit.
					 * @param knob Caller-provided chunk knob.
					 */
					void Apply(class WriteChunk knob) {
						m_write_chunk = knob.Value();
					}

					/**
					 * @brief Engage the ring cap.
					 * @param knob Caller-provided backpressure knob.
					 */
					void Apply(class BackPressure knob) {
						m_back_pressure = knob.Value();
					}

					/**
					 * @brief Engage the dirty-page budget.
					 * @param knob Caller-provided memory knob.
					 */
					void Apply(class MaxMemory knob) {
						m_max_memory = knob.Value();
					}

					/**
					 * @brief Engage the wait cap.
					 * @param knob Caller-provided wait knob.
					 */
					void Apply(class MaxWait knob) {
						m_max_wait = knob.Value().count();
					}

					/**
					 * @brief Reject unsupported writer knobs.
					 * @tparam Knob Unsupported knob type.
					 */
					template<typename Knob>
					void Apply(Knob&&) = delete;

					StormByte::Safe::Optional<StormByte::ByteSize> m_write_transfer;	///< Optional origin push maximum.
					StormByte::Safe::Optional<StormByte::ByteSize> m_write_threshold;	///< Optional page watermark.
					/**
					 * @brief Safe-owned push unit; absent keeps the default.
					 */
					StormByte::Safe::Optional<StormByte::ByteSize> m_write_chunk;
					/**
					 * @brief Safe-owned ring cap; absent keeps the default.
					 */
					StormByte::Safe::Optional<std::size_t> m_back_pressure;
					/**
					 * @brief Safe-owned dirty-page budget; absent keeps the default.
					 */
					StormByte::Safe::Optional<StormByte::ByteSize> m_max_memory;
					/**
					 * @brief Safe-owned signed millisecond count; absent keeps the default.
					 */
					StormByte::Safe::Optional<std::chrono::milliseconds::rep> m_max_wait;
			};
		}
	}
}

STORMBYTE_DECLARE_MAYBE_SAFE(StormByte::Buffer::IO::ReadAhead);
STORMBYTE_DECLARE_MAYBE_SAFE(StormByte::Buffer::IO::ReadChunk);
STORMBYTE_DECLARE_MAYBE_SAFE(StormByte::Buffer::IO::WriteTransfer);
STORMBYTE_DECLARE_MAYBE_SAFE(StormByte::Buffer::IO::WriteThreshold);
STORMBYTE_DECLARE_MAYBE_SAFE(StormByte::Buffer::IO::ReaderConfiguration);
STORMBYTE_DECLARE_MAYBE_SAFE(StormByte::Buffer::IO::WriterConfiguration);
STORMBYTE_DECLARE_MAYBE_SAFE(StormByte::Buffer::IO::TransferSnapshot);
STORMBYTE_DECLARE_MAYBE_SAFE(StormByte::Buffer::IO::MaxMemory);
STORMBYTE_DECLARE_MAYBE_SAFE(StormByte::Buffer::IO::MaxWait);
STORMBYTE_DECLARE_MAYBE_SAFE(StormByte::Buffer::IO::WriteChunk);
STORMBYTE_DECLARE_MAYBE_SAFE(StormByte::Buffer::IO::BackPressure);

STORMBYTE_DECLARE_MAYBE_SAFE(StormByte::Buffer::IO::ReaderParameters);
STORMBYTE_DECLARE_MAYBE_SAFE(StormByte::Buffer::IO::WriterParameters);
