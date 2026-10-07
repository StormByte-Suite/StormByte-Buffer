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

#include <StormByte/buffer/visibility.h>
#include <StormByte/byte_size.hxx>
#include <StormByte/safe/string.hxx>
#include <StormByte/telemetry.hxx>
#include <StormByte/type_traits.hxx>

#include <atomic>
#include <chrono>
#include <cstdint>
#include <string_view>
#include <string>

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
		 * @brief Forward declaration of the session owning basic counters.
		 */
		class Bridge;

		/**
		 * @class Telemetry
		 * @brief Buffer session counters layered on Base's independent clock samples.
		 *
		 * @par MeanRate
		 * Octets per second of requested user operations, not of the
		 * device. A cache Write can look like GiB/s; that is the caller
		 * rate, not a disk benchmark. Worker, GC and backpressure enter
		 * only when they delay that call. Explicit Flush / Close Flush
		 * pull the rate back when the origin works.
		 *
		 * Not wall time from Open. 0 until an operation has a non-zero
		 * duration. Flatten prints @ref StormByte::ByteSize IEC text
		 * plus "/s".
		 *
		 * Rate accumulators are atomic. Each operation records an independent
		 * sample against the stable @c Buffer.Operation clock name.
		 */
		class STORMBYTE_BUFFER_PUBLIC Telemetry: public StormByte::Telemetry {
			public:
				/**
				 * @class OperationSample
				 * @brief Measures one operation with an independent Base clock sample.
				 * @note The sample borrows its owning Telemetry; it must not outlive it.
				 */
				class STORMBYTE_BUFFER_PUBLIC OperationSample {
					public:
						/**
						 * @brief Copy construction is disabled.
						 */
						OperationSample(const OperationSample&) = delete;
						/**
						 * @brief Transfers the active sample.
						 * @param other Sample to take.
						 */
						OperationSample(OperationSample&& other) noexcept;
						/**
						 * @brief Copy assignment is disabled.
						 */
						OperationSample& operator=(const OperationSample&) = delete;
						/**
						 * @brief Move assignment is disabled.
						 */
						OperationSample& operator=(OperationSample&&) = delete;

						/**
						 * @brief Stops the sample and records a committed operation.
						 */
						~OperationSample() noexcept;

						/**
						 * @brief Includes this sample in the rate calculation.
						 * @param bytes Bytes delivered or accepted by the operation.
						 */
						void Commit(StormByte::ByteSize bytes) noexcept;

					private:
						friend class Telemetry;

						/**
						 * @brief Starts one independent Base sample for an operation.
						 * @param owner Telemetry receiving the sample.
						 */
						explicit OperationSample(Telemetry& owner) noexcept;

						/**
						 * @brief Borrowed telemetry receiving the sample.
						 */
						Telemetry& m_owner;
						/**
						 * @brief Independent Base interval whose telemetry owner remains borrowed.
						 */
						StormByte::Clock::Sample m_sample;
						/**
						 * @brief Committed operation bytes.
						 */
						StormByte::ByteSize m_bytes {0};
						/**
						 * @brief Whether to include the sample in the rate calculation.
						 */
						bool m_committed {false};
				};

				/**
				 * @brief Virtual destructor. Out-of-line for the DLL boundary.
				 */
				virtual ~Telemetry() noexcept;

				/**
				 * @brief Effective mean rate of requested user operations.
				 * @return Octets per second. 0 if no timed operation yet.
				 *
				 * Not origin throughput.
				 */
				StormByte::ByteSize MeanRate() const noexcept;

				/**
				 * @brief Starts an independent measurement on the aggregate operation clock.
				 * @return A scope sample. Call Commit only when the operation counts.
				 */
				OperationSample MeasureOperation() noexcept;

				/**
				 * @brief Flatten counters. Safe across the DLL boundary.
				 * @return Owned @ref StormByte::Safe::String.
				 */
				virtual operator StormByte::Safe::String() const = 0;

				/**
				 * @brief Flatten via @c StormByte::Safe::String in this TU.
				 * @return @c std::string on the caller heap.
				 */
				STORMBYTE_FORCE_INLINE operator std::string() const {
					return static_cast<std::string>(static_cast<StormByte::Safe::String>(*this));
				}

			protected:
				/**
				 * @brief Construct all counters at 0.
				 */
				Telemetry() noexcept;

				/**
				 * @brief Copy construction is disabled to preserve clock ownership.
				 */
				Telemetry(const Telemetry&) = delete;
				/**
				 * @brief Move construction is disabled while samples may borrow this object.
				 */
				Telemetry(Telemetry&&) = delete;
				/**
				 * @brief Copy assignment is disabled to preserve clock ownership.
				 */
				Telemetry& operator=(const Telemetry&) = delete;
				/**
				 * @brief Move assignment is disabled while samples may borrow this object.
				 */
				Telemetry& operator=(Telemetry&&) = delete;

				/**
				 * @brief Accumulate committed bytes and duration from a Base sample.
				 * @param bytes Octets this operation delivered or accepted.
				 * @param elapsed Duration recorded by its independent Base sample.
				 */
				void RecordOperation(StormByte::ByteSize bytes, std::chrono::microseconds elapsed) noexcept;

			private:
				friend class Bridge;

				/**
				 * @brief Octets counted toward MeanRate.
				 */
				std::atomic<std::uint64_t> m_rate_bytes;
				/**
				 * @brief Sum of operation durations in microseconds.
				 */
				std::atomic<std::uint64_t> m_op_us;
				/**
				 * @brief Cached octets per second.
				 */
				std::atomic<std::uint64_t> m_mean_rate;
		};

		/**
		 * @class ReadTelemetry
		 * @brief Basic read counters. Non-IO Bridge uses this type as-is.
		 */
		class STORMBYTE_BUFFER_PUBLIC ReadTelemetry: public Telemetry {
			public:
				/**
				 * @brief Construct all counters at 0.
				 */
				ReadTelemetry() noexcept;

				/**
				 * @brief Copy construction is disabled to preserve clock ownership.
				 */
				ReadTelemetry(const ReadTelemetry&) = delete;
				/**
				 * @brief Move construction is disabled while samples may borrow this object.
				 */
				ReadTelemetry(ReadTelemetry&&) = delete;

				/**
				 * @brief Virtual destructor. Out-of-line for the DLL boundary.
				 */
				virtual ~ReadTelemetry() noexcept override;

				/**
				 * @brief Copy assignment is disabled to preserve clock ownership.
				 */
				ReadTelemetry& operator=(const ReadTelemetry&) = delete;
				/**
				 * @brief Move assignment is disabled while samples may borrow this object.
				 */
				ReadTelemetry& operator=(ReadTelemetry&&) = delete;

				/**
				 * @brief Octets delivered to the caller.
				 * @return Accumulator. Includes cache hits when an IO leaf derives.
				 */
				StormByte::ByteSize Delivered() const noexcept;

				/**
				 * @brief Flatten base read counters.
				 * @return IEC ByteSize text. MeanRate ends with /s.
				 */
				operator StormByte::Safe::String() const override;

			protected:
				friend class Bridge;

				/**
				 * @brief Octets delivered.
				 */
				StormByte::ByteSize m_delivered;
		};

		/**
		 * @class WriteTelemetry
		 * @brief Basic write counters. Non-IO Bridge uses this type as-is.
		 */
		class STORMBYTE_BUFFER_PUBLIC WriteTelemetry: public Telemetry {
			public:
				/**
				 * @brief Construct all counters at 0.
				 */
				WriteTelemetry() noexcept;

				/**
				 * @brief Copy construction is disabled to preserve clock ownership.
				 */
				WriteTelemetry(const WriteTelemetry&) = delete;
				/**
				 * @brief Move construction is disabled while samples may borrow this object.
				 */
				WriteTelemetry(WriteTelemetry&&) = delete;

				/**
				 * @brief Virtual destructor. Out-of-line for the DLL boundary.
				 */
				virtual ~WriteTelemetry() noexcept override;

				/**
				 * @brief Copy assignment is disabled to preserve clock ownership.
				 */
				WriteTelemetry& operator=(const WriteTelemetry&) = delete;
				/**
				 * @brief Move assignment is disabled while samples may borrow this object.
				 */
				WriteTelemetry& operator=(WriteTelemetry&&) = delete;

				/**
				 * @brief Octets accepted by a successful write.
				 * @return Accumulator. Includes cache / ring when an IO leaf derives.
				 */
				StormByte::ByteSize Accepted() const noexcept;

				/**
				 * @brief Flatten base write counters.
				 * @return IEC ByteSize text. MeanRate ends with /s.
				 */
				operator StormByte::Safe::String() const override;

			protected:
				friend class Bridge;

				/**
				 * @brief Octets accepted.
				 */
				StormByte::ByteSize m_accepted;
		};
	}
}

/**
 * @brief Abstract telemetry has Base-owned clock storage and Buffer-defined virtual destruction.
 * @note Buffer, Base and each concrete telemetry provider must remain loaded with a compatible ABI.
 *       Derived telemetry types and their extra state require independent lifecycle verification.
 */
STORMBYTE_DECLARE_MAYBE_SAFE(StormByte::Buffer::Telemetry);

/**
 * @brief Shared telemetry handles require the Buffer and Base providers to remain loaded.
 */
STORMBYTE_DECLARE_MAYBE_SAFE(StormByte::Buffer::ReadTelemetry);
/**
 * @brief Shared telemetry handles require the Buffer and Base providers to remain loaded.
 */
STORMBYTE_DECLARE_MAYBE_SAFE(StormByte::Buffer::WriteTelemetry);
/**
 * @brief A moved sample requires its borrowed Telemetry owner and providers to remain alive.
 */
STORMBYTE_DECLARE_MAYBE_SAFE(StormByte::Buffer::Telemetry::OperationSample);
