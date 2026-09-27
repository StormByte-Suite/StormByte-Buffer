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

#include <StormByte/buffer/visibility.h>
#include <StormByte/byte_size.hxx>
#include <StormByte/string/string.hxx>

#include <atomic>
#include <chrono>
#include <cstdint>
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
		class Bridge;

		/**
		 * @class Telemetry
		 * @brief Base session counters. Offices call @ref DeltaOperation.
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
		 * Rate fields are atomics. @ref DeltaOperation takes no mutex.
		 */
		class STORMBYTE_BUFFER_PUBLIC Telemetry {
			public:
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
				 * @brief Flatten counters. Safe across the DLL boundary.
				 * @return Owned @ref StormByte::String::String.
				 */
				virtual operator StormByte::String::String() const = 0;

				/**
				 * @brief Flatten via @c StormByte::String::String in this TU.
				 * @return @c std::string on the caller heap.
				 */
				STORMBYTE_FORCE_INLINE operator std::string() const {
					return static_cast<std::string>(static_cast<StormByte::String::String>(*this));
				}

			protected:
				/**
				 * @brief Construct all counters at 0.
				 */
				Telemetry() noexcept;

				Telemetry(const Telemetry&) = delete;
				Telemetry(Telemetry&&) = delete;
				Telemetry& operator=(const Telemetry&) = delete;
				Telemetry& operator=(Telemetry&&) = delete;

				/**
				 * @brief Accumulate one requested operation and refresh @ref MeanRate.
				 * @param bytes Octets this operation delivered or accepted.
				 * @param elapsed Duration of that operation. 0 does not divide.
				 */
				void DeltaOperation(StormByte::ByteSize bytes, std::chrono::microseconds elapsed) noexcept;

			private:
				friend class Bridge;

				std::atomic<std::uint64_t> m_rate_bytes;	///< Octets counted toward MeanRate.
				std::atomic<std::uint64_t> m_op_us;		///< Sum of operation durations (us).
				std::atomic<std::uint64_t> m_mean_rate;	///< Cached octets/s.
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

				ReadTelemetry(const ReadTelemetry&) = delete;
				ReadTelemetry(ReadTelemetry&&) = delete;

				/**
				 * @brief Virtual destructor. Out-of-line for the DLL boundary.
				 */
				virtual ~ReadTelemetry() noexcept override;

				ReadTelemetry& operator=(const ReadTelemetry&) = delete;
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
				operator StormByte::String::String() const override;

			protected:
				friend class Bridge;

				StormByte::ByteSize m_delivered;	///< Octets delivered.
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

				WriteTelemetry(const WriteTelemetry&) = delete;
				WriteTelemetry(WriteTelemetry&&) = delete;

				/**
				 * @brief Virtual destructor. Out-of-line for the DLL boundary.
				 */
				virtual ~WriteTelemetry() noexcept override;

				WriteTelemetry& operator=(const WriteTelemetry&) = delete;
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
				operator StormByte::String::String() const override;

			protected:
				friend class Bridge;

				StormByte::ByteSize m_accepted;	///< Octets accepted.
		};
	}
}
