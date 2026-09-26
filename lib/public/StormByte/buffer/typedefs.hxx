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
#include <StormByte/buffer/exception.hxx>
#include <StormByte/logger/log.hxx>
#include <StormByte/expected.hxx>

#include <cstddef>
#include <functional>
#include <memory>
#include <span>

/**
 * @namespace StormByte::Buffer
 * @brief Buffer module of the StormByte suite.
 */
namespace StormByte::Buffer {
	class Consumer;		///< Forward declaration of the Consumer class.
	class Producer;		///< Forward declaration of the Producer class.
	class ReadOnly;		///< Forward declaration of the ReadOnly interface.
	class WriteOnly;	///< Forward declaration of the WriteOnly interface.

	/**
	 * @enum Position
	 * @brief Positioning mode for buffer seek / non-destructive read operations.
	 *
	 * Defines how offset values are interpreted by @c Seek() and related APIs.
	 *
	 * @see ReadOnly::Seek()
	 */
	enum class STORMBYTE_BUFFER_PUBLIC Position {
		Absolute,	///< Offset from the beginning of the buffer (position 0).
		Relative	///< Offset from the current logical read position.
	};

	/**
	 * @enum ExecutionMode
	 * @brief Bitmask controlling how @ref Pipeline::Process schedules work.
	 *
	 * Flags are orthogonal and may be combined with @c operator|.
	 *
	 * - @c Sync (0) — no flags: stages run sequentially on the **caller’s** thread;
	 *   @ref Pipeline::Process blocks until completion.
	 * - @c Async — run work on background thread(s); @ref Pipeline::Process
	 *   returns immediately with the final @ref Consumer.
	 * - @c Parallel — one thread **per stage** (pipeline parallelism via SPSC
	 *   intermediate rings). Without @c Async, @ref Pipeline::Process still waits for all
	 *   stages to finish before returning.
	 *
	 * Typical combinations:
	 * | Expression                    | Stages              | Process()      |
	 * |-------------------------------|---------------------|----------------|
	 * | @c Sync (or @c 0)             | sequential, caller  | blocks         |
	 * | @c Async                      | sequential, 1 worker| returns now    |
	 * | @c Parallel                   | 1 thread per stage  | blocks         |
	 * | @c Async \| Parallel          | 1 thread per stage  | returns now    |
	 *
	 * @note Prefer @c Async | Parallel for multi-stage streaming production.
	 *       Use @c Sync for deterministic debugging.
	 *
	 * @see Pipeline::Process(), HasExecutionFlag()
	 */
	enum class STORMBYTE_BUFFER_PUBLIC ExecutionMode : unsigned {
		Sync     = 0,			///< Sequential on caller thread; Process blocks.
		Async    = 1u << 0,		///< Background execution; Process returns immediately.
		Parallel = 1u << 1		///< One thread per stage (pipeline parallelism).
	};

	/**
	 * @brief Bitwise OR of execution flags.
	 * @param a Left-hand flags.
	 * @param b Right-hand flags.
	 * @return Combined flag set.
	 */
	inline constexpr ExecutionMode operator|(ExecutionMode a, ExecutionMode b) noexcept {
		return static_cast<ExecutionMode>(
			static_cast<unsigned>(a) | static_cast<unsigned>(b));
	}

	/**
	 * @brief Bitwise AND of execution flags.
	 * @param a Left-hand flags.
	 * @param b Right-hand flags.
	 * @return Intersection of flag sets.
	 */
	inline constexpr ExecutionMode operator&(ExecutionMode a, ExecutionMode b) noexcept {
		return static_cast<ExecutionMode>(
			static_cast<unsigned>(a) & static_cast<unsigned>(b));
	}

	/**
	 * @brief Bitwise OR-assignment of execution flags.
	 * @param a Flags to update.
	 * @param b Flags to add.
	 * @return Reference to @p a.
	 */
	inline constexpr ExecutionMode& operator|=(ExecutionMode& a, ExecutionMode b) noexcept {
		a = a | b;
		return a;
	}

	/**
	 * @brief Whether @p mode includes all bits of @p flag.
	 * @param mode Combined mode value.
	 * @param flag Flag (or flag set) to test.
	 * @return @c true if every bit in @p flag is set in @p mode.
	 */
	inline constexpr bool HasExecutionFlag(ExecutionMode mode, ExecutionMode flag) noexcept {
		return (static_cast<unsigned>(mode) & static_cast<unsigned>(flag)) ==
			static_cast<unsigned>(flag);
	}
}
