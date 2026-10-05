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

#include <StormByte/buffer/consumer.hxx>
#include <StormByte/buffer/generic.hxx>
#include <StormByte/buffer/pipe.hxx>
#include <StormByte/buffer/producer.hxx>
#include <StormByte/buffer/typedefs.hxx>
#include <StormByte/buffer/visibility.h>
#include <StormByte/logger/log.hxx>
#include <StormByte/safe/pointers.hxx>

#include <memory>

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
		 * @class Pipeline
		 * @brief Multi-pipe byte transformation over stream buffers.
		 *
		 * This type is for @ref ReadOnly / @ref WriteOnly ends
		 * (`Consumer`, `Producer`, `FIFO`, `Ring`, and the like).
		 * It does not take IO leaves. File or device origins join
		 * the tube through @ref Bridge / @c Pumper: run
		 * @ref Process on a @ref Consumer and pass that
		 * @ref Consumer to a Bridge, or the other way around.
		 *
		 * Each @ref Pipe receives the previous end and writes the
		 * next. Intermediate pipes use a private SPSC ring; the
		 * last writes a public @ref Producer. @ref Add copies or moves
		 * a value-owned pipe. Copies deep-clone creator-owned callable state;
		 * a const add leaves the caller's pipe and captures untouched.
		 * Copying a running pipeline must be externally serialized with its
		 * callbacks; copying does not share buffers, workers or value captures.
		 *
		 * @par Execution modes
		 * Flags combine with @c operator|:
		 * - @c Sync (0): pipes on the caller thread; @ref Process blocks.
		 * - @c Async: work in background; @ref Process returns immediately.
		 * - @c Parallel: one thread per pipe; without Async, Process joins.
		 * - @c Async | Parallel: concurrent pipes and non-blocking Process.
		 *
		 * @see Pipe, Producer, Consumer, Bridge, Pumper, ExecutionMode
		 */
		class STORMBYTE_BUFFER_PUBLIC Pipeline final {
			public:
				/**
				 * @name Lifecycle
				 * @{
				 */

				/**
				 * @brief Construct an empty pipeline.
				 */
				Pipeline() noexcept;

				/**
				 * @brief Copy pipes only. No running work is shared.
				 * @param other Source pipeline.
				 */
				Pipeline(const Pipeline& other);

				/**
				 * @brief Move constructor.
				 * @param other Source pipeline.
				 */
				Pipeline(Pipeline&& other) noexcept;

				/**
				 * @brief Join any background run, then destroy.
				 */
				~Pipeline() noexcept;

				/**
				 * @brief Copy pipes only.
				 * @param other Source pipeline.
				 * @return *this.
				 */
				Pipeline& operator=(const Pipeline& other);

				/**
				 * @brief Move assignment.
				 * @param other Source pipeline.
				 * @return *this.
				 */
				Pipeline& operator=(Pipeline&& other) noexcept;

				/**
				 * @}
				 */

				/**
				 * @name Pipes
				 * @{
				 */

				/**
				 * @brief Copy @p pipe and independently clone its callable state.
				 * @param pipe Caller pipe; left untouched.
				 * @throws StormByte::Exception Callback context cloning failed.
				 */
				void Add(const Pipe& pipe);

				/**
				 * @brief Take @p pipe by move into this Pipeline.
				 * @param pipe Caller pipe; its moved-from callback reports Missing.
				 */
				void Add(Pipe&& pipe);

				/**
				 * @}
				 */

				/**
				 * @name Execution
				 * @{
				 */

				/**
				 * @brief SetError on every intermediate ring and the final Producer.
				 */
				void SetError() const noexcept;

				/**
				 * @brief Run the pipes.
				 * @param buffer First-pipe input. A stream @ref Consumer.
				 * @param log Optional. Pipes receive a scoped shared handle.
				 * @param mode @ref ExecutionMode flags.
				 * @return Consumer of the last pipe.
				 * @note Failure, Missing or exceptions from a pipe mark all outputs errored
				 *       and wake waiters. Exceptions from pipeline setup also mark outputs errored;
				 *       none escape this noexcept boundary.
				 */
				Consumer Process(Consumer buffer,
					const StormByte::Safe::Shared<StormByte::Logger::Log>& log,
					const ExecutionMode& mode) const noexcept;

				/**
				 * @}
				 */

			private:
				/**
				 * @struct Backend
				 * @brief Coordinator. Defined in the implementation file.
				 */
				struct Backend;

				/**
				 * @brief Opaque coordinator allocated, replaced and destroyed only in Buffer.
				 */
				std::unique_ptr<Backend> m_io;
		};
	}
}

/**
 * @brief Pipeline ownership and worker teardown are implemented entirely in Buffer.
 * @note Buffer, Base, Logger and pipe providers must remain loaded with a compatible ABI.
 *       Pipe copies clone independent context and release through the creator callback.
 *       Workers must complete before teardown; owner operations must be externally serialized.
 */
STORMBYTE_DECLARE_MAYBE_SAFE(StormByte::Buffer::Pipeline);
