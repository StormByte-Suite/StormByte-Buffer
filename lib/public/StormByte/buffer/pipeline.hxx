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
#include <StormByte/buffer/producer.hxx>
#include <StormByte/buffer/typedefs.hxx>
#include <StormByte/buffer/visibility.h>
#include <StormByte/clonable.hxx>
#include <StormByte/logger/log.hxx>
#include <StormByte/safe_pointers.hxx>

#include <memory>
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
		 * @class Pipeline
		 * @brief Multi-stage byte transformation.
		 *
		 * Each stage receives @ref ReadOnly / @ref WriteOnly. Intermediate
		 * stages use a private SPSC ring; the last writes a public
		 * @ref Producer. After @ref AddPipe the Pipeline owns a
		 * @c Unique copy allocated on Base's heap. A const add leaves
		 * the caller's @ref Stage untouched; that object is destroyed
		 * in the caller's translation unit.
		 *
		 * @par Execution modes
		 * Flags combine with @c operator|:
		 * - @c Sync (0): stages on the caller thread; @ref Process blocks.
		 * - @c Async: work in background; @ref Process returns immediately.
		 * - @c Parallel: one thread per stage; without Async, Process joins.
		 * - @c Async | Parallel: concurrent stages and non-blocking Process.
		 *
		 * @see Producer, Consumer, ExecutionMode
		 */
		class STORMBYTE_BUFFER_PUBLIC Pipeline final {
			public:
				/**
				 * @class Stage
				 * @brief One transformation. Copyable and movable.
				 *
				 * A leaf implements @ref Run, @ref Clone and @ref Move.
				 * Copying a @c Stage by value slices; copy the leaf type
				 * or call @ref Clone. Pipeline stores the @c Unique that
				 * @ref Clone / @ref Move return.
				 */
				class STORMBYTE_BUFFER_PUBLIC Stage: public Clonable<Stage, StormByte::Unique<Stage>> {
					public:
						/**
						 * @brief Copy constructor. Defined in this module.
						 * @param other Instance to copy.
						 */
						Stage(const Stage& other);

						/**
						 * @brief Move constructor. Defined in this module.
						 * @param other Instance to take from.
						 */
						Stage(Stage&& other) noexcept;

						/**
						 * @brief Virtual destructor. Out-of-line for the DLL boundary.
						 */
						virtual ~Stage() noexcept;

						/**
						 * @brief Copy assignment. Defined in this module.
						 * @param other Instance to copy.
						 * @return *this.
						 */
						Stage& operator=(const Stage& other);

						/**
						 * @brief Move assignment. Defined in this module.
						 * @param other Instance to take from.
						 * @return *this.
						 */
						Stage& operator=(Stage&& other) noexcept;

						/**
						 * @brief Run this stage.
						 * @param in Source. Lives for the call.
						 * @param out Sink. Lives for the call. Close or SetError before return.
						 * @param log Shared handle. Empty if the caller passed none.
						 *        Already scoped Buffer/Pipeline when set.
						 */
						virtual void Run(ReadOnly& in, WriteOnly& out,
							const StormByte::Shared<StormByte::Logger::Log>& log) = 0;

						/**
						 * @brief Polymorphic copy. Allocated on Base's heap.
						 * @return Owned handle. The source is not touched.
						 */
						PointerType Clone() const noexcept override = 0;

						/**
						 * @brief Polymorphic move. Allocated on Base's heap.
						 * @return Owned handle. The source must not be used afterwards.
						 */
						PointerType Move() noexcept override = 0;

					protected:
						/**
						 * @brief Construct an abstract stage.
						 */
						Stage() noexcept = default;
				};

				/**
				 * @name Lifecycle
				 * @{
				 */

				/**
				 * @brief Construct an empty pipeline.
				 */
				Pipeline() noexcept;

				/**
				 * @brief Copy stages only. No running work is shared.
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
				 * @brief Copy stages only.
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
				 * @name Stages
				 * @{
				 */

				/**
				 * @brief Clone @p stage into this Pipeline. @p stage is not touched.
				 * @param stage Caller stage. Destroyed in the caller's TU.
				 */
				void AddPipe(const Stage& stage);

				/**
				 * @brief Take @p stage by move into this Pipeline.
				 * @param stage Caller stage. Moved-from must not be used.
				 */
				void AddPipe(Stage&& stage);

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
				 * @brief Run the stages.
				 * @param buffer First-stage input.
				 * @param mode @ref ExecutionMode flags.
				 * @param log Optional. Stages receive a scoped shared handle.
				 * @return Consumer of the last stage.
				 */
				Consumer Process(Consumer buffer, const ExecutionMode& mode,
					const StormByte::Shared<StormByte::Logger::Log>& log) const noexcept;

				/**
				 * @}
				 */

			private:
				/**
				 * @struct Backend
				 * @brief Coordinator. Defined in the implementation file.
				 */
				struct Backend;

				std::unique_ptr<Backend> m_io;	///< Opaque coordinator.
		};
	}
}
