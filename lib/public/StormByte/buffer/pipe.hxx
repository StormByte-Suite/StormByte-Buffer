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

#include <StormByte/buffer/generic.hxx>
#include <StormByte/buffer/visibility.h>
#include <StormByte/clonable.hxx>
#include <StormByte/logger/log.hxx>
#include <StormByte/safe_pointers.hxx>

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
		 * @class Pipe
		 * @brief One transformation in a @ref Pipeline.
		 *
		 * A leaf implements @ref Run, @ref Clone and @ref Move.
		 * @ref Pipeline stores the @c Unique that @ref Clone /
		 * @ref Move return. Allocation is on Base's heap.
		 */
		class STORMBYTE_BUFFER_PUBLIC Pipe: public Clonable<Pipe, StormByte::Unique<Pipe>> {
			public:
				/**
				 * @brief Copy constructor. Defined in this module.
				 * @param other Instance to copy.
				 */
				Pipe(const Pipe& other);

				/**
				 * @brief Move constructor. Defined in this module.
				 * @param other Instance to take from.
				 */
				Pipe(Pipe&& other) noexcept;

				/**
				 * @brief Virtual destructor. Out-of-line for the DLL boundary.
				 */
				virtual ~Pipe() noexcept;

				/**
				 * @brief Copy assignment. Defined in this module.
				 * @param other Instance to copy.
				 * @return *this.
				 */
				Pipe& operator=(const Pipe& other);

				/**
				 * @brief Move assignment. Defined in this module.
				 * @param other Instance to take from.
				 * @return *this.
				 */
				Pipe& operator=(Pipe&& other) noexcept;

				/**
				 * @brief Run this pipe.
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
				 * @brief Construct an abstract pipe.
				 */
				Pipe() noexcept = default;
		};
	}
}
