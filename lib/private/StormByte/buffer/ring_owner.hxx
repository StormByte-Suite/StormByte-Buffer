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

#include <StormByte/buffer/ring.hxx>
#include <StormByte/safe/exception.hxx>
#include <StormByte/safe/owner.hxx>

#include <atomic>
#include <cstddef>
#include <limits>
#include <new>

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
		 * @brief Private implementation helpers for Buffer ownership.
		 */
		namespace Backend {
			/**
			 * @struct RingOwnerState
			 * @brief Module-local Ring state shared by opaque owner claims.
			 */
			struct RingOwnerState {
				std::atomic<std::size_t> references{1};	///< Number of live Safe owner claims.
				Ring ring;								///< Shared storage. Destroyed in the Buffer module.
			};

			/**
			 * @brief Retain the shared Ring state for another owner claim.
			 * @param state Existing Ring owner state.
			 * @return The shared state, or null if its reference count is exhausted.
			 */
			inline void* CloneRingOwnerState(const void* state) noexcept {
				auto* ring_state = static_cast<RingOwnerState*>(const_cast<void*>(state));
				std::size_t references = ring_state->references.load(std::memory_order_relaxed);
				while (references != std::numeric_limits<std::size_t>::max()) {
					if (ring_state->references.compare_exchange_weak(references, references + 1,
							std::memory_order_relaxed, std::memory_order_relaxed))
						return ring_state;
				}
				return nullptr;
			}

			/**
			 * @brief Release an owner claim and destroy the Ring when it is last.
			 * @param state State previously returned by @ref CloneRingOwnerState or @ref MakeRingOwner.
			 */
			inline void DestroyRingOwnerState(void* state) noexcept {
				auto* ring_state = static_cast<RingOwnerState*>(state);
				if (ring_state->references.fetch_sub(1, std::memory_order_acq_rel) == 1)
					delete ring_state;
			}

			/**
			 * @brief Create a Safe owner for a new shared Ring.
			 * @return Owner whose callbacks remain in the Buffer module.
			 * @throws StormByte::Safe::AllocationError The Ring state cannot be allocated.
			 */
			inline StormByte::Safe::Owner MakeRingOwner() {
				try {
					return StormByte::Safe::Owner(new RingOwnerState(), &CloneRingOwnerState, &DestroyRingOwnerState);
				}
				catch (const std::bad_alloc&) {
					throw StormByte::Safe::AllocationError{};
				}
			}

			/**
			 * @brief Borrow the Ring held by an owner.
			 * @param owner Ring owner.
			 * @return Ring pointer. Valid only while @p owner remains alive.
			 */
			inline Ring* GetRing(const StormByte::Safe::Owner& owner) noexcept {
				auto* state = static_cast<RingOwnerState*>(owner.Get());
				return state ? &state->ring : nullptr;
			}
		}
	}
}

STORMBYTE_DECLARE_MAYBE_SAFE(StormByte::Buffer::Backend::RingOwnerState);
