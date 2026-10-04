#pragma once

#include <StormByte/buffer/ring.hxx>
#include <StormByte/exception.hxx>
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
				std::atomic<std::size_t> references{1}; ///< Number of live Safe owner claims.
				Ring ring; ///< Shared storage; destroyed in the Buffer module.
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
			 */
			inline StormByte::Safe::Owner MakeRingOwner() {
				try {
					return StormByte::Safe::Owner(new RingOwnerState(), &CloneRingOwnerState, &DestroyRingOwnerState);
				}
				catch (const std::bad_alloc&) {
					throw StormByte::AllocationError{};
				}
			}

			/**
			 * @brief Borrow the Ring held by an owner.
			 * @param owner Ring owner.
			 * @return Ring pointer; valid only while @p owner remains alive.
			 */
			inline Ring* GetRing(const StormByte::Safe::Owner& owner) noexcept {
				auto* state = static_cast<RingOwnerState*>(owner.Get());
				return state ? &state->ring : nullptr;
			}
		}
	}
}