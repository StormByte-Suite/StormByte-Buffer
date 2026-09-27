#pragma once

#include <StormByte/buffer/bridge.hxx>
#include <StormByte/buffer/telemetry.hxx>
#include <StormByte/buffer/visibility.h>
#include <StormByte/platform.h>
#include <StormByte/safe_pointers.hxx>

#include <memory>
#include <optional>
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
		 * @namespace StormByte::Buffer::Backend
		 * @brief PIMPL coordinators for public Buffer types that are not IO.
		 */
		namespace Backend {
			/**
			 * @class Pumper
			 * @brief Private worker for @ref StormByte::Buffer::Pumper.
			 */
			class Pumper;
		}

		/**
		 * @class Chunk
		 * @brief Pumper cycle size. 0 means automatic chunking, not "current contents".
		 */
		class Chunk {
			public:
				/**
				 * @brief Store the cycle size.
				 * @param value Bytes. 0 = automatic.
				 */
				STORMBYTE_FORCE_INLINE explicit Chunk(const StormByte::ByteSize value) noexcept:
					m_value(value) {}

				/**
				 * @brief Cycle size.
				 * @return Bytes.
				 */
				STORMBYTE_FORCE_INLINE StormByte::ByteSize Value() const noexcept {
					return m_value;
				}

			private:
				StormByte::ByteSize m_value;	///< Cycle size.
		};

		/**
		 * @class HighWater
		 * @brief Input occupancy cap for @ref Pumper.
		 *
		 * Applies to the read tip only. @c 0 is an explicit "no Pumper cap".
		 * Omitting the knob is not the same as @c 0: the office then picks
		 * 0 for an IO source and a non-IO default defined in the backend.
		 */
		class HighWater {
			public:
				/**
				 * @brief Store the input cap.
				 * @param value Bytes. 0 disables the Pumper cap.
				 */
				STORMBYTE_FORCE_INLINE explicit HighWater(const StormByte::ByteSize value) noexcept:
					m_value(value) {}

				/**
				 * @brief Input cap.
				 * @return Bytes.
				 */
				STORMBYTE_FORCE_INLINE StormByte::ByteSize Value() const noexcept {
					return m_value;
				}

			private:
				StormByte::ByteSize m_value;	///< Input cap.
		};

		/**
		 * @class Pumper
		 * @brief Owns a @ref Bridge and moves bytes until EoF or failure.
		 *
		 * Starts the worker in the constructor. The destructor joins; it
		 * may block until the current cycle finishes. There is no Stop.
		 * @ref Cancel is terminal (@ref Failed, no restart). @ref Toggle
		 * pauses and resumes.
		 *
		 * @par HighWater
		 * Caps how much the worker will pull from the input. @c nullopt
		 * (omitted knob): IO source → 0; non-IO source → backend default
		 * (constexpr in the .cxx). Explicit @c 0: no Pumper cap. Use 0
		 * only when the source is an IO leaf that already limits itself.
		 * Non-IO sources are unbounded by design; omitting HighWater is
		 * the safe default. 0 on a non-IO source is the caller's choice.
		 *
		 * @par Chunk
		 * Bytes the worker asks @ref Bridge::Passthrough per cycle.
		 * @c 0 is automatic chunking, not Bridge's "current contents".
		 *
		 * @par Telemetry
		 * Forwards the Bridge handles. No extra counters.
		 */
		class STORMBYTE_BUFFER_PUBLIC Pumper {
			public:
				/**
				 * @class Parameters
				 * @brief Optional Pumper knobs. A missing field keeps the office default.
				 *
				 * Header-only. The variadic list is applied in the caller
				 * (@c STORMBYTE_FORCE_INLINE). This module never
				 * instantiates @c Parameters.
				 */
				class Parameters {
					public:
						/**
						 * @brief No knobs. Every field is absent.
						 */
						STORMBYTE_FORCE_INLINE Parameters() noexcept = default;

						/**
						 * @brief Engage the listed knobs. Unknown types do not compile.
						 * @tparam Knobs @ref Chunk and/or @ref HighWater.
						 * @param knobs Values to store.
						 */
						template<typename... Knobs>
						STORMBYTE_FORCE_INLINE Parameters(Knobs... knobs) {
							(Apply(std::move(knobs)), ...);
						}

						/**
						 * @brief Cycle size when the caller set it.
						 * @return Empty when the caller omitted @ref Chunk.
						 */
						STORMBYTE_FORCE_INLINE const std::optional<StormByte::ByteSize>& Chunk() const noexcept {
							return m_chunk;
						}

						/**
						 * @brief Input cap when the caller set it.
						 * @return Empty when the caller omitted @ref HighWater.
						 */
						STORMBYTE_FORCE_INLINE const std::optional<StormByte::ByteSize>& HighWater() const noexcept {
							return m_high_water;
						}

					private:
						STORMBYTE_FORCE_INLINE void Apply(class Chunk knob) noexcept {
							m_chunk = knob.Value();
						}

						STORMBYTE_FORCE_INLINE void Apply(class HighWater knob) noexcept {
							m_high_water = knob.Value();
						}

						template<typename Knob>
						void Apply(Knob&&) = delete;

						std::optional<StormByte::ByteSize> m_chunk;			///< Absent = automatic.
						std::optional<StormByte::ByteSize> m_high_water;	///< Absent = office default.
				};

				/**
				 * @brief Take a Bridge and start the worker.
				 * @param bridge Owned bridge. Moved-from is empty.
				 * @param parameters Omitted knobs keep the office default.
				 */
				STORMBYTE_FORCE_INLINE explicit Pumper(Bridge&& bridge, Parameters parameters = {}):
					Pumper(std::move(bridge),
						parameters.Chunk().value_or(StormByte::ByteSize{0}),
						parameters.HighWater()) {}

				Pumper(const Pumper&) = delete;

				/**
				 * @brief Move constructor. Moved-from is Failed and joined.
				 * @param other Instance to take from.
				 */
				Pumper(Pumper&& other) noexcept;

				/**
				 * @brief Destructor. Joins the worker.
				 */
				~Pumper() noexcept;

				Pumper& operator=(const Pumper&) = delete;

				/**
				 * @brief Move assignment. Moved-from is Failed and joined.
				 * @param other Instance to take from.
				 * @return *this.
				 */
				Pumper& operator=(Pumper&& other) noexcept;

				/**
				 * @brief Whether the owned Bridge reports EoF.
				 * @return @c true on EoF or if moved-from.
				 */
				bool EoF() const noexcept;

				/**
				 * @brief Whether Cancel ran or the Bridge failed.
				 * @return Sticky. A Failed Pumper cannot be resumed.
				 */
				bool Failed() const noexcept;

				/**
				 * @brief Pause or resume the worker. No-op if @ref Failed.
				 */
				void Toggle() noexcept;

				/**
				 * @brief Terminal stop. Sets @ref Failed. Cannot restart.
				 */
				void Cancel() noexcept;

				/**
				 * @brief Read counters of the owned Bridge.
				 * @return Const shared handle. Empty if moved-from.
				 */
				const StormByte::Shared<StormByte::Buffer::ReadTelemetry> ReadTelemetry() const noexcept;

				/**
				 * @brief Write counters of the owned Bridge.
				 * @return Const shared handle. Empty if moved-from.
				 */
				const StormByte::Shared<StormByte::Buffer::WriteTelemetry> WriteTelemetry() const noexcept;

			private:
				/**
				 * @brief Resolved knobs. HighWater empty means office default.
				 * @param bridge Owned bridge.
				 * @param chunk 0 = automatic.
				 * @param high_water Empty = default from the input kind.
				 */
				Pumper(Bridge&& bridge, StormByte::ByteSize chunk,
					std::optional<StormByte::ByteSize> high_water);

				std::unique_ptr<Backend::Pumper> m_backend;	///< Worker.
		};
	}
}
