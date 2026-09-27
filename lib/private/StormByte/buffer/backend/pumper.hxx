#pragma once

#include <StormByte/buffer/bridge.hxx>
#include <StormByte/buffer/telemetry.hxx>
#include <StormByte/buffer/visibility.h>
#include <StormByte/safe_pointers.hxx>

#include <atomic>
#include <condition_variable>
#include <mutex>
#include <optional>
#include <thread>

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
			 * @brief Worker that drives @ref StormByte::Buffer::Bridge::Passthrough until EoF or failure.
			 *
			 * Owns the public Bridge. Starts in the constructor. @ref Cancel
			 * is terminal. @ref Toggle parks the worker. The destructor joins.
			 *
			 * HighWater applies to the read tip. 0 = no Pumper cap.
			 * Chunk 0 = automatic cycle size (constexpr in the .cxx),
			 * not Bridge "current contents".
			 */
			class STORMBYTE_BUFFER_PRIVATE Pumper {
				public:
					/**
					 * @brief Take the Bridge and start the worker.
					 * @param bridge Owned public bridge.
					 * @param chunk 0 = automatic cycle size.
					 * @param high_water Empty = office default from the input kind.
					 */
					Pumper(StormByte::Buffer::Bridge&& bridge, StormByte::ByteSize chunk,
						std::optional<StormByte::ByteSize> high_water);

					Pumper(const Pumper&) = delete;
					Pumper(Pumper&&) = delete;

					/**
					 * @brief Signal stop and join.
					 */
					~Pumper();

					Pumper& operator=(const Pumper&) = delete;
					Pumper& operator=(Pumper&&) = delete;

					/**
					 * @brief Whether the Bridge reports EoF.
					 * @return @c true on EoF.
					 */
					bool EoF() const noexcept;

					/**
					 * @brief Cancelled or Bridge failed.
					 * @return Sticky.
					 */
					bool Failed() const noexcept;

					/**
					 * @brief Pause or resume. No-op if Failed.
					 */
					void Toggle() noexcept;

					/**
					 * @brief Terminal stop. Sets Failed.
					 */
					void Cancel() noexcept;

					/**
					 * @brief Forward the Bridge read handle.
					 * @return Shared handle.
					 */
					const StormByte::Shared<StormByte::Buffer::ReadTelemetry> ReadTelemetry() const noexcept;

					/**
					 * @brief Forward the Bridge write handle.
					 * @return Shared handle.
					 */
					const StormByte::Shared<StormByte::Buffer::WriteTelemetry> WriteTelemetry() const noexcept;

				private:
					/**
					 * @brief Worker loop.
					 */
					void Worker();

					/**
					 * @brief Bytes this cycle will ask Passthrough.
					 * @return Chunk, capped by HighWater when HighWater > 0.
					 */
					StormByte::ByteSize CycleRequest() const noexcept;

					StormByte::Buffer::Bridge m_bridge;				///< Owned public bridge.
					StormByte::ByteSize m_chunk {0};				///< 0 = automatic.
					StormByte::ByteSize m_high_water {0};			///< 0 = no Pumper cap.
					bool m_failed {false};							///< Cancel or permanent fail.
					bool m_paused {false};							///< Toggle park.
					std::atomic<bool> m_stop {false};				///< Join flag.
					mutable std::mutex m_mutex;						///< Session.
					std::condition_variable m_cv;					///< Pause / stop.
					std::thread m_worker;							///< Pump thread.
			};
		}
	}
}
