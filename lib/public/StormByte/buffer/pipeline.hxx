#pragma once

#include <StormByte/buffer/consumer.hxx>
#include <StormByte/buffer/generic.hxx>
#include <StormByte/buffer/pipe.hxx>
#include <StormByte/buffer/producer.hxx>
#include <StormByte/buffer/typedefs.hxx>
#include <StormByte/buffer/visibility.h>
#include <StormByte/logger/log.hxx>
#include <StormByte/safe_pointers.hxx>

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
		 * the tube through @ref Bridge / @ref Pumper: run
		 * @ref Process on a @ref Consumer and pass that
		 * @ref Consumer to a Bridge, or the other way around.
		 *
		 * Each @ref Pipe receives the previous end and writes the
		 * next. Intermediate pipes use a private SPSC ring; the
		 * last writes a public @ref Producer. @ref Add clones or
		 * moves the pipe onto Base's heap. A const add leaves the
		 * caller's @ref Pipe untouched; that object is destroyed
		 * in the caller's translation unit.
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
				 * @brief Clone @p pipe into this Pipeline. @p pipe is not touched.
				 * @param pipe Caller pipe. Destroyed in the caller's TU.
				 */
				void Add(const Pipe& pipe);

				/**
				 * @brief Take @p pipe by move into this Pipeline.
				 * @param pipe Caller pipe. Moved-from must not be used.
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
				 */
				Consumer Process(Consumer buffer,
					const StormByte::Shared<StormByte::Logger::Log>& log,
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

				std::unique_ptr<Backend> m_io;	///< Opaque coordinator.
		};
	}
}
