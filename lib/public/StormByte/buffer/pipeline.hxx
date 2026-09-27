#pragma once

#include <StormByte/buffer/consumer.hxx>
#include <StormByte/buffer/generic.hxx>
#include <StormByte/buffer/producer.hxx>
#include <StormByte/buffer/typedefs.hxx>
#include <StormByte/buffer/visibility.h>
#include <StormByte/clonable.hxx>
#include <StormByte/platform.h>
#include <StormByte/safe_pointers.hxx>

#include <memory>
#include <utility>

/**
 * @namespace StormByte
 * @brief Root namespace of the StormByte C++ suite.
 */
namespace StormByte {
	/**
	 * @namespace StormByte::Logger
	 * @brief Forward declaration only. Full type lives in StormByte-Logger.
	 */
	namespace Logger {
		/**
		 * @class Log
		 * @brief Logger handle forwarded so Pipeline does not include Logger headers.
		 */
		class Log;
	}

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
		 * @ref Producer. Stages are owned here after @ref AddPipe.
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
				 * @brief One transformation. Owned by the Pipeline after @ref AddPipe.
				 *
				 * Copy is deleted (avoids slicing). Move is out of line so the
				 * DLL boundary has a single definition. @c Clone is private:
				 * only Pipeline copy may duplicate a stage. Hand a stage in
				 * with @ref Move (or @c AddPipe of a callable).
				 */
				class STORMBYTE_BUFFER_PUBLIC Stage: public Clonable<Stage, StormByte::Unique<Stage>> {
					friend class Pipeline;

					public:
						/**
						 * @brief Copy constructor is deleted.
						 */
						Stage(const Stage&) = delete;

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
						 * @brief Copy assignment is deleted.
						 */
						Stage& operator=(const Stage&) = delete;

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
						 * @param log May be null. Already scoped Buffer/Pipeline when set.
						 */
						virtual void Run(ReadOnly& in, WriteOnly& out, Logger::Log* log) = 0;

						/**
						 * @brief Polymorphic move. The source must not be used afterwards.
						 * @return Owned handle.
						 */
						PointerType Move() noexcept override = 0;

					protected:
						/**
						 * @brief Construct an abstract stage.
						 */
						Stage() noexcept = default;

					private:
						/**
						 * @brief Polymorphic copy. Only Pipeline may call this.
						 * @return Owned handle.
						 */
						PointerType Clone() const noexcept override = 0;
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
				 * @brief Take ownership of a stage.
				 * @param stage Handle from @ref Stage::Move. Empty is ignored.
				 */
				void AddPipe(StormByte::Unique<Stage> stage);

				/**
				 * @brief Box a callable in this TU and take ownership.
				 * @tparam F Invocable as @c void(ReadOnly&, WriteOnly&, Logger::Log*).
				 * @param fn Callable. Moved-from is empty.
				 */
				template<typename F>
				STORMBYTE_FORCE_INLINE void AddPipe(F&& fn) {
					/**
					 * @class Box
					 * @brief Caller-TU wrapper around @p F. Not a DLL type.
					 */
					class Box final: public Stage {
						public:
							/**
							 * @brief Take the callable by move.
							 * @param fn Callable.
							 */
							STORMBYTE_FORCE_INLINE explicit Box(F&& fn) noexcept:
								m_fn(std::forward<F>(fn)) {}

							/**
							 * @brief Copy constructor is deleted.
							 */
							STORMBYTE_FORCE_INLINE Box(const Box&) = delete;

							/**
							 * @brief Move constructor.
							 * @param other Instance to take from.
							 */
							STORMBYTE_FORCE_INLINE Box(Box&& other) noexcept = default;

							/**
							 * @brief Destructor.
							 */
							STORMBYTE_FORCE_INLINE ~Box() noexcept override = default;

							/**
							 * @brief Copy assignment is deleted.
							 */
							STORMBYTE_FORCE_INLINE Box& operator=(const Box&) = delete;

							/**
							 * @brief Move assignment.
							 * @param other Instance to take from.
							 * @return *this.
							 */
							STORMBYTE_FORCE_INLINE Box& operator=(Box&& other) noexcept = default;

							/**
							 * @brief Invoke the boxed callable.
							 * @param in Source.
							 * @param out Sink.
							 * @param log May be null.
							 */
							STORMBYTE_FORCE_INLINE void Run(ReadOnly& in, WriteOnly& out, Logger::Log* log) override {
								m_fn(in, out, log);
							}

							/**
							 * @brief Polymorphic move of this box.
							 * @return Owned handle.
							 */
							STORMBYTE_FORCE_INLINE PointerType Move() noexcept override {
								return StormByte::Unique<Stage>::MakePointer<Box>(std::move(*this));
							}

						private:
							/**
							 * @brief Copy the callable for @ref Clone.
							 * @param fn Callable.
							 */
							STORMBYTE_FORCE_INLINE explicit Box(const F& fn) noexcept:
								m_fn(fn) {}

							/**
							 * @brief Polymorphic copy of this box.
							 * @return Owned handle.
							 */
							STORMBYTE_FORCE_INLINE PointerType Clone() const noexcept override {
								return StormByte::Unique<Stage>::MakePointer<Box>(m_fn);
							}

							F m_fn;	///< Caller callable.
					};
					AddPipe(StormByte::Unique<Stage>::MakePointer<Box>(std::forward<F>(fn)));
				}

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
				 * @param log Optional. Stages receive Scope("Buffer/Pipeline") when set.
				 * @return Consumer of the last stage.
				 */
				Consumer Process(Consumer buffer, const ExecutionMode& mode,
					std::shared_ptr<Logger::Log> log) const noexcept;

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
