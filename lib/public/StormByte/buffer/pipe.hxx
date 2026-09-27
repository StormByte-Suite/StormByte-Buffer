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
