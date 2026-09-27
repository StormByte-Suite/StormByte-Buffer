#pragma once

#include <StormByte/buffer/external.hxx>
#include <StormByte/buffer/fifo.hxx>
#include <StormByte/buffer/generic.hxx>
#include <StormByte/buffer/io/buffered_reader.hxx>
#include <StormByte/buffer/io/buffered_writer.hxx>
#include <StormByte/buffer/telemetry.hxx>
#include <StormByte/buffer/visibility.h>
#include <StormByte/platform.h>
#include <StormByte/safe_pointers.hxx>

#include <memory>
#include <mutex>
#include <type_traits>
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
		 * @class Bridge
		 * @brief Manual bridge between two StormByte-Buffer ends.
		 *
		 * Connects a read tip to a write tip. Bytes cross only when the
		 * caller invokes @ref Passthrough. There is no worker and no
		 * occupancy cap at this layer. Continuous transfer is @ref Pumper.
		 *
		 * @par Ownership
		 * Non-IO tips are @ref ReadOnly / @ref WriteOnly references. The
		 * buffers must outlive the Bridge. The Bridge builds and owns the
		 * @ref ExternalBufferReader / @ref ExternalBufferWriter adapters;
		 * that layer is not part of the public contract. IO tips are taken
		 * by move as the concrete leaf so a second reader or writer cannot
		 * race @ref Passthrough.
		 *
		 * @par Passthrough
		 * One call is one atomic transfer. @c TryAgain on the write tip
		 * is retried until the requested write completes or the tip
		 * fails. That is not the same as @ref Operation::Blocking:
		 * Blocking applies only to the read side.
		 *
		 * @p n == 0 is the current contents of the read tip
		 * (@ref ReadOnly occupancy via the adapter, or
		 * @ref IO::BufferedReader::Available). Available on IO does not
		 * touch the origin.
		 *
		 * If @ref Failed is already true, @ref Passthrough returns 0 and
		 * does nothing.
		 *
		 * @par Telemetry
		 * An IO tip forwards that tip's @c Shared handle. A non-IO tip
		 * uses a basic @ref StormByte::Buffer::ReadTelemetry /
		 * @ref StormByte::Buffer::WriteTelemetry owned by this Bridge
		 * and updated on each successful transfer.
		 *
		 * @see ReadOnly, WriteOnly, IO::BufferedReader, IO::BufferedWriter
		 */
		class STORMBYTE_BUFFER_PUBLIC Bridge {
			public:
				/**
				 * @enum Operation
				 * @brief Read-side wait policy for @ref Passthrough.
				 */
				enum class Operation {
					Blocking,		///< Wait until N bytes or EoF.
					NonBlocking		///< Take what is available now, up to N.
				};

				/**
				 * @brief Two non-IO tips. Referenced. Adapters owned here.
				 * @param in Source. Must outlive *this.
				 * @param out Sink. Must outlive *this.
				 */
				Bridge(ReadOnly& in, WriteOnly& out) noexcept;

				/**
				 * @brief Two IO leaves. Stolen by move.
				 * @tparam In Concrete @ref IO::BufferedReader leaf.
				 * @tparam Out Concrete @ref IO::BufferedWriter leaf.
				 * @param in Source. Moved-from is empty.
				 * @param out Sink. Moved-from is empty.
				 */
				template<typename In, typename Out>
				STORMBYTE_FORCE_INLINE Bridge(In&& in, Out&& out) noexcept
					requires (std::is_base_of_v<IO::BufferedReader, std::decay_t<In>>
						&& std::is_base_of_v<IO::BufferedWriter, std::decay_t<Out>>):
					m_io_in(std::make_unique<std::decay_t<In>>(std::forward<In>(in))),
					m_io_out(std::make_unique<std::decay_t<Out>>(std::forward<Out>(out))) {}

				/**
				 * @brief Non-IO source, IO sink stolen.
				 * @tparam Out Concrete @ref IO::BufferedWriter leaf.
				 * @param in Source. Must outlive *this.
				 * @param out Sink. Moved-from is empty.
				 */
				template<typename Out>
				STORMBYTE_FORCE_INLINE Bridge(ReadOnly& in, Out&& out) noexcept
					requires std::is_base_of_v<IO::BufferedWriter, std::decay_t<Out>>:
					m_ext_in(std::make_unique<ExternalBufferReader>(in)),
					m_io_out(std::make_unique<std::decay_t<Out>>(std::forward<Out>(out))),
					m_owned_read(StormByte::Shared<StormByte::Buffer::ReadTelemetry>::MakePointer<StormByte::Buffer::ReadTelemetry>()) {}

				/**
				 * @brief IO source stolen, non-IO sink.
				 * @tparam In Concrete @ref IO::BufferedReader leaf.
				 * @param in Source. Moved-from is empty.
				 * @param out Sink. Must outlive *this.
				 */
				template<typename In>
				STORMBYTE_FORCE_INLINE Bridge(In&& in, WriteOnly& out) noexcept
					requires std::is_base_of_v<IO::BufferedReader, std::decay_t<In>>:
					m_ext_out(std::make_unique<ExternalBufferWriter>(out)),
					m_io_in(std::make_unique<std::decay_t<In>>(std::forward<In>(in))),
					m_owned_write(StormByte::Shared<StormByte::Buffer::WriteTelemetry>::MakePointer<StormByte::Buffer::WriteTelemetry>()) {}

				Bridge(const Bridge&) = delete;

				/**
				 * @brief Move constructor. Moved-from is Failed and empty.
				 * @param other Instance to take from.
				 */
				Bridge(Bridge&& other) noexcept;

				/**
				 * @brief Destructor. Releases adapters and stolen IO tips.
				 */
				~Bridge() noexcept;

				Bridge& operator=(const Bridge&) = delete;

				/**
				 * @brief Move assignment. Moved-from is Failed and empty.
				 * @param other Instance to take from.
				 * @return *this.
				 */
				Bridge& operator=(Bridge&& other) noexcept;

				/**
				 * @brief Whether the read tip reports end-of-stream.
				 * @return @c true on EoF or if there is no read tip.
				 */
				bool EoF() const noexcept;

				/**
				 * @brief Whether a tip has failed for real.
				 * @return Sticky flag. @ref Passthrough is then a no-op.
				 */
				bool Failed() const noexcept;

				/**
				 * @brief Move bytes from the read tip to the write tip.
				 * @param n Requested bytes. Zero means current contents.
				 * @param operation Read-side wait policy.
				 * @return Bytes actually moved. Zero if @ref Failed or empty.
				 */
				StormByte::ByteSize Passthrough(StormByte::ByteSize n,
					Operation operation = Operation::Blocking);

				/**
				 * @brief Read counters. IO tip forwards; non-IO is owned here.
				 * @return Const shared handle. Empty if moved-from.
				 */
				const StormByte::Shared<StormByte::Buffer::ReadTelemetry> ReadTelemetry() const noexcept;

				/**
				 * @brief Write counters. IO tip forwards; non-IO is owned here.
				 * @return Const shared handle. Empty if moved-from.
				 */
				const StormByte::Shared<StormByte::Buffer::WriteTelemetry> WriteTelemetry() const noexcept;

			private:
				/**
				 * @brief Pull up to @p n into @p dest according to @p operation.
				 */
				IO::Result Pull(StormByte::ByteSize n, FIFO& dest, Operation operation);

				/**
				 * @brief Push @p src. Retry TryAgain until Ok, End or fail.
				 */
				IO::Result Push(FIFO& src);

				std::unique_ptr<ExternalBufferReader> m_ext_in;		///< Adapter over a ReadOnly tip. Owned.
				std::unique_ptr<ExternalBufferWriter> m_ext_out;	///< Adapter over a WriteOnly tip. Owned.
				std::unique_ptr<IO::BufferedReader> m_io_in;		///< Stolen IO source (concrete leaf).
				std::unique_ptr<IO::BufferedWriter> m_io_out;		///< Stolen IO sink (concrete leaf).
				StormByte::Shared<StormByte::Buffer::ReadTelemetry> m_owned_read;	///< When in is not IO.
				StormByte::Shared<StormByte::Buffer::WriteTelemetry> m_owned_write;	///< When out is not IO.
				bool m_failed {false};								///< Sticky failure.
				mutable std::mutex m_mutex;							///< Session lock.
		};
	}
}
