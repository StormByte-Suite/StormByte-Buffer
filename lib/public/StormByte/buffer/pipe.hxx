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
#include <StormByte/safe/function.hxx>
#include <StormByte/logger/log.hxx>
#include <StormByte/safe/pointers.hxx>
#include <StormByte/type_traits.hxx>

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
		 * @class PipeInput
		 * @brief Synchronous, non-owning stage input borrow.
		 * @warning Neither this facade nor its copies may outlive the callback.
		 *          The underlying buffer and its provider must remain alive throughout it.
		 */
		class STORMBYTE_BUFFER_PUBLIC PipeInput final {
			public:
				/**
				 * @brief Borrow a live stream input.
				 * @param input Buffer kept alive by the caller for the whole invocation.
				 */
				explicit PipeInput(ReadOnly& input) noexcept;
				/**
				 * @brief Read bytes and advance the borrowed cursor.
				 * @param count Bytes to read; zero reads all available bytes.
				 * @param data Destination bytes.
				 * @return Whether the read succeeded.
				 */
				bool Read(const StormByte::ByteSize& count, BinaryData& data) const noexcept;
				/**
				 * @brief Query the end of the borrowed stream.
				 * @return Whether no more bytes can be read.
				 */
				bool EoF() const noexcept;
				/**
				 * @brief Query readable bytes.
				 * @return Available byte count.
				 */
				StormByte::ByteSize Available() const noexcept;
				/**
				 * @brief Query the borrowed stream error state.
				 * @return Whether the stream can still be read.
				 */
				bool IsReadable() const noexcept;
			private:
				/**
				 * @brief Borrowed input; never owned or lifetime-extended.
				 */
				ReadOnly* m_input;
		};

		/**
		 * @class PipeOutput
		 * @brief Synchronous, non-owning stage output borrow with const mutation forwarding.
		 * @warning Neither this facade nor its copies may outlive the callback.
		 *          Constness does not make the underlying writer immutable or thread-safe.
		 */
		class STORMBYTE_BUFFER_PUBLIC PipeOutput final {
			public:
				/**
				 * @brief Borrow a live stream output.
				 * @param output Buffer kept alive by the caller for the whole invocation.
				 */
				explicit PipeOutput(WriteOnly& output) noexcept;
				/**
				 * @brief Copy bytes to the borrowed writer.
				 * @param data Bytes to append.
				 * @return Whether the write succeeded.
				 */
				bool Write(const BinaryData& data) const noexcept;
				/**
				 * @brief Move bytes to the borrowed writer.
				 * @param data Bytes to append.
				 * @return Whether the write succeeded.
				 */
				bool Write(BinaryData&& data) const noexcept;
				/**
				 * @brief Copy length-aware text without a trailing NUL.
				 * @param text Text to append, including embedded NULs; borrowed only for this write.
				 * @return Whether the write succeeded.
				 */
				bool Write(std::string_view text) const noexcept;
				/**
				 * @brief Query whether the writer accepts more data.
				 * @return Whether the stream is writable.
				 */
				bool IsWritable() const noexcept;
				/**
				 * @brief Close the borrowed output and wake readers.
				 */
				void Close() const noexcept;
				/**
				 * @brief Fail the borrowed output and wake waiters.
				 */
				void SetError() const noexcept;
			private:
				/**
				 * @brief Borrowed output; never owned or lifetime-extended.
				 */
				WriteOnly* m_output;
		};
	}
}

/**
 * @brief Admit the exact input facade as a conditional synchronous callback borrow.
 * @note This does not certify pointer lifetime; callers must keep the endpoint and provider alive.
 */
STORMBYTE_DECLARE_MAYBE_SAFE(StormByte::Buffer::PipeInput);
/**
 * @brief Admit the exact output facade as a conditional synchronous callback borrow.
 * @note Copies are still borrows; they must not be retained after invocation.
 */
STORMBYTE_DECLARE_MAYBE_SAFE(StormByte::Buffer::PipeOutput);

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
		 * Owns a copyable creator-context callback, not a polymorphic stage.
		 * Copies clone the callable and its value captures independently; references
		 * and shared handles inside captures retain their normal sharing semantics.
		 * Endpoint facades and the logger reference are synchronous borrows only.
		 * Close or fail the output before returning. The callable provider, Buffer,
		 * Base and Logger must remain loaded with a compatible ABI until release.
		 */
		class STORMBYTE_BUFFER_PUBLIC Pipe final {
			public:
				/**
				 * @brief Owned callback with borrowed stream endpoints and logger.
				 */
				using Callback = StormByte::Safe::Function<void(const PipeInput&, const PipeOutput&,
					const StormByte::Safe::Shared<StormByte::Logger::Log>&)>;

				/**
				 * @brief Adopt an explicitly provided creator-context callback.
				 * @param callback Callback whose provider supplies independent clone and release operations.
				 */
				explicit Pipe(Callback callback) noexcept;

				/**
				 * @brief Own a copyable callable in its creator module.
				 * @tparam Callable Copyable stage callable type.
				 * @param callable Invoked with input, output and a borrowed scoped logger.
				 * @throws StormByte::Exception Context allocation or construction failed.
				 * @note Force-inlining constructs state in the caller's CRT; clone and
				 *       release use matching creator callbacks and Base's heap.
				 */
				template<typename Callable>
				requires (!Type::SameAs<std::remove_cvref_t<Callable>, Pipe>) &&
					Type::CopyConstructible<std::remove_cvref_t<Callable>> &&
					requires(std::remove_cvref_t<Callable>& callable, const PipeInput& input,
						const PipeOutput& output, const StormByte::Safe::Shared<StormByte::Logger::Log>& log) {
						{ callable(input, output, log) } -> Type::SameAs<void>;
					}
				STORMBYTE_FORCE_INLINE explicit Pipe(Callable&& callable):
					Pipe(MakeCallback(std::forward<Callable>(callable))) {}

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
				 * @brief Release context through its creator callback.
				 */
				~Pipe() noexcept;

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
				 * @return Success, Failure for foreign exceptions, or Missing after move.
				 * @throws StormByte::Exception Safe exceptions from the callback.
				 */
				StormByte::Safe::Status Run(const PipeInput& in, const PipeOutput& out,
					const StormByte::Safe::Shared<StormByte::Logger::Log>& log) const;

			private:
				/**
				 * @brief Allocate state and attach creator-side ownership operations.
				 * @tparam Callable Stage callable type.
				 * @param callable Initial stage state.
				 * @return Independently copyable callback.
				 * @throws StormByte::Exception Allocation or callable construction failed.
				 */
				template<typename Callable>
				static STORMBYTE_FORCE_INLINE Callback MakeCallback(Callable&& callable) {
					using Context = std::remove_cvref_t<Callable>;
					std::unique_ptr<Context, StormByte::Safe::Heap::ObjectDeleter> context =
						StormByte::Safe::Heap::MakeUnique<Context>(std::forward<Callable>(callable));
					Callback callback(context.get(),
						[](void* state, const PipeInput& input, const PipeOutput& output,
							const StormByte::Safe::Shared<StormByte::Logger::Log>& log) {
							(*static_cast<Context*>(state))(input, output, log);
							return StormByte::Safe::Status::Success;
						},
						[](const void* state) noexcept -> void* {
							try {
								std::unique_ptr<Context, StormByte::Safe::Heap::ObjectDeleter> copy =
									StormByte::Safe::Heap::MakeUnique<Context>(*static_cast<const Context*>(state));
								return copy.release();
							} catch (...) {
								return nullptr;
							}
						},
						[](void* state) noexcept {
							StormByte::Safe::Heap::ObjectDeleter{}(static_cast<Context*>(state));
						});
					(void)context.release();
					return callback;
				}

				/**
				 * @brief Provider-owned callable and its clone, invoke and release callbacks.
				 */
				Callback m_callback;
		};
	}
}

/**
 * @brief Pipe calls, clones and release require the callback provider module to remain loaded.
 */
STORMBYTE_DECLARE_MAYBE_SAFE(StormByte::Buffer::Pipe);
