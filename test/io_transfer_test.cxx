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

#include <StormByte/buffer/exception.hxx>
#include <StormByte/buffer/io/buffered_reader.hxx>
#include <StormByte/buffer/io/buffered_writer.hxx>
#include <StormByte/test_handlers.h>

#include <algorithm>
#include <atomic>
#include <chrono>
#include <limits>
#include <mutex>
#include <span>
#include <thread>
#include <vector>

using namespace StormByte::Buffer::IO;
using StormByte::Buffer::FIFO;
using StormByte::Buffer::Position;
using StormByte::ByteSize;

namespace {
	/**
	 * @brief One instrumented origin operation.
	 */
	struct Transfer {
		std::size_t m_offset;	///< Origin offset before the call.
		std::size_t m_requested;	///< Requested bytes.
		std::size_t m_count;		///< Actual bytes transferred.
	};

	/**
	 * @brief Generate repeatable nonuniform content.
	 * @param n Payload length.
	 * @return Deterministic bytes.
	 */
	std::vector<std::byte> Pattern(std::size_t n) {
		std::vector<std::byte> data(n);
		for (std::size_t i = 0; i < n; ++i)
			data[i] = static_cast<std::byte>((i * 13 + i / 7) % 251);
		return data;
	}

	/**
	 * @brief Instrumented seekable or sequential origin.
	 */
	class Reader final: public BufferedReader {
		public:
			/**
			 * @brief Construct origin data and transfer policy.
			 * @param parameters Reader knobs.
			 * @param seekable Whether random access is supported.
			 */
			Reader(Parameters parameters, bool seekable = true):
				BufferedReader("instrumented", Location::Remote, std::move(parameters)),
				m_data(Pattern(513)), m_seekable(seekable) {}

			Reader(const Reader&) = delete;

			Reader(Reader&&) = delete;

			/**
			 * @brief Stop workers before fixture storage is released.
			 */
			~Reader() override { static_cast<void>(Close()); }

			Reader& operator=(const Reader&) = delete;

			Reader& operator=(Reader&&) = delete;

			/**
			 * @brief Read instrumented calls after or during origin activity.
			 * @return Independent call log.
			 */
			std::vector<Transfer> TransfersMade() const {
				std::lock_guard lock(m_mutex);
				return m_transfers;
			}
			std::vector<std::byte> m_data;	///< Data configured before activity.
			std::size_t m_short_limit = 0;	///< Maximum successful pull.
			std::size_t m_try_call = 0;		///< Call returning TryAgain.
			std::size_t m_error_call = 0;	///< Call returning Error.
			std::atomic<bool> m_entered {false};	///< Origin activity observed.
			std::chrono::milliseconds m_delay {0};	///< Simulated latency.

		protected:
			/**
			 * @brief Reset the fixture cursor.
			 * @return Successful open.
			 */
			Result OriginOpen() override {
				m_pos = 0;
				SetState(State::Idle);
				return {Status::Ok, 0};
			}
			/**
			 * @brief Finish the fixture session.
			 * @return Successful close.
			 */
			Result OriginClose() override { return {Status::Ok, 0}; }

			/**
			 * @brief Report configured seek support.
			 * @return Whether seeking is available.
			 */
			bool OriginCanSeek() const noexcept override { return m_seekable; }

			/**
			 * @brief Report known fixture length.
			 * @return True.
			 */
			bool OriginHasSize() const noexcept override { return true; }

			/**
			 * @brief Report current payload length.
			 * @return Available bytes.
			 */
			StormByte::Safe::Optional<ByteSize> OriginSize() const noexcept override { return ByteSize{m_data.size()}; }

			/**
			 * @brief Reposition the fixture cursor.
			 * @param offset Target or relative offset.
			 * @param mode Position interpretation.
			 * @return Successful seek.
			 */
			Result OriginSeek(std::ptrdiff_t offset, Position mode) override {
				m_pos = mode == Position::Absolute ? offset : m_pos + offset;
				return {Status::Ok, 0};
			}
			/**
			 * @brief Record and satisfy a possibly short or failed pull.
			 * @param n Requested bytes.
			 * @param dest Pull destination.
			 * @return Instrumented origin result.
			 */
			Result OriginPull(ByteSize n, FIFO& dest) override {
				m_entered.store(true);
				std::this_thread::sleep_for(m_delay);
				std::lock_guard lock(m_mutex);
				const auto request = static_cast<std::size_t>(n);
				const auto call = m_transfers.size() + 1;
				if (call == m_try_call || call == m_error_call) {
					m_transfers.push_back({m_pos, request, 0});
					return {call == m_try_call ? Status::TryAgain : Status::Error, 0};
				}
				auto count = std::min(request, m_data.size() - m_pos);
				if (m_short_limit)
					count = std::min(count, m_short_limit);
				if (count)
					static_cast<void>(dest.Write(StormByte::Safe::Binary(m_data.data() + m_pos, ByteSize{count})));
				m_transfers.push_back({m_pos, request, count});
				m_pos += count;
				return {m_pos == m_data.size() ? Status::End : Status::Ok, ByteSize{count}};
			}
		private:
			mutable std::mutex m_mutex;	///< Protects the call log.
			std::vector<Transfer> m_transfers;	///< Observed origin calls.
			std::size_t m_pos = 0;	///< Current origin position.
			bool m_seekable;	///< Configured seek support.
	};

	/**
	 * @brief Instrumented random-access destination.
	 */
	class Writer final: public BufferedWriter {
		public:
			/**
			 * @brief Construct the fixture with a transfer policy.
			 * @param parameters Writer knobs.
			 */
			explicit Writer(Parameters parameters):
				BufferedWriter("instrumented", Location::Remote, std::move(parameters)) {}

			Writer(const Writer&) = delete;

			Writer(Writer&&) = delete;

			/**
			 * @brief Finish workers before releasing fixture storage.
			 */
			~Writer() override { static_cast<void>(Close()); }

			Writer& operator=(const Writer&) = delete;

			Writer& operator=(Writer&&) = delete;

			/**
			 * @brief Read observed origin operations.
			 * @return Independent call log.
			 */
			std::vector<Transfer> TransfersMade() const {
				std::lock_guard lock(m_mutex);
				return m_transfers;
			}
			/**
			 * @brief Read materialized content.
			 * @return Independent payload snapshot.
			 */
			std::vector<std::byte> Contents() const {
				std::lock_guard lock(m_mutex);
				return m_data;
			}
			std::size_t m_short_limit = 0;	///< Maximum successful push.
			std::size_t m_try_call = 0;	///< Call returning TryAgain.
			std::size_t m_error_call = 0;	///< Call returning Error with progress.
			bool m_fail_flush = false;	///< Fail origin Flush.
			bool m_retry_forever = false;	///< Simulate a stalled origin.
			std::atomic<bool> m_entered {false};	///< Origin activity observed.
			std::chrono::milliseconds m_delay {0};	///< Simulated latency.

		protected:
			/**
			 * @brief Start the fixture session.
			 * @return Successful open.
			 */
			Result OriginOpen() override {
				SetState(State::Idle);
				return {Status::Ok, 0};
			}
			/**
			 * @brief Finish the fixture session.
			 * @return Successful close.
			 */
			Result OriginClose() override { return {Status::Ok, 0}; }

			/**
			 * @brief Apply configured origin flush failure.
			 * @return Ok or Error.
			 */
			Result OriginFlush() override { return {m_fail_flush ? Status::Error : Status::Ok, 0}; }

			/**
			 * @brief Clear materialized fixture content.
			 * @return Successful truncate.
			 */
			Result OriginTruncate() override {
				std::lock_guard lock(m_mutex);
				m_data.clear();
				m_pos = 0;
				return {Status::Ok, 0};
			}
			/**
			 * @brief Set the fixture cursor.
			 * @param offset Absolute origin position.
			 * @return Successful seek.
			 */
			Result OriginSeek(ByteSize offset) override {
				m_pos = static_cast<std::size_t>(offset);
				return {Status::Ok, 0};
			}
			/**
			 * @brief Record and apply a possibly short or failed push.
			 * @param src Requested payload.
			 * @return Origin result including actual progress.
			 */
			Result OriginPush(std::span<const std::byte> src) override {
				m_entered.store(true);
				std::this_thread::sleep_for(m_delay);
				std::lock_guard lock(m_mutex);
				const auto call = m_transfers.size() + 1;
				if (call == m_try_call || m_retry_forever) {
					m_transfers.push_back({m_pos, src.size(), 0});
					return {Status::TryAgain, 0};
				}
				auto count = m_short_limit ? std::min(m_short_limit, src.size()) : src.size();
				if (call == m_error_call)
					count = std::min<std::size_t>(2, count);
				m_data.resize(std::max(m_data.size(), m_pos + count));
				std::copy_n(src.begin(), count, m_data.begin() + m_pos);
				m_transfers.push_back({m_pos, src.size(), count});
				m_pos += count;
				return {call == m_error_call ? Status::Error : Status::Ok, ByteSize{count}};
			}
		private:
			mutable std::mutex m_mutex;	///< Protects materialized state.
			std::vector<Transfer> m_transfers;	///< Observed origin operations.
			std::vector<std::byte> m_data;	///< Materialized bytes.
			std::size_t m_pos = 0;	///< Current origin position.
	};

	/**
	 * @brief Extract and compare exact fixture bytes.
	 * @param fifo Result to consume.
	 * @param data Expected source bytes.
	 * @param offset Expected first byte.
	 * @param count Expected result length.
	 * @return Whether content is exact.
	 */
	bool Matches(FIFO& fifo, const std::vector<std::byte>& data, std::size_t offset, std::size_t count) {
		StormByte::Safe::Binary bytes;
		return fifo.Extract(ByteSize{count}, bytes)
			&& std::equal(bytes.begin(), bytes.end(), data.begin() + offset);
	}

	/**
	 * @brief Check every recorded real request against its maximum.
	 * @param transfers Actual calls.
	 * @param limit Request ceiling.
	 * @return Whether a nonempty call log respects the bound.
	 */
	bool Bounded(const std::vector<Transfer>& transfers, std::size_t limit) {
		return !transfers.empty() && std::all_of(transfers.begin(), transfers.end(),
			[limit](const auto& transfer) { return transfer.m_requested > 0 && transfer.m_requested <= limit; });
	}
}

// -------------------
// Transfers
// -------------------

int test_defaults_and_invalid_values() {
	Reader reader({ReadAhead(0), MaxMemory(0)});
	reader.m_data = Pattern(140000);
	ASSERT_EQUAL(ByteSize{65536}, reader.ReadChunk());
	reader.ReadChunk(0);
	ASSERT_EQUAL(ByteSize{65536}, reader.ReadChunk());
	ASSERT_TRUE(reader.Open());
	FIFO got;
	ASSERT_EQUAL(Status::Ok, reader.Read(130000, got).status);
	ASSERT_TRUE(Matches(got, reader.m_data, 0, 130000));
	ASSERT_TRUE(Bounded(reader.TransfersMade(), 65536));
	auto policy = reader.Configuration();
	policy.MaxWaitMilliseconds = -1;
	ASSERT_EQUAL(Status::Failed, reader.Configure(policy).status);
	ASSERT_EQUAL(std::chrono::milliseconds(0), reader.MaxWait());
	ASSERT_EQUAL(Status::Failed, reader.Seek(std::numeric_limits<std::ptrdiff_t>::min(), Position::Relative).status);
	Writer writer({MaxMemory(0)});
	ASSERT_TRUE(writer.Open());
	const auto data = Pattern(257);
	ASSERT_EQUAL(Status::Ok, writer.Write(std::span(data)).status);
	ASSERT_EQUAL(ByteSize{257}, writer.Transfers().MaxRequest);
	ASSERT_EQUAL(ByteSize{0}, writer.WriteTransfer());
	ASSERT_EQUAL(Status::Failed, writer.Seek(std::numeric_limits<std::ptrdiff_t>::min(), Position::Relative).status);
	Writer stalled({MaxMemory(32), WriteTransfer(7), MaxWait(std::chrono::milliseconds(3))});
	stalled.m_retry_forever = true;
	ASSERT_TRUE(stalled.Open());
	ASSERT_EQUAL(Status::Ok, stalled.Write(std::span(data).first(10)).status);
	ASSERT_EQUAL(Status::Error, stalled.Flush().status);
	ASSERT_EQUAL(ByteSize{10}, stalled.Dirty());
	ASSERT_EQUAL(ByteSize{0}, stalled.Transfers().Bytes);
	RETURN_TEST(0);
}

int test_grouping_and_pressure() {
	const auto data = Pattern(257);
	for (const std::size_t transfer : {16u, 64u}) {
		Writer writer({WriteTransfer(ByteSize{transfer}), WriteThreshold(128), MaxMemory(128)});
		ASSERT_TRUE(writer.Open());
		for (std::size_t i = 0; i < 127; ++i) {
			ASSERT_EQUAL(Status::Ok, writer.Write(std::span(data).subspan(i, 1)).status);
			ASSERT_EQUAL(static_cast<std::size_t>(0), writer.Transfers().Calls);
		}
		ASSERT_EQUAL(Status::Ok, writer.Write(std::span(data).subspan(127, 1)).status);
		ASSERT_EQUAL(ByteSize{64}, writer.Dirty());
		ASSERT_EQUAL(64 / transfer, writer.Transfers().Calls);
		for (const auto& push : writer.TransfersMade())
			ASSERT_EQUAL(transfer, push.m_requested);
		ASSERT_EQUAL(Status::Ok, writer.Write(std::span(data).subspan(128)).status);
		ASSERT_TRUE(writer.Transfers().Pending <= ByteSize{128});
		ASSERT_EQUAL(Status::Ok, writer.Flush().status);
		ASSERT_TRUE(writer.Contents() == data);
		ASSERT_EQUAL(ByteSize{0}, writer.Transfers().Pending);
	}
	Writer leftovers({WriteTransfer(64), WriteThreshold(100), MaxMemory(128)});
	ASSERT_TRUE(leftovers.Open());
	ASSERT_EQUAL(Status::Ok, leftovers.Write(std::span(data).first(7)).status);
	ASSERT_EQUAL(static_cast<std::size_t>(0), leftovers.Transfers().Calls);
	ASSERT_TRUE(leftovers.Close());
	ASSERT_TRUE(leftovers.Contents() == std::vector<std::byte>(data.begin(), data.begin() + 7));
	ASSERT_TRUE(Bounded(leftovers.TransfersMade(), 64));
	RETURN_TEST(0);
}

int test_live_reconfiguration() {
	const auto data = Pattern(2100);
	Writer writer({WriteTransfer(23), MaxMemory(128), WriteThreshold(96)});
	writer.m_delay = std::chrono::milliseconds(1);
	ASSERT_TRUE(writer.Open());
	bool good = true;
	std::thread producer([&] {
		for (std::size_t i = 0; i < data.size(); i += 7) {
			auto result = writer.Write(std::span(data).subspan(i, 7));
			while (result.status == Status::TryAgain) {
				if (writer.Flush().status != Status::Ok) {
					good = false;
					return;
				}
				result = writer.Write(std::span(data).subspan(i, 7));
			}
			if (result.status != Status::Ok) {
				good = false;
				return;
			}
		}
	});
	bool configured = true;
	for (std::size_t i = 0; i < 30; ++i) {
		auto configuration = writer.Configuration();
		configuration.WriteTransfer = ByteSize{i % 9 + 1};
		configuration.MaxMemory = ByteSize{i % 3 == 0 ? 0u : 31u};
		configuration.WriteThreshold = ByteSize{21};
		configuration.WriteChunk = ByteSize{8};
		configuration.BackPressure = i % 2 ? 3 : 0;
		configured = configured && writer.Configure(configuration).status == Status::Ok;
		const auto snapshot = writer.Transfers();
		configured = configured && snapshot.Pending <= ByteSize{128};
	}
	producer.join();
	ASSERT_TRUE(good);
	ASSERT_TRUE(configured);
	ASSERT_EQUAL(Status::Ok, writer.Flush().status);
	ASSERT_TRUE(writer.Contents() == data);

	Reader reader({ReadChunk(31), MaxMemory(97), ReadAhead(50)});
	reader.m_delay = std::chrono::milliseconds(1);
	ASSERT_TRUE(reader.Open());
	bool read_good = true;
	std::thread consumer([&] {
		FIFO got;
		for (std::size_t offset = 0; offset < reader.m_data.size(); offset += 7) {
			const auto take = std::min<std::size_t>(7, reader.m_data.size() - offset);
			const auto result = reader.Read(ByteSize{take}, got);
			if (result.status != Status::Ok || !Matches(got, reader.m_data, offset, take)) {
				read_good = false;
				return;
			}
		}
	});
	for (std::size_t i = 0; i < 20; ++i) {
		auto configuration = reader.Configuration();
		configuration.ReadChunk = ByteSize{i % 7 + 1};
		configuration.MaxMemory = ByteSize{i % 2 ? 3u : 0u};
		configuration.ReadAhead = ByteSize{i % 2 ? 11u : 0u};
		configured = configured && reader.Configure(configuration).status == Status::Ok;
	}
	consumer.join();
	ASSERT_TRUE(read_good);
	ASSERT_TRUE(configured);
	ASSERT_EQUAL(Status::Ok, reader.Close().status);
	ASSERT_TRUE(Bounded(reader.TransfersMade(), 31));
	RETURN_TEST(0);
}

int test_nonseekable_peek_and_retry() {
	for (const bool seekable : {false, true}) {
		for (const std::size_t budget : {0u, 5u, 100u}) {
			Reader reader({ReadChunk(7), ReadAhead(0), MaxMemory(ByteSize{budget})}, seekable);
			reader.m_try_call = 3;
			ASSERT_TRUE(reader.Open());
			FIFO got("unchanged");
			ASSERT_EQUAL(Status::TryAgain, reader.Peek(83, got).status);
			ASSERT_EQUAL(ByteSize{9}, got.Available());
			ASSERT_EQUAL(ByteSize{0}, reader.Tell());
			ASSERT_EQUAL(Status::Ok, reader.Peek(83, got).status);
			ASSERT_TRUE(Matches(got, reader.m_data, 0, 83));
			ASSERT_EQUAL(Status::Ok, reader.Read(11, got).status);
			ASSERT_TRUE(Matches(got, reader.m_data, 0, 11));
			ASSERT_EQUAL(Status::Ok, reader.Read(100, got).status);
			ASSERT_TRUE(Matches(got, reader.m_data, 11, 100));
			ASSERT_TRUE(Bounded(reader.TransfersMade(), budget ? std::min<std::size_t>(7, budget) : 7));
		}
	}
	Reader broken({ReadChunk(4), ReadAhead(0), MaxMemory(0)});
	broken.m_error_call = 2;
	ASSERT_TRUE(broken.Open());
	FIFO unchanged("keep");
	ASSERT_EQUAL(Status::Error, broken.Read(20, unchanged).status);
	ASSERT_EQUAL(ByteSize{4}, unchanged.Available());
	ASSERT_EQUAL(ByteSize{0}, broken.Tell());
	RETURN_TEST(0);
}

int test_overwrite_fragmentation_fifo() {
	Writer writer({WriteTransfer(11), WriteThreshold(0), MaxMemory(47)});
	ASSERT_TRUE(writer.Open());
	auto expected = Pattern(43);
	ASSERT_EQUAL(Status::Ok, writer.Write(std::span<const std::byte>(expected)).status);
	const auto patch = Pattern(39);
	for (const auto offset : {17, 0, 35, 7, 100, 55, 3}) {
		ASSERT_EQUAL(Status::Ok, writer.Seek(offset, Position::Absolute).status);
		FIFO fifo;
		ASSERT_TRUE(fifo.Write(StormByte::Safe::Binary(patch.data(), ByteSize{patch.size()})));
		ASSERT_EQUAL(Status::Ok, writer.Write(fifo).status);
		ASSERT_EQUAL(ByteSize{0}, fifo.Available());
		expected.resize(std::max(expected.size(), offset + patch.size()));
		std::copy(patch.begin(), patch.end(), expected.begin() + offset);
		ASSERT_TRUE(writer.Transfers().Pending <= ByteSize{47});
	}
	ASSERT_EQUAL(Status::Ok, writer.Flush().status);
	ASSERT_TRUE(writer.Contents() == expected);
	ASSERT_TRUE(Bounded(writer.TransfersMade(), 11));
	RETURN_TEST(0);
}

int test_read_limits() {
	for (const std::size_t chunk : {1u, 17u, 128u, 1024u}) {
		for (const std::size_t budget : {0u, 11u, 256u}) {
			for (const std::size_t ahead : {0u, 200u}) {
				Reader reader({ReadChunk(ByteSize{chunk}), MaxMemory(ByteSize{budget}), ReadAhead(ByteSize{ahead})});
				reader.m_short_limit = 7;
				ASSERT_TRUE(reader.Open());
				FIFO peek;
				ASSERT_EQUAL(Status::Ok, reader.Peek(193, peek).status);
				ASSERT_TRUE(Matches(peek, reader.m_data, 0, 193));
				ASSERT_EQUAL(ByteSize{0}, reader.Tell());
				FIFO got;
				ASSERT_EQUAL(Status::Ok, reader.Read(193, got).status);
				ASSERT_TRUE(Matches(got, reader.m_data, 0, 193));
				ASSERT_EQUAL(Status::Ok, reader.Seek(5, Position::Absolute).status);
				std::vector<std::byte> small(3);
				ASSERT_EQUAL(Status::Ok, reader.Read(small).status);
				ASSERT_TRUE(std::equal(small.begin(), small.end(), reader.m_data.begin() + 5));
				ASSERT_EQUAL(Status::Ok, reader.Seek(500, Position::Absolute).status);
				FIFO tail;
				const auto result = reader.Read(99, tail);
				ASSERT_EQUAL(Status::End, result.status);
				ASSERT_EQUAL(ByteSize{13}, result.count);
				ASSERT_TRUE(Matches(tail, reader.m_data, 500, 13));
				ASSERT_EQUAL(Status::Ok, reader.Close().status);
				const auto limit = budget ? std::min(chunk, budget) : chunk;
				const auto transfers = reader.TransfersMade();
				ASSERT_TRUE(Bounded(transfers, limit));
				ASSERT_EQUAL(transfers.size(), reader.Transfers().Calls);
				std::size_t bytes = 0;
				for (const auto& transfer : transfers)
					bytes += transfer.m_count;
				ASSERT_EQUAL(ByteSize{bytes}, reader.Transfers().Bytes);
			}
		}
	}
	RETURN_TEST(0);
}

int test_reconfiguration_and_failures() {
	const auto data = Pattern(37);
	Writer writer({WriteChunk(32), BackPressure(3), WriteTransfer(7), MaxMemory(0)});
	ASSERT_TRUE(writer.Open());
	ASSERT_EQUAL(Status::Ok, writer.Write(std::span(data).first(13)).status);
	auto configuration = writer.Configuration();
	configuration.WriteChunk = ByteSize{5};
	configuration.BackPressure = 2;
	configuration.WriteTransfer = ByteSize{3};
	ASSERT_EQUAL(Status::Ok, writer.Configure(configuration).status);
	ASSERT_EQUAL(ByteSize{10}, writer.Transfers().QueueCapacity);
	ASSERT_EQUAL(ByteSize{16}, writer.Transfers().RingCapacity);
	ASSERT_EQUAL(Status::TryAgain, writer.Write(std::span(data)).status);
	ASSERT_EQUAL(Status::Ok, writer.Write(std::span(data).subspan(13, 9)).status);
	configuration.MaxMemory = ByteSize{4};
	ASSERT_EQUAL(Status::Ok, writer.Configure(configuration).status);
	ASSERT_EQUAL(ByteSize{0}, writer.Transfers().RingCapacity);
	ASSERT_EQUAL(Status::Ok, writer.Write(std::span(data).subspan(22)).status);
	configuration.MaxMemory = ByteSize{0};
	configuration.BackPressure = 0;
	ASSERT_EQUAL(Status::Ok, writer.Configure(configuration).status);
	ASSERT_TRUE(writer.Contents() == data);
	const auto previous = writer.Configuration();
	configuration.BackPressure = std::numeric_limits<std::size_t>::max();
	ASSERT_EQUAL(Status::Failed, writer.Configure(configuration).status);
	ASSERT_EQUAL(previous.BackPressure, writer.BackPressure());

	for (const bool flush_failure : {false, true}) {
		Writer broken({MaxMemory(128), WriteTransfer(7)});
		broken.m_error_call = flush_failure ? 0 : 2;
		broken.m_fail_flush = flush_failure;
		ASSERT_TRUE(broken.Open());
		ASSERT_EQUAL(Status::Ok, broken.Write(std::span(data)).status);
		auto changed = broken.Configuration();
		changed.MaxMemory = ByteSize{3};
		changed.WriteTransfer = ByteSize{2};
		changed.BackPressure = 4;
		ASSERT_EQUAL(Status::Error, broken.Configure(changed).status);
		ASSERT_EQUAL(ByteSize{128}, broken.MaxMemory());
		ASSERT_EQUAL(ByteSize{7}, broken.WriteTransfer());
		ASSERT_EQUAL(static_cast<std::size_t>(0), broken.BackPressure());
		ASSERT_EQUAL(flush_failure ? ByteSize{37} : ByteSize{9}, broken.Transfers().Bytes);
		ASSERT_EQUAL(flush_failure ? ByteSize{0} : ByteSize{28}, broken.Dirty());
		bool threw = false;
		try { broken.MaxMemory(0); }
		catch (const StormByte::Buffer::Exception&) { threw = true; }
		ASSERT_TRUE(threw);
		ASSERT_FALSE(broken.Close());
	}
	RETURN_TEST(0);
}

int test_write_limits() {
	const auto data = Pattern(379);
	for (const std::size_t chunk : {1u, 13u, 128u, 1024u}) {
		for (const std::size_t budget : {0u, 7u, 256u}) {
			for (const bool ring : {false, true}) {
				Writer writer({WriteTransfer(ByteSize{chunk}), MaxMemory(ByteSize{budget}),
					WriteChunk(ByteSize{ring ? 31u : 0u}), BackPressure(ring ? 16 : 0)});
				writer.m_short_limit = 5;
				writer.m_try_call = 2;
				ASSERT_TRUE(writer.Open());
				ASSERT_EQUAL(Status::Ok, writer.Write(std::span(data)).status);
				ASSERT_EQUAL(Status::Ok, writer.Flush().status);
				ASSERT_TRUE(writer.Contents() == data);
				ASSERT_EQUAL(ByteSize{0}, writer.Dirty());
				ASSERT_TRUE(writer.Close());
				const auto transfers = writer.TransfersMade();
				ASSERT_TRUE(Bounded(transfers, budget ? std::min(chunk, budget) : chunk));
				ASSERT_EQUAL(transfers.size(), writer.Transfers().Calls);
				ASSERT_EQUAL(ByteSize{data.size()}, writer.Transfers().Bytes);
				ASSERT_EQUAL(ByteSize{budget}, writer.Transfers().PageCapacity);
				ASSERT_EQUAL(budget > 0 || !ring, writer.Transfers().RingCapacity == ByteSize{0});
			}
		}
	}
	RETURN_TEST(0);
}

int main() {
	int result = 0;

	// -------------------
	// Transfers
	// -------------------
	result += test_defaults_and_invalid_values();
	result += test_grouping_and_pressure();
	result += test_live_reconfiguration();
	result += test_nonseekable_peek_and_retry();
	result += test_overwrite_fragmentation_fifo();
	result += test_read_limits();
	result += test_reconfiguration_and_failures();
	result += test_write_limits();
	return result;
}
