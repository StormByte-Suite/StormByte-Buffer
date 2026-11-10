// SPDX-License-Identifier: LGPL-3.0-or-later OR LicenseRef-StormByte-Commercial

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
	struct Transfer {
		std::size_t offset;
		std::size_t requested;
		std::size_t count;
	};

	std::vector<std::byte> Pattern(std::size_t n) {
		std::vector<std::byte> data(n);
		for (std::size_t i = 0; i < n; ++i)
			data[i] = static_cast<std::byte>((i * 13 + i / 7) % 251);
		return data;
	}

	class Reader final: public BufferedReader {
		public:
			Reader(Parameters parameters, bool seekable = true):
				BufferedReader("instrumented", Location::Remote, std::move(parameters)),
				data(Pattern(513)), seekable(seekable) {}
			~Reader() override { static_cast<void>(Close()); }
			std::vector<Transfer> TransfersMade() const {
				std::lock_guard lock(mutex);
				return transfers;
			}
			std::vector<std::byte> data;
			std::size_t short_limit = 0;
			std::size_t try_call = 0;
			std::size_t error_call = 0;
			std::atomic<bool> entered {false};
			std::chrono::milliseconds delay {0};
		protected:
			Result OriginOpen() override {
				pos = 0;
				SetState(State::Idle);
				return {Status::Ok, 0};
			}
			Result OriginClose() override { return {Status::Ok, 0}; }
			bool OriginCanSeek() const noexcept override { return seekable; }
			bool OriginHasSize() const noexcept override { return true; }
			StormByte::Safe::Optional<ByteSize> OriginSize() const noexcept override { return ByteSize{data.size()}; }
			Result OriginSeek(std::ptrdiff_t offset, Position mode) override {
				pos = mode == Position::Absolute ? offset : pos + offset;
				return {Status::Ok, 0};
			}
			Result OriginPull(ByteSize n, FIFO& dest) override {
				entered.store(true);
				std::this_thread::sleep_for(delay);
				std::lock_guard lock(mutex);
				const auto request = static_cast<std::size_t>(n);
				const auto call = transfers.size() + 1;
				if (call == try_call || call == error_call) {
					transfers.push_back({pos, request, 0});
					return {call == try_call ? Status::TryAgain : Status::Error, 0};
				}
				auto count = std::min(request, data.size() - pos);
				if (short_limit)
					count = std::min(count, short_limit);
				if (count)
					static_cast<void>(dest.Write(StormByte::Safe::Binary(data.data() + pos, ByteSize{count})));
				transfers.push_back({pos, request, count});
				pos += count;
				return {pos == data.size() ? Status::End : Status::Ok, ByteSize{count}};
			}
		private:
			mutable std::mutex mutex;
			std::vector<Transfer> transfers;
			std::size_t pos = 0;
			bool seekable;
	};

	class Writer final: public BufferedWriter {
		public:
			explicit Writer(Parameters parameters):
				BufferedWriter("instrumented", Location::Remote, std::move(parameters)) {}
			~Writer() override { static_cast<void>(Close()); }
			std::vector<Transfer> TransfersMade() const {
				std::lock_guard lock(mutex);
				return transfers;
			}
			std::vector<std::byte> Contents() const {
				std::lock_guard lock(mutex);
				return data;
			}
			std::size_t short_limit = 0;
			std::size_t try_call = 0;
			std::size_t error_call = 0;
			bool fail_flush = false;
			bool retry_forever = false;
			std::atomic<bool> entered {false};
			std::chrono::milliseconds delay {0};
		protected:
			Result OriginOpen() override {
				SetState(State::Idle);
				return {Status::Ok, 0};
			}
			Result OriginClose() override { return {Status::Ok, 0}; }
			Result OriginFlush() override { return {fail_flush ? Status::Error : Status::Ok, 0}; }
			Result OriginTruncate() override {
				std::lock_guard lock(mutex);
				data.clear();
				pos = 0;
				return {Status::Ok, 0};
			}
			Result OriginSeek(ByteSize offset) override {
				pos = static_cast<std::size_t>(offset);
				return {Status::Ok, 0};
			}
			Result OriginPush(std::span<const std::byte> src) override {
				entered.store(true);
				std::this_thread::sleep_for(delay);
				std::lock_guard lock(mutex);
				const auto call = transfers.size() + 1;
				if (call == try_call || retry_forever) {
					transfers.push_back({pos, src.size(), 0});
					return {Status::TryAgain, 0};
				}
				auto count = short_limit ? std::min(short_limit, src.size()) : src.size();
				if (call == error_call)
					count = std::min<std::size_t>(2, count);
				data.resize(std::max(data.size(), pos + count));
				std::copy_n(src.begin(), count, data.begin() + pos);
				transfers.push_back({pos, src.size(), count});
				pos += count;
				return {call == error_call ? Status::Error : Status::Ok, ByteSize{count}};
			}
		private:
			mutable std::mutex mutex;
			std::vector<Transfer> transfers;
			std::vector<std::byte> data;
			std::size_t pos = 0;
	};

	bool Matches(FIFO& fifo, const std::vector<std::byte>& data, std::size_t offset, std::size_t count) {
		StormByte::Safe::Binary bytes;
		return fifo.Extract(ByteSize{count}, bytes)
			&& std::equal(bytes.begin(), bytes.end(), data.begin() + offset);
	}

	bool Bounded(const std::vector<Transfer>& transfers, std::size_t limit) {
		return !transfers.empty() && std::all_of(transfers.begin(), transfers.end(),
			[limit](const auto& transfer) { return transfer.requested > 0 && transfer.requested <= limit; });
	}
}

int test_read_limits() {
	for (const std::size_t chunk : {1u, 17u, 128u, 1024u}) {
		for (const std::size_t budget : {0u, 11u, 256u}) {
			for (const std::size_t ahead : {0u, 200u}) {
				Reader reader({ReadChunk(ByteSize{chunk}), MaxMemory(ByteSize{budget}), ReadAhead(ByteSize{ahead})});
				reader.short_limit = 7;
				ASSERT_TRUE(reader.Open());
				FIFO peek;
				ASSERT_EQUAL(Status::Ok, reader.Peek(193, peek).status);
				ASSERT_TRUE(Matches(peek, reader.data, 0, 193));
				ASSERT_EQUAL(ByteSize{0}, reader.Tell());
				FIFO got;
				ASSERT_EQUAL(Status::Ok, reader.Read(193, got).status);
				ASSERT_TRUE(Matches(got, reader.data, 0, 193));
				ASSERT_EQUAL(Status::Ok, reader.Seek(5, Position::Absolute).status);
				std::vector<std::byte> small(3);
				ASSERT_EQUAL(Status::Ok, reader.Read(small).status);
				ASSERT_TRUE(std::equal(small.begin(), small.end(), reader.data.begin() + 5));
				ASSERT_EQUAL(Status::Ok, reader.Seek(500, Position::Absolute).status);
				FIFO tail;
				const auto result = reader.Read(99, tail);
				ASSERT_EQUAL(Status::End, result.status);
				ASSERT_EQUAL(ByteSize{13}, result.count);
				ASSERT_TRUE(Matches(tail, reader.data, 500, 13));
				ASSERT_EQUAL(Status::Ok, reader.Close().status);
				const auto limit = budget ? std::min(chunk, budget) : chunk;
				const auto transfers = reader.TransfersMade();
				ASSERT_TRUE(Bounded(transfers, limit));
				ASSERT_EQUAL(transfers.size(), reader.Transfers().calls);
				std::size_t bytes = 0;
				for (const auto& transfer : transfers)
					bytes += transfer.count;
				ASSERT_EQUAL(ByteSize{bytes}, reader.Transfers().bytes);
			}
		}
	}
	RETURN_TEST(0);
}

int test_nonseekable_peek_and_retry() {
	for (const bool seekable : {false, true}) {
		for (const std::size_t budget : {0u, 5u, 100u}) {
			Reader reader({ReadChunk(7), ReadAhead(0), MaxMemory(ByteSize{budget})}, seekable);
			reader.try_call = 3;
			ASSERT_TRUE(reader.Open());
			FIFO got("unchanged");
			ASSERT_EQUAL(Status::TryAgain, reader.Peek(83, got).status);
			ASSERT_EQUAL(ByteSize{9}, got.Available());
			ASSERT_EQUAL(ByteSize{0}, reader.Tell());
			ASSERT_EQUAL(Status::Ok, reader.Peek(83, got).status);
			ASSERT_TRUE(Matches(got, reader.data, 0, 83));
			ASSERT_EQUAL(Status::Ok, reader.Read(11, got).status);
			ASSERT_TRUE(Matches(got, reader.data, 0, 11));
			ASSERT_EQUAL(Status::Ok, reader.Read(100, got).status);
			ASSERT_TRUE(Matches(got, reader.data, 11, 100));
			ASSERT_TRUE(Bounded(reader.TransfersMade(), budget ? std::min<std::size_t>(7, budget) : 7));
		}
	}
	Reader broken({ReadChunk(4), ReadAhead(0), MaxMemory(0)});
	broken.error_call = 2;
	ASSERT_TRUE(broken.Open());
	FIFO unchanged("keep");
	ASSERT_EQUAL(Status::Error, broken.Read(20, unchanged).status);
	ASSERT_EQUAL(ByteSize{4}, unchanged.Available());
	ASSERT_EQUAL(ByteSize{0}, broken.Tell());
	RETURN_TEST(0);
}

int test_write_limits() {
	const auto data = Pattern(379);
	for (const std::size_t chunk : {1u, 13u, 128u, 1024u}) {
		for (const std::size_t budget : {0u, 7u, 256u}) {
			for (const bool ring : {false, true}) {
				Writer writer({WriteTransfer(ByteSize{chunk}), MaxMemory(ByteSize{budget}),
					WriteChunk(ByteSize{ring ? 31u : 0u}), BackPressure(ring ? 16 : 0)});
				writer.short_limit = 5;
				writer.try_call = 2;
				ASSERT_TRUE(writer.Open());
				ASSERT_EQUAL(Status::Ok, writer.Write(std::span(data)).status);
				ASSERT_EQUAL(Status::Ok, writer.Flush().status);
				ASSERT_TRUE(writer.Contents() == data);
				ASSERT_EQUAL(ByteSize{0}, writer.Dirty());
				ASSERT_TRUE(writer.Close());
				const auto transfers = writer.TransfersMade();
				ASSERT_TRUE(Bounded(transfers, budget ? std::min(chunk, budget) : chunk));
				ASSERT_EQUAL(transfers.size(), writer.Transfers().calls);
				ASSERT_EQUAL(ByteSize{data.size()}, writer.Transfers().bytes);
				ASSERT_EQUAL(ByteSize{budget}, writer.Transfers().page_capacity);
				ASSERT_EQUAL(budget > 0 || !ring, writer.Transfers().ring_capacity == ByteSize{0});
			}
		}
	}
	RETURN_TEST(0);
}

int test_grouping_and_pressure() {
	const auto data = Pattern(257);
	for (const std::size_t transfer : {16u, 64u}) {
		Writer writer({WriteTransfer(ByteSize{transfer}), WriteThreshold(128), MaxMemory(128)});
		ASSERT_TRUE(writer.Open());
		for (std::size_t i = 0; i < 127; ++i) {
			ASSERT_EQUAL(Status::Ok, writer.Write(std::span(data).subspan(i, 1)).status);
			ASSERT_EQUAL(static_cast<std::size_t>(0), writer.Transfers().calls);
		}
		ASSERT_EQUAL(Status::Ok, writer.Write(std::span(data).subspan(127, 1)).status);
		ASSERT_EQUAL(ByteSize{64}, writer.Dirty());
		ASSERT_EQUAL(64 / transfer, writer.Transfers().calls);
		for (const auto& push : writer.TransfersMade())
			ASSERT_EQUAL(transfer, push.requested);
		ASSERT_EQUAL(Status::Ok, writer.Write(std::span(data).subspan(128)).status);
		ASSERT_TRUE(writer.Transfers().pending <= ByteSize{128});
		ASSERT_EQUAL(Status::Ok, writer.Flush().status);
		ASSERT_TRUE(writer.Contents() == data);
		ASSERT_EQUAL(ByteSize{0}, writer.Transfers().pending);
	}
	Writer leftovers({WriteTransfer(64), WriteThreshold(100), MaxMemory(128)});
	ASSERT_TRUE(leftovers.Open());
	ASSERT_EQUAL(Status::Ok, leftovers.Write(std::span(data).first(7)).status);
	ASSERT_EQUAL(static_cast<std::size_t>(0), leftovers.Transfers().calls);
	ASSERT_TRUE(leftovers.Close());
	ASSERT_TRUE(leftovers.Contents() == std::vector<std::byte>(data.begin(), data.begin() + 7));
	ASSERT_TRUE(Bounded(leftovers.TransfersMade(), 64));
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
		ASSERT_TRUE(writer.Transfers().pending <= ByteSize{47});
	}
	ASSERT_EQUAL(Status::Ok, writer.Flush().status);
	ASSERT_TRUE(writer.Contents() == expected);
	ASSERT_TRUE(Bounded(writer.TransfersMade(), 11));
	RETURN_TEST(0);
}

int test_reconfiguration_and_failures() {
	const auto data = Pattern(37);
	Writer writer({WriteChunk(32), BackPressure(3), WriteTransfer(7), MaxMemory(0)});
	ASSERT_TRUE(writer.Open());
	ASSERT_EQUAL(Status::Ok, writer.Write(std::span(data).first(13)).status);
	auto configuration = writer.Configuration();
	configuration.write_chunk = ByteSize{5};
	configuration.back_pressure = 2;
	configuration.write_transfer = ByteSize{3};
	ASSERT_EQUAL(Status::Ok, writer.Configure(configuration).status);
	ASSERT_EQUAL(ByteSize{10}, writer.Transfers().queue_capacity);
	ASSERT_EQUAL(ByteSize{16}, writer.Transfers().ring_capacity);
	ASSERT_EQUAL(Status::TryAgain, writer.Write(std::span(data)).status);
	ASSERT_EQUAL(Status::Ok, writer.Write(std::span(data).subspan(13, 9)).status);
	configuration.max_memory = ByteSize{4};
	ASSERT_EQUAL(Status::Ok, writer.Configure(configuration).status);
	ASSERT_EQUAL(ByteSize{0}, writer.Transfers().ring_capacity);
	ASSERT_EQUAL(Status::Ok, writer.Write(std::span(data).subspan(22)).status);
	configuration.max_memory = ByteSize{0};
	configuration.back_pressure = 0;
	ASSERT_EQUAL(Status::Ok, writer.Configure(configuration).status);
	ASSERT_TRUE(writer.Contents() == data);
	const auto previous = writer.Configuration();
	configuration.back_pressure = std::numeric_limits<std::size_t>::max();
	ASSERT_EQUAL(Status::Failed, writer.Configure(configuration).status);
	ASSERT_EQUAL(previous.back_pressure, writer.BackPressure());

	for (const bool flush_failure : {false, true}) {
		Writer broken({MaxMemory(128), WriteTransfer(7)});
		broken.error_call = flush_failure ? 0 : 2;
		broken.fail_flush = flush_failure;
		ASSERT_TRUE(broken.Open());
		ASSERT_EQUAL(Status::Ok, broken.Write(std::span(data)).status);
		auto changed = broken.Configuration();
		changed.max_memory = ByteSize{3};
		changed.write_transfer = ByteSize{2};
		changed.back_pressure = 4;
		ASSERT_EQUAL(Status::Error, broken.Configure(changed).status);
		ASSERT_EQUAL(ByteSize{128}, broken.MaxMemory());
		ASSERT_EQUAL(ByteSize{7}, broken.WriteTransfer());
		ASSERT_EQUAL(static_cast<std::size_t>(0), broken.BackPressure());
		ASSERT_EQUAL(flush_failure ? ByteSize{37} : ByteSize{9}, broken.Transfers().bytes);
		ASSERT_EQUAL(flush_failure ? ByteSize{0} : ByteSize{28}, broken.Dirty());
		bool threw = false;
		try { broken.MaxMemory(0); }
		catch (const StormByte::Buffer::Exception&) { threw = true; }
		ASSERT_TRUE(threw);
		ASSERT_FALSE(broken.Close());
	}
	RETURN_TEST(0);
}

int test_live_reconfiguration() {
	const auto data = Pattern(2100);
	Writer writer({WriteTransfer(23), MaxMemory(128), WriteThreshold(96)});
	writer.delay = std::chrono::milliseconds(1);
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
		configuration.write_transfer = ByteSize{i % 9 + 1};
		configuration.max_memory = ByteSize{i % 3 == 0 ? 0u : 31u};
		configuration.write_threshold = ByteSize{21};
		configuration.write_chunk = ByteSize{8};
		configuration.back_pressure = i % 2 ? 3 : 0;
		configured = configured && writer.Configure(configuration).status == Status::Ok;
		const auto snapshot = writer.Transfers();
		configured = configured && snapshot.pending <= ByteSize{128};
	}
	producer.join();
	ASSERT_TRUE(good);
	ASSERT_TRUE(configured);
	ASSERT_EQUAL(Status::Ok, writer.Flush().status);
	ASSERT_TRUE(writer.Contents() == data);

	Reader reader({ReadChunk(31), MaxMemory(97), ReadAhead(50)});
	reader.delay = std::chrono::milliseconds(1);
	ASSERT_TRUE(reader.Open());
	bool read_good = true;
	std::thread consumer([&] {
		FIFO got;
		for (std::size_t offset = 0; offset < reader.data.size(); offset += 7) {
			const auto take = std::min<std::size_t>(7, reader.data.size() - offset);
			const auto result = reader.Read(ByteSize{take}, got);
			if (result.status != Status::Ok || !Matches(got, reader.data, offset, take)) {
				read_good = false;
				return;
			}
		}
	});
	for (std::size_t i = 0; i < 20; ++i) {
		auto configuration = reader.Configuration();
		configuration.read_chunk = ByteSize{i % 7 + 1};
		configuration.max_memory = ByteSize{i % 2 ? 3u : 0u};
		configuration.read_ahead = ByteSize{i % 2 ? 11u : 0u};
		configured = configured && reader.Configure(configuration).status == Status::Ok;
	}
	consumer.join();
	ASSERT_TRUE(read_good);
	ASSERT_TRUE(configured);
	ASSERT_EQUAL(Status::Ok, reader.Close().status);
	ASSERT_TRUE(Bounded(reader.TransfersMade(), 31));
	RETURN_TEST(0);
}

int test_defaults_and_invalid_values() {
	Reader reader({ReadAhead(0), MaxMemory(0)});
	reader.data = Pattern(140000);
	ASSERT_EQUAL(ByteSize{65536}, reader.ReadChunk());
	reader.ReadChunk(0);
	ASSERT_EQUAL(ByteSize{65536}, reader.ReadChunk());
	ASSERT_TRUE(reader.Open());
	FIFO got;
	ASSERT_EQUAL(Status::Ok, reader.Read(130000, got).status);
	ASSERT_TRUE(Matches(got, reader.data, 0, 130000));
	ASSERT_TRUE(Bounded(reader.TransfersMade(), 65536));
	auto policy = reader.Configuration();
	policy.max_wait = std::chrono::milliseconds(-1);
	ASSERT_EQUAL(Status::Failed, reader.Configure(policy).status);
	ASSERT_EQUAL(std::chrono::milliseconds(0), reader.MaxWait());
	ASSERT_EQUAL(Status::Failed, reader.Seek(std::numeric_limits<std::ptrdiff_t>::min(), Position::Relative).status);
	Writer writer({MaxMemory(0)});
	ASSERT_TRUE(writer.Open());
	const auto data = Pattern(257);
	ASSERT_EQUAL(Status::Ok, writer.Write(std::span(data)).status);
	ASSERT_EQUAL(ByteSize{257}, writer.Transfers().max_request);
	ASSERT_EQUAL(ByteSize{0}, writer.WriteTransfer());
	ASSERT_EQUAL(Status::Failed, writer.Seek(std::numeric_limits<std::ptrdiff_t>::min(), Position::Relative).status);
	Writer stalled({MaxMemory(32), WriteTransfer(7), MaxWait(std::chrono::milliseconds(3))});
	stalled.retry_forever = true;
	ASSERT_TRUE(stalled.Open());
	ASSERT_EQUAL(Status::Ok, stalled.Write(std::span(data).first(10)).status);
	ASSERT_EQUAL(Status::Error, stalled.Flush().status);
	ASSERT_EQUAL(ByteSize{10}, stalled.Dirty());
	ASSERT_EQUAL(ByteSize{0}, stalled.Transfers().bytes);
	RETURN_TEST(0);
}

int main() {
	int result = 0;
	result += test_read_limits();
	result += test_nonseekable_peek_and_retry();
	result += test_write_limits();
	result += test_grouping_and_pressure();
	result += test_overwrite_fragmentation_fifo();
	result += test_reconfiguration_and_failures();
	result += test_live_reconfiguration();
	result += test_defaults_and_invalid_values();
	return result;
}
