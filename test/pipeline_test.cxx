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

#include <StormByte/buffer/pipe.hxx>
#include <StormByte/buffer/pipeline.hxx>
#include <StormByte/buffer/producer.hxx>
#include <StormByte/logger/log.hxx>
#include <StormByte/test_handlers.h>

#include <algorithm>
#include <cctype>
#include <chrono>
#include <cstdint>
#include <functional>
#include <iostream>
#include <sstream>
#include <string>
#include <thread>
#include <vector>

using StormByte::BinaryData;
using StormByte::Buffer::Consumer;
using StormByte::Buffer::ExecutionMode;
using StormByte::Buffer::Pipe;
using StormByte::Buffer::Pipeline;
using StormByte::Buffer::Producer;
using StormByte::Buffer::ReadOnly;
using StormByte::Buffer::WriteOnly;

#define LARGE_TEST_SIZE_KB 1024

namespace {
	constexpr ExecutionMode kAsyncParallel = ExecutionMode::Async | ExecutionMode::Parallel;

	std::ostringstream logging_stream;
	const StormByte::Safe::Shared<StormByte::Logger::Log> logging =
		StormByte::Safe::Shared<StormByte::Logger::Log>::MakePointer<StormByte::Logger::Log>(
			logging_stream, StormByte::Logger::Level::Info);

	std::string BytesToText(const BinaryData& data) {
		if (data.empty())
			return {};
		return std::string(reinterpret_cast<const char*>(data.data()),
			static_cast<std::size_t>(data.size()));
	}

	BinaryData TextBytes(const std::string& text) {
		if (text.empty())
			return {};
		return BinaryData(
			reinterpret_cast<const std::byte*>(text.data()),
			StormByte::ByteSize{text.size()});
	}

	void WaitDone(Consumer& consumer) {
		while (consumer.IsWritable())
			std::this_thread::yield();
	}

	BinaryData Drain(Consumer& consumer) {
		WaitDone(consumer);
		BinaryData all;
		for (;;) {
			BinaryData chunk;
			if (!consumer.Read(0, chunk) || chunk.empty())
				break;
			all.insert(all.end(), chunk.begin(), chunk.end());
		}
		return all;
	}

	int ExpectBytes(const char* fn, int& result, Consumer& consumer, const BinaryData& expected) {
		const BinaryData got = Drain(consumer);
		ASSERT_TRUE(fn, got == expected);
		ASSERT_EQUAL(fn, expected.size(), got.size());
		return result;
	}

	int ExpectText(const char* fn, int& result, Consumer& consumer, const std::string& expected) {
		return ExpectBytes(fn, result, consumer, TextBytes(expected));
	}

	class FnPipe final: public Pipe {
		public:
			using Fn = std::function<void(ReadOnly&, WriteOnly&,
				const StormByte::Safe::Shared<StormByte::Logger::Log>&)>;

			explicit FnPipe(Fn fn):
				m_fn(std::move(fn)) {}

			void Run(ReadOnly& in, WriteOnly& out,
					const StormByte::Safe::Shared<StormByte::Logger::Log>& log) override {
				m_fn(in, out, log);
			}

			PointerType Clone() const noexcept override {
				return StormByte::Safe::Unique<Pipe>::MakePointer<FnPipe>(*this);
			}

			PointerType Move() noexcept override {
				return StormByte::Safe::Unique<Pipe>::MakePointer<FnPipe>(std::move(*this));
			}

		private:
			Fn m_fn;
	};

	FnPipe MakePipe(FnPipe::Fn fn) {
		return FnPipe(std::move(fn));
	}

	void CopyAll(ReadOnly& in, WriteOnly& out) {
		while (!in.EoF()) {
			BinaryData data;
			if (in.Read(0, data) && !data.empty())
				(void)out.Write(data);
		}
		out.Close();
	}

	void UpperAll(ReadOnly& in, WriteOnly& out) {
		while (!in.EoF()) {
			BinaryData data;
			if (in.Read(0, data) && !data.empty()) {
				std::string str = BytesToText(data);
				for (auto& c : str)
					c = static_cast<char>(std::toupper(static_cast<unsigned char>(c)));
				(void)out.Write(str);
			}
		}
		out.Close();
	}

	BinaryData MapBytes(BinaryData data, const auto& op) {
		for (auto& b : data)
			b = op(b);
		return data;
	}
}

// -------------------
// Basic
// -------------------

int test_pipeline_empty() {
	constexpr auto fn = "test_pipeline_empty";
	int result = 0;
	Pipeline pipeline;
	Producer input;
	(void)input.Write("TEST");
	input.Close();
	Consumer out = pipeline.Process(input.Consumer(), logging, ExecutionMode::Async);
	return ExpectText(fn, result, out, "TEST");
}

int test_pipeline_empty_input() {
	constexpr auto fn = "test_pipeline_empty_input";
	int result = 0;
	Pipeline pipeline;
	pipeline.Add(MakePipe([](ReadOnly& in, WriteOnly& out,
			const StormByte::Safe::Shared<StormByte::Logger::Log>&) {
		CopyAll(in, out);
	}));
	Producer input;
	input.Close();
	Consumer out = pipeline.Process(input.Consumer(), logging, ExecutionMode::Async);
	return ExpectBytes(fn, result, out, {});
}

int test_pipeline_filter_stage() {
	constexpr auto fn = "test_pipeline_filter_stage";
	int result = 0;
	Pipeline pipeline;
	pipeline.Add(MakePipe([](ReadOnly& in, WriteOnly& out,
			const StormByte::Safe::Shared<StormByte::Logger::Log>&) {
		while (!in.EoF()) {
			BinaryData data;
			if (in.Read(0, data) && !data.empty()) {
				std::string filtered;
				for (char c : BytesToText(data))
					if (std::isalpha(static_cast<unsigned char>(c)))
						filtered += c;
				if (!filtered.empty())
					(void)out.Write(filtered);
			}
		}
		out.Close();
	}));
	Producer input;
	(void)input.Write("Hello123World456!");
	input.Close();
	Consumer out = pipeline.Process(input.Consumer(), logging, ExecutionMode::Async);
	return ExpectText(fn, result, out, "HelloWorld");
}

int test_pipeline_incremental_processing() {
	constexpr auto fn = "test_pipeline_incremental_processing";
	int result = 0;
	Pipeline pipeline;
	pipeline.Add(MakePipe([](ReadOnly& in, WriteOnly& out,
			const StormByte::Safe::Shared<StormByte::Logger::Log>&) {
		while (!in.EoF()) {
			BinaryData data;
			if (in.Read(1, data) && !data.empty()) {
				const char c = static_cast<char>(std::toupper(
					static_cast<unsigned char>(data[0])));
				(void)out.Write(std::string(1, c));
			}
		}
		out.Close();
	}));
	Producer input;
	(void)input.Write("abc");
	input.Close();
	Consumer out = pipeline.Process(input.Consumer(), logging, ExecutionMode::Async);
	return ExpectText(fn, result, out, "ABC");
}

int test_pipeline_multiple_writes() {
	constexpr auto fn = "test_pipeline_multiple_writes";
	int result = 0;
	Pipeline pipeline;
	pipeline.Add(MakePipe([](ReadOnly& in, WriteOnly& out,
			const StormByte::Safe::Shared<StormByte::Logger::Log>&) {
		while (!in.EoF()) {
			BinaryData data;
			if (in.Read(0, data) && !data.empty()) {
				(void)out.Write(data);
				(void)out.Write(data);
			}
		}
		out.Close();
	}));
	Producer input;
	(void)input.Write("AB");
	input.Close();
	Consumer out = pipeline.Process(input.Consumer(), logging, ExecutionMode::Async);
	return ExpectText(fn, result, out, "ABAB");
}

int test_pipeline_single_stage() {
	constexpr auto fn = "test_pipeline_single_stage";
	int result = 0;
	Pipeline pipeline;
	pipeline.Add(MakePipe([](ReadOnly& in, WriteOnly& out,
			const StormByte::Safe::Shared<StormByte::Logger::Log>&) {
		UpperAll(in, out);
	}));
	Producer input;
	(void)input.Write("hello world");
	input.Close();
	Consumer out = pipeline.Process(input.Consumer(), logging, ExecutionMode::Async);
	return ExpectText(fn, result, out, "HELLO WORLD");
}

int test_pipeline_three_stages() {
	constexpr auto fn = "test_pipeline_three_stages";
	int result = 0;
	Pipeline pipeline;
	pipeline.Add(MakePipe([](ReadOnly& in, WriteOnly& out,
			const StormByte::Safe::Shared<StormByte::Logger::Log>&) {
		UpperAll(in, out);
	}));
	pipeline.Add(MakePipe([](ReadOnly& in, WriteOnly& out,
			const StormByte::Safe::Shared<StormByte::Logger::Log>&) {
		while (!in.EoF()) {
			BinaryData data;
			if (in.Read(0, data) && !data.empty()) {
				std::string str = BytesToText(data);
				std::replace(str.begin(), str.end(), ' ', '-');
				(void)out.Write(str);
			}
		}
		out.Close();
	}));
	pipeline.Add(MakePipe([](ReadOnly& in, WriteOnly& out,
			const StormByte::Safe::Shared<StormByte::Logger::Log>&) {
		(void)out.Write("[");
		while (!in.EoF()) {
			BinaryData data;
			if (in.Read(0, data) && !data.empty())
				(void)out.Write(BytesToText(data));
		}
		(void)out.Write("]");
		out.Close();
	}));
	Producer input;
	(void)input.Write("test data");
	input.Close();
	Consumer out = pipeline.Process(input.Consumer(), logging, kAsyncParallel);
	return ExpectText(fn, result, out, "[TEST-DATA]");
}

int test_pipeline_two_stages() {
	constexpr auto fn = "test_pipeline_two_stages";
	int result = 0;
	Pipeline pipeline;
	pipeline.Add(MakePipe([](ReadOnly& in, WriteOnly& out,
			const StormByte::Safe::Shared<StormByte::Logger::Log>&) {
		UpperAll(in, out);
	}));
	pipeline.Add(MakePipe([](ReadOnly& in, WriteOnly& out,
			const StormByte::Safe::Shared<StormByte::Logger::Log>&) {
		while (!in.EoF()) {
			BinaryData data;
			if (in.Read(0, data) && !data.empty()) {
				std::string str = BytesToText(data);
				std::replace(str.begin(), str.end(), ' ', '_');
				(void)out.Write(str);
			}
		}
		out.Close();
	}));
	Producer input;
	(void)input.Write("hello world test");
	input.Close();
	Consumer out = pipeline.Process(input.Consumer(), logging, kAsyncParallel);
	return ExpectText(fn, result, out, "HELLO_WORLD_TEST");
}

// -------------------
// Construct
// -------------------

int test_pipeline_add_move() {
	constexpr auto fn = "test_pipeline_add_move";
	int result = 0;
	Pipeline pipeline;
	FnPipe pipe = MakePipe([](ReadOnly& in, WriteOnly& out,
			const StormByte::Safe::Shared<StormByte::Logger::Log>&) {
		CopyAll(in, out);
	});
	pipeline.Add(std::move(pipe));
	Producer input;
	(void)input.Write("MOVE");
	input.Close();
	Consumer out = pipeline.Process(input.Consumer(), logging, ExecutionMode::Async);
	return ExpectText(fn, result, out, "MOVE");
}

int test_pipeline_copy_constructor() {
	constexpr auto fn = "test_pipeline_copy_constructor";
	int result = 0;
	Pipeline pipeline1;
	pipeline1.Add(MakePipe([](ReadOnly& in, WriteOnly& out,
			const StormByte::Safe::Shared<StormByte::Logger::Log>&) {
		UpperAll(in, out);
	}));
	Pipeline pipeline2 = pipeline1;
	Producer input;
	(void)input.Write("test");
	input.Close();
	Consumer out = pipeline2.Process(input.Consumer(), logging, ExecutionMode::Async);
	return ExpectText(fn, result, out, "TEST");
}

int test_pipeline_move_constructor() {
	constexpr auto fn = "test_pipeline_move_constructor";
	int result = 0;
	Pipeline pipeline1;
	pipeline1.Add(MakePipe([](ReadOnly& in, WriteOnly& out,
			const StormByte::Safe::Shared<StormByte::Logger::Log>&) {
		while (!in.EoF()) {
			BinaryData data;
			if (in.Read(0, data) && !data.empty()) {
				std::string str = BytesToText(data);
				for (auto& c : str)
					c = static_cast<char>(std::tolower(static_cast<unsigned char>(c)));
				(void)out.Write(str);
			}
		}
		out.Close();
	}));
	Pipeline pipeline2 = std::move(pipeline1);
	Producer input;
	(void)input.Write("TEST");
	input.Close();
	Consumer out = pipeline2.Process(input.Consumer(), logging, ExecutionMode::Async);
	return ExpectText(fn, result, out, "test");
}

int test_pipeline_null_logger() {
	constexpr auto fn = "test_pipeline_null_logger";
	int result = 0;
	Pipeline pipeline;
	pipeline.Add(MakePipe([](ReadOnly& in, WriteOnly& out,
			const StormByte::Safe::Shared<StormByte::Logger::Log>&) {
		CopyAll(in, out);
	}));
	Producer input;
	(void)input.Write("null-log");
	input.Close();
	Consumer out = pipeline.Process(input.Consumer(), {}, ExecutionMode::Sync);
	return ExpectText(fn, result, out, "null-log");
}

int test_pipeline_reuse() {
	constexpr auto fn = "test_pipeline_reuse";
	int result = 0;
	Pipeline pipeline;
	pipeline.Add(MakePipe([](ReadOnly& in, WriteOnly& out,
			const StormByte::Safe::Shared<StormByte::Logger::Log>&) {
		(void)out.Write(">");
		while (!in.EoF()) {
			BinaryData data;
			if (in.Read(0, data) && !data.empty())
				(void)out.Write(data);
		}
		out.Close();
	}));
	{
		Producer input1;
		(void)input1.Write("TEST1");
		input1.Close();
		Consumer out1 = pipeline.Process(input1.Consumer(), logging, ExecutionMode::Async);
		if (ExpectText(fn, result, out1, ">TEST1") != 0)
			return result;
	}
	{
		Producer input2;
		(void)input2.Write("TEST2");
		input2.Close();
		Consumer out2 = pipeline.Process(input2.Consumer(), logging, ExecutionMode::Async);
		return ExpectText(fn, result, out2, ">TEST2");
	}
}

int test_pipeline_stage_must_close() {
	constexpr auto fn = "test_pipeline_stage_must_close";
	int result = 0;
	Pipeline pipeline;
	pipeline.Add(MakePipe([](ReadOnly& in, WriteOnly& out,
			const StormByte::Safe::Shared<StormByte::Logger::Log>&) {
		CopyAll(in, out);
	}));
	Producer input;
	(void)input.Write("close-me");
	input.Close();
	Consumer out = pipeline.Process(input.Consumer(), logging, ExecutionMode::Sync);
	ASSERT_TRUE(fn, out.EoF() || !out.IsWritable());
	return ExpectText(fn, result, out, "close-me");
}

// -------------------
// Execution
// -------------------

int test_pipeline_async_reuse_many_times() {
	constexpr auto fn = "test_pipeline_async_reuse_many_times";
	int result = 0;
	Pipeline pipeline;
	pipeline.Add(MakePipe([](ReadOnly& in, WriteOnly& out,
			const StormByte::Safe::Shared<StormByte::Logger::Log>&) {
		CopyAll(in, out);
	}));
	for (int i = 0; i < 50; ++i) {
		Producer input;
		const std::string msg = "RUN-" + std::to_string(i);
		(void)input.Write(msg);
		input.Close();
		Consumer out = pipeline.Process(input.Consumer(), logging, ExecutionMode::Async);
		if (ExpectText(fn, result, out, msg) != 0)
			return result;
	}
	RETURN_TEST(fn, result);
}

int test_pipeline_async_seterror_interrupts_quickly() {
	constexpr auto fn = "test_pipeline_async_seterror_interrupts_quickly";
	int result = 0;
	Pipeline pipeline;
	for (int i = 0; i < 12; ++i) {
		pipeline.Add(MakePipe([](ReadOnly& in, WriteOnly& out,
				const StormByte::Safe::Shared<StormByte::Logger::Log>&) {
			while (!in.EoF()) {
				if (!out.IsWritable())
					return;
				BinaryData data;
				if (in.Read(0, data) && !data.empty()) {
					std::this_thread::sleep_for(std::chrono::milliseconds(2));
					if (!out.IsWritable())
						return;
					(void)out.Write(data);
				}
			}
			if (out.IsWritable())
				out.Close();
		}));
	}
	Producer input;
	(void)input.Write(std::string(100000, 'X'));
	input.Close();
	Consumer out = pipeline.Process(input.Consumer(), logging, kAsyncParallel);
	std::this_thread::sleep_for(std::chrono::milliseconds(5));
	pipeline.SetError();
	WaitDone(out);
	ASSERT_FALSE(fn, out.IsWritable());
	ASSERT_TRUE(fn, out.EoF());
	return ExpectBytes(fn, result, out, {});
}

int test_pipeline_interrupted_by_seterror() {
	constexpr auto fn = "test_pipeline_interrupted_by_seterror";
	int result = 0;
	Pipeline pipeline;
	for (int i = 0; i < 8; ++i) {
		pipeline.Add(MakePipe([](ReadOnly& in, WriteOnly& out,
				const StormByte::Safe::Shared<StormByte::Logger::Log>&) {
			while (!in.EoF()) {
				BinaryData data;
				if (in.Read(0, data) && !data.empty()) {
					for (int k = 0; k < 200; ++k) {
						if (!out.IsWritable())
							return;
						std::this_thread::yield();
					}
					if (!out.IsWritable())
						return;
					(void)out.Write(data);
				}
			}
			if (out.IsWritable())
				out.Close();
		}));
	}
	Producer input;
	(void)input.Write(std::string(50000, 'X'));
	input.Close();
	Consumer out = pipeline.Process(input.Consumer(), logging, kAsyncParallel);
	pipeline.SetError();
	WaitDone(out);
	ASSERT_FALSE(fn, out.IsWritable());
	ASSERT_TRUE(fn, out.EoF());
	return ExpectBytes(fn, result, out, {});
}

int test_pipeline_large_async_many_stages() {
	constexpr auto fn = "test_pipeline_large_async_many_stages";
	int result = 0;
	Pipeline pipeline;
	for (int i = 0; i < 25; ++i) {
		pipeline.Add(MakePipe([](ReadOnly& in, WriteOnly& out,
				const StormByte::Safe::Shared<StormByte::Logger::Log>&) {
			UpperAll(in, out);
		}));
	}
	Producer input;
	(void)input.Write(std::string(8192, 'a'));
	input.Close();
	Consumer out = pipeline.Process(input.Consumer(), logging, kAsyncParallel);
	return ExpectText(fn, result, out, std::string(8192, 'A'));
}

int test_pipeline_parallel_async_correctness() {
	constexpr auto fn = "test_pipeline_parallel_async_correctness";
	int result = 0;
	Pipeline pipeline;
	for (int i = 0; i < 6; ++i) {
		pipeline.Add(MakePipe([](ReadOnly& in, WriteOnly& out,
				const StormByte::Safe::Shared<StormByte::Logger::Log>&) {
			UpperAll(in, out);
		}));
	}
	Producer input;
	(void)input.Write("parallel-async-ok");
	input.Close();
	Consumer out = pipeline.Process(input.Consumer(), logging, kAsyncParallel);
	return ExpectText(fn, result, out, "PARALLEL-ASYNC-OK");
}

int test_pipeline_parallel_blocking() {
	constexpr auto fn = "test_pipeline_parallel_blocking";
	int result = 0;
	Pipeline pipeline;
	for (int i = 0; i < 4; ++i) {
		pipeline.Add(MakePipe([](ReadOnly& in, WriteOnly& out,
				const StormByte::Safe::Shared<StormByte::Logger::Log>&) {
			while (!in.EoF()) {
				BinaryData data;
				if (in.Read(0, data) && !data.empty()) {
					for (auto& b : data)
						b = static_cast<std::byte>(static_cast<std::uint8_t>(b) + 1);
					(void)out.Write(std::move(data));
				}
			}
			out.Close();
		}));
	}
	Producer input;
	BinaryData payload;
	BinaryData expected;
	for (int i = 0; i < 32; ++i) {
		payload.push_back(static_cast<std::byte>(i));
		expected.push_back(static_cast<std::byte>(i + 4));
	}
	(void)input.Write(payload);
	input.Close();
	Consumer out = pipeline.Process(input.Consumer(), logging, ExecutionMode::Parallel);
	ASSERT_FALSE(fn, out.IsWritable());
	return ExpectBytes(fn, result, out, expected);
}

int test_pipeline_sync_execution() {
	constexpr auto fn = "test_pipeline_sync_execution";
	int result = 0;
	Pipeline pipeline;
	pipeline.Add(MakePipe([](ReadOnly& in, WriteOnly& out,
			const StormByte::Safe::Shared<StormByte::Logger::Log>&) {
		UpperAll(in, out);
	}));
	pipeline.Add(MakePipe([](ReadOnly& in, WriteOnly& out,
			const StormByte::Safe::Shared<StormByte::Logger::Log>&) {
		while (!in.EoF()) {
			BinaryData data;
			if (in.Read(0, data) && !data.empty()) {
				std::string str = BytesToText(data);
				std::replace(str.begin(), str.end(), ' ', '-');
				(void)out.Write(str);
			}
		}
		out.Close();
	}));
	Producer input;
	(void)input.Write("sync mode test");
	input.Close();
	Consumer out = pipeline.Process(input.Consumer(), logging, ExecutionMode::Sync);
	ASSERT_FALSE(fn, out.IsWritable());
	return ExpectText(fn, result, out, "SYNC-MODE-TEST");
}

int test_pipeline_sync_vs_parallel_cpu_bound() {
	constexpr auto fn = "test_pipeline_sync_vs_parallel_cpu_bound";
	int result = 0;
	constexpr int kStages = 12;
	constexpr std::size_t kSize = 12 * 1024 * 1024;
	constexpr std::size_t kChunk = 2048;
	constexpr int kInnerWork = 48;
	auto cpu_fn = [](ReadOnly& in, WriteOnly& out,
			const StormByte::Safe::Shared<StormByte::Logger::Log>&) {
		while (!in.EoF()) {
			BinaryData data;
			if (in.Read(kChunk, data) && !data.empty()) {
				for (auto& b : data) {
					std::uint8_t v = static_cast<std::uint8_t>(b);
					for (int w = 0; w < kInnerWork; ++w) {
						v = static_cast<std::uint8_t>(v * 131u + 17u);
						v ^= static_cast<std::uint8_t>(w * 3);
						v = static_cast<std::uint8_t>((v << 1) | (v >> 7));
					}
					b = static_cast<std::byte>(v);
				}
				(void)out.Write(std::move(data));
			}
		}
		out.Close();
	};
	auto build_pipeline = [&] {
		Pipeline pipeline;
		for (int i = 0; i < kStages; ++i)
			pipeline.Add(MakePipe(cpu_fn));
		return pipeline;
	};
	BinaryData input_data(StormByte::ByteSize{kSize});
	for (std::size_t i = 0; i < kSize; ++i)
		input_data[i] = static_cast<std::byte>((i * 31u + 17u) & 0xFFu);
	BinaryData expected = input_data;
	for (int s = 0; s < kStages; ++s) {
		for (auto& b : expected) {
			std::uint8_t v = static_cast<std::uint8_t>(b);
			for (int w = 0; w < kInnerWork; ++w) {
				v = static_cast<std::uint8_t>(v * 131u + 17u);
				v ^= static_cast<std::uint8_t>(w * 3);
				v = static_cast<std::uint8_t>((v << 1) | (v >> 7));
			}
			b = static_cast<std::byte>(v);
		}
	}
	auto run_mode = [&](ExecutionMode mode) -> int {
		Pipeline pipe = build_pipeline();
		Producer input;
		(void)input.Write(input_data);
		input.Close();
		Consumer out = pipe.Process(input.Consumer(), logging, mode);
		return ExpectBytes(fn, result, out, expected);
	};
	if (run_mode(ExecutionMode::Sync) != 0)
		return result;
	if (run_mode(ExecutionMode::Parallel) != 0)
		return result;
	RETURN_TEST(fn, result);
}

// -------------------
// Stress
// -------------------

int test_pipeline_available_bytes_during_process() {
	constexpr auto fn = "test_pipeline_available_bytes_during_process";
	int result = 0;
	Pipeline pipeline;
	pipeline.Add(MakePipe([](ReadOnly& in, WriteOnly& out,
			const StormByte::Safe::Shared<StormByte::Logger::Log>&) {
		CopyAll(in, out);
	}));
	Producer input;
	(void)input.Write(std::string(1000, 'Z'));
	input.Close();
	Consumer out = pipeline.Process(input.Consumer(), logging, ExecutionMode::Async);
	return ExpectText(fn, result, out, std::string(1000, 'Z'));
}

int test_pipeline_byte_arithmetic() {
	constexpr auto fn = "test_pipeline_byte_arithmetic";
	int result = 0;
	Pipeline pipeline;
	auto map = [](auto op) {
		return MakePipe([op](ReadOnly& in, WriteOnly& out,
				const StormByte::Safe::Shared<StormByte::Logger::Log>&) {
			while (!in.EoF()) {
				BinaryData data;
				if (in.Read(0, data) && !data.empty())
					(void)out.Write(MapBytes(std::move(data), op));
			}
			out.Close();
		});
	};
	pipeline.Add(map([](std::byte b) {
		return static_cast<std::byte>(static_cast<int>(b) + 1);
	}));
	pipeline.Add(map([](std::byte b) {
		return static_cast<std::byte>(static_cast<int>(b) * 2);
	}));
	pipeline.Add(map([](std::byte b) {
		return static_cast<std::byte>(static_cast<int>(b) / 2);
	}));
	pipeline.Add(map([](std::byte b) {
		return static_cast<std::byte>(static_cast<int>(b) - 1);
	}));
	const BinaryData input_data {std::byte{1}, std::byte{2}, std::byte{3}, std::byte{4}, std::byte{5}};
	Producer input;
	(void)input.Write(input_data);
	input.Close();
	Consumer out = pipeline.Process(input.Consumer(), logging, kAsyncParallel);
	return ExpectBytes(fn, result, out, input_data);
}

int test_pipeline_identity_many_stages() {
	constexpr auto fn = "test_pipeline_identity_many_stages";
	int result = 0;
	Pipeline pipeline;
	for (int i = 0; i < 10; ++i) {
		pipeline.Add(MakePipe([](ReadOnly& in, WriteOnly& out,
				const StormByte::Safe::Shared<StormByte::Logger::Log>&) {
			CopyAll(in, out);
		}));
	}
	Producer input;
	const std::string msg = "identity-chain";
	(void)input.Write(msg);
	input.Close();
	Consumer out = pipeline.Process(input.Consumer(), logging, kAsyncParallel);
	return ExpectText(fn, result, out, msg);
}

int test_pipeline_large_concurrent_stress() {
	constexpr auto fn = "test_pipeline_large_concurrent_stress";
	int result = 0;
	Pipeline pipeline;
	auto xor55 = MakePipe([](ReadOnly& in, WriteOnly& out,
			const StormByte::Safe::Shared<StormByte::Logger::Log>&) {
		while (!in.EoF()) {
			BinaryData data;
			if (in.Read(0, data) && !data.empty()) {
				for (auto& b : data)
					b ^= std::byte{0x55};
				(void)out.Write(std::move(data));
			}
		}
		out.Close();
	});
	auto add17 = MakePipe([](ReadOnly& in, WriteOnly& out,
			const StormByte::Safe::Shared<StormByte::Logger::Log>&) {
		while (!in.EoF()) {
			BinaryData data;
			if (in.Read(0, data) && !data.empty()) {
				for (auto& b : data)
					b = static_cast<std::byte>(static_cast<std::uint8_t>(b) + 17);
				(void)out.Write(std::move(data));
			}
		}
		out.Close();
	});
	auto bnot = MakePipe([](ReadOnly& in, WriteOnly& out,
			const StormByte::Safe::Shared<StormByte::Logger::Log>&) {
		while (!in.EoF()) {
			BinaryData data;
			if (in.Read(0, data) && !data.empty()) {
				for (auto& b : data)
					b = ~b;
				(void)out.Write(std::move(data));
			}
		}
		out.Close();
	});
	auto xorAA = MakePipe([](ReadOnly& in, WriteOnly& out,
			const StormByte::Safe::Shared<StormByte::Logger::Log>&) {
		while (!in.EoF()) {
			BinaryData data;
			if (in.Read(0, data) && !data.empty()) {
				for (auto& b : data)
					b ^= std::byte{0xAA};
				(void)out.Write(std::move(data));
			}
		}
		out.Close();
	});
	auto mul3 = MakePipe([](ReadOnly& in, WriteOnly& out,
			const StormByte::Safe::Shared<StormByte::Logger::Log>&) {
		while (!in.EoF()) {
			BinaryData data;
			if (in.Read(0, data) && !data.empty()) {
				for (auto& b : data)
					b = static_cast<std::byte>(static_cast<std::uint8_t>(b) * 3);
				(void)out.Write(std::move(data));
			}
		}
		out.Close();
	});
	auto rotl3 = MakePipe([](ReadOnly& in, WriteOnly& out,
			const StormByte::Safe::Shared<StormByte::Logger::Log>&) {
		while (!in.EoF()) {
			BinaryData data;
			if (in.Read(0, data) && !data.empty()) {
				for (auto& b : data) {
					const auto v = static_cast<std::uint8_t>(b);
					b = static_cast<std::byte>(static_cast<std::uint8_t>((v << 3) | (v >> 5)));
				}
				(void)out.Write(std::move(data));
			}
		}
		out.Close();
	});
	auto sub42 = MakePipe([](ReadOnly& in, WriteOnly& out,
			const StormByte::Safe::Shared<StormByte::Logger::Log>&) {
		while (!in.EoF()) {
			BinaryData data;
			if (in.Read(0, data) && !data.empty()) {
				for (auto& b : data)
					b = static_cast<std::byte>(static_cast<std::uint8_t>(b) - 42);
				(void)out.Write(std::move(data));
			}
		}
		out.Close();
	});
	auto xor33 = MakePipe([](ReadOnly& in, WriteOnly& out,
			const StormByte::Safe::Shared<StormByte::Logger::Log>&) {
		while (!in.EoF()) {
			BinaryData data;
			if (in.Read(0, data) && !data.empty()) {
				for (auto& b : data)
					b ^= std::byte{0x33};
				(void)out.Write(std::move(data));
			}
		}
		out.Close();
	});
	auto mul171 = MakePipe([](ReadOnly& in, WriteOnly& out,
			const StormByte::Safe::Shared<StormByte::Logger::Log>&) {
		while (!in.EoF()) {
			BinaryData data;
			if (in.Read(0, data) && !data.empty()) {
				for (auto& b : data)
					b = static_cast<std::byte>(static_cast<std::uint8_t>(b) * 171);
				(void)out.Write(std::move(data));
			}
		}
		out.Close();
	});
	auto rotr3 = MakePipe([](ReadOnly& in, WriteOnly& out,
			const StormByte::Safe::Shared<StormByte::Logger::Log>&) {
		while (!in.EoF()) {
			BinaryData data;
			if (in.Read(0, data) && !data.empty()) {
				for (auto& b : data) {
					const auto v = static_cast<std::uint8_t>(b);
					b = static_cast<std::byte>(static_cast<std::uint8_t>((v >> 3) | (v << 5)));
				}
				(void)out.Write(std::move(data));
			}
		}
		out.Close();
	});
	auto add42 = MakePipe([](ReadOnly& in, WriteOnly& out,
			const StormByte::Safe::Shared<StormByte::Logger::Log>&) {
		while (!in.EoF()) {
			BinaryData data;
			if (in.Read(0, data) && !data.empty()) {
				for (auto& b : data)
					b = static_cast<std::byte>(static_cast<std::uint8_t>(b) + 42);
				(void)out.Write(std::move(data));
			}
		}
		out.Close();
	});
	auto sub17 = MakePipe([](ReadOnly& in, WriteOnly& out,
			const StormByte::Safe::Shared<StormByte::Logger::Log>&) {
		while (!in.EoF()) {
			BinaryData data;
			if (in.Read(0, data) && !data.empty()) {
				for (auto& b : data)
					b = static_cast<std::byte>(static_cast<std::uint8_t>(b) - 17);
				(void)out.Write(std::move(data));
			}
		}
		out.Close();
	});
	pipeline.Add(xor55);
	pipeline.Add(add17);
	pipeline.Add(bnot);
	pipeline.Add(xorAA);
	pipeline.Add(mul3);
	pipeline.Add(rotl3);
	pipeline.Add(sub42);
	pipeline.Add(xor33);
	pipeline.Add(xor33);
	pipeline.Add(add42);
	pipeline.Add(rotr3);
	pipeline.Add(mul171);
	pipeline.Add(xorAA);
	pipeline.Add(bnot);
	pipeline.Add(sub17);
	pipeline.Add(xor55);
	const std::size_t data_size = LARGE_TEST_SIZE_KB * 1024;
	BinaryData input_data;
	input_data.reserve(StormByte::ByteSize{data_size});
	for (std::size_t i = 0; i < data_size; ++i)
		input_data.push_back(static_cast<std::byte>((i * 31 + 17) % 256));
	Producer input;
	std::thread writer([&input, &input_data] {
		const std::size_t chunk_size = 4096;
		std::size_t offset = 0;
		while (offset < static_cast<std::size_t>(input_data.size())) {
			const std::size_t to_write = std::min(chunk_size,
				static_cast<std::size_t>(input_data.size()) - offset);
			BinaryData chunk(input_data.data() + offset, StormByte::ByteSize{to_write});
			(void)input.Write(std::move(chunk));
			offset += to_write;
			std::this_thread::yield();
		}
		input.Close();
	});
	Consumer out = pipeline.Process(input.Consumer(), logging, kAsyncParallel);
	writer.join();
	return ExpectBytes(fn, result, out, input_data);
}

int test_pipeline_large_data() {
	constexpr auto fn = "test_pipeline_large_data";
	int result = 0;
	Pipeline pipeline;
	pipeline.Add(MakePipe([](ReadOnly& in, WriteOnly& out,
			const StormByte::Safe::Shared<StormByte::Logger::Log>&) {
		std::size_t count = 0;
		while (!in.EoF()) {
			BinaryData data;
			if (in.Read(0, data) && !data.empty())
				count += static_cast<std::size_t>(data.size());
		}
		(void)out.Write(std::to_string(count));
		out.Close();
	}));
	Producer input;
	(void)input.Write(std::string(10000, 'A'));
	input.Close();
	Consumer out = pipeline.Process(input.Consumer(), logging, ExecutionMode::Async);
	return ExpectText(fn, result, out, "10000");
}

int test_pipeline_reverse_string() {
	constexpr auto fn = "test_pipeline_reverse_string";
	int result = 0;
	Pipeline pipeline;
	pipeline.Add(MakePipe([](ReadOnly& in, WriteOnly& out,
			const StormByte::Safe::Shared<StormByte::Logger::Log>&) {
		std::string buffer;
		while (!in.EoF()) {
			BinaryData data;
			if (in.Read(0, data) && !data.empty())
				buffer += BytesToText(data);
		}
		std::reverse(buffer.begin(), buffer.end());
		(void)out.Write(buffer);
		out.Close();
	}));
	Producer input;
	(void)input.Write("ABCDEF");
	input.Close();
	Consumer out = pipeline.Process(input.Consumer(), logging, ExecutionMode::Async);
	return ExpectText(fn, result, out, "FEDCBA");
}

int test_pipeline_streaming_data() {
	constexpr auto fn = "test_pipeline_streaming_data";
	int result = 0;
	Pipeline pipeline;
	pipeline.Add(MakePipe([](ReadOnly& in, WriteOnly& out,
			const StormByte::Safe::Shared<StormByte::Logger::Log>&) {
		while (!in.EoF()) {
			BinaryData data;
			if (in.Read(0, data) && !data.empty())
				(void)out.Write(data);
			std::this_thread::sleep_for(std::chrono::milliseconds(10));
		}
		out.Close();
	}));
	Producer input;
	std::thread writer([&input] {
		(void)input.Write("Part1");
		std::this_thread::sleep_for(std::chrono::milliseconds(20));
		(void)input.Write("Part2");
		std::this_thread::sleep_for(std::chrono::milliseconds(20));
		(void)input.Write("Part3");
		input.Close();
	});
	Consumer out = pipeline.Process(input.Consumer(), logging, ExecutionMode::Async);
	writer.join();
	return ExpectText(fn, result, out, "Part1Part2Part3");
}

int test_pipeline_word_count() {
	constexpr auto fn = "test_pipeline_word_count";
	int result = 0;
	Pipeline pipeline;
	pipeline.Add(MakePipe([](ReadOnly& in, WriteOnly& out,
			const StormByte::Safe::Shared<StormByte::Logger::Log>&) {
		std::string buffer;
		while (!in.EoF()) {
			BinaryData data;
			if (in.Read(0, data) && !data.empty())
				buffer += BytesToText(data);
		}
		std::size_t word_count = 0;
		bool in_word = false;
		for (char c : buffer) {
			if (std::isspace(static_cast<unsigned char>(c)))
				in_word = false;
			else if (!in_word) {
				in_word = true;
				++word_count;
			}
		}
		(void)out.Write(std::to_string(word_count));
		out.Close();
	}));
	Producer input;
	(void)input.Write("Hello world this is a test");
	input.Close();
	Consumer out = pipeline.Process(input.Consumer(), logging, ExecutionMode::Async);
	return ExpectText(fn, result, out, "6");
}

int main() {
	int result = 0;

	// -------------------
	// Basic
	// -------------------
	result += test_pipeline_empty();
	result += test_pipeline_empty_input();
	result += test_pipeline_filter_stage();
	result += test_pipeline_incremental_processing();
	result += test_pipeline_multiple_writes();
	result += test_pipeline_single_stage();
	result += test_pipeline_three_stages();
	result += test_pipeline_two_stages();

	// -------------------
	// Construct
	// -------------------
	result += test_pipeline_add_move();
	result += test_pipeline_copy_constructor();
	result += test_pipeline_move_constructor();
	result += test_pipeline_null_logger();
	result += test_pipeline_reuse();
	result += test_pipeline_stage_must_close();

	// -------------------
	// Execution
	// -------------------
	result += test_pipeline_async_reuse_many_times();
	result += test_pipeline_async_seterror_interrupts_quickly();
	result += test_pipeline_interrupted_by_seterror();
	result += test_pipeline_large_async_many_stages();
	result += test_pipeline_parallel_async_correctness();
	result += test_pipeline_parallel_blocking();
	result += test_pipeline_sync_execution();
	result += test_pipeline_sync_vs_parallel_cpu_bound();

	// -------------------
	// Stress
	// -------------------
	result += test_pipeline_available_bytes_during_process();
	result += test_pipeline_byte_arithmetic();
	result += test_pipeline_identity_many_stages();
	result += test_pipeline_large_concurrent_stress();
	result += test_pipeline_large_data();
	result += test_pipeline_reverse_string();
	result += test_pipeline_streaming_data();
	result += test_pipeline_word_count();

	if (result == 0)
		std::cout << "All tests passed!" << std::endl;
	else
		std::cout << result << " tests failed." << std::endl;
	return result;
}
