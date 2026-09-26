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

#include <StormByte/buffer/external.hxx>
#include <StormByte/buffer/pipeline.hxx>
#include <StormByte/logger/log.hxx>
#include <StormByte/test_handlers.h>

#include <algorithm>
#include <cctype>
#include <chrono>
#include <cstdint>
#include <iostream>
#include <memory>
#include <sstream>
#include <string>
#include <thread>
#include <vector>

using StormByte::Buffer::Consumer;
using StormByte::BinaryData;
using StormByte::Buffer::ExecutionMode;
using StormByte::Buffer::ExternalReader;
using StormByte::Buffer::ExternalWriter;
using StormByte::Buffer::Pipeline;
using StormByte::Buffer::Producer;

#define LARGE_TEST_SIZE_KB 1024
#define USE_READ
#ifdef USE_READ
	#define CONSUME(reader, count, buff) (reader).Read(count, buff)
#else
	#define CONSUME(reader, count, buff) (reader).Extract(count, buff)
#endif

namespace {
	constexpr ExecutionMode kAsyncParallel = ExecutionMode::Async | ExecutionMode::Parallel;

	std::ostringstream logging_stream;
	std::shared_ptr<StormByte::Logger::Log> logging =
		std::make_shared<StormByte::Logger::Log>(logging_stream, StormByte::Logger::Level::Info);

	std::string BytesToText(const BinaryData& data) {
		if (data.empty())
			return {};
		return std::string(reinterpret_cast<const char*>(data.data()),
			static_cast<std::size_t>(data.size()));
	}

	void WaitForPipelineCompletion(Consumer& consumer) {
		while (consumer.IsWritable())
			std::this_thread::yield();
	}
}

// -------------------
// Basic
// -------------------

int test_pipeline_empty() {
	const std::string fn = "test_pipeline_empty";
	Pipeline pipeline;
	Producer input;
	(void)input.Write("TEST");
	input.Close();
	Consumer result = pipeline.Process(input.Consumer(), ExecutionMode::Async, logging);
	WaitForPipelineCompletion(result);
	BinaryData data;
	ASSERT_TRUE(fn, CONSUME(result, 0, data));
	ASSERT_EQUAL(fn, BytesToText(data), std::string("TEST"));
	RETURN_TEST(fn, 0);
}

int test_pipeline_empty_input() {
	const std::string fn = "test_pipeline_empty_input";
	Pipeline pipeline;
	pipeline.AddPipe([](ExternalReader& in, ExternalWriter& out,
		std::shared_ptr<StormByte::Logger::Log>) {
		while (!in.EoF()) {
			BinaryData data;
			if (CONSUME(in, 0, data) && !data.empty())
				(void)out.Write(data);
		}
		out.Close();
	});
	Producer input;
	input.Close();
	Consumer result = pipeline.Process(input.Consumer(), ExecutionMode::Async, logging);
	WaitForPipelineCompletion(result);
	BinaryData data;
	if (!CONSUME(result, 0, data))
		ASSERT_TRUE(fn, result.EoF());
	else
		ASSERT_EQUAL(fn, data.size(), StormByte::ByteSize{0});
	RETURN_TEST(fn, 0);
}

int test_pipeline_filter_stage() {
	const std::string fn = "test_pipeline_filter_stage";
	Pipeline pipeline;
	pipeline.AddPipe([](ExternalReader& in, ExternalWriter& out,
		std::shared_ptr<StormByte::Logger::Log>) {
		while (!in.EoF()) {
			BinaryData data;
			if (CONSUME(in, 0, data) && !data.empty()) {
				std::string str = BytesToText(data);
				std::string filtered;
				for (char c : str)
					if (std::isalpha(static_cast<unsigned char>(c)))
						filtered += c;
				if (!filtered.empty())
					(void)out.Write(filtered);
			}
		}
		out.Close();
	});
	Producer input;
	(void)input.Write("Hello123World456!");
	input.Close();
	Consumer result = pipeline.Process(input.Consumer(), ExecutionMode::Async, logging);
	WaitForPipelineCompletion(result);
	BinaryData data;
	ASSERT_TRUE(fn, CONSUME(result, 0, data));
	ASSERT_EQUAL(fn, BytesToText(data), std::string("HelloWorld"));
	RETURN_TEST(fn, 0);
}

int test_pipeline_incremental_processing() {
	const std::string fn = "test_pipeline_incremental_processing";
	Pipeline pipeline;
	pipeline.AddPipe([](ExternalReader& in, ExternalWriter& out,
		std::shared_ptr<StormByte::Logger::Log>) {
		while (!in.EoF()) {
			BinaryData data;
			if (CONSUME(in, 1, data) && !data.empty()) {
				char c = static_cast<char>(std::toupper(static_cast<unsigned char>(data[0])));
				(void)out.Write(std::string(1, c));
			}
		}
		out.Close();
	});
	Producer input;
	(void)input.Write("abc");
	input.Close();
	Consumer result = pipeline.Process(input.Consumer(), ExecutionMode::Async, logging);
	WaitForPipelineCompletion(result);
	BinaryData data;
	ASSERT_TRUE(fn, CONSUME(result, 0, data));
	ASSERT_EQUAL(fn, BytesToText(data), std::string("ABC"));
	RETURN_TEST(fn, 0);
}

int test_pipeline_multiple_writes() {
	const std::string fn = "test_pipeline_multiple_writes";
	Pipeline pipeline;
	pipeline.AddPipe([](ExternalReader& in, ExternalWriter& out,
		std::shared_ptr<StormByte::Logger::Log>) {
		while (!in.EoF()) {
			BinaryData data;
			if (CONSUME(in, 0, data) && !data.empty()) {
				(void)out.Write(data);
				(void)out.Write(data);
			}
		}
		out.Close();
	});
	Producer input;
	(void)input.Write("AB");
	input.Close();
	Consumer result = pipeline.Process(input.Consumer(), ExecutionMode::Async, logging);
	WaitForPipelineCompletion(result);
	BinaryData data;
	ASSERT_TRUE(fn, CONSUME(result, 0, data));
	ASSERT_EQUAL(fn, BytesToText(data), std::string("ABAB"));
	RETURN_TEST(fn, 0);
}

int test_pipeline_single_stage() {
	const std::string fn = "test_pipeline_single_stage";
	Pipeline pipeline;
	pipeline.AddPipe([](ExternalReader& in, ExternalWriter& out,
		std::shared_ptr<StormByte::Logger::Log>) {
		while (!in.EoF()) {
			BinaryData data;
			if (CONSUME(in, 0, data) && !data.empty()) {
				std::string str = BytesToText(data);
				for (auto& c : str)
					c = static_cast<char>(std::toupper(static_cast<unsigned char>(c)));
				(void)out.Write(str);
			}
		}
		out.Close();
	});
	Producer input;
	(void)input.Write("hello world");
	input.Close();
	Consumer result = pipeline.Process(input.Consumer(), ExecutionMode::Async, logging);
	WaitForPipelineCompletion(result);
	BinaryData data;
	ASSERT_TRUE(fn, CONSUME(result, 0, data));
	ASSERT_EQUAL(fn, BytesToText(data), std::string("HELLO WORLD"));
	RETURN_TEST(fn, 0);
}

int test_pipeline_three_stages() {
	const std::string fn = "test_pipeline_three_stages";
	Pipeline pipeline;
	pipeline.AddPipe([](ExternalReader& in, ExternalWriter& out,
		std::shared_ptr<StormByte::Logger::Log>) {
		while (!in.EoF()) {
			BinaryData data;
			if (CONSUME(in, 0, data) && !data.empty()) {
				std::string str = BytesToText(data);
				for (auto& c : str)
					c = static_cast<char>(std::toupper(static_cast<unsigned char>(c)));
				(void)out.Write(str);
			}
		}
		out.Close();
	});
	pipeline.AddPipe([](ExternalReader& in, ExternalWriter& out,
		std::shared_ptr<StormByte::Logger::Log>) {
		while (!in.EoF()) {
			BinaryData data;
			if (CONSUME(in, 0, data) && !data.empty()) {
				std::string str = BytesToText(data);
				std::replace(str.begin(), str.end(), ' ', '-');
				(void)out.Write(str);
			}
		}
		out.Close();
	});
	pipeline.AddPipe([](ExternalReader& in, ExternalWriter& out,
		std::shared_ptr<StormByte::Logger::Log>) {
		(void)out.Write("[");
		while (!in.EoF()) {
			BinaryData data;
			if (CONSUME(in, 0, data) && !data.empty())
				(void)out.Write(BytesToText(data));
		}
		(void)out.Write("]");
		out.Close();
	});
	Producer input;
	(void)input.Write("test data");
	input.Close();
	Consumer result = pipeline.Process(input.Consumer(), kAsyncParallel, logging);
	WaitForPipelineCompletion(result);
	BinaryData data;
	ASSERT_TRUE(fn, CONSUME(result, 0, data));
	ASSERT_EQUAL(fn, BytesToText(data), std::string("[TEST-DATA]"));
	RETURN_TEST(fn, 0);
}

int test_pipeline_two_stages() {
	const std::string fn = "test_pipeline_two_stages";
	Pipeline pipeline;
	pipeline.AddPipe([](ExternalReader& in, ExternalWriter& out,
		std::shared_ptr<StormByte::Logger::Log>) {
		while (!in.EoF()) {
			BinaryData data;
			if (CONSUME(in, 0, data) && !data.empty()) {
				std::string str = BytesToText(data);
				for (auto& c : str)
					c = static_cast<char>(std::toupper(static_cast<unsigned char>(c)));
				(void)out.Write(str);
			}
		}
		out.Close();
	});
	pipeline.AddPipe([](ExternalReader& in, ExternalWriter& out,
		std::shared_ptr<StormByte::Logger::Log>) {
		while (!in.EoF()) {
			BinaryData data;
			if (CONSUME(in, 0, data) && !data.empty()) {
				std::string str = BytesToText(data);
				std::replace(str.begin(), str.end(), ' ', '_');
				(void)out.Write(str);
			}
		}
		out.Close();
	});
	Producer input;
	(void)input.Write("hello world test");
	input.Close();
	Consumer result = pipeline.Process(input.Consumer(), kAsyncParallel, logging);
	WaitForPipelineCompletion(result);
	BinaryData data;
	ASSERT_TRUE(fn, CONSUME(result, 0, data));
	ASSERT_EQUAL(fn, BytesToText(data), std::string("HELLO_WORLD_TEST"));
	RETURN_TEST(fn, 0);
}

// -------------------
// Construct
// -------------------

int test_pipeline_addpipe_move() {
	const std::string fn = "test_pipeline_addpipe_move";
	Pipeline pipeline;
	Pipeline::PipeFunction func =
		[](ExternalReader& in, ExternalWriter& out,
			std::shared_ptr<StormByte::Logger::Log>) {
			while (!in.EoF()) {
				BinaryData data;
				if (CONSUME(in, 0, data) && !data.empty())
					(void)out.Write(data);
			}
			out.Close();
		};
	pipeline.AddPipe(std::move(func));
	Producer input;
	(void)input.Write("MOVE");
	input.Close();
	Consumer result = pipeline.Process(input.Consumer(), ExecutionMode::Async, logging);
	WaitForPipelineCompletion(result);
	BinaryData data;
	ASSERT_TRUE(fn, CONSUME(result, 0, data));
	ASSERT_EQUAL(fn, BytesToText(data), std::string("MOVE"));
	RETURN_TEST(fn, 0);
}

int test_pipeline_copy_constructor() {
	const std::string fn = "test_pipeline_copy_constructor";
	Pipeline pipeline1;
	pipeline1.AddPipe([](ExternalReader& in, ExternalWriter& out,
		std::shared_ptr<StormByte::Logger::Log>) {
		while (!in.EoF()) {
			BinaryData data;
			if (CONSUME(in, 0, data) && !data.empty()) {
				std::string str = BytesToText(data);
				for (auto& c : str)
					c = static_cast<char>(std::toupper(static_cast<unsigned char>(c)));
				(void)out.Write(str);
			}
		}
		out.Close();
	});
	Pipeline pipeline2 = pipeline1;
	Producer input;
	(void)input.Write("test");
	input.Close();
	Consumer result = pipeline2.Process(input.Consumer(), ExecutionMode::Async, logging);
	WaitForPipelineCompletion(result);
	BinaryData data;
	ASSERT_TRUE(fn, CONSUME(result, 0, data));
	ASSERT_EQUAL(fn, BytesToText(data), std::string("TEST"));
	RETURN_TEST(fn, 0);
}

int test_pipeline_move_constructor() {
	const std::string fn = "test_pipeline_move_constructor";
	Pipeline pipeline1;
	pipeline1.AddPipe([](ExternalReader& in, ExternalWriter& out,
		std::shared_ptr<StormByte::Logger::Log>) {
		while (!in.EoF()) {
			BinaryData data;
			if (CONSUME(in, 0, data) && !data.empty()) {
				std::string str = BytesToText(data);
				for (auto& c : str)
					c = static_cast<char>(std::tolower(static_cast<unsigned char>(c)));
				(void)out.Write(str);
			}
		}
		out.Close();
	});
	Pipeline pipeline2 = std::move(pipeline1);
	Producer input;
	(void)input.Write("TEST");
	input.Close();
	Consumer result = pipeline2.Process(input.Consumer(), ExecutionMode::Async, logging);
	WaitForPipelineCompletion(result);
	BinaryData data;
	ASSERT_TRUE(fn, CONSUME(result, 0, data));
	ASSERT_EQUAL(fn, BytesToText(data), std::string("test"));
	RETURN_TEST(fn, 0);
}

int test_pipeline_null_logger() {
	const std::string fn = "test_pipeline_null_logger";
	Pipeline pipeline;
	pipeline.AddPipe([](ExternalReader& in, ExternalWriter& out,
		std::shared_ptr<StormByte::Logger::Log>) {
		while (!in.EoF()) {
			BinaryData d;
			if (CONSUME(in, 0, d) && !d.empty())
				(void)out.Write(d);
		}
		out.Close();
	});
	Producer input;
	(void)input.Write("null-log");
	input.Close();
	Consumer result = pipeline.Process(input.Consumer(), ExecutionMode::Sync, nullptr);
	BinaryData data;
	ASSERT_TRUE(fn, CONSUME(result, 0, data));
	ASSERT_EQUAL(fn, BytesToText(data), std::string("null-log"));
	RETURN_TEST(fn, 0);
}

int test_pipeline_reuse() {
	const std::string fn = "test_pipeline_reuse";
	Pipeline pipeline;
	pipeline.AddPipe([](ExternalReader& in, ExternalWriter& out,
		std::shared_ptr<StormByte::Logger::Log>) {
		(void)out.Write(">");
		while (!in.EoF()) {
			BinaryData data;
			if (CONSUME(in, 0, data) && !data.empty())
				(void)out.Write(data);
		}
		out.Close();
	});
	{
		Producer input1;
		(void)input1.Write("TEST1");
		input1.Close();
		Consumer result1 = pipeline.Process(input1.Consumer(), ExecutionMode::Async, logging);
		WaitForPipelineCompletion(result1);
		BinaryData data1;
		ASSERT_TRUE(fn, CONSUME(result1, 0, data1));
		ASSERT_EQUAL(fn, BytesToText(data1), std::string(">TEST1"));
	}
	{
		Producer input2;
		(void)input2.Write("TEST2");
		input2.Close();
		Consumer result2 = pipeline.Process(input2.Consumer(), ExecutionMode::Async, logging);
		WaitForPipelineCompletion(result2);
		BinaryData data2;
		ASSERT_TRUE(fn, CONSUME(result2, 0, data2));
		ASSERT_EQUAL(fn, BytesToText(data2), std::string(">TEST2"));
	}
	RETURN_TEST(fn, 0);
}

int test_pipeline_stage_must_close() {
	const std::string fn = "test_pipeline_stage_must_close";
	Pipeline pipeline;
	pipeline.AddPipe([](ExternalReader& in, ExternalWriter& out,
		std::shared_ptr<StormByte::Logger::Log>) {
		BinaryData d;
		while (!in.EoF()) {
			if (CONSUME(in, 0, d) && !d.empty())
				(void)out.Write(d);
		}
		out.Close();
	});
	Producer input;
	(void)input.Write("close-me");
	input.Close();
	Consumer result = pipeline.Process(input.Consumer(), ExecutionMode::Sync, logging);
	ASSERT_TRUE(fn, result.EoF() || !result.IsWritable());
	BinaryData data;
	ASSERT_TRUE(fn, CONSUME(result, 0, data));
	ASSERT_EQUAL(fn, BytesToText(data), std::string("close-me"));
	RETURN_TEST(fn, 0);
}

// -------------------
// Execution
// -------------------

int test_pipeline_async_reuse_many_times() {
	const std::string fn = "test_pipeline_async_reuse_many_times";
	Pipeline pipeline;
	pipeline.AddPipe([](ExternalReader& in, ExternalWriter& out,
		std::shared_ptr<StormByte::Logger::Log>) {
		while (!in.EoF()) {
			BinaryData d;
			if (CONSUME(in, 0, d) && !d.empty())
				(void)out.Write(d);
		}
		out.Close();
	});
	for (int i = 0; i < 50; ++i) {
		Producer input;
		const std::string msg = "RUN-" + std::to_string(i);
		(void)input.Write(msg);
		input.Close();
		Consumer result = pipeline.Process(input.Consumer(), ExecutionMode::Async, logging);
		WaitForPipelineCompletion(result);
		BinaryData data;
		ASSERT_TRUE(fn, CONSUME(result, 0, data));
		ASSERT_EQUAL(fn, BytesToText(data), msg);
	}
	RETURN_TEST(fn, 0);
}

int test_pipeline_async_seterror_interrupts_quickly() {
	const std::string fn = "test_pipeline_async_seterror_interrupts_quickly";
	Pipeline pipeline;
	for (int i = 0; i < 12; ++i) {
		pipeline.AddPipe([](ExternalReader& in, ExternalWriter& out,
			std::shared_ptr<StormByte::Logger::Log>) {
			while (!in.EoF()) {
				if (!out.IsWritable())
					return;
				BinaryData d;
				if (CONSUME(in, 0, d) && !d.empty()) {
					std::this_thread::sleep_for(std::chrono::milliseconds(2));
					if (!out.IsWritable())
						return;
					(void)out.Write(d);
				}
			}
			if (out.IsWritable())
				out.Close();
		});
	}
	Producer input;
	(void)input.Write(std::string(100000, 'X'));
	input.Close();
	Consumer result = pipeline.Process(input.Consumer(), kAsyncParallel, logging);
	std::this_thread::sleep_for(std::chrono::milliseconds(5));
	pipeline.SetError();
	WaitForPipelineCompletion(result);
	ASSERT_FALSE(fn, result.IsWritable());
	ASSERT_TRUE(fn, result.EoF());
	RETURN_TEST(fn, 0);
}

int test_pipeline_interrupted_by_seterror() {
	const std::string fn = "test_pipeline_interrupted_by_seterror";
	Pipeline pipeline;
	for (int i = 0; i < 8; ++i) {
		pipeline.AddPipe([](ExternalReader& in, ExternalWriter& out,
			std::shared_ptr<StormByte::Logger::Log>) {
			while (!in.EoF()) {
				BinaryData data;
				if (CONSUME(in, 0, data) && !data.empty()) {
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
		});
	}
	Producer input;
	(void)input.Write(std::string(50000, 'X'));
	input.Close();
	Consumer result = pipeline.Process(input.Consumer(), kAsyncParallel, logging);
	pipeline.SetError();
	WaitForPipelineCompletion(result);
	ASSERT_FALSE(fn, result.IsWritable());
	ASSERT_TRUE(fn, result.EoF());
	ASSERT_EQUAL(fn, result.Available(), StormByte::ByteSize{0});
	RETURN_TEST(fn, 0);
}

int test_pipeline_large_async_many_stages() {
	const std::string fn = "test_pipeline_large_async_many_stages";
	Pipeline pipeline;
	for (int i = 0; i < 25; ++i) {
		pipeline.AddPipe([](ExternalReader& in, ExternalWriter& out,
			std::shared_ptr<StormByte::Logger::Log>) {
			while (!in.EoF()) {
				BinaryData data;
				if (CONSUME(in, 0, data) && !data.empty()) {
					std::string s = BytesToText(data);
					for (char& c : s)
						c = static_cast<char>(std::toupper(static_cast<unsigned char>(c)));
					(void)out.Write(s);
				}
			}
			out.Close();
		});
	}
	Producer input;
	(void)input.Write(std::string(8192, 'a'));
	input.Close();
	Consumer result = pipeline.Process(input.Consumer(), kAsyncParallel, logging);
	WaitForPipelineCompletion(result);
	BinaryData data;
	ASSERT_TRUE(fn, CONSUME(result, 0, data));
	ASSERT_EQUAL(fn, BytesToText(data), std::string(8192, 'A'));
	RETURN_TEST(fn, 0);
}

int test_pipeline_parallel_async_correctness() {
	const std::string fn = "test_pipeline_parallel_async_correctness";
	Pipeline pipeline;
	for (int i = 0; i < 6; ++i) {
		pipeline.AddPipe([](ExternalReader& in, ExternalWriter& out,
			std::shared_ptr<StormByte::Logger::Log>) {
			while (!in.EoF()) {
				BinaryData d;
				if (CONSUME(in, 0, d) && !d.empty()) {
					std::string s = BytesToText(d);
					for (char& c : s)
						c = static_cast<char>(std::toupper(static_cast<unsigned char>(c)));
					(void)out.Write(s);
				}
			}
			out.Close();
		});
	}
	Producer input;
	(void)input.Write("parallel-async-ok");
	input.Close();
	Consumer result = pipeline.Process(input.Consumer(), kAsyncParallel, logging);
	WaitForPipelineCompletion(result);
	BinaryData data;
	ASSERT_TRUE(fn, CONSUME(result, 0, data));
	ASSERT_EQUAL(fn, BytesToText(data), std::string("PARALLEL-ASYNC-OK"));
	RETURN_TEST(fn, 0);
}

int test_pipeline_parallel_blocking() {
	const std::string fn = "test_pipeline_parallel_blocking";
	Pipeline pipeline;
	for (int i = 0; i < 4; ++i) {
		pipeline.AddPipe([](ExternalReader& in, ExternalWriter& out,
			std::shared_ptr<StormByte::Logger::Log>) {
			while (!in.EoF()) {
				BinaryData d;
				if (CONSUME(in, 0, d) && !d.empty()) {
					for (auto& b : d)
						b = static_cast<std::byte>(static_cast<std::uint8_t>(b) + 1);
					(void)out.Write(std::move(d));
				}
			}
			out.Close();
		});
	}
	Producer input;
	BinaryData payload;
	for (int i = 0; i < 32; ++i)
		payload.push_back(static_cast<std::byte>(i));
	(void)input.Write(payload);
	input.Close();
	Consumer result = pipeline.Process(input.Consumer(), ExecutionMode::Parallel, logging);
	ASSERT_FALSE(fn, result.IsWritable());
	BinaryData data;
	ASSERT_TRUE(fn, CONSUME(result, 0, data));
	ASSERT_EQUAL(fn, data.size(), StormByte::ByteSize{32});
	for (int i = 0; i < 32; ++i)
		ASSERT_EQUAL(fn, static_cast<int>(data[static_cast<std::size_t>(i)]), i + 4);
	RETURN_TEST(fn, 0);
}

int test_pipeline_sync_execution() {
	const std::string fn = "test_pipeline_sync_execution";
	Pipeline pipeline;
	pipeline.AddPipe([](ExternalReader& in, ExternalWriter& out,
		std::shared_ptr<StormByte::Logger::Log>) {
		while (!in.EoF()) {
			BinaryData data;
			if (CONSUME(in, 0, data) && !data.empty()) {
				std::string str = BytesToText(data);
				for (auto& c : str)
					c = static_cast<char>(std::toupper(static_cast<unsigned char>(c)));
				(void)out.Write(str);
			}
		}
		out.Close();
	});
	pipeline.AddPipe([](ExternalReader& in, ExternalWriter& out,
		std::shared_ptr<StormByte::Logger::Log>) {
		while (!in.EoF()) {
			BinaryData data;
			if (CONSUME(in, 0, data) && !data.empty()) {
				std::string str = BytesToText(data);
				std::replace(str.begin(), str.end(), ' ', '-');
				(void)out.Write(str);
			}
		}
		out.Close();
	});
	Producer input;
	(void)input.Write("sync mode test");
	input.Close();
	Consumer result = pipeline.Process(input.Consumer(), ExecutionMode::Sync, logging);
	ASSERT_FALSE(fn, result.IsWritable());
	BinaryData data;
	ASSERT_TRUE(fn, CONSUME(result, 0, data));
	ASSERT_EQUAL(fn, BytesToText(data), std::string("SYNC-MODE-TEST"));
	RETURN_TEST(fn, 0);
}

int test_pipeline_sync_vs_parallel_cpu_bound() {
	const std::string fn = "test_pipeline_sync_vs_parallel_cpu_bound";
	constexpr int kStages = 12;
	constexpr std::size_t kSize = 12 * 1024 * 1024;
	constexpr std::size_t kChunk = 2048;
	constexpr int kInnerWork = 48;
	auto cpu_stage = [](ExternalReader& in, ExternalWriter& out,
		std::shared_ptr<StormByte::Logger::Log>) {
		while (!in.EoF()) {
			BinaryData data;
			if (CONSUME(in, kChunk, data) && !data.empty()) {
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
			pipeline.AddPipe(cpu_stage);
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
	auto run_mode = [&](ExecutionMode mode, long long& total_ms) -> int {
		Pipeline pipe = build_pipeline();
		Producer input;
		(void)input.Write(input_data);
		input.Close();
		const auto t0 = std::chrono::steady_clock::now();
		Consumer result = pipe.Process(input.Consumer(), mode, logging);
		const auto t1 = std::chrono::steady_clock::now();
		total_ms = std::chrono::duration_cast<std::chrono::milliseconds>(t1 - t0).count();
		ASSERT_FALSE(fn, result.IsWritable());
		BinaryData out;
		ASSERT_TRUE(fn, CONSUME(result, 0, out));
		ASSERT_EQUAL(fn, out.size(), StormByte::ByteSize{kSize});
		ASSERT_TRUE(fn, out == expected);
		return 0;
	};
	long long dummy = 0;
	if (run_mode(ExecutionMode::Sync, dummy) != 0)
		return 1;
	if (run_mode(ExecutionMode::Parallel, dummy) != 0)
		return 1;
	long long sync_ms = 0;
	long long parallel_ms = 0;
	if (run_mode(ExecutionMode::Sync, sync_ms) != 0)
		return 1;
	if (run_mode(ExecutionMode::Parallel, parallel_ms) != 0)
		return 1;
	ASSERT_TRUE(fn, parallel_ms < sync_ms * 85 / 100);
	RETURN_TEST(fn, 0);
}

// -------------------
// Stress
// -------------------

int test_pipeline_available_bytes_during_process() {
	const std::string fn = "test_pipeline_available_bytes_during_process";
	Pipeline pipeline;
	pipeline.AddPipe([](ExternalReader& in, ExternalWriter& out,
		std::shared_ptr<StormByte::Logger::Log>) {
		while (!in.EoF()) {
			BinaryData d;
			if (CONSUME(in, 0, d) && !d.empty())
				(void)out.Write(d);
		}
		out.Close();
	});
	Producer input;
	(void)input.Write(std::string(1000, 'Z'));
	input.Close();
	Consumer result = pipeline.Process(input.Consumer(), ExecutionMode::Async, logging);
	WaitForPipelineCompletion(result);
	ASSERT_TRUE(fn, result.Available() > 0 || result.EoF());
	BinaryData data;
	ASSERT_TRUE(fn, CONSUME(result, 0, data));
	ASSERT_EQUAL(fn, data.size(), StormByte::ByteSize{1000});
	RETURN_TEST(fn, 0);
}

int test_pipeline_byte_arithmetic() {
	const std::string fn = "test_pipeline_byte_arithmetic";
	Pipeline pipeline;
	auto map = [](auto op) {
		return [op](ExternalReader& in, ExternalWriter& out,
			std::shared_ptr<StormByte::Logger::Log>) {
			while (!in.EoF()) {
				BinaryData data;
				if (CONSUME(in, 0, data) && !data.empty()) {
					BinaryData mapped;
					mapped.reserve(data.size());
					for (const auto& byte : data)
						mapped.push_back(op(byte));
					(void)out.Write(mapped);
				}
			}
			out.Close();
		};
	};
	pipeline.AddPipe(map([](std::byte b) {
		return static_cast<std::byte>(static_cast<int>(b) + 1);
	}));
	pipeline.AddPipe(map([](std::byte b) {
		return static_cast<std::byte>(static_cast<int>(b) * 2);
	}));
	pipeline.AddPipe(map([](std::byte b) {
		return static_cast<std::byte>(static_cast<int>(b) / 2);
	}));
	pipeline.AddPipe(map([](std::byte b) {
		return static_cast<std::byte>(static_cast<int>(b) - 1);
	}));
	BinaryData input_data {std::byte{1}, std::byte{2}, std::byte{3}, std::byte{4}, std::byte{5}};
	Producer input;
	(void)input.Write(input_data);
	input.Close();
	Consumer result = pipeline.Process(input.Consumer(), kAsyncParallel, logging);
	WaitForPipelineCompletion(result);
	BinaryData data;
	ASSERT_TRUE(fn, CONSUME(result, 0, data));
	ASSERT_EQUAL(fn, data.size(), StormByte::ByteSize{5});
	ASSERT_EQUAL(fn, static_cast<int>(data[0]), 1);
	ASSERT_EQUAL(fn, static_cast<int>(data[1]), 2);
	ASSERT_EQUAL(fn, static_cast<int>(data[2]), 3);
	ASSERT_EQUAL(fn, static_cast<int>(data[3]), 4);
	ASSERT_EQUAL(fn, static_cast<int>(data[4]), 5);
	RETURN_TEST(fn, 0);
}

int test_pipeline_identity_many_stages() {
	const std::string fn = "test_pipeline_identity_many_stages";
	Pipeline pipeline;
	for (int i = 0; i < 10; ++i) {
		pipeline.AddPipe([](ExternalReader& in, ExternalWriter& out,
			std::shared_ptr<StormByte::Logger::Log>) {
			while (!in.EoF()) {
				BinaryData d;
				if (CONSUME(in, 0, d) && !d.empty())
					(void)out.Write(d);
			}
			out.Close();
		});
	}
	Producer input;
	const std::string msg = "identity-chain";
	(void)input.Write(msg);
	input.Close();
	Consumer result = pipeline.Process(input.Consumer(), kAsyncParallel, logging);
	WaitForPipelineCompletion(result);
	BinaryData data;
	ASSERT_TRUE(fn, CONSUME(result, 0, data));
	ASSERT_EQUAL(fn, BytesToText(data), msg);
	RETURN_TEST(fn, 0);
}

int test_pipeline_large_concurrent_stress() {
	const std::string fn = "test_pipeline_large_concurrent_stress";
	Pipeline pipeline;
	auto xor55 = [](ExternalReader& in, ExternalWriter& out,
		std::shared_ptr<StormByte::Logger::Log>) {
		while (!in.EoF()) {
			BinaryData data;
			if (CONSUME(in, 0, data) && !data.empty()) {
				for (auto& b : data)
					b ^= std::byte{0x55};
				(void)out.Write(std::move(data));
			}
		}
		out.Close();
	};
	auto add17 = [](ExternalReader& in, ExternalWriter& out,
		std::shared_ptr<StormByte::Logger::Log>) {
		while (!in.EoF()) {
			BinaryData data;
			if (CONSUME(in, 0, data) && !data.empty()) {
				for (auto& b : data)
					b = static_cast<std::byte>(static_cast<std::uint8_t>(b) + 17);
				(void)out.Write(std::move(data));
			}
		}
		out.Close();
	};
	auto bnot = [](ExternalReader& in, ExternalWriter& out,
		std::shared_ptr<StormByte::Logger::Log>) {
		while (!in.EoF()) {
			BinaryData data;
			if (CONSUME(in, 0, data) && !data.empty()) {
				for (auto& b : data)
					b = ~b;
				(void)out.Write(std::move(data));
			}
		}
		out.Close();
	};
	auto xorAA = [](ExternalReader& in, ExternalWriter& out,
		std::shared_ptr<StormByte::Logger::Log>) {
		while (!in.EoF()) {
			BinaryData data;
			if (CONSUME(in, 0, data) && !data.empty()) {
				for (auto& b : data)
					b ^= std::byte{0xAA};
				(void)out.Write(std::move(data));
			}
		}
		out.Close();
	};
	auto mul3 = [](ExternalReader& in, ExternalWriter& out,
		std::shared_ptr<StormByte::Logger::Log>) {
		while (!in.EoF()) {
			BinaryData data;
			if (CONSUME(in, 0, data) && !data.empty()) {
				for (auto& b : data)
					b = static_cast<std::byte>(static_cast<std::uint8_t>(b) * 3);
				(void)out.Write(std::move(data));
			}
		}
		out.Close();
	};
	auto rotl3 = [](ExternalReader& in, ExternalWriter& out,
		std::shared_ptr<StormByte::Logger::Log>) {
		while (!in.EoF()) {
			BinaryData data;
			if (CONSUME(in, 0, data) && !data.empty()) {
				for (auto& b : data) {
					const auto v = static_cast<std::uint8_t>(b);
					b = static_cast<std::byte>(static_cast<std::uint8_t>((v << 3) | (v >> 5)));
				}
				(void)out.Write(std::move(data));
			}
		}
		out.Close();
	};
	auto sub42 = [](ExternalReader& in, ExternalWriter& out,
		std::shared_ptr<StormByte::Logger::Log>) {
		while (!in.EoF()) {
			BinaryData data;
			if (CONSUME(in, 0, data) && !data.empty()) {
				for (auto& b : data)
					b = static_cast<std::byte>(static_cast<std::uint8_t>(b) - 42);
				(void)out.Write(std::move(data));
			}
		}
		out.Close();
	};
	auto xor33 = [](ExternalReader& in, ExternalWriter& out,
		std::shared_ptr<StormByte::Logger::Log>) {
		while (!in.EoF()) {
			BinaryData data;
			if (CONSUME(in, 0, data) && !data.empty()) {
				for (auto& b : data)
					b ^= std::byte{0x33};
				(void)out.Write(std::move(data));
			}
		}
		out.Close();
	};
	auto mul171 = [](ExternalReader& in, ExternalWriter& out,
		std::shared_ptr<StormByte::Logger::Log>) {
		while (!in.EoF()) {
			BinaryData data;
			if (CONSUME(in, 0, data) && !data.empty()) {
				for (auto& b : data)
					b = static_cast<std::byte>(static_cast<std::uint8_t>(b) * 171);
				(void)out.Write(std::move(data));
			}
		}
		out.Close();
	};
	auto rotr3 = [](ExternalReader& in, ExternalWriter& out,
		std::shared_ptr<StormByte::Logger::Log>) {
		while (!in.EoF()) {
			BinaryData data;
			if (CONSUME(in, 0, data) && !data.empty()) {
				for (auto& b : data) {
					const auto v = static_cast<std::uint8_t>(b);
					b = static_cast<std::byte>(static_cast<std::uint8_t>((v >> 3) | (v << 5)));
				}
				(void)out.Write(std::move(data));
			}
		}
		out.Close();
	};
	auto add42 = [](ExternalReader& in, ExternalWriter& out,
		std::shared_ptr<StormByte::Logger::Log>) {
		while (!in.EoF()) {
			BinaryData data;
			if (CONSUME(in, 0, data) && !data.empty()) {
				for (auto& b : data)
					b = static_cast<std::byte>(static_cast<std::uint8_t>(b) + 42);
				(void)out.Write(std::move(data));
			}
		}
		out.Close();
	};
	auto sub17 = [](ExternalReader& in, ExternalWriter& out,
		std::shared_ptr<StormByte::Logger::Log>) {
		while (!in.EoF()) {
			BinaryData data;
			if (CONSUME(in, 0, data) && !data.empty()) {
				for (auto& b : data)
					b = static_cast<std::byte>(static_cast<std::uint8_t>(b) - 17);
				(void)out.Write(std::move(data));
			}
		}
		out.Close();
	};
	pipeline.AddPipe(xor55);
	pipeline.AddPipe(add17);
	pipeline.AddPipe(bnot);
	pipeline.AddPipe(xorAA);
	pipeline.AddPipe(mul3);
	pipeline.AddPipe(rotl3);
	pipeline.AddPipe(sub42);
	pipeline.AddPipe(xor33);
	pipeline.AddPipe(xor33);
	pipeline.AddPipe(add42);
	pipeline.AddPipe(rotr3);
	pipeline.AddPipe(mul171);
	pipeline.AddPipe(xorAA);
	pipeline.AddPipe(bnot);
	pipeline.AddPipe(sub17);
	pipeline.AddPipe(xor55);
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
	Consumer result = pipeline.Process(input.Consumer(), kAsyncParallel, logging);
	writer.join();
	WaitForPipelineCompletion(result);
	BinaryData output_data;
	output_data.reserve(StormByte::ByteSize{data_size});
	while (result.Available() > 0) {
		BinaryData chunk;
		if (CONSUME(result, 2048, chunk) && !chunk.empty())
			output_data.insert(output_data.end(), chunk.begin(), chunk.end());
		std::this_thread::yield();
	}
	ASSERT_EQUAL(fn, output_data.size(), StormByte::ByteSize{data_size});
	ASSERT_TRUE(fn, output_data == input_data);
	RETURN_TEST(fn, 0);
}

int test_pipeline_large_data() {
	const std::string fn = "test_pipeline_large_data";
	Pipeline pipeline;
	pipeline.AddPipe([](ExternalReader& in, ExternalWriter& out,
		std::shared_ptr<StormByte::Logger::Log>) {
		std::size_t count = 0;
		while (!in.EoF()) {
			BinaryData data;
			if (CONSUME(in, 0, data) && !data.empty())
				count += static_cast<std::size_t>(data.size());
		}
		(void)out.Write(std::to_string(count));
		out.Close();
	});
	Producer input;
	(void)input.Write(std::string(10000, 'A'));
	input.Close();
	Consumer result = pipeline.Process(input.Consumer(), ExecutionMode::Async, logging);
	WaitForPipelineCompletion(result);
	BinaryData data;
	ASSERT_TRUE(fn, CONSUME(result, 0, data));
	ASSERT_EQUAL(fn, BytesToText(data), std::string("10000"));
	RETURN_TEST(fn, 0);
}

int test_pipeline_reverse_string() {
	const std::string fn = "test_pipeline_reverse_string";
	Pipeline pipeline;
	pipeline.AddPipe([](ExternalReader& in, ExternalWriter& out,
		std::shared_ptr<StormByte::Logger::Log>) {
		std::string buffer;
		while (!in.EoF()) {
			BinaryData data;
			if (CONSUME(in, 0, data) && !data.empty())
				buffer += BytesToText(data);
		}
		std::reverse(buffer.begin(), buffer.end());
		(void)out.Write(buffer);
		out.Close();
	});
	Producer input;
	(void)input.Write("ABCDEF");
	input.Close();
	Consumer result = pipeline.Process(input.Consumer(), ExecutionMode::Async, logging);
	WaitForPipelineCompletion(result);
	BinaryData data;
	ASSERT_TRUE(fn, CONSUME(result, 0, data));
	ASSERT_EQUAL(fn, BytesToText(data), std::string("FEDCBA"));
	RETURN_TEST(fn, 0);
}

int test_pipeline_streaming_data() {
	const std::string fn = "test_pipeline_streaming_data";
	Pipeline pipeline;
	pipeline.AddPipe([](ExternalReader& in, ExternalWriter& out,
		std::shared_ptr<StormByte::Logger::Log>) {
		while (!in.EoF()) {
			BinaryData data;
			if (CONSUME(in, 0, data) && !data.empty())
				(void)out.Write(data);
			std::this_thread::sleep_for(std::chrono::milliseconds(10));
		}
		out.Close();
	});
	Producer input;
	std::thread writer([&input] {
		(void)input.Write("Part1");
		std::this_thread::sleep_for(std::chrono::milliseconds(20));
		(void)input.Write("Part2");
		std::this_thread::sleep_for(std::chrono::milliseconds(20));
		(void)input.Write("Part3");
		input.Close();
	});
	Consumer result = pipeline.Process(input.Consumer(), ExecutionMode::Async, logging);
	writer.join();
	WaitForPipelineCompletion(result);
	BinaryData data;
	ASSERT_TRUE(fn, CONSUME(result, 0, data));
	ASSERT_EQUAL(fn, BytesToText(data), std::string("Part1Part2Part3"));
	RETURN_TEST(fn, 0);
}

int test_pipeline_word_count() {
	const std::string fn = "test_pipeline_word_count";
	Pipeline pipeline;
	pipeline.AddPipe([](ExternalReader& in, ExternalWriter& out,
		std::shared_ptr<StormByte::Logger::Log>) {
		std::size_t word_count = 0;
		std::string buffer;
		while (!in.EoF()) {
			BinaryData data;
			if (CONSUME(in, 0, data) && !data.empty())
				buffer += BytesToText(data);
		}
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
	});
	Producer input;
	(void)input.Write("Hello world this is a test");
	input.Close();
	Consumer result = pipeline.Process(input.Consumer(), ExecutionMode::Async, logging);
	WaitForPipelineCompletion(result);
	BinaryData data;
	ASSERT_TRUE(fn, CONSUME(result, 0, data));
	ASSERT_EQUAL(fn, BytesToText(data), std::string("6"));
	RETURN_TEST(fn, 0);
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
	result += test_pipeline_addpipe_move();
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
