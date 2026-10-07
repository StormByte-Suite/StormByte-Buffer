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

#include <StormByte/buffer/bridge.hxx>
#include <StormByte/buffer/consumer.hxx>
#include <StormByte/buffer/fifo.hxx>
#include <StormByte/buffer/io/buffered_file_reader.hxx>
#include <StormByte/buffer/io/buffered_file_writer.hxx>
#include <StormByte/buffer/producer.hxx>
#include <StormByte/buffer/shared_fifo.hxx>
#include <StormByte/safe/binary.hxx>
#include <StormByte/safe/wstring.hxx>
#include <StormByte/test_handlers.h>

#include <algorithm>
#include <chrono>
#include <cstdint>
#include <filesystem>
#include <fstream>
#include <iostream>
#include <iterator>
#include <string>
#include <string_view>
#include <thread>
#include <utility>

using StormByte::ByteSize;
using StormByte::Buffer::Bridge;
using StormByte::Buffer::Consumer;
using StormByte::Buffer::FIFO;
using StormByte::Buffer::Producer;
using StormByte::Buffer::SharedFIFO;
using StormByte::Buffer::IO::BackPressure;
using StormByte::Buffer::IO::BufferedFileReader;
using StormByte::Buffer::IO::BufferedFileWriter;
using StormByte::Buffer::IO::MaxMemory;
using StormByte::Buffer::IO::ReadAhead;
using StormByte::Buffer::IO::WriteChunk;
using StormByte::Safe::Binary;

static_assert(StormByte::Type::MaybeSafe<Bridge>);
static_assert(!StormByte::Type::SafeValue<Bridge>);
static_assert(StormByte::Type::SafeComponent<Bridge::Operation>);
static_assert(StormByte::Type::SafeComponent<enum Bridge::State>);

namespace {
	template<typename In, typename Out>
	concept BridgeConstructible = requires(In&& in, Out&& out) {
		Bridge(std::forward<In>(in), std::forward<Out>(out));
	};

	class ForeignBridge: public Bridge {
		using Bridge::Bridge;
	};

	static_assert(!StormByte::Type::MaybeSafe<ForeignBridge>);
	static_assert(BridgeConstructible<BufferedFileReader, BufferedFileWriter>);
	static_assert(BridgeConstructible<FIFO&, BufferedFileWriter>);
	static_assert(BridgeConstructible<BufferedFileReader, FIFO&>);
	static_assert(!BridgeConstructible<BufferedFileReader&, BufferedFileWriter>);
	static_assert(!BridgeConstructible<BufferedFileReader, BufferedFileWriter&>);
	static_assert(!BridgeConstructible<const BufferedFileReader, BufferedFileWriter>);
	static_assert(!BridgeConstructible<BufferedFileReader, const BufferedFileWriter>);
	static_assert(!BridgeConstructible<FIFO&, BufferedFileWriter&>);
	static_assert(!BridgeConstructible<BufferedFileReader&, FIFO&>);
	static_assert(!noexcept(Bridge(std::declval<FIFO&>(), std::declval<FIFO&>())));

	std::string BytesToText(const Binary& data) {
		if (data.empty())
			return {};
		return std::string(reinterpret_cast<const char*>(data.data()),
			static_cast<std::size_t>(data.size()));
	}

	StormByte::Safe::String Loc(const std::filesystem::path& path) {
#ifdef WINDOWS
		return StormByte::Safe::String(StormByte::Safe::WString(std::wstring_view(path.wstring())));
#else
		return StormByte::Safe::String(std::string_view(path.string()));
#endif
	}

	std::filesystem::path Scratch(const char* tag) {
		static std::uint64_t n = 0;
		++n;
		return std::filesystem::temp_directory_path() / (std::string("sbb_br_") + tag + std::to_string(n) + ".tmp");
	}

	std::string Slurp(const std::filesystem::path& path) {
		std::ifstream in(path, std::ios::in | std::ios::binary);
		if (!in)
			return {};
		return std::string(std::istreambuf_iterator<char>(in), std::istreambuf_iterator<char>());
	}

	void Dump(const std::filesystem::path& path, const Binary& data) {
		std::ofstream out(path, std::ios::out | std::ios::binary | std::ios::trunc);
		if (data.empty())
			return;
		out.write(reinterpret_cast<const char*>(data.data()),
			static_cast<std::streamsize>(static_cast<std::size_t>(data.size())));
	}

	void DumpText(const std::filesystem::path& path, const std::string& text) {
		Binary data;
		data.resize(ByteSize{text.size()});
		for (std::size_t i = 0; i < text.size(); ++i)
			data[i] = static_cast<std::byte>(text[i]);
		Dump(path, data);
	}

	BufferedFileWriter::Parameters Direct() {
		return { WriteChunk(StormByte::ByteSize{0}), BackPressure(0), MaxMemory(StormByte::ByteSize{0}) };
	}

	Binary Pattern(const std::size_t n) {
		Binary data;
		data.resize(ByteSize{n});
		for (std::size_t i = 0; i < n; ++i)
			data[i] = static_cast<std::byte>(i & 0xFF);
		return data;
	}

	FIFO FromText(const std::string& text) {
		FIFO fifo;
		(void)fifo.Write(text);
		return fifo;
	}

	ByteSize Add(const ByteSize a, const ByteSize b) {
		return ByteSize{static_cast<std::uint64_t>(a) + static_cast<std::uint64_t>(b)};
	}

	ByteSize Drain(Bridge& bridge, const ByteSize chunk = ByteSize{64 * 1024}) {
		ByteSize total{0};
		for (;;) {
			if (bridge.Failed())
				break;
			const ByteSize got = bridge.Passthrough(chunk);
			total = Add(total, got);
			if (got == ByteSize{0})
				break;
		}
		return total;
	}

	bool WaitFile(const std::filesystem::path& path, const std::size_t n) {
		for (int i = 0; i < 200; ++i) {
			if (Slurp(path).size() >= n)
				return true;
			std::this_thread::sleep_for(std::chrono::milliseconds(10));
		}
		return Slurp(path).size() >= n;
	}
}

// -------------------
// Buffer
// -------------------

int test_buf_to_io() {
	const auto path = Scratch("b2io");
	std::filesystem::remove(path);
	FIFO src = FromText("HELLO");
	src.Close();
	BufferedFileWriter out(Loc(path), Direct());
	ASSERT_TRUE(out.Open());
	Bridge bridge(src, std::move(out));
	ASSERT_FALSE(bridge.InputIsIO());
	ASSERT_EQUAL(ByteSize{5}, Drain(bridge, ByteSize{16}));
	ASSERT_TRUE(WaitFile(path, 5));
	ASSERT_EQUAL(std::string("HELLO"), Slurp(path));
	ASSERT_FALSE(bridge.Failed());
	ASSERT_TRUE(bridge.State() == Bridge::State::Closed);
	bridge.Close();
	std::filesystem::remove(path);
	RETURN_TEST(0);
}

int test_buf_to_io_chunked() {
	const auto path = Scratch("b2ioc");
	std::filesystem::remove(path);
	const Binary payload = Pattern(8192);
	FIFO src;
	(void)src.Write(payload);
	src.Close();
	BufferedFileWriter out(Loc(path), Direct());
	ASSERT_TRUE(out.Open());
	Bridge bridge(src, std::move(out));
	ASSERT_EQUAL(ByteSize{8192}, Drain(bridge, ByteSize{300}));
	const std::string disk = Slurp(path);
	ASSERT_EQUAL(payload.size(), ByteSize{disk.size()});
	ASSERT_TRUE(std::equal(payload.begin(), payload.end(),
		reinterpret_cast<const std::byte*>(disk.data())));
	bridge.Close();
	std::filesystem::remove(path);
	RETURN_TEST(0);
}

int test_buf_to_io_writer_not_open() {
	const auto path = Scratch("b2ion");
	std::filesystem::remove(path);
	FIFO src = FromText("NOPE");
	src.Close();
	BufferedFileWriter out(Loc(path), Direct());
	Bridge bridge(src, std::move(out));
	const ByteSize got = bridge.Passthrough(4, Bridge::Operation::NonBlocking);
	ASSERT_TRUE(got == ByteSize{0});
	ASSERT_TRUE(bridge.Failed() || src.Available() == ByteSize{4});
	bridge.Close();
	std::filesystem::remove(path);
	RETURN_TEST(0);
}

// -------------------
// Close
// -------------------

int test_close_is_idempotent() {
	FIFO in = FromText("abc");
	in.Close();
	FIFO out;
	Bridge bridge(in, out);
	bridge.Close();
	bridge.Close();
	ASSERT_TRUE(bridge.State() == Bridge::State::Closed);
	ASSERT_FALSE(bridge.Failed());
	ASSERT_EQUAL(ByteSize{0}, bridge.Passthrough(3));
	RETURN_TEST(0);
}

int test_close_marks_closed_not_failed() {
	FIFO in = FromText("abc");
	in.Close();
	FIFO out;
	Bridge bridge(in, out);
	ASSERT_TRUE(bridge.State() == Bridge::State::Open);
	bridge.Close();
	ASSERT_TRUE(bridge.State() == Bridge::State::Closed);
	ASSERT_FALSE(bridge.Failed());
	ASSERT_TRUE(bridge.EoF());
	ASSERT_EQUAL(ByteSize{0}, bridge.Passthrough(3));
	RETURN_TEST(0);
}

int test_close_on_failed_keeps_failed() {
	FIFO in = FromText("abc");
	in.Close();
	FIFO out;
	out.Close();
	Bridge bridge(in, out);
	ASSERT_EQUAL(ByteSize{0}, bridge.Passthrough(3));
	ASSERT_TRUE(bridge.Failed());
	bridge.Close();
	ASSERT_TRUE(bridge.Failed());
	ASSERT_TRUE(bridge.State() == Bridge::State::Failed);
	RETURN_TEST(0);
}

int test_close_releases_io_for_remove() {
	const auto path = Scratch("closeio");
	std::filesystem::remove(path);
	DumpText(path, "LOCK");
	FIFO out;
	BufferedFileReader reader(Loc(path), { ReadAhead{0}, MaxMemory{0} });
	ASSERT_TRUE(reader.Open());
	Bridge bridge(std::move(reader), out);
	ASSERT_EQUAL(ByteSize{4}, Drain(bridge));
	bridge.Close();
	ASSERT_TRUE(std::filesystem::remove(path));
	RETURN_TEST(0);
}

int test_eof_closes_session() {
	FIFO in = FromText("xy");
	in.Close();
	FIFO out;
	Bridge bridge(in, out);
	ASSERT_EQUAL(ByteSize{2}, bridge.Passthrough(2));
	ASSERT_TRUE(bridge.EoF());
	ASSERT_FALSE(bridge.Failed());
	ASSERT_TRUE(bridge.State() == Bridge::State::Closed);
	ASSERT_EQUAL(ByteSize{0}, bridge.Passthrough(1));
	RETURN_TEST(0);
}

int test_input_is_io_by_ctor() {
	FIFO a;
	FIFO b;
	Bridge mem(a, b);
	ASSERT_FALSE(mem.InputIsIO());
	const auto path = Scratch("isio");
	std::filesystem::remove(path);
	DumpText(path, "Z");
	BufferedFileReader reader(Loc(path), { ReadAhead{0}, MaxMemory{0} });
	ASSERT_TRUE(reader.Open());
	FIFO dest;
	Bridge io(std::move(reader), dest);
	ASSERT_TRUE(io.InputIsIO());
	io.Close();
	std::filesystem::remove(path);
	RETURN_TEST(0);
}

int test_move_assign_releases_lhs_tips() {
	const auto path = Scratch("lhs");
	std::filesystem::remove(path);
	DumpText(path, "LHS");
	FIFO unused;
	BufferedFileReader reader(Loc(path), { ReadAhead{0}, MaxMemory{0} });
	ASSERT_TRUE(reader.Open());
	Bridge lhs(std::move(reader), unused);
	FIFO in = FromText("R");
	in.Close();
	FIFO out;
	Bridge rhs(in, out);
	lhs = std::move(rhs);
	ASSERT_TRUE(std::filesystem::remove(path));
	ASSERT_EQUAL(ByteSize{1}, Drain(lhs));
	RETURN_TEST(0);
}

int test_nonblocking_zero_keeps_open() {
	Producer src;
	FIFO out;
	Consumer in = src.Consumer();
	Bridge bridge(in, out);
	ASSERT_EQUAL(ByteSize{0}, bridge.Passthrough(8, Bridge::Operation::NonBlocking));
	ASSERT_TRUE(bridge.State() == Bridge::State::Open);
	ASSERT_FALSE(bridge.Failed());
	src.Write("ok");
	src.Close();
	ASSERT_EQUAL(ByteSize{2}, Drain(bridge));
	ASSERT_TRUE(bridge.State() == Bridge::State::Closed);
	RETURN_TEST(0);
}

// -------------------
// Failed
// -------------------

int test_bridge_failed_is_sticky() {
	FIFO in = FromText("x");
	in.Close();
	FIFO out;
	Bridge bridge(in, out);
	Bridge taken(std::move(bridge));
	ASSERT_FALSE(bridge.Failed());
	ASSERT_TRUE(bridge.State() == Bridge::State::Closed);
	ASSERT_EQUAL(ByteSize{0}, bridge.Passthrough(1));
	ASSERT_FALSE(taken.Failed());
	ASSERT_TRUE(taken.State() == Bridge::State::Open);
	ASSERT_EQUAL(ByteSize{1}, taken.Passthrough(1));
	ASSERT_TRUE(taken.EoF());
	ASSERT_TRUE(taken.State() == Bridge::State::Closed);
	RETURN_TEST(0);
}

int test_bridge_move_assign() {
	FIFO a_in = FromText("AAAA");
	a_in.Close();
	FIFO a_out;
	FIFO b_in = FromText("BBBBBB");
	b_in.Close();
	FIFO b_out;
	Bridge first(a_in, a_out);
	Bridge second(b_in, b_out);
	second = std::move(first);
	ASSERT_FALSE(first.Failed());
	ASSERT_TRUE(first.State() == Bridge::State::Closed);
	ASSERT_EQUAL(ByteSize{4}, Drain(second));
	Binary got;
	ASSERT_TRUE(a_out.Extract(0, got));
	ASSERT_EQUAL(std::string("AAAA"), BytesToText(got));
	RETURN_TEST(0);
}

int test_closed_dest_rejects_write() {
	FIFO in = FromText("still-here");
	in.Close();
	FIFO out;
	out.Close();
	Bridge bridge(in, out);
	ASSERT_EQUAL(ByteSize{0}, bridge.Passthrough(10));
	ASSERT_TRUE(bridge.Failed() || !out.IsWritable());
	Binary got;
	(void)out.Extract(0, got);
	ASSERT_TRUE(got.empty());
	RETURN_TEST(0);
}

int test_dest_seterror_after_partial_write() {
	FIFO in = FromText("ABCDEFGH");
	in.Close();
	FIFO out;
	Bridge bridge(in, out);
	ASSERT_EQUAL(ByteSize{3}, bridge.Passthrough(3));
	out.SetError();
	const ByteSize second = bridge.Passthrough(5);
	ASSERT_EQUAL(ByteSize{0}, second);
	ASSERT_TRUE(bridge.Failed());
	ASSERT_TRUE(bridge.State() == Bridge::State::Failed);
	ASSERT_EQUAL(ByteSize{0}, bridge.Passthrough(1));
	ASSERT_TRUE(bridge.Failed());
	ASSERT_FALSE(out.IsWritable());
	RETURN_TEST(0);
}

int test_missing_file_open_fails() {
	const auto path = Scratch("missing");
	std::filesystem::remove(path);
	FIFO out;
	BufferedFileReader reader(Loc(path));
	ASSERT_FALSE(reader.Open());
	Bridge bridge(std::move(reader), out);
	ASSERT_TRUE(bridge.InputIsIO());
	ASSERT_EQUAL(ByteSize{0}, bridge.Passthrough(16, Bridge::Operation::NonBlocking));
	ASSERT_TRUE(bridge.Failed() || bridge.EoF());
	bridge.Close();
	std::filesystem::remove(path);
	RETURN_TEST(0);
}

int test_passthrough_on_failed_is_zero() {
	FIFO in;
	FIFO out;
	Bridge first(in, out);
	Bridge second(std::move(first));
	ASSERT_TRUE(first.State() == Bridge::State::Closed);
	ASSERT_EQUAL(ByteSize{0}, first.Passthrough(8));
	ASSERT_EQUAL(ByteSize{0}, first.Passthrough(0, Bridge::Operation::NonBlocking));
	RETURN_TEST(0);
}

int test_producer_seterror_wakes_blocking() {
	Producer src;
	FIFO out;
	Consumer in = src.Consumer();
	Bridge bridge(in, out);
	std::thread bomber([&src] {
		std::this_thread::sleep_for(std::chrono::milliseconds(30));
		src.SetError();
	});
	const ByteSize got = bridge.Passthrough(64, Bridge::Operation::Blocking);
	bomber.join();
	ASSERT_EQUAL(ByteSize{0}, got);
	ASSERT_TRUE(bridge.Failed() || !in.IsReadable());
	ASSERT_EQUAL(ByteSize{0}, bridge.Passthrough(1));
	RETURN_TEST(0);
}

int test_source_seterror_after_bytes() {
	FIFO in = FromText("abcdef");
	FIFO out;
	Bridge bridge(in, out);
	ASSERT_EQUAL(ByteSize{3}, bridge.Passthrough(3));
	in.SetError();
	const ByteSize rest = bridge.Passthrough(3, Bridge::Operation::NonBlocking);
	ASSERT_TRUE(rest == ByteSize{0} || bridge.Failed());
	ASSERT_FALSE(in.IsReadable());
	const auto tel = bridge.ReadTelemetry();
	ASSERT_TRUE(static_cast<bool>(tel));
	ASSERT_TRUE(tel->Delivered() <= ByteSize{6});
	RETURN_TEST(0);
}

int test_source_seterror_before_passthrough() {
	FIFO in = FromText("abc");
	FIFO out;
	in.SetError();
	Bridge bridge(in, out);
	ASSERT_EQUAL(ByteSize{0}, bridge.Passthrough(3));
	ASSERT_TRUE(bridge.Failed() || !in.IsReadable());
	Binary got;
	(void)out.Extract(0, got);
	ASSERT_TRUE(got.empty());
	RETURN_TEST(0);
}

int test_write_to_directory_fails() {
	const auto dir = std::filesystem::temp_directory_path();
	FIFO src = FromText("nope");
	src.Close();
	BufferedFileWriter writer(Loc(dir), Direct());
	ASSERT_FALSE(writer.Open());
	Bridge bridge(src, std::move(writer));
	ASSERT_EQUAL(ByteSize{0}, bridge.Passthrough(4, Bridge::Operation::NonBlocking));
	ASSERT_TRUE(bridge.Failed() || src.Available() == ByteSize{4});
	bridge.Close();
	RETURN_TEST(0);
}

// -------------------
// File
// -------------------

int test_io_empty_file() {
	const auto path = Scratch("empty");
	DumpText(path, "");
	FIFO out;
	BufferedFileReader reader(Loc(path), { ReadAhead{0}, MaxMemory{0} });
	ASSERT_TRUE(reader.Open());
	Bridge bridge(std::move(reader), out);
	ASSERT_TRUE(bridge.InputIsIO());
	ASSERT_EQUAL(ByteSize{0}, Drain(bridge));
	ASSERT_TRUE(bridge.EoF());
	ASSERT_FALSE(bridge.Failed());
	ASSERT_TRUE(bridge.State() == Bridge::State::Closed);
	bridge.Close();
	std::filesystem::remove(path);
	RETURN_TEST(0);
}

int test_io_file_available_zero_without_readahead() {
	const auto path = Scratch("av0");
	DumpText(path, std::string(4096, 'Q'));
	FIFO out;
	BufferedFileReader reader(Loc(path), { ReadAhead{0}, MaxMemory{0} });
	ASSERT_TRUE(reader.Open());
	ASSERT_EQUAL(ByteSize{0}, reader.Available());
	Bridge bridge(std::move(reader), out);
	ASSERT_EQUAL(ByteSize{0}, bridge.Passthrough(0, Bridge::Operation::NonBlocking));
	ASSERT_TRUE(bridge.State() == Bridge::State::Open);
	ASSERT_EQUAL(ByteSize{4096}, bridge.Passthrough(4096, Bridge::Operation::Blocking));
	ASSERT_TRUE(bridge.EoF());
	ASSERT_EQUAL(ByteSize{0}, bridge.Passthrough(0));
	bridge.Close();
	std::filesystem::remove(path);
	RETURN_TEST(0);
}

int test_io_file_to_file() {
	const auto src = Scratch("ff_in");
	const auto dst = Scratch("ff_out");
	const Binary payload = Pattern(10000);
	Dump(src, payload);
	std::filesystem::remove(dst);
	BufferedFileReader reader(Loc(src));
	BufferedFileWriter writer(Loc(dst), Direct());
	ASSERT_TRUE(reader.Open());
	ASSERT_TRUE(writer.Open());
	Bridge bridge(std::move(reader), std::move(writer));
	ASSERT_TRUE(bridge.InputIsIO());
	ASSERT_EQUAL(ByteSize{10000}, Drain(bridge));
	const std::string disk = Slurp(dst);
	ASSERT_EQUAL(payload.size(), ByteSize{disk.size()});
	ASSERT_TRUE(std::equal(payload.begin(), payload.end(),
		reinterpret_cast<const std::byte*>(disk.data())));
	bridge.Close();
	std::filesystem::remove(src);
	std::filesystem::remove(dst);
	RETURN_TEST(0);
}

int test_io_pattern_256() {
	const auto src = Scratch("p256");
	const auto dst = Scratch("p256o");
	const Binary payload = Pattern(256);
	Dump(src, payload);
	BufferedFileReader reader(Loc(src), { ReadAhead{64}, MaxMemory{1024} });
	BufferedFileWriter writer(Loc(dst), Direct());
	ASSERT_TRUE(reader.Open());
	ASSERT_TRUE(writer.Open());
	Bridge bridge(std::move(reader), std::move(writer));
	ASSERT_EQUAL(ByteSize{256}, Drain(bridge, ByteSize{17}));
	const std::string disk = Slurp(dst);
	ASSERT_TRUE(std::equal(payload.begin(), payload.end(),
		reinterpret_cast<const std::byte*>(disk.data())));
	bridge.Close();
	std::filesystem::remove(src);
	std::filesystem::remove(dst);
	RETURN_TEST(0);
}

int test_io_reader_not_open() {
	const auto path = Scratch("rnopen");
	DumpText(path, "SECRET");
	FIFO out;
	BufferedFileReader reader(Loc(path));
	Bridge bridge(std::move(reader), out);
	const ByteSize got = bridge.Passthrough(6, Bridge::Operation::NonBlocking);
	ASSERT_TRUE(got == ByteSize{0} || bridge.Failed());
	Binary leftover;
	(void)out.Extract(0, leftover);
	ASSERT_TRUE(leftover.empty());
	bridge.Close();
	std::filesystem::remove(path);
	RETURN_TEST(0);
}

int test_io_to_buf() {
	const auto path = Scratch("i2b");
	DumpText(path, "WORLD");
	FIFO out;
	BufferedFileReader reader(Loc(path), { ReadAhead{0}, MaxMemory{0} });
	ASSERT_TRUE(reader.Open());
	Bridge bridge(std::move(reader), out);
	ASSERT_TRUE(bridge.InputIsIO());
	ASSERT_EQUAL(ByteSize{5}, Drain(bridge));
	Binary got;
	ASSERT_TRUE(out.Extract(0, got));
	ASSERT_EQUAL(std::string("WORLD"), BytesToText(got));
	bridge.Close();
	std::filesystem::remove(path);
	RETURN_TEST(0);
}

int test_io_to_buf_nul_and_binary() {
	const auto path = Scratch("nul");
	Binary payload{std::byte{0}, std::byte{'A'}, std::byte{0}, std::byte{0xFF}, std::byte{'Z'}};
	Dump(path, payload);
	FIFO out;
	BufferedFileReader reader(Loc(path));
	ASSERT_TRUE(reader.Open());
	Bridge bridge(std::move(reader), out);
	ASSERT_EQUAL(payload.size(), Drain(bridge, ByteSize{1}));
	Binary got;
	ASSERT_TRUE(out.Extract(0, got));
	ASSERT_TRUE(got == payload);
	bridge.Close();
	std::filesystem::remove(path);
	RETURN_TEST(0);
}

int test_io_to_buf_pattern() {
	const auto path = Scratch("pat");
	const Binary payload = Pattern(2048);
	Dump(path, payload);
	FIFO out;
	BufferedFileReader reader(Loc(path), { ReadAhead{512}, MaxMemory{4096} });
	ASSERT_TRUE(reader.Open());
	Bridge bridge(std::move(reader), out);
	ASSERT_EQUAL(ByteSize{2048}, Drain(bridge, ByteSize{128}));
	Binary got;
	ASSERT_TRUE(out.Extract(0, got));
	ASSERT_TRUE(got == payload);
	const auto tel = bridge.ReadTelemetry();
	ASSERT_TRUE(static_cast<bool>(tel));
	ASSERT_EQUAL(ByteSize{2048}, tel->Delivered());
	bridge.Close();
	ASSERT_EQUAL(ByteSize{2048}, tel->Delivered());
	std::filesystem::remove(path);
	RETURN_TEST(0);
}

int test_io_unopened_does_not_write() {
	const auto path = Scratch("unw");
	std::filesystem::remove(path);
	FIFO src = FromText("XXXX");
	src.Close();
	BufferedFileWriter writer(Loc(path), Direct());
	Bridge bridge(src, std::move(writer));
	(void)bridge.Passthrough(4, Bridge::Operation::NonBlocking);
	ASSERT_TRUE(!std::filesystem::exists(path) || Slurp(path).empty());
	bridge.Close();
	std::filesystem::remove(path);
	RETURN_TEST(0);
}

// -------------------
// Mux
// -------------------

int test_demuxer_file_to_producer() {
	const auto path = Scratch("dmx");
	DumpText(path, "DEMUX-DATA");
	Producer dst;
	Consumer view = dst.Consumer();
	BufferedFileReader reader(Loc(path));
	ASSERT_TRUE(reader.Open());
	Bridge bridge(std::move(reader), dst);
	ASSERT_EQUAL(ByteSize{10}, Drain(bridge, ByteSize{3}));
	dst.Close();
	Binary got;
	ASSERT_TRUE(view.Extract(0, got));
	ASSERT_EQUAL(std::string("DEMUX-DATA"), BytesToText(got));
	bridge.Close();
	std::filesystem::remove(path);
	RETURN_TEST(0);
}

int test_muxer_producer_to_file() {
	const auto path = Scratch("mux");
	std::filesystem::remove(path);
	Producer src;
	Consumer in = src.Consumer();
	BufferedFileWriter writer(Loc(path), Direct());
	ASSERT_TRUE(writer.Open());
	Bridge bridge(in, std::move(writer));
	src.Write("PART1");
	ASSERT_EQUAL(ByteSize{5}, bridge.Passthrough(5, Bridge::Operation::Blocking));
	src.Write("PART2");
	src.Close();
	ASSERT_EQUAL(ByteSize{5}, Drain(bridge));
	ASSERT_TRUE(WaitFile(path, 10));
	ASSERT_EQUAL(std::string("PART1PART2"), Slurp(path));
	bridge.Close();
	std::filesystem::remove(path);
	RETURN_TEST(0);
}

int test_muxer_then_demuxer_roundtrip() {
	const auto path = Scratch("round");
	std::filesystem::remove(path);
	const Binary payload = Pattern(4096);
	Producer src;
	Consumer in = src.Consumer();
	BufferedFileWriter writer(Loc(path), Direct());
	ASSERT_TRUE(writer.Open());
	Bridge to_file(in, std::move(writer));
	(void)src.Write(payload);
	src.Close();
	ASSERT_EQUAL(payload.size(), Drain(to_file, ByteSize{256}));
	to_file.Close();
	BufferedFileReader reader(Loc(path));
	ASSERT_TRUE(reader.Open());
	FIFO out;
	Bridge from_file(std::move(reader), out);
	ASSERT_EQUAL(payload.size(), Drain(from_file, ByteSize{333}));
	Binary got;
	ASSERT_TRUE(out.Extract(0, got));
	ASSERT_TRUE(got == payload);
	from_file.Close();
	std::filesystem::remove(path);
	RETURN_TEST(0);
}

int test_producer_close_empty() {
	Producer src;
	FIFO out;
	Consumer in = src.Consumer();
	Bridge bridge(in, out);
	src.Close();
	ASSERT_EQUAL(ByteSize{0}, bridge.Passthrough(16, Bridge::Operation::Blocking));
	ASSERT_TRUE(bridge.EoF());
	ASSERT_FALSE(bridge.Failed());
	ASSERT_TRUE(bridge.State() == Bridge::State::Closed);
	RETURN_TEST(0);
}

int test_producer_close_wakes_blocking() {
	Producer src;
	FIFO out;
	Consumer in = src.Consumer();
	Bridge bridge(in, out);
	std::thread closer([&src] {
		std::this_thread::sleep_for(std::chrono::milliseconds(40));
		src.Write("wake");
		src.Close();
	});
	ASSERT_EQUAL(ByteSize{4}, bridge.Passthrough(4, Bridge::Operation::Blocking));
	closer.join();
	ASSERT_TRUE(bridge.EoF());
	RETURN_TEST(0);
}

// -------------------
// Operation
// -------------------

int test_blocking_returns_partial_on_eof() {
	FIFO in = FromText("xyz");
	in.Close();
	FIFO out;
	Bridge bridge(in, out);
	ASSERT_EQUAL(ByteSize{3}, bridge.Passthrough(100, Bridge::Operation::Blocking));
	ASSERT_TRUE(bridge.EoF());
	ASSERT_FALSE(bridge.Failed());
	RETURN_TEST(0);
}

int test_blocking_waits_for_n() {
	Producer src;
	FIFO out;
	Consumer in = src.Consumer();
	Bridge bridge(in, out);
	std::thread writer([&src] {
		std::this_thread::sleep_for(std::chrono::milliseconds(30));
		src.Write("abcd");
		std::this_thread::sleep_for(std::chrono::milliseconds(30));
		src.Write("efgh");
		src.Close();
	});
	ASSERT_EQUAL(ByteSize{8}, bridge.Passthrough(8, Bridge::Operation::Blocking));
	writer.join();
	Binary got;
	ASSERT_TRUE(out.Extract(0, got));
	ASSERT_EQUAL(std::string("abcdefgh"), BytesToText(got));
	RETURN_TEST(0);
}

int test_nonblocking_does_not_wait() {
	Producer src;
	FIFO out;
	Consumer in = src.Consumer();
	Bridge bridge(in, out);
	const auto t0 = std::chrono::steady_clock::now();
	const ByteSize got = bridge.Passthrough(1024, Bridge::Operation::NonBlocking);
	const auto ms = std::chrono::duration_cast<std::chrono::milliseconds>(
		std::chrono::steady_clock::now() - t0).count();
	ASSERT_EQUAL(ByteSize{0}, got);
	ASSERT_TRUE(ms < 200);
	ASSERT_FALSE(bridge.EoF());
	ASSERT_TRUE(bridge.State() == Bridge::State::Open);
	src.Write("late");
	src.Close();
	ASSERT_EQUAL(ByteSize{4}, bridge.Passthrough(16, Bridge::Operation::Blocking));
	RETURN_TEST(0);
}

int test_nonblocking_takes_available() {
	FIFO in = FromText("xy");
	FIFO out;
	Bridge bridge(in, out);
	ASSERT_EQUAL(ByteSize{2}, bridge.Passthrough(16, Bridge::Operation::NonBlocking));
	ASSERT_FALSE(bridge.EoF());
	(void)in.Write("z");
	in.Close();
	ASSERT_EQUAL(ByteSize{1}, bridge.Passthrough(16, Bridge::Operation::NonBlocking));
	ASSERT_EQUAL(ByteSize{0}, bridge.Passthrough(16, Bridge::Operation::NonBlocking));
	ASSERT_TRUE(bridge.EoF());
	RETURN_TEST(0);
}

int test_zero_is_current_contents() {
	FIFO in = FromText("payload");
	FIFO out;
	Bridge bridge(in, out);
	ASSERT_EQUAL(ByteSize{7}, bridge.Passthrough(0));
	(void)in.Write("!!");
	ASSERT_EQUAL(ByteSize{2}, bridge.Passthrough(0));
	in.Close();
	ASSERT_EQUAL(ByteSize{0}, bridge.Passthrough(0));
	ASSERT_TRUE(bridge.EoF());
	Binary got;
	ASSERT_TRUE(out.Extract(0, got));
	ASSERT_EQUAL(std::string("payload!!"), BytesToText(got));
	RETURN_TEST(0);
}

// -------------------
// Stream
// -------------------

int test_backpressure_shared_fifo() {
	const std::string text = "ABCDEFGHIJKLMNOP";
	FIFO src = FromText(text);
	src.Close();
	SharedFIFO dst;
	Bridge bridge(src, dst);
	Binary collected;
	while (!bridge.EoF() && !bridge.Failed()) {
		const ByteSize got = bridge.Passthrough(2, Bridge::Operation::Blocking);
		if (got == ByteSize{0})
			break;
		Binary chunk;
		ASSERT_TRUE(dst.Extract(got, chunk));
		collected.insert(collected.end(), chunk.begin(), chunk.end());
	}
	ASSERT_EQUAL(text, BytesToText(collected));
	RETURN_TEST(0);
}

int test_chunks_then_eof() {
	FIFO in = FromText("0123456789");
	in.Close();
	FIFO out;
	Bridge bridge(in, out);
	ASSERT_EQUAL(ByteSize{3}, bridge.Passthrough(3));
	ASSERT_EQUAL(ByteSize{3}, bridge.Passthrough(3));
	ASSERT_EQUAL(ByteSize{4}, bridge.Passthrough(8));
	ASSERT_TRUE(bridge.EoF());
	ASSERT_FALSE(bridge.Failed());
	Binary got;
	ASSERT_TRUE(out.Extract(0, got));
	ASSERT_EQUAL(std::string("0123456789"), BytesToText(got));
	const auto read = bridge.ReadTelemetry();
	const auto write = bridge.WriteTelemetry();
	ASSERT_TRUE(static_cast<bool>(read));
	ASSERT_TRUE(static_cast<bool>(write));
	ASSERT_EQUAL(ByteSize{10}, read->Delivered());
	ASSERT_EQUAL(ByteSize{10}, write->Accepted());
	ASSERT_TRUE(read == bridge.ReadTelemetry());
	bridge.Close();
	ASSERT_EQUAL(ByteSize{10}, read->Delivered());
	RETURN_TEST(0);
}

int test_close_source_while_copying() {
	Producer src;
	FIFO out;
	Consumer in = src.Consumer();
	Bridge bridge(in, out);
	src.Write("HELLO");
	ASSERT_EQUAL(ByteSize{5}, bridge.Passthrough(5));
	src.Close();
	ASSERT_EQUAL(ByteSize{0}, bridge.Passthrough(8));
	ASSERT_TRUE(bridge.EoF());
	ASSERT_FALSE(bridge.Failed());
	RETURN_TEST(0);
}

int test_empty_closed_is_eof() {
	FIFO in;
	in.Close();
	FIFO out;
	Bridge bridge(in, out);
	ASSERT_EQUAL(ByteSize{0}, bridge.Passthrough(4));
	ASSERT_TRUE(bridge.EoF());
	ASSERT_FALSE(bridge.Failed());
	ASSERT_TRUE(bridge.State() == Bridge::State::Closed);
	RETURN_TEST(0);
}

int test_ext_drains_all() {
	FIFO in = FromText("DRAIN-ALL-PLEASE");
	in.Close();
	FIFO out;
	Bridge bridge(in, out);
	ASSERT_EQUAL(ByteSize{16}, Drain(bridge, ByteSize{5}));
	Binary got;
	ASSERT_TRUE(out.Extract(0, got));
	ASSERT_EQUAL(std::string("DRAIN-ALL-PLEASE"), BytesToText(got));
	RETURN_TEST(0);
}

int test_pattern_integrity() {
	const Binary payload = Pattern(50 * 1024);
	FIFO in;
	(void)in.Write(payload);
	in.Close();
	FIFO out;
	Bridge bridge(in, out);
	ASSERT_EQUAL(payload.size(), Drain(bridge, ByteSize{777}));
	Binary got;
	ASSERT_TRUE(out.Extract(0, got));
	ASSERT_TRUE(got == payload);
	RETURN_TEST(0);
}

int main() {
	int result = 0;

	// -------------------
	// Buffer
	// -------------------
	result += test_buf_to_io();
	result += test_buf_to_io_chunked();
	result += test_buf_to_io_writer_not_open();

	// -------------------
	// Close
	// -------------------
	result += test_close_is_idempotent();
	result += test_close_marks_closed_not_failed();
	result += test_close_on_failed_keeps_failed();
	result += test_close_releases_io_for_remove();
	result += test_eof_closes_session();
	result += test_input_is_io_by_ctor();
	result += test_move_assign_releases_lhs_tips();
	result += test_nonblocking_zero_keeps_open();

	// -------------------
	// Failed
	// -------------------
	result += test_bridge_failed_is_sticky();
	result += test_bridge_move_assign();
	result += test_closed_dest_rejects_write();
	result += test_dest_seterror_after_partial_write();
	result += test_missing_file_open_fails();
	result += test_passthrough_on_failed_is_zero();
	result += test_producer_seterror_wakes_blocking();
	result += test_source_seterror_after_bytes();
	result += test_source_seterror_before_passthrough();
	result += test_write_to_directory_fails();

	// -------------------
	// File
	// -------------------
	result += test_io_empty_file();
	result += test_io_file_available_zero_without_readahead();
	result += test_io_file_to_file();
	result += test_io_pattern_256();
	result += test_io_reader_not_open();
	result += test_io_to_buf();
	result += test_io_to_buf_nul_and_binary();
	result += test_io_to_buf_pattern();
	result += test_io_unopened_does_not_write();

	// -------------------
	// Mux
	// -------------------
	result += test_demuxer_file_to_producer();
	result += test_muxer_producer_to_file();
	result += test_muxer_then_demuxer_roundtrip();
	result += test_producer_close_empty();
	result += test_producer_close_wakes_blocking();

	// -------------------
	// Operation
	// -------------------
	result += test_blocking_returns_partial_on_eof();
	result += test_blocking_waits_for_n();
	result += test_nonblocking_does_not_wait();
	result += test_nonblocking_takes_available();
	result += test_zero_is_current_contents();

	// -------------------
	// Stream
	// -------------------
	result += test_backpressure_shared_fifo();
	result += test_chunks_then_eof();
	result += test_close_source_while_copying();
	result += test_empty_closed_is_eof();
	result += test_ext_drains_all();
	result += test_pattern_integrity();

	if (result == 0)
		std::cout << "All tests passed!" << std::endl;
	else
		std::cout << result << " tests failed." << std::endl;
	return result;
}
