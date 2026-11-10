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

#include <StormByte/buffer/byte_budget.hxx>
#include <StormByte/buffer/fifo.hxx>
#include <StormByte/buffer/io/buffered_file_reader.hxx>
#include <StormByte/buffer/io/buffered_file_writer.hxx>
#include <StormByte/buffer/sink.hxx>
#include <StormByte/logger/log.hxx>
#include <StormByte/system/host.hxx>

#include <climits>
#include <sstream>
#include <string>

#ifdef WINDOWS
#ifdef BUFFER_BUDGET_PROVIDER
#define BUFFER_TEST_PUBLIC __declspec(dllexport)
#else
#define BUFFER_TEST_PUBLIC __declspec(dllimport)
#endif
#else
#define BUFFER_TEST_PUBLIC __attribute__((visibility("default")))
#endif

/**
 * @brief Allocate shared accounting in a separate DLL.
 * @return Ten-byte budget.
 */
BUFFER_TEST_PUBLIC StormByte::Buffer::ByteBudget CreateBudget();

/**
 * @brief Allocate typed queue state and its destruction callbacks in a separate DLL.
 * @return Provider-owned hopper.
 */
BUFFER_TEST_PUBLIC StormByte::Safe::Shared<StormByte::Buffer::Hopper<int>> CreateHopper();

#ifdef BUFFER_BUDGET_PROVIDER

StormByte::Buffer::ByteBudget CreateBudget() {
	return StormByte::Buffer::ByteBudget(StormByte::ByteSize{10});
}

StormByte::Safe::Shared<StormByte::Buffer::Hopper<int>> CreateHopper() {
	return StormByte::Safe::Shared<StormByte::Buffer::Hopper<int>>::MakePointer<StormByte::Buffer::Hopper<int>>();
}

#else

int main() {
	using namespace StormByte::Buffer::IO;
	BufferedFileReader reader("unused", {ReadChunk(4096), ReadAhead(0), MaxMemory(0)});
	BufferedFileWriter writer("unused", {WriteTransfer(8192), WriteThreshold(16384), MaxMemory(32768)});
	if (reader.Transfers().TransferLimit != StormByte::ByteSize{4096}
			|| writer.Transfers().TransferLimit != StormByte::ByteSize{8192})
		return 5;
	auto configuration = writer.Configuration();
	configuration.WriteTransfer = StormByte::ByteSize{4096};
	if (writer.Configure(configuration).status != Status::Ok)
		return 6;
	auto budget = CreateBudget();
	StormByte::Buffer::ByteBudget::Lease lease;
	if (budget.ReserveFor(StormByte::ByteSize{5}, lease, std::chrono::milliseconds{1})
			!= StormByte::Buffer::QueueStatus::Ok)
		return 7;
	StormByte::Buffer::Sink<int> producer;
	StormByte::Buffer::Sink<int> consumer;
	producer.To(1) >> consumer;
	if (producer.Configure(1, {}, budget) != StormByte::Buffer::QueueStatus::Ok)
		return 8;
	int item = 11;
	if (producer.TryPush(1, item, lease) != StormByte::Buffer::QueueStatus::Ok || consumer.Pop(1) != 11)
		return 9;
	lease.Reset();
	if (budget.Snapshot().Reserved != StormByte::ByteSize{0})
		return 10;
	auto hopper = CreateHopper();
	if (hopper->Configure({}, budget) != StormByte::Buffer::QueueStatus::Ok
			|| hopper->TryPush(item, StormByte::ByteSize{8}) != StormByte::Buffer::QueueStatus::Ok)
		return 11;
	if (budget.Snapshot().Reserved != StormByte::ByteSize{8} || hopper->Pop() != 11)
		return 12;
	if (budget.Snapshot().Reserved != StormByte::ByteSize{0})
		return 13;
	StormByte::Buffer::FIFO fifo;
	if (!fifo.Write("hello") || fifo.Available() != StormByte::ByteSize{5})
		return 1;

	StormByte::Safe::Binary contents;
	if (!fifo.Extract(5, contents) || contents.size() != StormByte::ByteSize{5})
		return 2;

	std::ostringstream output;
	StormByte::Logger::Log log(output);
	log << "buffer consumer" << std::endl;
	if (output.str().find("buffer consumer") == std::string::npos)
		return 3;

	const StormByte::System::Host::CPU cpu;
	return cpu.Bitness() == sizeof(void*) * CHAR_BIT ? 0 : 4;
}

#endif
