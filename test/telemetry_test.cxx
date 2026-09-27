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
#include <StormByte/buffer/fifo.hxx>
#include <StormByte/buffer/telemetry.hxx>
#include <StormByte/test_handlers.h>

#include <iostream>
#include <string>

using StormByte::Buffer::Bridge;
using StormByte::Buffer::FIFO;

// -------------------
// Counters
// -------------------

int test_telemetry_empty_before_passthrough() {
	constexpr auto fn = "test_telemetry_empty_before_passthrough";
	int result = 0;
	FIFO in;
	FIFO out;
	in.Write("payload");
	in.Close();
	Bridge bridge(in, out);
	const auto read = bridge.ReadTelemetry();
	const auto write = bridge.WriteTelemetry();
	ASSERT_TRUE(fn, static_cast<bool>(read));
	ASSERT_TRUE(fn, static_cast<bool>(write));
	ASSERT_TRUE(fn, read->Delivered() == 0);
	ASSERT_TRUE(fn, write->Accepted() == 0);
	RETURN_TEST(fn, result);
}

int test_telemetry_flatten_after_transfer() {
	constexpr auto fn = "test_telemetry_flatten_after_transfer";
	int result = 0;
	FIFO in;
	FIFO out;
	in.Write("payload");
	in.Close();
	Bridge bridge(in, out);
	ASSERT_TRUE(fn, bridge.Passthrough(7) == 7);
	const auto read = bridge.ReadTelemetry();
	const auto write = bridge.WriteTelemetry();
	ASSERT_TRUE(fn, static_cast<bool>(read));
	ASSERT_TRUE(fn, static_cast<bool>(write));
	const std::string flat_read = *read;
	const std::string flat_write = *write;
	ASSERT_TRUE(fn, !flat_read.empty());
	ASSERT_TRUE(fn, !flat_write.empty());
	RETURN_TEST(fn, result);
}

int test_telemetry_same_handle() {
	constexpr auto fn = "test_telemetry_same_handle";
	int result = 0;
	FIFO in;
	FIFO out;
	in.Write("payload");
	in.Close();
	Bridge bridge(in, out);
	const auto first = bridge.ReadTelemetry();
	ASSERT_TRUE(fn, bridge.Passthrough(7) == 7);
	const auto second = bridge.ReadTelemetry();
	ASSERT_TRUE(fn, first == second);
	ASSERT_TRUE(fn, first->Delivered() == 7);
	ASSERT_TRUE(fn, second->Delivered() == 7);
	RETURN_TEST(fn, result);
}

int test_telemetry_tracks_delivered_and_accepted() {
	constexpr auto fn = "test_telemetry_tracks_delivered_and_accepted";
	int result = 0;
	FIFO in;
	FIFO out;
	in.Write("payload");
	in.Close();
	Bridge bridge(in, out);
	ASSERT_TRUE(fn, bridge.Passthrough(7) == 7);
	const auto read = bridge.ReadTelemetry();
	const auto write = bridge.WriteTelemetry();
	ASSERT_TRUE(fn, static_cast<bool>(read));
	ASSERT_TRUE(fn, static_cast<bool>(write));
	ASSERT_TRUE(fn, read->Delivered() == 7);
	ASSERT_TRUE(fn, write->Accepted() == 7);
	RETURN_TEST(fn, result);
}

int main() {
	int result = 0;

	// -------------------
	// Counters
	// -------------------
	result += test_telemetry_empty_before_passthrough();
	result += test_telemetry_flatten_after_transfer();
	result += test_telemetry_same_handle();
	result += test_telemetry_tracks_delivered_and_accepted();

	if (result == 0)
		std::cout << "All tests passed!" << std::endl;
	else
		std::cout << result << " tests failed." << std::endl;
	return result;
}
