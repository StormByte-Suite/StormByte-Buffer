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
#include <StormByte/safe/string.hxx>
#include <StormByte/test_handlers.h>

#include <iostream>
#include <string>
#include <string_view>
#include <utility>

using StormByte::Buffer::Error;
using StormByte::Buffer::Exception;
using StormByte::Buffer::ReadError;
using StormByte::Buffer::WriteError;

// -------------------
// Copy
// -------------------

int test_copy_move_and_assign() {
	Exception source(std::string_view("message"));
	Exception copied(source);
	ASSERT_EQUAL(std::string("StormByte.Buffer: message"), std::string(copied.what()));
	Exception moved(std::move(copied));
	ASSERT_EQUAL(std::string("StormByte.Buffer: message"), std::string(moved.what()));
	Exception assigned(std::string_view("other"));
	assigned = source;
	ASSERT_EQUAL(std::string("StormByte.Buffer: message"), std::string(assigned.what()));
	assigned = std::move(moved);
	ASSERT_EQUAL(std::string("StormByte.Buffer: message"), std::string(assigned.what()));

	ReadError read_source(std::string_view("read"));
	ReadError read_copy(read_source);
	ASSERT_EQUAL(std::string("StormByte.Buffer.Read: read"), std::string(read_copy.what()));
	WriteError write_source(std::string_view("write"));
	WriteError write_moved(std::move(write_source));
	ASSERT_EQUAL(std::string("StormByte.Buffer.Write: write"), std::string(write_moved.what()));
	RETURN_TEST(0);
}

// -------------------
// Hierarchy
// -------------------

int test_hierarchy_and_paths() {
	ReadError read(std::string_view("read failed"));
	WriteError write(std::string_view("write failed"));
	Error error(std::string_view("plain"));
	ASSERT_TRUE(dynamic_cast<Error*>(&read) != nullptr);
	ASSERT_TRUE(dynamic_cast<Exception*>(&write) != nullptr);
	ASSERT_TRUE(dynamic_cast<StormByte::Exception*>(&error) != nullptr);
	ASSERT_CONTAINS(std::string_view(read.what()), std::string_view("StormByte.Buffer.Read: "));
	ASSERT_CONTAINS(std::string_view(write.what()), std::string_view("StormByte.Buffer.Write: "));
	ASSERT_NOT_CONTAINS(std::string_view(error.what()), std::string_view("StormByte.Buffer.Read: "));
	RETURN_TEST(0);
}

// -------------------
// Messages
// -------------------

int test_error_from_std_string() {
	Error exception(std::string("message"));
	ASSERT_EQUAL(std::string("StormByte.Buffer: message"), std::string(exception.what()));
	RETURN_TEST(0);
}

int test_exception_empty_literal_and_null() {
	Exception empty(std::string_view{});
	ASSERT_EQUAL(std::string("StormByte.Buffer: "), std::string(empty.what()));
	Exception literal("literal");
	ASSERT_EQUAL(std::string("StormByte.Buffer: literal"), std::string(literal.what()));
	RETURN_TEST(0);
}

int test_exception_format() {
	Exception exception("value is {}", 42);
	ASSERT_EQUAL(std::string("StormByte.Buffer: value is 42"), std::string(exception.what()));
	ReadError read("read {}", "failed");
	ASSERT_EQUAL(std::string("StormByte.Buffer.Read: read failed"), std::string(read.what()));
	WriteError write("write {}", 7);
	ASSERT_EQUAL(std::string("StormByte.Buffer.Write: write 7"), std::string(write.what()));
	RETURN_TEST(0);
}

int test_exception_from_safe_string() {
	const StormByte::Safe::String message(std::string_view("base text"));
	Exception exception(message);
	ASSERT_EQUAL(std::string("StormByte.Buffer: base text"), std::string(exception.what()));
	const StormByte::Safe::String read_text(std::string_view("base read"));
	ReadError read(read_text);
	ASSERT_EQUAL(std::string("StormByte.Buffer.Read: base read"), std::string(read.what()));
	const StormByte::Safe::String write_text(std::string_view("base write"));
	WriteError write(write_text);
	ASSERT_EQUAL(std::string("StormByte.Buffer.Write: base write"), std::string(write.what()));
	RETURN_TEST(0);
}

int test_exception_view_lifetime() {
	std::string source("prefix:message:suffix");
	const std::string_view message(source.data() + 7, 7);
	Exception exception(message);
	Error error(message);
	ReadError read(message);
	WriteError write(message);
	source.assign(source.size(), 'x');
	ASSERT_EQUAL(std::string("StormByte.Buffer: message"), std::string(exception.what()));
	ASSERT_EQUAL(std::string("StormByte.Buffer: message"), std::string(error.what()));
	ASSERT_EQUAL(std::string("StormByte.Buffer.Read: message"), std::string(read.what()));
	ASSERT_EQUAL(std::string("StormByte.Buffer.Write: message"), std::string(write.what()));
	RETURN_TEST(0);
}

int test_write_error_from_std_string() {
	WriteError exception(std::string("write failed"));
	ASSERT_EQUAL(std::string("StormByte.Buffer.Write: write failed"), std::string(exception.what()));
	RETURN_TEST(0);
}

int main() {
	int result = 0;

	// -------------------
	// Copy
	// -------------------
	result += test_copy_move_and_assign();

	// -------------------
	// Hierarchy
	// -------------------
	result += test_hierarchy_and_paths();

	// -------------------
	// Messages
	// -------------------
	result += test_error_from_std_string();
	result += test_exception_empty_literal_and_null();
	result += test_exception_format();
	result += test_exception_from_safe_string();
	result += test_exception_view_lifetime();
	result += test_write_error_from_std_string();

	if (result == 0)
		std::cout << "All tests passed!" << std::endl;
	else
		std::cout << result << " tests failed." << std::endl;
	return result;
}
