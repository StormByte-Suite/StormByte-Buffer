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
#include <StormByte/buffer/fifo.hxx>
#include <StormByte/buffer/io/buffered_file_reader.hxx>
#include <StormByte/buffer/io/buffered_file_writer.hxx>
#include <StormByte/buffer/io/buffered_location_reader.hxx>
#include <StormByte/buffer/io/buffered_location_writer.hxx>
#include <StormByte/safe/pointers.hxx>
#include <StormByte/system/device.hxx>
#include <StormByte/test_handlers.h>

#include <chrono>
#include <cstddef>
#include <filesystem>
#include <fstream>
#include <iostream>
#include <optional>
#include <span>
#include <stdexcept>
#include <string_view>
#include <utility>

using StormByte::Buffer::FIFO;
using StormByte::Buffer::Position;
using StormByte::Buffer::IO::BufferedFileReader;
using StormByte::Buffer::IO::BufferedFileWriter;
using StormByte::Buffer::IO::BufferedLocationReader;
using StormByte::Buffer::IO::BufferedLocationWriter;
using StormByte::Buffer::IO::Location;
using StormByte::Buffer::IO::Result;
using StormByte::Buffer::IO::State;
using StormByte::Buffer::IO::Status;
using StormByte::Safe::Shared;
using StormByte::System::Device;

static_assert(StormByte::Type::MaybeSafe<StormByte::Buffer::IO::BufferedReader>);
static_assert(StormByte::Type::MaybeSafe<StormByte::Buffer::IO::BufferedWriter>);
static_assert(StormByte::Type::MaybeSafe<BufferedLocationReader>);
static_assert(StormByte::Type::MaybeSafe<BufferedLocationWriter>);
static_assert(StormByte::Type::MaybeSafe<BufferedFileReader>);
static_assert(StormByte::Type::MaybeSafe<BufferedFileWriter>);
static_assert(StormByte::Type::MaybeSafe<StormByte::Buffer::IO::ReadTelemetry>);
static_assert(StormByte::Type::MaybeSafe<StormByte::Buffer::IO::WriteTelemetry>);
static_assert(StormByte::Type::SafeValue<Result>);
static_assert(StormByte::Type::MaybeSafe<StormByte::Buffer::IO::ReadAhead>);
static_assert(StormByte::Type::MaybeSafe<StormByte::Buffer::IO::MaxMemory>);
static_assert(StormByte::Type::MaybeSafe<StormByte::Buffer::IO::MaxWait>);
static_assert(StormByte::Type::MaybeSafe<StormByte::Buffer::IO::WriteChunk>);
static_assert(StormByte::Type::MaybeSafe<StormByte::Buffer::IO::BackPressure>);

namespace {
	constexpr StormByte::ByteSize kFakeRead{123456};
	constexpr StormByte::ByteSize kFakeWrite{65432};
	constexpr StormByte::ByteSize kFakeReadBps{5000000};
	constexpr StormByte::ByteSize kFakeWriteBps{4000000};
	constexpr std::size_t kDefaultBackPressure = 4;
	constexpr StormByte::ByteSize kDefaultMaxMemory{1024ull * 1024ull};

	/**
	 * @brief Stand-in for a network device. The accessor is not a filesystem path.
	 */
	class FakeDevice final: public Device {
		public:
			/**
			 * @brief Construct a fake device with a non-path accessor.
			 * @param accessor Device name.
			 */
			explicit FakeDevice(std::string_view accessor) noexcept: Device(accessor) {}

			/**
			 * @brief Return the canned throughput.
			 * @return Fake read and write rates.
			 */
			struct Throughput Throughput() const noexcept override {
				return { kFakeReadBps, kFakeWriteBps };
			}

			/**
			 * @brief Return the canned window.
			 * @return Fake read and write windows.
			 */
			struct Window Window() const noexcept override {
				return { kFakeRead, kFakeWrite };
			}
	};

	/**
	 * @brief How a fake leaf reports its origin device.
	 */
	enum class DevicePolicy {
		Fake,		///< Non-path device, leaf declares it usable.
		FakeStrict,	///< Non-path device, default usability probe fails.
		Empty		///< Leaf hands out an empty owner.
	};

	/**
	 * @brief Location reader whose origin is a fake device.
	 */
	class FakeReader final: public BufferedLocationReader {
		public:
			/**
			 * @brief Construct a reader for the selected device policy.
			 * @param policy Device reporting policy.
			 */
			explicit FakeReader(const DevicePolicy policy):
				BufferedLocationReader(StormByte::Safe::String(std::string_view("nic://eth0")),
					Location::Remote, Parameters{}),
				m_policy(policy) {}

		protected:
			/**
			 * @brief Report the fake origin, or an empty owner.
			 * @return Shared device.
			 */
			Shared<StormByte::System::Device> OriginDevice() const override {
				if (m_policy == DevicePolicy::Empty)
					return {};
				return Shared<StormByte::System::Device>::MakePointer<FakeDevice>(std::string_view("nic://eth0"));
			}

			/**
			 * @brief Accept a present fake device when the policy says so.
			 * @param device Reported origin.
			 * @return Whether setup may use the device window.
			 */
			bool OriginDeviceUsable(const Shared<StormByte::System::Device>& device) const noexcept override {
				if (m_policy == DevicePolicy::Fake)
					return static_cast<bool>(device);
				return BufferedLocationReader::OriginDeviceUsable(device);
			}

			/**
			 * @brief Open without touching a real origin.
			 * @return Ok.
			 */
			Result OriginOpen() override {
				SetState(State::Idle);
				return { Status::Ok, 0 };
			}

			/**
			 * @brief Close without touching a real origin.
			 * @return Ok.
			 */
			Result OriginClose() override {
				SetState(State::Unavailable);
				return { Status::Ok, 0 };
			}

			/**
			 * @brief Report an immediate end of stream.
			 * @return End.
			 */
			Result OriginPull(StormByte::ByteSize, FIFO&) override {
				return { Status::End, 0 };
			}

			/**
			 * @brief Accept a seek without moving a real origin.
			 * @return Ok.
			 */
			Result OriginSeek(std::ptrdiff_t, Position) override {
				return { Status::Ok, 0 };
			}

			/**
			 * @brief Report a known empty origin.
			 * @return Zero.
			 */
			StormByte::Safe::Optional<StormByte::ByteSize> OriginSize() const noexcept override {
				return StormByte::ByteSize{0};
			}

		private:
			DevicePolicy m_policy;	///< How this leaf reports its origin.
	};

	/**
	 * @brief Location writer whose origin is a fake device.
	 */
	class FakeWriter final: public BufferedLocationWriter {
		public:
			/**
			 * @brief Construct a writer for the selected device policy.
			 * @param policy Device reporting policy.
			 */
			explicit FakeWriter(const DevicePolicy policy):
				BufferedLocationWriter(StormByte::Safe::String(std::string_view("nic://eth0")),
					Location::Remote, Parameters{}),
				m_policy(policy) {}

		protected:
			/**
			 * @brief Report the fake origin, or an empty owner.
			 * @return Shared device.
			 */
			Shared<StormByte::System::Device> OriginDevice() const override {
				if (m_policy == DevicePolicy::Empty)
					return {};
				return Shared<StormByte::System::Device>::MakePointer<FakeDevice>(std::string_view("nic://eth0"));
			}

			/**
			 * @brief Accept a present fake device when the policy says so.
			 * @param device Reported origin.
			 * @return Whether setup may use the device window.
			 */
			bool OriginDeviceUsable(const Shared<StormByte::System::Device>& device) const noexcept override {
				if (m_policy == DevicePolicy::Fake)
					return static_cast<bool>(device);
				return BufferedLocationWriter::OriginDeviceUsable(device);
			}

			/**
			 * @brief Open without touching a real origin.
			 * @return Ok.
			 */
			Result OriginOpen() override {
				SetState(State::Idle);
				return { Status::Ok, 0 };
			}

			/**
			 * @brief Close without touching a real origin.
			 * @return Ok.
			 */
			Result OriginClose() override {
				SetState(State::Unavailable);
				return { Status::Ok, 0 };
			}

			/**
			 * @brief Accept a push without storing it.
			 * @param data Bytes offered by the buffer.
			 * @return Ok with the offered length.
			 */
			Result OriginPush(std::span<const std::byte> data) override {
				return { Status::Ok, StormByte::ByteSize{data.size()} };
			}

			/**
			 * @brief Accept a flush.
			 * @return Ok.
			 */
			Result OriginFlush() override {
				return { Status::Ok, 0 };
			}

			/**
			 * @brief Accept a truncate.
			 * @return Ok.
			 */
			Result OriginTruncate() override {
				return { Status::Ok, 0 };
			}

			/**
			 * @brief Accept a seek.
			 * @return Ok.
			 */
			Result OriginSeek(StormByte::ByteSize) override {
				return { Status::Ok, 0 };
			}

			/**
			 * @brief Report a known empty origin.
			 * @return Zero.
			 */
			StormByte::ByteSize OriginSize() const noexcept override {
				return StormByte::ByteSize{0};
			}

		private:
			DevicePolicy m_policy;	///< How this leaf reports its origin.
	};

	/**
	 * @brief Which reader hook throws a foreign exception.
	 */
	enum class ThrowingHook {
		Open,	///< OriginOpen throws.
		Pull,	///< OriginPull throws.
		Device	///< OriginDevice throws.
	};

	/**
	 * @brief Location reader that throws from one origin hook.
	 */
	class ThrowingReader final: public BufferedLocationReader {
		public:
			/**
			 * @brief Construct a reader that fails in the selected hook.
			 * @param hook Hook that throws.
			 */
			explicit ThrowingReader(const ThrowingHook hook):
				BufferedLocationReader(StormByte::Safe::String(std::string_view("test://reader")),
					Location::Remote, Parameters{}),
				m_hook(hook) {}

		protected:
			/**
			 * @brief Throw when the device hook is selected.
			 * @return Empty owner otherwise.
			 */
			Shared<StormByte::System::Device> OriginDevice() const override {
				if (m_hook == ThrowingHook::Device)
					throw std::runtime_error("device hook failed");
				return {};
			}

			/**
			 * @brief Throw when the open hook is selected.
			 * @return Ok otherwise.
			 */
			Result OriginOpen() override {
				if (m_hook == ThrowingHook::Open)
					throw std::runtime_error("open hook failed");
				SetState(State::Idle);
				return { Status::Ok, 0 };
			}

			/**
			 * @brief Close without touching a real origin.
			 * @return Ok.
			 */
			Result OriginClose() override {
				SetState(State::Unavailable);
				return { Status::Ok, 0 };
			}

			/**
			 * @brief Throw when the pull hook is selected.
			 * @return End otherwise.
			 */
			Result OriginPull(StormByte::ByteSize, FIFO&) override {
				if (m_hook == ThrowingHook::Pull)
					throw std::runtime_error("pull hook failed");
				return { Status::End, 0 };
			}

			/**
			 * @brief Accept a seek.
			 * @return Ok.
			 */
			Result OriginSeek(std::ptrdiff_t, Position) override {
				return { Status::Ok, 0 };
			}

			/**
			 * @brief Report an unknown origin size.
			 * @return Empty optional.
			 */
			StormByte::Safe::Optional<StormByte::ByteSize> OriginSize() const noexcept override {
				return {};
			}

		private:
			ThrowingHook m_hook;	///< Hook that throws a foreign exception.
	};

	std::filesystem::path TempFile(const char* name) {
		return std::filesystem::temp_directory_path() / name;
	}

	StormByte::Safe::String Loc(const std::filesystem::path& path) {
#ifdef WINDOWS
		return StormByte::Safe::String(StormByte::Safe::WString(std::wstring_view(path.wstring())));
#else
		return StormByte::Safe::String(std::string_view(path.string()));
#endif
	}
}

// -------------------
// Device
// -------------------

int test_derived_device_survives_origin_device() {
	FakeReader reader(DevicePolicy::Fake);
	const auto device = reader.Device();
	ASSERT_TRUE(static_cast<bool>(device));
	ASSERT_NOT_NULL(dynamic_cast<const FakeDevice*>(device.get()));
	ASSERT_EQUAL(kFakeRead, device->Window().read);
	ASSERT_EQUAL(kFakeWrite, device->Window().write);
	ASSERT_EQUAL(kFakeReadBps, device->Throughput().read_bps);
	ASSERT_EQUAL(kFakeWriteBps, device->Throughput().write_bps);
	ASSERT_TRUE(device.use_count() >= 1);
	RETURN_TEST(0);
}

int test_file_reader_keeps_probe_behaviour() {
	const auto path = TempFile("stormbyte_buffer_device_reader.bin");
	{
		std::ofstream out(path, std::ios::binary | std::ios::trunc);
		out << "stormbyte";
	}
	BufferedFileReader reader(Loc(path));
	const auto device = reader.Device();
	ASSERT_TRUE(static_cast<bool>(device));
	ASSERT_NULL(dynamic_cast<const FakeDevice*>(device.get()));
	ASSERT_TRUE(static_cast<bool>(*device));
	const Device probe{Loc(path)};
	ASSERT_TRUE(reader.Open());
	ASSERT_EQUAL(probe.Window().read, reader.ReadAhead());
	ASSERT_EQUAL(kDefaultMaxMemory, reader.MaxMemory());
	static_cast<void>(reader.Close());
	std::filesystem::remove(path);
	RETURN_TEST(0);
}

int test_file_writer_keeps_probe_behaviour() {
	const auto path = TempFile("stormbyte_buffer_device_writer.bin");
	std::filesystem::remove(path);
	BufferedFileWriter writer(Loc(path));
	const auto device = writer.Device();
	ASSERT_TRUE(static_cast<bool>(device));
	ASSERT_NULL(dynamic_cast<const FakeDevice*>(device.get()));
	ASSERT_TRUE(writer.Open());
	const Device probe{Loc(path)};
	ASSERT_EQUAL(probe.Window().write, writer.WriteChunk());
	ASSERT_EQUAL(kDefaultBackPressure, writer.BackPressure());
	ASSERT_EQUAL(kDefaultMaxMemory, writer.MaxMemory());
	static_cast<void>(writer.Close());
	std::filesystem::remove(path);
	RETURN_TEST(0);
}

int test_reader_setup_falls_back_on_unusable_device() {
	FakeReader strict(DevicePolicy::FakeStrict);
	ASSERT_TRUE(strict.Open());
	ASSERT_EQUAL(StormByte::ByteSize{0}, strict.ReadAhead());
	static_cast<void>(strict.Close());
	FakeReader empty(DevicePolicy::Empty);
	ASSERT_FALSE(static_cast<bool>(empty.Device()));
	ASSERT_TRUE(empty.Open());
	ASSERT_EQUAL(StormByte::ByteSize{0}, empty.ReadAhead());
	static_cast<void>(empty.Close());
	RETURN_TEST(0);
}

int test_reader_setup_uses_overridden_window() {
	FakeReader reader(DevicePolicy::Fake);
	ASSERT_TRUE(reader.Open());
	ASSERT_EQUAL(kFakeRead, reader.ReadAhead());
	static_cast<void>(reader.Close());
	RETURN_TEST(0);
}

int test_writer_setup_falls_back_on_unusable_device() {
	FakeWriter strict(DevicePolicy::FakeStrict);
	ASSERT_TRUE(strict.Open());
	ASSERT_EQUAL(StormByte::ByteSize{0}, strict.WriteChunk());
	ASSERT_EQUAL(kDefaultBackPressure, strict.BackPressure());
	ASSERT_EQUAL(kDefaultMaxMemory, strict.MaxMemory());
	static_cast<void>(strict.Close());
	FakeWriter empty(DevicePolicy::Empty);
	ASSERT_FALSE(static_cast<bool>(empty.Device()));
	ASSERT_TRUE(empty.Open());
	ASSERT_EQUAL(StormByte::ByteSize{0}, empty.WriteChunk());
	static_cast<void>(empty.Close());
	RETURN_TEST(0);
}

int test_writer_setup_uses_overridden_window() {
	FakeWriter writer(DevicePolicy::Fake);
	const auto device = writer.Device();
	ASSERT_NOT_NULL(dynamic_cast<const FakeDevice*>(device.get()));
	ASSERT_TRUE(writer.Open());
	ASSERT_EQUAL(kFakeWrite, writer.WriteChunk());
	ASSERT_EQUAL(kDefaultBackPressure, writer.BackPressure());
	ASSERT_EQUAL(kDefaultMaxMemory, writer.MaxMemory());
	static_cast<void>(writer.Close());
	RETURN_TEST(0);
}

// -------------------
// Failure
// -------------------

int test_origin_hook_exceptions_become_domain_failures() {
	ThrowingReader open_reader(ThrowingHook::Open);
	ASSERT_FALSE(open_reader.Open());
	ThrowingReader pull_reader(ThrowingHook::Pull);
	ASSERT_TRUE(pull_reader.Open());
	FIFO data;
	const Result pulled = pull_reader.Read(StormByte::ByteSize{1}, data);
	ASSERT_EQUAL(Status::Error, pulled.status);
	static_cast<void>(pull_reader.Close());
	ThrowingReader device_reader(ThrowingHook::Device);
	bool translated = false;
	try {
		static_cast<void>(device_reader.Device());
	}
	catch (const StormByte::Buffer::Exception&) {
		translated = true;
	}
	ASSERT_TRUE(translated);
	RETURN_TEST(0);
}

// -------------------
// Parameters
// -------------------

int test_optional_size_distinguishes_unknown_from_empty() {
	FakeReader empty_reader(DevicePolicy::Empty);
	ThrowingReader unknown_reader(ThrowingHook::Pull);
	const auto empty_size = empty_reader.Size();
	const auto unknown_size = unknown_reader.Size();
	static_assert(StormByte::Type::SameAs<decltype(empty_reader.Size()),
		StormByte::Safe::Optional<StormByte::ByteSize>>);
	ASSERT_TRUE(empty_size.has_value());
	ASSERT_EQUAL(StormByte::ByteSize{0}, *empty_size);
	ASSERT_FALSE(unknown_size.has_value());
	RETURN_TEST(0);
}

int test_parameter_special_operations_preserve_knobs() {
	using namespace StormByte::Buffer::IO;
	static_assert(StormByte::Type::SafeValue<std::chrono::milliseconds::rep>);
	static_assert(!noexcept(BufferedFileReader::Parameters{}));
	static_assert(!noexcept(BufferedFileWriter::Parameters{}));
	static_assert(!noexcept(BufferedFileReader::Parameters(std::declval<const BufferedFileReader::Parameters&>())));
	static_assert(!noexcept(BufferedFileWriter::Parameters(std::declval<const BufferedFileWriter::Parameters&>())));
	static_assert(!noexcept(std::declval<BufferedFileReader::Parameters&>() = std::declval<const BufferedFileReader::Parameters&>()));
	static_assert(!noexcept(std::declval<BufferedFileWriter::Parameters&>() = std::declval<const BufferedFileWriter::Parameters&>()));
	static_assert(noexcept(BufferedFileReader::Parameters(std::declval<BufferedFileReader::Parameters&&>())));
	static_assert(noexcept(BufferedFileWriter::Parameters(std::declval<BufferedFileWriter::Parameters&&>())));
	BufferedFileReader::Parameters reader(ReadAhead(0), MaxWait(std::chrono::milliseconds{7}));
	static_assert(StormByte::Type::SameAs<decltype(reader.ReadAhead()),
		const StormByte::Safe::Optional<StormByte::ByteSize>&>);
	static_assert(StormByte::Type::SameAs<decltype(reader.MaxMemory()),
		const StormByte::Safe::Optional<StormByte::ByteSize>&>);
	static_assert(StormByte::Type::SameAs<decltype(reader.MaxWait()),
		const StormByte::Safe::Optional<std::chrono::milliseconds::rep>&>);
	BufferedFileReader::Parameters reader_copy(reader);
	BufferedFileReader::Parameters reader_move(std::move(reader_copy));
	ASSERT_FALSE(reader_copy.ReadAhead().has_value());
	ASSERT_FALSE(reader_copy.MaxWait().has_value());
	reader_copy = reader_move;
	reader = std::move(reader_copy);
	ASSERT_TRUE(reader.ReadAhead().has_value());
	ASSERT_EQUAL(StormByte::ByteSize{0}, *reader.ReadAhead());
	ASSERT_FALSE(reader.MaxMemory().has_value());
	ASSERT_EQUAL(std::chrono::milliseconds::rep{7}, *reader.MaxWait());

	BufferedFileWriter::Parameters writer(WriteChunk(0), BackPressure(0), MaxMemory(32));
	static_assert(StormByte::Type::SameAs<decltype(writer.WriteChunk()),
		const StormByte::Safe::Optional<StormByte::ByteSize>&>);
	static_assert(StormByte::Type::SameAs<decltype(writer.BackPressure()),
		const StormByte::Safe::Optional<std::size_t>&>);
	static_assert(StormByte::Type::SameAs<decltype(writer.MaxMemory()),
		const StormByte::Safe::Optional<StormByte::ByteSize>&>);
	static_assert(StormByte::Type::SameAs<decltype(writer.MaxWait()),
		const StormByte::Safe::Optional<std::chrono::milliseconds::rep>&>);
	BufferedFileWriter::Parameters writer_copy(writer);
	BufferedFileWriter::Parameters writer_move(std::move(writer_copy));
	ASSERT_FALSE(writer_copy.WriteChunk().has_value());
	ASSERT_FALSE(writer_copy.BackPressure().has_value());
	ASSERT_FALSE(writer_copy.MaxMemory().has_value());
	writer_copy = writer_move;
	writer = std::move(writer_copy);
	ASSERT_TRUE(writer.WriteChunk().has_value());
	ASSERT_EQUAL(StormByte::ByteSize{0}, *writer.WriteChunk());
	ASSERT_TRUE(writer.BackPressure().has_value());
	ASSERT_EQUAL(std::size_t{0}, *writer.BackPressure());
	ASSERT_EQUAL(StormByte::ByteSize{32}, *writer.MaxMemory());
	ASSERT_FALSE(writer.MaxWait().has_value());

	const BufferedFileReader::Parameters omitted_reader;
	const BufferedFileWriter::Parameters omitted_writer;
	ASSERT_FALSE(omitted_reader.MaxWait().has_value());
	ASSERT_FALSE(omitted_writer.MaxWait().has_value());
	for (const auto wait: { std::chrono::milliseconds{0}, std::chrono::milliseconds{-7},
			std::chrono::milliseconds::min(), std::chrono::milliseconds::max() }) {
		const BufferedFileReader::Parameters signed_reader(MaxWait{wait});
		const BufferedFileWriter::Parameters signed_writer(MaxWait{wait});
		const auto signed_reader_copy = signed_reader;
		const auto signed_writer_copy = signed_writer;
		ASSERT_TRUE(signed_reader_copy.MaxWait().has_value());
		ASSERT_TRUE(signed_writer_copy.MaxWait().has_value());
		ASSERT_EQUAL(wait.count(), signed_reader_copy.MaxWait().value());
		ASSERT_EQUAL(wait.count(), signed_writer_copy.MaxWait().value());
		ASSERT_EQUAL(wait, (std::chrono::milliseconds{signed_reader_copy.MaxWait().value_or(1)}));
		ASSERT_EQUAL(wait, (std::chrono::milliseconds{signed_writer_copy.MaxWait().value_or(1)}));
	}
	RETURN_TEST(0);
}

int main() {
	int result = 0;

	// -------------------
	// Device
	// -------------------
	result += test_derived_device_survives_origin_device();
	result += test_file_reader_keeps_probe_behaviour();
	result += test_file_writer_keeps_probe_behaviour();
	result += test_reader_setup_falls_back_on_unusable_device();
	result += test_reader_setup_uses_overridden_window();
	result += test_writer_setup_falls_back_on_unusable_device();
	result += test_writer_setup_uses_overridden_window();

	// -------------------
	// Failure
	// -------------------
	result += test_origin_hook_exceptions_become_domain_failures();

	// -------------------
	// Parameters
	// -------------------
	result += test_optional_size_distinguishes_unknown_from_empty();
	result += test_parameter_special_operations_preserve_knobs();

	if (result == 0)
		std::cout << "All tests passed!" << std::endl;
	else
		std::cout << result << " tests failed." << std::endl;
	return result;
}
