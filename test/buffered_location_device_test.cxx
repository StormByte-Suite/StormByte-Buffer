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

#include <StormByte/buffer/fifo.hxx>
#include <StormByte/buffer/exception.hxx>
#include <StormByte/buffer/io/buffered_file_reader.hxx>
#include <StormByte/buffer/io/buffered_file_writer.hxx>
#include <StormByte/buffer/io/buffered_location_reader.hxx>
#include <StormByte/buffer/io/buffered_location_writer.hxx>
#include <StormByte/safe/pointers.hxx>
#include <StormByte/system/device.hxx>
#include <StormByte/test_handlers.h>

#include <cstddef>
#include <filesystem>
#include <fstream>
#include <iostream>
#include <optional>
#include <span>
#include <stdexcept>
#include <string_view>
#include <utility>

using StormByte::Safe::Shared;
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

	// Stands in for a Network NIC device: the accessor is not a filesystem path.
	class FakeDevice final: public Device {
		public:
			explicit FakeDevice(std::string_view accessor) noexcept: Device(accessor) {}

			struct Throughput Throughput() const noexcept override {
				return { kFakeReadBps, kFakeWriteBps };
			}

			struct Window Window() const noexcept override {
				return { kFakeRead, kFakeWrite };
			}
	};

	enum class DevicePolicy {
		Fake,		// Non-path device, leaf declares it usable.
		FakeStrict,	// Non-path device, default usability (real path probe fails).
		Empty		// Leaf hands out an empty owner.
	};

	class FakeReader final: public BufferedLocationReader {
		public:
			explicit FakeReader(const DevicePolicy policy):
				BufferedLocationReader(StormByte::Safe::String(std::string_view("nic://eth0")),
					Location::Remote, Parameters{}),
				m_policy(policy) {}

		protected:
			// Device names the inherited accessor inside this scope, so qualify the type.
			Shared<StormByte::System::Device> OriginDevice() const override {
				if (m_policy == DevicePolicy::Empty)
					return {};
				return Shared<StormByte::System::Device>::MakePointer<FakeDevice>(std::string_view("nic://eth0"));
			}

			bool OriginDeviceUsable(const Shared<StormByte::System::Device>& device) const noexcept override {
				if (m_policy == DevicePolicy::Fake)
					return static_cast<bool>(device);
				return BufferedLocationReader::OriginDeviceUsable(device);
			}

			Result OriginOpen() override {
				SetState(State::Idle);
				return { Status::Ok, 0 };
			}

			Result OriginClose() override {
				SetState(State::Unavailable);
				return { Status::Ok, 0 };
			}

			Result OriginPull(StormByte::ByteSize, FIFO&) override {
				return { Status::End, 0 };
			}

			Result OriginSeek(std::ptrdiff_t, Position) override {
				return { Status::Ok, 0 };
			}

			StormByte::Safe::Optional<StormByte::ByteSize> OriginSize() const noexcept override {
				return StormByte::ByteSize{0};
			}

		private:
			DevicePolicy m_policy;
	};

	class FakeWriter final: public BufferedLocationWriter {
		public:
			explicit FakeWriter(const DevicePolicy policy):
				BufferedLocationWriter(StormByte::Safe::String(std::string_view("nic://eth0")),
					Location::Remote, Parameters{}),
				m_policy(policy) {}

		protected:
			Shared<StormByte::System::Device> OriginDevice() const override {
				if (m_policy == DevicePolicy::Empty)
					return {};
				return Shared<StormByte::System::Device>::MakePointer<FakeDevice>(std::string_view("nic://eth0"));
			}

			bool OriginDeviceUsable(const Shared<StormByte::System::Device>& device) const noexcept override {
				if (m_policy == DevicePolicy::Fake)
					return static_cast<bool>(device);
				return BufferedLocationWriter::OriginDeviceUsable(device);
			}

			Result OriginOpen() override {
				SetState(State::Idle);
				return { Status::Ok, 0 };
			}

			Result OriginClose() override {
				SetState(State::Unavailable);
				return { Status::Ok, 0 };
			}

			Result OriginPush(std::span<const std::byte> data) override {
				return { Status::Ok, StormByte::ByteSize{data.size()} };
			}

			Result OriginFlush() override {
				return { Status::Ok, 0 };
			}

			Result OriginTruncate() override {
				return { Status::Ok, 0 };
			}

			Result OriginSeek(StormByte::ByteSize) override {
				return { Status::Ok, 0 };
			}

			StormByte::ByteSize OriginSize() const noexcept override {
				return StormByte::ByteSize{0};
			}

		private:
			DevicePolicy m_policy;
	};

	enum class ThrowingHook {
		Open,
		Pull,
		Device
	};

	class ThrowingReader final: public BufferedLocationReader {
		public:
			explicit ThrowingReader(const ThrowingHook hook):
				BufferedLocationReader(StormByte::Safe::String(std::string_view("test://reader")),
					Location::Remote, Parameters{}),
				m_hook(hook) {}

		protected:
			Shared<StormByte::System::Device> OriginDevice() const override {
				if (m_hook == ThrowingHook::Device)
					throw std::runtime_error("device hook failed");
				return {};
			}

			Result OriginOpen() override {
				if (m_hook == ThrowingHook::Open)
					throw std::runtime_error("open hook failed");
				SetState(State::Idle);
				return { Status::Ok, 0 };
			}

			Result OriginClose() override {
				SetState(State::Unavailable);
				return { Status::Ok, 0 };
			}

			Result OriginPull(StormByte::ByteSize, FIFO&) override {
				if (m_hook == ThrowingHook::Pull)
					throw std::runtime_error("pull hook failed");
				return { Status::End, 0 };
			}

			Result OriginSeek(std::ptrdiff_t, Position) override {
				return { Status::Ok, 0 };
			}

			StormByte::Safe::Optional<StormByte::ByteSize> OriginSize() const noexcept override {
				return {};
			}

		private:
			ThrowingHook m_hook;
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

	int DerivedDeviceSurvivesOriginDevice() {
		const char* fn = "DerivedDeviceSurvivesOriginDevice";
		FakeReader reader(DevicePolicy::Fake);
		const auto device = reader.Device();
		ASSERT_TRUE(fn, static_cast<bool>(device));
		ASSERT_TRUE(fn, dynamic_cast<const FakeDevice*>(device.get()) != nullptr);
		ASSERT_EQUAL(fn, kFakeRead, device->Window().read);
		ASSERT_EQUAL(fn, kFakeWrite, device->Window().write);
		ASSERT_EQUAL(fn, kFakeReadBps, device->Throughput().read_bps);
		ASSERT_EQUAL(fn, kFakeWriteBps, device->Throughput().write_bps);
		// The caller keeps the object alive past the accessor.
		ASSERT_TRUE(fn, device.use_count() >= 1);
		RETURN_TEST(fn, 0);
	}

	int ReaderSetupUsesOverriddenWindow() {
		const char* fn = "ReaderSetupUsesOverriddenWindow";
		FakeReader reader(DevicePolicy::Fake);
		ASSERT_TRUE(fn, reader.Open());
		ASSERT_EQUAL(fn, kFakeRead, reader.ReadAhead());
		static_cast<void>(reader.Close());
		RETURN_TEST(fn, 0);
	}

	int ReaderSetupFallsBackOnUnusableDevice() {
		const char* fn = "ReaderSetupFallsBackOnUnusableDevice";
		FakeReader strict(DevicePolicy::FakeStrict);
		ASSERT_TRUE(fn, strict.Open());
		ASSERT_EQUAL(fn, StormByte::ByteSize{0}, strict.ReadAhead());
		static_cast<void>(strict.Close());

		FakeReader empty(DevicePolicy::Empty);
		ASSERT_FALSE(fn, static_cast<bool>(empty.Device()));
		ASSERT_TRUE(fn, empty.Open());
		ASSERT_EQUAL(fn, StormByte::ByteSize{0}, empty.ReadAhead());
		static_cast<void>(empty.Close());
		RETURN_TEST(fn, 0);
	}

	int WriterSetupUsesOverriddenWindow() {
		const char* fn = "WriterSetupUsesOverriddenWindow";
		FakeWriter writer(DevicePolicy::Fake);
		const auto device = writer.Device();
		ASSERT_TRUE(fn, dynamic_cast<const FakeDevice*>(device.get()) != nullptr);
		ASSERT_TRUE(fn, writer.Open());
		ASSERT_EQUAL(fn, kFakeWrite, writer.WriteChunk());
		ASSERT_EQUAL(fn, kDefaultBackPressure, writer.BackPressure());
		ASSERT_EQUAL(fn, kDefaultMaxMemory, writer.MaxMemory());
		static_cast<void>(writer.Close());
		RETURN_TEST(fn, 0);
	}

	int WriterSetupFallsBackOnUnusableDevice() {
		const char* fn = "WriterSetupFallsBackOnUnusableDevice";
		FakeWriter strict(DevicePolicy::FakeStrict);
		ASSERT_TRUE(fn, strict.Open());
		ASSERT_EQUAL(fn, StormByte::ByteSize{0}, strict.WriteChunk());
		ASSERT_EQUAL(fn, kDefaultBackPressure, strict.BackPressure());
		ASSERT_EQUAL(fn, kDefaultMaxMemory, strict.MaxMemory());
		static_cast<void>(strict.Close());

		FakeWriter empty(DevicePolicy::Empty);
		ASSERT_FALSE(fn, static_cast<bool>(empty.Device()));
		ASSERT_TRUE(fn, empty.Open());
		ASSERT_EQUAL(fn, StormByte::ByteSize{0}, empty.WriteChunk());
		static_cast<void>(empty.Close());
		RETURN_TEST(fn, 0);
	}

	int FileReaderKeepsProbeBehaviour() {
		const char* fn = "FileReaderKeepsProbeBehaviour";
		const auto path = TempFile("stormbyte_buffer_device_reader.bin");
		{
			std::ofstream out(path, std::ios::binary | std::ios::trunc);
			out << "stormbyte";
		}
		BufferedFileReader reader(Loc(path));
		const auto device = reader.Device();
		ASSERT_TRUE(fn, static_cast<bool>(device));
		// A file leaf keeps the exact base type; no leaf override is involved.
		ASSERT_TRUE(fn, dynamic_cast<const FakeDevice*>(device.get()) == nullptr);
		ASSERT_TRUE(fn, static_cast<bool>(*device));

		const Device probe{Loc(path)};
		ASSERT_TRUE(fn, reader.Open());
		ASSERT_EQUAL(fn, probe.Window().read, reader.ReadAhead());
		ASSERT_EQUAL(fn, kDefaultMaxMemory, reader.MaxMemory());
		static_cast<void>(reader.Close());
		std::filesystem::remove(path);
		RETURN_TEST(fn, 0);
	}

	int FileWriterKeepsProbeBehaviour() {
		const char* fn = "FileWriterKeepsProbeBehaviour";
		const auto path = TempFile("stormbyte_buffer_device_writer.bin");
		std::filesystem::remove(path);
		BufferedFileWriter writer(Loc(path));
		const auto device = writer.Device();
		ASSERT_TRUE(fn, static_cast<bool>(device));
		ASSERT_TRUE(fn, dynamic_cast<const FakeDevice*>(device.get()) == nullptr);

		ASSERT_TRUE(fn, writer.Open());
		const Device probe{Loc(path)};
		ASSERT_EQUAL(fn, probe.Window().write, writer.WriteChunk());
		ASSERT_EQUAL(fn, kDefaultBackPressure, writer.BackPressure());
		ASSERT_EQUAL(fn, kDefaultMaxMemory, writer.MaxMemory());
		static_cast<void>(writer.Close());
		std::filesystem::remove(path);
		RETURN_TEST(fn, 0);
	}

	/**
	 * @brief Reader size hooks preserve unknown and known-empty results.
	 * @return Zero on success.
	 */
	int OptionalSizeDistinguishesUnknownFromEmpty() {
		constexpr auto name = "OptionalSizeDistinguishesUnknownFromEmpty";
		FakeReader empty_reader(DevicePolicy::Empty);
		ThrowingReader unknown_reader(ThrowingHook::Pull);
		const auto empty_size = empty_reader.Size();
		const auto unknown_size = unknown_reader.Size();
		static_assert(StormByte::Type::SameAs<decltype(empty_reader.Size()),
			StormByte::Safe::Optional<StormByte::ByteSize>>);
		ASSERT_TRUE(name, empty_size.has_value());
		ASSERT_EQUAL(name, StormByte::ByteSize{0}, *empty_size);
		ASSERT_FALSE(name, unknown_size.has_value());
		RETURN_TEST(name, 0);
	}

	/**
	 * @brief Safe-owned parameter operations preserve absent knobs, explicit zero and signed waits.
	 * @return Zero on success.
	 */
	int ParameterSpecialOperationsPreserveKnobs() {
		constexpr auto name = "ParameterSpecialOperationsPreserveKnobs";
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
		ASSERT_FALSE(name, reader_copy.ReadAhead().has_value());
		ASSERT_FALSE(name, reader_copy.MaxWait().has_value());
		reader_copy = reader_move;
		reader = std::move(reader_copy);
		ASSERT_TRUE(name, reader.ReadAhead().has_value());
		ASSERT_EQUAL(name, StormByte::ByteSize{0}, *reader.ReadAhead());
		ASSERT_FALSE(name, reader.MaxMemory().has_value());
		ASSERT_EQUAL(name, std::chrono::milliseconds::rep{7}, *reader.MaxWait());

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
		ASSERT_FALSE(name, writer_copy.WriteChunk().has_value());
		ASSERT_FALSE(name, writer_copy.BackPressure().has_value());
		ASSERT_FALSE(name, writer_copy.MaxMemory().has_value());
		writer_copy = writer_move;
		writer = std::move(writer_copy);
		ASSERT_TRUE(name, writer.WriteChunk().has_value());
		ASSERT_EQUAL(name, StormByte::ByteSize{0}, *writer.WriteChunk());
		ASSERT_TRUE(name, writer.BackPressure().has_value());
		ASSERT_EQUAL(name, std::size_t{0}, *writer.BackPressure());
		ASSERT_EQUAL(name, StormByte::ByteSize{32}, *writer.MaxMemory());
		ASSERT_FALSE(name, writer.MaxWait().has_value());

		const BufferedFileReader::Parameters omitted_reader;
		const BufferedFileWriter::Parameters omitted_writer;
		ASSERT_FALSE(name, omitted_reader.MaxWait().has_value());
		ASSERT_FALSE(name, omitted_writer.MaxWait().has_value());
		for (const auto wait: { std::chrono::milliseconds{0}, std::chrono::milliseconds{-7},
				std::chrono::milliseconds::min(), std::chrono::milliseconds::max() }) {
			const BufferedFileReader::Parameters signed_reader(MaxWait{wait});
			const BufferedFileWriter::Parameters signed_writer(MaxWait{wait});
			const auto signed_reader_copy = signed_reader;
			const auto signed_writer_copy = signed_writer;
			ASSERT_TRUE(name, signed_reader_copy.MaxWait().has_value());
			ASSERT_TRUE(name, signed_writer_copy.MaxWait().has_value());
			ASSERT_EQUAL(name, wait.count(), signed_reader_copy.MaxWait().value());
			ASSERT_EQUAL(name, wait.count(), signed_writer_copy.MaxWait().value());
			ASSERT_EQUAL(name, wait, (std::chrono::milliseconds{signed_reader_copy.MaxWait().value_or(1)}));
			ASSERT_EQUAL(name, wait, (std::chrono::milliseconds{signed_writer_copy.MaxWait().value_or(1)}));
		}
		RETURN_TEST(name, 0);
	}

	int OriginHookExceptionsBecomeDomainFailures() {
		const char* fn = "OriginHookExceptionsBecomeDomainFailures";
		ThrowingReader open_reader(ThrowingHook::Open);
		ASSERT_FALSE(fn, open_reader.Open());

		ThrowingReader pull_reader(ThrowingHook::Pull);
		ASSERT_TRUE(fn, pull_reader.Open());
		FIFO data;
		const Result pulled = pull_reader.Read(StormByte::ByteSize{1}, data);
		ASSERT_EQUAL(fn, Status::Error, pulled.status);
		static_cast<void>(pull_reader.Close());

		ThrowingReader device_reader(ThrowingHook::Device);
		bool translated = false;
		try {
			static_cast<void>(device_reader.Device());
		}
		catch (const StormByte::Buffer::Exception&) {
			translated = true;
		}
		ASSERT_TRUE(fn, translated);
		RETURN_TEST(fn, 0);
	}
}

int main() {
	int result = 0;
	result += DerivedDeviceSurvivesOriginDevice();
	result += ReaderSetupUsesOverriddenWindow();
	result += ReaderSetupFallsBackOnUnusableDevice();
	result += WriterSetupUsesOverriddenWindow();
	result += WriterSetupFallsBackOnUnusableDevice();
	result += FileReaderKeepsProbeBehaviour();
	result += FileWriterKeepsProbeBehaviour();
	result += OptionalSizeDistinguishesUnknownFromEmpty();
	result += ParameterSpecialOperationsPreserveKnobs();
	result += OriginHookExceptionsBecomeDomainFailures();

	if (result == 0)
		std::cout << "All tests passed!" << std::endl;
	else
		std::cout << result << " tests failed." << std::endl;
	return result;
}
