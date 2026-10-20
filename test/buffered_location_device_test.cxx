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
#include <StormByte/safe/binary.hxx>
#include <StormByte/safe/pointers.hxx>
#include <StormByte/system/device.hxx>
#include <StormByte/test_handlers.h>

#include <algorithm>
#include <array>
#include <atomic>
#include <chrono>
#include <cstddef>
#include <filesystem>
#include <fstream>
#include <iostream>
#include <optional>
#include <span>
#include <stdexcept>
#include <string_view>
#include <thread>
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

	class TrackingWriter final: public BufferedLocationWriter {
		public:
			explicit TrackingWriter(std::atomic<int>& late_calls, const bool fail_push = true):
				BufferedLocationWriter(StormByte::Safe::String(std::string_view("test://writer")),
					Location::Remote, Parameters{
						StormByte::Buffer::IO::WriteChunk(StormByte::ByteSize{4}),
						StormByte::Buffer::IO::BackPressure(2),
						StormByte::Buffer::IO::MaxMemory(StormByte::ByteSize{0})}),
				m_late_calls(late_calls),
				m_fail_push(fail_push) {}

			~TrackingWriter() noexcept override {
				static_cast<void>(Close());
				m_closed.store(true);
				std::this_thread::sleep_for(std::chrono::milliseconds(20));
			}

			void MarkClosed() noexcept {
				m_closed.store(true);
			}

		protected:
			Result OriginOpen() override {
				NoteLateCall();
				SetState(State::Idle);
				return { Status::Ok, 0 };
			}

			Result OriginClose() override {
				SetState(State::Unavailable);
				return { Status::Ok, 0 };
			}

			Result OriginPush(const std::span<const std::byte> data) override {
				NoteLateCall();
				return m_fail_push
					? Result{ Status::Error, 0 }
					: Result{ Status::Ok, StormByte::ByteSize{data.size()} };
			}

			Result OriginFlush() override {
				NoteLateCall();
				return { Status::Ok, 0 };
			}

			Result OriginTruncate() override {
				NoteLateCall();
				return { Status::Ok, 0 };
			}

			Result OriginSeek(StormByte::ByteSize) override {
				NoteLateCall();
				return { Status::Ok, 0 };
			}

			StormByte::ByteSize OriginSize() const noexcept override {
				NoteLateCall();
				return StormByte::ByteSize{0};
			}

			Shared<StormByte::System::Device> OriginDevice() const override {
				NoteLateCall();
				return {};
			}

		private:
			void NoteLateCall() const noexcept {
				if (m_closed.load())
					++m_late_calls;
			}

			std::atomic<bool> m_closed{false};
			std::atomic<int>& m_late_calls;
			bool m_fail_push;
	};

	class TrackingReader final: public BufferedLocationReader {
		public:
			explicit TrackingReader(std::atomic<int>& late_calls, const bool prefetch = true):
				BufferedLocationReader(StormByte::Safe::String(std::string_view("test://reader")),
					Location::Remote, Parameters{
						StormByte::Buffer::IO::ReadAhead(StormByte::ByteSize{prefetch ? 4u : 0u}),
						StormByte::Buffer::IO::MaxMemory(StormByte::ByteSize{8})}),
				m_late_calls(late_calls) {}

			~TrackingReader() noexcept override {
				static_cast<void>(Close());
				m_closed.store(true);
				std::this_thread::sleep_for(std::chrono::milliseconds(20));
			}

			void MarkClosed() noexcept {
				m_closed.store(true);
			}

			void PausePull(const bool pause) noexcept {
				m_pause_pull.store(pause);
			}

			bool PullStarted() const noexcept {
				return m_pull_started.load();
			}

		protected:
			Shared<StormByte::System::Device> OriginDevice() const override {
				NoteLateCall();
				return {};
			}

			Result OriginOpen() override {
				NoteLateCall();
				SetState(State::Idle);
				return { Status::Ok, 0 };
			}

			Result OriginClose() override {
				SetState(State::Unavailable);
				return { Status::Ok, 0 };
			}

			Result OriginPull(const StormByte::ByteSize n, FIFO& dest) override {
				NoteLateCall();
				m_pull_started.store(true);
				while (m_pause_pull.load())
					std::this_thread::yield();
				NoteLateCall();
				const StormByte::ByteSize count = std::min(n, StormByte::ByteSize{4});
				StormByte::Safe::Binary data(count);
				static_cast<void>(dest.Write(StormByte::ByteSize{data.size()}, std::move(data)));
				return { Status::Ok, count };
			}

			Result OriginSeek(std::ptrdiff_t, Position) override {
				NoteLateCall();
				return { Status::Ok, 0 };
			}

			StormByte::Safe::Optional<StormByte::ByteSize> OriginSize() const noexcept override {
				NoteLateCall();
				return {};
			}

		private:
			void NoteLateCall() const noexcept {
				if (m_closed.load())
					++m_late_calls;
			}

			std::atomic<bool> m_closed{false};
			std::atomic<int>& m_late_calls;
			std::atomic<bool> m_pause_pull{false};
			std::atomic<bool> m_pull_started{false};
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

int test_writer_does_not_call_origin_during_derived_destruction() {
	std::atomic<int> late_calls{0};
	const std::array<std::byte, 12> payload{};
	for (int iteration = 0; iteration < 100; ++iteration) {
		{
			TrackingWriter writer(late_calls);
			ASSERT_TRUE(writer.Open());
			ASSERT_EQUAL(Status::TryAgain, writer.Write(std::span<const std::byte>(payload)).status);
			ASSERT_EQUAL(Status::Ok, writer.Flush().status);
			std::size_t offset = 0;
			while (offset < payload.size()) {
				const std::size_t count = std::min<std::size_t>(8, payload.size() - offset);
				const Result written = writer.Write(
					std::span<const std::byte>(payload).subspan(offset, count));
				if (written.status == Status::TryAgain) {
					const Result flushed = writer.Flush();
					ASSERT_TRUE(flushed.status == Status::Ok || flushed.status == Status::Error);
					if (flushed.status == Status::Error)
						break;
					continue;
				}
				ASSERT_TRUE(written.status == Status::Ok || written.status == Status::Failed);
				if (written.status == Status::Failed)
					break;
				ASSERT_EQUAL(StormByte::ByteSize{count}, written.count);
				offset += static_cast<std::size_t>(written.count);
			}
			ASSERT_EQUAL(Status::Error, writer.Flush().status);
		}
		ASSERT_EQUAL(0, late_calls.load());
	}
	RETURN_TEST(0);
}

int test_writer_close_unblocks_concurrent_flush() {
	std::atomic<int> late_calls{0};
	for (int iteration = 0; iteration < 32; ++iteration) {
		std::atomic<bool> flush_started{false};
		Status flush_status = Status::Ok;
		bool write_status_valid = true;
		const bool fail_push = iteration % 2 == 0;
		TrackingWriter writer(late_calls, fail_push);
		ASSERT_TRUE(writer.Open());
		std::thread io_thread([&] {
			const std::array<std::byte, 8> payload{};
			do {
				const Result written = writer.Write(std::span<const std::byte>(payload));
				write_status_valid = write_status_valid && (written.status == Status::Ok
					|| written.status == Status::TryAgain || written.status == Status::Failed);
				flush_started.store(true);
				flush_status = writer.Flush().status;
			} while (flush_status == Status::Ok);
		});
		while (!flush_started.load())
			std::this_thread::yield();
		static_cast<void>(writer.Close());
		writer.MarkClosed();
		io_thread.join();
		ASSERT_TRUE(write_status_valid);
		ASSERT_TRUE(flush_status == Status::Error || flush_status == Status::Failed);
		if (!fail_push)
			ASSERT_EQUAL(Status::Failed, flush_status);
		ASSERT_EQUAL(Status::Failed, writer.Flush().status);
		ASSERT_EQUAL(0, late_calls.load());
	}
	ASSERT_EQUAL(0, late_calls.load());
	RETURN_TEST(0);
}

int test_reader_serializes_prefetch_with_close() {
	std::atomic<int> late_calls{0};
	for (int iteration = 0; iteration < 32; ++iteration) {
		{
			TrackingReader reader(late_calls);
			ASSERT_TRUE(reader.Open());
			FIFO data;
			ASSERT_EQUAL(Status::Ok, reader.Read(StormByte::ByteSize{1}, data).status);
			ASSERT_EQUAL(Status::Ok, reader.Close().status);
			reader.MarkClosed();
		}
		ASSERT_EQUAL(0, late_calls.load());
	}

	std::atomic<bool> close_returned{false};
	{
		TrackingReader reader(late_calls);
		ASSERT_TRUE(reader.Open());
		std::atomic<bool> read_started{false};
		std::thread read_thread([&] {
			while (!close_returned.load()) {
				FIFO data;
				static_cast<void>(reader.Read(StormByte::ByteSize{1}, data));
				read_started.store(true);
			}
		});
		while (!read_started.load())
			std::this_thread::yield();
		const Result closed = reader.Close();
		reader.MarkClosed();
		close_returned.store(true);
		read_thread.join();
		ASSERT_EQUAL(Status::Ok, closed.status);
	}
	ASSERT_EQUAL(0, late_calls.load());
	RETURN_TEST(0);
}

int test_reader_close_waits_for_active_origin_pull() {
	std::atomic<int> late_calls{0};
	TrackingReader reader(late_calls, false);
	ASSERT_TRUE(reader.Open());
	reader.PausePull(true);
	std::thread read_thread([&] {
		FIFO data;
		static_cast<void>(reader.Read(StormByte::ByteSize{1}, data));
	});
	while (!reader.PullStarted())
		std::this_thread::yield();
	std::atomic<bool> close_started{false};
	std::atomic<bool> close_returned{false};
	Result closed;
	std::thread close_thread([&] {
		close_started.store(true);
		closed = reader.Close();
		reader.MarkClosed();
		close_returned.store(true);
	});
	while (!close_started.load())
		std::this_thread::yield();
	std::this_thread::sleep_for(std::chrono::milliseconds(20));
	const bool closed_during_pull = close_returned.load();
	reader.PausePull(false);
	read_thread.join();
	close_thread.join();
	ASSERT_FALSE(closed_during_pull);
	ASSERT_EQUAL(Status::Ok, closed.status);
	ASSERT_EQUAL(0, late_calls.load());
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
	result += test_writer_does_not_call_origin_during_derived_destruction();
	result += test_writer_close_unblocks_concurrent_flush();
	result += test_reader_serializes_prefetch_with_close();
	result += test_reader_close_waits_for_active_origin_pull();

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
