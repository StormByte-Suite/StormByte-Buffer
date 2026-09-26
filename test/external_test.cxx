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

#include <StormByte/buffer/consumer.hxx>
#include <StormByte/buffer/external.hxx>
#include <StormByte/buffer/fifo.hxx>
#include <StormByte/buffer/producer.hxx>
#include <StormByte/test_handlers.h>

#include <iostream>
#include <optional>
#include <string>

using StormByte::Buffer::Consumer;
using StormByte::BinaryData;
using StormByte::Buffer::ExternalBufferReader;
using StormByte::Buffer::ExternalBufferWriter;
using StormByte::Buffer::ExternalReader;
using StormByte::Buffer::ExternalWriter;
using StormByte::Buffer::FIFO;
using StormByte::Buffer::Position;
using StormByte::Buffer::Producer;

namespace {
	std::string BytesToText(const BinaryData& data) {
		if (data.empty())
			return {};
		return std::string(reinterpret_cast<const char*>(data.data()),
			static_cast<std::size_t>(data.size()));
	}

	class DefaultExternalReader final : public ExternalReader {
		public:
			StormByte::ByteSize Available() const noexcept override {
				return 0;
			}

			bool Empty() const noexcept override {
				return true;
			}

			bool EoF() const noexcept override {
				return true;
			}

			bool IsReadable() const noexcept override {
				return true;
			}

			bool Read(const StormByte::ByteSize&, BinaryData&) const noexcept override {
				return false;
			}

			bool Extract(const StormByte::ByteSize&, BinaryData&) noexcept override {
				return false;
			}

			bool Peek(const StormByte::ByteSize&, BinaryData&) const noexcept override {
				return false;
			}

			void ReadUntilEoF(BinaryData&) const noexcept override {
			}

			void ExtractUntilEoF(BinaryData&) noexcept override {
			}

			PointerType Clone() const noexcept override {
				return MakePointer<DefaultExternalReader>();
			}

			PointerType Move() noexcept override {
				return MakePointer<DefaultExternalReader>();
			}
	};

	class CountingWriter final : public ExternalWriter {
		public:
			explicit CountingWriter(FIFO& target) noexcept : m_target(target) {}

			bool IsWritable() const noexcept override {
				return m_target.IsWritable();
			}

			StormByte::ByteSize Occupied() const noexcept override {
				return m_target.Size();
			}

			bool Write(const BinaryData& data) noexcept override {
				return m_target.Write(0, data);
			}

			bool Write(BinaryData&& data) noexcept override {
				return m_target.Write(0, std::move(data));
			}

			bool Write(const StormByte::ByteSize& count, const BinaryData& data) noexcept override {
				return m_target.Write(count, data);
			}

			bool Write(const StormByte::ByteSize& count, BinaryData&& data) noexcept override {
				return m_target.Write(count, std::move(data));
			}

			using ExternalWriter::Write;

			void Close() noexcept override {
				m_target.Close();
			}

			void SetError() noexcept override {
				m_target.SetError();
			}

			PointerType Clone() const noexcept override {
				return MakePointer<CountingWriter>(m_target);
			}

			PointerType Move() noexcept override {
				return MakePointer<CountingWriter>(m_target);
			}

		private:
			FIFO& m_target;
	};
}

// -------------------
// Occupied
// -------------------

int test_occupied_empty_is_zero() {
	const std::string fn = "test_occupied_empty_is_zero";
	FIFO target;
	ExternalBufferWriter adapter(target);
	ASSERT_EQUAL(fn, StormByte::ByteSize{0}, adapter.Occupied());
	ASSERT_EQUAL(fn, target.Size(), adapter.Occupied());
	RETURN_TEST(fn, 0);
}

int test_occupied_leaf_matches_store() {
	const std::string fn = "test_occupied_leaf_matches_store";
	FIFO target;
	CountingWriter leaf(target);
	ExternalWriter& writer = leaf;
	ASSERT_EQUAL(fn, StormByte::ByteSize{0}, writer.Occupied());
	ASSERT_TRUE(fn, writer.Write("ABC"));
	ASSERT_EQUAL(fn, StormByte::ByteSize{3}, writer.Occupied());
	ASSERT_EQUAL(fn, target.Size(), writer.Occupied());
	RETURN_TEST(fn, 0);
}

int test_occupied_producer() {
	const std::string fn = "test_occupied_producer";
	Producer producer;
	ExternalBufferWriter adapter(producer);
	ExternalWriter& writer = adapter;
	ASSERT_EQUAL(fn, StormByte::ByteSize{0}, writer.Occupied());
	ASSERT_EQUAL(fn, producer.Size(), writer.Occupied());
	ASSERT_TRUE(fn, writer.Write("XYZ"));
	ASSERT_EQUAL(fn, StormByte::ByteSize{3}, writer.Occupied());
	ASSERT_EQUAL(fn, producer.Size(), writer.Occupied());
	RETURN_TEST(fn, 0);
}

int test_occupied_survives_close() {
	const std::string fn = "test_occupied_survives_close";
	FIFO target;
	ExternalBufferWriter adapter(target);
	ExternalWriter& writer = adapter;
	ASSERT_TRUE(fn, writer.Write("KEEP"));
	adapter.Close();
	ASSERT_FALSE(fn, adapter.IsWritable());
	ASSERT_EQUAL(fn, StormByte::ByteSize{4}, adapter.Occupied());
	ASSERT_EQUAL(fn, target.Size(), adapter.Occupied());
	RETURN_TEST(fn, 0);
}

int test_occupied_tracks_fifo_size() {
	const std::string fn = "test_occupied_tracks_fifo_size";
	FIFO target;
	ExternalBufferWriter adapter(target);
	ExternalWriter& writer = adapter;
	ASSERT_TRUE(fn, writer.Write("HELLO"));
	ASSERT_EQUAL(fn, StormByte::ByteSize{5}, writer.Occupied());
	ASSERT_EQUAL(fn, target.Size(), writer.Occupied());
	ASSERT_TRUE(fn, writer.Write("!!"));
	ASSERT_EQUAL(fn, StormByte::ByteSize{7}, writer.Occupied());
	ASSERT_EQUAL(fn, target.Size(), writer.Occupied());
	RETURN_TEST(fn, 0);
}

// -------------------
// Owned handles
// -------------------

int test_owned_writer_clone_shares_ring() {
	const std::string fn = "test_owned_writer_clone_shares_ring";
	std::optional<Consumer> reader;
	std::optional<ExternalBufferWriter> adapter;
	{
		Producer tip;
		reader = tip.Consumer();
		adapter.emplace(std::move(tip));
	}
	auto clone = adapter->Clone();
	ASSERT_TRUE(fn, static_cast<bool>(clone));
	ASSERT_TRUE(fn, clone->Write("CLON"));
	BinaryData got;
	ASSERT_TRUE(fn, reader->Extract(4, got));
	ASSERT_EQUAL(fn, std::string("CLON"), BytesToText(got));
	clone->Close();
	ASSERT_FALSE(fn, adapter->IsWritable());
	ASSERT_TRUE(fn, reader->EoF());
	RETURN_TEST(fn, 0);
}

int test_reader_owns_consumer_after_source_dies() {
	const std::string fn = "test_reader_owns_consumer_after_source_dies";
	std::optional<Producer> writer;
	std::optional<ExternalBufferReader> adapter;
	{
		Producer origin;
		writer = origin;
		adapter.emplace(origin.Consumer());
	}
	ASSERT_TRUE(fn, writer->Write("KEEP"));
	ASSERT_EQUAL(fn, StormByte::ByteSize{4}, adapter->Available());
	BinaryData got;
	ASSERT_TRUE(fn, adapter->Extract(4, got));
	ASSERT_EQUAL(fn, std::string("KEEP"), BytesToText(got));
	writer->Close();
	ASSERT_TRUE(fn, adapter->EoF());
	RETURN_TEST(fn, 0);
}

int test_writer_owns_producer_after_source_dies() {
	const std::string fn = "test_writer_owns_producer_after_source_dies";
	std::optional<Consumer> reader;
	std::optional<ExternalBufferWriter> adapter;
	{
		Producer tip;
		reader = tip.Consumer();
		adapter.emplace(tip);
	}
	ASSERT_TRUE(fn, adapter->IsWritable());
	ASSERT_TRUE(fn, adapter->Write("LIVE"));
	ASSERT_EQUAL(fn, StormByte::ByteSize{4}, adapter->Occupied());
	BinaryData got;
	ASSERT_TRUE(fn, reader->Extract(4, got));
	ASSERT_EQUAL(fn, std::string("LIVE"), BytesToText(got));
	adapter->Close();
	ASSERT_FALSE(fn, adapter->IsWritable());
	ASSERT_TRUE(fn, reader->EoF());
	RETURN_TEST(fn, 0);
}

int test_writer_owns_temporary_producer() {
	const std::string fn = "test_writer_owns_temporary_producer";
	Producer origin;
	auto reader = origin.Consumer();
	ExternalBufferWriter adapter(origin);
	ASSERT_TRUE(fn, adapter.Write("TIP"));
	BinaryData got;
	ASSERT_TRUE(fn, reader.Extract(3, got));
	ASSERT_EQUAL(fn, std::string("TIP"), BytesToText(got));
	adapter.Close();
	ASSERT_FALSE(fn, origin.IsWritable());
	ASSERT_TRUE(fn, reader.EoF());
	RETURN_TEST(fn, 0);
}

// -------------------
// Reader ABI
// -------------------

int test_external_buffer_reader_polymorphic_abi() {
	const std::string fn = "test_external_buffer_reader_polymorphic_abi";
	FIFO source;
	ASSERT_TRUE(fn, source.Write("AB"));
	ExternalBufferReader adapter(source);
	ExternalReader& reader = adapter;
	ASSERT_EQUAL(fn, StormByte::ByteSize{2}, reader.Available());
	ASSERT_FALSE(fn, reader.Empty());
	ASSERT_FALSE(fn, reader.EoF());
	ASSERT_TRUE(fn, reader.IsReadable());
	BinaryData peek;
	ASSERT_TRUE(fn, reader.Peek(1, peek));
	BinaryData read;
	ASSERT_TRUE(fn, reader.Read(1, read));
	reader.Seek(0, Position::Absolute);
	BinaryData extracted;
	ASSERT_TRUE(fn, reader.Extract(1, extracted));
	reader.Clean();
	source.Close();
	BinaryData remaining;
	reader.ReadUntilEoF(remaining);
	BinaryData none;
	reader.ExtractUntilEoF(none);
	auto clone = reader.Clone();
	ASSERT_TRUE(fn, static_cast<bool>(clone));
	auto moved = reader.Move();
	ASSERT_TRUE(fn, static_cast<bool>(moved));
	RETURN_TEST(fn, 0);
}

int test_external_reader_default_interface_abi() {
	const std::string fn = "test_external_reader_default_interface_abi";
	DefaultExternalReader adapter;
	ExternalReader& reader = adapter;
	reader.Seek(0, Position::Absolute);
	reader.Clean();
	RETURN_TEST(fn, 0);
}

// -------------------
// Writer ABI
// -------------------

int test_external_buffer_writer_polymorphic_abi() {
	const std::string fn = "test_external_buffer_writer_polymorphic_abi";
	FIFO target;
	ExternalBufferWriter adapter(target);
	ExternalWriter& writer = adapter;
	ASSERT_EQUAL(fn, StormByte::ByteSize{0}, writer.Occupied());
	BinaryData copy {std::byte{'A'}};
	BinaryData moved {std::byte{'B'}};
	ASSERT_TRUE(fn, writer.Write(copy));
	ASSERT_TRUE(fn, writer.Write(std::move(moved)));
	ASSERT_TRUE(fn, writer.Write(1, copy));
	BinaryData counted_move {std::byte{'C'}};
	ASSERT_TRUE(fn, writer.Write(1, std::move(counted_move)));
	ASSERT_TRUE(fn, writer.IsWritable());
	ASSERT_EQUAL(fn, target.Size(), writer.Occupied());
	auto clone = writer.Clone();
	ASSERT_TRUE(fn, static_cast<bool>(clone));
	auto moved_adapter = writer.Move();
	ASSERT_TRUE(fn, static_cast<bool>(moved_adapter));
	writer.Close();
	ASSERT_FALSE(fn, writer.IsWritable());
	ASSERT_EQUAL(fn, target.Size(), writer.Occupied());
	ExternalBufferWriter error_adapter(target);
	ExternalWriter& error_writer = error_adapter;
	error_writer.SetError();
	ASSERT_FALSE(fn, error_writer.IsWritable());
	RETURN_TEST(fn, 0);
}

int main() {
	int result = 0;

	// -------------------
	// Occupied
	// -------------------
	result += test_occupied_empty_is_zero();
	result += test_occupied_leaf_matches_store();
	result += test_occupied_producer();
	result += test_occupied_survives_close();
	result += test_occupied_tracks_fifo_size();

	// -------------------
	// Owned handles
	// -------------------
	result += test_owned_writer_clone_shares_ring();
	result += test_reader_owns_consumer_after_source_dies();
	result += test_writer_owns_producer_after_source_dies();
	result += test_writer_owns_temporary_producer();

	// -------------------
	// Reader ABI
	// -------------------
	result += test_external_buffer_reader_polymorphic_abi();
	result += test_external_reader_default_interface_abi();

	// -------------------
	// Writer ABI
	// -------------------
	result += test_external_buffer_writer_polymorphic_abi();

	if (result == 0)
		std::cout << "All tests passed!" << std::endl;
	else
		std::cout << result << " tests failed." << std::endl;
	return result;
}
