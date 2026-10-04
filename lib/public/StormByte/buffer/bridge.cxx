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

#include <chrono>
#include <optional>
#include <utility>

using namespace StormByte::Buffer;

namespace {
	StormByte::Safe::Shared<StormByte::Buffer::ReadTelemetry> MakeReadTelemetry() {
		return StormByte::Safe::Shared<StormByte::Buffer::ReadTelemetry>::MakePointer<StormByte::Buffer::ReadTelemetry>();
	}

	StormByte::Safe::Shared<StormByte::Buffer::WriteTelemetry> MakeWriteTelemetry() {
		return StormByte::Safe::Shared<StormByte::Buffer::WriteTelemetry>::MakePointer<StormByte::Buffer::WriteTelemetry>();
	}
}

Bridge::Bridge(ReadOnly& in, WriteOnly& out) noexcept:
	m_owned_read(MakeReadTelemetry()),
	m_owned_write(MakeWriteTelemetry()) {
	AttachNonIoIn(in);
	AttachNonIoOut(out);
}

void Bridge::AttachNonIoIn(ReadOnly& in) noexcept {
	m_ext_in = &in;
	if (!m_owned_read)
		m_owned_read = MakeReadTelemetry();
}

void Bridge::AttachNonIoOut(WriteOnly& out) noexcept {
	m_ext_out = &out;
	if (!m_owned_write)
		m_owned_write = MakeWriteTelemetry();
}

void Bridge::CacheReadTelemetry() noexcept {
	if (m_io_in)
		m_owned_read = m_io_in->Telemetry();
}

void Bridge::CacheWriteTelemetry() noexcept {
	if (m_io_out)
		m_owned_write = m_io_out->Telemetry();
}

Bridge::Bridge(Bridge&& other) noexcept {
	std::lock_guard lock(other.m_mutex);
	m_ext_in = other.m_ext_in;
	m_ext_out = other.m_ext_out;
	other.m_ext_in = nullptr;
	other.m_ext_out = nullptr;
	m_io_in = std::move(other.m_io_in);
	m_io_out = std::move(other.m_io_out);
	m_owned_read = std::move(other.m_owned_read);
	m_owned_write = std::move(other.m_owned_write);
	m_io_in_blocking = other.m_io_in_blocking;
	m_session_state = other.m_session_state;
	other.m_io_in_blocking = false;
	other.m_session_state = State::Closed;
}

Bridge::~Bridge() noexcept = default;

Bridge& Bridge::operator=(Bridge&& other) noexcept {
	if (this == &other)
		return *this;
	std::scoped_lock lock(m_mutex, other.m_mutex);
	ReleaseTips();
	m_ext_in = other.m_ext_in;
	m_ext_out = other.m_ext_out;
	other.m_ext_in = nullptr;
	other.m_ext_out = nullptr;
	m_io_in = std::move(other.m_io_in);
	m_io_out = std::move(other.m_io_out);
	m_owned_read = std::move(other.m_owned_read);
	m_owned_write = std::move(other.m_owned_write);
	m_io_in_blocking = other.m_io_in_blocking;
	m_session_state = other.m_session_state;
	other.m_io_in_blocking = false;
	other.m_session_state = State::Closed;
	return *this;
}

void Bridge::ReleaseTips() noexcept {
	m_ext_in = nullptr;
	m_ext_out = nullptr;
	m_io_in.reset();
	m_io_out.reset();
	m_io_in_blocking = false;
}

void Bridge::Close() noexcept {
	std::lock_guard lock(m_mutex);
	ReleaseTips();
	if (m_session_state == State::Open)
		m_session_state = State::Closed;
}

bool Bridge::SourceEoF() const noexcept {
	if (m_ext_in)
		return m_ext_in->EoF();
	if (m_io_in)
		return m_io_in->EoF();
	return true;
}

bool Bridge::EoF() const noexcept {
	std::lock_guard lock(m_mutex);
	if (m_session_state != State::Open)
		return true;
	return SourceEoF();
}

bool Bridge::Failed() const noexcept {
	std::lock_guard lock(m_mutex);
	return m_session_state == State::Failed;
}

bool Bridge::InputIsIO() const noexcept {
	std::lock_guard lock(m_mutex);
	return static_cast<bool>(m_io_in);
}

bool Bridge::InputPullBlocking() const noexcept {
	std::lock_guard lock(m_mutex);
	return m_io_in_blocking;
}

enum Bridge::State Bridge::State() const noexcept {
	std::lock_guard lock(m_mutex);
	return m_session_state;
}

const StormByte::Safe::Shared<StormByte::Buffer::ReadTelemetry> Bridge::ReadTelemetry() const noexcept {
	std::lock_guard lock(m_mutex);
	return m_owned_read;
}

const StormByte::Safe::Shared<StormByte::Buffer::WriteTelemetry> Bridge::WriteTelemetry() const noexcept {
	std::lock_guard lock(m_mutex);
	return m_owned_write;
}

IO::Result Bridge::Pull(const StormByte::ByteSize n, FIFO& dest, const Operation operation) {
	dest.Clear();

	if (m_ext_in) {
		if (!m_ext_in->IsReadable())
			return { IO::Status::Failed, 0 };
		if (m_ext_in->Available() == StormByte::ByteSize{0} && m_ext_in->EoF())
			return { IO::Status::End, 0 };

		StormByte::ByteSize want = n;
		const StormByte::ByteSize now = m_ext_in->Available();
		if (want == StormByte::ByteSize{0} || operation == Operation::NonBlocking) {
			if (want == StormByte::ByteSize{0})
				want = now;
			else if (now < want)
				want = now;
		}

		if (want == StormByte::ByteSize{0})
			return { IO::Status::Ok, 0 };

		BinaryData chunk;
		if (!m_ext_in->Extract(want, chunk)) {
			const StormByte::ByteSize left = m_ext_in->Available();
			if (left > StormByte::ByteSize{0} && left < want
					&& m_ext_in->Extract(left, chunk) && !chunk.empty()) {
				if (!dest.Write(std::move(chunk)))
					return { IO::Status::Failed, 0 };
				return { IO::Status::Ok, dest.Available() };
			}
			if (!m_ext_in->IsReadable())
				return { IO::Status::Failed, 0 };
			if (m_ext_in->EoF())
				return { IO::Status::End, 0 };
			if (operation == Operation::NonBlocking)
				return { IO::Status::Ok, 0 };
			return { IO::Status::Failed, 0 };
		}
		if (chunk.empty()) {
			if (!m_ext_in->IsReadable())
				return { IO::Status::Failed, 0 };
			return { m_ext_in->EoF() ? IO::Status::End : IO::Status::Ok, 0 };
		}
		if (!dest.Write(std::move(chunk)))
			return { IO::Status::Failed, 0 };
		return { IO::Status::Ok, dest.Available() };
	}

	if (m_io_in) {
		StormByte::ByteSize want = n;
		if (want == StormByte::ByteSize{0} || operation == Operation::NonBlocking) {
			const StormByte::ByteSize now = m_io_in->Available();
			if (want == StormByte::ByteSize{0})
				want = now;
			else if (now < want)
				want = now;
			if (want == StormByte::ByteSize{0})
				return { m_io_in->EoF() ? IO::Status::End : IO::Status::Ok, 0 };
		}
		return m_io_in->Read(want, dest);
	}

	return { IO::Status::Failed, 0 };
}

IO::Result Bridge::Push(FIFO& src) {
	const StormByte::ByteSize n = src.Available();
	if (n == StormByte::ByteSize{0})
		return { IO::Status::Ok, 0 };

	if (m_ext_out) {
		if (!m_ext_out->IsWritable())
			return { IO::Status::Failed, 0 };
		BinaryData chunk;
		if (!src.Extract(n, chunk))
			return { IO::Status::Failed, 0 };
		if (!m_ext_out->Write(StormByte::ByteSize{0}, std::move(chunk)))
			return { IO::Status::Failed, 0 };
		return { IO::Status::Ok, n };
	}

	if (m_io_out) {
		for (;;) {
			const IO::Result pushed = m_io_out->Write(src);
			if (pushed.status == IO::Status::TryAgain)
				continue;
			return pushed;
		}
	}

	return { IO::Status::Failed, 0 };
}

StormByte::ByteSize Bridge::Passthrough(const StormByte::ByteSize n, const Operation operation) {
	std::lock_guard lock(m_mutex);
	if (m_session_state != State::Open)
		return StormByte::ByteSize{0};

	std::optional<StormByte::Buffer::Telemetry::OperationSample> read_sample;
	std::optional<StormByte::Buffer::Telemetry::OperationSample> write_sample;
	if (m_ext_in && m_owned_read)
		read_sample.emplace(m_owned_read->MeasureOperation());
	if (m_ext_out && m_owned_write)
		write_sample.emplace(m_owned_write->MeasureOperation());
	FIFO work;
	const IO::Result pulled = Pull(n, work, operation);
	if (pulled.status == IO::Status::Failed || pulled.status == IO::Status::Error) {
		m_session_state = State::Failed;
		ReleaseTips();
		return StormByte::ByteSize{0};
	}

	const StormByte::ByteSize got = work.Available();
	if (got == StormByte::ByteSize{0}) {
		if (pulled.status == IO::Status::End) {
			m_session_state = State::Closed;
			ReleaseTips();
		}
		return StormByte::ByteSize{0};
	}

	const IO::Result pushed = Push(work);
	if (pushed.status != IO::Status::Ok) {
		m_session_state = State::Failed;
		ReleaseTips();
		return StormByte::ByteSize{0};
	}

	if (read_sample) {
		read_sample->Commit(got);
		m_owned_read->m_delivered = m_owned_read->m_delivered + got;
	}
	if (write_sample) {
		write_sample->Commit(got);
		m_owned_write->m_accepted = m_owned_write->m_accepted + got;
	}

	if (SourceEoF()) {
		m_session_state = State::Closed;
		ReleaseTips();
	}
	return got;
}
