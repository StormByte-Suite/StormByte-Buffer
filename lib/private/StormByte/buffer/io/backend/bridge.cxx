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

#include <StormByte/buffer/io/backend/bridge.hxx>

#include <algorithm>
#include <chrono>

using namespace StormByte::Buffer;

IO::Backend::Bridge::Bridge(ExternalReader& in, ExternalWriter& out, const StormByte::ByteSize high_water) noexcept:
	m_ext_in(&in),
	m_ext_out(&out),
	m_high_water(static_cast<std::size_t>(high_water)),
	m_status(IO::Drainer::Status::Started) {
	Launch();
}

IO::Backend::Bridge::Bridge(const IO::BufferedReader& in, IO::BufferedWriter& out, const StormByte::ByteSize high_water) noexcept:
	m_io_in(&in),
	m_io_out(&out),
	m_high_water(static_cast<std::size_t>(high_water)),
	m_status(IO::Drainer::Status::Started) {
	Launch();
}

IO::Backend::Bridge::Bridge(ExternalReader& in, IO::BufferedWriter& out, const StormByte::ByteSize high_water) noexcept:
	m_ext_in(&in),
	m_io_out(&out),
	m_high_water(static_cast<std::size_t>(high_water)),
	m_status(IO::Drainer::Status::Started) {
	Launch();
}

IO::Backend::Bridge::Bridge(const IO::BufferedReader& in, ExternalWriter& out, const StormByte::ByteSize high_water) noexcept:
	m_ext_out(&out),
	m_io_in(&in),
	m_high_water(static_cast<std::size_t>(high_water)),
	m_status(IO::Drainer::Status::Started) {
	Launch();
}

IO::Backend::Bridge::~Bridge() noexcept {
	{
		std::lock_guard lock(m_mutex);
		m_stop = true;
	}
	m_cv.notify_all();
	if (m_worker.joinable())
		m_worker.join();
}

void IO::Backend::Bridge::Launch() noexcept {
	m_worker = std::thread([this] { Worker(); });
}

bool IO::Backend::Bridge::EoF() const noexcept {
	if (m_ext_in)
		return m_ext_in->EoF();
	if (m_io_in)
		return m_io_in->EoF();
	return true;
}

bool IO::Backend::Bridge::IsReadable() const noexcept {
	if (m_ext_in)
		return m_ext_in->IsReadable();
	if (m_io_in)
		return static_cast<bool>(*m_io_in);
	return false;
}

bool IO::Backend::Bridge::IsWritable() const noexcept {
	if (m_ext_out)
		return m_ext_out->IsWritable();
	if (m_io_out)
		return static_cast<bool>(*m_io_out);
	return false;
}

StormByte::ByteSize IO::Backend::Bridge::HighWater() const noexcept {
	return StormByte::ByteSize{m_high_water.load()};
}

void IO::Backend::Bridge::HighWater(const StormByte::ByteSize high_water) noexcept {
	m_high_water.store(static_cast<std::size_t>(high_water));
	m_cv.notify_all();
}

IO::Drainer::Status IO::Backend::Bridge::Drainer() const noexcept {
	std::lock_guard lock(m_mutex);
	return m_status;
}

bool IO::Backend::Bridge::Drainer(const IO::Drainer::Operation operation) noexcept {
	std::unique_lock lock(m_mutex);
	if (operation == IO::Drainer::Operation::Toggle) {
		if (m_status == IO::Drainer::Status::Started)
			m_status = IO::Drainer::Status::Paused;
		else if (m_status == IO::Drainer::Status::Paused)
			m_status = IO::Drainer::Status::Started;
		else
			return false;
		m_cv.notify_all();
		return true;
	}

	if (operation != IO::Drainer::Operation::Flush)
		return false;

	m_hurry = true;
	m_cv.notify_all();
	m_cv.wait(lock, [this] { return m_stop || !m_hurry; });
	return !m_failed;
}

bool IO::Backend::Bridge::BarrierFlush() noexcept {
	std::unique_lock lock(m_mutex);
	m_barrier = true;
	m_cv.notify_all();
	m_cv.wait(lock, [this] { return m_stop || !m_barrier; });
	return !m_failed;
}

bool IO::Backend::Bridge::FlushAndClose() noexcept {
	const bool ok = BarrierFlush();
	if (m_ext_out)
		m_ext_out->Close();
	return ok;
}

void IO::Backend::Bridge::SetError() noexcept {
	if (m_ext_out)
		m_ext_out->SetError();
}

bool IO::Backend::Bridge::SourceDone() const noexcept {
	if (m_ext_in)
		return m_ext_in->EoF() && m_ext_in->Available() == StormByte::ByteSize{0};
	if (m_io_in)
		return m_io_in->EoF();
	return true;
}

StormByte::ByteSize IO::Backend::Bridge::AvailableNow() const noexcept {
	if (m_ext_in)
		return m_ext_in->Available();
	return StormByte::ByteSize{0};
}

StormByte::ByteSize IO::Backend::Bridge::OccupiedNow() const noexcept {
	if (m_ext_out)
		return m_ext_out->Occupied();
	if (m_io_out)
		return m_io_out->Dirty();
	return StormByte::ByteSize{0};
}

bool IO::Backend::Bridge::DestFlush() noexcept {
	if (!m_io_out)
		return true;
	return m_io_out->Flush().status == IO::Status::Ok;
}

bool IO::Backend::Bridge::Pull(const StormByte::ByteSize n, FIFO& dest) noexcept {
	dest.Clear();
	if (n == StormByte::ByteSize{0})
		return true;

	if (m_ext_in) {
		BinaryData chunk;
		if (!m_ext_in->Extract(n, chunk)) {
			if (!m_ext_in->Read(n, chunk))
				return m_ext_in->EoF();
		}
		if (chunk.empty())
			return true;
		return dest.Write(std::move(chunk));
	}

	if (m_io_in) {
		const IO::Result pulled = m_io_in->Read(n, dest);
		if (pulled.status == IO::Status::Ok || pulled.status == IO::Status::End)
			return true;
		return m_io_in->EoF();
	}

	return false;
}

bool IO::Backend::Bridge::Push(FIFO& src) noexcept {
	if (src.Available() == StormByte::ByteSize{0})
		return true;

	if (m_ext_out) {
		BinaryData chunk;
		if (!src.Extract(StormByte::ByteSize{0}, chunk))
			return false;
		if (chunk.empty())
			return true;
		return m_ext_out->Write(std::move(chunk));
	}

	if (m_io_out) {
		for (;;) {
			const IO::Result pushed = m_io_out->Write(src);
			if (pushed.status == IO::Status::Ok)
				return true;
			if (pushed.status != IO::Status::TryAgain)
				return false;
			static_cast<void>(m_io_out->Flush());
		}
	}

	return false;
}

bool IO::Backend::Bridge::Passthrough(const StormByte::ByteSize bytes) noexcept {
	if (bytes == StormByte::ByteSize{0})
		return true;
	if (!IsWritable())
		return false;

	if (m_io_out) {
		while (!m_io_out->WillWrite(bytes)) {
			if (m_io_out->Flush().status != IO::Status::Ok)
				return false;
			if (!m_io_out->WillWrite(bytes))
				return false;
		}
	}

	FIFO work;
	if (!Pull(bytes, work))
		return false;
	return Push(work);
}

void IO::Backend::Bridge::Worker() noexcept {
	for (;;) {
		std::unique_lock lock(m_mutex);
		m_cv.wait(lock, [this] {
			return m_stop || m_barrier || m_hurry || m_status == IO::Drainer::Status::Started;
		});
		if (m_stop)
			break;

		const bool barrier = m_barrier;
		const bool hurry = m_hurry;
		m_busy = true;
		lock.unlock();

		if (SourceDone()) {
			if (barrier) {
				if (!DestFlush())
					m_failed = true;
			}
			lock.lock();
			m_busy = false;
			if (m_barrier) {
				m_barrier = false;
				m_cv.notify_all();
			}
			if (m_hurry) {
				m_hurry = false;
				m_cv.notify_all();
			}
			m_cv.wait(lock, [this] {
				return m_stop || m_barrier || m_hurry ||
					(m_status == IO::Drainer::Status::Started && !SourceDone());
			});
			continue;
		}

		if (!IsWritable()) {
			m_failed = true;
			lock.lock();
			m_busy = false;
			m_barrier = false;
			m_hurry = false;
			m_cv.notify_all();
			continue;
		}

		const StormByte::ByteSize hw{m_high_water.load()};
		const StormByte::ByteSize occupied = OccupiedNow();
		StormByte::ByteSize room{0};
		if (hw == StormByte::ByteSize{0})
			room = m_chunk_max;
		else if (hw > occupied)
			room = hw - occupied;

		if (!hurry && !barrier && hw != StormByte::ByteSize{0} && room == StormByte::ByteSize{0}) {
			lock.lock();
			m_busy = false;
			m_cv.wait_for(lock, std::chrono::milliseconds(10), [this] {
				return m_stop || m_barrier || m_hurry ||
					m_status != IO::Drainer::Status::Started ||
					m_high_water.load() == 0 ||
					(StormByte::ByteSize{m_high_water.load()} > OccupiedNow());
			});
			continue;
		}

		StormByte::ByteSize want{0};
		if (hurry) {
			if (m_io_in)
				want = StormByte::ByteSize{1};
			else
				want = AvailableNow();
		}
		else {
			want = std::min(m_chunk_max, room == StormByte::ByteSize{0} ? m_chunk_min : room);
			if (want < m_chunk_min)
				want = StormByte::ByteSize{0};
			if (m_io_in) {
				if (want == StormByte::ByteSize{0})
					want = std::min(m_chunk_min, room == StormByte::ByteSize{0} ? m_chunk_min : room);
			}
			else {
				const StormByte::ByteSize avail = AvailableNow();
				if (avail == StormByte::ByteSize{0}) {
					lock.lock();
					m_busy = false;
					m_cv.wait_for(lock, std::chrono::milliseconds(10), [this] {
						return m_stop || m_barrier || m_hurry ||
							m_status != IO::Drainer::Status::Started ||
							AvailableNow() > StormByte::ByteSize{0} || SourceDone();
					});
					continue;
				}
				want = std::min(want == StormByte::ByteSize{0} ? avail : want, avail);
			}
		}

		if (want > StormByte::ByteSize{0} && !Passthrough(want))
			m_failed = true;

		if (barrier) {
			if (!DestFlush())
				m_failed = true;
		}

		lock.lock();
		m_busy = false;
		if (m_barrier) {
			m_barrier = false;
			m_cv.notify_all();
		}
		if (m_hurry) {
			m_hurry = false;
			m_cv.notify_all();
		}
	}
}
