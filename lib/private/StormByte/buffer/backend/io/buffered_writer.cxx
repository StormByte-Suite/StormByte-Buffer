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

#include <StormByte/buffer/backend/io/buffered_writer.hxx>
#include <StormByte/buffer/backend/telemetry_counter.hxx>
#include <StormByte/buffer/lockfree_ring.hxx>
#include <StormByte/buffer/exception.hxx>
#include <StormByte/safe/unique_lock.hxx>

#include <algorithm>
#include <optional>
#include <limits>
#include <new>
#include <thread>
#include <utility>

using namespace StormByte::Buffer::Backend::IO;
using Result = StormByte::Buffer::IO::Result;
using Status = StormByte::Buffer::IO::Status;
using FIFO = StormByte::Buffer::FIFO;
using Position = StormByte::Buffer::Position;
using BinaryData = StormByte::Safe::Binary;

namespace {
	template<typename Call>
	Result InvokeOrigin(Call&& call) noexcept {
		try {
			return call();
		}
		catch (...) {
			return { Status::Error, 0 };
		}
	}
}

BufferedWriter::BufferedWriter(StormByte::Buffer::IO::BufferedWriter& owner, StormByte::Safe::String path,
		const StormByte::Buffer::IO::Location location, const StormByte::Buffer::IO::WriterConfiguration configuration):
	m_owner(&owner),
	m_path(std::move(path)),
	m_location(location),
	m_state(StormByte::Buffer::IO::State::Unavailable) {
	if (Configure(configuration).status != Status::Ok)
		throw StormByte::Buffer::Exception("Invalid writer configuration");
	StartWorker();
}

BufferedWriter::~BufferedWriter() {
	Shutdown();
}

void BufferedWriter::Rebind(StormByte::Buffer::IO::BufferedWriter& owner) noexcept {
	m_owner = &owner;
}

const StormByte::Safe::String& BufferedWriter::Path() const noexcept {
	return m_path;
}

StormByte::Buffer::IO::Location BufferedWriter::Location() const noexcept {
	return m_location;
}

BufferedWriter::operator bool() const noexcept {
	StormByte::Safe::UniqueLock lock(m_mutex);
	return m_state == StormByte::Buffer::IO::State::Idle;
}

StormByte::Buffer::IO::State BufferedWriter::State() const noexcept {
	StormByte::Safe::UniqueLock lock(m_mutex);
	return m_state;
}

void BufferedWriter::SetState(const StormByte::Buffer::IO::State state) noexcept {
	StormByte::Safe::UniqueLock lock(m_mutex);
	m_state = state;
}

void BufferedWriter::SetTell(const StormByte::ByteSize offset) noexcept {
	StormByte::Safe::UniqueLock lock(m_mutex);
	m_tell = offset;
	if (m_tell > m_high_water)
		m_high_water = m_tell;
	if (StormByte::Buffer::IO::WriteTelemetry* io = IoTelemetry())
		StormByte::Buffer::Backend::StoreTelemetryCounter(io->m_high_water, m_high_water);
}

bool BufferedWriter::BufferedMode() const noexcept {
	return m_write_chunk > StormByte::ByteSize{0} && m_back_pressure > 0;
}

bool BufferedWriter::PageMode() const noexcept {
	return m_max_memory > StormByte::ByteSize{0};
}

StormByte::ByteSize BufferedWriter::PendingCap() const noexcept {
	if (!BufferedMode())
		return StormByte::ByteSize{0};
	return m_write_chunk * StormByte::Size{m_back_pressure};
}

StormByte::ByteSize BufferedWriter::PageDirty() const noexcept {
	StormByte::ByteSize total{0};
	for (const auto& item : m_pages)
		total = total + StormByte::ByteSize{item.second.bytes.size()};
	return total;
}

StormByte::ByteSize BufferedWriter::TotalDirty() const noexcept {
	StormByte::ByteSize total = PageDirty() + m_in_flight;
	if (m_ring)
		total = total + m_ring->Available();
	return total;
}

bool BufferedWriter::WouldAccept(const StormByte::ByteSize bytes) const noexcept {
	if (PageMode())
		return true;
	if (!BufferedMode())
		return true;
	if (!m_ring)
		return false;
	const auto pending = m_ring->Available();
	return pending <= PendingCap() && bytes <= PendingCap() - pending;
}

bool BufferedWriter::WillWrite(const StormByte::ByteSize n) const noexcept {
	StormByte::Safe::UniqueLock lock(m_mutex);
	return WouldAccept(n);
}

void BufferedWriter::ClearPages() noexcept {
	m_pages.clear();
}

void BufferedWriter::CloseSeekEpoch() noexcept {
	if (!m_epoch_open)
		return;
	if (m_epoch_hit) {
		if (m_epoch_origin)
			++m_seek_saved_partial;
		else
			++m_seek_saved_full;
	}
	m_epoch_open = false;
	if (StormByte::Buffer::IO::WriteTelemetry* io = IoTelemetry()) {
		StormByte::Buffer::Backend::StoreTelemetryCounter(io->m_seek_saved_full, m_seek_saved_full);
		StormByte::Buffer::Backend::StoreTelemetryCounter(io->m_seek_saved_partial, m_seek_saved_partial);
	}
}

void BufferedWriter::NoteWait(const std::chrono::nanoseconds elapsed) const noexcept {
	if (m_wait_samples == 0) {
		m_wait_min = elapsed;
		m_wait_max = elapsed;
	}
	else {
		if (elapsed < m_wait_min)
			m_wait_min = elapsed;
		if (elapsed > m_wait_max)
			m_wait_max = elapsed;
	}
	m_wait_total += elapsed;
	++m_wait_samples;
	if (StormByte::Buffer::IO::WriteTelemetry* io = IoTelemetry()) {
		StormByte::Buffer::Backend::StoreTelemetryCounter(io->m_wait_min, m_wait_min);
		StormByte::Buffer::Backend::StoreTelemetryCounter(io->m_wait_max, m_wait_max);
		StormByte::Buffer::Backend::StoreTelemetryCounter(io->m_wait_total, m_wait_total);
		StormByte::Buffer::Backend::StoreTelemetryCounter(io->m_wait_samples, m_wait_samples);
	}
}

void BufferedWriter::NoteDirty() const noexcept {
	const StormByte::ByteSize now = TotalDirty();
	if (now > m_dirty_peak)
		m_dirty_peak = now;
	if (m_max_memory > StormByte::ByteSize{0} && PageDirty() >= m_max_memory)
		++m_saturated;
	if (StormByte::Buffer::IO::WriteTelemetry* io = IoTelemetry()) {
		StormByte::Buffer::Backend::StoreTelemetryCounter(io->m_dirty, now);
		StormByte::Buffer::Backend::StoreTelemetryCounter(io->m_dirty_peak, m_dirty_peak);
		StormByte::Buffer::Backend::StoreTelemetryCounter(io->m_cap, m_ring ? PendingCap() : StormByte::ByteSize{0});
		StormByte::Buffer::Backend::StoreTelemetryCounter(io->m_saturated, m_saturated);
	}
}

StormByte::Buffer::IO::WriteTelemetry* BufferedWriter::IoTelemetry() const noexcept {
	if (!m_telemetry)
		return nullptr;
	return dynamic_cast<StormByte::Buffer::IO::WriteTelemetry*>(&*m_telemetry);
}

bool BufferedWriter::Open() {
	std::lock_guard operation(m_operations);
	if (!m_owner)
		return false;

	bool already = false;
	{
		StormByte::Safe::UniqueLock lock(m_mutex);
		already = m_open;
	}

	Result opened;
	{
		StormByte::Safe::UniqueLock origin(m_origin_io);
		opened = InvokeOrigin([this] { return m_owner->OriginOpen(); });
	}

	StormByte::Safe::UniqueLock lock(m_mutex);
	if (already)
		return false;

	if (opened.status != Status::Ok) {
		m_failed = true;
		m_open = false;
		return false;
	}

	m_open = true;
	m_failed = false;
	m_tell = StormByte::ByteSize{0};
	m_high_water = StormByte::ByteSize{0};
	m_origin_pos = StormByte::ByteSize{0};
	m_origin_cursor_dirty = false;
	m_materialized = StormByte::ByteSize{0};
	m_state = StormByte::Buffer::IO::State::Idle;
	ClearPages();
	if (m_ring)
		m_ring->Clear();
	return m_state == StormByte::Buffer::IO::State::Idle;
}

bool BufferedWriter::Close() {
	std::lock_guard operation(m_operations);
	const Result flushed = Flush();

	bool was_open = false;
	{
		StormByte::Safe::UniqueLock lock(m_mutex);
		was_open = m_open;
		CloseSeekEpoch();
	}

	Result closed{ Status::Ok, 0 };
	{
		StormByte::Safe::UniqueLock origin(m_origin_io);
		if (was_open && m_owner)
			closed = InvokeOrigin([this] { return m_owner->OriginClose(); });
		// Publish closure before another origin-lock holder can call a derived hook.
		StormByte::Safe::UniqueLock lock(m_mutex);
		m_open = false;
		m_cv.notify_all();
	}

	StormByte::Safe::UniqueLock lock(m_mutex);
	if (flushed.status == Status::Error) {
		m_failed = true;
		m_state = StormByte::Buffer::IO::State::Fault;
		return false;
	}
	if (flushed.status == Status::Failed && was_open) {
		m_failed = true;
		m_state = StormByte::Buffer::IO::State::Fault;
		return false;
	}
	if (closed.status != Status::Ok && was_open) {
		m_failed = true;
		m_state = StormByte::Buffer::IO::State::Fault;
		return false;
	}
	m_failed = false;
	m_state = StormByte::Buffer::IO::State::Unavailable;
	ClearPages();
	if (m_ring)
		m_ring->Clear();
	return true;
}

void BufferedWriter::Shutdown() {
	std::lock_guard operation(m_operations);
	StopWorker();
	StormByte::Safe::UniqueLock lock(m_mutex);
	m_open = false;
	m_state = StormByte::Buffer::IO::State::Unavailable;
}

bool BufferedWriter::Rewind() {
	std::lock_guard operation(m_operations);
	{
		StormByte::Safe::UniqueLock lock(m_mutex);
		if (!m_open)
			return false;
	}
	if (!Close())
		return false;
	return Open();
}

bool BufferedWriter::IsOpen() const noexcept {
	StormByte::Safe::UniqueLock lock(m_mutex);
	return m_open;
}

Result BufferedWriter::EnsureOrigin(const StormByte::ByteSize absolute) {
	if (!m_owner)
		return { Status::Failed, 0 };

	bool skip = false;
	{
		StormByte::Safe::UniqueLock lock(m_mutex);
		skip = !m_origin_cursor_dirty && m_origin_pos == absolute;
	}
	if (skip)
		return { Status::Ok, 0 };

	const Result moved = InvokeOrigin([this, absolute] { return m_owner->OriginSeek(absolute); });
	if (moved.status != Status::Ok)
		return moved;

	StormByte::Safe::UniqueLock lock(m_mutex);
	m_origin_pos = absolute;
	m_origin_cursor_dirty = false;
	++m_seek_origin;
	if (m_epoch_open)
		m_epoch_origin = true;
	if (StormByte::Buffer::IO::WriteTelemetry* io = IoTelemetry())
		StormByte::Buffer::Backend::StoreTelemetryCounter(io->m_seek_origin, m_seek_origin);
	return { Status::Ok, 0 };
}

Result BufferedWriter::StorePages(const std::span<const std::byte> src) {
	const StormByte::ByteSize at = m_tell;
	const StormByte::ByteSize n{src.size()};
	const StormByte::ByteSize range_end = at + n;

	bool hit = false;
	bool behind = false;
	for (const auto& item : m_pages) {
		const StormByte::ByteSize p0 = item.second.offset;
		const StormByte::ByteSize p1 = p0 + StormByte::ByteSize{item.second.bytes.size()};
		if (p1 <= at || p0 >= range_end)
			continue;
		hit = true;
		if (p1 <= m_high_water)
			behind = true;
	}

	if (hit) {
		if (behind)
			m_hit_back = m_hit_back + n;
		else
			m_hit_ahead = m_hit_ahead + n;
		if (m_epoch_open)
			m_epoch_hit = true;
	}
	else
		m_miss = m_miss + n;

	const auto page_limit = std::min(m_max_memory, m_write_transfer > StormByte::ByteSize{0}
		? m_write_transfer : StormByte::ByteSize{64 * 1024});
	std::size_t done = 0;
	while (done < src.size()) {
		const auto pos = at + StormByte::ByteSize{done};
		auto next = m_pages.upper_bound(static_cast<std::size_t>(pos));
		if (next != m_pages.begin()) {
			auto prev = std::prev(next);
			auto& page = prev->second;
			const auto size = static_cast<std::size_t>(page.bytes.size());
			const auto offset = static_cast<std::size_t>(pos - page.offset);
			if (offset < size) {
				const auto take = std::min(src.size() - done, size - offset);
				std::copy_n(src.data() + done, take, page.bytes.data() + offset);
				done += take;
				continue;
			}
			if (offset == size && StormByte::ByteSize{size} < page_limit) {
				auto take = std::min(src.size() - done, static_cast<std::size_t>(page_limit) - size);
				if (next != m_pages.end())
					take = std::min(take, static_cast<std::size_t>(next->second.offset - pos));
				page.bytes.insert(page.bytes.end(), src.subspan(done, take));
				done += take;
				continue;
			}
		}
		auto take = std::min(src.size() - done, static_cast<std::size_t>(page_limit));
		if (next != m_pages.end())
			take = std::min(take, static_cast<std::size_t>(next->second.offset - pos));
		Page fresh;
		fresh.offset = pos;
		fresh.bytes = BinaryData(src.data() + done, StormByte::ByteSize{take});
		m_pages.emplace(static_cast<std::size_t>(pos), std::move(fresh));
		done += take;
	}
	return { Status::Ok, n };
}

Result BufferedWriter::MaterializeFrom(StormByte::Safe::UniqueLock& lock,
		StormByte::Safe::Map<std::size_t, Page>::iterator it) {
	if (it == m_pages.end() || !m_owner)
		return { Status::Failed, 0 };

	Page page = std::move(it->second);
	m_pages.erase(it);

	const StormByte::ByteSize start = page.offset;
	BinaryData payload = std::move(page.bytes);
	m_in_flight = StormByte::ByteSize{payload.size()};
	lock.unlock();

	Result ensured;
	Result pushed;
	{
		StormByte::Safe::UniqueLock origin(m_origin_io);
		ensured = EnsureOrigin(start);
		if (ensured.status == Status::Ok) {
			const auto view = std::span<const std::byte>(payload.data(),
				static_cast<std::size_t>(payload.size()));
			pushed = PushAll(view);
		}
	}
	lock.lock();
	m_in_flight = StormByte::ByteSize{0};
	if (ensured.status != Status::Ok) {
		Page back;
		back.offset = start;
		back.bytes = std::move(payload);
		m_pages.emplace(static_cast<std::size_t>(start), std::move(back));
		return ensured;
	}
	if (pushed.status != Status::Ok) {
		Page back;
		back.offset = start + pushed.count;
		const auto written = static_cast<std::size_t>(pushed.count);
		std::move(payload.begin() + written, payload.end(), payload.begin());
		payload.resize(StormByte::ByteSize{payload.size()} - pushed.count);
		back.bytes = std::move(payload);
		if (!back.bytes.empty())
			m_pages.emplace(static_cast<std::size_t>(back.offset), std::move(back));
		m_failed = true;
		if (m_state == StormByte::Buffer::IO::State::Idle)
			m_state = StormByte::Buffer::IO::State::Fault;
		return pushed;
	}

	const StormByte::ByteSize n{payload.size()};
	m_origin_pos = start + n;
	const StormByte::ByteSize end = start + n;
	if (end > m_materialized)
		m_materialized = end;
	if (StormByte::Buffer::IO::WriteTelemetry* io = IoTelemetry()) {
		StormByte::Buffer::Backend::StoreTelemetryCounter(io->m_origin, m_origin_bytes);
		StormByte::Buffer::Backend::StoreTelemetryCounter(io->m_materialized, m_materialized);
	}
	return { Status::Ok, n };
}

Result BufferedWriter::CollectGarbage() {
	StormByte::Safe::UniqueLock lock(m_mutex);
	while (PageDirty() > m_max_memory) {
		if (m_pages.empty())
			break;

		auto victim = m_pages.end();
		for (auto it = m_pages.begin(); it != m_pages.end(); ++it) {
			const StormByte::ByteSize end = it->second.offset + StormByte::ByteSize{it->second.bytes.size()};
			if (end <= m_high_water) {
				victim = it;
				break;
			}
		}
		if (victim == m_pages.end())
			victim = std::prev(m_pages.end());

		++m_evicted;
		if (StormByte::Buffer::IO::WriteTelemetry* io = IoTelemetry())
			StormByte::Buffer::Backend::StoreTelemetryCounter(io->m_evicted, m_evicted);
		const Result evicted = MaterializeFrom(lock, victim);
		if (evicted.status != Status::Ok)
			return evicted;
	}
	return { Status::Ok, 0 };
}

Result BufferedWriter::MaterializeAll() {
	StormByte::Safe::UniqueLock lock(m_mutex);
	if (m_failed)
		return {Status::Error, 0};
	while (!m_pages.empty()) {
		const Result pushed = MaterializeFrom(lock, m_pages.begin());
		if (pushed.status != Status::Ok)
			return pushed;
	}
	return { Status::Ok, 0 };
}

Result BufferedWriter::Flush() {
	std::lock_guard operation(m_operations);
	if (!IsOpen())
		return {Status::Failed, 0};
	std::optional<StormByte::Buffer::Telemetry::OperationSample> telemetry_sample;
	if (StormByte::Buffer::IO::WriteTelemetry* io = IoTelemetry())
		telemetry_sample.emplace(io->MeasureOperation());
	const Result pages = MaterializeAll();
	if (pages.status != Status::Ok)
		return pages;

	StormByte::Safe::UniqueLock lock(m_mutex);
	if (!m_open || !m_owner)
		return { Status::Failed, 0 };
	if (m_failed)
		return { Status::Error, 0 };

	const bool ring_dirty = m_ring && m_ring->Available() > StormByte::ByteSize{0};
	if (ring_dirty) {
		m_flush.store(true, StormByte::Safe::MemoryOrder::Release);
		m_drain_run = true;
		m_cv.notify_all();
	}
	m_cv.wait(lock, [this] {
		const bool empty = !m_ring || m_ring->Available() == StormByte::ByteSize{0};
		return m_stop.load(StormByte::Safe::MemoryOrder::Acquire)
			|| m_failed
			|| !m_open
			|| (empty && !m_drain_run);
	});
	m_flush.store(false, StormByte::Safe::MemoryOrder::Release);
	if (m_failed)
		return { Status::Error, 0 };
	if (!m_open)
		return { Status::Failed, 0 };
	if (m_stop.load(StormByte::Safe::MemoryOrder::Acquire))
		return { Status::Failed, 0 };
	lock.unlock();

	Result visible;
	{
		StormByte::Safe::UniqueLock origin(m_origin_io);
		{
			StormByte::Safe::UniqueLock inner(m_mutex);
			if (m_failed)
				return { Status::Error, 0 };
			if (!m_open)
				return { Status::Failed, 0 };
		}
		visible = InvokeOrigin([this] { return m_owner->OriginFlush(); });
	}
	{
		StormByte::Safe::UniqueLock dirty(m_mutex);
		m_origin_cursor_dirty = true;
		if (visible.status != Status::Ok) {
			m_failed = true;
			if (m_state == StormByte::Buffer::IO::State::Idle)
				m_state = StormByte::Buffer::IO::State::Fault;
			return visible;
		}
		NoteDirty();
		if (telemetry_sample)
			telemetry_sample->Commit(StormByte::ByteSize{0});
	}
	return { Status::Ok, 0 };
}

Result BufferedWriter::Truncate() {
	std::lock_guard operation(m_operations);
	{
		StormByte::Safe::UniqueLock lock(m_mutex);
		m_cv.wait(lock, [this] { return !m_drain_run; });
		if (!m_open || m_failed || !m_owner)
			return { Status::Failed, 0 };
		ClearPages();
		if (m_ring)
			m_ring->Clear();
		CloseSeekEpoch();
	}

	Result truncated;
	{
		StormByte::Safe::UniqueLock origin(m_origin_io);
		truncated = InvokeOrigin([this] { return m_owner->OriginTruncate(); });
	}
	StormByte::Safe::UniqueLock lock(m_mutex);
	if (truncated.status != Status::Ok) {
		m_failed = true;
		m_state = StormByte::Buffer::IO::State::Fault;
		return { Status::Failed, 0 };
	}
	m_tell = StormByte::ByteSize{0};
	m_high_water = StormByte::ByteSize{0};
	m_origin_pos = StormByte::ByteSize{0};
	m_origin_cursor_dirty = true;
	m_materialized = StormByte::ByteSize{0};
	if (StormByte::Buffer::IO::WriteTelemetry* io = IoTelemetry()) {
		StormByte::Buffer::Backend::StoreTelemetryCounter(io->m_high_water, m_high_water);
		StormByte::Buffer::Backend::StoreTelemetryCounter(io->m_materialized, m_materialized);
		StormByte::Buffer::Backend::StoreTelemetryCounter(io->m_dirty, StormByte::ByteSize{0});
	}
	return { Status::Ok, 0 };
}

Result BufferedWriter::Seek(const std::ptrdiff_t offset, const Position mode) {
	std::lock_guard operation(m_operations);
	StormByte::Safe::UniqueLock lock(m_mutex);
	if (!m_open || m_failed || m_state != StormByte::Buffer::IO::State::Idle || !m_owner)
		return { Status::Failed, 0 };

	StormByte::ByteSize abs = m_tell;
	if (mode == Position::Absolute) {
		if (offset < 0)
			return { Status::Failed, 0 };
		abs = StormByte::ByteSize{static_cast<std::size_t>(offset)};
	}
	else {
		if (offset < 0) {
			const auto back = StormByte::ByteSize{static_cast<std::size_t>(-(offset + 1)) + 1};
			if (back > abs)
				return {Status::Failed, 0};
			abs = abs - back;
		}
		else {
			const auto step = StormByte::ByteSize{static_cast<std::size_t>(offset)};
			const auto limit = StormByte::ByteSize{static_cast<std::size_t>(std::numeric_limits<std::ptrdiff_t>::max())};
			if (step > limit - abs)
				return {Status::Failed, 0};
			abs = abs + step;
		}
	}

	CloseSeekEpoch();
	m_tell = abs;
	if (m_tell > m_high_water)
		m_high_water = m_tell;
	m_epoch_open = true;
	m_epoch_hit = false;
	m_epoch_origin = false;
	++m_seek_logical;
	if (StormByte::Buffer::IO::WriteTelemetry* io = IoTelemetry()) {
		StormByte::Buffer::Backend::StoreTelemetryCounter(io->m_seek_logical, m_seek_logical);
		StormByte::Buffer::Backend::StoreTelemetryCounter(io->m_high_water, m_high_water);
	}
	return { Status::Ok, 0 };
}

Result BufferedWriter::Write(const FIFO& src) {
	std::lock_guard operation(m_operations);
	const StormByte::ByteSize need = src.Available();
	if (need == StormByte::ByteSize{0})
		return { Status::Ok, 0 };
	{
		StormByte::Safe::UniqueLock lock(m_mutex);
		if (!m_open || m_failed || m_state != StormByte::Buffer::IO::State::Idle)
			return { Status::Failed, 0 };
		if (!WouldAccept(need)) {
			++m_try_again;
			if (StormByte::Buffer::IO::WriteTelemetry* io = IoTelemetry())
				StormByte::Buffer::Backend::StoreTelemetryCounter(io->m_try_again, m_try_again);
			return { Status::TryAgain, 0 };
		}
	}
	const auto start = src.Size() - src.Available();
	auto window = m_write_transfer > StormByte::ByteSize{0} ? m_write_transfer : StormByte::ByteSize{64 * 1024};
	if (PageMode())
		window = std::min(window, m_max_memory);
	StormByte::ByteSize done{0};
	while (done < need) {
		const auto take = std::min(window, need - done);
		BinaryData chunk;
		if (!src.Read(take, chunk)) {
			src.Seek(static_cast<std::ptrdiff_t>(start), Position::Absolute);
			return {Status::Failed, done};
		}
		const Result written = WriteSpan(std::span<const std::byte>(chunk.data(), static_cast<std::size_t>(chunk.size())));
		if (written.status != Status::Ok) {
			src.Seek(static_cast<std::ptrdiff_t>(start), Position::Absolute);
			return {written.status, done + written.count};
		}
		done = done + written.count;
	}
	return {Status::Ok, done};
}

Result BufferedWriter::Write(FIFO& src) {
	return Write(static_cast<const FIFO&>(src));
}

Result BufferedWriter::Write(const std::span<const std::byte> src) {
	std::lock_guard operation(m_operations);
	{
		StormByte::Safe::UniqueLock lock(m_mutex);
		if (!m_open || m_failed || m_state != StormByte::Buffer::IO::State::Idle)
			return { Status::Failed, 0 };
		if (src.empty())
			return { Status::Ok, 0 };
		if (!WouldAccept(StormByte::ByteSize{src.size()})) {
			++m_try_again;
			if (StormByte::Buffer::IO::WriteTelemetry* io = IoTelemetry())
				StormByte::Buffer::Backend::StoreTelemetryCounter(io->m_try_again, m_try_again);
			return { Status::TryAgain, 0 };
		}
	}
	return WriteSpan(src);
}

Result BufferedWriter::WriteSpan(const std::span<const std::byte> src) {
	if (src.empty())
		return { Status::Ok, 0 };

	const auto started = std::chrono::steady_clock::now();
	std::optional<StormByte::Buffer::Telemetry::OperationSample> telemetry_sample;
	if (StormByte::Buffer::IO::WriteTelemetry* io = IoTelemetry())
		telemetry_sample.emplace(io->MeasureOperation());
	const StormByte::ByteSize n{src.size()};
	if (n > StormByte::ByteSize{static_cast<std::size_t>(std::numeric_limits<std::ptrdiff_t>::max())} - m_tell)
		return {Status::Failed, 0};

	if (PageMode()) {
		StormByte::Safe::UniqueLock lock(m_mutex);
		std::size_t done = 0;
		while (done < src.size()) {
			auto room = m_max_memory - PageDirty();
			auto containing = m_pages.upper_bound(static_cast<std::size_t>(m_tell));
			if (containing != m_pages.begin()) {
				const auto& page = std::prev(containing)->second;
				const auto end = page.offset + StormByte::ByteSize{page.bytes.size()};
				if (m_tell < end)
					room = room + (end - m_tell);
			}
			if (room == StormByte::ByteSize{0}) {
				++m_evicted;
				NoteWait(std::chrono::steady_clock::now() - started);
				const Result drained = MaterializeFrom(lock, m_pages.begin());
				if (drained.status != Status::Ok)
					return {drained.status, StormByte::ByteSize{done}};
				continue;
			}
			const auto take = std::min(src.size() - done, static_cast<std::size_t>(room));
			const Result stored = StorePages(src.subspan(done, take));
			if (stored.status != Status::Ok)
				return stored;
			done += take;
			m_tell = m_tell + StormByte::ByteSize{take};
			if (m_tell > m_high_water)
				m_high_water = m_tell;
			m_accepted = m_accepted + StormByte::ByteSize{take};
			m_behind = m_behind + StormByte::ByteSize{take};
			NoteDirty();
			const auto threshold = std::min(m_write_threshold, m_max_memory);
			if (threshold > StormByte::ByteSize{0} && PageDirty() >= threshold) {
				const auto low = threshold / 2;
				while (PageDirty() > low) {
					const Result drained = MaterializeFrom(lock, m_pages.begin());
					if (drained.status != Status::Ok)
						return {drained.status, StormByte::ByteSize{done}};
				}
			}
			if (StormByte::Buffer::IO::WriteTelemetry* io = IoTelemetry()) {
				StormByte::Buffer::Backend::StoreTelemetryCounter(io->m_accepted, m_accepted);
				StormByte::Buffer::Backend::StoreTelemetryCounter(io->m_behind, m_behind);
				StormByte::Buffer::Backend::StoreTelemetryCounter(io->m_hit_ahead, m_hit_ahead);
				StormByte::Buffer::Backend::StoreTelemetryCounter(io->m_hit_back, m_hit_back);
				StormByte::Buffer::Backend::StoreTelemetryCounter(io->m_miss, m_miss);
				StormByte::Buffer::Backend::StoreTelemetryCounter(io->m_high_water, m_high_water);
				StormByte::Buffer::Backend::StoreTelemetryCounter(io->m_evicted, m_evicted);
			}
		}
		NoteDirty();
		NoteWait(std::chrono::steady_clock::now() - started);
		if (telemetry_sample)
			telemetry_sample->Commit(n);
		return { Status::Ok, n };
	}

	if (!BufferedMode()) {
		Result aligned;
		Result pushed;
		Result visible;
		{
			StormByte::Safe::UniqueLock origin(m_origin_io);
			aligned = EnsureOrigin(m_tell);
			if (aligned.status == Status::Ok)
				pushed = PushAll(src);
			if (aligned.status == Status::Ok && pushed.status == Status::Ok)
				visible = InvokeOrigin([this] { return m_owner->OriginFlush(); });
		}
		if (aligned.status != Status::Ok)
			return aligned;
		if (pushed.status != Status::Ok) {
			StormByte::Safe::UniqueLock lock(m_mutex);
			m_failed = true;
			if (m_state == StormByte::Buffer::IO::State::Idle)
				m_state = StormByte::Buffer::IO::State::Fault;
			NoteWait(std::chrono::steady_clock::now() - started);
			return pushed;
		}
		if (visible.status != Status::Ok) {
			StormByte::Safe::UniqueLock lock(m_mutex);
			m_failed = true;
			if (m_state == StormByte::Buffer::IO::State::Idle)
				m_state = StormByte::Buffer::IO::State::Fault;
			NoteWait(std::chrono::steady_clock::now() - started);
			return visible;
		}
		StormByte::Safe::UniqueLock lock(m_mutex);
		m_tell = m_tell + n;
		if (m_tell > m_high_water)
			m_high_water = m_tell;
		m_origin_pos = m_tell;
		m_origin_cursor_dirty = true;
		if (m_tell > m_materialized)
			m_materialized = m_tell;
		m_accepted = m_accepted + n;
		m_direct = m_direct + n;
		NoteDirty();
		NoteWait(std::chrono::steady_clock::now() - started);
		if (StormByte::Buffer::IO::WriteTelemetry* io = IoTelemetry()) {
			StormByte::Buffer::Backend::StoreTelemetryCounter(io->m_accepted, m_accepted);
			StormByte::Buffer::Backend::StoreTelemetryCounter(io->m_direct, m_direct);
			StormByte::Buffer::Backend::StoreTelemetryCounter(io->m_origin, m_origin_bytes);
			StormByte::Buffer::Backend::StoreTelemetryCounter(io->m_materialized, m_materialized);
			StormByte::Buffer::Backend::StoreTelemetryCounter(io->m_high_water, m_high_water);
			if (telemetry_sample)
				telemetry_sample->Commit(n);
		}
		return { Status::Ok, n };
	}

	if (!m_ring)
		return { Status::Failed, 0 };

	bool need_align = false;
	StormByte::ByteSize align_to{0};
	{
		StormByte::Safe::UniqueLock lock(m_mutex);
		const StormByte::ByteSize pending = m_ring->Available();
		const StormByte::ByteSize tail = m_origin_pos + pending;
		if (m_tell != tail) {
			need_align = true;
			align_to = m_tell;
		}
	}
	if (need_align) {
		const Result drained = Flush();
		if (drained.status != Status::Ok)
			return drained;
		StormByte::Safe::UniqueLock origin(m_origin_io);
		const Result aligned = EnsureOrigin(align_to);
		if (aligned.status != Status::Ok)
			return aligned;
	}

	if (!m_ring->Write(src))
		return { Status::Failed, 0 };

	{
		StormByte::Safe::UniqueLock lock(m_mutex);
		m_tell = m_tell + n;
		if (m_tell > m_high_water)
			m_high_water = m_tell;
		m_accepted = m_accepted + n;
		m_behind = m_behind + n;
		NoteDirty();
		NoteWait(std::chrono::steady_clock::now() - started);
		if (StormByte::Buffer::IO::WriteTelemetry* io = IoTelemetry()) {
			StormByte::Buffer::Backend::StoreTelemetryCounter(io->m_accepted, m_accepted);
			StormByte::Buffer::Backend::StoreTelemetryCounter(io->m_behind, m_behind);
			StormByte::Buffer::Backend::StoreTelemetryCounter(io->m_high_water, m_high_water);
			if (telemetry_sample)
				telemetry_sample->Commit(n);
		}
	}
	RequestDrain();
	return { Status::Ok, n };
}

StormByte::ByteSize BufferedWriter::Tell() const noexcept {
	StormByte::Safe::UniqueLock lock(m_mutex);
	return m_tell;
}

StormByte::ByteSize BufferedWriter::Dirty() const noexcept {
	StormByte::Safe::UniqueLock lock(m_mutex);
	return TotalDirty();
}

void BufferedWriter::BindTelemetry(StormByte::Safe::Shared<StormByte::Buffer::WriteTelemetry> telemetry) noexcept {
	m_telemetry = std::move(telemetry);
}

const StormByte::Safe::Shared<StormByte::Buffer::WriteTelemetry> BufferedWriter::Telemetry() const noexcept {
	return m_telemetry;
}

StormByte::ByteSize BufferedWriter::WriteChunk() const noexcept {
	StormByte::Safe::UniqueLock lock(m_mutex);
	return m_write_chunk;
}

void BufferedWriter::WriteChunk(const StormByte::ByteSize bytes) {
	std::lock_guard operation(m_operations);
	auto configuration = Configuration();
	configuration.WriteChunk = bytes;
	if (Configure(configuration).status != Status::Ok)
		throw StormByte::Buffer::Exception("Unable to reconfigure writer chunk");
}

std::size_t BufferedWriter::BackPressure() const noexcept {
	StormByte::Safe::UniqueLock lock(m_mutex);
	return m_back_pressure;
}

void BufferedWriter::BackPressure(const std::size_t chunks) {
	std::lock_guard operation(m_operations);
	auto configuration = Configuration();
	configuration.BackPressure = chunks;
	if (Configure(configuration).status != Status::Ok)
		throw StormByte::Buffer::Exception("Unable to reconfigure writer queue");
}

StormByte::ByteSize BufferedWriter::MaxMemory() const noexcept {
	StormByte::Safe::UniqueLock lock(m_mutex);
	return m_max_memory;
}

void BufferedWriter::MaxMemory(const StormByte::ByteSize bytes) {
	std::lock_guard operation(m_operations);
	auto configuration = Configuration();
	configuration.MaxMemory = bytes;
	if (Configure(configuration).status != Status::Ok)
		throw StormByte::Buffer::Exception("Unable to reconfigure writer memory");
}

std::chrono::milliseconds BufferedWriter::MaxWait() const noexcept {
	StormByte::Safe::UniqueLock lock(m_mutex);
	return m_max_wait;
}

void BufferedWriter::MaxWait(const std::chrono::milliseconds wait) {
	std::lock_guard operation(m_operations);
	auto configuration = Configuration();
	configuration.MaxWaitMilliseconds = wait.count();
	if (Configure(configuration).status != Status::Ok)
		throw StormByte::Buffer::Exception("Unable to reconfigure writer wait");
}

StormByte::ByteSize BufferedWriter::WriteTransfer() const noexcept {
	StormByte::Safe::UniqueLock lock(m_mutex);
	return m_write_transfer;
}

void BufferedWriter::WriteTransfer(const StormByte::ByteSize bytes) {
	std::lock_guard operation(m_operations);
	auto configuration = Configuration();
	configuration.WriteTransfer = bytes;
	if (Configure(configuration).status != Status::Ok)
		throw StormByte::Buffer::Exception("Unable to reconfigure writer transfer");
}

StormByte::ByteSize BufferedWriter::WriteThreshold() const noexcept {
	StormByte::Safe::UniqueLock lock(m_mutex);
	return m_write_threshold;
}

void BufferedWriter::WriteThreshold(const StormByte::ByteSize bytes) {
	std::lock_guard operation(m_operations);
	auto configuration = Configuration();
	configuration.WriteThreshold = bytes;
	if (Configure(configuration).status != Status::Ok)
		throw StormByte::Buffer::Exception("Unable to reconfigure writer threshold");
}

StormByte::Buffer::IO::WriterConfiguration BufferedWriter::Configuration() const noexcept {
	StormByte::Safe::UniqueLock lock(m_mutex);
	return {m_write_chunk, m_back_pressure, m_max_wait.count(), m_max_memory, m_write_transfer, m_write_threshold};
}

Result BufferedWriter::Configure(const StormByte::Buffer::IO::WriterConfiguration configuration) {
	std::lock_guard operation(m_operations);
	const auto limit = static_cast<std::size_t>(std::numeric_limits<std::ptrdiff_t>::max());
	const auto chunk = static_cast<std::size_t>(configuration.WriteChunk);
	if (chunk > limit || static_cast<std::size_t>(configuration.MaxMemory) > limit
			|| static_cast<std::size_t>(configuration.WriteTransfer) > limit
			|| static_cast<std::size_t>(configuration.WriteThreshold) > limit
			|| configuration.MaxWaitMilliseconds < 0
			|| std::chrono::milliseconds{configuration.MaxWaitMilliseconds} > std::chrono::duration_cast<std::chrono::milliseconds>(
				std::chrono::steady_clock::time_point::max() - std::chrono::steady_clock::now())
			|| (chunk != 0 && configuration.BackPressure > limit / chunk))
		return {Status::Failed, 0};
	const auto cap = configuration.MaxMemory == StormByte::ByteSize{0}
		? StormByte::ByteSize{chunk * configuration.BackPressure} : StormByte::ByteSize{0};
	if (cap >= StormByte::ByteSize{(limit + 1) / 2})
		return {Status::Failed, 0};
	const bool opened = IsOpen();
	if (opened) {
		const Result flushed = Flush();
		if (flushed.status != Status::Ok)
			return flushed;
	}
	StormByte::Safe::Unique<LockFreeRing> replacement;
	if (cap > StormByte::ByteSize{0}) {
		std::size_t reserved = 16;
		while (reserved <= static_cast<std::size_t>(cap))
			reserved *= 2;
		{
			StormByte::Safe::UniqueLock lock(m_mutex);
			m_transfers.Reconfiguration = StormByte::ByteSize{reserved};
		}
		try {
			// LockFreeRing keeps one spare slot; prevent Grow while the worker borrows storage.
			replacement = StormByte::Safe::MakeUnique<LockFreeRing>(cap + StormByte::ByteSize{1});
		}
		catch (const std::bad_alloc&) {
			StormByte::Safe::UniqueLock lock(m_mutex);
			m_transfers.Reconfiguration = StormByte::ByteSize{0};
			return {Status::Error, 0};
		}
	}
	StormByte::Safe::UniqueLock lock(m_mutex);
	m_ring = std::move(replacement);
	m_transfers.Reconfiguration = StormByte::ByteSize{0};
	m_write_chunk = configuration.WriteChunk;
	m_back_pressure = configuration.BackPressure;
	m_max_memory = configuration.MaxMemory;
	m_max_wait = std::chrono::milliseconds{configuration.MaxWaitMilliseconds};
	m_write_transfer = configuration.WriteTransfer;
	m_write_threshold = configuration.WriteThreshold;
	NoteDirty();
	return {Status::Ok, 0};
}

StormByte::Buffer::IO::TransferSnapshot BufferedWriter::Transfers() const noexcept {
	StormByte::Safe::UniqueLock lock(m_mutex);
	auto snapshot = m_transfers;
	snapshot.TransferLimit = m_write_transfer;
	if (PageMode())
		snapshot.TransferLimit = std::min(m_max_memory, m_write_transfer > StormByte::ByteSize{0}
			? m_write_transfer : StormByte::ByteSize{64 * 1024});
	snapshot.PageCapacity = m_max_memory;
	snapshot.RingCapacity = m_ring ? m_ring->Capacity() : StormByte::ByteSize{0};
	snapshot.QueueCapacity = m_ring ? PendingCap() : StormByte::ByteSize{0};
	snapshot.Threshold = std::min(m_write_threshold, m_max_memory);
	snapshot.Pending = TotalDirty();
	return snapshot;
}

void BufferedWriter::StartWorker() {
	if (m_worker.joinable())
		return;
	m_stop.store(false, StormByte::Safe::MemoryOrder::Release);
	m_worker = StormByte::Safe::Thread([this] { Worker(); });
}

void BufferedWriter::StopWorker() {
	{
		StormByte::Safe::UniqueLock lock(m_mutex);
		m_stop.store(true, StormByte::Safe::MemoryOrder::Release);
	}
	m_cv.notify_all();
	if (m_worker.joinable())
		m_worker.join();
}

void BufferedWriter::RequestDrain() const {
	StormByte::Safe::UniqueLock lock(m_mutex);
	if (!BufferedMode() || !m_ring)
		return;
	if (m_ring->Available() < m_write_chunk
			&& !m_flush.load(StormByte::Safe::MemoryOrder::Acquire))
		return;
	m_drain_run = true;
	m_cv.notify_all();
}

void BufferedWriter::Worker() {
	for (;;) {
		StormByte::Safe::UniqueLock lock(m_mutex);
		m_cv.wait(lock, [this] {
			return m_stop.load(StormByte::Safe::MemoryOrder::Acquire)
				|| m_drain_run
				|| m_flush.load(StormByte::Safe::MemoryOrder::Acquire);
		});
		if (m_stop.load(StormByte::Safe::MemoryOrder::Acquire)) {
			m_cv.notify_all();
			return;
		}

		const StormByte::ByteSize chunk = m_write_chunk;
		lock.unlock();

		while (!m_stop.load(StormByte::Safe::MemoryOrder::Acquire) && m_ring && m_owner) {
			const bool flush = m_flush.load(StormByte::Safe::MemoryOrder::Acquire);
			const StormByte::ByteSize dirty = m_ring->Available();
			if (dirty == StormByte::ByteSize{0})
				break;
			if (!flush && dirty < chunk)
				break;

			auto front = m_ring->BorrowFrontSpan();
			if (front.empty())
				break;
			const StormByte::ByteSize want = flush
				? StormByte::ByteSize{front.size()}
				: std::min(StormByte::ByteSize{front.size()}, chunk);
			front = front.first(static_cast<std::size_t>(want));

			StormByte::ByteSize start{0};
			{
				StormByte::Safe::UniqueLock inner(m_mutex);
				start = m_origin_pos;
			}

			Result aligned;
			Result pushed;
			bool live = false;
			{
				StormByte::Safe::UniqueLock origin(m_origin_io);
				{
					StormByte::Safe::UniqueLock inner(m_mutex);
					live = m_open && !m_failed;
				}
				if (live) {
					aligned = EnsureOrigin(start);
					if (aligned.status == Status::Ok)
						pushed = PushAll(front);
				}
			}
			if (!live)
				break;
			if (aligned.status != Status::Ok) {
				StormByte::Safe::UniqueLock inner(m_mutex);
				m_failed = true;
				if (m_state == StormByte::Buffer::IO::State::Idle)
					m_state = StormByte::Buffer::IO::State::Fault;
				m_cv.notify_all();
				break;
			}
			if (pushed.status != Status::Ok) {
				static_cast<void>(m_ring->Consume(pushed.count));
				StormByte::Safe::UniqueLock inner(m_mutex);
				m_failed = true;
				if (m_state == StormByte::Buffer::IO::State::Idle)
					m_state = StormByte::Buffer::IO::State::Fault;
				m_cv.notify_all();
				break;
			}
			static_cast<void>(m_ring->Consume(StormByte::ByteSize{front.size()}));
			{
				StormByte::Safe::UniqueLock inner(m_mutex);
				m_origin_pos = start + StormByte::ByteSize{front.size()};
				if (m_origin_pos > m_materialized)
					m_materialized = m_origin_pos;
				if (StormByte::Buffer::IO::WriteTelemetry* io = IoTelemetry()) {
					StormByte::Buffer::Backend::StoreTelemetryCounter(io->m_origin, m_origin_bytes);
					StormByte::Buffer::Backend::StoreTelemetryCounter(io->m_materialized, m_materialized);
				}
			}
		}

		lock.lock();
		m_drain_run = false;
		m_cv.notify_all();
		if (!m_stop.load(StormByte::Safe::MemoryOrder::Acquire)
				&& !m_failed
				&& m_ring
				&& (m_ring->Available() >= m_write_chunk
					|| (m_flush.load(StormByte::Safe::MemoryOrder::Acquire)
						&& m_ring->Available() > StormByte::ByteSize{0}))) {
			m_drain_run = true;
			continue;
		}
	}
}

Result BufferedWriter::PushAll(const std::span<const std::byte> data) {
	if (!m_owner)
		return { Status::Failed, 0 };
	if (data.empty())
		return { Status::Ok, 0 };

	StormByte::ByteSize transfer{0};
	std::chrono::milliseconds wait{0};
	{
		StormByte::Safe::UniqueLock lock(m_mutex);
		transfer = m_write_transfer;
		if (PageMode())
			transfer = std::min(m_max_memory, transfer > StormByte::ByteSize{0}
				? transfer : StormByte::ByteSize{64 * 1024});
		wait = m_max_wait;
	}
	const auto deadline = wait.count() == 0
		? std::chrono::steady_clock::time_point::max()
		: std::chrono::steady_clock::now() + wait;

	StormByte::ByteSize off{0};
	while (off < StormByte::ByteSize{data.size()}) {
		if (m_stop.load(StormByte::Safe::MemoryOrder::Acquire))
			return { Status::Failed, 0 };
		if (wait.count() != 0 && std::chrono::steady_clock::now() >= deadline)
			return { Status::Error, off };

		auto rest = data.subspan(static_cast<std::size_t>(off));
		if (transfer > StormByte::ByteSize{0})
			rest = rest.first(std::min(rest.size(), static_cast<std::size_t>(transfer)));
		{
			StormByte::Safe::UniqueLock lock(m_mutex);
			++m_transfers.Calls;
			m_transfers.LastRequest = StormByte::ByteSize{rest.size()};
			m_transfers.MaxRequest = std::max(m_transfers.MaxRequest, m_transfers.LastRequest);
			m_transfers.InFlight = StormByte::ByteSize{rest.size()};
		}
		const Result pushed = InvokeOrigin([this, rest] { return m_owner->OriginPush(rest); });
		{
			StormByte::Safe::UniqueLock lock(m_mutex);
			m_transfers.InFlight = StormByte::ByteSize{0};
			if (pushed.count > StormByte::ByteSize{rest.size()})
				return {Status::Error, off};
			m_transfers.Bytes = m_transfers.Bytes + pushed.count;
			m_origin_bytes = m_origin_bytes + pushed.count;
			m_origin_pos = m_origin_pos + pushed.count;
			m_materialized = std::max(m_materialized, m_origin_pos);
			if (StormByte::Buffer::IO::WriteTelemetry* io = IoTelemetry()) {
				StormByte::Buffer::Backend::StoreTelemetryCounter(io->m_origin, m_origin_bytes);
				StormByte::Buffer::Backend::StoreTelemetryCounter(io->m_materialized, m_materialized);
			}
		}
		off = off + pushed.count;
		if (pushed.status == Status::Failed || pushed.status == Status::Error)
			return { pushed.status, off };
		if (pushed.status == Status::TryAgain && pushed.count == StormByte::ByteSize{0}) {
			{
				StormByte::Safe::UniqueLock lock(m_mutex);
				++m_try_again;
				if (StormByte::Buffer::IO::WriteTelemetry* io = IoTelemetry())
					StormByte::Buffer::Backend::StoreTelemetryCounter(io->m_try_again, m_try_again);
			}
			std::this_thread::yield();
			continue;
		}
		if (pushed.count == StormByte::ByteSize{0} || pushed.status == Status::End)
			return {Status::Error, off};
	}
	return { Status::Ok, StormByte::ByteSize{data.size()} };
}
