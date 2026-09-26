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

#include <StormByte/buffer/io/backend/buffered_writer.hxx>
#include <StormByte/buffer/lockfree_ring.hxx>

#include <algorithm>
#include <mutex>
#include <vector>

using namespace StormByte::Buffer::IO::Backend;
using Result = StormByte::Buffer::IO::Result;
using State = StormByte::Buffer::IO::State;
using Status = StormByte::Buffer::IO::Status;
using BinaryData = StormByte::BinaryData;
using Position = StormByte::Buffer::Position;

BufferedWriter::BufferedWriter(IO::BufferedWriter& owner, StormByte::String::String path,
		const IO::Location location, const StormByte::ByteSize write_chunk,
		const std::size_t back_pressure, const std::chrono::milliseconds max_wait,
		const StormByte::ByteSize max_memory):
	m_owner(&owner),
	m_path(std::move(path)),
	m_location(location),
	m_write_chunk(write_chunk),
	m_back_pressure(back_pressure),
	m_max_memory(max_memory),
	m_max_wait(max_wait),
	m_state(State::Unavailable) {
	if (BufferedMode())
		m_ring = std::make_unique<LockFreeRing>(PendingCap());
	StartWorker();
}

BufferedWriter::~BufferedWriter() {
	Shutdown();
}

void BufferedWriter::Rebind(IO::BufferedWriter& owner) noexcept {
	m_owner = &owner;
}

const StormByte::String::String& BufferedWriter::Path() const noexcept {
	return m_path;
}

StormByte::Buffer::IO::Location BufferedWriter::Location() const noexcept {
	return m_location;
}

BufferedWriter::operator bool() const noexcept {
	std::lock_guard lock(m_mutex);
	return m_state == State::Idle;
}

State BufferedWriter::State() const noexcept {
	std::lock_guard lock(m_mutex);
	return m_state;
}

void BufferedWriter::SetState(const enum State state) noexcept {
	std::lock_guard lock(m_mutex);
	m_state = state;
}

void BufferedWriter::SetTell(const StormByte::ByteSize offset) noexcept {
	std::lock_guard lock(m_mutex);
	m_tell = offset;
	if (m_tell > m_high_water)
		m_high_water = m_tell;
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
	return m_back_pressure * m_write_chunk;
}

StormByte::ByteSize BufferedWriter::PageDirty() const noexcept {
	StormByte::ByteSize n{0};
	for (const auto& item : m_pages)
		n = n + StormByte::ByteSize{item.second.bytes.size()};
	return n;
}

StormByte::ByteSize BufferedWriter::TotalDirty() const noexcept {
	StormByte::ByteSize n = PageDirty();
	if (m_ring)
		n = n + m_ring->Available();
	return n;
}

bool BufferedWriter::WouldAccept(const StormByte::ByteSize bytes) const noexcept {
	if (PageMode())
		return true;
	if (!BufferedMode())
		return true;
	if (!m_ring)
		return false;
	return m_ring->Available() + bytes <= PendingCap();
}

bool BufferedWriter::WillWrite(const StormByte::ByteSize n) const noexcept {
	std::lock_guard lock(m_mutex);
	return WouldAccept(n);
}

void BufferedWriter::ClearPages() noexcept {
	m_pages.clear();
}

void BufferedWriter::CloseSeekEpoch() noexcept {
	if (!m_epoch_open)
		return;
	if (m_epoch_hit && !m_epoch_origin)
		++m_seek_saved_full;
	else if (m_epoch_hit && m_epoch_origin)
		++m_seek_saved_partial;
	m_epoch_open = false;
	m_epoch_hit = false;
	m_epoch_origin = false;
}

struct StormByte::Buffer::IO::BufferedWriter::Telemetry BufferedWriter::Telemetry() const noexcept {
	std::lock_guard lock(m_mutex);
	struct StormByte::Buffer::IO::BufferedWriter::Telemetry out;
	out.Accepted = m_accepted;
	out.Behind = m_behind;
	out.Direct = m_direct;
	out.Origin = m_origin_bytes;
	out.Materialized = m_materialized;
	out.HighWater = m_high_water;
	out.HitAhead = m_hit_ahead;
	out.HitBack = m_hit_back;
	out.Miss = m_miss;
	out.Dirty = TotalDirty();
	out.DirtyPeak = m_dirty_peak;
	out.Cap = PendingCap();
	out.SeekLogical = m_seek_logical;
	out.SeekOrigin = m_seek_origin;
	out.SeekSavedFull = m_seek_saved_full;
	out.SeekSavedPartial = m_seek_saved_partial;
	out.TryAgain = m_try_again;
	out.Saturated = m_saturated;
	out.Evicted = m_evicted;
	out.WaitMin = m_wait_min;
	out.WaitMax = m_wait_max;
	out.WaitTotal = m_wait_total;
	out.WaitSamples = m_wait_samples;
	return out;
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
}

void BufferedWriter::NoteDirty() const noexcept {
	const StormByte::ByteSize now = TotalDirty();
	if (now > m_dirty_peak)
		m_dirty_peak = now;
	const StormByte::ByteSize cap = PendingCap();
	if (cap > StormByte::ByteSize{0} && m_ring && m_ring->Available() >= cap)
		++m_saturated;
}

bool BufferedWriter::Open() {
	if (!m_owner)
		return false;

	bool already = false;
	{
		std::lock_guard lock(m_mutex);
		already = m_open;
	}

	Result opened;
	{
		std::lock_guard origin(m_origin_io);
		opened = m_owner->OriginOpen();
	}

	std::lock_guard lock(m_mutex);
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
	ClearPages();
	m_epoch_open = false;
	m_epoch_hit = false;
	m_epoch_origin = false;
	if (m_ring)
		m_ring->Clear();
	return m_state == State::Idle;
}

bool BufferedWriter::Close() {
	const Result flushed = Flush();

	bool was_open = false;
	{
		std::lock_guard lock(m_mutex);
		was_open = m_open;
		CloseSeekEpoch();
	}

	if (was_open && m_owner) {
		std::lock_guard origin(m_origin_io);
		static_cast<void>(m_owner->OriginClose());
	}

	std::lock_guard lock(m_mutex);
	m_open = false;
	if (flushed.status == Status::Error) {
		m_failed = true;
		m_state = State::Fault;
		return false;
	}
	if (flushed.status == Status::Failed && was_open) {
		m_failed = true;
		m_state = State::Fault;
		return false;
	}
	m_failed = false;
	m_state = State::Unavailable;
	ClearPages();
	if (m_ring)
		m_ring->Clear();
	return true;
}

void BufferedWriter::Shutdown() {
	m_stop.store(true, std::memory_order_release);
	m_cv.notify_all();
	StopWorker();
	std::lock_guard lock(m_mutex);
	m_open = false;
	m_state = State::Unavailable;
}

bool BufferedWriter::Rewind() {
	{
		std::lock_guard lock(m_mutex);
		if (!m_open)
			return false;
	}
	if (!Close())
		return false;
	return Open();
}

bool BufferedWriter::IsOpen() const noexcept {
	std::lock_guard lock(m_mutex);
	return m_open;
}

Result BufferedWriter::EnsureOrigin(const StormByte::ByteSize absolute) {
	if (!m_owner)
		return { Status::Failed, 0 };

	bool skip = false;
	{
		std::lock_guard lock(m_mutex);
		skip = !m_origin_cursor_dirty && m_origin_pos == absolute;
	}
	if (skip)
		return { Status::Ok, 0 };

	const Result moved = m_owner->OriginSeek(absolute);
	if (moved.status != Status::Ok)
		return moved;

	std::lock_guard lock(m_mutex);
	m_origin_pos = absolute;
	m_origin_cursor_dirty = false;
	++m_seek_origin;
	if (m_epoch_open)
		m_epoch_origin = true;
	return { Status::Ok, 0 };
}

void BufferedWriter::Coalesce(const StormByte::ByteSize offset) {
	auto it = m_pages.find(static_cast<std::size_t>(offset));
	if (it == m_pages.end())
		return;

	for (;;) {
		auto next = std::next(it);
		if (next == m_pages.end())
			break;
		const StormByte::ByteSize end = it->second.offset + StormByte::ByteSize{it->second.bytes.size()};
		if (next->second.offset > end)
			break;
		const StormByte::ByteSize overlap = end - next->second.offset;
		const StormByte::ByteSize keep = StormByte::ByteSize{next->second.bytes.size()} > overlap
			? StormByte::ByteSize{next->second.bytes.size()} - overlap
			: StormByte::ByteSize{0};
		if (keep > StormByte::ByteSize{0}) {
			const std::byte* src = next->second.bytes.data() + static_cast<std::size_t>(overlap);
			it->second.bytes.insert(it->second.bytes.end(),
				std::span<const std::byte>(src, static_cast<std::size_t>(keep)));
		}
		m_pages.erase(next);
	}
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
	else {
		m_miss = m_miss + n;
	}

	auto it = m_pages.upper_bound(static_cast<std::size_t>(at));
	if (it != m_pages.begin()) {
		auto prev = std::prev(it);
		const StormByte::ByteSize p1 = prev->second.offset + StormByte::ByteSize{prev->second.bytes.size()};
		if (p1 >= at)
			it = prev;
	}

	if (it != m_pages.end()) {
		Page& page = it->second;
		const StormByte::ByteSize p0 = page.offset;
		const StormByte::ByteSize p1 = p0 + StormByte::ByteSize{page.bytes.size()};
		if (p1 >= at && p0 <= range_end) {
			if (at < p0) {
				BinaryData grown;
				grown.reserve(n + StormByte::ByteSize{page.bytes.size()});
				grown.insert(grown.end(), src);
				if (range_end < p1) {
					const std::byte* tail = page.bytes.data() + static_cast<std::size_t>(range_end - p0);
					grown.insert(grown.end(), std::span<const std::byte>(tail,
						static_cast<std::size_t>(p1 - range_end)));
				}
				page.offset = at;
				page.bytes = std::move(grown);
			}
			else {
				const StormByte::ByteSize off = at - p0;
				if (off + n > StormByte::ByteSize{page.bytes.size()})
					page.bytes.resize(static_cast<std::size_t>(off + n));
				std::copy(src.begin(), src.end(), page.bytes.data() + static_cast<std::size_t>(off));
			}
			Coalesce(page.offset);
			return { Status::Ok, n };
		}
	}

	Page fresh;
	fresh.offset = at;
	fresh.bytes = BinaryData(src.data(), n);
	m_pages.emplace(static_cast<std::size_t>(at), std::move(fresh));
	Coalesce(at);
	return { Status::Ok, n };
}

Result BufferedWriter::MaterializeFrom(std::unique_lock<std::mutex>& lock,
		std::map<std::size_t, Page>::iterator it) {
	if (it == m_pages.end() || !m_owner)
		return { Status::Failed, 0 };

	Page page = std::move(it->second);
	m_pages.erase(it);

	const StormByte::ByteSize start = page.offset;
	BinaryData payload = std::move(page.bytes);
	lock.unlock();

	Result ensured;
	Result pushed;
	{
		std::lock_guard origin(m_origin_io);
		ensured = EnsureOrigin(start);
		if (ensured.status == Status::Ok) {
			const auto view = std::span<const std::byte>(payload.data(),
				static_cast<std::size_t>(payload.size()));
			pushed = PushAll(view);
		}
	}
	lock.lock();
	if (ensured.status != Status::Ok) {
		Page back;
		back.offset = start;
		back.bytes = std::move(payload);
		m_pages.emplace(static_cast<std::size_t>(start), std::move(back));
		return ensured;
	}
	if (pushed.status != Status::Ok) {
		Page back;
		back.offset = start;
		back.bytes = std::move(payload);
		m_pages.emplace(static_cast<std::size_t>(start), std::move(back));
		m_failed = true;
		if (m_state == State::Idle)
			m_state = State::Fault;
		return pushed;
	}

	const StormByte::ByteSize n{payload.size()};
	m_origin_pos = start + n;
	m_origin_bytes = m_origin_bytes + n;
	const StormByte::ByteSize end = start + n;
	if (end > m_materialized)
		m_materialized = end;
	return { Status::Ok, n };
}

Result BufferedWriter::CollectGarbage() {
	std::unique_lock lock(m_mutex);
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
		const Result evicted = MaterializeFrom(lock, victim);
		if (evicted.status != Status::Ok)
			return evicted;
	}
	return { Status::Ok, 0 };
}

Result BufferedWriter::MaterializeAll() {
	std::unique_lock lock(m_mutex);
	while (!m_pages.empty()) {
		const Result pushed = MaterializeFrom(lock, m_pages.begin());
		if (pushed.status != Status::Ok)
			return pushed;
	}
	return { Status::Ok, 0 };
}

Result BufferedWriter::Flush() {
	const Result pages = MaterializeAll();
	if (pages.status != Status::Ok)
		return pages;

	std::unique_lock lock(m_mutex);
	if (!m_open || m_failed || !m_owner)
		return { Status::Failed, 0 };

	const bool ring_dirty = m_ring && m_ring->Available() > StormByte::ByteSize{0};
	if (ring_dirty) {
		m_flush.store(true, std::memory_order_release);
		m_drain_run = true;
		m_cv.notify_all();
	}
	m_cv.wait(lock, [this] {
		const bool empty = !m_ring || m_ring->Available() == StormByte::ByteSize{0};
		return m_stop.load(std::memory_order_acquire)
			|| m_failed
			|| (empty && !m_drain_run);
	});
	m_flush.store(false, std::memory_order_release);
	if (m_failed)
		return { Status::Error, 0 };
	if (m_stop.load(std::memory_order_acquire))
		return { Status::Failed, 0 };
	lock.unlock();

	Result visible;
	{
		std::lock_guard origin(m_origin_io);
		visible = m_owner->OriginFlush();
	}
	{
		std::lock_guard dirty(m_mutex);
		m_origin_cursor_dirty = true;
		if (visible.status != Status::Ok) {
			m_failed = true;
			if (m_state == State::Idle)
				m_state = State::Fault;
			return visible;
		}
	}
	return { Status::Ok, 0 };
}

Result BufferedWriter::Truncate() {
	{
		std::lock_guard lock(m_mutex);
		if (!m_open || m_failed || !m_owner)
			return { Status::Failed, 0 };
		ClearPages();
		if (m_ring)
			m_ring->Clear();
		CloseSeekEpoch();
	}

	Result truncated;
	{
		std::lock_guard origin(m_origin_io);
		truncated = m_owner->OriginTruncate();
	}
	std::lock_guard lock(m_mutex);
	if (truncated.status != Status::Ok) {
		m_failed = true;
		m_state = State::Fault;
		return { Status::Failed, 0 };
	}
	m_tell = StormByte::ByteSize{0};
	m_high_water = StormByte::ByteSize{0};
	m_origin_pos = StormByte::ByteSize{0};
	m_origin_cursor_dirty = true;
	m_materialized = StormByte::ByteSize{0};
	return { Status::Ok, 0 };
}

Result BufferedWriter::Seek(const std::ptrdiff_t offset, const Position mode) {
	std::lock_guard lock(m_mutex);
	if (!m_open || m_failed || m_state != State::Idle || !m_owner)
		return { Status::Failed, 0 };

	StormByte::ByteSize abs = m_tell;
	if (mode == Position::Absolute) {
		if (offset < 0)
			return { Status::Failed, 0 };
		abs = StormByte::ByteSize{static_cast<std::size_t>(offset)};
	}
	else {
		if (offset < 0 && StormByte::ByteSize{static_cast<std::size_t>(-offset)} > abs)
			return { Status::Failed, 0 };
		abs = StormByte::ByteSize{static_cast<std::size_t>(
			static_cast<std::ptrdiff_t>(static_cast<std::size_t>(abs)) + offset)};
	}

	CloseSeekEpoch();
	m_tell = abs;
	if (m_tell > m_high_water)
		m_high_water = m_tell;
	m_epoch_open = true;
	m_epoch_hit = false;
	m_epoch_origin = false;
	++m_seek_logical;
	return { Status::Ok, 0 };
}

Result BufferedWriter::Write(const FIFO& src) {
	const StormByte::ByteSize need = src.Available();
	if (need == StormByte::ByteSize{0})
		return { Status::Ok, 0 };
	{
		std::lock_guard lock(m_mutex);
		if (!m_open || m_failed || m_state != State::Idle)
			return { Status::Failed, 0 };
		if (!WouldAccept(need)) {
			++m_try_again;
			return { Status::TryAgain, 0 };
		}
	}
	BinaryData chunk;
	if (!src.Read(need, chunk))
		return { Status::Failed, 0 };
	return WriteSpan(std::span<const std::byte>(chunk.data(), static_cast<std::size_t>(chunk.size())));
}

Result BufferedWriter::Write(FIFO& src) {
	return Write(static_cast<const FIFO&>(src));
}

Result BufferedWriter::Write(const std::span<const std::byte> src) {
	{
		std::lock_guard lock(m_mutex);
		if (!m_open || m_failed || m_state != State::Idle)
			return { Status::Failed, 0 };
		if (src.empty())
			return { Status::Ok, 0 };
		if (!WouldAccept(StormByte::ByteSize{src.size()})) {
			++m_try_again;
			return { Status::TryAgain, 0 };
		}
	}
	return WriteSpan(src);
}

Result BufferedWriter::WriteSpan(const std::span<const std::byte> src) {
	if (src.empty())
		return { Status::Ok, 0 };

	const auto started = std::chrono::steady_clock::now();
	const StormByte::ByteSize n{src.size()};

	if (PageMode()) {
		Result stored;
		{
			std::lock_guard lock(m_mutex);
			stored = StorePages(src);
			if (stored.status != Status::Ok)
				return stored;
			m_tell = m_tell + n;
			if (m_tell > m_high_water)
				m_high_water = m_tell;
			m_accepted = m_accepted + n;
			m_behind = m_behind + n;
			NoteDirty();
			NoteWait(std::chrono::steady_clock::now() - started);
		}
		const Result gc = CollectGarbage();
		if (gc.status != Status::Ok)
			return gc;
		return { Status::Ok, n };
	}

	if (!BufferedMode()) {
		Result aligned;
		Result pushed;
		Result visible;
		{
			std::lock_guard origin(m_origin_io);
			aligned = EnsureOrigin(m_tell);
			if (aligned.status == Status::Ok)
				pushed = PushAll(src);
			if (aligned.status == Status::Ok && pushed.status == Status::Ok)
				visible = m_owner->OriginFlush();
		}
		if (aligned.status != Status::Ok)
			return aligned;
		if (pushed.status != Status::Ok) {
			std::lock_guard lock(m_mutex);
			m_failed = true;
			if (m_state == State::Idle)
				m_state = State::Fault;
			NoteWait(std::chrono::steady_clock::now() - started);
			return pushed;
		}
		if (visible.status != Status::Ok) {
			std::lock_guard lock(m_mutex);
			m_failed = true;
			if (m_state == State::Idle)
				m_state = State::Fault;
			NoteWait(std::chrono::steady_clock::now() - started);
			return visible;
		}
		std::lock_guard lock(m_mutex);
		m_tell = m_tell + n;
		if (m_tell > m_high_water)
			m_high_water = m_tell;
		m_origin_pos = m_tell;
		m_origin_cursor_dirty = true;
		m_origin_bytes = m_origin_bytes + n;
		if (m_tell > m_materialized)
			m_materialized = m_tell;
		m_accepted = m_accepted + n;
		m_direct = m_direct + n;
		NoteWait(std::chrono::steady_clock::now() - started);
		return { Status::Ok, n };
	}

	if (!m_ring)
		return { Status::Failed, 0 };

	bool need_align = false;
	StormByte::ByteSize align_to{0};
	{
		std::lock_guard lock(m_mutex);
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
		std::lock_guard origin(m_origin_io);
		const Result aligned = EnsureOrigin(align_to);
		if (aligned.status != Status::Ok)
			return aligned;
	}

	if (!m_ring->Write(src))
		return { Status::Failed, 0 };

	{
		std::lock_guard lock(m_mutex);
		m_tell = m_tell + n;
		if (m_tell > m_high_water)
			m_high_water = m_tell;
		m_accepted = m_accepted + n;
		m_behind = m_behind + n;
		NoteDirty();
		NoteWait(std::chrono::steady_clock::now() - started);
	}
	RequestDrain();
	return { Status::Ok, n };
}

StormByte::ByteSize BufferedWriter::Tell() const noexcept {
	std::lock_guard lock(m_mutex);
	return m_tell;
}

StormByte::ByteSize BufferedWriter::Dirty() const noexcept {
	std::lock_guard lock(m_mutex);
	return TotalDirty();
}

StormByte::ByteSize BufferedWriter::WriteChunk() const noexcept {
	std::lock_guard lock(m_mutex);
	return m_write_chunk;
}

void BufferedWriter::WriteChunk(const StormByte::ByteSize bytes) {
	const bool disable = bytes == StormByte::ByteSize{0} || m_back_pressure == 0;
	if (disable && Dirty() > StormByte::ByteSize{0})
		static_cast<void>(Flush());

	{
		std::lock_guard lock(m_mutex);
		m_write_chunk = bytes;
		if (!BufferedMode()) {
			m_ring.reset();
			return;
		}
		if (!m_ring)
			m_ring = std::make_unique<LockFreeRing>(PendingCap());
	}
	RequestDrain();
}

std::size_t BufferedWriter::BackPressure() const noexcept {
	std::lock_guard lock(m_mutex);
	return m_back_pressure;
}

void BufferedWriter::BackPressure(const std::size_t chunks) {
	const StormByte::ByteSize unit = WriteChunk();
	const StormByte::ByteSize cap = (chunks == 0 || unit == StormByte::ByteSize{0})
		? StormByte::ByteSize{0}
		: chunks * unit;
	if (cap == StormByte::ByteSize{0} || Dirty() > cap)
		static_cast<void>(Flush());

	{
		std::lock_guard lock(m_mutex);
		m_back_pressure = chunks;
		if (!BufferedMode()) {
			m_ring.reset();
			return;
		}
		if (!m_ring)
			m_ring = std::make_unique<LockFreeRing>(PendingCap());
	}
	RequestDrain();
}

StormByte::ByteSize BufferedWriter::MaxMemory() const noexcept {
	std::lock_guard lock(m_mutex);
	return m_max_memory;
}

void BufferedWriter::MaxMemory(const StormByte::ByteSize bytes) {
	{
		std::lock_guard lock(m_mutex);
		m_max_memory = bytes;
	}
	if (bytes == StormByte::ByteSize{0})
		static_cast<void>(Flush());
	else
		static_cast<void>(CollectGarbage());
}

std::chrono::milliseconds BufferedWriter::MaxWait() const noexcept {
	std::lock_guard lock(m_mutex);
	return m_max_wait;
}

void BufferedWriter::MaxWait(const std::chrono::milliseconds wait) {
	std::lock_guard lock(m_mutex);
	m_max_wait = wait;
}

void BufferedWriter::StartWorker() {
	if (m_worker.joinable())
		return;
	m_stop.store(false, std::memory_order_release);
	m_worker = std::thread(&BufferedWriter::Worker, this);
}

void BufferedWriter::StopWorker() {
	m_stop.store(true, std::memory_order_release);
	m_cv.notify_all();
	if (m_worker.joinable())
		m_worker.join();
}

void BufferedWriter::RequestDrain() const {
	std::lock_guard lock(m_mutex);
	if (!BufferedMode() || !m_ring)
		return;
	if (m_ring->Available() < m_write_chunk
			&& !m_flush.load(std::memory_order_acquire))
		return;
	m_drain_run = true;
	m_cv.notify_all();
}

void BufferedWriter::Worker() {
	for (;;) {
		std::unique_lock lock(m_mutex);
		m_cv.wait(lock, [this] {
			return m_stop.load(std::memory_order_acquire)
				|| m_drain_run
				|| m_flush.load(std::memory_order_acquire);
		});
		if (m_stop.load(std::memory_order_acquire)) {
			m_cv.notify_all();
			return;
		}

		const StormByte::ByteSize chunk = m_write_chunk;
		lock.unlock();

		while (!m_stop.load(std::memory_order_acquire) && m_ring && m_owner) {
			const bool flush = m_flush.load(std::memory_order_acquire);
			const StormByte::ByteSize dirty = m_ring->Available();
			if (dirty == StormByte::ByteSize{0})
				break;
			if (!flush && dirty < chunk)
				break;

			auto front = m_ring->FrontSpan();
			if (front.empty())
				break;
			const StormByte::ByteSize want = flush
				? StormByte::ByteSize{front.size()}
				: std::min(StormByte::ByteSize{front.size()}, chunk);
			front = front.first(static_cast<std::size_t>(want));

			StormByte::ByteSize start{0};
			{
				std::lock_guard inner(m_mutex);
				start = m_origin_pos;
			}

			Result aligned;
			Result pushed;
			{
				std::lock_guard origin(m_origin_io);
				aligned = EnsureOrigin(start);
				if (aligned.status == Status::Ok)
					pushed = PushAll(front);
			}
			if (aligned.status != Status::Ok) {
				std::lock_guard inner(m_mutex);
				m_failed = true;
				if (m_state == State::Idle)
					m_state = State::Fault;
				m_cv.notify_all();
				break;
			}
			if (pushed.status != Status::Ok) {
				std::lock_guard inner(m_mutex);
				m_failed = true;
				if (m_state == State::Idle)
					m_state = State::Fault;
				m_cv.notify_all();
				break;
			}
			static_cast<void>(m_ring->Consume(StormByte::ByteSize{front.size()}));
			{
				std::lock_guard inner(m_mutex);
				m_origin_pos = start + StormByte::ByteSize{front.size()};
				m_origin_bytes = m_origin_bytes + StormByte::ByteSize{front.size()};
				if (m_origin_pos > m_materialized)
					m_materialized = m_origin_pos;
			}
		}

		lock.lock();
		m_drain_run = false;
		m_cv.notify_all();
		if (!m_stop.load(std::memory_order_acquire)
				&& m_flush.load(std::memory_order_acquire)
				&& m_ring
				&& m_ring->Available() > StormByte::ByteSize{0}) {
			m_drain_run = true;
			continue;
		}
	}
}

Result BufferedWriter::PushAll(const std::span<const std::byte> data) const {
	if (!m_owner)
		return { Status::Failed, 0 };
	if (data.empty())
		return { Status::Ok, 0 };

	const auto deadline = m_max_wait.count() == 0
		? std::chrono::steady_clock::time_point::max()
		: std::chrono::steady_clock::now() + m_max_wait;

	StormByte::ByteSize off{0};
	while (off < StormByte::ByteSize{data.size()}) {
		if (m_stop.load(std::memory_order_acquire))
			return { Status::Failed, 0 };
		if (m_max_wait.count() != 0 && std::chrono::steady_clock::now() >= deadline)
			return { Status::Error, off };

		const auto rest = data.subspan(static_cast<std::size_t>(off));
		const Result pushed = m_owner->OriginPush(rest);
		if (pushed.status == Status::Failed || pushed.status == Status::Error)
			return { pushed.status, off };
		if (pushed.count == StormByte::ByteSize{0})
			return { Status::Error, off };
		if (pushed.count > StormByte::ByteSize{rest.size()})
			return { Status::Error, off };
		off = off + pushed.count;
	}
	return { Status::Ok, StormByte::ByteSize{data.size()} };
}
