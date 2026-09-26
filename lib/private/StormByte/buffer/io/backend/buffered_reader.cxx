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

#include <StormByte/buffer/io/backend/buffered_reader.hxx>

#include <limits>
#include <utility>
#include <vector>

using namespace StormByte::Buffer::IO::Backend;
using Result = StormByte::Buffer::IO::Result;
using State = StormByte::Buffer::IO::State;
using Status = StormByte::Buffer::IO::Status;
using FIFO = StormByte::Buffer::FIFO;

namespace {
	constexpr StormByte::ByteSize PullBatch{4096};

	StormByte::ByteSize SpanEnd(const StormByte::ByteSize start, const StormByte::Buffer::FIFO& fifo) noexcept {
		return start + fifo.Available();
	}

	StormByte::ByteSize DistanceToTell(const StormByte::ByteSize start, const StormByte::Buffer::FIFO& fifo,
			const StormByte::ByteSize tell) noexcept {
		const StormByte::ByteSize end = SpanEnd(start, fifo);
		if (tell >= start && tell < end)
			return StormByte::ByteSize{0};
		if (tell < start)
			return start - tell;
		return tell - end;
	}

	bool OffsetFits(const StormByte::ByteSize value) noexcept {
		return value <= StormByte::ByteSize{static_cast<std::size_t>(std::numeric_limits<std::ptrdiff_t>::max())};
	}

	void AppendFifo(FIFO& dest, FIFO& piece) {
		const StormByte::ByteSize n = piece.Available();
		if (n == StormByte::ByteSize{0})
			return;
		static_cast<void>(dest.Write(n, std::move(piece)));
	}

	void SplitHit(const StormByte::ByteSize pos, const StormByte::ByteSize take, const StormByte::ByteSize max_tell,
			StormByte::ByteSize& ahead, StormByte::ByteSize& back) noexcept {
		if (take == StormByte::ByteSize{0})
			return;
		if (pos >= max_tell) {
			ahead = ahead + take;
			return;
		}
		if (pos + take <= max_tell) {
			back = back + take;
			return;
		}
		const StormByte::ByteSize replay = max_tell - pos;
		back = back + replay;
		ahead = ahead + (take - replay);
	}
}

BufferedReader::BufferedReader(IO::BufferedReader& owner, StormByte::String::String path,
		const IO::Location location, const StormByte::ByteSize read_ahead,
		const StormByte::ByteSize max_memory, const std::chrono::milliseconds max_wait):
	m_owner(&owner),
	m_path(std::move(path)),
	m_location(location),
	m_read_ahead(read_ahead),
	m_max_memory(max_memory),
	m_max_wait(max_wait),
	m_state(State::Unavailable) {
	StartWorker();
}

BufferedReader::~BufferedReader() {
	Shutdown();
}

void BufferedReader::Rebind(IO::BufferedReader& owner) noexcept {
	m_owner = &owner;
}

const StormByte::String::String& BufferedReader::Path() const noexcept {
	return m_path;
}

IO::Location BufferedReader::Location() const noexcept {
	return m_location;
}

BufferedReader::operator bool() const noexcept {
	return IsReadable();
}

State BufferedReader::State() const noexcept {
	std::lock_guard lock(m_mutex);
	return m_state;
}

void BufferedReader::SetState(const enum State state) noexcept {
	std::lock_guard lock(m_mutex);
	m_state = state;
}

bool BufferedReader::Open() {
	if (!m_owner)
		return false;

	bool already = false;
	{
		std::lock_guard lock(m_mutex);
		already = m_open;
	}

	const Result opened = m_owner->OriginOpen();

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
	m_origin_exhausted = false;
	m_tell = StormByte::ByteSize{0};
	m_max_tell = StormByte::ByteSize{0};
	m_origin_pos = StormByte::ByteSize{0};
	m_origin_valid = true;
	m_hold_prefetch = false;
	DropCache();
	return m_state == State::Idle;
}

Result BufferedReader::Close() {
	FlushPrefetch();

	bool was_open = false;
	{
		std::lock_guard lock(m_mutex);
		was_open = m_open;
		CloseSeekEpoch();
	}

	if (was_open && m_owner)
		static_cast<void>(m_owner->OriginClose());

	std::lock_guard lock(m_mutex);
	m_open = false;
	m_failed = false;
	m_origin_exhausted = true;
	m_origin_valid = false;
	m_hold_prefetch = false;
	m_state = State::Unavailable;
	DropCache();
	return { Status::Ok, 0 };
}

void BufferedReader::Shutdown() {
	FlushPrefetch();
	StopWorker();
	std::lock_guard lock(m_mutex);
	CloseSeekEpoch();
	m_open = false;
	m_state = State::Unavailable;
	m_origin_exhausted = true;
	m_origin_valid = false;
	m_hold_prefetch = false;
	DropCache();
}

bool BufferedReader::Rewind() {
	{
		std::lock_guard lock(m_mutex);
		if (!m_open)
			return false;
	}
	static_cast<void>(Close());
	return Open();
}

bool BufferedReader::IsOpen() const noexcept {
	std::lock_guard lock(m_mutex);
	return m_open;
}

bool BufferedReader::IsReadable() const noexcept {
	std::lock_guard lock(m_mutex);
	if (m_state != State::Idle || !m_open || m_failed)
		return false;
	return !(m_origin_exhausted && CoverageFrom(m_tell) == StormByte::ByteSize{0});
}

bool BufferedReader::EoF() const noexcept {
	std::lock_guard lock(m_mutex);
	if (!m_open)
		return true;
	return m_origin_exhausted && CoverageFrom(m_tell) == StormByte::ByteSize{0};
}

Result BufferedReader::Read(const StormByte::ByteSize n, FIFO& dest) const {
	return Serve(n, dest, true);
}

Result BufferedReader::Peek(const StormByte::ByteSize n, FIFO& dest) const {
	return Serve(n, dest, false);
}

Result BufferedReader::Seek(const std::ptrdiff_t offset, const Position mode) const {
	FlushPrefetch();

	std::lock_guard lock(m_mutex);
	if (!m_open || m_failed || m_state != State::Idle || !m_owner)
		return { Status::Failed, 0 };
	if (!m_owner->OriginCanSeek())
		return { Status::Failed, 0 };

	StormByte::ByteSize target = m_tell;
	if (mode == Position::Absolute) {
		if (offset < 0)
			return { Status::Failed, 0 };
		target = StormByte::ByteSize{static_cast<std::size_t>(offset)};
	}
	else {
		if (offset < 0) {
			const StormByte::ByteSize back{static_cast<std::size_t>(-offset)};
			if (back > m_tell)
				return { Status::Failed, 0 };
			target = m_tell - back;
		}
		else {
			target = m_tell + StormByte::ByteSize{static_cast<std::size_t>(offset)};
		}
	}

	if (!OffsetFits(target))
		return { Status::Failed, 0 };

	CloseSeekEpoch();
	m_tell = target;
	m_origin_exhausted = false;
	++m_seek_logical;
	m_seek_epoch = true;
	m_seek_had_cache = CoverageFrom(target) > StormByte::ByteSize{0};
	m_seek_did_origin = false;
	m_hold_prefetch = !m_origin_valid || target != m_origin_pos;
	return { Status::Ok, 0 };
}

StormByte::ByteSize BufferedReader::Tell() const noexcept {
	std::lock_guard lock(m_mutex);
	return m_tell;
}

bool BufferedReader::IsSeekable() const noexcept {
	return m_owner && m_owner->OriginCanSeek();
}

bool BufferedReader::IsSized() const noexcept {
	return m_owner && m_owner->OriginHasSize();
}

std::optional<StormByte::ByteSize> BufferedReader::Size() const noexcept {
	if (!m_owner)
		return std::nullopt;
	return m_owner->OriginSize();
}

struct StormByte::Buffer::IO::BufferedReader::Telemetry BufferedReader::Telemetry() const noexcept {
	std::lock_guard lock(m_mutex);
	struct StormByte::Buffer::IO::BufferedReader::Telemetry out;
	out.Delivered = m_delivered;
	out.HitAhead = m_hit_ahead;
	out.HitBack = m_hit_back;
	out.Miss = m_miss;
	out.Origin = m_origin;
	out.Cached = CachedBytes();
	out.CachedPeak = m_cached_peak;
	out.Cap = m_max_memory;
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

void BufferedReader::NoteWait(const std::chrono::nanoseconds elapsed) const noexcept {
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

void BufferedReader::NoteResident() const noexcept {
	const StormByte::ByteSize now = CachedBytes();
	if (now > m_cached_peak)
		m_cached_peak = now;
	if (m_max_memory > StormByte::ByteSize{0} && now >= m_max_memory)
		++m_saturated;
}

bool BufferedReader::DeviceSynced() const noexcept {
	return m_origin_valid && m_tell == m_origin_pos;
}

void BufferedReader::CloseSeekEpoch() const noexcept {
	if (!m_seek_epoch)
		return;
	if (m_seek_had_cache) {
		if (m_seek_did_origin)
			++m_seek_saved_partial;
		else
			++m_seek_saved_full;
	}
	m_seek_epoch = false;
}

StormByte::ByteSize BufferedReader::ReadAhead() const noexcept {
	std::lock_guard lock(m_mutex);
	return m_read_ahead;
}

void BufferedReader::ReadAhead(const StormByte::ByteSize bytes) {
	FlushPrefetch();
	std::lock_guard lock(m_mutex);
	m_read_ahead = bytes;
	CollectGarbage();
}

StormByte::ByteSize BufferedReader::MaxMemory() const noexcept {
	std::lock_guard lock(m_mutex);
	return m_max_memory;
}

void BufferedReader::MaxMemory(const StormByte::ByteSize bytes) {
	FlushPrefetch();
	std::lock_guard lock(m_mutex);
	m_max_memory = bytes;
	CollectGarbage();
}

std::chrono::milliseconds BufferedReader::MaxWait() const noexcept {
	std::lock_guard lock(m_mutex);
	return m_max_wait;
}

void BufferedReader::MaxWait(const std::chrono::milliseconds wait) {
	std::lock_guard lock(m_mutex);
	m_max_wait = wait;
}

void BufferedReader::StartWorker() {
	if (m_worker.joinable())
		return;
	m_stop.store(false);
	m_worker = std::thread(&BufferedReader::Worker, this);
}

void BufferedReader::StopWorker() {
	m_stop.store(true);
	m_cancel_prefetch.store(true);
	m_cv.notify_all();
	if (m_worker.joinable())
		m_worker.join();
}

void BufferedReader::RequestPrefetch() const {
	std::lock_guard lock(m_mutex);
	if (m_hold_prefetch)
		return;
	if (!m_open || m_failed || m_state != State::Idle || m_origin_exhausted)
		return;
	if (m_read_ahead == StormByte::ByteSize{0} || m_max_memory == StormByte::ByteSize{0})
		return;
	if (CoverageFrom(m_tell) >= m_read_ahead)
		return;
	m_prefetch_target = m_read_ahead;
	m_prefetch_run = true;
	m_cancel_prefetch.store(false);
	m_cv.notify_all();
}

void BufferedReader::FlushPrefetch() const {
	m_cancel_prefetch.store(true);
	m_cv.notify_all();
	std::unique_lock lock(m_mutex);
	m_cv.wait(lock, [this] {
		return m_stop.load() || !m_prefetch_run;
	});
}

void BufferedReader::Worker() {
	for (;;) {
		std::unique_lock lock(m_mutex);
		m_cv.wait(lock, [this] {
			return m_stop.load() || m_prefetch_run;
		});
		if (m_stop.load()) {
			m_prefetch_run = false;
			m_cv.notify_all();
			return;
		}

		const StormByte::ByteSize target = m_prefetch_target;
		lock.unlock();

		while (!m_stop.load() && !m_cancel_prefetch.load()) {
			StormByte::ByteSize pull_at{0};
			StormByte::ByteSize need{0};
			{
				std::lock_guard inner(m_mutex);
				if (m_hold_prefetch)
					break;
				if (!m_open || m_origin_exhausted || m_state != State::Idle)
					break;
				const StormByte::ByteSize covered = CoverageFrom(m_tell);
				if (covered >= target)
					break;
				pull_at = m_tell + covered;
				need = target - covered;
				if (need > PullBatch)
					need = PullBatch;
				if (m_max_memory > StormByte::ByteSize{0}) {
					const StormByte::ByteSize used = CachedBytes();
					if (used >= m_max_memory) {
						CollectGarbage();
						if (CachedBytes() >= m_max_memory)
							break;
					}
				}
			}
			if (!m_owner)
				break;

			FIFO chunk;
			const Result pulled = PullAt(pull_at, need, chunk);
			std::lock_guard inner(m_mutex);
			if (pulled.status == Status::Failed || pulled.status == Status::Error) {
				m_failed = true;
				m_prefetch_run = false;
				m_cv.notify_all();
				break;
			}
			m_cv.notify_all();
			if (pulled.status != Status::Ok)
				break;
		}

		lock.lock();
		m_prefetch_run = false;
		m_cancel_prefetch.store(false);
		m_cv.notify_all();
	}
}

void BufferedReader::DropCache() const {
	m_spans.clear();
}

StormByte::ByteSize BufferedReader::CachedBytes() const noexcept {
	StormByte::ByteSize total{0};
	for (const auto& [start, fifo] : m_spans)
		total = total + fifo.Available();
	return total;
}

StormByte::ByteSize BufferedReader::CoverageFrom(const StormByte::ByteSize pos) const noexcept {
	const auto it = FindSpan(pos);
	if (it == m_spans.end())
		return StormByte::ByteSize{0};
	return SpanEnd(it->first, it->second) - pos;
}

std::map<StormByte::ByteSize, FIFO>::iterator BufferedReader::FindSpan(const StormByte::ByteSize pos) const noexcept {
	auto it = m_spans.upper_bound(pos);
	if (it == m_spans.begin())
		return m_spans.end();
	--it;
	if (pos < SpanEnd(it->first, it->second))
		return it;
	return m_spans.end();
}

bool BufferedReader::CopyFromCache(const StormByte::ByteSize pos, const StormByte::ByteSize n, FIFO& dest) const {
	const auto it = FindSpan(pos);
	if (it == m_spans.end())
		return false;
	if (n > SpanEnd(it->first, it->second) - pos)
		return false;
	FIFO view = it->second;
	view.Seek(static_cast<std::ptrdiff_t>(pos - it->first), Position::Absolute);
	return view.Peek(n, dest);
}

void BufferedReader::EraseRange(const StormByte::ByteSize from, const StormByte::ByteSize to) const {
	if (from >= to)
		return;

	auto it = m_spans.begin();
	while (it != m_spans.end()) {
		const StormByte::ByteSize start = it->first;
		const StormByte::ByteSize end = SpanEnd(start, it->second);
		if (end <= from || start >= to) {
			++it;
			continue;
		}

		FIFO left;
		FIFO right;
		if (start < from) {
			FIFO view = it->second;
			static_cast<void>(view.Peek(from - start, left));
		}
		if (end > to) {
			FIFO view = it->second;
			view.Seek(static_cast<std::ptrdiff_t>(to - start), Position::Absolute);
			static_cast<void>(view.Peek(end - to, right));
		}

		it = m_spans.erase(it);
		if (left.Available() > StormByte::ByteSize{0})
			m_spans.emplace(start, std::move(left));
		if (right.Available() > StormByte::ByteSize{0})
			m_spans.emplace(to, std::move(right));
	}
}

void BufferedReader::CommitSpan(const StormByte::ByteSize start, FIFO&& piece) const {
	if (m_max_memory == StormByte::ByteSize{0} || piece.Available() == StormByte::ByteSize{0})
		return;

	const StormByte::ByteSize piece_end = SpanEnd(start, piece);
	auto it = m_spans.upper_bound(start);
	if (it != m_spans.begin()) {
		auto prev = std::prev(it);
		if (SpanEnd(prev->first, prev->second) >= start)
			it = prev;
	}

	StormByte::ByteSize merged_start = start;
	StormByte::ByteSize merged_end = piece_end;
	std::vector<std::pair<StormByte::ByteSize, FIFO>> parts;
	parts.emplace_back(start, std::move(piece));

	while (it != m_spans.end() && it->first <= merged_end) {
		const StormByte::ByteSize end = SpanEnd(it->first, it->second);
		if (end < merged_start)
			break;
		merged_start = merged_start < it->first ? merged_start : it->first;
		merged_end = merged_end > end ? merged_end : end;
		parts.emplace_back(it->first, std::move(it->second));
		it = m_spans.erase(it);
	}

	FIFO merged;
	StormByte::ByteSize cursor = merged_start;
	while (cursor < merged_end) {
		bool progressed = false;
		for (auto& [part_start, part] : parts) {
			const StormByte::ByteSize part_end = SpanEnd(part_start, part);
			if (cursor < part_start || cursor >= part_end)
				continue;
			FIFO view = part;
			view.Seek(static_cast<std::ptrdiff_t>(cursor - part_start), Position::Absolute);
			FIFO slice;
			static_cast<void>(view.Peek(part_end - cursor, slice));
			if (slice.Available() > StormByte::ByteSize{0})
				static_cast<void>(merged.Write(slice.Available(), std::move(slice)));
			cursor = part_end;
			progressed = true;
			break;
		}
		if (!progressed)
			break;
	}

	if (merged.Available() > StormByte::ByteSize{0})
		m_spans.insert_or_assign(merged_start, std::move(merged));
	CollectGarbage();
	NoteResident();
}

void BufferedReader::CollectGarbage() const {
	if (m_max_memory == StormByte::ByteSize{0}) {
		m_evicted += m_spans.size();
		DropCache();
		return;
	}

	while (CachedBytes() > m_max_memory && !m_spans.empty()) {
		auto victim = m_spans.end();
		StormByte::ByteSize farthest{0};
		for (auto it = m_spans.begin(); it != m_spans.end(); ++it) {
			const StormByte::ByteSize distance = DistanceToTell(it->first, it->second, m_tell);
			if (distance == StormByte::ByteSize{0})
				continue;
			if (victim == m_spans.end() || distance > farthest) {
				victim = it;
				farthest = distance;
			}
		}

		if (victim != m_spans.end()) {
			m_spans.erase(victim);
			++m_evicted;
			continue;
		}

		auto it = FindSpan(m_tell);
		if (it == m_spans.end()) {
			m_spans.erase(std::prev(m_spans.end()));
			++m_evicted;
			continue;
		}

		const StormByte::ByteSize start = it->first;
		const StormByte::ByteSize end = SpanEnd(start, it->second);
		const StormByte::ByteSize others = CachedBytes() - it->second.Available();
		const StormByte::ByteSize budget = m_max_memory > others ? m_max_memory - others : StormByte::ByteSize{0};
		if (budget == StormByte::ByteSize{0}) {
			m_spans.erase(it);
			++m_evicted;
			break;
		}

		const StormByte::ByteSize keep_from = m_tell < start ? start : m_tell;
		StormByte::ByteSize keep_to = keep_from + budget;
		if (keep_to > end)
			keep_to = end;
		if (keep_from > start)
			EraseRange(start, keep_from);
		if (keep_to < end)
			EraseRange(keep_to, end);
		++m_evicted;
		break;
	}
}

Result BufferedReader::EnsureOrigin(const StormByte::ByteSize pos) const {
	if (!m_owner)
		return { Status::Failed, 0 };

	bool valid = false;
	StormByte::ByteSize origin{0};
	bool seekable = false;
	bool exhausted = false;
	{
		std::lock_guard lock(m_mutex);
		valid = m_origin_valid;
		origin = m_origin_pos;
		exhausted = m_origin_exhausted;
		seekable = m_owner->OriginCanSeek();
	}

	if (valid && origin == pos && !exhausted)
		return { Status::Ok, 0 };
	if (!seekable)
		return { Status::Failed, 0 };
	if (!OffsetFits(pos))
		return { Status::Failed, 0 };

	const Result seeked = m_owner->OriginSeek(static_cast<std::ptrdiff_t>(pos), Position::Absolute);
	if (seeked.status != Status::Ok)
		return { Status::Failed, 0 };

	std::lock_guard lock(m_mutex);
	m_origin_pos = pos;
	m_origin_valid = true;
	m_origin_exhausted = false;
	m_hold_prefetch = false;
	++m_seek_origin;
	if (m_seek_epoch)
		m_seek_did_origin = true;
	return { Status::Ok, 0 };
}

Result BufferedReader::PullAt(const StormByte::ByteSize at, const StormByte::ByteSize n, FIFO& dest) const {
	if (!m_owner)
		return { Status::Failed, 0 };
	if (n == StormByte::ByteSize{0})
		return { Status::Ok, 0 };

	const Result aligned = EnsureOrigin(at);
	if (aligned.status != Status::Ok)
		return aligned;

	FIFO chunk;
	const Result pulled = m_owner->OriginPull(n, chunk);
	if (pulled.status == Status::Failed || pulled.status == Status::Error)
		return { pulled.status, 0 };

	std::lock_guard lock(m_mutex);
	if (pulled.count > StormByte::ByteSize{0}) {
		m_origin = m_origin + pulled.count;
		m_origin_pos = at + pulled.count;
		m_origin_valid = true;
		FIFO stored;
		static_cast<void>(chunk.Peek(pulled.count, stored));
		static_cast<void>(dest.Write(pulled.count, chunk));
		CommitSpan(at, std::move(stored));
	}
	if (pulled.status == Status::End)
		m_origin_exhausted = true;
	return { pulled.status, pulled.count };
}

Result BufferedReader::Serve(const StormByte::ByteSize n, FIFO& dest, const bool consume) const {
	const auto started = std::chrono::steady_clock::now();

	std::chrono::milliseconds wait{0};
	bool can_prefetch = false;
	{
		std::lock_guard lock(m_mutex);
		if (!m_open || m_failed || m_state != State::Idle || !m_owner)
			return { Status::Failed, 0 };
		wait = m_max_wait;
		can_prefetch = !m_hold_prefetch
			&& m_read_ahead > StormByte::ByteSize{0}
			&& m_max_memory > StormByte::ByteSize{0};
	}

	if (n == StormByte::ByteSize{0}) {
		FIFO out;
		StormByte::ByteSize count{0};
		{
			std::lock_guard lock(m_mutex);
			count = CoverageFrom(m_tell);
			if (count > StormByte::ByteSize{0})
				static_cast<void>(CopyFromCache(m_tell, count, out));
			if (consume && count > StormByte::ByteSize{0}) {
				StormByte::ByteSize ahead{0};
				StormByte::ByteSize back{0};
				SplitHit(m_tell, count, m_max_tell, ahead, back);
				m_tell = m_tell + count;
				if (m_tell > m_max_tell)
					m_max_tell = m_tell;
				if (DeviceSynced())
					m_hold_prefetch = false;
				CollectGarbage();
				m_delivered = m_delivered + count;
				m_hit_ahead = m_hit_ahead + ahead;
				m_hit_back = m_hit_back + back;
				NoteWait(std::chrono::steady_clock::now() - started);
			}
		}
		if (count > StormByte::ByteSize{0})
			dest = std::move(out);
		bool ended = false;
		{
			std::lock_guard lock(m_mutex);
			ended = count == StormByte::ByteSize{0} && m_origin_exhausted;
		}
		RequestPrefetch();
		if (ended)
			return { Status::End, 0 };
		return { Status::Ok, count };
	}

	if (can_prefetch) {
		std::unique_lock lock(m_mutex);
		if (CoverageFrom(m_tell) < n && !m_origin_exhausted && !m_failed && !m_hold_prefetch) {
			m_prefetch_target = m_read_ahead > n ? m_read_ahead : n;
			m_prefetch_run = true;
			m_cancel_prefetch.store(false);
			m_cv.notify_all();
			if (wait.count() == 0) {
				m_cv.wait(lock, [this, n] {
					return m_stop.load()
						|| m_failed
						|| m_origin_exhausted
						|| CoverageFrom(m_tell) >= n
						|| !m_prefetch_run
						|| m_hold_prefetch;
				});
			}
			else {
				const auto deadline = std::chrono::steady_clock::now() + wait;
				while (CoverageFrom(m_tell) < n && !m_origin_exhausted && !m_failed
						&& !m_stop.load() && m_prefetch_run && !m_hold_prefetch) {
					if (m_cv.wait_until(lock, deadline) == std::cv_status::timeout)
						break;
				}
				if (CoverageFrom(m_tell) < n && !m_origin_exhausted && !m_failed
						&& m_prefetch_run && !m_hold_prefetch) {
					++m_try_again;
					NoteWait(std::chrono::steady_clock::now() - started);
					return { Status::TryAgain, 0 };
				}
			}
		}
	}

	FIFO assembled;
	StormByte::ByteSize hit_ahead{0};
	StormByte::ByteSize hit_back{0};
	StormByte::ByteSize miss_bytes{0};
	for (;;) {
		StormByte::ByteSize have = assembled.Available();
		if (have >= n)
			break;

		StormByte::ByteSize pos{0};
		StormByte::ByteSize cached{0};
		StormByte::ByteSize max_tell{0};
		bool exhausted = false;
		bool failed = false;
		FIFO from_cache;
		{
			std::lock_guard lock(m_mutex);
			pos = m_tell + have;
			cached = CoverageFrom(pos);
			max_tell = m_max_tell;
			exhausted = m_origin_exhausted;
			failed = m_failed;
			if (cached > StormByte::ByteSize{0}) {
				const StormByte::ByteSize take = cached < (n - have) ? cached : (n - have);
				if (!CopyFromCache(pos, take, from_cache))
					return { Status::Failed, 0 };
				cached = take;
			}
		}
		if (failed)
			return { Status::Failed, 0 };

		if (from_cache.Available() > StormByte::ByteSize{0}) {
			SplitHit(pos, cached, max_tell, hit_ahead, hit_back);
			AppendFifo(assembled, from_cache);
			continue;
		}

		if (exhausted) {
			bool seekable = false;
			{
				std::lock_guard lock(m_mutex);
				seekable = m_owner && m_owner->OriginCanSeek();
			}
			if (!seekable)
				break;
		}

		FlushPrefetch();
		FIFO piece;
		const Result pulled = PullAt(pos, n - have, piece);
		if (pulled.status == Status::Failed || pulled.status == Status::Error) {
			std::lock_guard lock(m_mutex);
			m_failed = true;
			return { pulled.status, 0 };
		}
		miss_bytes = miss_bytes + pulled.count;
		AppendFifo(assembled, piece);
		if (pulled.count == StormByte::ByteSize{0})
			break;
	}

	const StormByte::ByteSize take = assembled.Available();
	bool exhausted = false;
	bool failed = false;
	{
		std::lock_guard lock(m_mutex);
		exhausted = m_origin_exhausted;
		failed = m_failed;
		if (take < n && !exhausted && !failed)
			return { Status::Failed, 0 };
		if (consume && take > StormByte::ByteSize{0}) {
			m_tell = m_tell + take;
			if (m_tell > m_max_tell)
				m_max_tell = m_tell;
			if (DeviceSynced())
				m_hold_prefetch = false;
			CollectGarbage();
			m_delivered = m_delivered + take;
			m_hit_ahead = m_hit_ahead + hit_ahead;
			m_hit_back = m_hit_back + hit_back;
			m_miss = m_miss + miss_bytes;
		}
		if (take > StormByte::ByteSize{0} || consume)
			NoteWait(std::chrono::steady_clock::now() - started);
	}

	if (take > StormByte::ByteSize{0})
		dest = std::move(assembled);
	RequestPrefetch();
	const bool end = take < n || (take == StormByte::ByteSize{0} && exhausted);
	return { end ? Status::End : Status::Ok, take };
}
