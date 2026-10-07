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

#include <StormByte/buffer/lockfree_ring.hxx>
#include <StormByte/safe/unique_lock.hxx>

#include <algorithm>
#include <iterator>

using namespace StormByte::Buffer;

std::size_t LockFreeRing::RoundUpPow2(std::size_t v) noexcept {
	if (v < 16)
		return 16;
	--v;
	v |= v >> 1;
	v |= v >> 2;
	v |= v >> 4;
	v |= v >> 8;
	v |= v >> 16;
#if SIZE_MAX > 0xFFFFFFFFu
	v |= v >> 32;
#endif
	return ++v;
}

LockFreeRing::LockFreeRing(StormByte::ByteSize initial_capacity) {
	m_capacity = RoundUpPow2(static_cast<std::size_t>(initial_capacity));
	m_mask = m_capacity - 1;
	m_storage.resize(m_capacity);
}

LockFreeRing::LockFreeRing(LockFreeRing&& other) noexcept {
	m_storage = std::move(other.m_storage);
	m_front_cache = std::move(other.m_front_cache);
	m_capacity = other.m_capacity;
	m_mask = other.m_mask;
	m_head.store(other.m_head.load(StormByte::Safe::MemoryOrder::Relaxed), StormByte::Safe::MemoryOrder::Relaxed);
	m_tail.store(other.m_tail.load(StormByte::Safe::MemoryOrder::Relaxed), StormByte::Safe::MemoryOrder::Relaxed);
	m_logical.store(other.m_logical.load(StormByte::Safe::MemoryOrder::Relaxed), StormByte::Safe::MemoryOrder::Relaxed);
	m_closed.store(other.m_closed.load(StormByte::Safe::MemoryOrder::Relaxed), StormByte::Safe::MemoryOrder::Relaxed);
	m_error.store(other.m_error.load(StormByte::Safe::MemoryOrder::Relaxed), StormByte::Safe::MemoryOrder::Relaxed);
	other.m_capacity = 0;
	other.m_mask = 0;
	other.m_head.store(0, StormByte::Safe::MemoryOrder::Relaxed);
	other.m_tail.store(0, StormByte::Safe::MemoryOrder::Relaxed);
	other.m_logical.store(0, StormByte::Safe::MemoryOrder::Relaxed);
}

LockFreeRing& LockFreeRing::operator=(LockFreeRing&& other) noexcept {
	if (this != &other) {
		m_storage = std::move(other.m_storage);
		m_front_cache = std::move(other.m_front_cache);
		m_capacity = other.m_capacity;
		m_mask = other.m_mask;
		m_head.store(other.m_head.load(StormByte::Safe::MemoryOrder::Relaxed), StormByte::Safe::MemoryOrder::Relaxed);
		m_tail.store(other.m_tail.load(StormByte::Safe::MemoryOrder::Relaxed), StormByte::Safe::MemoryOrder::Relaxed);
		m_logical.store(other.m_logical.load(StormByte::Safe::MemoryOrder::Relaxed), StormByte::Safe::MemoryOrder::Relaxed);
		m_closed.store(other.m_closed.load(StormByte::Safe::MemoryOrder::Relaxed), StormByte::Safe::MemoryOrder::Relaxed);
		m_error.store(other.m_error.load(StormByte::Safe::MemoryOrder::Relaxed), StormByte::Safe::MemoryOrder::Relaxed);
		other.m_capacity = 0;
		other.m_mask = 0;
		other.m_head.store(0, StormByte::Safe::MemoryOrder::Relaxed);
		other.m_tail.store(0, StormByte::Safe::MemoryOrder::Relaxed);
		other.m_logical.store(0, StormByte::Safe::MemoryOrder::Relaxed);
	}

	return *this;
}

StormByte::ByteSize LockFreeRing::Available() const noexcept {
	const std::size_t t = m_tail.load(StormByte::Safe::MemoryOrder::Acquire);
	const std::size_t l = m_logical.load(StormByte::Safe::MemoryOrder::Relaxed);
	return StormByte::ByteSize{t - l};
}

bool LockFreeRing::Empty() const noexcept {
	return m_head.load(StormByte::Safe::MemoryOrder::Acquire) == m_tail.load(StormByte::Safe::MemoryOrder::Acquire);
}

bool LockFreeRing::EoF() const noexcept {
	if (m_error.load(StormByte::Safe::MemoryOrder::Acquire))
		return true;
	if (!m_closed.load(StormByte::Safe::MemoryOrder::Acquire))
		return false;
	return Available() == StormByte::ByteSize{0};
}

bool LockFreeRing::HasError() const noexcept {
	return m_error.load(StormByte::Safe::MemoryOrder::Acquire);
}

bool LockFreeRing::IsReadable() const noexcept {
	return !m_error.load(StormByte::Safe::MemoryOrder::Acquire);
}

bool LockFreeRing::IsWritable() const noexcept {
	return !m_closed.load(StormByte::Safe::MemoryOrder::Acquire) &&
		!m_error.load(StormByte::Safe::MemoryOrder::Acquire);
}

StormByte::ByteSize LockFreeRing::Size() const noexcept {
	const std::size_t h = m_head.load(StormByte::Safe::MemoryOrder::Acquire);
	const std::size_t t = m_tail.load(StormByte::Safe::MemoryOrder::Acquire);
	return StormByte::ByteSize{t - h};
}

const StormByte::Safe::Binary& LockFreeRing::Data() const noexcept {
	StormByte::Safe::UniqueLock lock(m_wait_mtx);
	const std::size_t h = m_head.load(StormByte::Safe::MemoryOrder::Relaxed);
	const std::size_t t = m_tail.load(StormByte::Safe::MemoryOrder::Relaxed);
	const std::size_t sz = t - h;
	const std::size_t mask = m_mask;
	m_data_cache.clear();
	m_data_cache.reserve(StormByte::ByteSize{sz});
	for (std::size_t i = 0; i < sz; ++i)
		m_data_cache.push_back(m_storage[(h + i) & mask]);
	return m_data_cache;
}

std::span<const std::byte> LockFreeRing::FrontSpan() const noexcept {
	if (m_error.load(StormByte::Safe::MemoryOrder::Acquire))
		return {};
	StormByte::Safe::UniqueLock lock(m_wait_mtx);
	const std::size_t logical = m_logical.load(StormByte::Safe::MemoryOrder::Acquire);
	const std::size_t tail = m_tail.load(StormByte::Safe::MemoryOrder::Acquire);
	if (logical >= tail)
		return {};
	const std::size_t avail = tail - logical;
	const std::size_t pos = logical & m_mask;
	const std::size_t linear = m_capacity - pos;
	const std::size_t n = avail < linear ? avail : linear;
	m_front_cache.assign(m_storage.begin() + static_cast<std::ptrdiff_t>(pos), m_storage.begin() + static_cast<std::ptrdiff_t>(pos + n));
	return std::span<const std::byte>(m_front_cache.data(), m_front_cache.size());
}

void LockFreeRing::Clean() noexcept {
	{
		StormByte::Safe::UniqueLock lock(m_wait_mtx);
		const std::size_t l = m_logical.load(StormByte::Safe::MemoryOrder::Relaxed);
		m_head.store(l, StormByte::Safe::MemoryOrder::Release);
	}
	m_cv.notify_all();
}

void LockFreeRing::Clear() noexcept {
	StormByte::Safe::UniqueLock lock(m_wait_mtx);
	m_head.store(0, StormByte::Safe::MemoryOrder::Relaxed);
	m_tail.store(0, StormByte::Safe::MemoryOrder::Relaxed);
	m_logical.store(0, StormByte::Safe::MemoryOrder::Relaxed);
	m_front_cache.clear();
	m_cv.notify_all();
}

void LockFreeRing::Close() noexcept {
	{
		StormByte::Safe::UniqueLock lock(m_wait_mtx);
		m_closed.store(true, StormByte::Safe::MemoryOrder::Release);
	}
	m_cv.notify_all();
}

void LockFreeRing::SetError() noexcept {
	{
		StormByte::Safe::UniqueLock lock(m_wait_mtx);
		m_error.store(true, StormByte::Safe::MemoryOrder::Release);
	}
	m_cv.notify_all();
}

bool LockFreeRing::Drop(const StormByte::ByteSize& count) noexcept {
	if (count == StormByte::ByteSize{0})
		return true;
	{
		StormByte::Safe::UniqueLock lock(m_wait_mtx);
		const StormByte::ByteSize avail = Available();
		if (count > avail)
			return false;
		m_logical.fetch_add(static_cast<std::size_t>(count), StormByte::Safe::MemoryOrder::Relaxed);
		m_head.store(m_logical.load(StormByte::Safe::MemoryOrder::Relaxed), StormByte::Safe::MemoryOrder::Release);
	}
	m_cv.notify_all();
	return true;
}

bool LockFreeRing::Consume(const StormByte::ByteSize n) noexcept {
	if (n == StormByte::ByteSize{0})
		return true;
	{
		StormByte::Safe::UniqueLock lock(m_wait_mtx);
		if (m_error.load(StormByte::Safe::MemoryOrder::Acquire))
			return false;
		const StormByte::ByteSize avail = Available();
		if (n > avail)
			return false;
		const std::size_t next = m_logical.load(StormByte::Safe::MemoryOrder::Relaxed) + static_cast<std::size_t>(n);
		m_logical.store(next, StormByte::Safe::MemoryOrder::Relaxed);
		m_head.store(next, StormByte::Safe::MemoryOrder::Release);
	}
	m_cv.notify_all();
	return true;
}

void LockFreeRing::Seek(const std::ptrdiff_t& offset, const Position& mode) const noexcept {
	std::size_t base = (mode == Position::Absolute)
		? m_head.load(StormByte::Safe::MemoryOrder::Relaxed)
		: m_logical.load(StormByte::Safe::MemoryOrder::Relaxed);
	std::ptrdiff_t target = static_cast<std::ptrdiff_t>(base) + offset;
	if (target < static_cast<std::ptrdiff_t>(m_head.load(StormByte::Safe::MemoryOrder::Relaxed)))
		target = static_cast<std::ptrdiff_t>(m_head.load(StormByte::Safe::MemoryOrder::Relaxed));
	const std::size_t t = m_tail.load(StormByte::Safe::MemoryOrder::Acquire);
	if (static_cast<std::size_t>(target) > t)
		target = static_cast<std::ptrdiff_t>(t);
	m_logical.store(static_cast<std::size_t>(target), StormByte::Safe::MemoryOrder::Relaxed);
}

void LockFreeRing::Grow() noexcept {
	const std::size_t old_cap = m_capacity;
	const std::size_t new_cap = old_cap * 2;
	StormByte::Safe::Vector<std::byte> new_storage(new_cap);
	const std::size_t h = m_head.load(StormByte::Safe::MemoryOrder::Relaxed);
	const std::size_t t = m_tail.load(StormByte::Safe::MemoryOrder::Relaxed);
	const std::size_t sz = t - h;
	const std::size_t old_mask = m_mask;
	for (std::size_t i = 0; i < sz; ++i)
		new_storage[i] = m_storage[(h + i) & old_mask];
	const std::size_t logical = m_logical.load(StormByte::Safe::MemoryOrder::Relaxed);
	const std::size_t logical_off = logical - h;
	m_storage = std::move(new_storage);
	m_capacity = new_cap;
	m_mask = new_cap - 1;
	m_head.store(0, StormByte::Safe::MemoryOrder::Relaxed);
	m_logical.store(logical_off, StormByte::Safe::MemoryOrder::Relaxed);
	m_tail.store(sz, StormByte::Safe::MemoryOrder::Release);
}

bool LockFreeRing::WaitFor(StormByte::ByteSize n) const {
	if (n == StormByte::ByteSize{0})
		return true;
	StormByte::Safe::UniqueLock lock(m_wait_mtx);
	m_cv.wait(lock, [&] {
		if (m_error.load(StormByte::Safe::MemoryOrder::Acquire) ||
			m_closed.load(StormByte::Safe::MemoryOrder::Acquire))
			return true;
		return Available() >= n;
	});
	if (m_error.load(StormByte::Safe::MemoryOrder::Acquire))
		return false;
	return Available() >= n;
}

bool LockFreeRing::ReadInternal(StormByte::ByteSize count, StormByte::Safe::Binary& out, Operation op) noexcept {
	if (m_error.load(StormByte::Safe::MemoryOrder::Acquire))
		return false;
	StormByte::ByteSize avail = Available();
	if (count == StormByte::ByteSize{0}) {
		if (avail == StormByte::ByteSize{0}) {
			if (m_closed.load(StormByte::Safe::MemoryOrder::Acquire))
				return false;
			if (!WaitFor(StormByte::ByteSize{1}))
				return false;
			avail = Available();
			if (avail == StormByte::ByteSize{0})
				return false;
		}

		count = avail;
	}

	if (avail == StormByte::ByteSize{0} && m_closed.load(StormByte::Safe::MemoryOrder::Acquire))
		return false;
	const StormByte::ByteSize want = count;
	if (want > avail) {
		if (!WaitFor(want))
			return false;
		avail = Available();
		if (want > avail)
			return false;
	}

	StormByte::Safe::UniqueLock lock(m_wait_mtx);
	avail = Available();
	if (m_error.load(StormByte::Safe::MemoryOrder::Acquire))
		return false;
	if (want > avail)
		return false;
	const std::size_t logical = m_logical.load(StormByte::Safe::MemoryOrder::Relaxed);
	const std::size_t mask = m_mask;
	const std::size_t want_n = static_cast<std::size_t>(want);
	out.reserve(out.size() + StormByte::ByteSize{want_n});
	for (std::size_t i = 0; i < want_n; ++i)
		out.push_back(m_storage[(logical + i) & mask]);
	if (op == Operation::Read || op == Operation::Extract) {
		const std::size_t new_logical = logical + want_n;
		m_logical.store(new_logical, StormByte::Safe::MemoryOrder::Relaxed);
		m_head.store(new_logical, StormByte::Safe::MemoryOrder::Release);
		m_cv.notify_all();
	}

	return true;
}

bool LockFreeRing::Peek(const StormByte::ByteSize& count, StormByte::Safe::Binary& out) const noexcept {
	return const_cast<LockFreeRing*>(this)->ReadInternal(count, out, Operation::Peek);
}

bool LockFreeRing::Peek(const StormByte::ByteSize& count, WriteOnly& out) const noexcept {
	StormByte::Safe::Binary tmp;
	if (!Peek(count, tmp))
		return false;
	return out.Write(std::move(tmp));
}

bool LockFreeRing::Read(const StormByte::ByteSize& count, StormByte::Safe::Binary& out) const noexcept {
	return const_cast<LockFreeRing*>(this)->ReadInternal(count, out, Operation::Read);
}

bool LockFreeRing::Read(const StormByte::ByteSize& count, WriteOnly& out) const noexcept {
	StormByte::Safe::Binary tmp;
	if (!Read(count, tmp))
		return false;
	return out.Write(std::move(tmp));
}

bool LockFreeRing::Extract(const StormByte::ByteSize& count, StormByte::Safe::Binary& out) noexcept {
	return ReadInternal(count, out, Operation::Extract);
}

bool LockFreeRing::Extract(const StormByte::ByteSize& count, WriteOnly& out) noexcept {
	StormByte::Safe::Binary tmp;
	if (!Extract(count, tmp))
		return false;
	return out.Write(std::move(tmp));
}

void LockFreeRing::ReadUntilEoF(StormByte::Safe::Binary& out) const noexcept {
	auto* self = const_cast<LockFreeRing*>(this);
	while (true) {
		if (self->m_error.load(StormByte::Safe::MemoryOrder::Acquire))
			return;
		{
			StormByte::Safe::UniqueLock lock(self->m_wait_mtx);
			self->m_cv.wait(lock, [&] {
				if (self->m_error.load(StormByte::Safe::MemoryOrder::Acquire) ||
					self->m_closed.load(StormByte::Safe::MemoryOrder::Acquire))
					return true;
				return self->Available() > StormByte::ByteSize{0};
			});
		}

		if (self->m_error.load(StormByte::Safe::MemoryOrder::Acquire))
			return;
		if (self->Available() == StormByte::ByteSize{0} &&
			self->m_closed.load(StormByte::Safe::MemoryOrder::Acquire))
			return;
		StormByte::Safe::Binary chunk;
		if (!self->Read(StormByte::ByteSize{0}, chunk) || chunk.empty()) {
			if (self->EoF())
				return;
			continue;
		}

		out.insert(out.end(), chunk.begin(), chunk.end());
	}
}

void LockFreeRing::ReadUntilEoF(WriteOnly& out) const noexcept {
	StormByte::Safe::Binary tmp;
	ReadUntilEoF(tmp);
	if (!tmp.empty())
		(void)out.Write(std::move(tmp));
}

void LockFreeRing::ExtractUntilEoF(StormByte::Safe::Binary& out) noexcept {
	while (true) {
		if (m_error.load(StormByte::Safe::MemoryOrder::Acquire))
			return;
		{
			StormByte::Safe::UniqueLock lock(m_wait_mtx);
			m_cv.wait(lock, [&] {
				if (m_error.load(StormByte::Safe::MemoryOrder::Acquire) ||
					m_closed.load(StormByte::Safe::MemoryOrder::Acquire))
					return true;
				return Available() > StormByte::ByteSize{0};
			});
		}

		if (m_error.load(StormByte::Safe::MemoryOrder::Acquire))
			return;
		if (Available() == StormByte::ByteSize{0} &&
			m_closed.load(StormByte::Safe::MemoryOrder::Acquire))
			return;
		StormByte::Safe::Binary chunk;
		if (!Extract(StormByte::ByteSize{0}, chunk) || chunk.empty()) {
			if (EoF())
				return;
			continue;
		}

		out.insert(out.end(),
			std::make_move_iterator(chunk.begin()),
			std::make_move_iterator(chunk.end()));
	}
}

void LockFreeRing::ExtractUntilEoF(WriteOnly& out) noexcept {
	StormByte::Safe::Binary tmp;
	ExtractUntilEoF(tmp);
	if (!tmp.empty())
		(void)out.Write(std::move(tmp));
}

bool LockFreeRing::WriteInternal(StormByte::ByteSize count, const std::byte* src) noexcept {
	if (m_closed.load(StormByte::Safe::MemoryOrder::Acquire) ||
		m_error.load(StormByte::Safe::MemoryOrder::Acquire))
		return false;
	if (count == StormByte::ByteSize{0} || src == nullptr)
		return true;

	const std::size_t n = static_cast<std::size_t>(count);
	StormByte::Safe::UniqueLock lock(m_wait_mtx);
	std::size_t h = m_head.load(StormByte::Safe::MemoryOrder::Acquire);
	std::size_t t = m_tail.load(StormByte::Safe::MemoryOrder::Relaxed);
	while ((t - h) + n + 1 > m_capacity)
		Grow();
	t = m_tail.load(StormByte::Safe::MemoryOrder::Relaxed);
	const std::size_t mask = m_mask;
	for (std::size_t i = 0; i < n; ++i)
		m_storage[(t + i) & mask] = src[i];
	m_tail.store(t + n, StormByte::Safe::MemoryOrder::Release);
	m_cv.notify_all();
	return true;
}

bool LockFreeRing::Write(const StormByte::ByteSize& count, const StormByte::Safe::Binary& data) noexcept {
	const std::size_t n = (count == StormByte::ByteSize{0})
		? static_cast<std::size_t>(data.size())
		: std::min(static_cast<std::size_t>(count), static_cast<std::size_t>(data.size()));
	return WriteInternal(StormByte::ByteSize{n}, data.data());
}

bool LockFreeRing::Write(const StormByte::ByteSize& count, StormByte::Safe::Binary&& data) noexcept {
	const std::size_t n = (count == StormByte::ByteSize{0})
		? static_cast<std::size_t>(data.size())
		: std::min(static_cast<std::size_t>(count), static_cast<std::size_t>(data.size()));
	bool ok = WriteInternal(StormByte::ByteSize{n}, data.data());
	if (ok && StormByte::ByteSize{n} == data.size())
		data.clear();
	return ok;
}

bool LockFreeRing::Write(const StormByte::ByteSize& count, const ReadOnly& data) noexcept {
	StormByte::Safe::Binary tmp;
	if (!data.Read(count, tmp))
		return false;
	return Write(StormByte::ByteSize{0}, std::move(tmp));
}

bool LockFreeRing::Write(const StormByte::ByteSize& count, ReadOnly&& data) noexcept {
	StormByte::Safe::Binary tmp;
	if (!data.Extract(count, tmp))
		return false;
	return Write(StormByte::ByteSize{0}, std::move(tmp));
}

bool LockFreeRing::Write(const std::span<const std::byte> src) noexcept {
	if (src.empty())
		return true;
	return WriteInternal(StormByte::ByteSize{src.size()}, src.data());
}
