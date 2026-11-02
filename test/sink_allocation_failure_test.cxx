/*
 * Copyright (C) 2024-2026 David C. Manuelda (StormBytePP)
 *
 * SPDX-License-Identifier: LGPL-3.0-or-later OR LicenseRef-StormByte-Commercial
 */

#include <StormByte/buffer/sink.hxx>

#include <cerrno>
#include <cstddef>
#include <cstdio>
#include <cstdlib>

#if defined(__linux__) && defined(__GLIBC__)
namespace {
	bool fail_allocations = false;
	std::size_t allocation_attempts = 0;
}

extern "C" void* __libc_malloc(std::size_t) noexcept;
extern "C" void* __libc_calloc(std::size_t, std::size_t) noexcept;
extern "C" void* __libc_realloc(void*, std::size_t) noexcept;
extern "C" void* __libc_memalign(std::size_t, std::size_t) noexcept;

extern "C" void* malloc(std::size_t size) noexcept {
	if (fail_allocations) {
		++allocation_attempts;
		errno = ENOMEM;
		return nullptr;
	}
	return __libc_malloc(size);
}

extern "C" void* calloc(std::size_t count, std::size_t size) noexcept {
	if (fail_allocations) {
		++allocation_attempts;
		errno = ENOMEM;
		return nullptr;
	}
	return __libc_calloc(count, size);
}

extern "C" void* realloc(void* pointer, std::size_t size) noexcept {
	if (fail_allocations) {
		++allocation_attempts;
		errno = ENOMEM;
		return nullptr;
	}
	return __libc_realloc(pointer, size);
}

extern "C" int posix_memalign(void** pointer, std::size_t alignment, std::size_t size) noexcept {
	if (alignment < sizeof(void*) || alignment % sizeof(void*) != 0
		|| (alignment & (alignment - 1)) != 0)
		return EINVAL;
	if (fail_allocations) {
		++allocation_attempts;
		return ENOMEM;
	}
	void* allocation = __libc_memalign(alignment, size);
	if (!allocation)
		return ENOMEM;
	*pointer = allocation;
	return 0;
}

extern "C" void* aligned_alloc(std::size_t alignment, std::size_t size) noexcept {
	if (fail_allocations) {
		++allocation_attempts;
		errno = ENOMEM;
		return nullptr;
	}
	return __libc_memalign(alignment, size);
}
#endif

int main() {
#if defined(__linux__) && defined(__GLIBC__)
	StormByte::Buffer::Sink<int> warm_producer;
	StormByte::Buffer::Sink<int> warm_consumer;
	warm_producer.To(6) >> warm_consumer;
	warm_producer.Eof();
	StormByte::Buffer::Sink<int> warm_abort_producer;
	StormByte::Buffer::Sink<int> warm_abort_consumer;
	warm_abort_producer.To(8) >> warm_abort_consumer;
	warm_abort_producer.Push(8, 1);
	warm_abort_producer.Abort();

	StormByte::Buffer::Sink<int> producer;
	StormByte::Buffer::Sink<int> consumer;
	producer.To(7) >> consumer;

	fail_allocations = true;
	producer.Eof();
	fail_allocations = false;

	if (allocation_attempts != 0) {
		std::fprintf(stderr, "Sink::Eof attempted %zu allocations\n", allocation_attempts);
		return 1;
	}
	if (!consumer.EoF(7)) {
		std::fputs("Sink::Eof did not close the final writer\n", stderr);
		return 1;
	}

	StormByte::Buffer::Sink<int> abort_producer;
	StormByte::Buffer::Sink<int> abort_consumer;
	abort_producer.To(8) >> abort_consumer;
	abort_producer.Push(8, 2);
	fail_allocations = true;
	abort_producer.Abort();
	fail_allocations = false;

	if (allocation_attempts != 0) {
		std::fprintf(stderr, "Sink::Abort attempted %zu allocations\n", allocation_attempts);
		return 1;
	}
	if (!abort_consumer.EoF(8) || abort_consumer.Size(8) != StormByte::Size{0}) {
		std::fputs("Sink::Abort did not close and drain the hopper\n", stderr);
		return 1;
	}
#endif
	return 0;
}
