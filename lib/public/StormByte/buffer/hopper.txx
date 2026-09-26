/*
* Copyright (C) 2024-2026 David C. Manuelda (StormBytePP)
*
* This file is part of StormByte-Buffer.
*
* StormByte-Buffer is free software: you can redistribute it and/or modify
* it under the terms of the GNU Lesser General Public License version 3
* or later, as published by the Free Software Foundation.
*
* StormByte-Buffer is distributed in the hope that it will be useful,
* but WITHOUT ANY WARRANTY; without even the implied warranty of
* MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
* GNU Lesser General Public License for more details.
*
* You should have received a copy of the GNU Lesser General Public License
* along with StormByte-Buffer. If not, see
* <https://www.gnu.org/licenses/lgpl-3.0.html>.
*/

#pragma once

#include <StormByte/type_traits.hxx>

#include <atomic>
#include <concepts>
#include <condition_variable>
#include <cstddef>
#include <memory>
#include <mutex>
#include <queue>
#include <utility>

namespace StormByte::Buffer {
	/**
	 * @class Hopper<T>::Implementation
	 * @brief Internal implementation of Hopper queue details.
	 *
	 * Handles mutex-protected queue operations, atomic capacity settings,
	 * condition variable notifications, and EoF flags.
	 */
	template<Type::MoveConstructible T>
	class Hopper<T>::Implementation {
		public:
			/**
			 * @brief Constructs an unbounded Implementation instance.
			 */
			Implementation() noexcept
			: m_eof(false), m_wake(nullptr), m_cap(0), m_writers(1) {}

			/**
			 * @brief Constructs a bounded Implementation instance.
			 * @param capacity Maximum items allowed.
			 */
			explicit Implementation(StormByte::Size capacity) noexcept
			: m_eof(false), m_wake(nullptr), m_cap(static_cast<std::size_t>(capacity)), m_writers(1) {}

			/**
			 * @brief Destructor. Marks EoF and wakes waiting producers.
			 */
			~Implementation() noexcept {
				m_eof.store(true, std::memory_order_release);
				m_space.notify_all();
			}

			/**
			 * @brief Gets capacity ceiling.
			 * @return Capacity value.
			 */
			StormByte::Size Capacity() const noexcept {
				return StormByte::Size{m_cap.load(std::memory_order_acquire)};
			}

			/**
			 * @brief Sets capacity ceiling.
			 * @param capacity New capacity value.
			 */
			void Capacity(StormByte::Size capacity) noexcept {
				m_cap.store(static_cast<std::size_t>(capacity), std::memory_order_release);
				m_space.notify_all();
			}

			/**
			 * @brief Gets item count in queue.
			 * @return Count of items.
			 */
			StormByte::Size Size() const noexcept {
				std::lock_guard<std::mutex> lock(m_mutex);
				return StormByte::Size{m_items.size()};
			}

			/**
			 * @brief Checks if bounded bucket is full.
			 * @return true if capacity > 0 and size >= capacity.
			 */
			bool Full() const noexcept {
				const std::size_t cap = m_cap.load(std::memory_order_acquire);
				if (cap == 0)
					return false;
				return Size() >= cap;
			}

			/**
			 * @brief Live writer count.
			 * @return Writers still open.
			 */
			unsigned Writers() const noexcept {
				return m_writers.load(std::memory_order_acquire);
			}

			/**
			 * @brief Enqueues an item, waiting if full.
			 * @param item Item to enqueue.
			 */
			void Push(T item) noexcept {
				if constexpr (Type::NullablePointer<T>) {
					if (!item)
						return;
				}
				{
					std::unique_lock<std::mutex> lock(m_mutex);
					m_space.wait(lock, [this]() {
						const std::size_t cap = m_cap.load(std::memory_order_acquire);
						return cap == 0
							|| m_items.size() < cap
							|| m_eof.load(std::memory_order_acquire);
					});
					if (m_eof.load(std::memory_order_acquire))
						return;
					m_items.push(std::move(item));
				}
				SignalConsumer();
			}

			/**
			 * @brief Signals end of production.
			 */
			void Eof() noexcept {
				m_eof.store(true, std::memory_order_release);
				SignalConsumer();
				m_space.notify_all();
			}

			/**
			 * @brief Registers an extra writer.
			 */
			void AddWriter() noexcept {
				m_writers.fetch_add(1, std::memory_order_acq_rel);
			}

			/**
			 * @brief Releases one writer. Last writer force-closes.
			 */
			void CloseWriter() noexcept {
				unsigned prev = m_writers.load(std::memory_order_acquire);
				while (prev > 0) {
					if (m_writers.compare_exchange_weak(prev, prev - 1,
							std::memory_order_acq_rel, std::memory_order_acquire)) {
						if (prev == 1)
							Eof();
						return;
					}
				}
			}

			/**
			 * @brief Pops next item from queue without waiting.
			 * @return Next item, or default T if empty.
			 */
			T Pop() noexcept {
				T item{};
				{
					std::lock_guard<std::mutex> lock(m_mutex);
					if (m_items.empty())
						return T{};
					item = std::move(m_items.front());
					m_items.pop();
				}
				m_space.notify_one();
				return item;
			}

			/**
			 * @brief Copy of front item. Does not dequeue.
			 * @return Front or default T.
			 */
			T Front() const noexcept requires std::copy_constructible<T> {
				std::lock_guard<std::mutex> lock(m_mutex);
				if (m_items.empty())
					return T{};
				return m_items.front();
			}

			/**
			 * @brief Checks if Eof was signaled.
			 * @return true if Eof set.
			 */
			bool EoF() const noexcept {
				return m_eof.load(std::memory_order_acquire);
			}

			/**
			 * @brief Checks if queue is empty.
			 * @return true if empty.
			 */
			bool Empty() const noexcept {
				std::lock_guard<std::mutex> lock(m_mutex);
				return m_items.empty();
			}

			/**
			 * @brief Item ready or production finished.
			 * @return true if !Empty() or EoF().
			 */
			bool Ready() const noexcept {
				return !Empty() || EoF();
			}

			void Notify(std::condition_variable& wake) noexcept {
				m_wake.store(&wake, std::memory_order_release);
			}

			void Unnotify() noexcept {
				m_wake.store(nullptr, std::memory_order_release);
			}

		private:
			/**
			 * @brief Notifies registered consumer condition variable if set.
			 */
			void SignalConsumer() noexcept {
				std::condition_variable* wake = m_wake.load(std::memory_order_acquire);
				if (wake == nullptr)
					return;
				wake->notify_one();
			}

			mutable std::mutex m_mutex;						///< Guards queue access.
			std::condition_variable m_space;				///< Producer wait condition when full.
			std::queue<T> m_items;							///< Queue of stored items.
			std::atomic<bool> m_eof;						///< End of production flag.
			std::atomic<std::condition_variable*> m_wake;	///< Consumer condition variable.
			std::atomic<std::size_t> m_cap;					///< Capacity ceiling (0 = unbounded).
			std::atomic<unsigned> m_writers;				///< Live writers; last CloseWriter Eofs.
	};

	template<Type::MoveConstructible T>
	Hopper<T>::Hopper() noexcept
	: m_io(std::make_unique<Implementation>()) {}

	template<Type::MoveConstructible T>
	Hopper<T>::Hopper(StormByte::Size capacity) noexcept
	: m_io(std::make_unique<Implementation>(capacity)) {}

	template<Type::MoveConstructible T>
	Hopper<T>::~Hopper() noexcept = default;

	template<Type::MoveConstructible T>
	StormByte::Size Hopper<T>::Capacity() const noexcept {
		return m_io->Capacity();
	}

	template<Type::MoveConstructible T>
	void Hopper<T>::Capacity(StormByte::Size capacity) noexcept {
		m_io->Capacity(capacity);
	}

	template<Type::MoveConstructible T>
	StormByte::Size Hopper<T>::Size() const noexcept {
		return m_io->Size();
	}

	template<Type::MoveConstructible T>
	bool Hopper<T>::Full() const noexcept {
		return m_io->Full();
	}

	template<Type::MoveConstructible T>
	unsigned Hopper<T>::Writers() const noexcept {
		return m_io->Writers();
	}

	template<Type::MoveConstructible T>
	void Hopper<T>::Push(T item) noexcept {
		m_io->Push(std::move(item));
	}

	template<Type::MoveConstructible T>
	Hopper<T>& Hopper<T>::operator<<(T item) noexcept {
		Push(std::move(item));
		return *this;
	}

	template<Type::MoveConstructible T>
	void Hopper<T>::Eof() noexcept {
		m_io->Eof();
	}

	template<Type::MoveConstructible T>
	void Hopper<T>::AddWriter() noexcept {
		m_io->AddWriter();
	}

	template<Type::MoveConstructible T>
	void Hopper<T>::CloseWriter() noexcept {
		m_io->CloseWriter();
	}

	template<Type::MoveConstructible T>
	T Hopper<T>::Pop() noexcept {
		return m_io->Pop();
	}

	template<Type::MoveConstructible T>
	T Hopper<T>::Front() const noexcept requires std::copy_constructible<T> {
		return m_io->Front();
	}

	template<Type::MoveConstructible T>
	Hopper<T>& Hopper<T>::operator>>(T& item) noexcept {
		item = Pop();
		return *this;
	}

	template<Type::MoveConstructible T>
	bool Hopper<T>::EoF() const noexcept {
		return m_io->EoF();
	}

	template<Type::MoveConstructible T>
	bool Hopper<T>::Empty() const noexcept {
		return m_io->Empty();
	}

	template<Type::MoveConstructible T>
	bool Hopper<T>::Ready() const noexcept {
		return m_io->Ready();
	}

	template<Type::MoveConstructible T>
	void Hopper<T>::Notify(std::condition_variable& wake) noexcept {
		m_io->Notify(wake);
	}

	template<Type::MoveConstructible T>
	void Hopper<T>::Unnotify() noexcept {
		m_io->Unnotify();
	}
}
