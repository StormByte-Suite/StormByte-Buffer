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
#include <condition_variable>
#include <cstddef>
#include <functional>
#include <map>
#include <memory>
#include <mutex>
#include <set>
#include <utility>
#include <vector>

namespace StormByte::Buffer {
	/**
	 * @class Sink<T>::Implementation
	 * @brief Internal implementation class for Sink.
	 *
	 * Manages the map of key-to-Hopper buckets, thread synchronization,
	 * wiring condition variables, and pop selection algorithms.
	 */
	template<Type::MoveConstructible T>
	class Sink<T>::Implementation {
		public:
			/**
			 * @brief Constructs the Sink Implementation instance.
			 */
			Implementation() noexcept
			: m_rr(0), m_consumer(nullptr), m_closed(false), m_drain(false) {}

			/**
			 * @brief Destructor. Marks Sink closed and wakes any waiting threads on m_wired.
			 */
			~Implementation() noexcept {
				m_closed.store(true, std::memory_order_release);
				m_wired.notify_all();
			}

			/**
			 * @brief Enqueues an item into the hopper for key.
			 * @param key Bucket key identifier.
			 * @param item Item to push.
			 */
			void Push(int key, T item) noexcept {
				if constexpr (Type::NullablePointer<T>) {
					if (!item)
						return;
				}
				std::shared_ptr<Hopper<T>> hopper;
				{
					std::unique_lock<std::mutex> lock(m_mutex);
					m_wired.wait(lock, [this, key] {
						return m_closed.load(std::memory_order_acquire)
							|| m_drain.load(std::memory_order_acquire)
							|| m_buckets.find(key) != m_buckets.end();
					});
					if (m_buckets.find(key) == m_buckets.end())
						return;
					hopper = m_buckets[key];
				}
				if (!hopper)
					return;
				hopper->Push(std::move(item));
			}

			/**
			 * @brief Closes this Sink and returns hoppers it writes, for last-writer Eof.
			 * @param cv Set to the consumer condition variable, if any.
			 * @return Writer hoppers to CloseWriter (empty if already closed).
			 */
			std::vector<std::shared_ptr<Hopper<T>>> Close(std::condition_variable*& cv) noexcept {
				std::vector<std::shared_ptr<Hopper<T>>> writers;
				{
					std::lock_guard<std::mutex> lock(m_mutex);
					const bool already = m_closed.exchange(true, std::memory_order_acq_rel);
					if (!already) {
						for (auto& hopper : m_order) {
							if (m_writers.contains(hopper))
								writers.push_back(hopper);
						}
					}
					m_wired.notify_all();
					cv = m_consumer.load(std::memory_order_acquire);
				}
				return writers;
			}

			/**
			 * @brief Shares all existing hoppers with consumer.
			 * @param consumer Consumer Sink implementation reference.
			 */
			void Bind(Implementation& consumer) {
				std::scoped_lock lock(m_mutex, consumer.m_mutex);
				const bool closed = m_closed.load(std::memory_order_acquire)
					|| consumer.m_closed.load(std::memory_order_acquire);
				std::condition_variable* cv = consumer.m_consumer.load(std::memory_order_acquire);
				for (auto& [key, hopper] : m_buckets) {
					if (closed)
						hopper->Eof();
					if (cv != nullptr)
						hopper->Notify(*cv);
					consumer.m_buckets[key] = hopper;
				}
				if (closed)
					consumer.m_closed.store(true, std::memory_order_release);
				consumer.RebuildOrder();
				consumer.m_wired.notify_all();
				m_wired.notify_all();
			}

			/**
			 * @brief Creates or shares the hopper for key with consumer.
			 * @param key Bucket key.
			 * @param consumer Consumer Sink implementation reference.
			 * @return Hopper when this Sink already held it (extra writer); empty otherwise.
			 */
			std::shared_ptr<Hopper<T>> Bind(int key, Implementation& consumer) {
				std::scoped_lock lock(m_mutex, consumer.m_mutex);
				const bool closed = m_closed.load(std::memory_order_acquire)
					|| consumer.m_closed.load(std::memory_order_acquire);
				std::condition_variable* cv = consumer.m_consumer.load(std::memory_order_acquire);
				const bool existed = m_buckets.contains(key);
				auto hopper = Ensure(key);
				const bool already_writer = existed && consumer.m_writers.contains(hopper);
				if (existed)
					consumer.m_writers.insert(hopper);
				if (closed)
					hopper->Eof();
				if (cv != nullptr)
					hopper->Notify(*cv);
				consumer.m_buckets[key] = hopper;
				if (closed)
					consumer.m_closed.store(true, std::memory_order_release);
				consumer.RebuildOrder();
				consumer.m_wired.notify_all();
				m_wired.notify_all();
				return (existed && !already_writer) ? hopper : nullptr;
			}

			/**
			 * @brief Sets Drain mode.
			 */
			void Drain() noexcept {
				m_drain.store(true, std::memory_order_release);
				m_wired.notify_all();
			}

			/**
			 * @brief Checks if Drain was set.
			 * @return true if draining.
			 */
			bool Draining() const noexcept {
				return m_drain.load(std::memory_order_acquire);
			}

			/**
			 * @brief Registers condition variable for consumer notifications.
			 * @param consumer Condition variable reference.
			 */
			void Notify(std::condition_variable& consumer) noexcept {
				m_consumer.store(&consumer, std::memory_order_release);
				const auto hoppers = Order();
				for (auto& hopper : hoppers)
					hopper->Notify(consumer);
			}

			/**
			 * @brief Drops the consumer condition variable on this Sink and its hoppers.
			 */
			void Unnotify() noexcept {
				m_consumer.store(nullptr, std::memory_order_release);
				const auto hoppers = Order();
				for (auto& hopper : hoppers) {
					if (hopper)
						hopper->Unnotify();
				}
			}

			/**
			 * @brief Snapshot of wired keys in map order.
			 * @return Keys, empty if none.
			 */
			std::vector<int> Keys() const noexcept {
				std::lock_guard<std::mutex> lock(m_mutex);
				std::vector<int> keys;
				keys.reserve(m_buckets.size());
				for (const auto& [key, hopper] : m_buckets)
					keys.push_back(key);
				return keys;
			}

			/**
			 * @brief Number of wired hoppers.
			 * @return Bucket count.
			 */
			StormByte::Size Buckets() const noexcept {
				std::lock_guard<std::mutex> lock(m_mutex);
				return StormByte::Size{m_buckets.size()};
			}

			/**
			 * @brief Whether key is wired.
			 * @param key Bucket key.
			 * @return true if present.
			 */
			bool Contains(int key) const noexcept {
				return static_cast<bool>(Bucket(key));
			}

			/**
			 * @brief Gets capacity of key hopper.
			 * @param key Bucket key.
			 * @return Capacity value.
			 */
			StormByte::Size Capacity(int key) const noexcept {
				const auto hopper = Bucket(key);
				if (!hopper)
					return StormByte::Size{0};
				return hopper->Capacity();
			}

			/**
			 * @brief Sets capacity of key hopper.
			 * @param key Bucket key.
			 * @param capacity New capacity.
			 */
			void Capacity(int key, StormByte::Size capacity) noexcept {
				std::lock_guard<std::mutex> lock(m_mutex);
				auto found = m_buckets.find(key);
				if (found != m_buckets.end() && found->second)
					found->second->Capacity(capacity);
			}

			/**
			 * @brief Gets pending item count of key hopper.
			 * @param key Bucket key.
			 * @return Item count.
			 */
			StormByte::Size Size(int key) const noexcept {
				const auto hopper = Bucket(key);
				if (!hopper)
					return StormByte::Size{0};
				return hopper->Size();
			}

			/**
			 * @brief Checks if key hopper is full.
			 * @param key Bucket key.
			 * @return true if full.
			 */
			bool Full(int key) const noexcept {
				const auto hopper = Bucket(key);
				if (!hopper)
					return false;
				return hopper->Full();
			}

			/**
			 * @brief Whether key hopper has no items.
			 * @param key Bucket key.
			 * @return true if missing or empty.
			 */
			bool Empty(int key) const noexcept {
				const auto hopper = Bucket(key);
				if (!hopper)
					return true;
				return hopper->Empty();
			}

			/**
			 * @brief Whether producers marked Eof on key hopper.
			 * @param key Bucket key.
			 * @return Hopper EoF, or false if missing.
			 */
			bool EoF(int key) const noexcept {
				const auto hopper = Bucket(key);
				if (!hopper)
					return false;
				return hopper->EoF();
			}

			/**
			 * @brief Whether key hopper has an item or is finished.
			 * @param key Bucket key.
			 * @return false if missing.
			 */
			bool Ready(int key) const noexcept {
				const auto hopper = Bucket(key);
				if (!hopper)
					return false;
				return !hopper->Empty() || hopper->EoF();
			}

			/**
			 * @brief Copy of front item of key hopper. Does not dequeue.
			 * @param key Bucket key.
			 * @return Front or default T.
			 */
			T Front(int key) const noexcept requires Type::CopyConstructible<T> {
				const auto hopper = Bucket(key);
				if (!hopper)
					return T{};
				return hopper->Front();
			}

			/**
			 * @brief Pops item using default selection.
			 * @return Popped item or default T.
			 */
			T Pop() noexcept {
				return Pop(typename Sink<T>::Select{});
			}

			/**
			 * @brief Pops item using specified selection function.
			 * @param select Bucket index chooser.
			 * @return Popped item or default T.
			 */
			T Pop(const typename Sink<T>::Select& select) noexcept {
				std::vector<std::shared_ptr<Hopper<T>>> hoppers;
				{
					std::unique_lock<std::mutex> lock(m_mutex);
					m_wired.wait(lock, [this] {
						return m_closed.load(std::memory_order_acquire) || !m_order.empty();
					});
					hoppers = m_order;
				}
				if (hoppers.empty())
					return T{};
				if (hoppers.size() == 1)
					return hoppers.front()->Pop();

				const std::size_t count = hoppers.size();
				std::size_t start = 0;
				if (select)
					start = static_cast<std::size_t>(select(StormByte::Size{count})) % count;
				else
					start = m_rr.fetch_add(1, std::memory_order_relaxed) % count;

				for (std::size_t offset = 0; offset < count; ++offset) {
					auto& hopper = hoppers[(start + offset) % count];
					if (!hopper->Empty())
						return hopper->Pop();
				}
				return T{};
			}

			/**
			 * @brief Pops from one key only.
			 * @param key Bucket key.
			 * @return Item or default T.
			 */
			T Pop(int key) noexcept {
				std::shared_ptr<Hopper<T>> hopper;
				{
					std::unique_lock<std::mutex> lock(m_mutex);
					m_wired.wait(lock, [this, key] {
						return m_closed.load(std::memory_order_acquire)
							|| m_buckets.contains(key);
					});
					auto found = m_buckets.find(key);
					if (found == m_buckets.end() || !found->second)
						return T{};
					hopper = found->second;
				}
				return hopper->Pop();
			}

			/**
			 * @brief Checks if Sink is finished.
			 * @return true if closed and all hoppers drained.
			 */
			bool EoF() const noexcept {
				const auto hoppers = Order();
				if (hoppers.empty())
					return m_closed.load(std::memory_order_acquire);
				for (const auto& hopper : hoppers) {
					if (!hopper->EoF())
						return false;
					if (!hopper->Empty())
						return false;
				}
				return true;
			}

			/**
			 * @brief Checks if Pop can return immediately.
			 * @return true if item is ready or EoF reached.
			 */
			bool Ready() const noexcept {
				const auto hoppers = Order();
				if (hoppers.empty())
					return m_closed.load(std::memory_order_acquire);
				bool drained = true;
				for (const auto& hopper : hoppers) {
					if (!hopper->Empty())
						return true;
					if (!hopper->EoF())
						drained = false;
				}
				return drained;
			}

		private:
			/**
			 * @brief Ensures hopper for key exists. Caller holds m_mutex.
			 * @param key Bucket key.
			 * @return Shared hopper instance.
			 */
			std::shared_ptr<Hopper<T>> Ensure(int key) {
				auto found = m_buckets.find(key);
				if (found != m_buckets.end())
					return found->second;
				auto hopper = std::make_shared<Hopper<T>>();
				std::condition_variable* cv = m_consumer.load(std::memory_order_acquire);
				if (cv != nullptr)
					hopper->Notify(*cv);
				m_buckets.emplace(key, hopper);
				m_writers.insert(hopper);
				RebuildOrder();
				return hopper;
			}

			/**
			 * @brief Rebuilds order vector from m_buckets. Caller holds m_mutex.
			 */
			void RebuildOrder() {
				m_order.clear();
				m_order.reserve(m_buckets.size());
				for (auto& [key, hopper] : m_buckets)
					m_order.push_back(hopper);
			}

			/**
			 * @brief Returns snapshot of current hoppers in order.
			 * @return Vector of hoppers.
			 */
			std::vector<std::shared_ptr<Hopper<T>>> Order() const {
				std::lock_guard<std::mutex> lock(m_mutex);
				return m_order;
			}

			/**
			 * @brief Retrieves hopper for key.
			 * @param key Bucket key.
			 * @return Hopper pointer or nullptr.
			 */
			std::shared_ptr<Hopper<T>> Bucket(int key) const {
				std::lock_guard<std::mutex> lock(m_mutex);
				auto found = m_buckets.find(key);
				if (found == m_buckets.end())
					return nullptr;
				return found->second;
			}

			mutable std::mutex m_mutex;							///< Guards bucket map and order vector.
			std::condition_variable m_wired;					///< Waits for bucket binding or closure.
			std::map<int, std::shared_ptr<Hopper<T>>> m_buckets;///< Map of integer keys to Hopper buckets.
			std::set<std::shared_ptr<Hopper<T>>> m_writers;	///< Hoppers this Sink writes (CloseWriter on Eof).
			std::vector<std::shared_ptr<Hopper<T>>> m_order;	///< Order vector of hoppers for Pop.
			std::atomic<std::size_t> m_rr;						///< Round-robin counter.
			std::atomic<std::condition_variable*> m_consumer;	///< Registered consumer condition variable.
			std::atomic<bool> m_closed;							///< Closed flag.
			std::atomic<bool> m_drain;							///< Drain mode flag.
	};

	template<Type::MoveConstructible T>
	Sink<T>::Sink() noexcept
	: m_io(std::make_unique<Implementation>()) {}

	template<Type::MoveConstructible T>
	Sink<T>::~Sink() noexcept = default;

	template<Type::MoveConstructible T>
	void Sink<T>::Push(int key, T item) noexcept {
		m_io->Push(key, std::move(item));
	}

	template<Type::MoveConstructible T>
	void Sink<T>::Eof() noexcept {
		std::condition_variable* cv = nullptr;
		const auto writers = m_io->Close(cv);
		for (const auto& hopper : writers)
			hopper->CloseWriter();
		if (cv)
			cv->notify_all();
	}

	template<Type::MoveConstructible T>
	Sink<T>::Lane::Lane(Sink& from, int key) noexcept
	: m_from(&from), m_key(key) {}

	template<Type::MoveConstructible T>
	typename Sink<T>::Lane Sink<T>::To(int key) noexcept {
		return Lane(*this, key);
	}

	template<Type::MoveConstructible T>
	Sink<T>& Sink<T>::Lane::operator>>(Sink& dest) noexcept {
		if (auto extra = m_from->m_io->Bind(m_key, *dest.m_io))
			extra->AddWriter();
		return dest;
	}

	template<Type::MoveConstructible T>
	Sink<T>& Sink<T>::operator>>(Sink& dest) noexcept {
		m_io->Bind(*dest.m_io);
		return dest;
	}

	template<Type::MoveConstructible T>
	Sink<T>& Sink<T>::operator<<(Sink& src) noexcept {
		src >> *this;
		return *this;
	}

	template<Type::MoveConstructible T>
	Sink<T>& Sink<T>::operator<<(Lane lane) noexcept {
		lane >> *this;
		return *this;
	}

	template<Type::MoveConstructible T>
	void Sink<T>::Drain() noexcept {
		m_io->Drain();
	}

	template<Type::MoveConstructible T>
	bool Sink<T>::Draining() const noexcept {
		return m_io->Draining();
	}

	template<Type::MoveConstructible T>
	void Sink<T>::Notify(std::condition_variable& consumer) noexcept {
		m_io->Notify(consumer);
	}

	template<Type::MoveConstructible T>
	void Sink<T>::Unnotify() noexcept {
		m_io->Unnotify();
	}

	template<Type::MoveConstructible T>
	std::vector<int> Sink<T>::Keys() const noexcept {
		return m_io->Keys();
	}

	template<Type::MoveConstructible T>
	StormByte::Size Sink<T>::Buckets() const noexcept {
		return m_io->Buckets();
	}

	template<Type::MoveConstructible T>
	bool Sink<T>::Contains(int key) const noexcept {
		return m_io->Contains(key);
	}

	template<Type::MoveConstructible T>
	StormByte::Size Sink<T>::Capacity(int key) const noexcept {
		return m_io->Capacity(key);
	}

	template<Type::MoveConstructible T>
	void Sink<T>::Capacity(int key, StormByte::Size capacity) noexcept {
		m_io->Capacity(key, capacity);
	}

	template<Type::MoveConstructible T>
	StormByte::Size Sink<T>::Size(int key) const noexcept {
		return m_io->Size(key);
	}

	template<Type::MoveConstructible T>
	bool Sink<T>::Full(int key) const noexcept {
		return m_io->Full(key);
	}

	template<Type::MoveConstructible T>
	bool Sink<T>::Empty(int key) const noexcept {
		return m_io->Empty(key);
	}

	template<Type::MoveConstructible T>
	bool Sink<T>::EoF(int key) const noexcept {
		return m_io->EoF(key);
	}

	template<Type::MoveConstructible T>
	bool Sink<T>::Ready(int key) const noexcept {
		return m_io->Ready(key);
	}

	template<Type::MoveConstructible T>
	T Sink<T>::Front(int key) const noexcept requires Type::CopyConstructible<T> {
		return m_io->Front(key);
	}

	template<Type::MoveConstructible T>
	T Sink<T>::Pop() noexcept {
		return m_io->Pop();
	}

	template<Type::MoveConstructible T>
	T Sink<T>::Pop(const Select& select) noexcept {
		return m_io->Pop(select);
	}

	template<Type::MoveConstructible T>
	T Sink<T>::Pop(int key) noexcept {
		return m_io->Pop(key);
	}

	template<Type::MoveConstructible T>
	bool Sink<T>::EoF() const noexcept {
		return m_io->EoF();
	}

	template<Type::MoveConstructible T>
	bool Sink<T>::Ready() const noexcept {
		return m_io->Ready();
	}
}
