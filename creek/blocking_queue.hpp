#pragma once

#include <atomic>
#include <chrono>
#include <condition_variable>
#include <cstddef>
#include <memory>
#include <mutex>
#include <optional>
#include <queue>

namespace creek {

template <typename T>
class BlockingQueue {
public:
    explicit BlockingQueue(std::size_t capacity = 0)
        : m_capacity(capacity), m_closed(false) {}

    ~BlockingQueue() { close(); }

    BlockingQueue(const BlockingQueue&) = delete;
    BlockingQueue& operator=(const BlockingQueue&) = delete;
    BlockingQueue(BlockingQueue&&) = delete;
    BlockingQueue& operator=(BlockingQueue&&) = delete;

    bool push(T item) {
        std::unique_lock<std::mutex> lock(m_mutex);
        if (m_closed) return false;
        if (m_capacity > 0 && m_queue.size() >= m_capacity) {
            m_not_full.wait(lock, [this] {
                return m_queue.size() < m_capacity || m_closed;
            });
        }
        if (m_closed) return false;
        m_queue.push(std::move(item));
        m_not_empty.notify_one();
        return true;
    }

    bool try_push(T item) {
        std::lock_guard<std::mutex> lock(m_mutex);
        if (m_closed) return false;
        if (m_capacity > 0 && m_queue.size() >= m_capacity) return false;
        m_queue.push(std::move(item));
        m_not_empty.notify_one();
        return true;
    }

    std::optional<T> take() {
        std::unique_lock<std::mutex> lock(m_mutex);
        m_not_empty.wait(lock, [this] {
            return !m_queue.empty() || m_closed;
        });
        if (m_closed && m_queue.empty()) return std::nullopt;
        T item = std::move(m_queue.front());
        m_queue.pop();
        m_not_full.notify_one();
        return item;
    }

    std::optional<T> take_for(std::chrono::milliseconds timeout) {
        std::unique_lock<std::mutex> lock(m_mutex);
        bool ok = m_not_empty.wait_for(lock, timeout, [this] {
            return !m_queue.empty() || m_closed;
        });
        if (!ok || (m_closed && m_queue.empty())) return std::nullopt;
        T item = std::move(m_queue.front());
        m_queue.pop();
        m_not_full.notify_one();
        return item;
    }

    std::optional<T> poll() {
        std::lock_guard<std::mutex> lock(m_mutex);
        if (m_queue.empty()) return std::nullopt;
        T item = std::move(m_queue.front());
        m_queue.pop();
        m_not_full.notify_one();
        return item;
    }

    void close() {
        std::lock_guard<std::mutex> lock(m_mutex);
        m_closed = true;
        m_not_empty.notify_all();
        m_not_full.notify_all();
    }

    bool is_closed() const {
        std::lock_guard<std::mutex> lock(m_mutex);
        return m_closed;
    }

    std::size_t size() const {
        std::lock_guard<std::mutex> lock(m_mutex);
        return m_queue.size();
    }

private:
    std::size_t                m_capacity;
    std::queue<T>              m_queue;
    mutable std::mutex         m_mutex;
    std::condition_variable    m_not_empty;
    std::condition_variable    m_not_full;
    bool                       m_closed;
};

} // namespace creek
