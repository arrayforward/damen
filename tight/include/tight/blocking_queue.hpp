#pragma once

// 有界阻塞队列（单链表实现）。
// 逐节点分配/释放：出队即归还节点内存，不像 std::queue(deque) 那样
// 在排空后仍保留内部块——高水位流量过后内存可真正回落。

#include <atomic>
#include <chrono>
#include <condition_variable>
#include <cstddef>
#include <mutex>
#include <optional>
#include <utility>

namespace tight {

template <typename T>
class BlockingQueue {
public:
    explicit BlockingQueue(std::size_t capacity = 0)
        : m_capacity(capacity), m_closed(false) {}

    ~BlockingQueue() {
        close();
        Node* node = m_head;
        while (node) {
            Node* next = node->next;
            delete node;
            node = next;
        }
    }

    BlockingQueue(const BlockingQueue&) = delete;
    BlockingQueue& operator=(const BlockingQueue&) = delete;
    BlockingQueue(BlockingQueue&&) = delete;
    BlockingQueue& operator=(BlockingQueue&&) = delete;

    bool push(T item) {
        Node* node = new Node{std::move(item), nullptr};
        std::unique_lock<std::mutex> lock(m_mutex);
        if (m_closed) { delete node; return false; }
        if (m_capacity > 0 && m_size >= m_capacity) {
            m_not_full.wait(lock, [this] {
                return m_size < m_capacity || m_closed;
            });
        }
        if (m_closed) { delete node; return false; }
        link_tail(node);
        m_not_empty.notify_one();
        return true;
    }

    bool try_push(T item) {
        Node* node = new Node{std::move(item), nullptr};
        std::lock_guard<std::mutex> lock(m_mutex);
        if (m_closed || (m_capacity > 0 && m_size >= m_capacity)) {
            delete node;
            return false;
        }
        link_tail(node);
        m_not_empty.notify_one();
        return true;
    }

    std::optional<T> take() {
        std::unique_lock<std::mutex> lock(m_mutex);
        m_not_empty.wait(lock, [this] {
            return m_size > 0 || m_closed;
        });
        if (m_closed && m_size == 0) return std::nullopt;
        return unlink_head_locked();
    }

    std::optional<T> take_for(std::chrono::milliseconds timeout) {
        std::unique_lock<std::mutex> lock(m_mutex);
        bool ok = m_not_empty.wait_for(lock, timeout, [this] {
            return m_size > 0 || m_closed;
        });
        if (!ok || (m_closed && m_size == 0)) return std::nullopt;
        return unlink_head_locked();
    }

    std::optional<T> poll() {
        std::lock_guard<std::mutex> lock(m_mutex);
        if (m_size == 0) return std::nullopt;
        return unlink_head_locked();
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
        return m_size;
    }

private:
    struct Node {
        T item;
        Node* next{nullptr};
    };

    // 入队到尾部（调用方需已持锁且完成关闭/容量检查）
    void link_tail(Node* node) {
        if (m_tail) {
            m_tail->next = node;
            m_tail = node;
        } else {
            m_head = m_tail = node;
        }
        ++m_size;
    }

    // 出队头部节点并释放其内存（调用方需已持锁且 m_size > 0）
    std::optional<T> unlink_head_locked() {
        Node* node = m_head;
        m_head = node->next;
        if (!m_head) m_tail = nullptr;
        --m_size;
        std::optional<T> item{std::move(node->item)};
        delete node;
        m_not_full.notify_one();
        return item;
    }

    std::size_t                m_capacity;
    std::size_t                m_size{0};
    Node*                      m_head{nullptr};
    Node*                      m_tail{nullptr};
    mutable std::mutex         m_mutex;
    std::condition_variable    m_not_empty;
    std::condition_variable    m_not_full;
    bool                       m_closed;
};

} // namespace tight
