#pragma once

#include "gateway/gateway_types.hpp"

#include <atomic>
#include <condition_variable>
#include <memory>
#include <mutex>
#include <optional>
#include <queue>

namespace gateway {

template <typename T>
class CopyChannel {
public:
    struct Node {
        T data;
        Node* next{nullptr};
    };

    explicit CopyChannel(std::size_t capacity = 256)
        : m_capacity(capacity), m_closed(false) {
        m_head = new Node{};
        m_tail.store(m_head, std::memory_order_release);
    }

    ~CopyChannel() {
        close();
        Node* node = m_head;
        while (node) {
            Node* next = node->next;
            delete node;
            node = next;
        }
    }

    CopyChannel(const CopyChannel&) = delete;
    CopyChannel& operator=(const CopyChannel&) = delete;

    void send(const T& item) {
        auto* node = new Node{item, nullptr};
        {
            std::unique_lock<std::mutex> lock(m_mutex);
            if (m_closed) { delete node; return; }
            if (m_capacity > 0 && m_count >= m_capacity) {
                m_not_full.wait(lock, [this] { return m_count < m_capacity || m_closed; });
            }
            if (m_closed) { delete node; return; }
            Node* tail = m_tail.load(std::memory_order_acquire);
            tail->next = node;
            m_tail.store(node, std::memory_order_release);
            ++m_count;
        }
        m_not_empty.notify_one();
    }

    void send(T&& item) {
        auto* node = new Node{std::move(item), nullptr};
        {
            std::unique_lock<std::mutex> lock(m_mutex);
            if (m_closed) { delete node; return; }
            if (m_capacity > 0 && m_count >= m_capacity) {
                m_not_full.wait(lock, [this] { return m_count < m_capacity || m_closed; });
            }
            if (m_closed) { delete node; return; }
            Node* tail = m_tail.load(std::memory_order_acquire);
            tail->next = node;
            m_tail.store(node, std::memory_order_release);
            ++m_count;
        }
        m_not_empty.notify_one();
    }

    bool try_send(const T& item) {
        auto* node = new Node{item, nullptr};
        {
            std::lock_guard<std::mutex> lock(m_mutex);
            if (m_closed) { delete node; return false; }
            if (m_capacity > 0 && m_count >= m_capacity) { delete node; return false; }
            Node* tail = m_tail.load(std::memory_order_acquire);
            tail->next = node;
            m_tail.store(node, std::memory_order_release);
            ++m_count;
        }
        m_not_empty.notify_one();
        return true;
    }

    bool recv(T& out) {
        std::unique_lock<std::mutex> lock(m_mutex);
        m_not_empty.wait(lock, [this] {
            return m_head->next != nullptr || m_closed;
        });
        if (m_closed && m_head->next == nullptr) return false;
        Node* node = m_head;
        Node* new_head = node->next;
        out = std::move(new_head->data);
        m_head = new_head;
        delete node;
        --m_count;
        m_not_full.notify_one();
        return true;
    }

    bool try_recv(T& out) {
        std::lock_guard<std::mutex> lock(m_mutex);
        if (m_head->next == nullptr) return false;
        Node* node = m_head;
        Node* new_head = node->next;
        out = std::move(new_head->data);
        m_head = new_head;
        delete node;
        --m_count;
        m_not_full.notify_one();
        return true;
    }

    void swap_out(std::vector<T>& batch) {
        std::lock_guard<std::mutex> lock(m_mutex);
        Node* current = m_head->next;
        batch.clear();
        while (current) {
            batch.push_back(std::move(current->data));
            Node* prev = m_head;
            Node* nxt = current->next;
            m_head = current;
            current = nxt;
            delete prev;
            --m_count;
        }
        m_not_full.notify_all();
    }

    void close() {
        {
            std::lock_guard<std::mutex> lock(m_mutex);
            m_closed = true;
        }
        m_not_empty.notify_all();
        m_not_full.notify_all();
    }

    std::size_t size() const {
        std::lock_guard<std::mutex> lock(m_mutex);
        return m_count;
    }

    bool is_closed() const {
        std::lock_guard<std::mutex> lock(m_mutex);
        return m_closed;
    }

private:
    std::size_t                    m_capacity;
    std::size_t                    m_count{0};
    Node*                          m_head{nullptr};
    std::atomic<Node*>             m_tail{nullptr};
    mutable std::mutex             m_mutex;
    std::condition_variable        m_not_empty;
    std::condition_variable        m_not_full;
    bool                           m_closed;
};

} // namespace gateway
