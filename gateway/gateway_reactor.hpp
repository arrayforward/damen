#pragma once

#include "gateway/gateway_channel.hpp"
#include "gateway/gateway_types.hpp"
#include "tight/blocking_queue.hpp"

#include <atomic>
#include <chrono>
#include <functional>
#include <memory>
#include <queue>
#include <string>
#include <thread>
#include <vector>

namespace gateway {

class Reactor {
public:
    struct Task {
        std::uint64_t    m_id{};
        std::string      m_name;
        std::function<void()> m_fn;
        std::chrono::steady_clock::time_point m_deadline;
        bool             m_skippable{false};
        std::chrono::steady_clock::time_point m_created_at;
    };

    struct TaskStats {
        std::string      m_name;
        std::uint64_t    m_total_executions{};
        std::uint64_t    m_skipped{};
        std::uint64_t    m_slow_count{};
        std::chrono::microseconds m_max_duration{0};
        std::chrono::microseconds m_total_duration{0};
    };

    static constexpr std::chrono::milliseconds kSlowCpuThreshold{10};
    static constexpr std::chrono::milliseconds kSlowIoThreshold{1000};
    static constexpr std::chrono::seconds      kSkipThreshold{1};

    Reactor(std::shared_ptr<CopyChannel<GatewayMessage>> inbox)
        : m_inbox(std::move(inbox)), m_running(false) {}

    ~Reactor() { stop(); }

    Reactor(const Reactor&) = delete;
    Reactor& operator=(const Reactor&) = delete;

    void set_message_processor(std::function<void(std::vector<GatewayMessage>&)> fn) {
        m_message_processor = std::move(fn);
    }

    void set_data_evolver(std::function<void()> fn) {
        m_data_evolver = std::move(fn);
    }

    void start() {
        if (m_running.exchange(true)) return;
        m_io_thread = std::thread([this] { io_loop(); });
        m_cpu_thread = std::thread([this] { cpu_loop(); });
        m_reactor_thread = std::thread([this] { reactor_loop(); });
    }

    void stop() {
        if (!m_running.exchange(false)) return;
        m_io_queue.close();
        m_cpu_queue.close();
        if (m_reactor_thread.joinable()) m_reactor_thread.join();
        if (m_io_thread.joinable()) m_io_thread.join();
        if (m_cpu_thread.joinable()) m_cpu_thread.join();
    }

    bool submit_io(Task task) {
        return submit(m_io_queue, std::move(task));
    }

    bool submit_cpu(Task task) {
        return submit(m_cpu_queue, std::move(task));
    }

    bool schedule_timer(const std::string& name,
                        std::function<void()> fn,
                        std::chrono::milliseconds interval,
                        bool skippable = true) {
        std::lock_guard<std::mutex> lock(m_timer_mutex);
        auto deadline = std::chrono::steady_clock::now() + interval;
        m_timer_queue.push(TimerEntry{
            name,
            std::move(fn),
            interval,
            skippable,
            deadline,
        });
        return true;
    }

    std::vector<TaskStats> task_stats() const {
        std::lock_guard<std::mutex> lock(m_stats_mutex);
        std::vector<TaskStats> out;
        out.reserve(m_task_stats.size());
        for (const auto& [name, s] : m_task_stats) out.push_back(s);
        return out;
    }

private:
    struct TimerEntry {
        std::string                               m_name;
        std::function<void()>                     m_fn;
        std::chrono::milliseconds                  m_interval;
        bool                                       m_skippable;
        std::chrono::steady_clock::time_point      m_next_deadline;

        bool operator>(const TimerEntry& o) const {
            return m_next_deadline > o.m_next_deadline;
        }
    };

    std::atomic<std::uint64_t> m_task_id_counter{1};

    bool submit(tight::BlockingQueue<Task>& queue, Task task) {
        task.m_id = m_task_id_counter.fetch_add(1, std::memory_order_relaxed);
        task.m_created_at = std::chrono::steady_clock::now();
        return queue.try_push(std::move(task));
    }

    void record_stats(const Task& task, std::chrono::microseconds duration) {
        std::lock_guard<std::mutex> lock(m_stats_mutex);
        auto& s = m_task_stats[task.m_name];
        s.m_name = task.m_name;
        s.m_total_executions++;
        s.m_total_duration += duration;
        if (duration > s.m_max_duration) s.m_max_duration = duration;
        bool slow = duration > kSlowCpuThreshold;
        if (slow) s.m_slow_count++;
    }

    void reactor_loop() {
        while (m_running.load(std::memory_order_acquire)) {
            process_timers();
            process_messages();
            std::this_thread::sleep_for(std::chrono::milliseconds(10));
        }
    }

    void process_timers() {
        auto now = std::chrono::steady_clock::now();
        std::lock_guard<std::mutex> lock(m_timer_mutex);
        std::vector<TimerEntry> reinsert;
        while (!m_timer_queue.empty()) {
            auto top = m_timer_queue.top();
            if (top.m_next_deadline > now) break;
            m_timer_queue.pop();
            bool missed = (now - top.m_next_deadline) > kSkipThreshold;
            if (missed && top.m_skippable) {
                top.m_next_deadline = now + top.m_interval;
                reinsert.push_back(std::move(top));
                continue;
            }
            Task task;
            task.m_name = top.m_name;
            task.m_fn = std::move(top.m_fn);
            task.m_skippable = top.m_skippable;
            m_cpu_queue.try_push(std::move(task));
            top.m_next_deadline = now + top.m_interval;
            reinsert.push_back(std::move(top));
        }
        for (auto& t : reinsert) m_timer_queue.push(std::move(t));
    }

    void process_messages() {
        std::vector<GatewayMessage> batch;
        m_inbox->swap_out(batch);
        if (batch.empty()) return;

        if (m_message_processor) {
            Task task;
            task.m_name = "message_processor";
            task.m_fn = [this, batch = std::move(batch)]() mutable {
                m_message_processor(batch);
            };
            task.m_skippable = false;
            m_cpu_queue.try_push(std::move(task));
        }

        if (m_data_evolver) {
            Task evolve_task;
            evolve_task.m_name = "data_evolver";
            evolve_task.m_fn = [this]() { m_data_evolver(); };
            evolve_task.m_skippable = true;
            m_cpu_queue.try_push(std::move(evolve_task));
        }
    }

    void io_loop() {
        while (m_running.load(std::memory_order_acquire)) {
            auto opt = m_io_queue.take_for(std::chrono::milliseconds(100));
            if (!opt) continue;
            auto& task = *opt;
            auto start = std::chrono::steady_clock::now();
            try {
                if (task.m_fn) task.m_fn();
            } catch (...) {}
            auto duration = std::chrono::duration_cast<std::chrono::microseconds>(
                std::chrono::steady_clock::now() - start);
            record_stats(task, duration);
        }
    }

    void cpu_loop() {
        while (m_running.load(std::memory_order_acquire)) {
            auto opt = m_cpu_queue.take_for(std::chrono::milliseconds(100));
            if (!opt) continue;
            auto& task = *opt;
            auto start = std::chrono::steady_clock::now();
            try {
                if (task.m_fn) task.m_fn();
            } catch (...) {}
            auto duration = std::chrono::duration_cast<std::chrono::microseconds>(
                std::chrono::steady_clock::now() - start);
            record_stats(task, duration);
        }
    }

    std::shared_ptr<CopyChannel<GatewayMessage>> m_inbox;
    tight::BlockingQueue<Task> m_io_queue{256};
    tight::BlockingQueue<Task> m_cpu_queue{512};
    std::function<void(std::vector<GatewayMessage>&)> m_message_processor;
    std::function<void()> m_data_evolver;

    std::priority_queue<TimerEntry, std::vector<TimerEntry>, std::greater<TimerEntry>> m_timer_queue;
    mutable std::mutex m_timer_mutex;

    std::atomic<bool> m_running{false};
    std::thread m_reactor_thread;
    std::thread m_io_thread;
    std::thread m_cpu_thread;

    mutable std::mutex m_stats_mutex;
    std::unordered_map<std::string, TaskStats> m_task_stats;
};

} // namespace gateway
