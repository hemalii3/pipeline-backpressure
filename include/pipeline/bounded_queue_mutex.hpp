#pragma once

#include <condition_variable>
#include <deque>
#include <mutex>
#include <optional>

namespace pipeline {

// Bounded blocking queue using std::mutex + std::condition_variable.
//
// This is the "obviously correct" baseline: push() blocks (via OS-level
// wait, not spinning) when the queue is full, pop() blocks when empty.
// Backpressure falls out naturally -- a slow consumer causes push() to
// block, which propagates upstream through however many stages are
// waiting to push into this queue.
//
// close() lets a producer signal "no more items coming"; pop() then
// drains whatever's left and returns std::nullopt once empty+closed, so
// consumers can exit their loop cleanly instead of blocking forever.
template <typename T>
class BoundedQueueMutex {
public:
    explicit BoundedQueueMutex(size_t capacity) : capacity_(capacity) {}

    // Blocks if the queue is full. Returns false if the queue was closed
    // before space became available (caller should stop producing).
    bool push(T item) {
        std::unique_lock<std::mutex> lock(mutex_);
        not_full_.wait(lock, [&] { return buf_.size() < capacity_ || closed_; });
        if (closed_) return false;
        buf_.push_back(std::move(item));
        lock.unlock();
        not_empty_.notify_one();
        return true;
    }

    // Blocks if the queue is empty and not yet closed. Returns nullopt
    // once the queue is closed AND drained -- the consumer's exit signal.
    std::optional<T> pop() {
        std::unique_lock<std::mutex> lock(mutex_);
        not_empty_.wait(lock, [&] { return !buf_.empty() || closed_; });
        if (buf_.empty()) return std::nullopt; // closed and drained
        T item = std::move(buf_.front());
        buf_.pop_front();
        lock.unlock();
        not_full_.notify_one();
        return item;
    }

    // Signals no more pushes will happen. Wakes any threads blocked in
    // push() or pop() so they can observe closed_ and exit.
    void close() {
        {
            std::lock_guard<std::mutex> lock(mutex_);
            closed_ = true;
        }
        not_full_.notify_all();
        not_empty_.notify_all();
    }

    size_t size() const {
        std::lock_guard<std::mutex> lock(mutex_);
        return buf_.size();
    }

    size_t capacity() const { return capacity_; }

private:
    mutable std::mutex mutex_;
    std::condition_variable not_full_;
    std::condition_variable not_empty_;
    std::deque<T> buf_;
    size_t capacity_;
    bool closed_ = false;
};

} // namespace pipeline
