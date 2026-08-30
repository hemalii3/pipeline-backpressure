#pragma once

#include <atomic>
#include <cstddef>
#include <optional>
#include <thread>
#include <vector>

namespace pipeline {

// Lock-free bounded SPSC (single-producer, single-consumer) ring buffer.
//
// This is NOT safe for multiple producers or multiple consumers -- if you
// need that, look at Vyukov's bounded MPMC queue design (a per-slot
// sequence number instead of the two plain indices used here); don't just
// slap more threads on this without redesigning it.
//
// Memory ordering, and why it's not all relaxed:
//   - The producer publishes an item by writing the data, THEN
//     store-releasing the updated head_. The consumer load-acquires
//     head_ before reading the data. This release/acquire pair is what
//     makes the data write visible to the consumer -- without it, the
//     compiler or CPU could reorder the data write after the index
//     update, and the consumer could read a stale/torn slot despite
//     seeing the "new" index.
//   - Symmetrically for tail_: consumer store-releases after consuming,
//     producer load-acquires tail_ before checking if there's space.
//   - The producer's own head_ load (to check fullness) and consumer's
//     own tail_ load (to check emptiness) use relaxed ordering -- each
//     thread only reads back its OWN previous writes to that index, so
//     no cross-thread synchronization is needed there.
//
// Backpressure: push()/pop() spin (with a brief yield after a threshold)
// when full/empty, rather than blocking via the OS like the mutex
// version. This trades lower latency (no syscall, no context switch) for
// burning CPU while blocked -- worth measuring explicitly, not assuming.
template <typename T>
class BoundedQueueLockFree {
public:
    explicit BoundedQueueLockFree(size_t capacity)
        : capacity_(capacity), buf_(capacity), head_(0), tail_(0) {}

    // Spins until space is available or the queue is closed. Returns
    // false if closed before space appeared.
    bool push(T item) {
        size_t spins = 0;
        for (;;) {
            size_t head = head_.load(std::memory_order_relaxed);
            size_t tail = tail_.load(std::memory_order_acquire);
            if (head - tail < capacity_) {
                buf_[head % capacity_] = std::move(item);
                head_.store(head + 1, std::memory_order_release);
                return true;
            }
            if (closed_.load(std::memory_order_acquire)) return false;
            spin_backoff(spins++);
        }
    }

    // Spins until an item is available or the queue is closed+drained.
    // Returns nullopt in the closed+drained case (consumer exit signal).
    std::optional<T> pop() {
        size_t spins = 0;
        for (;;) {
            size_t tail = tail_.load(std::memory_order_relaxed);
            size_t head = head_.load(std::memory_order_acquire);
            if (tail < head) {
                T item = std::move(buf_[tail % capacity_]);
                tail_.store(tail + 1, std::memory_order_release);
                return item;
            }
            if (closed_.load(std::memory_order_acquire)) return std::nullopt;
            spin_backoff(spins++);
        }
    }

    void close() { closed_.store(true, std::memory_order_release); }

    // Approximate -- reads two atomics without a shared snapshot, so this
    // can be momentarily stale under concurrent push/pop. Fine for
    // monitoring/occupancy sampling, not for correctness decisions.
    size_t size() const {
        size_t head = head_.load(std::memory_order_acquire);
        size_t tail = tail_.load(std::memory_order_acquire);
        return head - tail;
    }

    size_t capacity() const { return capacity_; }

private:
    static void spin_backoff(size_t spins) {
        if (spins < 1000) {
            // busy-spin briefly -- cheapest option for very short waits
        } else {
            std::this_thread::yield();
        }
    }

    size_t capacity_;
    std::vector<T> buf_;
    alignas(64) std::atomic<size_t> head_; // producer-owned index
    alignas(64) std::atomic<size_t> tail_; // consumer-owned index
    std::atomic<bool> closed_{false};
};

} // namespace pipeline
