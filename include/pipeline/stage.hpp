#pragma once

#include <functional>
#include <thread>

namespace pipeline {

// Runs a single pipeline stage on its own thread: pop from `in`, apply
// `transform`, push to `out`. Templated on the queue type (Queue) so the
// same stage code works with BoundedQueueMutex or BoundedQueueLockFree
// without any virtual dispatch overhead.
//
// `transform` returns std::optional<Out>: nullopt means "drop this item"
// (used by the Filter stage), otherwise the returned value is pushed
// downstream.
//
// The stage exits its loop when `in.pop()` returns nullopt (upstream
// closed and drained), and then closes `out` itself so the next stage's
// exit signal propagates automatically -- one close() call at the very
// front of the pipeline cascades all the way to the end.
template <typename In, typename Out, typename InQueue, typename OutQueue>
std::thread run_stage(InQueue& in, OutQueue& out,
                       std::function<std::optional<Out>(In)> transform) {
    return std::thread([&in, &out, transform = std::move(transform)]() {
        for (;;) {
            auto item = in.pop();
            if (!item) break; // upstream closed and drained
            auto result = transform(std::move(*item));
            if (result) {
                out.push(std::move(*result));
            }
        }
        out.close();
    });
}

// Runs a source stage: no input queue, just generates `count` items via
// `generate(i)` and pushes them to `out`, then closes `out`.
template <typename Out, typename OutQueue>
std::thread run_source(OutQueue& out, size_t count,
                        std::function<Out(size_t)> generate) {
    return std::thread([&out, count, generate = std::move(generate)]() {
        for (size_t i = 0; i < count; ++i) {
            out.push(generate(i));
        }
        out.close();
    });
}

// Runs a sink stage: pop from `in` until closed+drained, calling
// `consume` on each item. Returns the thread; the caller typically joins
// it to know when the whole pipeline has finished.
template <typename In, typename InQueue>
std::thread run_sink(InQueue& in, std::function<void(In)> consume) {
    return std::thread([&in, consume = std::move(consume)]() {
        for (;;) {
            auto item = in.pop();
            if (!item) break;
            consume(std::move(*item));
        }
    });
}

} // namespace pipeline
