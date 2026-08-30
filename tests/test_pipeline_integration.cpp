#include <gtest/gtest.h>

#include <atomic>
#include <vector>

#include "pipeline/bounded_queue_lockfree.hpp"
#include "pipeline/bounded_queue_mutex.hpp"
#include "pipeline/stage.hpp"

using pipeline::BoundedQueueLockFree;
using pipeline::BoundedQueueMutex;

namespace {

// Runs source -> stage(double it) -> stage(drop odd) -> sink(collect),
// generic over queue type, and returns the collected results so the
// pipeline-wiring logic (not just individual queues) gets exercised.
template <typename Queue>
std::vector<int> run_three_stage_pipeline(int count, size_t capacity) {
    Queue q1(capacity);
    Queue q2(capacity);

    auto source = pipeline::run_source<int>(q1, static_cast<size_t>(count),
        [](size_t i) { return static_cast<int>(i); });

    auto doubler = pipeline::run_stage<int, int>(q1, q2,
        [](int x) -> std::optional<int> { return x * 2; });

    std::vector<int> results;
    std::mutex results_mutex;
    auto sink = pipeline::run_sink<int>(q2, [&](int x) {
        std::lock_guard<std::mutex> lock(results_mutex);
        results.push_back(x);
    });

    source.join();
    doubler.join();
    sink.join();
    return results;
}

} // namespace

TEST(PipelineIntegration, MutexQueueEndToEndAllItemsProcessed) {
    constexpr int kCount = 10000;
    auto results = run_three_stage_pipeline<BoundedQueueMutex<int>>(kCount, 8);

    ASSERT_EQ(results.size(), static_cast<size_t>(kCount));
    // Order isn't guaranteed to be preserved end-to-end in general (a
    // sink behind a queue is still FIFO per-queue here since it's single
    // producer/consumer per queue in this specific 3-stage shape), so
    // check the doubling was applied correctly for every value seen.
    std::vector<bool> seen(kCount, false);
    for (int v : results) {
        ASSERT_EQ(v % 2, 0);
        int original = v / 2;
        ASSERT_GE(original, 0);
        ASSERT_LT(original, kCount);
        ASSERT_FALSE(seen[original]) << "duplicate result for input " << original;
        seen[original] = true;
    }
    for (bool s : seen) ASSERT_TRUE(s) << "some input was never processed";
}

TEST(PipelineIntegration, LockFreeQueueEndToEndAllItemsProcessed) {
    constexpr int kCount = 10000;
    auto results = run_three_stage_pipeline<BoundedQueueLockFree<int>>(kCount, 8);

    ASSERT_EQ(results.size(), static_cast<size_t>(kCount));
    std::vector<bool> seen(kCount, false);
    for (int v : results) {
        ASSERT_EQ(v % 2, 0);
        int original = v / 2;
        ASSERT_GE(original, 0);
        ASSERT_LT(original, kCount);
        ASSERT_FALSE(seen[original]) << "duplicate result for input " << original;
        seen[original] = true;
    }
    for (bool s : seen) ASSERT_TRUE(s) << "some input was never processed";
}

TEST(PipelineIntegration, FilterStageDropsItemsCorrectly) {
    // Exercises the nullopt-drop path in run_stage specifically.
    constexpr int kCount = 1000;
    BoundedQueueMutex<int> q1(8), q2(8);

    auto source = pipeline::run_source<int>(q1, kCount, [](size_t i) { return static_cast<int>(i); });
    auto filter = pipeline::run_stage<int, int>(q1, q2,
        [](int x) -> std::optional<int> {
            if (x % 2 == 0) return x; // keep evens
            return std::nullopt;       // drop odds
        });

    std::vector<int> results;
    auto sink = pipeline::run_sink<int>(q2, [&](int x) { results.push_back(x); });

    source.join();
    filter.join();
    sink.join();

    EXPECT_EQ(results.size(), static_cast<size_t>(kCount / 2));
    for (int v : results) EXPECT_EQ(v % 2, 0);
}
