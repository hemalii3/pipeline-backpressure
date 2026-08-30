#include <gtest/gtest.h>

#include <atomic>
#include <chrono>
#include <numeric>
#include <thread>
#include <vector>

#include "pipeline/bounded_queue_mutex.hpp"

using pipeline::BoundedQueueMutex;

TEST(BoundedQueueMutex, PushPopSingleThreadFIFO) {
    BoundedQueueMutex<int> q(4);
    ASSERT_TRUE(q.push(1));
    ASSERT_TRUE(q.push(2));
    ASSERT_TRUE(q.push(3));

    EXPECT_EQ(q.pop(), 1);
    EXPECT_EQ(q.pop(), 2);
    EXPECT_EQ(q.pop(), 3);
}

TEST(BoundedQueueMutex, SizeTracksPushesAndPops) {
    BoundedQueueMutex<int> q(4);
    EXPECT_EQ(q.size(), 0u);
    q.push(1);
    q.push(2);
    EXPECT_EQ(q.size(), 2u);
    q.pop();
    EXPECT_EQ(q.size(), 1u);
}

TEST(BoundedQueueMutex, PushBlocksWhenFullUntilPop) {
    BoundedQueueMutex<int> q(2);
    q.push(1);
    q.push(2);

    std::atomic<bool> push_returned{false};
    std::thread producer([&] {
        q.push(3); // should block until a slot opens
        push_returned.store(true);
    });

    // Give the producer a moment to actually block. This is a timing-based
    // check (not airtight), but sufficient to catch a push() that doesn't
    // block at all, which is the failure mode we care about here.
    std::this_thread::sleep_for(std::chrono::milliseconds(50));
    EXPECT_FALSE(push_returned.load()) << "push() returned before a slot was free";

    q.pop(); // frees a slot
    producer.join();
    EXPECT_TRUE(push_returned.load());
}

TEST(BoundedQueueMutex, PopBlocksWhenEmptyUntilPush) {
    BoundedQueueMutex<int> q(4);

    std::atomic<bool> pop_returned{false};
    int popped_value = -1;
    std::thread consumer([&] {
        auto v = q.pop();
        popped_value = *v;
        pop_returned.store(true);
    });

    std::this_thread::sleep_for(std::chrono::milliseconds(50));
    EXPECT_FALSE(pop_returned.load()) << "pop() returned before an item was pushed";

    q.push(42);
    consumer.join();
    EXPECT_TRUE(pop_returned.load());
    EXPECT_EQ(popped_value, 42);
}

TEST(BoundedQueueMutex, CloseUnblocksWaitingPushAndPop) {
    BoundedQueueMutex<int> q(1);
    q.push(1); // fill it

    std::thread blocked_producer([&] {
        bool ok = q.push(2); // should unblock via close(), return false
        EXPECT_FALSE(ok);
    });

    std::this_thread::sleep_for(std::chrono::milliseconds(20));
    q.close();
    blocked_producer.join();

    // pop() should still drain the one item that was already in the queue...
    EXPECT_EQ(q.pop(), 1);
    // ...then return nullopt once drained.
    EXPECT_EQ(q.pop(), std::nullopt);
}

// Concurrency stress test: N producers each push a disjoint range of
// values, M consumers pop until the queue is closed+drained and record
// what they saw. No values should be lost or duplicated. This is the
// test to run under -DSANITIZE=thread -- a subtle bug in the condvar
// predicate or lock scope would show up as either a TSan report or a
// mismatched final sum here.
TEST(BoundedQueueMutex, ConcurrentProducersConsumersNoLostItems) {
    constexpr int kProducers = 4;
    constexpr int kConsumers = 3;
    constexpr int kItemsPerProducer = 5000;
    constexpr int kTotalItems = kProducers * kItemsPerProducer;

    BoundedQueueMutex<int> q(16); // small capacity to force lots of blocking

    std::vector<std::thread> producers;
    for (int p = 0; p < kProducers; ++p) {
        producers.emplace_back([&, p] {
            for (int i = 0; i < kItemsPerProducer; ++i) {
                q.push(p * kItemsPerProducer + i);
            }
        });
    }

    std::atomic<long long> sum{0};
    std::atomic<int> received{0};
    std::vector<std::thread> consumers;
    for (int c = 0; c < kConsumers; ++c) {
        consumers.emplace_back([&] {
            for (;;) {
                auto v = q.pop();
                if (!v) break;
                sum.fetch_add(*v, std::memory_order_relaxed);
                received.fetch_add(1, std::memory_order_relaxed);
            }
        });
    }

    for (auto& t : producers) t.join();
    q.close(); // all producers done -- signal consumers to exit once drained
    for (auto& t : consumers) t.join();

    EXPECT_EQ(received.load(), kTotalItems);

    long long expected_sum = 0;
    for (int i = 0; i < kTotalItems; ++i) expected_sum += i;
    EXPECT_EQ(sum.load(), expected_sum) << "lost or duplicated items under concurrency";
}
