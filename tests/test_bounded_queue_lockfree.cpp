#include <gtest/gtest.h>

#include <atomic>
#include <thread>
#include <vector>

#include "pipeline/bounded_queue_lockfree.hpp"

using pipeline::BoundedQueueLockFree;

TEST(BoundedQueueLockFree, PushPopSingleThreadFIFO) {
    BoundedQueueLockFree<int> q(4);
    ASSERT_TRUE(q.push(1));
    ASSERT_TRUE(q.push(2));
    ASSERT_TRUE(q.push(3));

    EXPECT_EQ(q.pop(), 1);
    EXPECT_EQ(q.pop(), 2);
    EXPECT_EQ(q.pop(), 3);
}

TEST(BoundedQueueLockFree, SizeTracksPushesAndPops) {
    BoundedQueueLockFree<int> q(4);
    EXPECT_EQ(q.size(), 0u);
    q.push(1);
    q.push(2);
    EXPECT_EQ(q.size(), 2u);
    q.pop();
    EXPECT_EQ(q.size(), 1u);
}

TEST(BoundedQueueLockFree, CloseUnblocksSpinningPushAndPop) {
    BoundedQueueLockFree<int> q(1);
    q.push(1); // fill it

    std::atomic<bool> push_returned{false};
    std::thread blocked_producer([&] {
        bool ok = q.push(2); // should be unblocked by close(), return false
        EXPECT_FALSE(ok);
        push_returned.store(true);
    });

    std::this_thread::sleep_for(std::chrono::milliseconds(20));
    q.close();
    blocked_producer.join();
    EXPECT_TRUE(push_returned.load());

    // Drain the one item that was already in before close()...
    EXPECT_EQ(q.pop(), 1);
    // ...then nullopt once drained.
    EXPECT_EQ(q.pop(), std::nullopt);
}

// IMPORTANT: this queue is SPSC only. This stress test uses exactly one
// producer thread and one consumer thread -- do not "extend" this test to
// multiple producers/consumers without first redesigning the queue (see
// the class comment in bounded_queue_lockfree.hpp for why: the two plain
// atomic indices here are only safe with a single writer and single
// reader each).
//
// This is the test to run under -DSANITIZE=thread. If the release/acquire
// pairing in push()/pop() were wrong (e.g. accidentally relaxed), this
// test would likely still pass most of the time on x86 (strong memory
// model masks many ordering bugs) but TSan's happens-before analysis
// would flag the race regardless of whether it manifested as a wrong
// answer on this particular run -- that gap is exactly why TSan matters
// for lock-free code more than for mutex-based code.
TEST(BoundedQueueLockFree, SPSCConcurrentProducerConsumerNoLostItems) {
    constexpr int kItems = 200000;
    BoundedQueueLockFree<int> q(64); // small capacity to force lots of spin-waiting

    std::thread producer([&] {
        for (int i = 0; i < kItems; ++i) {
            q.push(i);
        }
        q.close();
    });

    std::vector<int> received;
    received.reserve(kItems);
    std::thread consumer([&] {
        for (;;) {
            auto v = q.pop();
            if (!v) break;
            received.push_back(*v);
        }
    });

    producer.join();
    consumer.join();

    ASSERT_EQ(received.size(), static_cast<size_t>(kItems));
    // FIFO ordering must hold exactly for SPSC -- unlike the MPMC mutex
    // test, there's no interleaving ambiguity to allow for.
    for (int i = 0; i < kItems; ++i) {
        ASSERT_EQ(received[i], i) << "FIFO order violated at index " << i;
    }
}
