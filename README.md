# Concurrent Pipeline with Backpressure: Mutex vs. Lock-Free

A 4-stage concurrent pipeline (`Reader -> Parser -> Filter -> Writer`),
implemented twice with different bounded-queue synchronization strategies,
compared under artificial backpressure. Built to answer a specific
question rather than just implement two textbook primitives: **when the
downstream consumer is slow, does swapping in a lock-free queue actually
help, and what does it cost?**

## TL;DR result

Lock-free wins on raw uncontended throughput (~1.5-2.3x faster with no
backpressure in local testing) but **burns a full CPU core spinning while
"blocked"** under sustained backpressure — measured CPU time was ~1.0x
wall-clock time for the lock-free queue vs. ~0.2x for the mutex queue
under the same artificial writer delay, because the mutex version's
blocked threads are actually asleep (OS-scheduled wakeup via
`condition_variable`), while the lock-free version's blocked threads spin.
Latency was also *higher* for lock-free under sustained backpressure
(p99 ~7.2ms vs ~6.7ms in one test run) despite lock-free's lower
uncontended latency — spinning steals CPU time from the very threads that
need to run to drain the backlog. **The "lock-free is always faster"
intuition doesn't hold once the system is actually backpressured**, which
is the whole point of building both instead of just one.

See `docs/backpressure.png` (generate with the commands below) for the
occupancy and latency comparison plot.

## Architecture

```
Reader --[queue]--> Parser --[queue]--> Filter --[queue]--> Writer
```

Each stage runs on its own `std::thread`. Two interchangeable bounded
queue implementations connect them, sharing an identical interface
(`push`, `pop`, `close`, `size`, `capacity`) so the pipeline-wiring code
(`include/pipeline/stage.hpp`) is completely queue-agnostic — the
`pipeline_demo_mutex` and `pipeline_demo_lockfree` executables are built
from the *same* `src/pipeline_demo.cpp`, differing only in which queue
header gets `#include`d via a compile-time flag.

- **`BoundedQueueMutex<T>`** (`include/pipeline/bounded_queue_mutex.hpp`):
  `std::mutex` + two `std::condition_variable`s. `push()`/`pop()` block
  via OS-level wait when full/empty. MPMC-safe (any number of
  producers/consumers).

- **`BoundedQueueLockFree<T>`** (`include/pipeline/bounded_queue_lockfree.hpp`):
  atomics-based ring buffer, **SPSC only** (single producer, single
  consumer — see the class-level comment for why it's unsafe to extend to
  multiple producers/consumers without a redesign). `push()`/`pop()` spin
  with a yield-after-threshold backoff when full/empty. Memory ordering
  is deliberately not all-relaxed: the producer store-releases its index
  after writing data, the consumer load-acquires it before reading — this
  release/acquire pairing is what makes the written data visible to the
  consumer, and is exactly the kind of thing worth being able to explain
  in an interview rather than just getting right by luck.

Because the lock-free queue is SPSC-only, the 4-stage pipeline (one
thread per stage, so every queue in it genuinely has one producer and one
consumer) is the right shape to use it correctly — this wasn't an
accidental fit, it's why the pipeline is 4 single-threaded stages rather
than, say, a thread pool feeding a shared queue.

## Testing

Proper test setup, not just a manual smoke test:

- **Unit tests** (`tests/test_bounded_queue_*.cpp`): FIFO ordering,
  blocking/spinning behavior when full or empty, `close()` correctly
  unblocking waiters and signaling drain-completion.
- **Concurrency stress tests**: a real multi-producer/multi-consumer run
  against the mutex queue (4 producers, 3 consumers, 20k items) checking
  no item is lost or duplicated; a real single-producer/single-consumer
  run against the lock-free queue (200k items) checking exact FIFO order
  is preserved.
- **Integration tests** (`tests/test_pipeline_integration.cpp`): the
  actual `run_source`/`run_stage`/`run_sink` machinery wired into a
  3-stage pipeline end-to-end, for both queue types, including the
  item-dropping (`std::nullopt`) path used by Filter stages.
- **Run under ThreadSanitizer**: `cmake .. -DSANITIZE=thread` — this is
  the test that actually validates the lock-free queue's memory ordering
  is correct, since a subtly-wrong `memory_order_relaxed` can pass every
  functional test on x86 (strong memory model masks a lot) and still be
  a real bug on ARM or under compiler reordering. Both queue
  implementations pass clean under TSan in local testing (no reported
  races).
- **CI** (`.github/workflows/ci.yml`): builds and runs the full test
  suite three ways on every push — plain, `-DSANITIZE=thread`, and
  `-DSANITIZE=address+undefined`.

## Build

```bash
mkdir build && cd build
cmake .. -DCMAKE_BUILD_TYPE=Release
make -j
ctest --output-on-failure
```

With a sanitizer:
```bash
mkdir build-tsan && cd build-tsan
cmake .. -DCMAKE_BUILD_TYPE=Debug -DSANITIZE=thread
make -j
ctest --output-on-failure
```

## Running the demo

```bash
./build/src/pipeline_demo_mutex 50000 64 0        # no artificial delay
./build/src/pipeline_demo_lockfree 50000 64 0
./build/src/pipeline_demo_mutex 20000 64 5        # 5us writer delay -> backpressure
./build/src/pipeline_demo_lockfree 20000 64 5
```

## Running the backpressure benchmark

```bash
./build/benchmarks/backpressure_bench_mutex 20000 32 30 mutex
./build/benchmarks/backpressure_bench_lockfree 20000 32 30 lockfree
python3 scripts/plot_backpressure.py --mutex-prefix mutex --lockfree-prefix lockfree \
    --out docs/backpressure.png
```

This reports p50/p90/p99 end-to-end latency and CPU-time-vs-wall-time
ratio, and writes occupancy + latency CSVs for the comparison plot.

## Project structure

```
include/pipeline/
  record.hpp                  -- the item type flowing through the pipeline
  bounded_queue_mutex.hpp      -- MPMC blocking queue (condvar-based)
  bounded_queue_lockfree.hpp    -- SPSC lock-free ring buffer (atomics-based)
  stage.hpp                    -- generic source/stage/sink thread runners, queue-agnostic
src/
  pipeline_demo.cpp   -- the 4-stage demo, compiled twice (once per queue type)
benchmarks/
  backpressure_bench.cpp   -- instrumented version: occupancy sampling, latency percentiles, CPU-time accounting
tests/
  test_bounded_queue_mutex.cpp       -- correctness + MPMC concurrency stress test
  test_bounded_queue_lockfree.cpp    -- correctness + SPSC concurrency stress test
  test_pipeline_integration.cpp      -- end-to-end pipeline wiring tests, both queue types
scripts/
  plot_backpressure.py   -- renders the occupancy + latency comparison figure
```

## Design decisions worth defending in an interview

- **Why SPSC for lock-free, not MPMC?** MPMC lock-free queues need a
  fundamentally different design (per-slot sequence numbers, e.g.
  Vyukov's bounded MPMC queue) because two plain head/tail indices are
  only safe with exactly one writer and one reader each. Rather than
  half-implement a harder primitive, this project uses a pipeline shape
  (one thread per stage) where SPSC is the *correct* fit, not a
  simplification papering over a gap.
- **Why does closing propagate downstream automatically?** Each stage's
  thread closes its own output queue when its input queue signals
  closed+drained (see `run_stage` in `stage.hpp`). One `close()` call at
  the very front of the pipeline cascades all the way to the sink without
  every stage needing to know about total pipeline shutdown.
- **Why measure CPU time, not just wall-clock throughput?** Wall-clock
  numbers alone would make lock-free look strictly better under
  backpressure (it's not slower, usually). CPU-time accounting is what
  actually surfaces the real cost: a spinning thread looks "fast" in wall
  time while quietly consuming a full core that could be doing something
  else, which matters a great deal on a shared or cost-constrained
  system and is invisible if you only look at throughput.

## Known limitations / open items

- The lock-free queue's spin-then-yield backoff is simple; a production
  version would likely want exponential backoff or `_mm_pause()` on x86 to
  reduce power/cache-bus pressure while spinning — not implemented here
  since the point was to measure the basic tradeoff, not build a
  production-grade primitive.
- Benchmark numbers above are from local/interactive runs and will vary
  by machine; re-run `backpressure_bench` on your own hardware before
  quoting specific figures in an interview — the qualitative
  finding (lock-free burns CPU under backpressure) is robust, the exact
  ratios are not.
