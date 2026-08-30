// pipeline_demo.cpp
//
// Reader -> Parser -> Filter -> Writer, four stages each on their own
// thread, connected by bounded queues. Compiled twice (see
// src/CMakeLists.txt): once with QUEUE_MUTEX defined (uses
// BoundedQueueMutex), once with QUEUE_LOCKFREE defined (uses
// BoundedQueueLockFree) -- same pipeline logic, different queue, so any
// behavior difference you observe is attributable to the queue choice.
//
// Usage: ./pipeline_demo_{mutex,lockfree} <num_records> <queue_capacity> <writer_delay_us>
//
// writer_delay_us artificially slows the Writer stage to create
// backpressure -- that's the whole point of this demo. Try increasing it
// and watch queue occupancy climb toward capacity and plateau (see
// benchmarks/backpressure_bench.cpp for the instrumented version that
// actually records this over time instead of just printing a summary).

#include <chrono>
#include <cstdio>
#include <cstdlib>
#include <optional>
#include <string>
#include <thread>

#include "pipeline/record.hpp"
#include "pipeline/stage.hpp"

#if defined(QUEUE_MUTEX)
#include "pipeline/bounded_queue_mutex.hpp"
using Queue = pipeline::BoundedQueueMutex<pipeline::Record>;
static const char* kQueueKind = "mutex";
#elif defined(QUEUE_LOCKFREE)
#include "pipeline/bounded_queue_lockfree.hpp"
using Queue = pipeline::BoundedQueueLockFree<pipeline::Record>;
static const char* kQueueKind = "lockfree";
#else
#error "Define QUEUE_MUTEX or QUEUE_LOCKFREE"
#endif

using pipeline::Record;

int main(int argc, char** argv) {
    size_t num_records = argc > 1 ? std::stoul(argv[1]) : 100000;
    size_t capacity = argc > 2 ? std::stoul(argv[2]) : 64;
    int writer_delay_us = argc > 3 ? std::stoi(argv[3]) : 0;

    std::printf("pipeline_demo (%s queue): records=%zu capacity=%zu writer_delay_us=%d\n",
                kQueueKind, num_records, capacity, writer_delay_us);

    Queue q_reader_parser(capacity);
    Queue q_parser_filter(capacity);
    Queue q_filter_writer(capacity);

    size_t written_count = 0;

    auto t_start = std::chrono::steady_clock::now();

    // Reader: generates synthetic "log lines"
    auto reader = pipeline::run_source<Record>(q_reader_parser, num_records,
        [](size_t i) {
            return Record(i, "line_" + std::to_string(i) + " level=INFO value=" + std::to_string(i * 7 % 100));
        });

    // Parser: extracts a "field" from raw -- trivial here, stands in for
    // real parsing work.
    auto parser = pipeline::run_stage<Record, Record>(q_reader_parser, q_parser_filter,
        [](Record r) -> std::optional<Record> {
            auto pos = r.raw.find("value=");
            r.field = (pos != std::string::npos) ? r.raw.substr(pos + 6) : "";
            return r;
        });

    // Filter: drops records that don't meet some condition (here: even
    // parsed value) -- stands in for a real filter/business-rule stage.
    auto filter = pipeline::run_stage<Record, Record>(q_parser_filter, q_filter_writer,
        [](Record r) -> std::optional<Record> {
            int value = r.field.empty() ? -1 : std::stoi(r.field);
            r.keep = (value % 2 == 0);
            if (!r.keep) return std::nullopt; // drop
            return r;
        });

    // Writer: the deliberately-slowed-down sink stage.
    auto writer = pipeline::run_sink<Record>(q_filter_writer,
        [&](Record r) {
            if (writer_delay_us > 0) {
                std::this_thread::sleep_for(std::chrono::microseconds(writer_delay_us));
            }
            ++written_count;
            (void)r;
        });

    reader.join();
    parser.join();
    filter.join();
    writer.join();

    auto t_end = std::chrono::steady_clock::now();
    double secs = std::chrono::duration<double>(t_end - t_start).count();

    std::printf("done: written=%zu (%.1f%% kept) in %.3fs -> %.1f records/sec\n",
                written_count, 100.0 * written_count / num_records, secs,
                written_count / secs);

    return 0;
}
