// backpressure_bench.cpp
//
// Same 4-stage pipeline as src/pipeline_demo.cpp, but instrumented to
// actually answer the backpressure question quantitatively instead of
// just printing a throughput summary:
//   - samples queue occupancy (of the queue right before the artificially
//     slowed Writer stage) at fixed intervals, so you can see it climb
//     to capacity and plateau
//   - records per-record end-to-end latency (source push -> sink
//     consume), reporting p50/p99
//   - reports CPU time consumed by the pipeline threads (relevant mainly
//     for the lock-free variant, which spins while blocked -- expect
//     this to be visibly higher than the mutex variant under sustained
//     backpressure, since blocking via condvar sleeps the thread instead
//     of spinning)
//
// Usage: ./backpressure_bench_{mutex,lockfree} <num_records> <capacity> <writer_delay_us> [out_prefix]
//
// Writes <out_prefix>_occupancy.csv (elapsed_ms,occupancy) and
// <out_prefix>_latency.csv (record_id,latency_us) for offline plotting.

#include <algorithm>
#include <atomic>
#include <chrono>
#include <cstdio>
#include <cstdlib>
#include <fstream>
#include <optional>
#include <string>
#include <sys/resource.h>
#include <thread>
#include <vector>

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
using Clock = std::chrono::steady_clock;

namespace {

double now_ms(Clock::time_point start) {
    return std::chrono::duration<double, std::milli>(Clock::now() - start).count();
}

double cpu_seconds_used() {
    struct rusage usage;
    getrusage(RUSAGE_SELF, &usage);
    auto to_secs = [](const timeval& tv) { return tv.tv_sec + tv.tv_usec / 1e6; };
    return to_secs(usage.ru_utime) + to_secs(usage.ru_stime);
}

} // namespace

int main(int argc, char** argv) {
    size_t num_records = argc > 1 ? std::stoul(argv[1]) : 50000;
    size_t capacity = argc > 2 ? std::stoul(argv[2]) : 64;
    int writer_delay_us = argc > 3 ? std::stoi(argv[3]) : 20;
    std::string out_prefix = argc > 4 ? argv[4] : std::string("bench_") + kQueueKind;

    std::printf("backpressure_bench (%s queue): records=%zu capacity=%zu writer_delay_us=%d\n",
                kQueueKind, num_records, capacity, writer_delay_us);

    Queue q_reader_parser(capacity);
    Queue q_parser_filter(capacity);
    Queue q_filter_writer(capacity); // the one we sample -- right before the slow stage

    std::vector<double> start_ms(num_records, -1.0);
    std::vector<double> end_ms(num_records, -1.0);

    auto t_start = Clock::now();
    double cpu_start = cpu_seconds_used();

    // Occupancy sampler: runs concurrently, records (elapsed_ms, occupancy)
    // of q_filter_writer every ~1ms until told to stop.
    std::atomic<bool> stop_sampling{false};
    std::vector<std::pair<double, size_t>> occupancy_samples;
    occupancy_samples.reserve(4096);
    std::thread sampler([&] {
        while (!stop_sampling.load(std::memory_order_relaxed)) {
            occupancy_samples.emplace_back(now_ms(t_start), q_filter_writer.size());
            std::this_thread::sleep_for(std::chrono::milliseconds(1));
        }
    });

    auto reader = pipeline::run_source<Record>(q_reader_parser, num_records,
        [&](size_t i) {
            start_ms[i] = now_ms(t_start);
            return Record(i, "line_" + std::to_string(i) + " value=" + std::to_string(i * 7 % 100));
        });

    auto parser = pipeline::run_stage<Record, Record>(q_reader_parser, q_parser_filter,
        [](Record r) -> std::optional<Record> {
            auto pos = r.raw.find("value=");
            r.field = (pos != std::string::npos) ? r.raw.substr(pos + 6) : "";
            return r;
        });

    auto filter = pipeline::run_stage<Record, Record>(q_parser_filter, q_filter_writer,
        [](Record r) -> std::optional<Record> {
            int value = r.field.empty() ? -1 : std::stoi(r.field);
            if (value % 2 != 0) return std::nullopt; // drop odd values
            return r;
        });

    auto writer = pipeline::run_sink<Record>(q_filter_writer,
        [&](Record r) {
            if (writer_delay_us > 0) {
                std::this_thread::sleep_for(std::chrono::microseconds(writer_delay_us));
            }
            end_ms[r.id] = now_ms(t_start);
        });

    reader.join();
    parser.join();
    filter.join();
    writer.join();
    stop_sampling.store(true, std::memory_order_relaxed);
    sampler.join();

    double wall_secs = now_ms(t_start) / 1000.0;
    double cpu_secs = cpu_seconds_used() - cpu_start;

    // Latency: only records that made it through the filter have a valid
    // end_ms (others were dropped, end_ms stays -1).
    std::vector<double> latencies_us;
    latencies_us.reserve(num_records);
    for (size_t i = 0; i < num_records; ++i) {
        if (end_ms[i] >= 0.0) {
            latencies_us.push_back((end_ms[i] - start_ms[i]) * 1000.0);
        }
    }
    std::sort(latencies_us.begin(), latencies_us.end());
    auto pct = [&](double p) {
        if (latencies_us.empty()) return 0.0;
        size_t idx = std::min(latencies_us.size() - 1,
                               static_cast<size_t>(p * latencies_us.size()));
        return latencies_us[idx];
    };

    std::printf("done: kept=%zu/%zu  wall=%.3fs  cpu=%.3fs (%.1fx wall)\n",
                latencies_us.size(), num_records, wall_secs, cpu_secs, cpu_secs / wall_secs);
    std::printf("latency (us): p50=%.1f  p90=%.1f  p99=%.1f  max=%.1f\n",
                pct(0.50), pct(0.90), pct(0.99),
                latencies_us.empty() ? 0.0 : latencies_us.back());

    std::ofstream occ_out(out_prefix + "_occupancy.csv");
    occ_out << "elapsed_ms,occupancy\n";
    for (auto& [t, occ] : occupancy_samples) occ_out << t << "," << occ << "\n";

    std::ofstream lat_out(out_prefix + "_latency.csv");
    lat_out << "latency_us\n";
    for (double l : latencies_us) lat_out << l << "\n";

    std::printf("wrote %s_occupancy.csv and %s_latency.csv\n", out_prefix.c_str(), out_prefix.c_str());
    return 0;
}
