#!/usr/bin/env python3
"""
plot_backpressure.py

Renders two comparison figures from backpressure_bench's CSV output:
  1. Queue occupancy over time, mutex vs lock-free, on one axes -- shows
     both climbing to capacity and plateauing under backpressure.
  2. Latency distribution (CDF), mutex vs lock-free.

Usage:
    ./build/benchmarks/backpressure_bench_mutex 20000 32 30 mutex
    ./build/benchmarks/backpressure_bench_lockfree 20000 32 30 lockfree
    python3 scripts/plot_backpressure.py --mutex-prefix mutex --lockfree-prefix lockfree
"""
import argparse
import csv

import matplotlib.pyplot as plt


def load_occupancy(path):
    xs, ys = [], []
    with open(path) as f:
        for row in csv.DictReader(f):
            xs.append(float(row["elapsed_ms"]))
            ys.append(int(row["occupancy"]))
    return xs, ys


def load_latency(path):
    vals = []
    with open(path) as f:
        for row in csv.DictReader(f):
            vals.append(float(row["latency_us"]))
    vals.sort()
    return vals


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--mutex-prefix", default="bench_mutex")
    ap.add_argument("--lockfree-prefix", default="bench_lockfree")
    ap.add_argument("--out", default="docs/backpressure.png")
    args = ap.parse_args()

    mx, my = load_occupancy(f"{args.mutex_prefix}_occupancy.csv")
    lx, ly = load_occupancy(f"{args.lockfree_prefix}_occupancy.csv")
    m_lat = load_latency(f"{args.mutex_prefix}_latency.csv")
    l_lat = load_latency(f"{args.lockfree_prefix}_latency.csv")

    fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(13, 5))

    ax1.plot(mx, my, label="mutex", color="#2b6cb0", linewidth=1)
    ax1.plot(lx, ly, label="lock-free", color="#c05621", linewidth=1)
    ax1.set_xlabel("Elapsed time (ms)")
    ax1.set_ylabel("Queue occupancy (items)")
    ax1.set_title("Occupancy under backpressure")
    ax1.legend()
    ax1.grid(True, linestyle="--", alpha=0.3)

    def cdf(vals):
        n = len(vals)
        return vals, [i / n for i in range(1, n + 1)]

    mx2, my2 = cdf(m_lat)
    lx2, ly2 = cdf(l_lat)
    ax2.plot(mx2, my2, label="mutex", color="#2b6cb0")
    ax2.plot(lx2, ly2, label="lock-free", color="#c05621")
    ax2.set_xlabel("End-to-end latency (us)")
    ax2.set_ylabel("CDF")
    ax2.set_title("Latency distribution")
    ax2.legend()
    ax2.grid(True, linestyle="--", alpha=0.3)

    fig.tight_layout()
    fig.savefig(args.out, dpi=150)
    print(f"Wrote {args.out}")


if __name__ == "__main__":
    main()
