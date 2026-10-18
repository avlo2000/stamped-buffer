# stamped-buffer benchmarks

These benchmarks use the current production headers directly. They measure
small timestamped sensor samples, batch copying and concurrent readers.
Requirements: Linux, GCC or Clang with C++17 support, and CMake. The C++
executable runs the full suite directly. Building benchmarks does not download
test or benchmark dependencies.

## Run

```sh
cmake -S . -B /tmp/stamped-buffer-bench -DCMAKE_BUILD_TYPE=Release \
  -DSTAMPED_BUFFER_BUILD_TESTS=OFF -DSTAMPED_BUFFER_BUILD_BENCHMARKS=ON \
  -DSTAMPED_BUFFER_BENCHMARK_NATIVE=ON
cmake --build /tmp/stamped-buffer-bench --target stamped_buffer_benchmark
/tmp/stamped-buffer-bench/benchmarks/stamped_buffer_benchmark --suite > /tmp/stamped-buffer-results.jsonl
```

This builds a Release executable with native CPU tuning. `--suite` verifies
sample integrity, then runs 330 cases sequentially, shuffling case order within
each workload and repetition with a fixed seed. It emits one JSON line per
case to standard output, including latency, throughput and CPU measurements.
Progress and completion messages go to standard error. A nonzero exit status
indicates failure; a partial output file is not a completed run.

Results contain raw repetitions; aggregation and machine metadata are not
generated automatically. Record your compiler, CPU, build flags and affinity
alongside results when comparing runs. The optional `benchmarks/results/`
directory is ignored by Git. Measurements and build products are not part of
the installed library.

Use `--quick` to check all four workloads with reduced capacities, short
durations and one repetition (46 cases). These numbers are smoke-test output,
not reliable performance evidence. Explicit options override quick defaults:

```sh
/tmp/stamped-buffer-bench/benchmarks/stamped_buffer_benchmark --quick > /tmp/stamped-buffer-smoke.jsonl
/tmp/stamped-buffer-bench/benchmarks/stamped_buffer_benchmark --suite \
  --micro-repeats 5 --batch-repeats 3 --concurrent-repeats 3 \
  --micro-seconds .12 --concurrent-seconds 1.25 --readers 1,2,4,8 \
  > /tmp/stamped-buffer-results.jsonl
```

`--help` lists the options. Shell redirection overwrites an existing file;
choose a fresh results path to retain earlier measurements.

## Workloads

| Mode | Work measured | Default cases |
| --- | --- | --- |
| `micro` | Overwriting writes, timestamp lookup, latest-sample copy | 16/32/64/128-byte records; capacities 64/4,096/5,000/65,536; five runs |
| `batch` | `get_last_n` returning a vector or using caller-owned storage | 32/64-byte records; batches of 8/32/128/512; capacity 4,096; three runs |
| `concurrent` | One producer and one timestamp reader, paced or continuously active | 32/64-byte records; capacity 4,096; 1 kHz/10 kHz/unpaced; three runs |
| `multi` | One continuous producer and multiple timestamp readers | 32/64-byte records; capacity 4,096; 1/2/4/8 readers; three runs |

Record sizes include the eight-byte timestamp. Buffers start filled, and static
workloads rotate the history before timing. Timestamp queries cover exact hits
and gaps between ordered, unique timestamps. Reads materialize the full sample.
The multiple-reader workload uses exact timestamps from the recent half-window.
Production unit tests separately cover empty buffers, duplicates and edge cases.

Isolated operations warm up for 8,192 calls, then measure batched throughput for
at least 120 ms by default, followed by a separate 10,000-call latency pass. Batch copies
warm up for 4,096 calls; vector timing includes allocation and destruction,
while caller storage is constructed before timing. Neither static workload
includes a competing writer.

Concurrent runs last 1.25 seconds by default. Both threads in paced runs target
the requested rate using absolute sleeps; expired ticks are skipped and counted.
Saturation samples every 64th write and every fourth lookup for latency.
Reservoirs retain at most 262,144 samples spread across the run. The `multi`
mode measures throughput and CPU use without per-operation latency sampling.

## CPU placement

By default, threads use the process's inherited CPU affinity. For repeatable
measurements, select available physical cores and avoid sibling hyperthreads:

```sh
BENCH_WRITER_CPU=2 BENCH_READER_CPU=8 \
BENCH_READER_CPUS=8,10,4,6,12,13,14,15 \
/tmp/stamped-buffer-bench/benchmarks/stamped_buffer_benchmark --suite \
  > /tmp/stamped-buffer-pinned.jsonl
```

These CPU numbers are an example; choose indices appropriate to your machine.
`BENCH_READER_CPU` applies to the one-reader paced/saturation cases.
`BENCH_READER_CPUS` applies to `multi` and must contain at least as many entries
as the largest requested reader count. Use `--readers 1,2` on a smaller machine.
A CPU value of `-1` restores the process's inherited affinity. Pinning does not
reserve a core; avoid other benchmark/build jobs while measuring.

## Interpretation

- `ns_per_op` is batched wall time per operation; CPU time is recorded separately.
  Per-call percentiles include clock overhead and describe each run's sampled
  operations. Averaging or taking the median of p99 values across repetitions
  does not give a pooled percentile or a latency guarantee.
- Bandwidth counts useful sample bytes processed, not measured DRAM traffic.
  Storage includes the buffer object and slots, excluding allocator bookkeeping.
- 100% CPU means one logical core. Multiple-reader CPU percentages are summed.
  CPU and throughput include sample preparation, validation and pacing overhead;
  operation latency excludes that work. Each thread uses its actual elapsed
  interval, including an operation that completes after the nominal deadline.
- Continuous writes can repeatedly invalidate a read. These workloads use the
  retrying `binsearch` API; they do not measure `try_binsearch` failure rates.
  Increasing writer throughput can reduce completed reads. `stale` counts queries
  overwritten before completion; `corrupt` detects malformed or invalid results.
- Repetition ranges describe observed variation, not statistical confidence
  intervals. Frequency scaling, competing processes and scheduling affect results.
  Hardware counters, energy and NUMA effects are not measured.

## Run individual cases

The suite's correctness checks can also run separately:

```sh
/tmp/stamped-buffer-bench/benchmarks/stamped_buffer_benchmark --verify
```

The executable takes `MODE BYTES CAPACITY OPERATION PARAMETER REPEAT SECONDS`.
Capacities must be at least 16 for these workloads. `REPEAT` labels the run.

```sh
/tmp/stamped-buffer-bench/benchmarks/stamped_buffer_benchmark micro 32 4096 lookup 0 0 .12
/tmp/stamped-buffer-bench/benchmarks/stamped_buffer_benchmark batch 32 4096 caller 32 0 .12
/tmp/stamped-buffer-bench/benchmarks/stamped_buffer_benchmark concurrent 32 4096 lookup 10000 0 1.25
/tmp/stamped-buffer-bench/benchmarks/stamped_buffer_benchmark multi 32 4096 lookup 4 0 1.25
```

For `micro`, operations are `write`, `lookup` or `latest`, and the parameter is
zero. For `batch`, operations are `vector` or `caller`, and the parameter is the
batch count. For `concurrent`, it is the target rate (zero means unpaced); for
`multi`, it is the reader count. `--clock` measures back-to-back clock overhead.
