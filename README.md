# stamped-buffer

`stamped-buffer` is a small, header-only C++17 library providing a timestamped,
fixed-capacity ring buffer for a single producer and multiple concurrent
consumers.

## Requirements

- A C++17 compiler
- Lock-free 64-bit counters and 8-byte representation atomics (checked at compile time)
- CMake 3.14 or newer (for building tests or installing the package)

## Usage

```cpp
#include <stamped_buffer/stamped_ring_buffer.hpp>

stamped_buffer::StampedRingBuffer<int> samples(128);
samples.add({100, 42});

auto latest = samples.get_latest();
```

Timestamps must be added in non-decreasing order. Calling `add` is restricted
to one producer thread; all read methods may be called by multiple consumer
threads. Each consumer owns its output storage. Construction and destruction
must not overlap buffer operations.

Payloads must be trivially copyable, trivially copy assignable, and default
constructible. Each successful read returns a consistent snapshot. `add` performs a bounded
number of lock-free atomic operations proportional to the element size, without
allocations, locks, or waiting for consumers. Readers retry when a write overlaps
their snapshot, so continuous writes can delay readers. `try_` methods instead
make one validation attempt and report contention to the caller.

Payload representations use word-sized atomic copies. Binary search probes
only atomic timestamps, then copies the selected payload. Slots are aligned to
64 bytes; power-of-two capacities use masked indexing, while arbitrary positive
capacities keep their exact requested size. Sequentially consistent slot and
generation operations validate the complete snapshot. The 64-bit publication
counter must not wrap during the buffer's lifetime.

`binsearch` returns the first retained element whose timestamp is greater than
or equal to the requested timestamp, or the newest retained element when the
requested timestamp is newer than the available range.

### Batch copies and bounded read attempts

`get_last_n(n)` returns a vector of up to `n` retained samples, oldest first.
It reuses allocated storage across retries. For repeated polling, use an array
or a pre-sized vector to avoid allocating output storage on every call:

```cpp
#include <array>

using Buffer = stamped_buffer::StampedRingBuffer<int>;
std::array<Buffer::value_type, 32> batch{};
auto copied = samples.get_last_n(batch.data(), batch.size());

Buffer::value_type match{};
auto status = samples.try_binsearch(100, match);
if (status == Buffer::ReadStatus::success) {
  // match contains a validated sample.
}

status = samples.try_get_last_n(batch.data(), batch.size(), copied);
if (status == Buffer::ReadStatus::success) {
  // Only batch[0..copied) belongs to this validated snapshot.
}
```

| Method | On an empty buffer | On an overlapping write |
| --- | --- | --- |
| `get_latest()`, `binsearch(ts)` | Throws `std::out_of_range` | Retries |
| `get_last_n(n)`, `get_last_n(output, n)` | Empty vector / zero count | Retries |
| `try_get_latest(output)`, `try_binsearch(ts, output)` | `ReadStatus::empty` | `ReadStatus::busy` |
| `try_get_last_n(output, n, copied)` | `ReadStatus::empty`, `copied = 0` | `ReadStatus::busy`, `copied = 0` |

Single-sample `try_` methods leave output unchanged on failure. A failed batch
attempt may modify output; discard it unless the status is `success`. Requesting
zero samples succeeds with zero copied, including on an empty buffer. A null
output pointer is accepted only for a zero-sized request; otherwise it throws
`std::invalid_argument`. Non-null output must point to at least `n` constructed
values, and unused elements remain unchanged on success.

Both outer validation and individual slot reads are attempted once by `try_`
methods. They bound the library's work, not scheduler delays or arbitrary
payload default constructors; those constructors may also throw. Existing
retrying methods retain their original behavior.

## Build and test

```sh
cmake -S . -B build -DCMAKE_BUILD_TYPE=Release
cmake --build build
ctest --test-dir build --output-on-failure
```

## Use with CMake

After installing the project:

```cmake
find_package(stamped-buffer CONFIG REQUIRED)
target_link_libraries(your_target PRIVATE stamped_buffer::stamped_buffer)
```

It can also be embedded with `add_subdirectory`; set
`STAMPED_BUFFER_BUILD_TESTS=OFF` when the parent project should not build this
library's tests.

## Benchmarks

The optional [benchmark suite](benchmarks/README.md) measures the production
headers: write, timestamp-lookup and latest-sample latency, batch copying,
throughput, CPU time, storage overhead and concurrent-reader behavior.
It requires Linux and GCC or Clang and is disabled by default.

```sh
cmake -S . -B /tmp/stamped-buffer-bench -DCMAKE_BUILD_TYPE=Release \
  -DSTAMPED_BUFFER_BUILD_TESTS=OFF -DSTAMPED_BUFFER_BUILD_BENCHMARKS=ON
cmake --build /tmp/stamped-buffer-bench --target stamped_buffer_benchmark
/tmp/stamped-buffer-bench/benchmarks/stamped_buffer_benchmark --suite > /tmp/stamped-buffer-results.jsonl
```

The C++ executable verifies correctness and runs the suite directly, emitting
one JSON line per case. Use `--quick` instead of `--suite` for a smoke check of
every workload, or `--help` for repetition, duration and reader-count options.

## License

MIT. See [LICENSE](LICENSE) and [THIRD_PARTY_NOTICES.md](THIRD_PARTY_NOTICES.md).
