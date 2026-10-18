// Benchmarks for the production stamped-buffer headers. Linux/GCC or Clang.
#include "stamped_buffer/stamped_ring_buffer.hpp"

#include <algorithm>
#include <array>
#include <atomic>
#include <cerrno>
#include <cmath>
#include <cstdlib>
#include <exception>
#include <iomanip>
#include <iostream>
#include <random>
#include <sstream>
#include <stdexcept>
#include <string>
#include <thread>
#include <vector>
#include <pthread.h>
#include <time.h>

using u64 = std::uint64_t;
constexpr u64 billion = 1000000000;
int writer_cpu = -1, reader_cpu = -1;
cpu_set_t initial_affinity;

u64 now(clockid_t clock = CLOCK_MONOTONIC) {
  timespec t{};
  if (clock_gettime(clock, &t)) throw std::runtime_error("clock_gettime failed");
  return u64(t.tv_sec) * billion + t.tv_nsec;
}
void pin(int cpu) {
  if (cpu < -1 || cpu >= CPU_SETSIZE) throw std::invalid_argument("Invalid CPU index");
  cpu_set_t set = initial_affinity;
  if (cpu != -1) {
    CPU_ZERO(&set);
    CPU_SET(cpu, &set);
  }
  if (pthread_setaffinity_np(pthread_self(), sizeof(set), &set))
    throw std::runtime_error("Cannot pin to CPU " + std::to_string(cpu));
}
void sleep_until(u64 deadline) {
  timespec t{static_cast<time_t>(deadline / billion), static_cast<long>(deadline % billion)};
  int error;
  do { error = clock_nanosleep(CLOCK_MONOTONIC, TIMER_ABSTIME, &t, nullptr); }
  while (error == EINTR);
  if (error) throw std::runtime_error("clock_nanosleep failed");
}
u64 random_next(u64& state) {
  state ^= state << 13;
  state ^= state >> 7;
  return state ^= state << 17;
}
template<class T> void consume(const T& value) {
  // Force the entire copy-out to materialize, including every payload word.
  asm volatile("" : : "m"(value) : "memory");
}
template<std::size_t Bytes> using Payload = std::array<u64, (Bytes - sizeof(u64)) / sizeof(u64)>;
template<std::size_t Bytes> using Value = stamped_buffer::StampedElement<Payload<Bytes>>;
template<std::size_t Bytes> using Buffer = stamped_buffer::StampedRingBuffer<Payload<Bytes>>;
template<std::size_t Bytes> Value<Bytes> sample(u64 sequence) {
  Value<Bytes> v;
  v.ts = sequence * 2;
  for (std::size_t i = 0; i < v.elem.size(); ++i)
    v.elem[i] = v.ts ^ (0x9e3779b97f4a7c15ULL * (i + 1));
  return v;
}
template<class V> bool intact(const V& v) {
  for (std::size_t i = 0; i < v.elem.size(); ++i)
    if (v.elem[i] != (v.ts ^ (0x9e3779b97f4a7c15ULL * (i + 1)))) return false;
  return !(v.ts & 1);
}

template<std::size_t Bytes> std::size_t footprint(const Buffer<Bytes>& queue) {
  return sizeof(queue) + queue.capacity() * sizeof(stamped_buffer::detail::StampedSlot<Payload<Bytes>>);
}
std::vector<int> reader_cpus(std::size_t count) {
  const auto* value = std::getenv("BENCH_READER_CPUS");
  if (!value) return std::vector<int>(count, -1);
  std::istringstream input(value);
  std::vector<int> result;
  for (std::string part; std::getline(input, part, ',');) {
    std::size_t consumed = 0;
    const int cpu = std::stoi(part, &consumed);
    if (consumed != part.size() || cpu < -1 || cpu >= CPU_SETSIZE)
      throw std::invalid_argument("Invalid reader CPU index");
    result.push_back(cpu);
  }
  if (result.size() < count) throw std::invalid_argument("Too few BENCH_READER_CPUS entries");
  return result;
}

struct Latencies {
  std::vector<u64> values;
  u64 maximum = 0, observations = 0, rng = 0xb8f716d294c50a31ULL;
  explicit Latencies(std::size_t reserve = 10000) { values.reserve(reserve); }
  void add(u64 value) {
    maximum = std::max(maximum, value);
    ++observations;
    if (values.size() < values.capacity()) values.push_back(value);
    else {
      // Reservoir sampling keeps the full run represented when the cap is hit.
      const auto index = random_next(rng) % observations;
      if (index < values.size()) values[index] = value;
    }
  }
  u64 percentile(double fraction) {
    if (values.empty()) return 0;
    std::sort(values.begin(), values.end());
    return values[static_cast<std::size_t>(fraction * (values.size() - 1))];
  }
};

template<std::size_t Bytes> void verify() {
  for (std::size_t capacity : {1, 2, 3, 64, 4096}) {
    Buffer<Bytes> queue(capacity);
    for (std::size_t i = 0; i < capacity; ++i) queue.add(sample<Bytes>(i));
    for (std::size_t step = 0; step < capacity + 3; ++step) {
      if (step) queue.add(sample<Bytes>(capacity + step - 1));
      if (step > 2 && step + 1 < capacity) continue;
      const auto stride = std::max<std::size_t>(1, capacity / 31);
      for (std::size_t index = 0; index < capacity; index += stride) {
        for (u64 gap : {0, 1}) {
          const auto target = 2 * (step + index) + gap;
          const auto value = queue.binsearch(target);
          const auto expected = std::min(target + gap, 2 * (step + capacity - 1));
          if (value.ts != expected || !intact(value))
            throw std::runtime_error("Timestamp lookup validation failed");
        }
      }
      const auto last = queue.get_latest();
      if (last.ts != 2 * (step + capacity - 1) || !intact(last))
        throw std::runtime_error("Latest sample validation failed");
      std::vector<Value<Bytes>> batch(capacity);
      if (queue.get_last_n(batch.data(), batch.size()) != capacity) std::abort();
      for (std::size_t i = 0; i < capacity; ++i)
        if (batch[i].ts != 2 * (step + i) || !intact(batch[i])) std::abort();
    }
  }
}

template<std::size_t Bytes, int Operation> void micro(std::size_t capacity, const std::string& operation,
                                   int repetition, double seconds) {
  Buffer<Bytes> q(capacity);
  // One full rotation before timing to exercise wrapped indexes.
  for (std::size_t i = 0; i < capacity; ++i) q.add(sample<Bytes>(i));
  for (std::size_t i = capacity; i < 2 * capacity; ++i) q.add(sample<Bytes>(i));
  std::array<u64, 8192> queries{};
  std::array<Value<Bytes>, 8192> inputs{};
  u64 rng = 0x123456789abcdefULL;
  for (std::size_t i = 0; i < queries.size(); ++i) {
    queries[i] = 2 * (capacity + random_next(rng) % (capacity - 1)) + (i & 1);
    inputs[i] = sample<Bytes>(2 * capacity + i);
  }
  u64 sequence = 2 * capacity;
  auto one = [&](u64 index) {
    if constexpr (Operation == 0) {
      // Reuse a prepared payload; only timestamps advance inside the timed loop.
      auto& value = inputs[index & (inputs.size() - 1)];
      value.ts = 2 * sequence++;
      q.add(value);
      consume(q);
    } else if constexpr (Operation == 1) {
      const auto out = q.binsearch(queries[index & (queries.size() - 1)]);
      consume(out);
    } else {
      auto out = q.get_latest();
      consume(out);
    }
  };
  for (u64 i = 0; i < 8192; ++i) one(i);
  u64 operations = 0;
  const auto cpu_start = now(CLOCK_THREAD_CPUTIME_ID);
  const auto start = now();
  const auto deadline = start + static_cast<u64>(seconds * billion);
  do {
    for (unsigned i = 0; i < 256; ++i) one(operations++);
  } while (now() < deadline);
  const auto elapsed = now() - start;
  const auto cpu_ns = now(CLOCK_THREAD_CPUTIME_ID) - cpu_start;
  Latencies lat;
  for (u64 i = 0; i < 10000; ++i) {
    const auto before = now();
    one(i);
    lat.add(now() - before);
  }
  std::cout << "{\"kind\":\"micro\",\"bytes\":" << Bytes << ",\"capacity\":" << capacity
            << ",\"operation\":\"" << operation << "\",\"repeat\":" << repetition
            << ",\"ops\":" << operations << ",\"wall_ns\":" << elapsed
            << ",\"cpu_ns\":" << cpu_ns << ",\"ns_per_op\":" << double(elapsed) / operations
            << ",\"ops_per_s\":" << double(operations) * billion / elapsed
            << ",\"useful_MB_per_s\":" << double(operations) * Bytes * 1000 / elapsed
            << ",\"p50_ns\":" << lat.percentile(.50) << ",\"p95_ns\":" << lat.percentile(.95)
            << ",\"p99_ns\":" << lat.percentile(.99) << ",\"max_ns\":" << lat.maximum
            << ",\"latency_samples\":" << lat.values.size()
            << ",\"buffer_bytes\":" << footprint<Bytes>(q) << "}\n" << std::flush;
}

struct alignas(128) ThreadResult {
  u64 operations = 0, stale = 0, corrupt = 0;
  u64 cpu_ns = 0, wall_ns = 0, missed_ticks = 0;
  Latencies latency{262144};
  Latencies lateness{262144};
};

template<std::size_t Bytes> void concurrent(std::size_t capacity, int rate, int repetition,
                                       double seconds, bool emit = true) {
  Buffer<Bytes> q(capacity);
  const std::size_t retained = capacity;
  for (std::size_t i = 0; i < retained; ++i) q.add(sample<Bytes>(i));
  alignas(128) std::atomic<u64> latest{retained - 1};
  alignas(128) std::atomic<int> ready{0};
  alignas(128) std::atomic<bool> go{false};
  u64 start = 0, end = 0;
  ThreadResult producer, consumer;
  std::exception_ptr writer_error, reader_error;
  auto wait_start = [&] {
    ready.fetch_add(1, std::memory_order_release);
    while (!go.load(std::memory_order_acquire)) std::this_thread::yield();
    sleep_until(start);
  };
  // No catch-up bursts: skip expired scheduled ticks and report how many.
  auto advance = [&](u64& tick, ThreadResult& result) {
    if (!rate) return;
    const u64 period = billion / rate;
    tick += period;
    const auto current = now();
    if (tick < current) {
      const auto skipped = (current - tick) / period + 1;
      result.missed_ticks += skipped;
      tick += skipped * period;
    }
    if (tick < end) sleep_until(tick);
  };
  std::thread writer([&] {
    try {
      pin(writer_cpu);
      wait_start();
      const auto wall_start = now(), cpu_start = now(CLOCK_THREAD_CPUTIME_ID);
      u64 next = retained, tick = start;
      while (rate ? (tick < end && now() < end)
                  : ((producer.operations & 63) || now() < end)) {
        const auto value = sample<Bytes>(next);
        const bool timed = rate || !(producer.operations & 63);
        const auto before = timed ? now() : 0;
        q.add(value);
        if (timed) {
          const auto elapsed = now() - before;
          producer.latency.add(elapsed);
        }
        if (rate) producer.lateness.add(before > tick ? before - tick : 0);
        latest.store(next++, std::memory_order_release);
        ++producer.operations;
        advance(tick, producer);
      }
      producer.cpu_ns = now(CLOCK_THREAD_CPUTIME_ID) - cpu_start;
      producer.wall_ns = now() - wall_start;
    } catch (...) { writer_error = std::current_exception(); ready.store(2); }
  });
  std::thread reader([&] {
    try {
      pin(reader_cpu);
      wait_start();
      const auto wall_start = now(), cpu_start = now(CLOCK_THREAD_CPUTIME_ID);
      u64 rng = 0x842341235abcdefULL, tick = start;
      while (rate ? (tick < end && now() < end) : ((consumer.operations & 63) || now() < end)) {
        const auto last = latest.load(std::memory_order_acquire);
        const auto depth = 2 + random_next(rng) % (retained / 2);
        const auto target = (last - depth) * 2 + (consumer.operations & 1);
        const bool timed = rate || !(consumer.operations & 3);
        const auto before = timed ? now() : 0;
        const auto out = q.binsearch(target);
        if (timed) consumer.latency.add(now() - before);
        if (rate) consumer.lateness.add(before > tick ? before - tick : 0);
        ++consumer.operations;
        consume(out);
        if (!intact(out) || out.ts < target) ++consumer.corrupt;
        if (out.ts != target + (target & 1)) ++consumer.stale;
        advance(tick, consumer);
      }
      consumer.cpu_ns = now(CLOCK_THREAD_CPUTIME_ID) - cpu_start;
      consumer.wall_ns = now() - wall_start;
    } catch (...) { reader_error = std::current_exception(); ready.store(2); }
  });
  while (ready.load(std::memory_order_acquire) < 2) std::this_thread::yield();
  start = now() + 10000000;
  end = start + static_cast<u64>(seconds * billion);
  go.store(true, std::memory_order_release);
  writer.join();
  reader.join();
  if (writer_error) std::rethrow_exception(writer_error);
  if (reader_error) std::rethrow_exception(reader_error);
  if (consumer.corrupt) throw std::runtime_error("Concurrent sample corruption detected");
  if (!emit) return;
  auto output_thread = [](const char* name, ThreadResult& result) {
    std::cout << ",\"" << name << "_ops\":" << result.operations
              << ",\"" << name << "_wall_ns\":" << result.wall_ns
              << ",\"" << name << "_cpu_ns\":" << result.cpu_ns
              << ",\"" << name << "_cpu_pct\":" << 100.0 * result.cpu_ns / result.wall_ns
              << ",\"" << name << "_ops_per_s\":" << double(result.operations) * billion / result.wall_ns
              << ",\"" << name << "_p50_ns\":" << result.latency.percentile(.5)
              << ",\"" << name << "_p99_ns\":" << result.latency.percentile(.99)
              << ",\"" << name << "_max_ns\":" << result.latency.maximum
              << ",\"" << name << "_latency_samples\":" << result.latency.values.size()
              << ",\"" << name << "_latency_observations\":" << result.latency.observations
              << ",\"" << name << "_missed_ticks\":" << result.missed_ticks
              << ",\"" << name << "_lateness_p99_ns\":" << result.lateness.percentile(.99);
  };
  std::cout << "{\"kind\":\"concurrent\",\"bytes\":" << Bytes
            << ",\"capacity\":" << capacity << ",\"rate\":" << rate << ",\"repeat\":" << repetition;
  output_thread("writer", producer);
  output_thread("reader", consumer);
  std::cout << ",\"stale\":" << consumer.stale << ",\"corrupt\":" << consumer.corrupt
            << ",\"useful_write_MB_per_s\":" << double(producer.operations) * Bytes * 1000 / producer.wall_ns
            << ",\"buffer_bytes\":" << footprint<Bytes>(q) << "}\n" << std::flush;
}

template<std::size_t Bytes, bool Caller>
void batch(std::size_t capacity, std::size_t count, double seconds, int repetition) {
  pin(writer_cpu);
  Buffer<Bytes> queue(capacity);
  for (u64 i = 0; i < 2 * capacity; ++i) queue.add(sample<Bytes>(i));
  std::vector<Value<Bytes>> output(count);
  auto one = [&] {
    if constexpr (Caller) {
      if (queue.get_last_n(output.data(), count) != count) std::abort();
      for (const auto& value : output) consume(value);
    } else {
      auto values = queue.get_last_n(count);
      for (const auto& value : values) consume(value);
    }
  };
  auto check = queue.get_last_n(count);
  for (std::size_t i = 0; i < count; ++i)
    if (!intact(check[i]) || check[i].ts != 2 * (2 * capacity - count + i)) std::abort();
  for (unsigned i = 0; i < 4096; ++i) one();
  u64 operations = 0;
  const auto cpu_start = now(CLOCK_THREAD_CPUTIME_ID), start = now();
  const auto end = start + u64(seconds * billion);
  do {
    for (unsigned i = 0; i < 64; ++i) { one(); ++operations; }
  } while (now() < end);
  const auto elapsed = now() - start, cpu = now(CLOCK_THREAD_CPUTIME_ID) - cpu_start;
  std::cout << "{\"kind\":\"batch\",\"operation\":\"" << (Caller ? "caller" : "vector")
            << "\",\"bytes\":" << Bytes << ",\"capacity\":" << capacity
            << ",\"count\":" << count << ",\"repeat\":" << repetition
            << ",\"ns_per_op\":" << double(elapsed) / operations
            << ",\"cpu_ns_per_op\":" << double(cpu) / operations
            << ",\"cpu_ns\":" << cpu
            << ",\"ops_per_s\":" << double(operations) * billion / elapsed
            << ",\"useful_MB_per_s\":" << double(operations) * count * Bytes * 1000 / elapsed
            << ",\"ops\":" << operations << ",\"wall_ns\":" << elapsed << "}\n";
}

struct alignas(128) Result {
  u64 operations = 0, corrupt = 0, stale = 0, cpu_ns = 0, wall_ns = 0;
};
template<std::size_t Bytes>
void multi(std::size_t capacity, std::size_t readers, double seconds, int repetition, bool emit = true) {
  const auto cpus = reader_cpus(readers);
  if (readers > cpus.size()) throw std::invalid_argument("too few reader CPUs");
  Buffer<Bytes> queue(capacity);
  for (u64 i = 0; i < capacity; ++i) queue.add(sample<Bytes>(i));
  alignas(128) std::atomic<u64> latest{capacity - 1};
  alignas(128) std::atomic<unsigned> ready{0};
  alignas(128) std::atomic<bool> go{false};
  u64 deadline = 0;
  Result producer;
  std::vector<Result> results(readers);
  std::vector<std::exception_ptr> errors(readers + 1);
  std::vector<std::thread> threads;
  threads.emplace_back([&] {
    try {
      pin(writer_cpu);
      ready.fetch_add(1);
      while (!go.load(std::memory_order_acquire)) std::this_thread::yield();
      const auto cpu_start = now(CLOCK_THREAD_CPUTIME_ID), start = now();
      u64 sequence = capacity;
      do {
        for (unsigned i = 0; i < 256; ++i) {
          queue.add(sample<Bytes>(sequence));
          latest.store(sequence++, std::memory_order_release);
        }
      } while (now() < deadline);
      producer.operations = sequence - capacity;
      producer.wall_ns = now() - start;
      producer.cpu_ns = now(CLOCK_THREAD_CPUTIME_ID) - cpu_start;
    } catch (...) { errors[0] = std::current_exception(); ready.fetch_add(1); }
  });
  for (std::size_t reader = 0; reader < readers; ++reader) threads.emplace_back([&, reader] {
    try {
      pin(cpus[reader]);
      auto& result = results[reader];
      u64 rng = 0xb8f716d294c50a31ULL + reader;
      ready.fetch_add(1);
      while (!go.load(std::memory_order_acquire)) std::this_thread::yield();
      const auto cpu_start = now(CLOCK_THREAD_CPUTIME_ID), start = now();
      do {
        const auto target = 2 * (latest.load(std::memory_order_acquire) - random_next(rng) % (capacity / 2));
        const auto value = queue.binsearch(target);
        consume(value);
        result.corrupt += !intact(value) || value.ts < target;
        result.stale += value.ts != target; // A query may be overwritten while the reader retries.
        ++result.operations;
      } while (now() < deadline);
      result.wall_ns = now() - start;
      result.cpu_ns = now(CLOCK_THREAD_CPUTIME_ID) - cpu_start;
    } catch (...) { errors[reader + 1] = std::current_exception(); ready.fetch_add(1); }
  });
  while (ready.load() < readers + 1) std::this_thread::yield();
  deadline = now() + u64(seconds * billion);
  go.store(true, std::memory_order_release);
  for (auto& thread : threads) thread.join();
  for (const auto& error : errors) if (error) std::rethrow_exception(error);
  double reader_rate = 0, reader_cpu = 0;
  u64 corrupt = 0, stale = 0, operations = 0, cpu_ns = 0;
  for (const auto& result : results) {
    reader_rate += double(result.operations) * billion / result.wall_ns;
    reader_cpu += double(result.cpu_ns) * 100 / result.wall_ns;
    operations += result.operations;
    cpu_ns += result.cpu_ns;
    corrupt += result.corrupt;
    stale += result.stale;
  }
  if (corrupt) throw std::runtime_error("Concurrent sample corruption detected");
  if (!emit) return;
  std::cout << "{\"kind\":\"multi\",\"bytes\":" << Bytes << ",\"capacity\":" << capacity
            << ",\"readers\":" << readers << ",\"repeat\":" << repetition
            << ",\"writer_ops_per_s\":" << double(producer.operations) * billion / producer.wall_ns
            << ",\"reader_ops_per_s\":" << reader_rate
            << ",\"writer_cpu_pct\":" << double(producer.cpu_ns) * 100 / producer.wall_ns
            << ",\"reader_cpu_pct\":" << reader_cpu
            << ",\"reader_ops_per_cpu_s\":" << double(operations) * billion / cpu_ns
            << ",\"reader_useful_MB_per_s\":" << reader_rate * Bytes / 1e6
            << ",\"corrupt\":" << corrupt << ",\"stale\":" << stale << "}\n";
}

template<std::size_t Bytes>
void dispatch(const std::string& mode, const std::string& operation, std::size_t capacity,
              std::size_t parameter, int repetition, double seconds) {
  if (mode == "micro") {
    if (parameter != 0) throw std::invalid_argument("Micro parameter must be zero");
    if (operation == "write") micro<Bytes, 0>(capacity, operation, repetition, seconds);
    else if (operation == "lookup") micro<Bytes, 1>(capacity, operation, repetition, seconds);
    else if (operation == "latest") micro<Bytes, 2>(capacity, operation, repetition, seconds);
    else throw std::invalid_argument("Unknown isolated operation");
  } else if (mode == "batch") {
    if (!parameter || parameter > capacity) throw std::invalid_argument("Invalid batch count");
    if (operation == "vector") batch<Bytes, false>(capacity, parameter, seconds, repetition);
    else if (operation == "caller") batch<Bytes, true>(capacity, parameter, seconds, repetition);
    else throw std::invalid_argument("Unknown batch operation");
  } else if (operation == "lookup" && mode == "concurrent") {
    if (parameter > 1000000000) throw std::invalid_argument("Rate exceeds clock resolution");
    concurrent<Bytes>(capacity, static_cast<int>(parameter), repetition, seconds);
  } else if (operation == "lookup" && mode == "multi") {
    if (!parameter || parameter > 64) throw std::invalid_argument("Reader count must be 1..64");
    multi<Bytes>(capacity, parameter, seconds, repetition);
  } else throw std::invalid_argument("Unknown benchmark mode/operation");
}

std::size_t parse_size(const char* input) {
  const std::string text(input);
  if (text.empty() || text.find_first_not_of("0123456789") != std::string::npos)
    throw std::invalid_argument("Expected a nonnegative integer");
  return std::stoul(text);
}

double parse_seconds(const char* input) {
  std::size_t consumed = 0;
  const auto seconds = std::stod(input, &consumed);
  if (consumed != std::string(input).size() || !std::isfinite(seconds) || seconds <= 0 || seconds > 3600)
    throw std::invalid_argument("Duration must be finite and in (0, 3600] seconds");
  return seconds;
}

void verify_all() {
  verify<16>(); verify<32>(); verify<64>(); verify<128>();
  for (int rate : {10000, 0}) {
    concurrent<32>(64, rate, 0, .05, false);
    concurrent<64>(4096, rate, 0, .05, false);
  }
  multi<32>(64, 1, .05, 0, false);
}

struct BenchmarkCase {
  std::string mode, operation;
  std::size_t bytes, capacity, parameter;

  void run(int repetition, double seconds) const {
    switch (bytes) {
      case 16: dispatch<16>(mode, operation, capacity, parameter, repetition, seconds); break;
      case 32: dispatch<32>(mode, operation, capacity, parameter, repetition, seconds); break;
      case 64: dispatch<64>(mode, operation, capacity, parameter, repetition, seconds); break;
      case 128: dispatch<128>(mode, operation, capacity, parameter, repetition, seconds); break;
      default: throw std::invalid_argument("Sample size must be 16, 32, 64 or 128 bytes");
    }
  }
};

void usage() {
  std::cout <<
      "Usage: stamped_buffer_benchmark --suite [OPTIONS]\n"
      "       stamped_buffer_benchmark --quick [OPTIONS]\n"
      "       stamped_buffer_benchmark MODE BYTES CAPACITY OPERATION PARAMETER REPEAT SECONDS\n"
      "       stamped_buffer_benchmark --verify | --clock | --help\n\n"
      "Suite options (explicit values override quick defaults):\n"
      "  --quick                  Short smoke run: one repetition, 0.01/0.05 seconds\n"
      "  --micro-repeats N         Isolated operation repetitions (default 5)\n"
      "  --batch-repeats N         Batch copy repetitions (default 3)\n"
      "  --concurrent-repeats N    Concurrent and multi-reader repetitions (default 3)\n"
      "  --micro-seconds S         Isolated and batch duration (default 0.12)\n"
      "  --concurrent-seconds S    Concurrent and multi-reader duration (default 1.25)\n"
      "  --readers N,N,...         Multi-reader counts, each 1..64 (default 1,2,4,8)\n\n"
      "Suite runs verify correctness first, then emit one JSON line per case to stdout.\n"
      "Progress goes to stderr. Redirect stdout to a file to save measurements.\n"
      "Modes: micro (write/lookup/latest, parameter 0), batch (vector/caller, count),\n"
      "       concurrent (lookup, rate in Hz or 0), multi (lookup, reader count).\n"
      "Record sizes: 16/32/64/128 bytes; capacity >= 16; repeat labels start at 0.\n";
}

void run_suite(int argc, char** argv) {
  bool quick = false;
  for (int i = 1; i < argc; ++i) {
    if (std::string(argv[i]) == "--help") { usage(); return; }
    if (std::string(argv[i]) == "--quick") quick = true;
  }
  std::size_t micro_repeats = quick ? 1 : 5, batch_repeats = quick ? 1 : 3;
  std::size_t concurrent_repeats = quick ? 1 : 3;
  double micro_seconds = quick ? .01 : .12, concurrent_seconds = quick ? .05 : 1.25;
  std::vector<std::size_t> readers{1, 2, 4, 8};
  for (int i = 2; i < argc; ++i) {
    const std::string option = argv[i];
    if (option == "--quick") continue;
    if (option != "--micro-repeats" && option != "--batch-repeats" &&
        option != "--concurrent-repeats" && option != "--micro-seconds" &&
        option != "--concurrent-seconds" && option != "--readers")
      throw std::invalid_argument("Unknown suite option: " + option);
    if (++i == argc) throw std::invalid_argument("Missing value for " + option);
    if (option == "--micro-seconds") micro_seconds = parse_seconds(argv[i]);
    else if (option == "--concurrent-seconds") concurrent_seconds = parse_seconds(argv[i]);
    else if (option == "--readers") {
      const std::string value = argv[i];
      if (value.empty() || value.back() == ',') throw std::invalid_argument("Invalid reader counts");
      std::istringstream input(value);
      readers.clear();
      for (std::string part; std::getline(input, part, ',');) {
        const auto count = parse_size(part.c_str());
        if (!count || count > 64) throw std::invalid_argument("Reader count must be 1..64");
        readers.push_back(count);
      }
    } else {
      const auto count = parse_size(argv[i]);
      if (!count || count > 1000000) throw std::invalid_argument("Repetitions must be 1..1000000");
      if (option == "--micro-repeats") micro_repeats = count;
      else if (option == "--batch-repeats") batch_repeats = count;
      else concurrent_repeats = count;
    }
  }
  // Check the requested affinity list before starting any measured work.
  reader_cpus(*std::max_element(readers.begin(), readers.end()));
  std::vector<BenchmarkCase> isolated, batches, concurrent_cases, multi_reader;
  const std::vector<std::size_t> capacities = quick ? std::vector<std::size_t>{64, 65}
      : std::vector<std::size_t>{64, 4096, 5000, 65536};
  const std::vector<std::size_t> counts = quick ? std::vector<std::size_t>{8, 32}
      : std::vector<std::size_t>{8, 32, 128, 512};
  for (std::size_t bytes : {16, 32, 64, 128})
    for (auto capacity : capacities)
      for (const auto* operation : {"write", "lookup", "latest"})
        isolated.push_back({"micro", operation, bytes, capacity, 0});
  for (std::size_t bytes : {32, 64}) {
    for (auto count : counts)
      for (const auto* operation : {"vector", "caller"})
        batches.push_back({"batch", operation, bytes, 4096, count});
    for (std::size_t rate : {0, 1000, 10000})
      concurrent_cases.push_back({"concurrent", "lookup", bytes, 4096, rate});
    for (auto count : readers) multi_reader.push_back({"multi", "lookup", bytes, 4096, count});
  }
  const auto total = isolated.size() * micro_repeats + batches.size() * batch_repeats +
      (concurrent_cases.size() + multi_reader.size()) * concurrent_repeats;
  std::cerr << "Verifying benchmark correctness...\n";
  verify_all();
  std::cerr << "Correctness checks passed; running " << total << " cases\n";
  std::size_t completed = 0;
  std::mt19937 random(0x5a17);
  auto run_group = [&](std::vector<BenchmarkCase>& cases, std::size_t repeats, double seconds) {
    for (std::size_t repetition = 0; repetition < repeats; ++repetition) {
      std::shuffle(cases.begin(), cases.end(), random);
      for (const auto& test : cases) {
        std::cerr << '[' << completed + 1 << '/' << total << "] " << test.mode << ' '
                  << test.bytes << "B capacity=" << test.capacity << ' ' << test.operation
                  << " parameter=" << test.parameter << " repeat=" << repetition << '\n';
        test.run(static_cast<int>(repetition), seconds);
        std::cout.flush();
        ++completed;
      }
    }
  };
  run_group(isolated, micro_repeats, micro_seconds);
  run_group(batches, batch_repeats, micro_seconds);
  run_group(concurrent_cases, concurrent_repeats, concurrent_seconds);
  run_group(multi_reader, concurrent_repeats, concurrent_seconds);
  std::cerr << "Completed " << completed << " benchmark cases\n";
}

int main(int argc, char** argv) {
  try {
    std::cerr.tie(nullptr);
    std::cout.exceptions(std::ios::badbit | std::ios::failbit);
    std::cout << std::setprecision(10);
    if (argc == 2 && std::string(argv[1]) == "--help") { usage(); return 0; }
    if (pthread_getaffinity_np(pthread_self(), sizeof(initial_affinity), &initial_affinity))
      throw std::runtime_error("Cannot read initial CPU affinity");
    if (const auto* cpu = std::getenv("BENCH_WRITER_CPU")) writer_cpu = std::stoi(cpu);
    if (const auto* cpu = std::getenv("BENCH_READER_CPU")) reader_cpu = std::stoi(cpu);
    pin(writer_cpu);
    if (argc == 2 && std::string(argv[1]) == "--verify") {
      verify_all();
      std::cout << "Benchmark correctness checks passed\n";
      return 0;
    }
    if (argc == 2 && std::string(argv[1]) == "--clock") {
      Latencies lat(100000);
      for (int i = 0; i < 100000; ++i) { const auto a = now(); lat.add(now() - a); }
      std::cout << "{\"p50_ns\":" << lat.percentile(.5) << ",\"p99_ns\":" << lat.percentile(.99) << "}\n";
      return 0;
    }
    if (argc >= 2 && (std::string(argv[1]) == "--suite" || std::string(argv[1]) == "--quick")) {
      run_suite(argc, argv);
      return 0;
    }
    if (argc != 8) throw std::invalid_argument(
        "Usage: stamped_buffer_benchmark --help");
    const std::string mode = argv[1], operation = argv[4];
    const auto bytes = parse_size(argv[2]), capacity = parse_size(argv[3]), parameter = parse_size(argv[5]);
    const auto repetition = parse_size(argv[6]);
    const auto seconds = parse_seconds(argv[7]);
    if (capacity < 16 || repetition > 1000000) throw std::invalid_argument("Invalid benchmark parameters");
    BenchmarkCase{mode, operation, bytes, capacity, parameter}.run(static_cast<int>(repetition), seconds);
    std::cout.flush();
  } catch (const std::exception& error) { std::cerr << error.what() << '\n'; return 1; }
}
