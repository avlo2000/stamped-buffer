// SPDX-License-Identifier: MIT

#include <gtest/gtest.h>

#include <algorithm>
#include <array>
#include <atomic>
#include <chrono>
#include <cmath>
#include <cstdint>
#include <stamped_buffer/stamped_ring_buffer.hpp>
#include <random>
#include <thread>
#include <vector>

using stamped_buffer::StampedRingBuffer;

namespace {

struct Payload {
  std::uint64_t sequence{};
  std::uint64_t complement{};
  std::array<std::uint64_t, 4> repeated{};
  bool even{};
};

Payload make_payload(std::uint64_t sequence) {
  return {sequence, ~sequence, {sequence, sequence, sequence, sequence}, sequence % 2 == 0};
}

bool valid_payload(const stamped_buffer::StampedElement<Payload>& element) {
  const auto& payload = element.elem;
  return payload.sequence == element.ts && payload.complement == ~element.ts &&
         payload.even == (element.ts % 2 == 0) &&
         std::all_of(payload.repeated.begin(), payload.repeated.end(),
                     [&](std::uint64_t value) { return value == element.ts; });
}

class ConcurrentSnapshotsTest : public testing::TestWithParam<std::size_t> {};

TEST_P(ConcurrentSnapshotsTest, FullBufferReadsStayConsistentDuringOverwrite) {
  const auto capacity = GetParam();
  constexpr std::uint64_t num_elements = 20000;
  constexpr int num_consumers = 4;
  StampedRingBuffer<Payload> buffer(capacity);
  std::atomic<int> ready{0};
  std::atomic<bool> start{false};
  std::atomic<bool> producer_done{false};
  std::atomic<std::uint64_t> errors{0};

  std::thread producer([&] {
    for (std::uint64_t i = 0; i < capacity; ++i) {
      buffer.add({i, make_payload(i)});
    }
    ready.fetch_add(1);
    while (!start.load()) std::this_thread::yield();
    for (std::uint64_t i = capacity; i < num_elements; ++i) {
      buffer.add({i, make_payload(i)});
      if (i % 16 == 0) std::this_thread::yield();
    }
    producer_done.store(true);
  });

  std::vector<std::thread> consumers;
  for (int c = 0; c < num_consumers; ++c) {
    consumers.emplace_back([&] {
      ready.fetch_add(1);
      while (!start.load()) std::this_thread::yield();
      std::uint64_t last_seen = 0;
      std::size_t reads = 0;
      do {
        const auto latest = buffer.get_latest();
        if (!valid_payload(latest) || latest.ts < last_seen) errors.fetch_add(1);
        last_seen = latest.ts;

        // Request the entire retained window, including the slot being overwritten.
        const auto history = buffer.get_last_n(capacity + 1);
        if (history.size() != capacity) errors.fetch_add(1);
        for (std::size_t i = 0; i < history.size(); ++i) {
          if (!valid_payload(history[i]) ||
              (i > 0 && history[i].ts != history[i - 1].ts + 1)) {
            errors.fetch_add(1);
          }
        }

        // With consecutive timestamps, the oldest retained timestamp must lie
        // between the oldest timestamps before and after this search.
        const auto before = buffer.get_latest().ts;
        const auto oldest = buffer.binsearch(0);
        const auto after = buffer.get_latest().ts;
        if (!valid_payload(oldest) || oldest.ts < before - (capacity - 1) ||
            oldest.ts > after - (capacity - 1)) {
          errors.fetch_add(1);
        }
        ++reads;
      } while (!producer_done.load() || reads < 100);
    });
  }

  while (ready.load() != num_consumers + 1) std::this_thread::yield();
  start.store(true);
  producer.join();
  for (auto& consumer : consumers) consumer.join();
  EXPECT_EQ(errors.load(), std::uint64_t{0});
}

INSTANTIATE_TEST_SUITE_P(SmallCapacities, ConcurrentSnapshotsTest, testing::Values(1, 2, 3));

struct SlowReaderPayload {
  static thread_local bool pause_next_construction;
  static std::atomic<bool> reader_paused;
  static std::atomic<bool> release_reader;
  std::uint64_t value{};

  SlowReaderPayload() {
    if (pause_next_construction) {
      pause_next_construction = false;
      reader_paused.store(true);
      while (!release_reader.load()) std::this_thread::yield();
    }
  }
};

thread_local bool SlowReaderPayload::pause_next_construction = false;
std::atomic<bool> SlowReaderPayload::reader_paused{false};
std::atomic<bool> SlowReaderPayload::release_reader{false};

TEST(StampedRingBufferTest, ProducerDoesNotWaitForPausedReader) {
  StampedRingBuffer<SlowReaderPayload> buffer(2);
  SlowReaderPayload::reader_paused.store(false);
  SlowReaderPayload::release_reader.store(false);
  std::atomic<bool> initialized{false};
  std::atomic<bool> producer_done{false};
  stamped_buffer::StampedElement<SlowReaderPayload> result;

  std::thread producer([&] {
    SlowReaderPayload payload;
    buffer.add({0, payload});
    initialized.store(true);
    while (!SlowReaderPayload::reader_paused.load() &&
           !SlowReaderPayload::release_reader.load()) {
      std::this_thread::yield();
    }
    for (std::uint64_t i = 1; i <= 100; ++i) {
      payload.value = i;
      buffer.add({i, payload});
    }
    producer_done.store(true);
  });

  std::thread consumer([&] {
    while (!initialized.load()) std::this_thread::yield();
    SlowReaderPayload::pause_next_construction = true;
    result = buffer.get_latest();
  });

  const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(5);
  while (!producer_done.load() && std::chrono::steady_clock::now() < deadline) {
    std::this_thread::yield();
  }
  const bool completed_while_reader_paused = producer_done.load();
  SlowReaderPayload::release_reader.store(true);
  producer.join();
  consumer.join();
  EXPECT_TRUE(completed_while_reader_paused);
  EXPECT_EQ(result.ts, std::uint64_t{100});
  EXPECT_EQ(result.elem.value, std::uint64_t{100});
}

}  // namespace

TEST(StampedRingBufferTest, CallerBuffersAndTryReadsStayConsistentWithFourReaders) {
  using Buffer = StampedRingBuffer<Payload>;
  using Status = Buffer::ReadStatus;
  std::atomic<std::uint64_t> errors{0};
  for (std::size_t capacity : {2, 3, 64, 65}) {
    Buffer buffer(capacity);
    for (std::uint64_t i = 0; i < capacity; ++i) buffer.add({i, make_payload(i)});
    std::atomic<int> ready{0};
    std::atomic<bool> start{false}, done{false};
    std::thread writer([&] {
      ready.fetch_add(1);
      while (!start.load()) std::this_thread::yield();
      for (std::uint64_t i = capacity; i < capacity + 30000; ++i) {
        buffer.add({i, make_payload(i)});
        if (i % 16 == 0) std::this_thread::yield();
      }
      done.store(true);
    });
    std::vector<std::thread> readers;
    for (int reader = 0; reader < 4; ++reader) readers.emplace_back([&] {
      std::vector<Buffer::value_type> output(capacity + 1);
      auto check_batch = [&](std::size_t count) {
        if (count != capacity) errors.fetch_add(1);
        for (std::size_t i = 0; i < count; ++i) {
          if (!valid_payload(output[i]) || (i && output[i].ts != output[i - 1].ts + 1)) errors.fetch_add(1);
        }
      };
      ready.fetch_add(1);
      while (!start.load()) std::this_thread::yield();
      unsigned attempts = 0;
      do {
        check_batch(buffer.get_last_n(output.data(), output.size()));
        std::size_t count = 999;
        const auto status = buffer.try_get_last_n(output.data(), output.size(), count);
        if (status == Status::success) check_batch(count);
        else if (status != Status::busy || count != 0) errors.fetch_add(1);

        constexpr std::uint64_t sentinel = 999999;
        Buffer::value_type value{sentinel, make_payload(sentinel)};
        const auto latest_status = buffer.try_get_latest(value);
        if (latest_status == Status::success) {
          if (!valid_payload(value) || value.ts == sentinel) errors.fetch_add(1);
        } else if (latest_status != Status::busy || value.ts != sentinel || !valid_payload(value)) {
          errors.fetch_add(1);
        }
        value = {sentinel, make_payload(sentinel)};
        const auto search_status = buffer.try_binsearch(0, value);
        if (search_status == Status::success) {
          if (!valid_payload(value) || value.ts == sentinel) errors.fetch_add(1);
        } else if (search_status != Status::busy || value.ts != sentinel || !valid_payload(value)) {
          errors.fetch_add(1);
        }
        ++attempts;
      } while (!done.load() || attempts < 100);
    });
    while (ready.load() != 5) std::this_thread::yield();
    start.store(true);
    writer.join();
    for (auto& reader : readers) reader.join();
  }
  EXPECT_EQ(errors.load(), 0U);
}

TEST(StampedRingBufferTest, MultithreadedGetLastN) {
  const int capacity = 4096;
  const int num_elements = 50000;
  const int num_consumers = 4;
  const int n_last = 10;

  StampedRingBuffer<int> buffer(capacity);
  std::atomic<bool> producer_done{false};

  std::thread producer([&]() {
    for (int i = 0; i < num_elements; ++i) {
      buffer.add({(uint64_t)i, i});
      if (i % 5 == 0) std::this_thread::yield();
    }
    producer_done = true;
  });

  std::vector<std::thread> consumers;
  std::atomic<int> errors{0};

  for (int c = 0; c < num_consumers; ++c) {
    consumers.emplace_back([&]() {
      while (!producer_done) {
        auto result = buffer.get_last_n(n_last);

        if (result.empty()) {
          continue;
        }

        if (result.size() > n_last) {
          errors++;
        }

        for (size_t i = 0; i < result.size(); ++i) {
          if (result[i].ts != (uint64_t)result[i].elem) {
            errors++;
          }

          if (i > 0) {
            if (result[i].ts <= result[i - 1].ts) {
              errors++;
            }
            if (result[i].ts != result[i - 1].ts + 1) {
              errors++;
            }
          }
        }
      }
    });
  }

  producer.join();
  for (auto& t : consumers) t.join();

  EXPECT_EQ(errors.load(), 0);
}

TEST(StampedRingBufferTest, MultithreadedBinSearch) {
  const int capacity = 5000;
  const int num_elements = 50000;
  const int num_consumers = 4;

  StampedRingBuffer<int> buffer(capacity);
  std::atomic<bool> producer_done{false};
  std::atomic<uint64_t> max_ts{0};

  std::thread producer([&]() {
    for (int i = 0; i < num_elements; ++i) {
      buffer.add({(uint64_t)i, i});
      max_ts.store(i, std::memory_order_release);
      if (i % 5 == 0) std::this_thread::yield();
    }
    producer_done = true;
  });

  std::vector<std::thread> consumers;
  std::atomic<int> errors{0};

  for (int c = 0; c < num_consumers; ++c) {
    consumers.emplace_back([&]() {
      std::random_device rd;
      std::mt19937 gen(rd());

      while (!producer_done) {
        const uint64_t current_max = max_ts.load(std::memory_order_acquire);
        const uint64_t current_min = (current_max > capacity) ? (current_max - capacity) : 0;
        if (current_max == 0) continue;

        std::uniform_int_distribution<uint64_t> dis(0, current_max);
        uint64_t target_ts = dis(gen);

        auto result = buffer.binsearch(target_ts);
        const uint64_t max_after_search = max_ts.load(std::memory_order_acquire);

        if (result.elem != (int)result.ts) {
          errors++;
        }

        // add() publishes to the buffer immediately before max_ts is updated,
        // so the buffer may legitimately be one element ahead of max_ts.
        if (result.ts < current_min || result.ts > max_after_search + 1 ||
            result.ts >= num_elements) {
          errors++;
        }
      }
    });
  }

  producer.join();
  for (auto& t : consumers) t.join();

  EXPECT_EQ(errors.load(), 0);
}

TEST(StampedRingBufferTest, MultithreadedGetNewest) {
  const int capacity = 2048;
  const int num_elements = 5000;
  const int num_consumers = 4;

  StampedRingBuffer<int> buffer(capacity);
  std::atomic<bool> producer_done{false};

  std::thread producer([&]() {
    for (int i = 0; i < num_elements; ++i) {
      buffer.add({(uint64_t)i, i});
      std::this_thread::yield();
    }
    producer_done = true;
  });

  std::vector<std::thread> consumers;
  std::atomic<int> errors{0};

  for (int c = 0; c < num_consumers; ++c) {
    consumers.emplace_back([&]() {
      uint64_t last_seen_ts = 0;
      while (!producer_done) {
        if (buffer.empty()) continue;

        auto result = buffer.get_latest();

        if (result.elem != (int)result.ts) {
          errors++;
        }

        if (result.ts < last_seen_ts) {
          errors++;
          printf("Error: timestamps went backwards from %llu to %llu, diff %llu\n",
                 static_cast<unsigned long long>(last_seen_ts),
                 static_cast<unsigned long long>(result.ts),
                 static_cast<unsigned long long>(last_seen_ts - result.ts));
        }
        last_seen_ts = result.ts;
      }
    });
  }

  producer.join();
  for (auto& t : consumers) t.join();

  EXPECT_EQ(errors.load(), 0);
}
