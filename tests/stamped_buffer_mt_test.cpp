// SPDX-License-Identifier: MIT

#include <gtest/gtest.h>

#include <algorithm>
#include <atomic>
#include <chrono>
#include <cmath>
#include <stamped_buffer/stamped_ring_buffer.hpp>
#include <random>
#include <thread>
#include <vector>

using stamped_buffer::StampedRingBuffer;

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
