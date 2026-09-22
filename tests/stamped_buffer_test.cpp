// SPDX-License-Identifier: MIT

#include <gtest/gtest.h>

#include <algorithm>
#include <atomic>
#include <chrono>
#include <cmath>
#include <cstddef>
#include <stamped_buffer/stamped_ring_buffer.hpp>
#include <random>
#include <stdexcept>
#include <thread>
#include <vector>

using stamped_buffer::StampedRingBuffer;

TEST(StampedRingBufferTest, BasicFunctionality) {
  StampedRingBuffer<int> buffer(5);

  EXPECT_TRUE(buffer.empty());
  EXPECT_EQ(buffer.size(), std::size_t{0});

  for (int i = 0; i < 3; ++i) {
    buffer.add({(uint64_t)(i * 10), i});
  }

  EXPECT_EQ(buffer.size(), std::size_t{3});
  EXPECT_FALSE(buffer.empty());

  auto result = buffer.binsearch(10);
  EXPECT_EQ(result.ts, uint64_t{10});
  EXPECT_EQ(result.elem, 1);

  result = buffer.binsearch(0);
  EXPECT_EQ(result.ts, uint64_t{0});
  EXPECT_EQ(result.elem, 0);

  result = buffer.binsearch(20);
  EXPECT_EQ(result.ts, uint64_t{20});
  EXPECT_EQ(result.elem, 2);
}

TEST(StampedRingBufferTest, ClosestTimestampSearch) {
  StampedRingBuffer<char> buffer(10);

  for (int i = 1; i <= 5; ++i) {
    buffer.add({(uint64_t)(i * 10), (char)('A' + i - 1)});
  }

  auto result = buffer.binsearch(15);
  EXPECT_TRUE(result.ts == 10 || result.ts == 20);

  result = buffer.binsearch(25);
  EXPECT_TRUE(result.ts == 20 || result.ts == 30);

  result = buffer.binsearch(35);
  EXPECT_TRUE(result.ts == 30 || result.ts == 40);

  result = buffer.binsearch(5);
  EXPECT_EQ(result.ts, uint64_t{10});

  result = buffer.binsearch(55);
  EXPECT_EQ(result.ts, uint64_t{50});
}

TEST(StampedRingBufferTest, RingBufferWraparound) {
  StampedRingBuffer<int> buffer(3);

  for (int i = 0; i < 3; ++i) {
    buffer.add({(uint64_t)(i * 10), i});
  }
  EXPECT_EQ(buffer.size(), std::size_t{3});

  for (int i = 3; i < 6; ++i) {
    buffer.add({(uint64_t)(i * 10), i});
  }

  EXPECT_EQ(buffer.size(), std::size_t{3});

  auto result = buffer.binsearch(40);
  EXPECT_EQ(result.ts, uint64_t{40});

  result = buffer.binsearch(35);
  EXPECT_TRUE(result.ts == 30 || result.ts == 40);

  result = buffer.binsearch(55);
  EXPECT_EQ(result.ts, uint64_t{50});
}

TEST(StampedRingBufferTest, SingleElement) {
  StampedRingBuffer<double> buffer(5);
  buffer.add({100, 3.14});

  EXPECT_EQ(buffer.size(), std::size_t{1});

  auto result = buffer.binsearch(100);
  EXPECT_EQ(result.ts, uint64_t{100});
  EXPECT_DOUBLE_EQ(result.elem, 3.14);

  result = buffer.binsearch(50);
  EXPECT_EQ(result.ts, uint64_t{100});

  result = buffer.binsearch(150);
  EXPECT_EQ(result.ts, uint64_t{100});
}

TEST(StampedRingBufferTest, TwoElements) {
  StampedRingBuffer<int> buffer(5);
  buffer.add({10, 1});
  buffer.add({30, 3});

  EXPECT_EQ(buffer.size(), std::size_t{2});

  auto result = buffer.binsearch(10);
  EXPECT_EQ(result.ts, uint64_t{10});

  result = buffer.binsearch(30);
  EXPECT_EQ(result.ts, uint64_t{30});

  result = buffer.binsearch(20);
  EXPECT_TRUE(result.ts == 10 || result.ts == 30);

  result = buffer.binsearch(5);
  EXPECT_EQ(result.ts, uint64_t{10});

  result = buffer.binsearch(35);
  EXPECT_EQ(result.ts, uint64_t{30});
}

TEST(StampedRingBufferTest, DuplicateTimestamps) {
  StampedRingBuffer<int> buffer(5);
  buffer.add({10, 1});
  buffer.add({20, 2});
  buffer.add({20, 22});
  buffer.add({30, 3});

  auto result = buffer.binsearch(20);
  EXPECT_EQ(result.ts, uint64_t{20});
  EXPECT_TRUE(result.elem == 2 || result.elem == 22);
}

TEST(StampedRingBufferTest, LargeBufferRandom) {
  StampedRingBuffer<int> buffer(100);
  std::vector<uint64_t> timestamps;

  for (int i = 0; i < 50; ++i) {
    uint64_t ts = i * 100;
    timestamps.push_back(ts);
    buffer.add({ts, i});
  }

  std::random_device rd;
  std::mt19937 gen(rd());
  std::uniform_int_distribution<> dis(0, 4900);

  for (int i = 0; i < 20; ++i) {
    uint64_t search_ts = dis(gen);
    auto result = buffer.binsearch(search_ts);

    auto it = std::lower_bound(timestamps.begin(), timestamps.end(), search_ts);
    uint64_t expected_ts;

    if (it == timestamps.end()) {
      expected_ts = timestamps.back();
    } else if (it == timestamps.begin()) {
      expected_ts = timestamps.front();
    } else {
      uint64_t upper = *it;
      uint64_t lower = *(it - 1);
      expected_ts = (search_ts - lower <= upper - search_ts) ? lower : upper;
    }

    bool close_match =
        (result.ts == expected_ts) || (std::abs((int64_t)result.ts - (int64_t)search_ts) <=
                                       std::abs((int64_t)expected_ts - (int64_t)search_ts) + 100);

    EXPECT_TRUE(close_match) << "Random search " << i << " for ts=" << search_ts;
  }
}

TEST(StampedRingBufferTest, ComplexWraparound) {
  StampedRingBuffer<int> buffer(4);

  for (int i = 0; i < 10; ++i) {
    buffer.add({(uint64_t)(i * 10), i});
  }

  EXPECT_EQ(buffer.size(), std::size_t{4});

  auto result = buffer.binsearch(75);
  EXPECT_TRUE(result.ts == 70 || result.ts == 80);

  result = buffer.binsearch(55);
  EXPECT_EQ(result.ts, uint64_t{60});

  result = buffer.binsearch(95);
  EXPECT_EQ(result.ts, uint64_t{90});
}

TEST(StampedRingBufferTest, CapacityOne) {
  StampedRingBuffer<int> buffer(1);

  for (int i = 0; i < 5; ++i) {
    buffer.add({(uint64_t)(i * 10), i});
    EXPECT_EQ(buffer.size(), std::size_t{1});

    auto result = buffer.binsearch(i * 10);
    EXPECT_EQ(result.ts, (uint64_t)(i * 10));
    EXPECT_EQ(result.elem, i);
  }
}

TEST(StampedRingBufferTest, BoundaryValues) {
  StampedRingBuffer<int> buffer(5);

  buffer.add({0, 0});
  buffer.add({UINT64_MAX / 2, 1});
  buffer.add({UINT64_MAX - 1, 2});

  auto result = buffer.binsearch(0);
  EXPECT_EQ(result.ts, uint64_t{0});

  result = buffer.binsearch(UINT64_MAX);
  EXPECT_EQ(result.ts, UINT64_MAX - 1);

  result = buffer.binsearch(UINT64_MAX / 4);
  EXPECT_TRUE(result.ts == 0 || result.ts == UINT64_MAX / 2);
}

TEST(StampedRingBufferTest, GetLastNAfterWraparound) {
  StampedRingBuffer<int> buffer(3);
  for (int value = 0; value < 5; ++value) {
    buffer.add({static_cast<uint64_t>(value * 10), value});
  }

  const auto last = buffer.get_last_n(10);
  ASSERT_EQ(last.size(), std::size_t{3});
  EXPECT_EQ(last[0].elem, 2);
  EXPECT_EQ(last[1].elem, 3);
  EXPECT_EQ(last[2].elem, 4);
  EXPECT_TRUE(buffer.get_last_n(0).empty());
}

TEST(StampedRingBufferTest, RejectsZeroCapacity) {
  EXPECT_THROW(StampedRingBuffer<int>(0), std::invalid_argument);
}

TEST(StampedRingBufferTest, EmptyReadsThrow) {
  StampedRingBuffer<int> buffer(1);
  EXPECT_THROW(static_cast<void>(buffer.get_latest()), std::out_of_range);
  EXPECT_THROW(static_cast<void>(buffer.binsearch(42)), std::out_of_range);
}
