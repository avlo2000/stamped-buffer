#include <gtest/gtest.h>
#include <stamped_buffer/stamped_ring_buffer.hpp>

#include <algorithm>
#include <array>
#include <cstdint>
#include <cstring>
#include <deque>
#include <stdexcept>
#include <type_traits>
#include <vector>

namespace {

template <std::size_t N>
void check_partial_word() {
  using Bytes = std::array<unsigned char, N>;
  stamped_buffer::detail::Seqlock<Bytes> slot;
  EXPECT_EQ(slot.load(), Bytes{});
  for (unsigned iteration = 0; iteration < 3; ++iteration) {
    Bytes input{};
    for (std::size_t i = 0; i < N; ++i) input[i] = static_cast<unsigned char>(255 - i - iteration);
    slot.store(input);
    EXPECT_EQ(slot.load(), input);
    Bytes output{};
    ASSERT_TRUE(slot.try_load(output));
    EXPECT_EQ(output, input);
  }
}

TEST(StampedBufferStorageTest, PreservesPartialWordRepresentations) {
  check_partial_word<1>();
  check_partial_word<3>();
  check_partial_word<7>();
  check_partial_word<8>();
  check_partial_word<9>();
  check_partial_word<15>();
  check_partial_word<17>();
}

struct BasePayload { std::uint64_t first{}; };
struct InheritedPayload : BasePayload { std::uint64_t second{}; };
static_assert(std::is_trivially_copyable<InheritedPayload>::value);
static_assert(!std::is_standard_layout<InheritedPayload>::value);

TEST(StampedBufferStorageTest, SupportsNonStandardLayoutPayloads) {
  stamped_buffer::StampedRingBuffer<InheritedPayload> buffer(3);
  for (std::uint64_t i = 1; i <= 8; ++i) {
    InheritedPayload payload;
    payload.first = i + 1000;
    payload.second = ~i;
    buffer.add({i * 10, payload});
  }
  const auto result = buffer.binsearch(65);
  EXPECT_EQ(result.ts, 70U);
  EXPECT_EQ(result.elem.first, 1007U);
  EXPECT_EQ(result.elem.second, ~std::uint64_t{7});
}

struct alignas(256) AlignedPayload {
  bool enabled{};
  double number{};
  const int* pointer{};
};

TEST(StampedBufferStorageTest, PreservesPaddingAlignmentPointersAndNanBits) {
  stamped_buffer::StampedRingBuffer<AlignedPayload> buffer(2);
  const int marker = 42;
  AlignedPayload payload;
  payload.enabled = true;
  payload.pointer = &marker;
  const std::uint64_t nan_bits = 0x7ff8000000000042ULL;
  static_assert(sizeof(double) == sizeof(nan_bits));
  std::memcpy(&payload.number, &nan_bits, sizeof(nan_bits));
  buffer.add({10, payload});
  const auto value = buffer.binsearch(10);
  EXPECT_TRUE(value.elem.enabled);
  EXPECT_EQ(value.elem.pointer, &marker);
  std::uint64_t returned_bits{};
  std::memcpy(&returned_bits, &value.elem.number, sizeof(returned_bits));
  EXPECT_EQ(returned_bits, nan_bits);
}

struct ThrowingDefault {
  static bool should_throw;
  int value{};
  ThrowingDefault() { if (should_throw) throw std::runtime_error("default construction"); }
};
bool ThrowingDefault::should_throw = false;

TEST(StampedBufferStorageTest, PropagatesDefaultConstructionExceptions) {
  stamped_buffer::StampedRingBuffer<ThrowingDefault> buffer(2);
  ThrowingDefault payload;
  buffer.add({10, payload});
  ThrowingDefault::should_throw = true;
  EXPECT_THROW(static_cast<void>(buffer.get_latest()), std::runtime_error);
  EXPECT_THROW(static_cast<void>(buffer.binsearch(10)), std::runtime_error);
  EXPECT_THROW((stamped_buffer::StampedRingBuffer<ThrowingDefault>(2)), std::runtime_error);
  ThrowingDefault::should_throw = false;
  EXPECT_EQ(buffer.get_latest().ts, 10U);
}

TEST(StampedBufferOptimizedTest, LowerBoundMatchesReferenceIncludingFirstDuplicate) {
  using Buffer = stamped_buffer::StampedRingBuffer<int>;
  for (std::size_t capacity : {1, 2, 3, 4, 5, 63, 64, 65, 128, 129}) {
    Buffer buffer(capacity);
    std::deque<Buffer::value_type> expected;
    for (std::size_t i = 0; i < capacity * 2 + 5; ++i) {
      Buffer::value_type value{2 * (i / 3), static_cast<int>(i)};
      buffer.add(value);
      expected.push_back(value);
      if (expected.size() > capacity) expected.pop_front();
      for (std::uint64_t query = 0; query <= value.ts + 2; ++query) {
        auto it = std::lower_bound(expected.begin(), expected.end(), query,
                                  [](const auto& item, auto ts) { return item.ts < ts; });
        if (it == expected.end()) --it;
        const auto actual = buffer.binsearch(query);
        ASSERT_EQ(actual.ts, it->ts);
        ASSERT_EQ(actual.elem, it->elem) << "capacity=" << capacity << ", query=" << query;
        Buffer::value_type attempted;
        ASSERT_EQ(buffer.try_binsearch(query, attempted), Buffer::ReadStatus::success);
        ASSERT_EQ(attempted.ts, it->ts);
        ASSERT_EQ(attempted.elem, it->elem);
      }
    }
  }
}

TEST(StampedBufferOptimizedTest, CallerBufferBatchesMatchRetainedWindow) {
  using Buffer = stamped_buffer::StampedRingBuffer<int>;
  for (std::size_t capacity : {1, 2, 3, 4, 63, 64, 65}) {
    Buffer buffer(capacity);
    std::vector<Buffer::value_type> output(capacity + 3, {9999, -1});
    EXPECT_EQ(buffer.get_last_n(output.data(), output.size()), 0U);
    for (std::size_t i = 0; i < capacity * 2 + 5; ++i) buffer.add({i * 2, static_cast<int>(i)});
    for (std::size_t requested : {std::size_t{0}, std::size_t{1}, capacity - 1, capacity, capacity + 2}) {
      std::fill(output.begin(), output.end(), Buffer::value_type{9999, -1});
      const auto expected = buffer.get_last_n(requested);
      const auto count = buffer.get_last_n(output.data(), requested);
      ASSERT_EQ(count, expected.size());
      for (std::size_t i = 0; i < count; ++i) {
        EXPECT_EQ(output[i].ts, expected[i].ts);
        EXPECT_EQ(output[i].elem, expected[i].elem);
      }
      for (std::size_t i = count; i < output.size(); ++i) EXPECT_EQ(output[i].elem, -1);
      std::size_t attempted_count = 1234;
      ASSERT_EQ(buffer.try_get_last_n(output.data(), requested, attempted_count), Buffer::ReadStatus::success);
      ASSERT_EQ(attempted_count, count);
      for (std::size_t i = 0; i < count; ++i) {
        EXPECT_EQ(output[i].ts, expected[i].ts);
        EXPECT_EQ(output[i].elem, expected[i].elem);
      }
    }
  }
}

TEST(StampedBufferOptimizedTest, TryReadEmptyAndOutputContracts) {
  using Buffer = stamped_buffer::StampedRingBuffer<int>;
  Buffer buffer(3);
  Buffer::value_type out{42, -1};
  EXPECT_EQ(buffer.try_get_latest(out), Buffer::ReadStatus::empty);
  EXPECT_EQ(buffer.try_binsearch(10, out), Buffer::ReadStatus::empty);
  EXPECT_EQ(out.ts, 42U);
  EXPECT_EQ(out.elem, -1);
  std::size_t count = 123;
  EXPECT_EQ(buffer.try_get_last_n(&out, 1, count), Buffer::ReadStatus::empty);
  EXPECT_EQ(count, 0U);
  EXPECT_EQ(out.ts, 42U);
  EXPECT_EQ(buffer.get_last_n(nullptr, 0), 0U);
  EXPECT_EQ(buffer.try_get_last_n(nullptr, 0, count), Buffer::ReadStatus::success);
  EXPECT_EQ(count, 0U);
  EXPECT_THROW(static_cast<void>(buffer.get_last_n(nullptr, 1)), std::invalid_argument);
  EXPECT_THROW(static_cast<void>(buffer.try_get_last_n(nullptr, 1, count)), std::invalid_argument);
  buffer.add({10, 100});
  EXPECT_EQ(buffer.try_get_latest(out), Buffer::ReadStatus::success);
  EXPECT_EQ(out.ts, 10U);
  EXPECT_EQ(out.elem, 100);
}

}  // namespace
