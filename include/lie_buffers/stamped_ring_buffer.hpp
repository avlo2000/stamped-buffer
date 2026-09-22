#ifndef LIE_BUFFERS_STAMPED_RING_BUFFER_HPP
#define LIE_BUFFERS_STAMPED_RING_BUFFER_HPP

#include <algorithm>
#include <atomic>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <stdexcept>
#include <type_traits>
#include <vector>

#include "lie_buffers/detail/seqlock.hpp"

namespace lie_buffers {

template <class T>
struct StampedElement {
  StampedElement() = default;
  StampedElement(std::uint64_t timestamp, T element) : ts(timestamp), elem(element) {}

  std::uint64_t ts{};
  T elem{};
};

// A fixed-capacity ring buffer for one producer and any number of consumers.
// Elements must be added in non-decreasing timestamp order.
template <class T>
class StampedRingBuffer {
public:
  using value_type = StampedElement<T>;
  using size_type = std::size_t;

  static_assert(std::is_trivially_copy_assignable<value_type>::value,
                "StampedRingBuffer values must be trivially copy assignable");
  static_assert(std::is_nothrow_copy_assignable<value_type>::value,
                "StampedRingBuffer values must be nothrow copy assignable");
  static_assert(std::is_default_constructible<value_type>::value,
                "StampedRingBuffer values must be default constructible");

  explicit StampedRingBuffer(size_type capacity)
      : buffer_(capacity == 0 ? nullptr : std::make_unique<Slot[]>(capacity)), capacity_(capacity) {
    if (capacity == 0) {
      throw std::invalid_argument("StampedRingBuffer capacity must be greater than zero");
    }
  }

  StampedRingBuffer(const StampedRingBuffer&) = delete;
  StampedRingBuffer& operator=(const StampedRingBuffer&) = delete;
  StampedRingBuffer(StampedRingBuffer&&) = delete;
  StampedRingBuffer& operator=(StampedRingBuffer&&) = delete;

  // add() may be called by one producer thread only.
  void add(const value_type& element) noexcept {
    const auto published = published_.load(std::memory_order_relaxed);
    buffer_[physical_index(published)].store(element);
    published_.store(published + 1U, std::memory_order_release);
  }

  [[nodiscard]] bool empty() const noexcept {
    return published_.load(std::memory_order_acquire) == 0;
  }

  [[nodiscard]] size_type size() const noexcept {
    return snapshot_size(published_.load(std::memory_order_acquire));
  }

  [[nodiscard]] constexpr size_type capacity() const noexcept { return capacity_; }

  [[nodiscard]] value_type get_latest() const {
    while (true) {
      const auto published = published_.load(std::memory_order_acquire);
      if (published == 0) {
        throw std::out_of_range("cannot read from an empty StampedRingBuffer");
      }

      const auto result = buffer_[physical_index(published - 1U)].load();
      if (published_.load(std::memory_order_acquire) == published) {
        return result;
      }
    }
  }

  // Returns the first element whose timestamp is not less than ts. If ts is
  // newer than every retained element, the newest element is returned.
  [[nodiscard]] value_type binsearch(std::uint64_t ts) const {
    while (true) {
      const auto published = published_.load(std::memory_order_acquire);
      const auto count = snapshot_size(published);
      if (count == 0) {
        throw std::out_of_range("cannot search an empty StampedRingBuffer");
      }

      const auto first_sequence = published - count;
      size_type first = 0;
      size_type length = count;
      while (length != 0) {
        const auto half = length / 2U;
        const auto middle = first + half;
        const auto element = buffer_[physical_index(first_sequence + middle)].load();
        if (element.ts < ts) {
          first = middle + 1U;
          length -= half + 1U;
        } else {
          length = half;
        }
      }

      const auto offset = first == count ? count - 1U : first;
      const auto result = buffer_[physical_index(first_sequence + offset)].load();
      if (published_.load(std::memory_order_acquire) == published) {
        return result;
      }
    }
  }

  // Returns up to n newest elements in oldest-to-newest order.
  [[nodiscard]] std::vector<value_type> get_last_n(size_type n) const {
    if (n == 0) {
      return {};
    }

    while (true) {
      const auto published = published_.load(std::memory_order_acquire);
      const auto available = snapshot_size(published);
      const auto count = std::min(n, available);
      std::vector<value_type> result;
      result.reserve(count);

      const auto first_sequence = published - count;
      for (size_type offset = 0; offset < count; ++offset) {
        result.push_back(buffer_[physical_index(first_sequence + offset)].load());
      }

      if (published_.load(std::memory_order_acquire) == published) {
        return result;
      }
    }
  }

private:
  using Slot = detail::Seqlock<value_type>;

  [[nodiscard]] size_type physical_index(std::uint64_t sequence) const noexcept {
    return static_cast<size_type>(sequence % capacity_);
  }

  [[nodiscard]] size_type snapshot_size(std::uint64_t published) const noexcept {
    return static_cast<size_type>(std::min<std::uint64_t>(published, capacity_));
  }

  std::unique_ptr<Slot[]> buffer_;
  const size_type capacity_;
  alignas(64) std::atomic<std::uint64_t> published_{0};
};

}  // namespace lie_buffers

#endif  // LIE_BUFFERS_STAMPED_RING_BUFFER_HPP
