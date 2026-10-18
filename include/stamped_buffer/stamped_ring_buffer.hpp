#ifndef STAMPED_BUFFER_STAMPED_RING_BUFFER_HPP
#define STAMPED_BUFFER_STAMPED_RING_BUFFER_HPP

#include <algorithm>
#include <atomic>
#include <cstddef>
#include <cstdint>
#include <iterator>
#include <memory>
#include <stdexcept>
#include <type_traits>
#include <vector>

#include "stamped_buffer/detail/stamped_slot.hpp"

namespace stamped_buffer {

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

  // A try-read performs one validation attempt, including each payload load.
  enum class ReadStatus { success, empty, busy };

  static_assert(std::is_trivially_copyable<value_type>::value,
                "StampedRingBuffer values must be trivially copyable");
  static_assert(std::is_trivially_copy_assignable<value_type>::value,
                "StampedRingBuffer values must be trivially copy assignable");
  static_assert(std::is_nothrow_copy_assignable<value_type>::value,
                "StampedRingBuffer values must be nothrow copy assignable");
  static_assert(std::is_default_constructible<value_type>::value,
                "StampedRingBuffer values must be default constructible");
  static_assert(std::atomic<std::uint64_t>::is_always_lock_free,
                "StampedRingBuffer requires lock-free 64-bit atomics");

  explicit StampedRingBuffer(size_type capacity)
      : buffer_(capacity == 0 ? nullptr : std::make_unique<Slot[]>(capacity)),
        capacity_(capacity), power_of_two_(capacity != 0 && (capacity & (capacity - 1U)) == 0) {
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
    const auto generation = generation_.load(std::memory_order_relaxed);
    // Mark the entire update in progress before overwriting a retained slot.
    generation_.store(generation + 1U, std::memory_order_seq_cst);
    buffer_[physical_index(published)].store(element.ts, element.elem);
    published_.store(published + 1U, std::memory_order_release);
    generation_.store(generation + 2U, std::memory_order_seq_cst);
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
      const auto generation = generation_.load(std::memory_order_seq_cst);
      if ((generation & 1U) != 0U) {
        continue;
      }
      const auto published = published_.load(std::memory_order_acquire);
      if (published == 0) {
        throw std::out_of_range("cannot read from an empty StampedRingBuffer");
      }

      const auto result = load_element(physical_index(published - 1U));
      if (generation_.load(std::memory_order_seq_cst) == generation) {
        return result;
      }
    }
  }

  // Returns the first element whose timestamp is not less than ts. If ts is
  // newer than every retained element, the newest element is returned.
  [[nodiscard]] value_type binsearch(std::uint64_t ts) const {
    // Dispatch once per search; arbitrary capacities do not pay for a mask
    // branch at every binary-search probe.
    return power_of_two_ ? binsearch_impl<true>(ts) : binsearch_impl<false>(ts);
  }

  // On failure, output is unchanged. User-defined default constructors may run;
  // these APIs bound validation attempts, not arbitrary user code or wall time.
  [[nodiscard]] ReadStatus try_get_latest(value_type& output) const {
    value_type candidate{};
    const auto generation = generation_.load(std::memory_order_seq_cst);
    if ((generation & 1U) != 0U) return ReadStatus::busy;
    const auto published = published_.load(std::memory_order_acquire);
    if (published == 0) return ReadStatus::empty;
    if (!try_load_element(physical_index(published - 1U), candidate) ||
        generation_.load(std::memory_order_seq_cst) != generation) return ReadStatus::busy;
    output = candidate;
    return ReadStatus::success;
  }

  [[nodiscard]] ReadStatus try_binsearch(std::uint64_t ts, value_type& output) const {
    return power_of_two_ ? try_binsearch_impl<true>(ts, output) : try_binsearch_impl<false>(ts, output);
  }

  // Returns up to n newest elements in oldest-to-newest order.
  [[nodiscard]] std::vector<value_type> get_last_n(size_type n) const {
    std::vector<value_type> result;
    if (n == 0) return result;
    while (true) {
      result.clear();
      const auto generation = generation_.load(std::memory_order_seq_cst);
      if ((generation & 1U) != 0U) continue;
      const auto published = published_.load(std::memory_order_acquire);
      const auto count = std::min(n, snapshot_size(published));
      if (result.capacity() < count) {
        result.reserve(count);
        // Start a fresh attempt after allocation, retaining storage on retries.
        continue;
      }
      copy_range(published - count, count, std::back_inserter(result));
      if (generation_.load(std::memory_order_seq_cst) == generation) return result;
    }
  }

  // Allocation-free overload. output points to n constructed values owned by
  // this caller, or may be null when n == 0. Returns the number copied.
  [[nodiscard]] size_type get_last_n(value_type* output, size_type n) const {
    check_output(output, n);
    if (n == 0) return 0;
    while (true) {
      const auto generation = generation_.load(std::memory_order_seq_cst);
      if ((generation & 1U) != 0U) continue;
      const auto published = published_.load(std::memory_order_acquire);
      const auto count = std::min(n, snapshot_size(published));
      copy_range(published - count, count, output);
      if (generation_.load(std::memory_order_seq_cst) == generation) return count;
    }
  }

  // One attempt, with no allocation or inner retry loops. On busy, output may
  // contain a partial batch and copied is zero; only success exposes a snapshot.
  [[nodiscard]] ReadStatus try_get_last_n(value_type* output, size_type n, size_type& copied) const {
    check_output(output, n);
    copied = 0;
    if (n == 0) return ReadStatus::success;
    const auto generation = generation_.load(std::memory_order_seq_cst);
    if ((generation & 1U) != 0U) return ReadStatus::busy;
    const auto published = published_.load(std::memory_order_acquire);
    if (published == 0) return ReadStatus::empty;
    const auto count = std::min(n, snapshot_size(published));
    auto index = physical_index(published - count);
    for (size_type offset = 0; offset < count; ++offset) {
      if (!try_load_element(index, output[offset])) return ReadStatus::busy;
      if (++index == capacity_) index = 0;
    }
    if (generation_.load(std::memory_order_seq_cst) != generation) return ReadStatus::busy;
    copied = count;
    return ReadStatus::success;
  }

private:
  using Slot = detail::StampedSlot<T>;

  static void check_output(value_type* output, size_type n) {
    if (output == nullptr && n != 0) throw std::invalid_argument("output must not be null when n is nonzero");
  }

  [[nodiscard]] value_type load_element(size_type index) const {
    return {buffer_[index].timestamp(), buffer_[index].payload()};
  }

  [[nodiscard]] bool try_load_element(size_type index, value_type& output) const noexcept {
    const auto timestamp = buffer_[index].timestamp();
    if (!buffer_[index].try_payload(output.elem)) return false;
    output.ts = timestamp;
    return true;
  }

  template <class OutputIterator>
  void copy_range(std::uint64_t first_sequence, size_type count, OutputIterator output) const {
    if (count == 0) return;
    auto index = physical_index(first_sequence);
    for (size_type offset = 0; offset < count; ++offset) {
      *output++ = load_element(index);
      if (++index == capacity_) index = 0;
    }
  }

  template <bool Masked>
  [[nodiscard]] size_type search_index(std::uint64_t ts, std::uint64_t published, size_type count) const {
    const auto first_sequence = published - count;
    size_type first = 0;
    size_type length = count;
    while (length != 0) {
      const auto half = length / 2U;
      const auto middle = first + half;
      const auto timestamp = buffer_[physical_index_impl<Masked>(first_sequence + middle)].timestamp();
      if (timestamp < ts) {
        first = middle + 1U;
        length -= half + 1U;
      } else {
        length = half;
      }
    }
    const auto offset = first == count ? count - 1U : first;
    return physical_index_impl<Masked>(first_sequence + offset);
  }

  template <bool Masked>
  [[nodiscard]] value_type binsearch_impl(std::uint64_t ts) const {
    while (true) {
      const auto generation = generation_.load(std::memory_order_seq_cst);
      if ((generation & 1U) != 0U) {
        continue;
      }
      const auto published = published_.load(std::memory_order_acquire);
      const auto count = snapshot_size(published);
      if (count == 0) {
        throw std::out_of_range("cannot search an empty StampedRingBuffer");
      }

      const auto result = load_element(search_index<Masked>(ts, published, count));
      if (generation_.load(std::memory_order_seq_cst) == generation) {
        return result;
      }
    }
  }

  template <bool Masked>
  [[nodiscard]] ReadStatus try_binsearch_impl(std::uint64_t ts, value_type& output) const {
    value_type candidate{};
    const auto generation = generation_.load(std::memory_order_seq_cst);
    if ((generation & 1U) != 0U) return ReadStatus::busy;
    const auto published = published_.load(std::memory_order_acquire);
    const auto count = snapshot_size(published);
    if (count == 0) return ReadStatus::empty;
    if (!try_load_element(search_index<Masked>(ts, published, count), candidate) ||
        generation_.load(std::memory_order_seq_cst) != generation) return ReadStatus::busy;
    output = candidate;
    return ReadStatus::success;
  }

  template <bool Masked>
  [[nodiscard]] size_type physical_index_impl(std::uint64_t sequence) const noexcept {
    if constexpr (Masked) return static_cast<size_type>(sequence & (capacity_ - 1U));
    else return static_cast<size_type>(sequence % capacity_);
  }

  [[nodiscard]] size_type physical_index(std::uint64_t sequence) const noexcept {
    return power_of_two_ ? physical_index_impl<true>(sequence) : physical_index_impl<false>(sequence);
  }

  [[nodiscard]] size_type snapshot_size(std::uint64_t published) const noexcept {
    return static_cast<size_type>(std::min<std::uint64_t>(published, capacity_));
  }

  std::unique_ptr<Slot[]> buffer_;
  const size_type capacity_;
  const bool power_of_two_;
  alignas(64) std::atomic<std::uint64_t> published_{0};
  // Even generations identify complete snapshots; odd generations are writes.
  // Use the same sequentially consistent order as the slot payload operations
  // so validation also detects data written before published_ changes.
  std::atomic<std::uint64_t> generation_{0};
};

}  // namespace stamped_buffer

#endif  // STAMPED_BUFFER_STAMPED_RING_BUFFER_HPP
