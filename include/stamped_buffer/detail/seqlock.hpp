/*
Copyright (c) 2018 Erik Rigtorp <erik@rigtorp.se>

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all
copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
SOFTWARE.
*/

#ifndef STAMPED_BUFFER_DETAIL_SEQLOCK_HPP
#define STAMPED_BUFFER_DETAIL_SEQLOCK_HPP

#include <atomic>
#include <cstddef>
#include <type_traits>

#if !defined(NDEBUG) && (defined(__GNUC__) || defined(__clang__))
#define STAMPED_BUFFER_NOINLINE __attribute__((noinline))
#else
#define STAMPED_BUFFER_NOINLINE
#endif

namespace stamped_buffer::detail {

// Adapted from https://github.com/rigtorp/Seqlock.
template <typename T>
class Seqlock {
public:
  static_assert(std::is_nothrow_copy_assignable<T>::value, "T must be nothrow copy assignable");
  static_assert(std::is_trivially_copy_assignable<T>::value, "T must be trivially copy assignable");

  Seqlock() noexcept : sequence_(0) {}

  STAMPED_BUFFER_NOINLINE T load() const noexcept {
    T copy;
    std::size_t before;
    std::size_t after;
    do {
      before = sequence_.load(std::memory_order_acquire);
      std::atomic_signal_fence(std::memory_order_acq_rel);
      copy = value_;
      std::atomic_signal_fence(std::memory_order_acq_rel);
      after = sequence_.load(std::memory_order_acquire);
    } while (before != after || (before & 1U) != 0U);
    return copy;
  }

  STAMPED_BUFFER_NOINLINE void store(const T& desired) noexcept {
    const auto sequence = sequence_.load(std::memory_order_relaxed);
    sequence_.store(sequence + 1U, std::memory_order_release);
    std::atomic_signal_fence(std::memory_order_acq_rel);
    value_ = desired;
    std::atomic_signal_fence(std::memory_order_acq_rel);
    sequence_.store(sequence + 2U, std::memory_order_release);
  }

private:
  static constexpr std::size_t cache_line_size = 128;

  alignas(cache_line_size) T value_;
  std::atomic<std::size_t> sequence_;
};

}  // namespace stamped_buffer::detail

#undef STAMPED_BUFFER_NOINLINE

#endif  // STAMPED_BUFFER_DETAIL_SEQLOCK_HPP
