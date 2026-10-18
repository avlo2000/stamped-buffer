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

#include <array>
#include <atomic>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <memory>
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
  // Keep object representations as bytes, including padding, while performing
  // one lock-free atomic operation per machine word. No aliasing cast of T is
  // used, and a partial final word is zero-filled before publication.
  using Word = std::array<unsigned char, sizeof(std::uint64_t)>;
  static constexpr std::size_t word_count = (sizeof(T) + sizeof(Word) - 1) / sizeof(Word);
  using Representation = std::array<Word, word_count>;

public:
  static_assert(std::is_trivially_copyable<T>::value, "T must be trivially copyable");
  static_assert(std::is_nothrow_copy_assignable<T>::value, "T must be nothrow copy assignable");
  static_assert(std::is_trivially_copy_assignable<T>::value, "T must be trivially copy assignable");
  static_assert(sizeof(Word) == sizeof(std::uint64_t), "Unexpected word representation padding");
  static_assert(std::atomic<Word>::is_always_lock_free,
                "Seqlock requires lock-free word-sized atomics");
  static_assert(std::atomic<std::uint64_t>::is_always_lock_free,
                "Seqlock requires a lock-free sequence counter");

  Seqlock() noexcept(std::is_nothrow_default_constructible<T>::value) {
    const T initial{};
    Representation bytes{};
    std::memcpy(bytes.data(), std::addressof(initial), sizeof(T));
    for (std::size_t i = 0; i < bytes.size(); ++i) {
      // Default-constructed atomics require initialization in C++17.
      std::atomic_init(std::addressof(value_[i]), bytes[i]);
    }
  }

  STAMPED_BUFFER_NOINLINE T load() const {
    Representation bytes;
    while (!try_read(bytes)) {}
    // Reconstruct T only after validation: mixed words may not represent a valid T.
    T copy{};
    std::memcpy(static_cast<void*>(std::addressof(copy)), bytes.data(), sizeof(T));
    return copy;
  }

  // One attempt, including the slot validation. Failure leaves output unchanged.
  [[nodiscard]] bool try_load(T& output) const noexcept {
    Representation bytes;
    if (!try_read(bytes)) return false;
    std::memcpy(static_cast<void*>(std::addressof(output)), bytes.data(), sizeof(T));
    return true;
  }

  STAMPED_BUFFER_NOINLINE void store(const T& desired) noexcept {
    Representation bytes{};
    std::memcpy(bytes.data(), std::addressof(desired), sizeof(T));
    const auto sequence = sequence_.load(std::memory_order_relaxed);
    sequence_.store(sequence + 1U, std::memory_order_seq_cst);
    for (std::size_t i = 0; i < bytes.size(); ++i) {
      value_[i].store(bytes[i], std::memory_order_seq_cst);
    }
    sequence_.store(sequence + 2U, std::memory_order_seq_cst);
  }

private:
  [[nodiscard]] bool try_read(Representation& bytes) const noexcept {
    const auto before = sequence_.load(std::memory_order_seq_cst);
    if ((before & 1U) != 0U) return false;
    for (std::size_t i = 0; i < bytes.size(); ++i) {
      bytes[i] = value_[i].load(std::memory_order_seq_cst);
    }
    return sequence_.load(std::memory_order_seq_cst) == before;
  }

  // Atomic payload access makes even discarded reads race-free. Sequentially
  // consistent payload and sequence operations share one total order: a reader
  // that observes an overwrite must also observe its sequence change.
  // The containing ring slot supplies cache-line alignment.
  std::array<std::atomic<Word>, word_count> value_;
  std::atomic<std::uint64_t> sequence_{0};
};

}  // namespace stamped_buffer::detail

#undef STAMPED_BUFFER_NOINLINE

#endif  // STAMPED_BUFFER_DETAIL_SEQLOCK_HPP
