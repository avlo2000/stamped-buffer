#ifndef STAMPED_BUFFER_DETAIL_STAMPED_SLOT_HPP
#define STAMPED_BUFFER_DETAIL_STAMPED_SLOT_HPP

#include <atomic>
#include <cstdint>
#include "stamped_buffer/detail/seqlock.hpp"

namespace stamped_buffer::detail {

// Timestamp and payload are separate atomic representations. The ring's global
// generation check validates their association and the complete search/batch.
// Payload validation remains necessary before reconstructing an arbitrary T.
template <class T>
class alignas(64) StampedSlot {
public:
  void store(std::uint64_t timestamp, const T& payload) noexcept {
    payload_.store(payload);
    timestamp_.store(timestamp, std::memory_order_seq_cst);
  }

  [[nodiscard]] std::uint64_t timestamp() const noexcept {
    return timestamp_.load(std::memory_order_seq_cst);
  }

  [[nodiscard]] T payload() const { return payload_.load(); }

  [[nodiscard]] bool try_payload(T& output) const noexcept { return payload_.try_load(output); }

private:
  std::atomic<std::uint64_t> timestamp_{0};
  Seqlock<T> payload_;
};

}  // namespace stamped_buffer::detail

#endif  // STAMPED_BUFFER_DETAIL_STAMPED_SLOT_HPP
