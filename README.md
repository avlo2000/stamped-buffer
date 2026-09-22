# lie-buffers

`lie-buffers` is a small, header-only C++17 library providing a timestamped,
fixed-capacity ring buffer for a single producer and multiple concurrent
consumers.

## Requirements

- A C++17 compiler
- CMake 3.14 or newer (for building tests or installing the package)

## Usage

```cpp
#include <lie_buffers/stamped_ring_buffer.hpp>

lie_buffers::StampedRingBuffer<int> samples(128);
samples.add({100, 42});

auto latest = samples.get_latest();
```

Timestamps must be added in non-decreasing order. Calling `add` is restricted
to one producer thread; `empty`, `size`, `get_latest`, `binsearch`, and
`get_last_n` may be called by multiple consumer threads.

`binsearch` returns the first retained element whose timestamp is greater than
or equal to the requested timestamp, or the newest retained element when the
requested timestamp is newer than the available range.

## Build and test

```sh
cmake -S . -B build -DCMAKE_BUILD_TYPE=Release
cmake --build build
ctest --test-dir build --output-on-failure
```

## Use with CMake

After installing the project:

```cmake
find_package(lie-buffers CONFIG REQUIRED)
target_link_libraries(your_target PRIVATE lie_buffers::lie_buffers)
```

It can also be embedded with `add_subdirectory`; set
`LIE_BUFFERS_BUILD_TESTS=OFF` when the parent project should not build this
library's tests.

## License

MIT. See [LICENSE](LICENSE) and [THIRD_PARTY_NOTICES.md](THIRD_PARTY_NOTICES.md).
