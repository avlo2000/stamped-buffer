if(NOT CMAKE_SYSTEM_NAME STREQUAL "Linux" OR NOT CMAKE_CXX_COMPILER_ID MATCHES "GNU|Clang")
  message(FATAL_ERROR "The optional benchmarks require Linux and GCC or Clang")
endif()

find_package(Threads REQUIRED)
add_executable(stamped_buffer_benchmark stamped_buffer_benchmark.cpp)
target_link_libraries(stamped_buffer_benchmark PRIVATE stamped_buffer::stamped_buffer Threads::Threads)
target_compile_options(stamped_buffer_benchmark PRIVATE -Wall -Wextra -Wpedantic)

option(STAMPED_BUFFER_BENCHMARK_NATIVE "Tune benchmarks for the build machine" OFF)
if(STAMPED_BUFFER_BENCHMARK_NATIVE)
  target_compile_options(stamped_buffer_benchmark PRIVATE -march=native)
endif()
