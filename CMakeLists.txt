cmake_minimum_required(VERSION 3.14)

project(
  stamped-buffer
  VERSION 0.1.0
  DESCRIPTION "A small single-producer, multi-consumer stamped ring buffer"
  LANGUAGES CXX
)

include(GNUInstallDirs)
include(CMakePackageConfigHelpers)

add_library(stamped_buffer INTERFACE)
add_library(stamped_buffer::stamped_buffer ALIAS stamped_buffer)

target_compile_features(stamped_buffer INTERFACE cxx_std_17)
target_include_directories(
  stamped_buffer
  INTERFACE
    $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>
    $<INSTALL_INTERFACE:${CMAKE_INSTALL_INCLUDEDIR}>
)

if(CMAKE_SOURCE_DIR STREQUAL CMAKE_CURRENT_SOURCE_DIR)
  set(STAMPED_BUFFER_IS_TOP_LEVEL ON)
else()
  set(STAMPED_BUFFER_IS_TOP_LEVEL OFF)
endif()

option(STAMPED_BUFFER_BUILD_TESTS "Build stamped-buffer tests" ${STAMPED_BUFFER_IS_TOP_LEVEL})
option(STAMPED_BUFFER_BUILD_BENCHMARKS "Build stamped-buffer benchmarks" OFF)

if(STAMPED_BUFFER_BUILD_TESTS)
  enable_testing()
  add_subdirectory(tests)
endif()

if(STAMPED_BUFFER_BUILD_BENCHMARKS)
  add_subdirectory(benchmarks)
endif()

install(TARGETS stamped_buffer EXPORT stamped-bufferTargets)
install(DIRECTORY include/ DESTINATION ${CMAKE_INSTALL_INCLUDEDIR})
install(
  FILES LICENSE THIRD_PARTY_NOTICES.md
  DESTINATION "${CMAKE_INSTALL_DATAROOTDIR}/doc/stamped-buffer"
)

write_basic_package_version_file(
  "${CMAKE_CURRENT_BINARY_DIR}/stamped-bufferConfigVersion.cmake"
  VERSION ${PROJECT_VERSION}
  COMPATIBILITY SameMajorVersion
)

configure_package_config_file(
  "${CMAKE_CURRENT_SOURCE_DIR}/cmake/stamped-bufferConfig.cmake.in"
  "${CMAKE_CURRENT_BINARY_DIR}/stamped-bufferConfig.cmake"
  INSTALL_DESTINATION "${CMAKE_INSTALL_LIBDIR}/cmake/stamped-buffer"
)

install(
  EXPORT stamped-bufferTargets
  FILE stamped-bufferTargets.cmake
  NAMESPACE stamped_buffer::
  DESTINATION "${CMAKE_INSTALL_LIBDIR}/cmake/stamped-buffer"
)

install(
  FILES
    "${CMAKE_CURRENT_BINARY_DIR}/stamped-bufferConfig.cmake"
    "${CMAKE_CURRENT_BINARY_DIR}/stamped-bufferConfigVersion.cmake"
  DESTINATION "${CMAKE_INSTALL_LIBDIR}/cmake/stamped-buffer"
)
