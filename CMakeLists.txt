cmake_minimum_required(VERSION 3.14)

project(
  lie-buffers
  VERSION 0.1.0
  DESCRIPTION "A small single-producer, multi-consumer stamped ring buffer"
  LANGUAGES CXX
)

include(GNUInstallDirs)
include(CMakePackageConfigHelpers)

add_library(lie_buffers INTERFACE)
add_library(lie_buffers::lie_buffers ALIAS lie_buffers)

target_compile_features(lie_buffers INTERFACE cxx_std_17)
target_include_directories(
  lie_buffers
  INTERFACE
    $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>
    $<INSTALL_INTERFACE:${CMAKE_INSTALL_INCLUDEDIR}>
)

if(CMAKE_SOURCE_DIR STREQUAL CMAKE_CURRENT_SOURCE_DIR)
  set(LIE_BUFFERS_IS_TOP_LEVEL ON)
else()
  set(LIE_BUFFERS_IS_TOP_LEVEL OFF)
endif()

option(LIE_BUFFERS_BUILD_TESTS "Build lie-buffers tests" ${LIE_BUFFERS_IS_TOP_LEVEL})

if(LIE_BUFFERS_BUILD_TESTS)
  enable_testing()
  add_subdirectory(tests)
endif()

install(TARGETS lie_buffers EXPORT lie-buffersTargets)
install(DIRECTORY include/ DESTINATION ${CMAKE_INSTALL_INCLUDEDIR})
install(
  FILES LICENSE THIRD_PARTY_NOTICES.md
  DESTINATION "${CMAKE_INSTALL_DATAROOTDIR}/doc/lie-buffers"
)

write_basic_package_version_file(
  "${CMAKE_CURRENT_BINARY_DIR}/lie-buffersConfigVersion.cmake"
  VERSION ${PROJECT_VERSION}
  COMPATIBILITY SameMajorVersion
)

configure_package_config_file(
  "${CMAKE_CURRENT_SOURCE_DIR}/cmake/lie-buffersConfig.cmake.in"
  "${CMAKE_CURRENT_BINARY_DIR}/lie-buffersConfig.cmake"
  INSTALL_DESTINATION "${CMAKE_INSTALL_LIBDIR}/cmake/lie-buffers"
)

install(
  EXPORT lie-buffersTargets
  FILE lie-buffersTargets.cmake
  NAMESPACE lie_buffers::
  DESTINATION "${CMAKE_INSTALL_LIBDIR}/cmake/lie-buffers"
)

install(
  FILES
    "${CMAKE_CURRENT_BINARY_DIR}/lie-buffersConfig.cmake"
    "${CMAKE_CURRENT_BINARY_DIR}/lie-buffersConfigVersion.cmake"
  DESTINATION "${CMAKE_INSTALL_LIBDIR}/cmake/lie-buffers"
)
