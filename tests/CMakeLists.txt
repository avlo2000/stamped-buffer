include(FetchContent)

find_package(GTest CONFIG QUIET)
if(NOT TARGET GTest::gtest_main)
  FetchContent_Declare(
    googletest
    URL https://github.com/google/googletest/archive/refs/tags/v1.15.2.tar.gz
    DOWNLOAD_EXTRACT_TIMESTAMP TRUE
  )
  set(gtest_force_shared_crt ON CACHE BOOL "" FORCE)
  set(BUILD_GMOCK OFF CACHE BOOL "" FORCE)
  set(INSTALL_GTEST OFF CACHE BOOL "" FORCE)
  FetchContent_MakeAvailable(googletest)
endif()

find_package(Threads REQUIRED)
include(GoogleTest)

foreach(test_source IN ITEMS stamped_buffer_test.cpp stamped_buffer_mt_test.cpp)
  get_filename_component(test_name "${test_source}" NAME_WE)
  add_executable("${test_name}" "${test_source}")
  target_link_libraries(
    "${test_name}"
    PRIVATE stamped_buffer::stamped_buffer GTest::gtest_main Threads::Threads
  )
  gtest_discover_tests("${test_name}")
endforeach()
