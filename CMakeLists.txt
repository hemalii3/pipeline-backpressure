cmake_minimum_required(VERSION 3.16)
project(pipeline_backpressure LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_EXPORT_COMPILE_COMMANDS ON)  # for clangd / clang-tidy

if(NOT CMAKE_BUILD_TYPE)
  set(CMAKE_BUILD_TYPE Release)
endif()
set(CMAKE_CXX_FLAGS_RELEASE "-O3 -DNDEBUG")
set(CMAKE_CXX_FLAGS_DEBUG "-O0 -g")

# --- Sanitizers -------------------------------------------------------
# Usage: cmake .. -DSANITIZE=thread   (or address, undefined, address+undefined)
# TSan is the one that actually matters for this project -- it's how you
# validate the lock-free queue isn't secretly racy. Run the test suite
# under -DSANITIZE=thread before trusting any lock-free result.
set(SANITIZE "" CACHE STRING "Sanitizer to build with: thread, address, undefined, or address+undefined")
if(SANITIZE STREQUAL "thread")
  add_compile_options(-fsanitize=thread -g -O1)
  add_link_options(-fsanitize=thread)
elseif(SANITIZE STREQUAL "address")
  add_compile_options(-fsanitize=address -g -O1)
  add_link_options(-fsanitize=address)
elseif(SANITIZE STREQUAL "undefined")
  add_compile_options(-fsanitize=undefined -g -O1)
  add_link_options(-fsanitize=undefined)
elseif(SANITIZE STREQUAL "address+undefined")
  add_compile_options(-fsanitize=address,undefined -g -O1)
  add_link_options(-fsanitize=address,undefined)
elseif(NOT SANITIZE STREQUAL "")
  message(FATAL_ERROR "Unknown SANITIZE value: ${SANITIZE}")
endif()

find_package(Threads REQUIRED)

include_directories(${PROJECT_SOURCE_DIR}/include)

# --- GoogleTest ---------------------------------------------------------
include(FetchContent)
FetchContent_Declare(
  googletest
  URL https://github.com/google/googletest/archive/refs/tags/v1.15.2.zip
)
# Avoid overriding parent project's compiler/linker settings on Windows (harmless elsewhere)
set(gtest_force_shared_crt ON CACHE BOOL "" FORCE)
FetchContent_MakeAvailable(googletest)

enable_testing()

add_subdirectory(src)
add_subdirectory(tests)
add_subdirectory(benchmarks)
