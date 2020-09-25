cmake_minimum_required(VERSION 3.1)

# Add project version information
configure_file (
    "${CMAKE_CURRENT_SOURCE_DIR}/include/persist/version.hpp.in"
    "${CMAKE_CURRENT_SOURCE_DIR}/include/persist/version.hpp"
)

# Set C++ standrard to 14
set(CMAKE_CXX_STANDARD 14)

# Set compiler flags for code coverage
# set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -fprofile-arcs -ftest-coverage")

# Set all targets include directories
include_directories(
    ${CMAKE_CURRENT_SOURCE_DIR}/src
    ${CMAKE_CURRENT_SOURCE_DIR}/include
)

# Source code
add_subdirectory(${CMAKE_CURRENT_SOURCE_DIR}/src)
# Tests
add_subdirectory(${CMAKE_CURRENT_SOURCE_DIR}/tests)
