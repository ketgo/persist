cmake_minimum_required(VERSION 2.8.9)

# Set C++ standrard to 17
set(CMAKE_CXX_STANDARD 17)

# Set common compiler flags
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -Wall -Wextra -ggdb")

# Set compiler flags for code coverage
# set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -fprofile-arcs -ftest-coverage")

# Set all targets include directories
include_directories(
    ${CMAKE_CURRENT_SOURCE_DIR}/src
    ${CMAKE_CURRENT_SOURCE_DIR}/include
    /usr/local/include
)

# Set lib directories for all targets
link_directories(
    ${CMAKE_CURRENT_SOURCE_DIR}/lib
    /usr/local/lib
)

# Source code
add_subdirectory(${CMAKE_CURRENT_SOURCE_DIR}/src)
# Tests
add_subdirectory(${CMAKE_CURRENT_SOURCE_DIR}/tests)
