cmake_minimum_required(VERSION 3.1)

# Add project version information
configure_file (
    "${CMAKE_CURRENT_SOURCE_DIR}/include/persist/version.hpp.in"
    "${CMAKE_CURRENT_SOURCE_DIR}/include/persist/version.hpp"
)

# Set C++ standrard to 17
set(CMAKE_CXX_STANDARD 17)

# Set compiler flags for code coverage
# set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -fprofile-arcs -ftest-coverage")

# Set all targets include directories
include_directories(
    ${CMAKE_CURRENT_SOURCE_DIR}/src
    ${CMAKE_CURRENT_SOURCE_DIR}/include
)

# Set project binary name
set(BINARY ${PROJECT_NAME})
# Set project lib name
set(LIB ${PROJECT_NAME}_lib)
# Set test binary name
set(TEST_BINARY ${PROJECT_NAME}_test)

# Coverage
include(CodeCoverage)
append_coverage_compiler_flags()
setup_target_for_coverage_lcov(
    NAME ${PROJECT_NAME}_cov 
    EXECUTABLE ${PROJECT_NAME}_test 
    DEPENDENCIES ${PROJECT_NAME} ${PROJECT_NAME}_test
    BASE_DIRECTORY coverage)
setup_target_for_coverage_clean(
    NAME scrub
)

# Source code
add_subdirectory(${CMAKE_CURRENT_SOURCE_DIR}/src)
# Tests
add_subdirectory(${CMAKE_CURRENT_SOURCE_DIR}/tests)
