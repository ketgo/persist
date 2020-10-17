cmake_minimum_required(VERSION 3.1)

# Add project version information
configure_file (
    "${CMAKE_CURRENT_SOURCE_DIR}/include/persist/version.hpp.in"
    "${CMAKE_CURRENT_SOURCE_DIR}/include/persist/version.hpp"
)

# Set C++ standrard to 14
set(CMAKE_CXX_STANDARD 14)

# Set all targets include directories
include_directories(
    ${CMAKE_CURRENT_SOURCE_DIR}/src
    ${CMAKE_CURRENT_SOURCE_DIR}/include
)

# Set project lib name
set(LIB ${PROJECT_NAME}_lib)
# Set test binary name
set(TEST_BINARY ${PROJECT_NAME}_test)

# Coverage
if (COVERAGE)
    include(CodeCoverage)
    append_coverage_compiler_flags()
    setup_target_for_coverage_lcov(
        NAME ${PROJECT_NAME}_cov 
        EXECUTABLE ${TEST_BINARY} 
        DEPENDENCIES ${LIB} ${TEST_BINARY}
        EXCLUDE "third_party/*" "/*usr/*")
    setup_target_for_coverage_clean(
        NAME scrub
    )
endif()

# Source code
add_subdirectory(${CMAKE_CURRENT_SOURCE_DIR}/src)
# Tests
add_subdirectory(${CMAKE_CURRENT_SOURCE_DIR}/tests)
