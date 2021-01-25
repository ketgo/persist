cmake_minimum_required(VERSION 3.1)

# Add project version information
configure_file (
    "${CMAKE_CURRENT_SOURCE_DIR}/include/persist/version.hpp.in"
    "${CMAKE_CURRENT_SOURCE_DIR}/include/persist/version.hpp"
)

# Set C++ standrard to 14
set(CMAKE_CXX_STANDARD 14)

# Set project lib name
set(LIB ${PROJECT_NAME}_lib)
# Set test binary name
set(TEST_BINARY ${PROJECT_NAME}_test)
# Set test binary name
set(BENCHMARK_BINARY ${PROJECT_NAME}_benchmark)

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

# Get source files
file(
    GLOB_RECURSE 
    SOURCES 
    ${CMAKE_CURRENT_SOURCE_DIR}/src/*.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/src/*.c
)

# Create static lib for running test suits
add_library(
    ${LIB} 
    STATIC 
    ${SOURCES}
)

# Add include directories
target_include_directories(
    ${LIB} 
    PUBLIC 
    ${CMAKE_CURRENT_SOURCE_DIR}/include
)

# Add libraries to link for lib
if (${CMAKE_BUILD_TYPE} MATCHES Debug)
target_link_libraries(
    ${LIB} 
    loguru::loguru
)
else()
target_link_libraries(
    ${LIB} 
)
endif()

# Set compiler flags
if (${CMAKE_BUILD_TYPE} MATCHES Debug)
    target_compile_definitions(${LIB} PUBLIC __PERSIST_DEBUG__)
endif()

# Tests
add_subdirectory(${CMAKE_CURRENT_SOURCE_DIR}/tests)
# Benchmarks
add_subdirectory(${CMAKE_CURRENT_SOURCE_DIR}/benchmarks)
