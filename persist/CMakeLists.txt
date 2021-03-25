cmake_minimum_required(VERSION 3.1)

# Add project version information
configure_file (
    "${CMAKE_CURRENT_SOURCE_DIR}/include/persist/version.hpp.in"
    "${CMAKE_CURRENT_SOURCE_DIR}/include/persist/version.hpp"
)

include(c++-standards)
include(code-coverage)
include(formatting)
include(sanitizers)
include(tools)

# Require C++14
cxx_14()

# Cpp checks for code quality
cppcheck(
    --enable=warning,performance,portability,missingInclude
  --template="[{severity}][{id}] {message} {callstack} \(On {file}:{line}\)"
  --suppress=missingIncludeSystem --quiet --verbose --force
)

# Set project lib name
set(LIB ${PROJECT_NAME}_lib)
# Set test binary name
set(TEST_BINARY ${PROJECT_NAME}_test)
# Set test binary name
set(BENCHMARK_BINARY ${PROJECT_NAME}_benchmark)

# Get source files
file(
    GLOB_RECURSE 
    SOURCES 
    ${CMAKE_CURRENT_SOURCE_DIR}/src/*.[hc]pp
    ${CMAKE_CURRENT_SOURCE_DIR}/src/*.[hc]
)

# If source files found then make static lib else create header-only interface lib
if(${SOURCES})
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
            # NOTE: Add any required dependencies here
        )
        target_compile_definitions(${LIB} PUBLIC __PERSIST_DEBUG__)
    else()
        target_link_libraries(
            ${LIB} 
            # NOTE: Add any required dependencies here
        )
    endif()
else()
    # Create interface lib for running test suits
    add_library(${LIB} INTERFACE)
    # Add include directories
    target_include_directories(${LIB} INTERFACE ${CMAKE_CURRENT_SOURCE_DIR}/include)
    # Set compiler flags
    if (${CMAKE_BUILD_TYPE} MATCHES Debug)
        target_compile_definitions(${LIB} INTERFACE __PERSIST_DEBUG__)
    endif()
endif()

# PoCs
add_subdirectory(${CMAKE_CURRENT_SOURCE_DIR}/pocs)
# Tests
add_subdirectory(${CMAKE_CURRENT_SOURCE_DIR}/tests)
# Benchmarks
add_subdirectory(${CMAKE_CURRENT_SOURCE_DIR}/benchmarks)
