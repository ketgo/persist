cmake_minimum_required(VERSION 3.1)

# Get source files
file(
    GLOB_RECURSE 
    SOURCES 
    ${CMAKE_CURRENT_SOURCE_DIR}/*.[hc]pp
    ${CMAKE_CURRENT_SOURCE_DIR}/*.[hc]
)

# Create executable
add_executable(
    ${TEST_BINARY} 
    ${SOURCES}
)

# Set build specific information in header for unit tests
get_target_property(DATA_PATH ${TEST_BINARY} BINARY_DIR)
set(DATA_PATH ${DATA_PATH}/data)
configure_file (
    "${CMAKE_CURRENT_SOURCE_DIR}/include/common.hpp.in"
    "${CMAKE_CURRENT_SOURCE_DIR}/include/common.hpp"
)

# Set include directories
target_include_directories(
    ${TEST_BINARY} 
    PRIVATE 
    ${CMAKE_CURRENT_SOURCE_DIR}/include
)

# Setup code coverage
target_code_coverage(${TEST_BINARY} AUTO)

# Add executable to run by ctest
# TODO: This runs all the tests as one test. Need to use gtest_Add_test function.
add_test(NAME ${TEST_BINARY} COMMAND ${TEST_BINARY})

# Add tests folder to targets include directories
target_include_directories(
    ${TEST_BINARY}
    PRIVATE
    ${CMAKE_CURRENT_SOURCE_DIR}
)

# Add libraries to link
target_link_libraries(
    ${TEST_BINARY}
    ${LIB}
    gtest
    gmock
    tstest
)

# Copy test data directory
add_custom_command(
    TARGET ${TEST_BINARY} PRE_BUILD
    COMMAND ${CMAKE_COMMAND} -E remove_directory ${DATA_PATH}
    COMMAND ${CMAKE_COMMAND} -E copy_directory  ${CMAKE_CURRENT_SOURCE_DIR}/data/ ${DATA_PATH})
