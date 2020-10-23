cmake_minimum_required(VERSION 3.1)

# Get source files
file(
    GLOB_RECURSE 
    SOURCES 
    ${CMAKE_CURRENT_SOURCE_DIR}/*.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/*.c
)

# Create executable
add_executable(
    ${TEST_BINARY} 
    ${SOURCES}
)

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
    loguru::loguru
)

# Copy test data directory
add_custom_command(
    TARGET ${TEST_BINARY} PRE_BUILD
    COMMAND ${CMAKE_COMMAND} -E remove_directory $<TARGET_FILE_DIR:${TEST_BINARY}>/data
    COMMAND ${CMAKE_COMMAND} -E copy_directory  ${CMAKE_CURRENT_SOURCE_DIR}/data/ $<TARGET_FILE_DIR:${TEST_BINARY}>/data)
