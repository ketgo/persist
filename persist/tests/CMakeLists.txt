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
)
