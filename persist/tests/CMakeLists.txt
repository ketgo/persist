cmake_minimum_required(VERSION 3.1)

# Check for dependencies
include(CMakeLists.txt.in)

# Get source files
file(
    GLOB_RECURSE 
    SOURCES 
    ${CMAKE_CURRENT_SOURCE_DIR}/*.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/*.c
)

# Set binary name
set(BINARY ${PROJECT_NAME}_test)

# Create executable
add_executable(
    ${BINARY} 
    ${SOURCES}
)

# Add executable to run by ctest
add_test(NAME ${BINARY} COMMAND ${BINARY})

# Add tests folder to targets include directories
target_include_directories(
    ${BINARY}
    PRIVATE
    ${CMAKE_CURRENT_SOURCE_DIR}
)

# Add libraries to link
target_link_libraries(
    ${BINARY}
    ${PROJECT_NAME}_lib
    ${GTEST_LIBRARIES}
)
