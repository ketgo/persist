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
set(BINARY ${PROJECT_NAME})

# Create executable
add_executable(
    ${BINARY} 
    ${SOURCES}
)

# Create static lib for running test suits
add_library(
    ${BINARY}_lib 
    STATIC 
    ${SOURCES}
)

# Add libraries to link
target_link_libraries(
    ${BINARY}
    nlohmann_json::nlohmann_json
)

# Set compiler flags
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -DBOOST_LOG_DYN_LINK")
