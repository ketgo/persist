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
    ${BINARY} 
    ${SOURCES}
)

# Create static lib for running test suits
add_library(
    ${LIB} 
    STATIC 
    ${SOURCES}
)

# Add libraries to link for binary
target_link_libraries(
    ${BINARY}
    PRIVATE 
    nlohmann_json::nlohmann_json
)

# Add libraries to link for lib
target_link_libraries(
    ${LIB}
    PRIVATE 
    nlohmann_json::nlohmann_json
)

# Set compiler flags
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -DBOOST_LOG_DYN_LINK")
