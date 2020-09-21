cmake_minimum_required(VERSION 2.8.9)

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
    boost_thread-mt
    boost_log-mt
    boost_log_setup-mt
)

# Set compiler flags
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -Wall -Wextra -ggdb -DBOOST_LOG_DYN_LINK")
