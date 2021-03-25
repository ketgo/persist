cmake_minimum_required(VERSION 3.1)

# PoC binary name
set(POC_BINARY concurrency_manager_poc)

# Get source files
file(
    GLOB_RECURSE 
    SOURCES 
    ${CMAKE_CURRENT_SOURCE_DIR}/*.[hc]pp
    ${CMAKE_CURRENT_SOURCE_DIR}/*.[hc]
)

# Create executable
add_executable(
    ${POC_BINARY}
    ${SOURCES}
)

# Add tests folder to targets include directories
target_include_directories(
    ${POC_BINARY}
    PRIVATE
    ${CMAKE_CURRENT_SOURCE_DIR}
)

# Add libraries to link
target_link_libraries(
    ${POC_BINARY}
    gtest
    gmock
    tstest
)