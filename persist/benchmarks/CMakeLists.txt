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
    ${BENCHMARK_BINARY} 
    ${SOURCES}
)

# Add tests folder to targets include directories
target_include_directories(
    ${BENCHMARK_BINARY}
    PRIVATE
    ${CMAKE_CURRENT_SOURCE_DIR}
)

# Add libraries to link
target_link_libraries(
    ${BENCHMARK_BINARY}
    ${LIB}
    benchmark::benchmark
)

# Copy test data directory
add_custom_command(
    TARGET ${BENCHMARK_BINARY} PRE_BUILD
    COMMAND ${CMAKE_COMMAND} -E remove_directory $<TARGET_FILE_DIR:${BENCHMARK_BINARY}>/data
    COMMAND ${CMAKE_COMMAND} -E copy_directory  ${CMAKE_CURRENT_SOURCE_DIR}/data/ $<TARGET_FILE_DIR:${BENCHMARK_BINARY}>/data)
