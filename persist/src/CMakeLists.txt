cmake_minimum_required(VERSION 3.1)

# Get source files
file(
    GLOB_RECURSE 
    SOURCES 
    ${CMAKE_CURRENT_SOURCE_DIR}/*.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/*.c
)

# Create static lib for running test suits
add_library(
    ${LIB} 
    STATIC 
    ${SOURCES}
)

# Add libraries to link for lib
if (${CMAKE_BUILD_TYPE} MATCHES Debug)
target_link_libraries(
    ${LIB} 
    nlohmann_json::nlohmann_json
    loguru::loguru
)
else()
target_link_libraries(
    ${LIB} 
    nlohmann_json::nlohmann_json
)
endif()

# Set compiler flags
if (${CMAKE_BUILD_TYPE} MATCHES Debug)
    target_compile_definitions(${LIB} PUBLIC __PERSIST_DEBUG__)
endif()
