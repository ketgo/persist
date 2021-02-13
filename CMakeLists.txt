cmake_minimum_required(VERSION 3.1)

project(persist VERSION 0.0.1 LANGUAGES CXX)

# Add package modules to cmake module path
list(APPEND CMAKE_MODULE_PATH ${PROJECT_SOURCE_DIR}/cmake)

# Enable testing
enable_testing()

add_subdirectory(${PROJECT_SOURCE_DIR}/third_party)
add_subdirectory(${PROJECT_SOURCE_DIR}/persist)
