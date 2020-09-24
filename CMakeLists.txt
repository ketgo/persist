cmake_minimum_required(VERSION 3.1)

project(persist LANGUAGES CXX)

# Add package modules to cmake module path
list(APPEND CMAKE_MODULE_PATH ${PROJECT_SOURCE_DIR}/cmake)

# Enable testing
enable_testing()

add_subdirectory(${CMAKE_CURRENT_SOURCE_DIR}/persist)
