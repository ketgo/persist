cmake_minimum_required(VERSION 3.1)

# Update git submodules
include(GitSubmodules)

# List of dependencies
add_subdirectory(googletest)
add_subdirectory(benchmark)
add_subdirectory(loguru)
