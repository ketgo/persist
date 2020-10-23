cmake_minimum_required(VERSION 3.1)
project(loguru LANGUAGES CXX)

add_library(loguru STATIC ${CMAKE_CURRENT_SOURCE_DIR}/loguru/loguru.cpp)
add_library(loguru::loguru ALIAS loguru)
target_include_directories(loguru
  PUBLIC ${CMAKE_CURRENT_SOURCE_DIR}/loguru
)
target_compile_features(loguru PRIVATE cxx_std_11)
find_package(Threads)
target_link_libraries(loguru PRIVATE ${CMAKE_THREAD_LIBS_INIT} dl)