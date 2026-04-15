cmake_minimum_required(VERSION 3.20)
project(ysql LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_EXPORT_COMPILE_COMMANDS ON)

# 라이브러리
add_library(ysql
    src/page.cpp
    src/pager.cpp
    src/btree.cpp
)
target_include_directories(ysql PUBLIC .)

# 테스트
add_executable(test_pager tests/test_pager.cpp)
target_link_libraries(test_pager PRIVATE ysql)
