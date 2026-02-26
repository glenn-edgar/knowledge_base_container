cmake_minimum_required(VERSION 3.16)
project(knowledge_base_c C)

set(CMAKE_C_STANDARD 11)
set(CMAKE_C_STANDARD_REQUIRED ON)

# ── Find SQLite3 ──
find_package(SQLite3 REQUIRED)

# ── cJSON (vendored) ──
add_library(cjson STATIC third_party/cJSON/cJSON.c)
target_include_directories(cjson PUBLIC third_party/cJSON)

# ── libkb (static + shared) ──
file(GLOB KB_SOURCES src/*.c)

add_library(kb_static STATIC ${KB_SOURCES})
target_include_directories(kb_static PUBLIC include)
target_link_libraries(kb_static PUBLIC SQLite::SQLite3 cjson m)

add_library(kb_shared SHARED ${KB_SOURCES})
target_include_directories(kb_shared PUBLIC include)
target_link_libraries(kb_shared PUBLIC SQLite::SQLite3 cjson m)

# ── Tests ──
enable_testing()

set(TESTS
    test_kb_data_structures
    test_kb_query_support
    test_bit_s_expression
)

foreach(test ${TESTS})
    add_executable(${test} tests/${test}.c)
    target_include_directories(${test} PRIVATE include tests third_party/cJSON)
    target_link_libraries(${test} PRIVATE kb_static)
    add_test(NAME ${test} COMMAND ${test})
endforeach()
