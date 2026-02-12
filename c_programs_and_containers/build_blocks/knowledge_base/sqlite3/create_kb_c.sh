#!/bin/bash
# create_kb_c_structure.sh
# Creates the knowledge_base_c directory structure with empty shell files
# Usage: bash create_kb_c_structure.sh [base_dir]
#   base_dir defaults to current directory

BASE_DIR="${1:-.}/knowledge_base_c_a"

echo "Creating knowledge_base_c directory structure in: $BASE_DIR"

# ── Create directories ──────────────────────────────────────
mkdir -p "$BASE_DIR/include"
mkdir -p "$BASE_DIR/src"
mkdir -p "$BASE_DIR/tests"
mkdir -p "$BASE_DIR/third_party/cJSON"

# ── Include headers ─────────────────────────────────────────
HEADERS=(
    kb_common.h
    kb_json.h
    kb_uuid.h
    kb_query_support.h
    bit_mask_rt_operations.h
    bit_s_expression.h
    kb_bit_structures.h
    kb_status_table.h
    kb_stream.h
    kb_job_queue.h
    kb_link_table.h
    kb_link_mount_table.h
    kb_rpc_server.h
    kb_rpc_client.h
    kb_data_structures.h
)

for f in "${HEADERS[@]}"; do
    GUARD=$(echo "$f" | tr '[:lower:].' '[:upper:]_')
    cat > "$BASE_DIR/include/$f" << EOF
#ifndef ${GUARD}
#define ${GUARD}

/*
 * $f
 * Knowledge Base C Port
 * Paste implementation here
 */

#include <sqlite3.h>

#endif /* ${GUARD} */
EOF
    echo "  created include/$f"
done

# ── Source files ────────────────────────────────────────────
SOURCES=(
    kb_common.c
    kb_json.c
    kb_uuid.c
    kb_query_support.c
    bit_mask_rt_operations.c
    bit_s_expression.c
    kb_bit_structures.c
    kb_status_table.c
    kb_stream.c
    kb_job_queue.c
    kb_link_table.c
    kb_link_mount_table.c
    kb_rpc_server.c
    kb_rpc_client.c
    kb_data_structures.c
)

for f in "${SOURCES[@]}"; do
    HEADER="${f%.c}.h"
    cat > "$BASE_DIR/src/$f" << EOF
/*
 * $f
 * Knowledge Base C Port
 * Paste implementation here
 */

#include "$HEADER"
#include "kb_common.h"
EOF
    echo "  created src/$f"
done

# ── Test files ──────────────────────────────────────────────
cat > "$BASE_DIR/tests/test_common.h" << 'EOF'
#ifndef TEST_COMMON_H
#define TEST_COMMON_H

/*
 * test_common.h
 * Lightweight test macros for knowledge_base_c tests
 */

#include <stdio.h>
#include <stdlib.h>
#include <string.h>

static int _test_pass_count = 0;
static int _test_fail_count = 0;

#define TEST_BEGIN(name) \
    printf("── TEST: %s ──\n", name)

#define TEST_END() \
    printf("\n── RESULTS: %d passed, %d failed ──\n", \
           _test_pass_count, _test_fail_count)

#define ASSERT_OK(rc, msg) do { \
    if ((rc) == KB_OK) { \
        _test_pass_count++; \
        printf("  PASS: %s\n", msg); \
    } else { \
        _test_fail_count++; \
        printf("  FAIL: %s (rc=%d)\n", msg, (rc)); \
    } \
} while(0)

#define ASSERT_EQ_INT(a, b, msg) do { \
    if ((a) == (b)) { \
        _test_pass_count++; \
        printf("  PASS: %s\n", msg); \
    } else { \
        _test_fail_count++; \
        printf("  FAIL: %s (expected %d, got %d)\n", msg, (int)(b), (int)(a)); \
    } \
} while(0)

#define ASSERT_EQ_STR(a, b, msg) do { \
    if ((a) && (b) && strcmp((a),(b)) == 0) { \
        _test_pass_count++; \
        printf("  PASS: %s\n", msg); \
    } else { \
        _test_fail_count++; \
        printf("  FAIL: %s (expected \"%s\", got \"%s\")\n", \
               msg, (b) ? (b) : "NULL", (a) ? (a) : "NULL"); \
    } \
} while(0)

#define ASSERT_NOT_NULL(ptr, msg) do { \
    if ((ptr) != NULL) { \
        _test_pass_count++; \
        printf("  PASS: %s\n", msg); \
    } else { \
        _test_fail_count++; \
        printf("  FAIL: %s (got NULL)\n", msg); \
    } \
} while(0)

#define ASSERT_NULL(ptr, msg) do { \
    if ((ptr) == NULL) { \
        _test_pass_count++; \
        printf("  PASS: %s\n", msg); \
    } else { \
        _test_fail_count++; \
        printf("  FAIL: %s (expected NULL)\n", msg); \
    } \
} while(0)

#define ASSERT_TRUE(cond, msg) do { \
    if ((cond)) { \
        _test_pass_count++; \
        printf("  PASS: %s\n", msg); \
    } else { \
        _test_fail_count++; \
        printf("  FAIL: %s\n", msg); \
    } \
} while(0)

#endif /* TEST_COMMON_H */
EOF
echo "  created tests/test_common.h"

TESTS=(
    test_kb_query_support.c
    test_bit_s_expression.c
    test_kb_data_structures.c
)

for f in "${TESTS[@]}"; do
    cat > "$BASE_DIR/tests/$f" << EOF
/*
 * $f
 * Knowledge Base C Port — Test Driver
 * Paste implementation here
 */

#include <stdio.h>
#include "test_common.h"
#include "kb_common.h"

int main(int argc, char *argv[]) {
    printf("$f — placeholder\\n");
    return 0;
}
EOF
    echo "  created tests/$f"
done

# ── Test Makefile ───────────────────────────────────────────
cat > "$BASE_DIR/tests/Makefile" << 'EOF'
# Tests Makefile
CC       = gcc
CFLAGS   = -Wall -Wextra -g -I../include -I../third_party/cJSON
LDFLAGS  = -lsqlite3 -lm

SRC_DIR  = ../src
SRCS     = $(wildcard $(SRC_DIR)/*.c) ../third_party/cJSON/cJSON.c

TESTS    = test_kb_data_structures test_kb_query_support test_bit_s_expression

all: $(TESTS)

test_kb_data_structures: test_kb_data_structures.c $(SRCS)
	$(CC) $(CFLAGS) -o $@ $^ $(LDFLAGS)

test_kb_query_support: test_kb_query_support.c $(SRCS)
	$(CC) $(CFLAGS) -o $@ $^ $(LDFLAGS)

test_bit_s_expression: test_bit_s_expression.c $(SRCS)
	$(CC) $(CFLAGS) -o $@ $^ $(LDFLAGS)

run: $(TESTS)
	@for t in $(TESTS); do echo "── Running $$t ──"; ./$$t; echo; done

clean:
	rm -f $(TESTS)

.PHONY: all run clean
EOF
echo "  created tests/Makefile"

# ── Top-level Makefile ──────────────────────────────────────
cat > "$BASE_DIR/Makefile" << 'EOF'
# knowledge_base_c top-level Makefile
CC       = gcc
AR       = ar
CFLAGS   = -Wall -Wextra -O2 -Iinclude -Ithird_party/cJSON
LDFLAGS  = -lsqlite3 -lm

SRC_DIR  = src
OBJ_DIR  = obj
LIB_DIR  = lib

SRCS     = $(wildcard $(SRC_DIR)/*.c) third_party/cJSON/cJSON.c
OBJS     = $(patsubst %.c,$(OBJ_DIR)/%.o,$(notdir $(SRCS)))

STATIC   = $(LIB_DIR)/libkb.a
SHARED   = $(LIB_DIR)/libkb.so

all: dirs $(STATIC) $(SHARED)

dirs:
	@mkdir -p $(OBJ_DIR) $(LIB_DIR)

$(OBJ_DIR)/%.o: $(SRC_DIR)/%.c
	$(CC) $(CFLAGS) -fPIC -c $< -o $@

$(OBJ_DIR)/cJSON.o: third_party/cJSON/cJSON.c
	$(CC) $(CFLAGS) -fPIC -c $< -o $@

$(STATIC): $(OBJS)
	$(AR) rcs $@ $^

$(SHARED): $(OBJS)
	$(CC) -shared -o $@ $^ $(LDFLAGS)

tests:
	$(MAKE) -C tests

run-tests:
	$(MAKE) -C tests run

clean:
	rm -rf $(OBJ_DIR) $(LIB_DIR)
	$(MAKE) -C tests clean

.PHONY: all dirs tests run-tests clean
EOF
echo "  created Makefile"

# ── CMakeLists.txt ──────────────────────────────────────────
cat > "$BASE_DIR/CMakeLists.txt" << 'EOF'
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
EOF
echo "  created CMakeLists.txt"

# ── Third-party placeholders ───────────────────────────────
cat > "$BASE_DIR/third_party/cJSON/cJSON.h" << 'EOF'
/* Placeholder — download cJSON from https://github.com/DaveGamble/cJSON */
/* Copy cJSON.h and cJSON.c into this directory */
#error "Replace this placeholder with the real cJSON.h"
EOF

cat > "$BASE_DIR/third_party/cJSON/cJSON.c" << 'EOF'
/* Placeholder — download cJSON from https://github.com/DaveGamble/cJSON */
#error "Replace this placeholder with the real cJSON.c"
EOF
echo "  created third_party/cJSON placeholders"

cat > "$BASE_DIR/third_party/README.md" << 'EOF'
# Third-Party Dependencies

## cJSON
- Source: https://github.com/DaveGamble/cJSON
- License: MIT
- Files needed: `cJSON.h`, `cJSON.c`
- Download and copy into `third_party/cJSON/`

## SQLite3
- System library: install via `apt install libsqlite3-dev` (Debian/Ubuntu)
- Minimum version: 3.30+ (for `COUNT(*) FILTER (WHERE ...)`)

## ltree extension
- Required for path-based queries
- Must be loadable via `sqlite3_load_extension()`
EOF
echo "  created third_party/README.md"

# ── Top-level README ────────────────────────────────────────
cat > "$BASE_DIR/README.md" << 'EOF'
# Knowledge Base C Port

C client library port of the LuaJIT Knowledge Base runtime operations.

## Build

```bash
# GNU Make
make            # builds lib/libkb.a and lib/libkb.so
make tests      # builds test executables
make run-tests  # builds and runs all tests

# CMake
mkdir build && cd build
cmake ..
make
ctest
```

## Dependencies
- GCC or Clang (C11)
- SQLite 3.30+ with dev headers
- ltree SQLite extension
- cJSON (vendored in third_party/)

## Structure
- `include/` — Public headers
- `src/`     — Implementation
- `tests/`   — Test drivers
- `lib/`     — Built libraries (created by make)

## API Mapping from LuaJIT
See the continuation guide markdown for the full LuaJIT → C API translation table.
EOF
echo "  created README.md"

# ── Summary ─────────────────────────────────────────────────
echo ""
echo "Done! Structure created:"
echo ""
find "$BASE_DIR" -type f | sort | sed "s|$BASE_DIR/|  |"
echo ""
echo "File counts:"
echo "  Headers:  $(find "$BASE_DIR/include" -name '*.h' | wc -l)"
echo "  Sources:  $(find "$BASE_DIR/src" -name '*.c' | wc -l)"
echo "  Tests:    $(find "$BASE_DIR/tests" -name '*.c' -o -name '*.h' | wc -l)"
echo "  Build:    3 (Makefile, tests/Makefile, CMakeLists.txt)"
echo "  Total:    $(find "$BASE_DIR" -type f | wc -l)"
