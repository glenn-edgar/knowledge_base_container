/*
 * test_common.h
 * Knowledge Base C Port — Lightweight test macros
 */

#ifndef TEST_COMMON_H
#define TEST_COMMON_H

#include <stdio.h>
#include <stdlib.h>
#include <string.h>

static int _test_pass_count = 0;
static int _test_fail_count = 0;

#define TEST_BEGIN(name) \
    printf("\n== TEST: %s ==\n", name)

#define TEST_END() do { \
    printf("\n== RESULTS: %d passed, %d failed ==\n", \
           _test_pass_count, _test_fail_count); \
    return _test_fail_count > 0 ? 1 : 0; \
} while(0)

#define ASSERT_OK(rc, msg) do { \
    if ((rc) == KB_OK) { \
        _test_pass_count++; \
        printf("  PASS: %s\n", msg); \
    } else { \
        _test_fail_count++; \
        printf("  FAIL: %s (rc=%d: %s)\n", msg, (rc), kb_error_str(rc)); \
    } \
} while(0)

#define ASSERT_ERR(rc, expected, msg) do { \
    if ((rc) == (expected)) { \
        _test_pass_count++; \
        printf("  PASS: %s\n", msg); \
    } else { \
        _test_fail_count++; \
        printf("  FAIL: %s (expected %d, got %d)\n", msg, (expected), (rc)); \
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
