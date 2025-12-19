/* ============= UNIT TEST: cfl_perm.c ============= */
#include "cfl_perm.h"
#include <stdio.h>
#include <string.h>
#include <assert.h>

/* Test framework globals */
static int tests_run = 0;
static int tests_passed = 0;
static int tests_failed = 0;

/* Color codes for output */
#define COLOR_GREEN  "\033[0;32m"
#define COLOR_RED    "\033[0;31m"
#define COLOR_YELLOW "\033[0;33m"
#define COLOR_RESET  "\033[0m"
#define COLOR_CYAN   "\033[0;36m"

/* Test macros */
#define TEST_START(name) \
    do { \
        tests_run++; \
        printf(COLOR_CYAN "\n[TEST %d] %s\n" COLOR_RESET, tests_run, name); \
    } while(0)

#define TEST_ASSERT(condition, message) \
    do { \
        if (!(condition)) { \
            printf(COLOR_RED "  ✗ FAIL: %s\n" COLOR_RESET, message); \
            printf("    Line %d: %s\n", __LINE__, #condition); \
            tests_failed++; \
            return; \
        } \
    } while(0)

#define TEST_PASS() \
    do { \
        printf(COLOR_GREEN "  ✓ PASS\n" COLOR_RESET); \
        tests_passed++; \
    } while(0)

#define TEST_SECTION(name) \
    printf(COLOR_YELLOW "  [%s]\n" COLOR_RESET, name)

/* ============= TEST FUNCTIONS ============= */

void test_create_destroy(void) {
    TEST_START("Create and Destroy");
    
    CflPerm* perm = cfl_perm_create();
    TEST_ASSERT(perm != NULL, "Create should return non-NULL pointer");
    TEST_ASSERT(perm->initialized == false, "Should be uninitialized");
    TEST_ASSERT(perm->pool == NULL, "Pool should be NULL");
    TEST_ASSERT(perm->owns_pool == false, "Should not own pool");
    
    cfl_perm_destroy(perm);
    
    TEST_PASS();
}

void test_malloc_create_destroy(void) {
    TEST_START("Malloc Create and Destroy");
    
    uint16_t pool_size = 1024;
    cfl_perm_t* perm = cfl_perm_malloc_create(pool_size);
    
    TEST_ASSERT(perm != NULL, "Malloc create should return non-NULL");
    TEST_ASSERT(perm->pool != NULL, "Pool should be allocated");
    TEST_ASSERT(perm->initialized == true, "Should be initialized");
    TEST_ASSERT(perm->owns_pool == true, "Should own pool");
    TEST_ASSERT(perm->pool_size == pool_size, "Pool size should match");
    TEST_ASSERT(perm->used == 0, "Should start with 0 used bytes");
    
    cfl_perm_malloc_destroy(perm);
    
    TEST_PASS();
}

void test_init_external_buffer(void) {
    TEST_START("Init with External Buffer");
    
    uint8_t buffer[512];
    CflPerm perm;
    
    cfl_perm_init(&perm, buffer, sizeof(buffer));
    
    TEST_ASSERT(perm.initialized == true, "Should be initialized");
    TEST_ASSERT(perm.pool == buffer, "Pool should point to buffer");
    TEST_ASSERT(perm.pool_size == sizeof(buffer), "Pool size should match");
    TEST_ASSERT(perm.used == 0, "Should start with 0 used bytes");
    TEST_ASSERT(perm.owns_pool == false, "Should not own pool");
    
    /* Verify buffer is zeroed */
    bool all_zero = true;
    for (int i = 0; i < sizeof(buffer); i++) {
        if (buffer[i] != 0) {
            all_zero = false;
            break;
        }
    }
    TEST_ASSERT(all_zero, "Buffer should be zeroed on init");
    
    TEST_PASS();
}

void test_basic_allocation(void) {
    TEST_START("Basic Allocation");
    
    cfl_perm_t* perm = cfl_perm_malloc_create(1024);
    
    TEST_SECTION("Single allocation");
    uint16_t idx1 = cfl_perm_alloc(perm, 64);
    TEST_ASSERT(idx1 != INVALID_PERM_IDX, "Should return valid index");
    TEST_ASSERT(perm->used > 0, "Used bytes should increase");
    TEST_ASSERT(perm->used <= 64, "Used should not exceed size");
    
    TEST_SECTION("Multiple allocations");
    uint16_t idx2 = cfl_perm_alloc(perm, 32);
    uint16_t idx3 = cfl_perm_alloc(perm, 16);
    
    TEST_ASSERT(idx2 > idx1, "Second index should be greater");
    TEST_ASSERT(idx3 > idx2, "Third index should be greater");
    
    TEST_SECTION("Verify pointers are distinct");
    void* ptr1 = cfl_perm_ptr(perm, idx1);
    void* ptr2 = cfl_perm_ptr(perm, idx2);
    void* ptr3 = cfl_perm_ptr(perm, idx3);
    
    TEST_ASSERT(ptr1 != ptr2, "Pointers should be distinct");
    TEST_ASSERT(ptr2 != ptr3, "Pointers should be distinct");
    TEST_ASSERT(ptr1 != ptr3, "Pointers should be distinct");
    
    cfl_perm_malloc_destroy(perm);
    TEST_PASS();
}

void test_pointer_allocation(void) {
    TEST_START("Pointer Allocation");
    
    cfl_perm_t* perm = cfl_perm_malloc_create(1024);
    
    void* ptr1 = cfl_perm_alloc_pointer(perm, 64);
    TEST_ASSERT(ptr1 != NULL, "Should return non-NULL pointer");
    
    void* ptr2 = cfl_perm_alloc_pointer(perm, 32);
    TEST_ASSERT(ptr2 != NULL, "Should return non-NULL pointer");
    TEST_ASSERT(ptr2 != ptr1, "Pointers should be distinct");
    
    /* Verify we can write to allocated memory */
    memset(ptr1, 0xAA, 64);
    memset(ptr2, 0xBB, 32);
    
    TEST_ASSERT(((uint8_t*)ptr1)[0] == 0xAA, "Memory should be writable");
    TEST_ASSERT(((uint8_t*)ptr2)[0] == 0xBB, "Memory should be writable");
    
    cfl_perm_malloc_destroy(perm);
    TEST_PASS();
}

void test_aligned_allocation(void) {
    TEST_START("Aligned Allocation");
    
    cfl_perm_t* perm = cfl_perm_malloc_create(2048);
    
    TEST_SECTION("4-byte alignment");
    void* ptr4 = cfl_perm_alloc_pointer_aligned(perm, 10, 4);
    TEST_ASSERT(((uintptr_t)ptr4 % 4) == 0, "Should be 4-byte aligned");
    
    TEST_SECTION("8-byte alignment");
    void* ptr8 = cfl_perm_alloc_pointer_aligned(perm, 10, 8);
    TEST_ASSERT(((uintptr_t)ptr8 % 8) == 0, "Should be 8-byte aligned");
    
    TEST_SECTION("16-byte alignment");
    void* ptr16 = cfl_perm_alloc_pointer_aligned(perm, 10, 16);
    TEST_ASSERT(((uintptr_t)ptr16 % 16) == 0, "Should be 16-byte aligned");
    
    TEST_SECTION("32-byte alignment");
    void* ptr32 = cfl_perm_alloc_pointer_aligned(perm, 10, 32);
    TEST_ASSERT(((uintptr_t)ptr32 % 32) == 0, "Should be 32-byte aligned");
    
    cfl_perm_malloc_destroy(perm);
    TEST_PASS();
}

void test_index_pointer_conversion(void) {
    TEST_START("Index/Pointer Conversion");
    
    cfl_perm_t* perm = cfl_perm_malloc_create(1024);
    
    TEST_SECTION("Round-trip conversion");
    uint16_t idx1 = cfl_perm_alloc(perm, 64);
    void* ptr1 = cfl_perm_ptr(perm, idx1);
    uint16_t idx1_back = cfl_perm_ptr_to_idx(perm, ptr1);
    
    TEST_ASSERT(idx1 == idx1_back, "Round-trip should preserve index");
    
    TEST_SECTION("Multiple conversions");
    uint16_t idx2 = cfl_perm_alloc(perm, 32);
    uint16_t idx3 = cfl_perm_alloc(perm, 16);
    
    void* ptr2 = cfl_perm_ptr(perm, idx2);
    void* ptr3 = cfl_perm_ptr(perm, idx3);
    
    TEST_ASSERT(cfl_perm_ptr_to_idx(perm, ptr2) == idx2, "Conversion 2 should match");
    TEST_ASSERT(cfl_perm_ptr_to_idx(perm, ptr3) == idx3, "Conversion 3 should match");
    
    cfl_perm_malloc_destroy(perm);
    TEST_PASS();
}

void test_reset(void) {
    TEST_START("Reset Functionality");
    
    cfl_perm_t* perm = cfl_perm_malloc_create(1024);
    
    TEST_SECTION("Allocate some memory");
    cfl_perm_alloc(perm, 64);
    cfl_perm_alloc(perm, 32);
    cfl_perm_alloc(perm, 16);
    
    uint16_t used_before = perm->used;
    TEST_ASSERT(used_before > 0, "Should have used memory");
    
    TEST_SECTION("Reset allocator");
    cfl_perm_reset(perm);
    
    TEST_ASSERT(perm->used == 0, "Used should be reset to 0");
    TEST_ASSERT(perm->initialized == true, "Should still be initialized");
    TEST_ASSERT(perm->stats.total_allocations == 0, "Stats should be reset");
    
    TEST_SECTION("Allocate after reset");
    uint16_t idx_after = cfl_perm_alloc(perm, 64);
    TEST_ASSERT(idx_after == 0, "First allocation after reset should start at 0");
    
    cfl_perm_malloc_destroy(perm);
    TEST_PASS();
}

void test_used_free_bytes(void) {
    TEST_START("Used and Free Bytes");
    
    uint16_t pool_size = 1024;
    cfl_perm_t* perm = cfl_perm_malloc_create(pool_size);
    
    TEST_SECTION("Initial state");
    TEST_ASSERT(cfl_perm_used_bytes(perm) == 0, "Used should be 0");
    TEST_ASSERT(cfl_perm_free_bytes(perm) == pool_size, "Free should be pool size");
    
    TEST_SECTION("After allocations");
    cfl_perm_alloc(perm, 64);
    uint16_t used1 = cfl_perm_used_bytes(perm);
    uint16_t free1 = cfl_perm_free_bytes(perm);
    
    TEST_ASSERT(used1 > 0, "Used should increase");
    TEST_ASSERT(free1 < pool_size, "Free should decrease");
    TEST_ASSERT(used1 + free1 == pool_size, "Used + Free should equal pool size");
    
    cfl_perm_alloc(perm, 32);
    uint16_t used2 = cfl_perm_used_bytes(perm);
    uint16_t free2 = cfl_perm_free_bytes(perm);
    
    TEST_ASSERT(used2 > used1, "Used should increase more");
    TEST_ASSERT(free2 < free1, "Free should decrease more");
    TEST_ASSERT(used2 + free2 == pool_size, "Used + Free should equal pool size");
    
    cfl_perm_malloc_destroy(perm);
    TEST_PASS();
}

void test_statistics(void) {
    TEST_START("Statistics Tracking");
    
    cfl_perm_t* perm = cfl_perm_malloc_create(2048);
    CflPermStats stats;
    
    TEST_SECTION("Initial statistics");
    cfl_perm_get_stats(perm, &stats);
    TEST_ASSERT(stats.total_allocations == 0, "Should have 0 allocations");
    TEST_ASSERT(stats.current_used_bytes == 0, "Should have 0 used bytes");
    TEST_ASSERT(stats.peak_used_bytes == 0, "Should have 0 peak bytes");
    
    TEST_SECTION("After allocations");
    cfl_perm_alloc(perm, 64);
    cfl_perm_alloc(perm, 128);
    cfl_perm_alloc(perm, 32);
    
    cfl_perm_get_stats(perm, &stats);
    TEST_ASSERT(stats.total_allocations == 3, "Should have 3 allocations");
    TEST_ASSERT(stats.current_used_bytes > 0, "Should track used bytes");
    TEST_ASSERT(stats.peak_used_bytes >= stats.current_used_bytes, "Peak >= current");
    TEST_ASSERT(stats.largest_allocation == 128, "Should track largest");
    TEST_ASSERT(stats.smallest_allocation == 32, "Should track smallest");
    
    TEST_SECTION("After reset");
    cfl_perm_reset(perm);
    cfl_perm_get_stats(perm, &stats);
    TEST_ASSERT(stats.total_allocations == 0, "Stats should reset");
    
    cfl_perm_malloc_destroy(perm);
    TEST_PASS();
}

void test_validation(void) {
    TEST_START("Validation");
    
    cfl_perm_t* perm = cfl_perm_malloc_create(1024);
    
    TEST_SECTION("Valid allocator");
    TEST_ASSERT(cfl_perm_validate(perm) == true, "Should validate initially");
    
    TEST_SECTION("Valid after allocations");
    cfl_perm_alloc(perm, 64);
    cfl_perm_alloc(perm, 32);
    TEST_ASSERT(cfl_perm_validate(perm) == true, "Should validate after allocs");
    
    TEST_SECTION("Invalid allocator");
    TEST_ASSERT(cfl_perm_validate(NULL) == false, "NULL should be invalid");
    
    cfl_perm_malloc_destroy(perm);
    TEST_PASS();
}

void test_out_of_memory(void) {
    TEST_START("Out of Memory Detection");
    
    uint16_t pool_size = 256;
    cfl_perm_t* perm = cfl_perm_malloc_create(pool_size);
    
    TEST_SECTION("Fill nearly to capacity");
    cfl_perm_alloc(perm, 200);
    
    uint16_t free = cfl_perm_free_bytes(perm);
    TEST_ASSERT(free < 100, "Should have limited space left");
    
    printf("  Free bytes: %u\n", free);
    
    /* Note: Actual OOM test would trigger EXCEPTION, which would need
     * exception handling framework to test properly */
    
    cfl_perm_malloc_destroy(perm);
    TEST_PASS();
}

void test_memory_pattern(void) {
    TEST_START("Memory Pattern Write/Read");
    
    cfl_perm_t* perm = cfl_perm_malloc_create(1024);
    
    TEST_SECTION("Write patterns to allocated blocks");
    void* block1 = cfl_perm_alloc_pointer(perm, 64);
    void* block2 = cfl_perm_alloc_pointer(perm, 64);
    void* block3 = cfl_perm_alloc_pointer(perm, 64);
    
    /* Write distinct patterns */
    memset(block1, 0xAA, 64);
    memset(block2, 0xBB, 64);
    memset(block3, 0xCC, 64);
    
    TEST_SECTION("Verify patterns");
    bool pattern1_ok = true, pattern2_ok = true, pattern3_ok = true;
    
    for (int i = 0; i < 64; i++) {
        if (((uint8_t*)block1)[i] != 0xAA) pattern1_ok = false;
        if (((uint8_t*)block2)[i] != 0xBB) pattern2_ok = false;
        if (((uint8_t*)block3)[i] != 0xCC) pattern3_ok = false;
    }
    
    TEST_ASSERT(pattern1_ok, "Block 1 pattern should be intact");
    TEST_ASSERT(pattern2_ok, "Block 2 pattern should be intact");
    TEST_ASSERT(pattern3_ok, "Block 3 pattern should be intact");
    
    cfl_perm_malloc_destroy(perm);
    TEST_PASS();
}

void test_alignment_padding(void) {
    TEST_START("Alignment Padding Verification");
    
    cfl_perm_t* perm = cfl_perm_malloc_create(2048);
    
    TEST_SECTION("Allocate with various alignments");
    
    /* Start with misaligned position */
    cfl_perm_alloc(perm, 1);  /* Force misalignment */
    
    uint16_t used_before = perm->used;
    
    /* Allocate with 16-byte alignment */
    void* ptr16 = cfl_perm_alloc_pointer_aligned(perm, 16, 16);
    
    uint16_t used_after = perm->used;
    uint16_t total_consumed = used_after - used_before;
    
    TEST_ASSERT(((uintptr_t)ptr16 % 16) == 0, "Should be 16-byte aligned");
    TEST_ASSERT(total_consumed >= 16, "Should consume at least requested size");
    
    printf("  Requested: 16, Consumed: %u (includes padding)\n", total_consumed);
    
    cfl_perm_malloc_destroy(perm);
    TEST_PASS();
}

void test_minimum_allocation(void) {
    TEST_START("Minimum Allocation Size");
    
    cfl_perm_t* perm = cfl_perm_malloc_create(1024);
    
    TEST_SECTION("Very small allocations");
    
    /* Request 1 byte - should be rounded up to MIN_ALLOC_SIZE (4) */
    uint16_t used_before = perm->used;
    cfl_perm_alloc(perm, 1);
    uint16_t used_after = perm->used;
    
    TEST_ASSERT(used_after > used_before, "Should allocate something");
    TEST_ASSERT(used_after - used_before >= 4, "Should meet minimum size");
    
    cfl_perm_malloc_destroy(perm);
    TEST_PASS();
}

void test_static_buffer(void) {
    TEST_START("Static Buffer Initialization");
    
    static uint8_t static_buffer[512];
    CflPerm perm;
    
    cfl_perm_init(&perm, static_buffer, sizeof(static_buffer));
    
    TEST_ASSERT(perm.initialized == true, "Should be initialized");
    TEST_ASSERT(perm.pool == static_buffer, "Should use static buffer");
    TEST_ASSERT(perm.owns_pool == false, "Should not own static buffer");
    
    /* Test allocation works with static buffer */
    uint16_t idx = cfl_perm_alloc(&perm, 64);
    TEST_ASSERT(idx != INVALID_PERM_IDX, "Should allocate successfully");
    
    void* ptr = cfl_perm_ptr(&perm, idx);
    TEST_ASSERT(ptr >= (void*)static_buffer && 
                ptr < (void*)(static_buffer + sizeof(static_buffer)),
                "Pointer should be within static buffer");
    
    TEST_PASS();
}

void test_sequential_allocations(void) {
    TEST_START("Sequential Allocations (Bump Behavior)");
    
    cfl_perm_t* perm = cfl_perm_malloc_create(1024);
    
    TEST_SECTION("Verify bump pointer behavior");
    
    uint16_t idx1 = cfl_perm_alloc(perm, 64);
    uint16_t used1 = perm->used;
    
    uint16_t idx2 = cfl_perm_alloc(perm, 64);
    uint16_t used2 = perm->used;
    
    uint16_t idx3 = cfl_perm_alloc(perm, 64);
    uint16_t used3 = perm->used;
    
    TEST_ASSERT(idx2 > idx1, "Indices should increase");
    TEST_ASSERT(idx3 > idx2, "Indices should increase");
    TEST_ASSERT(used2 > used1, "Used bytes should increase");
    TEST_ASSERT(used3 > used2, "Used bytes should increase");
    
    printf("  idx1=%u used1=%u\n", idx1, used1);
    printf("  idx2=%u used2=%u\n", idx2, used2);
    printf("  idx3=%u used3=%u\n", idx3, used3);
    
    cfl_perm_malloc_destroy(perm);
    TEST_PASS();
}

void test_large_allocation(void) {
    TEST_START("Large Single Allocation");
    
    uint16_t pool_size = 4096;
    cfl_perm_t* perm = cfl_perm_malloc_create(pool_size);
    
    TEST_SECTION("Allocate most of pool");
    uint16_t large_size = 3500;
    void* large_block = cfl_perm_alloc_pointer(perm, large_size);
    
    TEST_ASSERT(large_block != NULL, "Should allocate large block");
    TEST_ASSERT(perm->used >= large_size, "Should consume requested size");
    
    CflPermStats stats;
    cfl_perm_get_stats(perm, &stats);
    TEST_ASSERT(stats.largest_allocation >= large_size, "Stats should track large size");
    
    /* Verify we can write to all of it */
    memset(large_block, 0x55, large_size);
    
    bool write_ok = true;
    for (int i = 0; i < large_size; i++) {
        if (((uint8_t*)large_block)[i] != 0x55) {
            write_ok = false;
            break;
        }
    }
    TEST_ASSERT(write_ok, "Should be able to write to entire block");
    
    cfl_perm_malloc_destroy(perm);
    TEST_PASS();
}

/* ============= TEST SUITE RUNNER ============= */

void run_all_tests(void) {
    printf("\n");
    printf("═══════════════════════════════════════════════════════\n");
    printf("  CFL_PERM UNIT TEST SUITE\n");
    printf("═══════════════════════════════════════════════════════\n");
    
    /* Basic functionality */
    test_create_destroy();
    test_malloc_create_destroy();
    test_init_external_buffer();
    test_static_buffer();
    
    /* Allocation tests */
    test_basic_allocation();
    test_pointer_allocation();
    test_aligned_allocation();
    test_minimum_allocation();
    test_sequential_allocations();
    test_large_allocation();
    
    /* Conversion and utilities */
    test_index_pointer_conversion();
    test_reset();
    test_used_free_bytes();
    
    /* Statistics and validation */
    test_statistics();
    test_validation();
    
    /* Memory integrity */
    test_memory_pattern();
    test_alignment_padding();
    
    /* Edge cases */
    test_out_of_memory();
    
    /* Print summary */
    printf("\n");
    printf("═══════════════════════════════════════════════════════\n");
    printf("  TEST SUMMARY\n");
    printf("═══════════════════════════════════════════════════════\n");
    printf("  Total tests:  %d\n", tests_run);
    printf(COLOR_GREEN "  Passed:       %d\n" COLOR_RESET, tests_passed);
    
    if (tests_failed > 0) {
        printf(COLOR_RED "  Failed:       %d\n" COLOR_RESET, tests_failed);
        printf("═══════════════════════════════════════════════════════\n");
        printf(COLOR_RED "  RESULT: FAILED\n" COLOR_RESET);
    } else {
        printf(COLOR_GREEN "  Failed:       0\n" COLOR_RESET);
        printf("═══════════════════════════════════════════════════════\n");
        printf(COLOR_GREEN "  RESULT: ALL TESTS PASSED ✓\n" COLOR_RESET);
    }
    printf("═══════════════════════════════════════════════════════\n");
    printf("\n");
}

int main(void) {
    run_all_tests();
    return (tests_failed == 0) ? 0 : 1;
}