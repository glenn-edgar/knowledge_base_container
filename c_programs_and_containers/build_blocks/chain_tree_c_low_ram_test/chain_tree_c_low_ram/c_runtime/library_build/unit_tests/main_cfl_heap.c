/* ============= UNIT TEST: cfl_heap.c ============= */
#include "cfl_heap.h"
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

/* Helper to create test environment */
typedef struct {
    CflPerm* perm;
    CflHeap* heap;
} TestEnv;

static TestEnv create_test_env(uint16_t perm_size, uint16_t heap_size) {
    TestEnv env;
    env.perm = cfl_perm_malloc_create(perm_size);
    env.heap = cfl_heap_init(env.perm, heap_size);
    return env;
}

static void destroy_test_env(TestEnv* env) {
    cfl_perm_malloc_destroy(env->perm);
}

/* ============= TEST FUNCTIONS ============= */

void test_heap_init(void) {
    TEST_START("Heap Initialization");
    
    CflPerm* perm = cfl_perm_malloc_create(8192);
    
    TEST_SECTION("Create heap from perm");
    CflHeap* heap = cfl_heap_init(perm, 2048);
    
    TEST_ASSERT(heap != NULL, "Heap should be created");
    TEST_ASSERT(heap->initialized == true, "Heap should be initialized");
    TEST_ASSERT(heap->pool != NULL, "Pool should be allocated");
    TEST_ASSERT(heap->pool_size == 2048, "Pool size should match");
    TEST_ASSERT(heap->owns_pool == false, "Should not own pool (perm owns it)");
    
    TEST_SECTION("Check initial statistics");
    CflHeapStats stats;
    cfl_heap_get_stats(heap, &stats);
    TEST_ASSERT(stats.total_allocations == 0, "Should have 0 allocations");
    TEST_ASSERT(stats.current_blocks == 1, "Should have 1 free block");
    TEST_ASSERT(stats.free_blocks == 1, "Should have 1 free block");
    TEST_ASSERT(stats.allocated_blocks == 0, "Should have 0 allocated blocks");
    
    cfl_perm_malloc_destroy(perm);
    TEST_PASS();
}

void test_basic_malloc_free(void) {
    TEST_START("Basic Malloc and Free");
    
    TestEnv env = create_test_env(8192, 2048);
    
    TEST_SECTION("Allocate a block");
    uint16_t idx1 = cfl_heap_malloc(env.heap, 64);
    TEST_ASSERT(idx1 != INVALID_HEAP_IDX, "Should return valid index");
    
    CflHeapStats stats;
    cfl_heap_get_stats(env.heap, &stats);
    TEST_ASSERT(stats.allocated_blocks == 1, "Should have 1 allocated block");
    TEST_ASSERT(stats.total_allocations == 1, "Should track allocation");
    
    TEST_SECTION("Free the block");
    cfl_heap_free(env.heap, idx1);
    
    cfl_heap_get_stats(env.heap, &stats);
    TEST_ASSERT(stats.allocated_blocks == 0, "Should have 0 allocated blocks");
    TEST_ASSERT(stats.free_blocks >= 1, "Should have free blocks");
    TEST_ASSERT(stats.total_frees == 1, "Should track free");
    
    destroy_test_env(&env);
    TEST_PASS();
}

void test_pointer_malloc_free(void) {
    TEST_START("Pointer-based Malloc and Free");
    
    TestEnv env = create_test_env(8192, 2048);
    
    TEST_SECTION("Allocate using pointer API");
    void* ptr1 = cfl_heap_malloc_pointer(env.heap, 64);
    TEST_ASSERT(ptr1 != NULL, "Should return non-NULL pointer");
    
    TEST_SECTION("Write to allocated memory");
    memset(ptr1, 0xAA, 64);
    TEST_ASSERT(((uint8_t*)ptr1)[0] == 0xAA, "Memory should be writable");
    
    TEST_SECTION("Free using pointer API");
    cfl_heap_free_pointer(env.heap, ptr1);
    
    CflHeapStats stats;
    cfl_heap_get_stats(env.heap, &stats);
    TEST_ASSERT(stats.allocated_blocks == 0, "Should have 0 allocated blocks");
    
    destroy_test_env(&env);
    TEST_PASS();
}

void test_multiple_allocations(void) {
    TEST_START("Multiple Allocations");
    
    TestEnv env = create_test_env(8192, 2048);
    
    TEST_SECTION("Allocate multiple blocks");
    uint16_t idx1 = cfl_heap_malloc(env.heap, 64);
    uint16_t idx2 = cfl_heap_malloc(env.heap, 128);
    uint16_t idx3 = cfl_heap_malloc(env.heap, 32);
    
    TEST_ASSERT(idx1 != INVALID_HEAP_IDX, "Allocation 1 should succeed");
    TEST_ASSERT(idx2 != INVALID_HEAP_IDX, "Allocation 2 should succeed");
    TEST_ASSERT(idx3 != INVALID_HEAP_IDX, "Allocation 3 should succeed");
    
    CflHeapStats stats;
    cfl_heap_get_stats(env.heap, &stats);
    TEST_ASSERT(stats.allocated_blocks == 3, "Should have 3 allocated blocks");
    TEST_ASSERT(stats.total_allocations == 3, "Should track 3 allocations");
    
    TEST_SECTION("Verify pointers are distinct");
    void* ptr1 = cfl_heap_ptr(env.heap, idx1);
    void* ptr2 = cfl_heap_ptr(env.heap, idx2);
    void* ptr3 = cfl_heap_ptr(env.heap, idx3);
    
    TEST_ASSERT(ptr1 != ptr2, "Pointers should be distinct");
    TEST_ASSERT(ptr2 != ptr3, "Pointers should be distinct");
    TEST_ASSERT(ptr1 != ptr3, "Pointers should be distinct");
    
    destroy_test_env(&env);
    TEST_PASS();
}

void test_coalescing(void) {
    TEST_START("Free Block Coalescing");
    
    TestEnv env = create_test_env(8192, 2048);
    
    TEST_SECTION("Allocate three adjacent blocks");
    uint16_t idx1 = cfl_heap_malloc(env.heap, 64);
    uint16_t idx2 = cfl_heap_malloc(env.heap, 64);
    uint16_t idx3 = cfl_heap_malloc(env.heap, 64);
    
    CflHeapStats stats;
    cfl_heap_get_stats(env.heap, &stats);
    uint16_t blocks_before = stats.current_blocks;
    
    TEST_SECTION("Free middle block");
    cfl_heap_free(env.heap, idx2);
    
    TEST_SECTION("Free first block - should coalesce");
    cfl_heap_free(env.heap, idx1);
    
    cfl_heap_get_stats(env.heap, &stats);
    TEST_ASSERT(stats.allocated_blocks == 1, "Should have 1 allocated block remaining");
    
    TEST_SECTION("Free last block - should coalesce all");
    cfl_heap_free(env.heap, idx3);
    
    cfl_heap_get_stats(env.heap, &stats);
    TEST_ASSERT(stats.allocated_blocks == 0, "Should have 0 allocated blocks");
    TEST_ASSERT(stats.free_blocks >= 1, "Should have at least 1 free block");
    
    /* After coalescing, we should be able to allocate larger block */
    uint16_t idx_large = cfl_heap_malloc(env.heap, 192);
    TEST_ASSERT(idx_large != INVALID_HEAP_IDX, "Should allocate large block after coalescing");
    
    destroy_test_env(&env);
    TEST_PASS();
}

void test_arena_alloc_aligned(void) {
    TEST_START("Arena Allocation with Alignment");
    
    TestEnv env = create_test_env(8192, 2048);
    
    TEST_SECTION("8-byte aligned allocation");
    uint16_t idx8 = cfl_heap_arena_alloc_aligned(env.heap, 100, 64, 8);
    void* ptr8 = cfl_heap_ptr(env.heap, idx8);
    TEST_ASSERT(((uintptr_t)ptr8 % 8) == 0, "Should be 8-byte aligned");
    
    TEST_SECTION("16-byte aligned allocation");
    uint16_t idx16 = cfl_heap_arena_alloc_aligned(env.heap, 200, 64, 16);
    void* ptr16 = cfl_heap_ptr(env.heap, idx16);
    TEST_ASSERT(((uintptr_t)ptr16 % 16) == 0, "Should be 16-byte aligned");
    
    TEST_SECTION("32-byte aligned allocation");
    uint16_t idx32 = cfl_heap_arena_alloc_aligned(env.heap, 300, 64, 32);
    void* ptr32 = cfl_heap_ptr(env.heap, idx32);
    TEST_ASSERT(((uintptr_t)ptr32 % 32) == 0, "Should be 32-byte aligned");
    
    destroy_test_env(&env);
    TEST_PASS();
}

void test_node_id_tracking(void) {
    TEST_START("Node ID Tracking");
    
    TestEnv env = create_test_env(8192, 2048);
    
    TEST_SECTION("Allocate with different node IDs");
    uint16_t idx1 = cfl_heap_arena_alloc_aligned(env.heap, 100, 64, 4);
    uint16_t idx2 = cfl_heap_arena_alloc_aligned(env.heap, 200, 64, 4);
    uint16_t idx3 = cfl_heap_arena_alloc_aligned(env.heap, 300, 64, 4);
    
    TEST_SECTION("Verify node IDs are tracked");
    uint16_t node1 = cfl_heap_get_node_id(env.heap, idx1);
    uint16_t node2 = cfl_heap_get_node_id(env.heap, idx2);
    uint16_t node3 = cfl_heap_get_node_id(env.heap, idx3);
    
    TEST_ASSERT(node1 == 100, "Node ID 1 should be 100");
    TEST_ASSERT(node2 == 200, "Node ID 2 should be 200");
    TEST_ASSERT(node3 == 300, "Node ID 3 should be 300");
    
    TEST_SECTION("Standard malloc should have NODE_ID_NONE");
    uint16_t idx_std = cfl_heap_malloc(env.heap, 64);
    uint16_t node_std = cfl_heap_get_node_id(env.heap, idx_std);
    TEST_ASSERT(node_std == NODE_ID_NONE, "Standard malloc should have NODE_ID_NONE");
    
    destroy_test_env(&env);
    TEST_PASS();
}

void test_index_pointer_conversion(void) {
    TEST_START("Index/Pointer Conversion");
    
    TestEnv env = create_test_env(8192, 2048);
    
    TEST_SECTION("Round-trip conversion");
    uint16_t idx1 = cfl_heap_malloc(env.heap, 64);
    void* ptr1 = cfl_heap_ptr(env.heap, idx1);
    uint16_t idx1_back = cfl_heap_ptr_to_idx(env.heap, ptr1);
    
    TEST_ASSERT(idx1 == idx1_back, "Round-trip should preserve index");
    
    TEST_SECTION("Multiple conversions");
    uint16_t idx2 = cfl_heap_malloc(env.heap, 32);
    uint16_t idx3 = cfl_heap_malloc(env.heap, 16);
    
    void* ptr2 = cfl_heap_ptr(env.heap, idx2);
    void* ptr3 = cfl_heap_ptr(env.heap, idx3);
    
    TEST_ASSERT(cfl_heap_ptr_to_idx(env.heap, ptr2) == idx2, "Conversion 2 should match");
    TEST_ASSERT(cfl_heap_ptr_to_idx(env.heap, ptr3) == idx3, "Conversion 3 should match");
    
    destroy_test_env(&env);
    TEST_PASS();
}

void test_used_free_bytes(void) {
    TEST_START("Used and Free Bytes Tracking");
    
    TestEnv env = create_test_env(8192, 2048);
    
    TEST_SECTION("Initial state");
    uint16_t free_initial = cfl_heap_free_bytes(env.heap);
    uint16_t used_initial = cfl_heap_used_bytes(env.heap);
    
    TEST_ASSERT(used_initial >= 0, "Used should be >= 0");
    TEST_ASSERT(free_initial > 0, "Should have free space");
    
    TEST_SECTION("After allocations");
    cfl_heap_malloc(env.heap, 64);
    cfl_heap_malloc(env.heap, 128);
    
    uint16_t used_after = cfl_heap_used_bytes(env.heap);
    uint16_t free_after = cfl_heap_free_bytes(env.heap);
    
    TEST_ASSERT(used_after > used_initial, "Used should increase");
    TEST_ASSERT(free_after < free_initial, "Free should decrease");
    
    printf("  Used: %u -> %u, Free: %u -> %u\n", 
           used_initial, used_after, free_initial, free_after);
    
    destroy_test_env(&env);
    TEST_PASS();
}

void test_reset(void) {
    TEST_START("Heap Reset");
    
    TestEnv env = create_test_env(8192, 2048);
    
    TEST_SECTION("Make some allocations");
    cfl_heap_malloc(env.heap, 64);
    cfl_heap_malloc(env.heap, 128);
    cfl_heap_malloc(env.heap, 32);
    
    CflHeapStats stats;
    cfl_heap_get_stats(env.heap, &stats);
    TEST_ASSERT(stats.allocated_blocks == 3, "Should have 3 allocated blocks");
    
    TEST_SECTION("Reset heap");
    cfl_heap_reset(env.heap);
    
    cfl_heap_get_stats(env.heap, &stats);
    TEST_ASSERT(stats.total_allocations == 0, "Stats should be reset");
    TEST_ASSERT(stats.allocated_blocks == 0, "Should have 0 allocated blocks");
    TEST_ASSERT(stats.free_blocks == 1, "Should have 1 free block");
    
    TEST_SECTION("Allocate after reset");
    uint16_t idx = cfl_heap_malloc(env.heap, 64);
    TEST_ASSERT(idx != INVALID_HEAP_IDX, "Should allocate after reset");
    
    destroy_test_env(&env);
    TEST_PASS();
}

void test_validation(void) {
    TEST_START("Heap Validation");
    
    TestEnv env = create_test_env(8192, 2048);
    
    TEST_SECTION("Valid heap");
    TEST_ASSERT(cfl_heap_validate(env.heap) == true, "Heap should be valid initially");
    
    TEST_SECTION("Valid after allocations");
    cfl_heap_malloc(env.heap, 64);
    cfl_heap_malloc(env.heap, 32);
    TEST_ASSERT(cfl_heap_validate(env.heap) == true, "Heap should be valid after allocs");
    
    TEST_SECTION("Valid after free");
    uint16_t idx = cfl_heap_malloc(env.heap, 16);
    cfl_heap_free(env.heap, idx);
    TEST_ASSERT(cfl_heap_validate(env.heap) == true, "Heap should be valid after free");
    
    destroy_test_env(&env);
    TEST_PASS();
}

void test_statistics(void) {
    TEST_START("Statistics Tracking");
    
    TestEnv env = create_test_env(8192, 2048);
    
    TEST_SECTION("Initial statistics");
    CflHeapStats stats;
    cfl_heap_get_stats(env.heap, &stats);
    TEST_ASSERT(stats.total_allocations == 0, "Should have 0 allocations");
    TEST_ASSERT(stats.total_frees == 0, "Should have 0 frees");
    
    TEST_SECTION("After allocations");
    cfl_heap_malloc(env.heap, 64);
    cfl_heap_malloc(env.heap, 128);
    cfl_heap_malloc(env.heap, 32);
    
    cfl_heap_get_stats(env.heap, &stats);
    TEST_ASSERT(stats.total_allocations == 3, "Should track allocations");
    TEST_ASSERT(stats.allocated_blocks == 3, "Should have 3 allocated blocks");
    TEST_ASSERT(stats.peak_used_bytes > 0, "Peak should be tracked");
    
    TEST_SECTION("After frees");
    uint16_t idx = cfl_heap_malloc(env.heap, 16);
    cfl_heap_free(env.heap, idx);
    
    cfl_heap_get_stats(env.heap, &stats);
    TEST_ASSERT(stats.total_frees == 1, "Should track frees");
    
    destroy_test_env(&env);
    TEST_PASS();
}

void test_memory_pattern(void) {
    TEST_START("Memory Pattern Write/Read");
    
    TestEnv env = create_test_env(8192, 2048);
    
    TEST_SECTION("Allocate and write patterns");
    void* ptr1 = cfl_heap_malloc_pointer(env.heap, 64);
    void* ptr2 = cfl_heap_malloc_pointer(env.heap, 64);
    void* ptr3 = cfl_heap_malloc_pointer(env.heap, 64);
    
    memset(ptr1, 0xAA, 64);
    memset(ptr2, 0xBB, 64);
    memset(ptr3, 0xCC, 64);
    
    TEST_SECTION("Verify patterns");
    bool pattern1_ok = true, pattern2_ok = true, pattern3_ok = true;
    
    for (int i = 0; i < 64; i++) {
        if (((uint8_t*)ptr1)[i] != 0xAA) pattern1_ok = false;
        if (((uint8_t*)ptr2)[i] != 0xBB) pattern2_ok = false;
        if (((uint8_t*)ptr3)[i] != 0xCC) pattern3_ok = false;
    }
    
    TEST_ASSERT(pattern1_ok, "Pattern 1 should be intact");
    TEST_ASSERT(pattern2_ok, "Pattern 2 should be intact");
    TEST_ASSERT(pattern3_ok, "Pattern 3 should be intact");
    
    destroy_test_env(&env);
    TEST_PASS();
}

void test_fragmentation(void) {
    TEST_START("Fragmentation and Reuse");
    
    TestEnv env = create_test_env(8192, 2048);
    
    TEST_SECTION("Create fragmentation pattern");
    uint16_t idx1 = cfl_heap_malloc(env.heap, 64);
    uint16_t idx2 = cfl_heap_malloc(env.heap, 64);
    uint16_t idx3 = cfl_heap_malloc(env.heap, 64);
    uint16_t idx4 = cfl_heap_malloc(env.heap, 64);
    
    TEST_SECTION("Free alternating blocks");
    cfl_heap_free(env.heap, idx1);
    cfl_heap_free(env.heap, idx3);
    
    CflHeapStats stats;
    cfl_heap_get_stats(env.heap, &stats);
    TEST_ASSERT(stats.allocated_blocks == 2, "Should have 2 allocated blocks");
    TEST_ASSERT(stats.free_blocks >= 2, "Should have multiple free blocks");
    
    TEST_SECTION("Allocate into freed space");
    uint16_t idx_new = cfl_heap_malloc(env.heap, 32);
    TEST_ASSERT(idx_new != INVALID_HEAP_IDX, "Should reuse freed space");
    
    destroy_test_env(&env);
    TEST_PASS();
}

void test_heap_walk(void) {
    TEST_START("Heap Walk Callback");
    
    TestEnv env = create_test_env(8192, 2048);
    
    TEST_SECTION("Allocate some blocks");
    cfl_heap_arena_alloc_aligned(env.heap, 100, 64, 4);
    cfl_heap_arena_alloc_aligned(env.heap, 200, 32, 4);
    cfl_heap_malloc(env.heap, 16);
    
    TEST_SECTION("Walk heap blocks");
    int callback_count = 0;
    
    void walk_callback(void* block_ptr, uint16_t size, bool allocated, uint16_t node_id) {
        (void)block_ptr;
        (void)size;
        (void)allocated;
        (void)node_id;
        callback_count++;
    }
    
    cfl_heap_walk(env.heap, walk_callback);
    
    TEST_ASSERT(callback_count > 0, "Should have called callback");
    printf("  Walked %d blocks\n", callback_count);
    
    destroy_test_env(&env);
    TEST_PASS();
}

void test_large_allocation(void) {
    TEST_START("Large Single Allocation");
    
    TestEnv env = create_test_env(16384, 8192);
    
    TEST_SECTION("Allocate large block");
    uint16_t large_size = 4096;
    void* large_ptr = cfl_heap_malloc_pointer(env.heap, large_size);
    
    TEST_ASSERT(large_ptr != NULL, "Should allocate large block");
    
    TEST_SECTION("Write to large block");
    memset(large_ptr, 0x55, large_size);
    
    bool write_ok = true;
    for (int i = 0; i < large_size; i++) {
        if (((uint8_t*)large_ptr)[i] != 0x55) {
            write_ok = false;
            break;
        }
    }
    TEST_ASSERT(write_ok, "Large block should be writable");
    
    destroy_test_env(&env);
    TEST_PASS();
}

void test_minimum_allocation(void) {
    TEST_START("Minimum Allocation Size");
    
    TestEnv env = create_test_env(8192, 2048);
    
    TEST_SECTION("Very small allocations");
    uint16_t idx1 = cfl_heap_malloc(env.heap, 1);
    TEST_ASSERT(idx1 != INVALID_HEAP_IDX, "Should allocate 1 byte");
    
    uint16_t idx2 = cfl_heap_malloc(env.heap, 2);
    TEST_ASSERT(idx2 != INVALID_HEAP_IDX, "Should allocate 2 bytes");
    
    /* Verify they don't overlap */
    void* ptr1 = cfl_heap_ptr(env.heap, idx1);
    void* ptr2 = cfl_heap_ptr(env.heap, idx2);
    TEST_ASSERT(ptr1 != ptr2, "Small allocations should not overlap");
    
    destroy_test_env(&env);
    TEST_PASS();
}

/* ============= TEST SUITE RUNNER ============= */

void run_all_tests(void) {
    printf("\n");
    printf("═══════════════════════════════════════════════════════\n");
    printf("  CFL_HEAP UNIT TEST SUITE\n");
    printf("═══════════════════════════════════════════════════════\n");
    
    /* Basic functionality */
    test_heap_init();
    test_basic_malloc_free();
    test_pointer_malloc_free();
    test_multiple_allocations();
    
    /* Memory management */
    test_coalescing();
    test_fragmentation();
    test_reset();
    
    /* Advanced features */
    test_arena_alloc_aligned();
    test_node_id_tracking();
    test_heap_walk();
    
    /* Utilities */
    test_index_pointer_conversion();
    test_used_free_bytes();
    
    /* Statistics and validation */
    test_statistics();
    test_validation();
    
    /* Memory integrity */
    test_memory_pattern();
    test_minimum_allocation();
    test_large_allocation();
    
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