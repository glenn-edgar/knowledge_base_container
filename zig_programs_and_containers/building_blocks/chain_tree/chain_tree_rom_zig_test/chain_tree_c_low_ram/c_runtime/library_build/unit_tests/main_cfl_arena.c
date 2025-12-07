/* ============= main.c - Unit Test for cfl_heap_arena_allocate ============= */
#include <stdio.h>
#include <string.h>
#include <stdint.h>
#include <stdbool.h>
#include <stdlib.h>

#include "cfl_perm.h"
#include "cfl_heap.h"
#include "cfl_heap_arena_allocate.h"
#include "cfl_exception.h"

/* Test configuration */
#define PERM_SIZE           8192     // Permanent allocator size
#define HEAP_SIZE           4096     // Heap size for dynamic arenas
#define MAX_ALLOCATORS      10       // Maximum number of arenas
#define TOTAL_NODES         20       // Total nodes in system
#define ALLOCATOR_0_SIZE    512      // Size of permanent allocator 0

/* Test counters */
static int tests_run = 0;
static int tests_passed = 0;
static int tests_failed = 0;

/* ANSI color codes for output */
#define COLOR_GREEN   "\033[0;32m"
#define COLOR_RED     "\033[0;31m"
#define COLOR_YELLOW  "\033[0;33m"
#define COLOR_BLUE    "\033[0;34m"
#define COLOR_RESET   "\033[0m"

/* Test result macros */
#define TEST_START(name) \
    do { \
        tests_run++; \
        printf(COLOR_BLUE "TEST %d: %s" COLOR_RESET "\n", tests_run, name); \
    } while(0)

#define TEST_ASSERT(condition, message) \
    do { \
        if (!(condition)) { \
            printf(COLOR_RED "  ✗ FAILED: %s" COLOR_RESET "\n", message); \
            tests_failed++; \
            return false; \
        } \
    } while(0)

#define TEST_PASS() \
    do { \
        printf(COLOR_GREEN "  ✓ PASSED" COLOR_RESET "\n"); \
        tests_passed++; \
        return true; \
    } while(0)

#define TEST_SUMMARY() \
    do { \
        printf("\n" COLOR_YELLOW "========================================" COLOR_RESET "\n"); \
        printf(COLOR_YELLOW "TEST SUMMARY" COLOR_RESET "\n"); \
        printf(COLOR_YELLOW "========================================" COLOR_RESET "\n"); \
        printf("Total tests:  %d\n", tests_run); \
        printf(COLOR_GREEN "Passed:       %d" COLOR_RESET "\n", tests_passed); \
        printf(COLOR_RED "Failed:       %d" COLOR_RESET "\n", tests_failed); \
        printf(COLOR_YELLOW "========================================" COLOR_RESET "\n"); \
    } while(0)

/* ============= Test Functions ============= */

bool test_system_creation() {
    TEST_START("System Creation");
    
    // Create permanent allocator
    CflPerm* perm = cfl_perm_malloc_create(PERM_SIZE);
    TEST_ASSERT(perm != NULL, "Failed to create perm allocator");
    
    // Create heap
    CflHeap* heap = cfl_heap_init(perm, HEAP_SIZE);
    TEST_ASSERT(heap != NULL, "Failed to create heap");
    
    // Create arena system
    CflHeapArenaSystem* sys = cfl_heap_arena_system_create(
        perm, heap, MAX_ALLOCATORS, TOTAL_NODES, ALLOCATOR_0_SIZE
    );
    TEST_ASSERT(sys != NULL, "Failed to create arena system");
    TEST_ASSERT(sys->heap == heap, "Heap not set correctly");
    TEST_ASSERT(sys->max_allocator_count == MAX_ALLOCATORS, "Max allocator count incorrect");
    TEST_ASSERT(sys->total_node_count == TOTAL_NODES, "Total node count incorrect");
    TEST_ASSERT(sys->arenas != NULL, "Arena table not allocated");
    TEST_ASSERT(sys->node_allocator_ids != NULL, "Node allocator IDs not allocated");
    TEST_ASSERT(sys->node_memory_index != NULL, "Node memory index not allocated");
    
    // Verify allocator 0 exists
    TEST_ASSERT(sys->arenas[0] != NULL, "Allocator 0 not created");
    TEST_ASSERT(cfl_heap_arena_get_id(sys->arenas[0]) == 0, "Allocator 0 has wrong ID");
    TEST_ASSERT(sys->allocator_0_buffer != NULL, "Allocator 0 buffer not allocated");
    
    // Verify all nodes start unassigned
    for (uint16_t i = 0; i < TOTAL_NODES; i++) {
        TEST_ASSERT(sys->node_allocator_ids[i] == 0, "Node should start with allocator 0");
        TEST_ASSERT(sys->node_memory_index[i] == 0xFFFF, "Node memory index should be invalid");
    }
    
    cfl_perm_malloc_destroy(perm);
    TEST_PASS();
}

bool test_allocator_0_operations() {
    TEST_START("Allocator 0 Operations (Permanent Allocator)");
    
    CflPerm* perm = cfl_perm_malloc_create(PERM_SIZE);
    CflHeap* heap = cfl_heap_init(perm, HEAP_SIZE);
    CflHeapArenaSystem* sys = cfl_heap_arena_system_create(
        perm, heap, MAX_ALLOCATORS, TOTAL_NODES, ALLOCATOR_0_SIZE
    );
    
    // Set node 0 to use allocator 0
    cfl_heap_arena_set_node_allocator_id(sys, 0, 0);
    
    // Allocate from allocator 0
    void* ptr1 = cfl_arena_system_alloc(sys, 0, 64);
    TEST_ASSERT(ptr1 != NULL, "Failed to allocate from allocator 0");
    
    void* ptr2 = cfl_arena_system_alloc(sys, 0, 128);
    TEST_ASSERT(ptr2 != NULL, "Failed second allocation from allocator 0");
    TEST_ASSERT(ptr2 > ptr1, "Second allocation not after first");
    
    // Check used bytes
    uint16_t used = cfl_heap_arena_used_bytes(sys->arenas[0]);
    TEST_ASSERT(used > 0, "Allocator 0 should show used bytes");
    TEST_ASSERT(used <= ALLOCATOR_0_SIZE, "Allocator 0 used exceeds size");
    
    // Write data to verify memory works
    memset(ptr1, 0xAA, 64);
    memset(ptr2, 0xBB, 128);
    
    cfl_perm_malloc_destroy(perm);
    TEST_PASS();
}

bool test_arena_creation_destruction() {
    TEST_START("Arena Creation and Destruction");
    
    CflPerm* perm = cfl_perm_malloc_create(PERM_SIZE);
    CflHeap* heap = cfl_heap_init(perm, HEAP_SIZE);
    CflHeapArenaSystem* sys = cfl_heap_arena_system_create(
        perm, heap, MAX_ALLOCATORS, TOTAL_NODES, ALLOCATOR_0_SIZE
    );
    
    // Create arena for node 1
    cfl_heap_arena_t arena1 = cfl_heap_arena_create(perm, sys, 1, 256);
    TEST_ASSERT(arena1 != NULL, "Failed to create arena 1");
    
    cfl_heap_allocator_id_t id1 = cfl_heap_arena_get_id(arena1);
    TEST_ASSERT(id1 > 0, "Arena ID should be > 0");
    TEST_ASSERT(id1 < MAX_ALLOCATORS, "Arena ID should be < max");
    
    // Verify node assignment
    TEST_ASSERT(sys->node_allocator_ids[1] == id1, "Node not assigned to arena");
    
    // Create another arena
    cfl_heap_arena_t arena2 = cfl_heap_arena_create(perm, sys, 2, 256);
    TEST_ASSERT(arena2 != NULL, "Failed to create arena 2");
    
    cfl_heap_allocator_id_t id2 = cfl_heap_arena_get_id(arena2);
    TEST_ASSERT(id2 != id1, "Arena IDs should be different");
    TEST_ASSERT(sys->node_allocator_ids[2] == id2, "Node 2 not assigned to arena 2");
    
    // Destroy arena 1
    cfl_heap_arena_destroy(sys, arena1, 1);
    TEST_ASSERT(sys->arenas[id1] == NULL, "Arena not cleared from table");
    TEST_ASSERT(sys->node_allocator_ids[1] == 0, "Node not reset after arena destroy");
    
    // Verify arena 2 still exists
    TEST_ASSERT(sys->arenas[id2] != NULL, "Arena 2 should still exist");
    
    // Destroy arena 2
    cfl_heap_arena_destroy(sys, arena2, 2);
    
    cfl_perm_malloc_destroy(perm);
    TEST_PASS();
}

bool test_multiple_arenas() {
    TEST_START("Multiple Arena Management");
    
    CflPerm* perm = cfl_perm_malloc_create(PERM_SIZE);
    CflHeap* heap = cfl_heap_init(perm, HEAP_SIZE);
    CflHeapArenaSystem* sys = cfl_heap_arena_system_create(
        perm, heap, MAX_ALLOCATORS, TOTAL_NODES, ALLOCATOR_0_SIZE
    );
    
    // Create multiple arenas
    cfl_heap_arena_t arenas[5];
    cfl_heap_allocator_id_t ids[5];
    
    for (int i = 0; i < 5; i++) {
        arenas[i] = cfl_heap_arena_create(perm, sys, i + 1, 200);
        TEST_ASSERT(arenas[i] != NULL, "Failed to create arena");
        ids[i] = cfl_heap_arena_get_id(arenas[i]);
        TEST_ASSERT(sys->node_allocator_ids[i + 1] == ids[i], "Node not assigned");
    }
    
    // Verify all IDs are unique
    for (int i = 0; i < 5; i++) {
        for (int j = i + 1; j < 5; j++) {
            TEST_ASSERT(ids[i] != ids[j], "Duplicate arena IDs");
        }
    }
    
    // Destroy middle arena
    cfl_heap_arena_destroy(sys, arenas[2], 3);
    TEST_ASSERT(sys->arenas[ids[2]] == NULL, "Arena not destroyed");
    
    // Create new arena - should reuse ID
    cfl_heap_arena_t new_arena = cfl_heap_arena_create(perm, sys, 10, 200);
    TEST_ASSERT(new_arena != NULL, "Failed to create new arena");
    
    // Cleanup
    for (int i = 0; i < 5; i++) {
        if (i != 2 && sys->arenas[ids[i]] != NULL) {
            cfl_heap_arena_destroy(sys, arenas[i], i + 1);
        }
    }
    cfl_heap_arena_destroy(sys, new_arena, 10);
    
    cfl_perm_malloc_destroy(perm);
    TEST_PASS();
}

bool test_arena_allocation() {
    TEST_START("Arena Allocation");
    
    CflPerm* perm = cfl_perm_malloc_create(PERM_SIZE);
    CflHeap* heap = cfl_heap_init(perm, HEAP_SIZE);
    CflHeapArenaSystem* sys = cfl_heap_arena_system_create(
        perm, heap, MAX_ALLOCATORS, TOTAL_NODES, ALLOCATOR_0_SIZE
    );
    
    // Create arena for node 5
    cfl_heap_arena_t arena = cfl_heap_arena_create(perm, sys, 5, 512);
    TEST_ASSERT(arena != NULL, "Failed to create arena");
    
    // Allocate from arena
    void* ptr1 = cfl_arena_system_alloc(sys, 5, 64);
    TEST_ASSERT(ptr1 != NULL, "Failed to allocate");
    
    void* ptr2 = cfl_arena_system_alloc(sys, 5, 128);
    TEST_ASSERT(ptr2 != NULL, "Failed second allocation");
    TEST_ASSERT(ptr2 > ptr1, "Allocations not sequential");
    
    // Check arena usage
    uint16_t used = cfl_heap_arena_used_bytes(arena);
    TEST_ASSERT(used >= 64 + 128, "Arena usage incorrect");
    
    uint16_t free = cfl_heap_arena_free_bytes(arena);
    TEST_ASSERT(free < 512, "Arena free space incorrect");
    TEST_ASSERT(used + free == 512, "Arena accounting incorrect");
    
    // Write and verify data
    memset(ptr1, 0x55, 64);
    memset(ptr2, 0xAA, 128);
    TEST_ASSERT(((uint8_t*)ptr1)[0] == 0x55, "Data corruption");
    TEST_ASSERT(((uint8_t*)ptr2)[0] == 0xAA, "Data corruption");
    
    cfl_heap_arena_destroy(sys, arena, 5);
    cfl_perm_malloc_destroy(perm);
    TEST_PASS();
}

bool test_aligned_allocation() {
    TEST_START("Aligned Allocation");
    
    CflPerm* perm = cfl_perm_malloc_create(PERM_SIZE);
    CflHeap* heap = cfl_heap_init(perm, HEAP_SIZE);
    CflHeapArenaSystem* sys = cfl_heap_arena_system_create(
        perm, heap, MAX_ALLOCATORS, TOTAL_NODES, ALLOCATOR_0_SIZE
    );
    
    // Create arena
    cfl_heap_arena_t arena = cfl_heap_arena_create(perm, sys, 7, 512);
    TEST_ASSERT(arena != NULL, "Failed to create arena");
    
    // Allocate with 16-byte alignment
    void* ptr1 = cfl_arena_system_alloc_aligned(sys, 7, 32, 16);
    TEST_ASSERT(ptr1 != NULL, "Failed aligned allocation");
    TEST_ASSERT(((uintptr_t)ptr1 & 0x0F) == 0, "Pointer not 16-byte aligned");
    
    // Allocate with 8-byte alignment
    void* ptr2 = cfl_arena_system_alloc_aligned(sys, 7, 64, 8);
    TEST_ASSERT(ptr2 != NULL, "Failed second aligned allocation");
    TEST_ASSERT(((uintptr_t)ptr2 & 0x07) == 0, "Pointer not 8-byte aligned");
    
    // Allocate with 4-byte alignment
    void* ptr3 = cfl_arena_system_alloc_aligned(sys, 7, 16, 4);
    TEST_ASSERT(ptr3 != NULL, "Failed third aligned allocation");
    TEST_ASSERT(((uintptr_t)ptr3 & 0x03) == 0, "Pointer not 4-byte aligned");
    
    cfl_heap_arena_destroy(sys, arena, 7);
    cfl_perm_malloc_destroy(perm);
    TEST_PASS();
}

bool test_context_switching() {
    TEST_START("Active Allocator Context Switching");
    
    CflPerm* perm = cfl_perm_malloc_create(PERM_SIZE);
    CflHeap* heap = cfl_heap_init(perm, HEAP_SIZE);
    CflHeapArenaSystem* sys = cfl_heap_arena_system_create(
        perm, heap, MAX_ALLOCATORS, TOTAL_NODES, ALLOCATOR_0_SIZE
    );
    
    // Create arenas for nodes 1 and 2
    cfl_heap_arena_t arena1 = cfl_heap_arena_create(perm, sys, 1, 256);
    cfl_heap_arena_t arena2 = cfl_heap_arena_create(perm, sys, 2, 256);
    
    // Set active context to node 1's arena
    cfl_heap_arena_set_active_allocator(sys, 1);
    TEST_ASSERT(sys->active_allocator_context == cfl_heap_arena_get_id(arena1),
                "Active context not set to arena 1");
    
    // Node 3 captures current context
    cfl_heap_arena_set_node_allocator(sys, 3);
    TEST_ASSERT(sys->node_allocator_ids[3] == cfl_heap_arena_get_id(arena1),
                "Node 3 didn't capture arena 1 context");
    
    // Switch to node 2's arena
    cfl_heap_arena_set_active_allocator(sys, 2);
    TEST_ASSERT(sys->active_allocator_context == cfl_heap_arena_get_id(arena2),
                "Active context not set to arena 2");
    
    // Node 4 captures current context
    cfl_heap_arena_set_node_allocator(sys, 4);
    TEST_ASSERT(sys->node_allocator_ids[4] == cfl_heap_arena_get_id(arena2),
                "Node 4 didn't capture arena 2 context");
    
    // Verify nodes can allocate from their captured contexts
    void* ptr3 = cfl_arena_system_alloc(sys, 3, 64);
    void* ptr4 = cfl_arena_system_alloc(sys, 4, 64);
    TEST_ASSERT(ptr3 != NULL && ptr4 != NULL, "Allocation from captured context failed");
    
    cfl_heap_arena_destroy(sys, arena1, 1);
    cfl_heap_arena_destroy(sys, arena2, 2);
    cfl_perm_malloc_destroy(perm);
    TEST_PASS();
}

bool test_system_reset() {
    TEST_START("System Reset");
    
    CflPerm* perm = cfl_perm_malloc_create(PERM_SIZE);
    CflHeap* heap = cfl_heap_init(perm, HEAP_SIZE);
    CflHeapArenaSystem* sys = cfl_heap_arena_system_create(
        perm, heap, MAX_ALLOCATORS, TOTAL_NODES, ALLOCATOR_0_SIZE
    );
    
    // Create multiple arenas
    cfl_heap_arena_t arena1 = cfl_heap_arena_create(perm, sys, 1, 256);
    cfl_heap_arena_t arena2 = cfl_heap_arena_create(perm, sys, 2, 256);
    
    cfl_heap_allocator_id_t id1 = cfl_heap_arena_get_id(arena1);
    cfl_heap_allocator_id_t id2 = cfl_heap_arena_get_id(arena2);
    
    // Allocate from arenas
    void* ptr1 = cfl_arena_system_alloc(sys, 1, 64);
    void* ptr2 = cfl_arena_system_alloc(sys, 2, 64);
    TEST_ASSERT(ptr1 != NULL && ptr2 != NULL, "Allocation failed");
    
    // Allocate from allocator 0
    cfl_heap_arena_set_node_allocator_id(sys, 0, 0);
    void* ptr0 = cfl_arena_system_alloc(sys, 0, 64);
    TEST_ASSERT(ptr0 != NULL, "Allocator 0 allocation failed");
    uint16_t used0_before = cfl_heap_arena_used_bytes(sys->arenas[0]);
    
    // Reset system
    cfl_heap_arena_system_reset(sys);
    
    // Verify arenas 1 and 2 are destroyed
    TEST_ASSERT(sys->arenas[id1] == NULL, "Arena 1 not destroyed");
    TEST_ASSERT(sys->arenas[id2] == NULL, "Arena 2 not destroyed");
    
    // Verify allocator 0 still exists but is reset
    TEST_ASSERT(sys->arenas[0] != NULL, "Allocator 0 destroyed");
    uint16_t used0_after = cfl_heap_arena_used_bytes(sys->arenas[0]);
    TEST_ASSERT(used0_after == 0, "Allocator 0 not reset");
    
    // Verify node arrays are reinitialized
    for (uint16_t i = 0; i < TOTAL_NODES; i++) {
        TEST_ASSERT(sys->node_allocator_ids[i] == 0, "Node allocator not reset");
        TEST_ASSERT(sys->node_memory_index[i] == 0xFFFF, "Node memory index not reset");
    }
    
    // Verify can allocate again after reset
    cfl_heap_arena_set_node_allocator_id(sys, 0, 0);
    void* ptr0_new = cfl_arena_system_alloc(sys, 0, 64);
    TEST_ASSERT(ptr0_new != NULL, "Cannot allocate after reset");
    
    cfl_perm_malloc_destroy(perm);
    TEST_PASS();
}

bool test_resource_exhaustion() {
    TEST_START("Resource Exhaustion");
    
    CflPerm* perm = cfl_perm_malloc_create(PERM_SIZE);
    CflHeap* heap = cfl_heap_init(perm, HEAP_SIZE);
    CflHeapArenaSystem* sys = cfl_heap_arena_system_create(
        perm, heap, MAX_ALLOCATORS, TOTAL_NODES, ALLOCATOR_0_SIZE
    );
    
    // Create arena with limited size
    cfl_heap_arena_t arena = cfl_heap_arena_create(perm, sys, 5, 128);
    TEST_ASSERT(arena != NULL, "Failed to create arena");
    
    // Allocate until exhaustion (should not crash, should throw exception)
    // In production, this would trigger exception handler
    // For testing, we'll just verify we can fill the arena
    void* ptr1 = cfl_arena_system_alloc(sys, 5, 64);
    void* ptr2 = cfl_arena_system_alloc(sys, 5, 64);
    TEST_ASSERT(ptr1 != NULL && ptr2 != NULL, "Should be able to allocate");
    
    uint16_t used = cfl_heap_arena_used_bytes(arena);
    uint16_t free = cfl_heap_arena_free_bytes(arena);
    TEST_ASSERT(used >= 128 || free < 64, "Arena not near exhaustion");
    
    cfl_heap_arena_destroy(sys, arena, 5);
    cfl_perm_malloc_destroy(perm);
    TEST_PASS();
}

bool test_node_array_accessors() {
    TEST_START("Node Array Accessors");
    
    CflPerm* perm = cfl_perm_malloc_create(PERM_SIZE);
    CflHeap* heap = cfl_heap_init(perm, HEAP_SIZE);
    CflHeapArenaSystem* sys = cfl_heap_arena_system_create(
        perm, heap, MAX_ALLOCATORS, TOTAL_NODES, ALLOCATOR_0_SIZE
    );
    
    // Test allocator ID accessors
    cfl_heap_arena_set_node_allocator_id(sys, 5, 42);
    TEST_ASSERT(cfl_heap_arena_get_node_allocator_id(sys, 5) == 42,
                "Get/Set allocator ID failed");
    
    // Test memory index accessors
    cfl_heap_arena_set_node_memory_index(sys, 7, 0x1234);
    TEST_ASSERT(cfl_heap_arena_get_node_memory_index(sys, 7) == 0x1234,
                "Get/Set memory index failed");
    
    // Test multiple nodes
    for (uint16_t i = 0; i < 10; i++) {
        cfl_heap_arena_set_node_allocator_id(sys, i, i + 10);
        cfl_heap_arena_set_node_memory_index(sys, i, i * 100);
    }
    
    for (uint16_t i = 0; i < 10; i++) {
        TEST_ASSERT(cfl_heap_arena_get_node_allocator_id(sys, i) == i + 10,
                    "Allocator ID corruption");
        TEST_ASSERT(cfl_heap_arena_get_node_memory_index(sys, i) == i * 100,
                    "Memory index corruption");
    }
    
    cfl_perm_malloc_destroy(perm);
    TEST_PASS();
}

bool test_stats_and_diagnostics() {
    TEST_START("Stats and Diagnostics");
    
    CflPerm* perm = cfl_perm_malloc_create(PERM_SIZE);
    CflHeap* heap = cfl_heap_init(perm, HEAP_SIZE);
    CflHeapArenaSystem* sys = cfl_heap_arena_system_create(
        perm, heap, MAX_ALLOCATORS, TOTAL_NODES, ALLOCATOR_0_SIZE
    );
    
    // Create arena and allocate
    cfl_heap_arena_t arena = cfl_heap_arena_create(perm, sys, 3, 512);
    
    uint16_t initial_used = cfl_heap_arena_used_bytes(arena);
    uint16_t initial_free = cfl_heap_arena_free_bytes(arena);
    TEST_ASSERT(initial_used == 0, "Arena should start empty");
    TEST_ASSERT(initial_free == 512, "Arena free should equal size");
    
    // Allocate and check stats
    void* ptr = cfl_arena_system_alloc(sys, 3, 100);
    TEST_ASSERT(ptr != NULL, "Allocation failed");
    
    uint16_t used = cfl_heap_arena_used_bytes(arena);
    uint16_t free = cfl_heap_arena_free_bytes(arena);
    TEST_ASSERT(used > 0, "Used should increase");
    TEST_ASSERT(free < 512, "Free should decrease");
    TEST_ASSERT(used + free == 512, "Accounting mismatch");
    
    // Dump stats (should not crash)
    cfl_heap_arena_dump_stats(sys);
    
    cfl_heap_arena_destroy(sys, arena, 3);
    cfl_perm_malloc_destroy(perm);
    TEST_PASS();
}

/* ============= Main Test Runner ============= */

int main(void) {
    printf("\n");
    printf(COLOR_YELLOW "========================================" COLOR_RESET "\n");
    printf(COLOR_YELLOW "CFL_HEAP_ARENA_ALLOCATE UNIT TESTS" COLOR_RESET "\n");
    printf(COLOR_YELLOW "========================================" COLOR_RESET "\n");
    printf("\n");
    
    // Run all tests
    test_system_creation();
    test_allocator_0_operations();
    test_arena_creation_destruction();
    test_multiple_arenas();
    test_arena_allocation();
    test_aligned_allocation();
    test_context_switching();
    test_system_reset();
    test_resource_exhaustion();
    test_node_array_accessors();
    test_stats_and_diagnostics();
    
    // Print summary
    TEST_SUMMARY();
    
    return (tests_failed == 0) ? 0 : 1;
}