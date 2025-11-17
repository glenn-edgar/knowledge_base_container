#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdbool.h>
#include <stdint.h>
#include "arena_stack.h"

int main(void) {
    /* Create a 1KB memory buffer */
    const uint16_t HEAP_SIZE = 1024;
    char memory_buffer[HEAP_SIZE];
    
    /* Create arena stack handle */
    arena_stack* arena = arena_stack_create();
    if (!arena) {
        printf("Failed to create arena stack\n");
        return 1;
    }
    
    /* Initialize arena */
    if (!arena_stack_initialize(arena, memory_buffer, HEAP_SIZE)) {
        printf("Failed to initialize arena stack\n");
        arena_stack_destroy(arena);
        return 1;
    }
    
    printf("Arena stack initialized with %u bytes\n\n", HEAP_SIZE);
    
    /* Allocate some blocks */
    uint16_t block0 = arena_stack_allocate(arena, 100);
    printf("Allocated block %u (100 bytes)\n", block0);
    
    uint16_t block1 = arena_stack_allocate(arena, 200);
    printf("Allocated block %u (200 bytes)\n", block1);
    
    uint16_t block2 = arena_stack_allocate(arena, 150);
    printf("Allocated block %u (150 bytes)\n", block2);
    
    /* Display arena status */
    printf("\nArena Status:\n");
    printf("Total blocks: %u\n", arena_stack_get_block_count(arena));
    printf("Remaining space: %u bytes\n", arena_stack_get_remaining_space(arena));
    
    /* Check block sizes */
    printf("\nBlock Sizes:\n");
    printf("Block %u: %u bytes\n", block0, arena_stack_get_block_size(arena, block0));
    printf("Block %u: %u bytes\n", block1, arena_stack_get_block_size(arena, block1));
    printf("Block %u: %u bytes\n", block2, arena_stack_get_block_size(arena, block2));
    
    /* Write to a block */
    void* ptr = arena_stack_get_block_pointer(arena, block1);
    if (ptr) {
        strcpy((char*)ptr, "Hello, Arena!");
        printf("\nWritten to block %u: %s\n", block1, (char*)ptr);
    }
    
    /* Deallocate a block */
    if (arena_stack_deallocate(arena, block1)) {
        printf("\nDeallocated block %u\n", block1);
        printf("Total blocks: %u\n", arena_stack_get_block_count(arena));
    }
    
    /* Try to allocate after deallocation */
    uint16_t block3 = arena_stack_allocate(arena, 50);
    printf("\nAllocated block %u (50 bytes)\n", block3);
    printf("Remaining space: %u bytes\n", arena_stack_get_remaining_space(arena));
    
    /* Test error conditions */
    printf("\n--- Testing Error Conditions ---\n");
    
    /* Try to get size of deallocated block */
    uint16_t size = arena_stack_get_block_size(arena, block1);
    printf("Size of deallocated block %u: %u (should be 0)\n", block1, size);
    
    /* Try to deallocate non-existent block */
    bool result = arena_stack_deallocate(arena, 999);
    printf("Deallocate non-existent block: %s\n", result ? "true" : "false");
    
    /* Try to allocate too much memory */
    uint16_t failed_block = arena_stack_allocate(arena, 10000);
    if (failed_block == ARENA_STACK_INVALID_ID) {
        printf("Allocate 10000 bytes (too much): FAILED as expected (ARENA_STACK_INVALID_ID)\n");
    } else {
        printf("Allocate 10000 bytes: Unexpectedly succeeded with block_id = %u\n", failed_block);
    }
    
    /* Cleanup */
    arena_stack_destroy(arena);
    printf("\nArena stack destroyed\n");
    
    return 0;
}