#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdbool.h>
#include "arena_stack.h"

/* Block header structure - optimized for memory alignment */
typedef struct {
    uint16_t size;         /* Size of the block */
    uint16_t block_id;     /* Block ID */
    uint16_t offset;       /* Offset from base memory */
    bool is_allocated;     /* Allocation status */
    /* 1 byte padding here on most systems */
} block_header;

/* Arena stack structure - optimized for memory alignment
 * Pointers first (8 bytes on 64-bit, 4 bytes on 32-bit)
 * Then uint16_t fields (2 bytes each)
 */
struct arena_stack {
    void* base_memory;         /* Pointer to managed memory (8/4 bytes) */
    block_header* blocks;      /* Array of block headers (8/4 bytes) */
    uint16_t total_size;       /* Total size of managed memory */
    uint16_t used_size;        /* Currently used size */
    uint16_t next_block_id;    /* Next block ID to assign */
    uint16_t block_count;      /* Number of blocks */
    uint16_t block_capacity;   /* Capacity of blocks array */
    /* 2 bytes padding on 64-bit to align to 8-byte boundary */
};

/* Constructor-like function */
arena_stack* arena_stack_create(void) {
    arena_stack* self = (arena_stack*)malloc(sizeof(arena_stack));
    if (!self) {
        return NULL;
    }
    
    self->base_memory = NULL;
    self->blocks = NULL;
    self->total_size = 0;
    self->used_size = 0;
    self->next_block_id = 0;
    self->block_count = 0;
    self->block_capacity = 0;
    
    return self;
}

/* Initialize heap with a block of memory */
bool arena_stack_initialize(arena_stack* self, void* memory, uint16_t total_size) {
    if (!self || !memory || total_size == 0) {
        return false;
    }
    
    /* Clean up any existing blocks */
    if (self->blocks) {
        free(self->blocks);
    }
    
    self->base_memory = memory;
    self->total_size = total_size;
    self->used_size = 0;
    self->next_block_id = 0;
    self->block_count = 0;
    
    /* Allocate initial block array */
    self->block_capacity = 16;
    self->blocks = (block_header*)malloc(sizeof(block_header) * self->block_capacity);
    if (!self->blocks) {
        return false;
    }
    
    return true;
}

/* Helper function to expand block array if needed */
static bool expand_blocks(arena_stack* self) {
    if (self->block_count >= self->block_capacity) {
        uint16_t new_capacity = self->block_capacity * 2;
        block_header* new_blocks = (block_header*)realloc(
            self->blocks, 
            sizeof(block_header) * new_capacity
        );
        if (!new_blocks) {
            return false;
        }
        self->blocks = new_blocks;
        self->block_capacity = new_capacity;
    }
    return true;
}

/* Allocate a block of specified size */
uint16_t arena_stack_allocate(arena_stack* self, uint16_t size) {
    if (!self || !self->base_memory || size == 0) {
        return ARENA_STACK_INVALID_ID;
    }
    
    /* Check if we have enough space */
    if (self->used_size + size > self->total_size) {
        return ARENA_STACK_INVALID_ID;
    }
    
    /* Check if we've reached maximum block ID */
    if (self->next_block_id >= ARENA_STACK_INVALID_ID) {
        return ARENA_STACK_INVALID_ID;
    }
    
    /* Expand block array if needed */
    if (!expand_blocks(self)) {
        return ARENA_STACK_INVALID_ID;
    }
    
    /* Create new block */
    block_header* header = &self->blocks[self->block_count];
    header->size = size;
    header->block_id = self->next_block_id;
    header->is_allocated = true;
    header->offset = self->used_size;
    
    self->block_count++;
    self->used_size += size;
    
    return self->next_block_id++;
}

/* Get the total number of allocated blocks */
uint16_t arena_stack_get_block_count(const arena_stack* self) {
    if (!self) {
        return 0;
    }
    
    uint16_t count = 0;
    for (uint16_t i = 0; i < self->block_count; i++) {
        if (self->blocks[i].is_allocated) {
            count++;
        }
    }
    return count;
}

/* Get the size of a specific block by ID */
uint16_t arena_stack_get_block_size(const arena_stack* self, uint16_t block_id) {
    if (!self || !self->blocks) {
        return 0;
    }
    
    /* Quick check for invalid block ID */
    if (block_id >= ARENA_STACK_INVALID_ID) {
        return 0;
    }
    
    for (uint16_t i = 0; i < self->block_count; i++) {
        if (self->blocks[i].block_id == block_id && 
            self->blocks[i].is_allocated) {
            return self->blocks[i].size;
        }
    }
    return 0;  /* Block not found or not allocated */
}

/* Get remaining free space in heap */
uint16_t arena_stack_get_remaining_space(const arena_stack* self) {
    if (!self) {
        return 0;
    }
    
    if (self->total_size >= self->used_size) {
        return self->total_size - self->used_size;
    }
    return 0;
}

/* Deallocate/reclaim memory starting from block ID */
bool arena_stack_deallocate(arena_stack* self, uint16_t block_id) {
    if (!self || !self->blocks) {
        return false;
    }
    
    /* Quick check for invalid block ID */
    if (block_id >= ARENA_STACK_INVALID_ID) {
        return false;
    }
    
    for (uint16_t i = 0; i < self->block_count; i++) {
        if (self->blocks[i].block_id == block_id) {
            if (!self->blocks[i].is_allocated) {
                return false;  /* Already deallocated */
            }
            
            self->blocks[i].is_allocated = false;
            /* Note: This simple implementation doesn't reclaim space */
            /* A more sophisticated version would compact memory */
            
            return true;
        }
    }
    return false;  /* Block not found */
}

/* Get pointer to the actual memory block data */
void* arena_stack_get_block_pointer(const arena_stack* self, uint16_t block_id) {
    if (!self || !self->blocks || !self->base_memory) {
        return NULL;
    }
    
    /* Quick check for invalid block ID */
    if (block_id >= ARENA_STACK_INVALID_ID) {
        return NULL;
    }
    
    for (uint16_t i = 0; i < self->block_count; i++) {
        if (self->blocks[i].block_id == block_id && 
            self->blocks[i].is_allocated) {
            return (char*)self->base_memory + self->blocks[i].offset;
        }
    }
    return NULL;  /* Block not found or not allocated */
}

/* Destructor-like function */
void arena_stack_destroy(arena_stack* self) {
    if (self) {
        if (self->blocks) {
            free(self->blocks);
        }
        free(self);
    }
}