#include "arena_alloc.h"
#include <stdlib.h>
#include <string.h>
#include <stdio.h>
#include <assert.h>

// Forward declarations of virtual methods
static uint32_t arena_alloc_impl(void* self, uint32_t size);
static uint32_t arena_alloc_aligned_impl(void* self, uint32_t size, uint32_t alignment);
static void* arena_to_ptr_impl(void* self, uint32_t offset);
static uint32_t arena_to_offset_impl(void* self, const void* ptr);
static void arena_reset_impl(void* self);
static void arena_stats_impl(void* self, uint32_t* used, uint32_t* capacity);
static bool arena_is_valid_impl(void* self, uint32_t offset);
static void arena_destroy_impl(void* self);

// Global virtual table (shared by all instances)
static const ArenaVTable g_arena_vtable = {
    .alloc = arena_alloc_impl,
    .alloc_aligned = arena_alloc_aligned_impl,
    .to_ptr = arena_to_ptr_impl,
    .to_offset = arena_to_offset_impl,
    .reset = arena_reset_impl,
    .stats = arena_stats_impl,
    .is_valid = arena_is_valid_impl,
    .destroy = arena_destroy_impl
};

// Constructor - allocates buffer
ArenaAllocator* arena_create(uint32_t capacity) {
    // Ensure capacity is 4-byte aligned
    capacity = ARENA_ALIGN(capacity);
    
    // Allocate arena structure
    ArenaAllocator* arena = (ArenaAllocator*)malloc(sizeof(ArenaAllocator));
    if (!arena) {
        fprintf(stderr, "Failed to allocate arena structure\n");
        return NULL;
    }
    
    // Allocate aligned buffer
    arena->buffer = (uint8_t*)aligned_alloc(ARENA_ALIGNMENT, capacity);
    if (!arena->buffer) {
        fprintf(stderr, "Failed to allocate arena buffer of %u bytes\n", capacity);
        free(arena);
        return NULL;
    }
    
    // Initialize
    arena->vtable = &g_arena_vtable;
    arena->capacity = capacity;
    arena->used = 0;
    arena->owns_buffer = true;
    
    printf("✓ Arena created: %u bytes at %p\n", capacity, (void*)arena->buffer);
    
    return arena;
}

// Constructor - uses provided buffer
ArenaAllocator* arena_create_from_buffer(void* buffer, uint32_t capacity) {
    if (!buffer) {
        fprintf(stderr, "Null buffer provided\n");
        return NULL;
    }
    
    // Check alignment
    if ((uintptr_t)buffer & (ARENA_ALIGNMENT - 1)) {
        fprintf(stderr, "Buffer not %d-byte aligned\n", ARENA_ALIGNMENT);
        return NULL;
    }
    
    // Ensure capacity is 4-byte aligned
    capacity = ARENA_ALIGN(capacity);
    
    ArenaAllocator* arena = (ArenaAllocator*)malloc(sizeof(ArenaAllocator));
    if (!arena) {
        fprintf(stderr, "Failed to allocate arena structure\n");
        return NULL;
    }
    
    arena->vtable = &g_arena_vtable;
    arena->buffer = (uint8_t*)buffer;
    arena->capacity = capacity;
    arena->used = 0;
    arena->owns_buffer = false;
    
    printf("✓ Arena created from buffer: %u bytes at %p\n", capacity, buffer);
    
    return arena;
}

// Virtual method implementations

static uint32_t arena_alloc_impl(void* self, uint32_t size) {
    return arena_alloc_aligned_impl(self, size, ARENA_ALIGNMENT);
}

static uint32_t arena_alloc_aligned_impl(void* self, uint32_t size, uint32_t alignment) {
    ArenaAllocator* arena = (ArenaAllocator*)self;
    
    // Validate alignment (must be power of 2)
    if (alignment == 0 || (alignment & (alignment - 1)) != 0) {
        fprintf(stderr, "Invalid alignment: %u\n", alignment);
        return ARENA_OFFSET_INVALID;
    }
    
    // ARM requires at least 4-byte alignment
    if (alignment < ARENA_ALIGNMENT) {
        alignment = ARENA_ALIGNMENT;
    }
    
    // Align current position
    uint32_t aligned_used = (arena->used + alignment - 1) & ~(alignment - 1);
    
    // Align size
    uint32_t aligned_size = (size + ARENA_ALIGNMENT - 1) & ~(ARENA_ALIGNMENT - 1);
    
    // Check capacity
    if (aligned_used + aligned_size > arena->capacity) {
        fprintf(stderr, "Arena out of memory: requested %u, available %u\n",
                aligned_size, arena->capacity - aligned_used);
        return ARENA_OFFSET_NULL;
    }
    
    // Allocate
    uint32_t offset = aligned_used;
    arena->used = aligned_used + aligned_size;
    
    // Verify alignment
    assert((offset & (alignment - 1)) == 0);
    assert(((uintptr_t)(arena->buffer + offset) & (alignment - 1)) == 0);
    
    return offset;
}

static void* arena_to_ptr_impl(void* self, uint32_t offset) {
    ArenaAllocator* arena = (ArenaAllocator*)self;
    
    if (offset >= arena->capacity) {
        return NULL;
    }
    
    return arena->buffer + offset;
}

static uint32_t arena_to_offset_impl(void* self, const void* ptr) {
    ArenaAllocator* arena = (ArenaAllocator*)self;
    
    if (!ptr) {
        return ARENA_OFFSET_NULL;
    }
    
    // Check if pointer is within arena
    const uint8_t* byte_ptr = (const uint8_t*)ptr;
    if (byte_ptr < arena->buffer || byte_ptr >= arena->buffer + arena->capacity) {
        fprintf(stderr, "Pointer %p not in arena [%p, %p)\n",
                ptr, (void*)arena->buffer, (void*)(arena->buffer + arena->capacity));
        return ARENA_OFFSET_INVALID;
    }
    
    return (uint32_t)(byte_ptr - arena->buffer);
}

static void arena_reset_impl(void* self) {
    ArenaAllocator* arena = (ArenaAllocator*)self;
    arena->used = 0;
    printf("✓ Arena reset\n");
}

static void arena_stats_impl(void* self, uint32_t* used, uint32_t* capacity) {
    ArenaAllocator* arena = (ArenaAllocator*)self;
    
    if (used) *used = arena->used;
    if (capacity) *capacity = arena->capacity;
}

static bool arena_is_valid_impl(void* self, uint32_t offset) {
    ArenaAllocator* arena = (ArenaAllocator*)self;
    
    return offset < arena->capacity && 
           offset != ARENA_OFFSET_NULL && 
           offset != ARENA_OFFSET_INVALID;
}

static void arena_destroy_impl(void* self) {
    ArenaAllocator* arena = (ArenaAllocator*)self;
    
    if (arena->owns_buffer && arena->buffer) {
        free(arena->buffer);
        printf("✓ Arena buffer freed\n");
    }
    
    free(arena);
    printf("✓ Arena destroyed\n");
}

// Public wrapper functions (for direct calling without vtable)

uint32_t arena_alloc(void* self, uint32_t size) {
    return arena_alloc_impl(self, size);
}

uint32_t arena_alloc_aligned(void* self, uint32_t size, uint32_t alignment) {
    return arena_alloc_aligned_impl(self, size, alignment);
}

void* arena_to_ptr(void* self, uint32_t offset) {
    return arena_to_ptr_impl(self, offset);
}

uint32_t arena_to_offset(void* self, const void* ptr) {
    return arena_to_offset_impl(self, ptr);
}

void arena_reset(void* self) {
    arena_reset_impl(self);
}

void arena_stats(void* self, uint32_t* used, uint32_t* capacity) {
    arena_stats_impl(self, used, capacity);
}

bool arena_is_valid(void* self, uint32_t offset) {
    return arena_is_valid_impl(self, offset);
}

void arena_destroy(void* self) {
    arena_destroy_impl(self);
}

