/* ============= UPDATED cfl_heap_arena_allocate.c ============= */
#include "cfl_heap_arena_allocate.h"
#include "cfl_heap.h"
#include "cfl_perm.h"
#include "cfl_exception.h"
#include <string.h>

#include "cfl_global_definitions.h"

/* Configuration - use generated values or defaults */
#ifndef MAX_ALLOCATORS
#define MAX_ALLOCATORS      254      // Default: support 254 concurrent arenas
#endif

#ifndef ARENA_ALIGNMENT
#define ARENA_ALIGNMENT     4        // Default: 4-byte alignment
#endif

#define NO_ALLOCATOR        0xFF
    
#define INVALID_MEMORY_IDX  0xFFFF

/* Arena control block - optimally packed */
typedef struct CflHeapArenaControl {
    uint16_t memory_idx;       // 2 bytes - heap index to data block
    uint16_t size;             // 2 bytes - size of data block
    uint16_t used;             // 2 bytes - bump pointer offset
    uint16_t owner_node_id;    // 2 bytes - which node owns this
    cfl_heap_allocator_id_t id; // 1 byte  - allocator ID
    uint8_t pad;               // 1 byte  - explicit padding
} CflHeapArenaControl;  // Total: 10 bytes (compiler adds 2 for 4-byte alignment = 12 bytes)

/* ============= INTERNAL HELPERS ============= */

static inline uint16_t align_up(uint16_t value, uint8_t alignment) {
    return (value + alignment - 1) & ~(alignment - 1);
}


static inline cfl_heap_allocator_id_t alloc_id(CflHeapArenaSystem* sys) {
    for (uint16_t i = 0; i < MAX_ALLOCATORS; ++i) {
        cfl_heap_allocator_id_t id = (sys->next_allocator_id + i) % MAX_ALLOCATORS;
        if (sys->arenas[id] == NULL) {
            sys->next_allocator_id = (id + 1) % MAX_ALLOCATORS;
            return id;
        }
    }
    EXCEPTION("alloc_id: All allocator IDs exhausted - increase MAX_ALLOCATORS");
    return NO_ALLOCATOR; // Never reached
}


static inline void free_id(CflHeapArenaSystem* sys, cfl_heap_allocator_id_t id) {
    if (id < MAX_ALLOCATORS) {
        sys->arenas[id] = NULL;
    }
}

static inline cfl_heap_arena_t get_arena_by_id(CflHeapArenaSystem* sys, cfl_heap_allocator_id_t id) {
    if (id >= MAX_ALLOCATORS) {
        EXCEPTION("get_arena_by_id: Invalid allocator ID");
    }
    return sys->arenas[id];
}

/* ============= PUBLIC API ============= */

CflHeapArenaSystem* cfl_heap_arena_system_create(CflPerm* perm, cfl_heap_t* heap, 
                                                   uint16_t max_allocator_count, uint16_t total_node_count,
                                                   uint16_t allocator_0_size) {
    if (!perm) {
        EXCEPTION("cfl_heap_arena_system_create: NULL perm pointer");
    }
    
    if (!heap) {
        EXCEPTION("cfl_heap_arena_system_create: NULL heap pointer");
    }
    
    if (allocator_0_size == 0) {
        EXCEPTION("cfl_heap_arena_system_create: allocator_0_size is zero");
    }

    // Allocate system structure from permanent allocator
    CflHeapArenaSystem* sys = (CflHeapArenaSystem*)cfl_perm_alloc_pointer(perm, sizeof(CflHeapArenaSystem));
    if (!sys) {
        EXCEPTION("cfl_heap_arena_system_create: Failed to allocate system structure");
    }
    
    sys->heap = heap;
    sys->max_allocator_count = max_allocator_count;
    
    // Allocate arena table from permanent allocator
    sys->arenas = (cfl_heap_arena_t*)cfl_perm_alloc_pointer(perm, max_allocator_count * sizeof(cfl_heap_arena_t));
    if (!sys->arenas) {
        EXCEPTION("cfl_heap_arena_system_create: Failed to allocate arena table");
    }
    memset(sys->arenas, 0, max_allocator_count * sizeof(cfl_heap_arena_t));
    
    sys->next_allocator_id = 1;  // Start at 1, since 0 is reserved
    sys->active_allocator_context = NO_ALLOCATOR;
    
    // Allocate node arrays from permanent allocator
    sys->node_allocator_ids = (uint8_t*)cfl_perm_alloc_pointer(perm, total_node_count * sizeof(uint8_t));
    if (!sys->node_allocator_ids) {
        EXCEPTION("cfl_heap_arena_system_create: Failed to allocate node_allocator_ids");
    }
    
    sys->node_memory_index = (uint16_t*)cfl_perm_alloc_pointer(perm, total_node_count * sizeof(uint16_t));
    if (!sys->node_memory_index) {
        EXCEPTION("cfl_heap_arena_system_create: Failed to allocate node_memory_index");
    }
    
    sys->total_node_count = total_node_count;
    
    // Initialize arrays - all nodes start unassigned
    memset(sys->node_allocator_ids, 0, total_node_count * sizeof(uint8_t));
    for (uint16_t i = 0; i < total_node_count; ++i) {
        sys->node_memory_index[i] = 0xFFFF;
    }
    
    // Create allocator 0 - permanent, cannot be destroyed
    allocator_0_size = align_up(allocator_0_size, ARENA_ALIGNMENT);
    
    // Allocate control block from perm (persistent)
    CflHeapArenaControl* arena0 = (CflHeapArenaControl*)cfl_perm_alloc_pointer(perm, sizeof(CflHeapArenaControl));
    if (!arena0) {
        EXCEPTION("cfl_heap_arena_system_create: Failed to allocate allocator 0 control block");
    }
    
    // Allocate buffer from perm (persistent) - NOT from heap
    void* arena0_buffer = cfl_perm_alloc_pointer(perm, allocator_0_size);
    if (!arena0_buffer) {
        EXCEPTION("cfl_heap_arena_system_create: Failed to allocate allocator 0 buffer");
    }
    
    // Store allocator 0 buffer pointer in system
    sys->allocator_0_buffer = arena0_buffer;
    
    // Initialize arena 0 - use memory_idx=0 as marker for perm-allocated
    arena0->memory_idx = 0; // Special marker: 0 means use sys->allocator_0_buffer
    arena0->size = allocator_0_size;
    arena0->used = 0;
    arena0->owner_node_id = 0xFFFF; // Special owner - system owned
    arena0->id = 0;
    arena0->pad = 0;
    
    // Register allocator 0
    sys->arenas[0] = arena0;
    
    return sys;
}

void cfl_heap_arena_system_reset(CflHeapArenaSystem* sys) {
    if (!sys) {
        EXCEPTION("cfl_heap_arena_system_reset: NULL system pointer");
    }
    
    if (!sys->node_allocator_ids || !sys->node_memory_index) {
        EXCEPTION("cfl_heap_arena_system_reset: Arena system not initialized");
    }
    
    // Destroy all allocators except 0
    for (uint16_t i = 1; i < sys->max_allocator_count; ++i) {
        if (sys->arenas[i]) {
            cfl_heap_arena_t arena = sys->arenas[i];
            
            // Free heap-allocated data block
            cfl_heap_free(sys->heap, arena->memory_idx);
            
            // Clear from arena table
            sys->arenas[i] = NULL;
        }
    }
    
    // Reset allocator 0 buffer (bump pointer back to start)
    if (sys->arenas[0]) {
        sys->arenas[0]->used = 0;
        
        // Clear the buffer
        memset(sys->allocator_0_buffer, 0, sys->arenas[0]->size);
    }
    
    // Reinitialize node arrays
    memset(sys->node_allocator_ids, 0, sys->total_node_count * sizeof(uint8_t));
    for (uint16_t i = 0; i < sys->total_node_count; ++i) {
        sys->node_memory_index[i] = 0xFFFF;
    }
    
    // Reset ID allocation (start at 1, since 0 is reserved)
    sys->next_allocator_id = 1;
    sys->active_allocator_context = NO_ALLOCATOR;
}

cfl_heap_arena_t cfl_heap_arena_create(CflPerm* perm, CflHeapArenaSystem* sys, uint16_t owner_node_id, uint16_t size_bytes) {
    if (!perm) {
        EXCEPTION("cfl_heap_arena_create: NULL perm pointer");
    }
    
    if (!sys) {
        EXCEPTION("cfl_heap_arena_create: NULL system pointer");
    }
    
    if (size_bytes == 0) {
        EXCEPTION("cfl_heap_arena_create: size_bytes is zero");
    }
    
    if (!sys->node_allocator_ids) {
        EXCEPTION("cfl_heap_arena_create: Arena system not initialized");
    }
    
    if (owner_node_id >= sys->total_node_count) {
        EXCEPTION("cfl_heap_arena_create: owner_node_id out of bounds");
    }
    
    size_bytes = align_up(size_bytes, ARENA_ALIGNMENT);
    
    // Allocate control block from permanent allocator (persistent)
    CflHeapArenaControl* arena = (CflHeapArenaControl*)cfl_perm_alloc_pointer(perm, sizeof(CflHeapArenaControl));
    if (!arena) {
        EXCEPTION("cfl_heap_arena_create: Failed to allocate arena control block");
    }
    
    // Allocate data block from heap (dynamic/temporary)
    uint16_t data_idx = cfl_heap_malloc(sys->heap, size_bytes);
    if (data_idx == INVALID_HEAP_IDX) {
        EXCEPTION("cfl_heap_arena_create: Heap exhausted - failed to allocate data block");
    }
    
    // Get allocator ID (alloc_id throws exception if exhausted)
    cfl_heap_allocator_id_t id = alloc_id(sys);
    
    // Initialize arena
    arena->memory_idx = data_idx;
    arena->size = size_bytes;
    arena->used = 0;
    arena->owner_node_id = owner_node_id;
    arena->id = id;
    
    // Register
    sys->arenas[id] = arena;
    sys->node_allocator_ids[owner_node_id] = id;
    
    return arena;
}

void cfl_heap_arena_destroy(CflHeapArenaSystem* sys, cfl_heap_arena_t arena, uint16_t owner_node_id) {
    if (!sys) {
        EXCEPTION("cfl_heap_arena_destroy: NULL system pointer");
    }
    
    if (!arena) {
        EXCEPTION("cfl_heap_arena_destroy: NULL arena pointer");
    }
    
    if (!sys->node_allocator_ids) {
        EXCEPTION("cfl_heap_arena_destroy: Arena system not initialized");
    }
    
    // CRITICAL: Cannot destroy allocator 0
    if (arena->id == 0) {
        EXCEPTION("cfl_heap_arena_destroy: Cannot destroy allocator 0 (permanent allocator)");
    }
    
    // Ownership validation - only owner can destroy
    if (arena->owner_node_id != owner_node_id) {
        EXCEPTION("cfl_heap_arena_destroy: Node does not own this arena");
    }
    
    cfl_heap_allocator_id_t id = arena->id;
    
    // Scan and clear all nodes using this allocator
    for (uint16_t i = 0; i < sys->total_node_count; ++i) {
        if (sys->node_allocator_ids[i] == id) {
            sys->node_allocator_ids[i] = 0;
            sys->node_memory_index[i] = 0xFFFF;
        }
    }
    
    // Clear context if this was the active allocator
    if (sys->active_allocator_context == id) {
        sys->active_allocator_context = NO_ALLOCATOR;
    }
    
    // Free data block from heap
    cfl_heap_free(sys->heap, arena->memory_idx);
    
    // Note: Control block is allocated from perm, so we don't free it
    // Just clear from arena table
    sys->arenas[id] = NULL;
    
    // Free allocator ID for reuse
    free_id(sys, id);
}

cfl_heap_allocator_id_t cfl_heap_arena_get_id(cfl_heap_arena_t arena) {
    if (!arena) {
        EXCEPTION("cfl_heap_arena_get_id: NULL arena pointer");
    }
    return arena->id;
}

void cfl_heap_arena_set_active_allocator(CflHeapArenaSystem* sys, uint16_t owner_node_id) {
    if (!sys) {
        EXCEPTION("cfl_heap_arena_set_active_allocator: NULL system pointer");
    }
    
    if (!sys->node_allocator_ids) {
        EXCEPTION("cfl_heap_arena_set_active_allocator: Arena system not initialized");
    }
    
    if (owner_node_id >= sys->total_node_count) {
        EXCEPTION("cfl_heap_arena_set_active_allocator: owner_node_id out of bounds");
    }
    
    // Set the execution context to this node's allocator
    sys->active_allocator_context = sys->node_allocator_ids[owner_node_id];
}

void cfl_heap_arena_set_node_allocator(CflHeapArenaSystem* sys, uint16_t requesting_node_id) {
    if (!sys) {
        EXCEPTION("cfl_heap_arena_set_node_allocator: NULL system pointer");
    }
    
    if (!sys->node_allocator_ids) {
        EXCEPTION("cfl_heap_arena_set_node_allocator: Arena system not initialized");
    }
    
    if (requesting_node_id >= sys->total_node_count) {
        EXCEPTION("cfl_heap_arena_set_node_allocator: requesting_node_id out of bounds");
    }
    
    // Capture current allocator context
    sys->node_allocator_ids[requesting_node_id] = sys->active_allocator_context;
}

void* cfl_arena_system_alloc(CflHeapArenaSystem* sys, uint16_t requesting_node_id, uint16_t size_bytes) {
    return cfl_arena_system_alloc_aligned(sys, requesting_node_id, size_bytes, ARENA_ALIGNMENT);
}

void* cfl_arena_system_alloc_aligned(CflHeapArenaSystem* sys, uint16_t requesting_node_id, uint16_t size_bytes, uint8_t alignment) {
    if (!sys) {
        EXCEPTION("cfl_arena_system_alloc_aligned: NULL system pointer");
    }
    
    if (size_bytes == 0) {
        EXCEPTION("cfl_arena_system_alloc_aligned: size_bytes is zero");
    }
    
    if (!sys->node_allocator_ids) {
        EXCEPTION("cfl_arena_system_alloc_aligned: Arena system not initialized");
    }
    
    if (requesting_node_id >= sys->total_node_count) {
        EXCEPTION("cfl_arena_system_alloc_aligned: requesting_node_id out of bounds");
    }
    
    cfl_heap_allocator_id_t alloc_id = sys->node_allocator_ids[requesting_node_id];
    if (alloc_id == NO_ALLOCATOR) {
        EXCEPTION("cfl_arena_system_alloc_aligned: Node has no allocator assigned");
    }
    
    cfl_heap_arena_t arena = get_arena_by_id(sys, alloc_id);
    if (!arena) {
        EXCEPTION("cfl_arena_system_alloc_aligned: Invalid arena for allocator ID");
    }
    
    // Get pointer to data block base
    uint8_t* base;
    if (arena->id == 0) {
        // Allocator 0 uses permanent buffer
        base = (uint8_t*)sys->allocator_0_buffer;
    } else {
        // Other allocators use heap-allocated buffers
        base = (uint8_t*)cfl_heap_ptr(sys->heap, arena->memory_idx);
    }
    
    // Calculate aligned pointer address (align the ABSOLUTE address, not the offset)
    uintptr_t current_addr = (uintptr_t)(base + arena->used);
    uintptr_t aligned_addr = (current_addr + alignment - 1) & ~(uintptr_t)(alignment - 1);
    uint16_t padding = (uint16_t)(aligned_addr - current_addr);
    
    uint16_t total_needed = padding + size_bytes;
    
    // Resource exhaustion - arena is too small for workload
    if (arena->used + total_needed > arena->size) {
        EXCEPTION("cfl_arena_system_alloc_aligned: Arena exhausted - insufficient space");
    }
    
    void* ptr = (void*)aligned_addr;
    
    // Store the aligned offset for this node (for future reuse)
    sys->node_memory_index[requesting_node_id] = arena->used + padding;
    
    // Update arena used pointer (includes padding)
    arena->used += total_needed;
    
    return ptr;
}

/* ============= NODE ARRAY ACCESSORS ============= */

cfl_heap_allocator_id_t cfl_heap_arena_get_node_allocator_id(CflHeapArenaSystem* sys, uint16_t node_id) {
    if (!sys) {
        EXCEPTION("cfl_heap_arena_get_node_allocator_id: NULL system pointer");
    }
    
    if (!sys->node_allocator_ids) {
        EXCEPTION("cfl_heap_arena_get_node_allocator_id: Arena system not initialized");
    }
    
    if (node_id >= sys->total_node_count) {
        EXCEPTION("cfl_heap_arena_get_node_allocator_id: node_id out of bounds");
    }
    
    return sys->node_allocator_ids[node_id];
}

void cfl_heap_arena_set_node_allocator_id(CflHeapArenaSystem* sys, uint16_t node_id, cfl_heap_allocator_id_t allocator_id) {
    if (!sys) {
        EXCEPTION("cfl_heap_arena_set_node_allocator_id: NULL system pointer");
    }
    
    if (!sys->node_allocator_ids) {
        EXCEPTION("cfl_heap_arena_set_node_allocator_id: Arena system not initialized");
    }
    
    if (node_id >= sys->total_node_count) {
        EXCEPTION("cfl_heap_arena_set_node_allocator_id: node_id out of bounds");
    }
    
    sys->node_allocator_ids[node_id] = allocator_id;
}

uint16_t cfl_heap_arena_get_node_memory_index(CflHeapArenaSystem* sys, uint16_t node_id) {
    if (!sys) {
        EXCEPTION("cfl_heap_arena_get_node_memory_index: NULL system pointer");
    }
    
    if (!sys->node_memory_index) {
        EXCEPTION("cfl_heap_arena_get_node_memory_index: Arena system not initialized");
    }
    
    if (node_id >= sys->total_node_count) {
        EXCEPTION("cfl_heap_arena_get_node_memory_index: node_id out of bounds");
    }
    
    return sys->node_memory_index[node_id];
}

void cfl_heap_arena_set_node_memory_index(CflHeapArenaSystem* sys, uint16_t node_id, uint16_t memory_idx) {
    if (!sys) {
        EXCEPTION("cfl_heap_arena_set_node_memory_index: NULL system pointer");
    }
    
    if (!sys->node_memory_index) {
        EXCEPTION("cfl_heap_arena_set_node_memory_index: Arena system not initialized");
    }
    
    if (node_id >= sys->total_node_count) {
        EXCEPTION("cfl_heap_arena_set_node_memory_index: node_id out of bounds");
    }
    
    sys->node_memory_index[node_id] = memory_idx;
}

/* ============= DIAGNOSTICS ============= */

uint16_t cfl_heap_arena_used_bytes(cfl_heap_arena_t arena) {
    if (!arena) {
        EXCEPTION("cfl_heap_arena_used_bytes: NULL arena pointer");
    }
    return arena->used;
}

uint16_t cfl_heap_arena_free_bytes(cfl_heap_arena_t arena) {
    if (!arena) {
        EXCEPTION("cfl_heap_arena_free_bytes: NULL arena pointer");
    }
    return arena->size - arena->used;
}

void cfl_heap_arena_dump_stats(CflHeapArenaSystem* sys) {
    if (!sys) {
        EXCEPTION("cfl_heap_arena_dump_stats: NULL system pointer");
    }
    
    uint16_t active_count = 0;
    uint16_t total_data_allocated = 0;
    uint16_t total_data_used = 0;
    
    for (uint16_t i = 0; i < MAX_ALLOCATORS; ++i) {
        if (sys->arenas[i]) {
            active_count++;
            total_data_allocated += sys->arenas[i]->size;
            total_data_used += sys->arenas[i]->used;
        }
    }
    
    // Stats are now available in local variables
    // Can be used by caller or printed if needed
}