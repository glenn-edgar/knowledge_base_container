/* ============= UPDATED arena_alloc.c ============= */
#include "cfl_heap_arena_allocate.h"
#include "cfl_heap.h"
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


CflHeapArenaSystem* cfl_heap_arena_system_create(cfl_heap_t* heap, uint16_t max_allocator_count, uint16_t total_node_count){

    CflHeapArenaSystem* sys = (CflHeapArenaSystem*)cfl_heap_malloc_pointer(heap, sizeof(CflHeapArenaSystem));
    if (!sys) {
        EXCEPTION("cfl_heap_arena_system_create: Failed to allocate memory");
    }
    sys->heap = heap;
    sys->max_allocator_count = max_allocator_count;
    sys->arenas= (cfl_heap_arena_t*)cfl_heap_malloc_pointer(heap, max_allocator_count * sizeof(cfl_heap_arena_t*));
    if (!sys->arenas) {
        EXCEPTION("cfl_heap_arena_system_create: Failed to allocate memory");
    }
    memset(sys->arenas, 0, max_allocator_count * sizeof(cfl_heap_arena_t*));
    sys->next_allocator_id = 0;
    sys->active_allocator_context = NO_ALLOCATOR;
    sys->node_allocator_ids = (uint8_t*)cfl_heap_malloc_pointer(heap, total_node_count * sizeof(uint8_t));
    sys->node_memory_index = (uint16_t*)cfl_heap_malloc_pointer(heap, total_node_count * sizeof(uint16_t));
    sys->total_node_count = total_node_count;
    memset(sys->node_allocator_ids, NO_ALLOCATOR, total_node_count * sizeof(uint8_t));
    memset(sys->node_memory_index, INVALID_MEMORY_IDX, total_node_count * sizeof(uint16_t));
    return sys;
}



#if 0
/* ============= PUBLIC API ============= */

void cfl_heap_arena_system_init(CflHeapArenaSystem* sys, 
                                  uint16_t total_nodes, 
                                  uint8_t* allocator_id_array,
                                  uint16_t* memory_index) {
    if (!sys) {
        EXCEPTION("cfl_heap_arena_system_init: NULL system pointer");
    }
    
    if (!allocator_id_array) {
        EXCEPTION("cfl_heap_arena_system_init: NULL allocator_id_array");
    }
    
    if (!memory_index) {
        EXCEPTION("cfl_heap_arena_system_init: NULL memory_index");
    }
    

    
    memset(sys->active_arenas, 0, sys->max_allocator_count * sizeof(cfl_heap_arena_t));
    
    sys->next_allocator_id = 0;
    sys->active_allocator_context = NO_ALLOCATOR;
    sys->node_allocator_ids = allocator_id_array;
    sys->node_memory_index = memory_index;
    sys->total_node_count = total_nodes;
    
    // Initialize allocator IDs to NO_ALLOCATOR
    memset(sys->node_allocator_ids, NO_ALLOCATOR, total_nodes);
    
    // Initialize memory indices to INVALID_MEMORY_IDX (0xFFFF)
    for (uint16_t i = 0; i < total_nodes; ++i) {
        sys->node_memory_index[i] = INVALID_MEMORY_IDX;
    }
}
#endif

cfl_heap_arena_t cfl_heap_arena_create(CflHeapArenaSystem* sys, uint16_t owner_node_id, uint16_t size_bytes) {
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
    
    // Allocate control block (returns index)
    uint16_t ctrl_idx = cfl_heap_malloc(sys->heap, sizeof(CflHeapArenaControl));
    if (ctrl_idx == INVALID_HEAP_IDX) {
        EXCEPTION("cfl_heap_arena_create: Heap exhausted - failed to allocate control block");
    }
    
    CflHeapArenaControl* arena = (CflHeapArenaControl*)cfl_heap_malloc_pointer(sys->heap, sizeof(CflHeapArenaControl));
    
    // Allocate data block (returns index)
    uint16_t data_idx = cfl_heap_malloc(sys->heap, size_bytes);
    if (data_idx == INVALID_HEAP_IDX) {
       //cfl_heap_free(sys->heap, ctrl_idx); no need crashing now
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
    
    // Ownership validation - only owner can destroy
    if (arena->owner_node_id != owner_node_id) {
        EXCEPTION("cfl_heap_arena_destroy: Node does not own this arena");
    }
    
    cfl_heap_allocator_id_t id = arena->id;
    
    // Scan and clear all nodes using this allocator
    for (uint16_t i = 0; i < sys->total_node_count; ++i) {
        if (sys->node_allocator_ids[i] == id) {
            sys->node_allocator_ids[i] = NO_ALLOCATOR;
            sys->node_memory_index[i] = INVALID_MEMORY_IDX;
        }
    }
    
    // Clear context if this was the active allocator
    if (sys->active_allocator_context == id) {
        sys->active_allocator_context = NO_ALLOCATOR;
    }
    
    // Free data block
    cfl_heap_free(sys->heap, arena->memory_idx);
    
    // Free control block (need to convert pointer back to index)
    uint16_t ctrl_idx = cfl_heap_ptr_to_idx(sys->heap, (void*)arena);
    cfl_heap_free(sys->heap, ctrl_idx);
    
    // Free allocator ID
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

void* cfl_heap_arena_alloc(CflHeapArenaSystem* sys, uint16_t requesting_node_id, uint16_t size_bytes) {
    return cfl_heap_arena_alloc_aligned(sys, requesting_node_id, size_bytes, ARENA_ALIGNMENT);
}

void* cfl_heap_arena_alloc_aligned(CflHeapArenaSystem* sys, uint16_t requesting_node_id, uint16_t size_bytes, uint8_t alignment) {
    if (!sys) {
        EXCEPTION("cfl_heap_arena_alloc_aligned: NULL system pointer");
    }
    
    if (size_bytes == 0) {
        EXCEPTION("cfl_heap_arena_alloc_aligned: size_bytes is zero");
    }
    
    if (!sys->node_allocator_ids) {
        EXCEPTION("cfl_heap_arena_alloc_aligned: Arena system not initialized");
    }
    
    if (requesting_node_id >= sys->total_node_count) {
        EXCEPTION("cfl_heap_arena_alloc_aligned: requesting_node_id out of bounds");
    }
    
    cfl_heap_allocator_id_t alloc_id = sys->node_allocator_ids[requesting_node_id];
    if (alloc_id == NO_ALLOCATOR) {
        EXCEPTION("cfl_heap_arena_alloc_aligned: Node has no allocator assigned");
    }
    
    cfl_heap_arena_t arena = get_arena_by_id(sys, alloc_id);
    if (!arena) {
        EXCEPTION("cfl_heap_arena_alloc_aligned: Invalid arena for allocator ID");
    }
    
    // New allocation needed
    uint16_t aligned_used = align_up(arena->used, alignment);
    
    // Resource exhaustion - arena is too small for workload
    if (aligned_used + size_bytes > arena->size) {
        EXCEPTION("cfl_heap_arena_alloc_aligned: Arena exhausted - insufficient space");
    }
    
    // Get pointer to data block using index
    uint8_t* base = (uint8_t*)cfl_heap_ptr(arena->memory_idx);
    void* ptr = base + aligned_used;
    
    // Store the offset for this node (for future reuse)
    sys->node_memory_index[requesting_node_id] = aligned_used;
    
    // Update arena used pointer
    arena->used = aligned_used + size_bytes;
    
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
        if (sys->active_arenas[i]) {
            active_count++;
            total_data_allocated += sys->active_arenas[i]->size;
            total_data_used += sys->active_arenas[i]->used;
        }
    }
    
    cfl_heap_dump_stats();
}
#endif