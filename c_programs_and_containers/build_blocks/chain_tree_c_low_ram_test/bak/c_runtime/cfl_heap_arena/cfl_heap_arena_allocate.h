/* ============= UPDATED arena_alloc.h ============= */
#ifndef ARENA_ALLOC_H
#define ARENA_ALLOC_H

#include <stdint.h>
#include <stdbool.h>

/* Include generated definitions (if available) */


/* Allocator ID type */
typedef uint8_t cfl_heap_allocator_id_t;

/* Forward declaration of internal types */
typedef struct CflHeapArenaControl* cfl_heap_arena_t;
#define INVALID_ARENA NULL

/* Arena system instance - encapsulates all global state */
typedef struct CflHeapArenaSystem {
    cfl_heap_arena_t active_arenas[MAX_ALLOCATORS];  // Arena lookup table
    uint8_t* node_allocator_ids;                     // Node-to-allocator mapping
    uint16_t* node_memory_index;                     // Node memory offset within arena
    uint16_t total_node_count;                       // Total nodes in system
    cfl_heap_allocator_id_t next_allocator_id;       // Next ID to allocate
    cfl_heap_allocator_id_t active_allocator_context; // Current execution context
} CflHeapArenaSystem;

/* System initialization */
void cfl_heap_arena_system_init(CflHeapArenaSystem* sys, 
                                  uint16_t total_nodes, 
                                  uint8_t* allocator_id_array,
                                  uint16_t* memory_index);

/* Arena lifecycle */
cfl_heap_arena_t cfl_heap_arena_create(CflHeapArenaSystem* sys, uint16_t owner_node_id, uint16_t size_bytes);
void cfl_heap_arena_destroy(CflHeapArenaSystem* sys, cfl_heap_arena_t arena, uint16_t owner_node_id);

/* Get allocator ID from arena */
cfl_heap_allocator_id_t cfl_heap_arena_get_id(cfl_heap_arena_t arena);

/* Allocator context management (for tree walker execution) */
void cfl_heap_arena_set_active_allocator(CflHeapArenaSystem* sys, uint16_t owner_node_id);
void cfl_heap_arena_set_node_allocator(CflHeapArenaSystem* sys, uint16_t requesting_node_id);

/* Child allocation - reuses existing allocation if available */
void* cfl_heap_arena_alloc(CflHeapArenaSystem* sys, uint16_t requesting_node_id, uint16_t size_bytes);
void* cfl_heap_arena_alloc_aligned(CflHeapArenaSystem* sys, uint16_t requesting_node_id, uint16_t size_bytes, uint8_t alignment);

/* Node array accessors */
cfl_heap_allocator_id_t cfl_heap_arena_get_node_allocator_id(CflHeapArenaSystem* sys, uint16_t node_id);
void cfl_heap_arena_set_node_allocator_id(CflHeapArenaSystem* sys, uint16_t node_id, cfl_heap_allocator_id_t allocator_id);

uint16_t cfl_heap_arena_get_node_memory_index(CflHeapArenaSystem* sys, uint16_t node_id);
void cfl_heap_arena_set_node_memory_index(CflHeapArenaSystem* sys, uint16_t node_id, uint16_t memory_idx);

/* Diagnostics */
uint16_t cfl_heap_arena_used_bytes(cfl_heap_arena_t arena);
uint16_t cfl_heap_arena_free_bytes(cfl_heap_arena_t arena);
void cfl_heap_arena_dump_stats(CflHeapArenaSystem* sys);

#endif /* ARENA_ALLOC_H */