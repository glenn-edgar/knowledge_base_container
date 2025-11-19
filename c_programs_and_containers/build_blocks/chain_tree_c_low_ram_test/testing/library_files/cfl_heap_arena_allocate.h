/* ============= UPDATED cfl_heap_arena_allocate.h ============= */
#ifndef CFL_HEAP_ARENA_ALLOCATE_H
#define CFL_HEAP_ARENA_ALLOCATE_H

#include <stdint.h>
#include <stdbool.h>

#include "cfl_heap.h"
#include "cfl_global_definitions.h"

/* Forward declarations */
struct CflPerm;

/* Allocator ID type */
typedef uint8_t cfl_heap_allocator_id_t;

/* Forward declaration of internal types */
typedef struct CflHeapArenaControl* cfl_heap_arena_t;
#define INVALID_ARENA NULL

/* Arena system instance - encapsulates all global state */
typedef struct CflHeapArenaSystem {
    volatile cfl_heap_t* heap;
    uint16_t max_allocator_count;
    cfl_heap_arena_t* arenas;                        // Arena lookup table
    uint8_t* node_allocator_ids;                     // Node-to-allocator mapping
    uint16_t* node_memory_index;                     // Node memory offset within arena
    uint16_t total_node_count;                       // Total nodes in system
    cfl_heap_allocator_id_t next_allocator_id;       // Next ID to allocate
    cfl_heap_allocator_id_t active_allocator_context; // Current execution context
    void* allocator_0_buffer;                        // Permanent buffer for allocator 0 (from perm)
} CflHeapArenaSystem, cfl_heap_arena_system_t;

/* System creation - allocates from cfl_perm (persistent) 
 * Creates allocator 0 automatically with allocator_0_size from perm (permanent, cannot be destroyed)
 */
CflHeapArenaSystem* cfl_heap_arena_system_create(struct CflPerm* perm, volatile cfl_heap_t* heap, 
                                                   uint16_t max_allocator_count, uint16_t total_node_count,
                                                   uint16_t allocator_0_size);

/* Reset system - destroys allocators 1-253, resets allocator 0, reinitializes node arrays */
void cfl_heap_arena_system_reset(volatile CflHeapArenaSystem* sys);

/* Arena creation - allocates control structures from cfl_perm (persistent) */
cfl_heap_arena_t cfl_heap_arena_create(struct CflPerm* perm, volatile CflHeapArenaSystem* sys, uint16_t owner_node_id, uint16_t size_bytes);

/* Arena destruction - frees heap memory (EXCEPTION if attempting to destroy allocator 0) */
void cfl_heap_arena_destroy(volatile CflHeapArenaSystem* sys, cfl_heap_arena_t arena, uint16_t owner_node_id);

/* Get allocator ID from arena */
cfl_heap_allocator_id_t cfl_heap_arena_get_id(cfl_heap_arena_t arena);

/* Allocator context management (for tree walker execution) */
void cfl_heap_arena_set_active_allocator(volatile CflHeapArenaSystem* sys, uint16_t owner_node_id);
void cfl_heap_arena_set_node_allocator(volatile CflHeapArenaSystem* sys, uint16_t requesting_node_id);

/* Allocation from arena system */
void* cfl_arena_system_alloc(volatile CflHeapArenaSystem* sys, uint16_t requesting_node_id, uint16_t size_bytes);
void* cfl_arena_system_alloc_aligned(volatile CflHeapArenaSystem* sys, uint16_t requesting_node_id, uint16_t size_bytes, uint8_t alignment);

/* Node array accessors */
cfl_heap_allocator_id_t cfl_heap_arena_get_node_allocator_id(volatile CflHeapArenaSystem* sys, uint16_t node_id);
void cfl_heap_arena_set_node_allocator_id(volatile CflHeapArenaSystem* sys, uint16_t node_id, cfl_heap_allocator_id_t allocator_id);

uint16_t cfl_heap_arena_get_node_memory_index(volatile CflHeapArenaSystem* sys, uint16_t node_id);
void cfl_heap_arena_set_node_memory_index(volatile CflHeapArenaSystem* sys, uint16_t node_id, uint16_t memory_idx);

/* Diagnostics */
uint16_t cfl_heap_arena_used_bytes(cfl_heap_arena_t arena);
uint16_t cfl_heap_arena_free_bytes(cfl_heap_arena_t arena);
void cfl_heap_arena_dump_stats(volatile CflHeapArenaSystem* sys);

#endif /* CFL_HEAP_ARENA_ALLOCATE_H */