/* ============= cfl_heap_arena_allocate.h ============= */
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
typedef struct CflHeapArenaControl CflHeapArenaControl;

/* Arena statistics structure */
typedef struct CflHeapArenaStats {
    uint32_t active_count;
    uint32_t total_data_allocated;
    uint32_t total_data_used;
} CflHeapArenaStats;

/* Arena system instance - encapsulates all global state */
typedef struct CflHeapArenaSystem {
    cfl_heap_t* heap;
    uint16_t max_allocator_count;
    CflHeapArenaControl* control_blocks;             // Pre-allocated control blocks
    CflHeapArenaControl** arenas;                    // Arena lookup table (pointers into control_blocks)
    uint8_t* node_allocator_ids;                     // Node-to-allocator mapping
    uint16_t* node_memory_index;                     // Node memory offset within arena
    uint16_t total_node_count;                       // Total nodes in system
    cfl_heap_allocator_id_t next_allocator_id;       // Next ID to allocate
    cfl_heap_allocator_id_t active_allocator_context; // Current execution context
    void* allocator_0_buffer;                        // Permanent buffer for allocator 0 (from perm)
} CflHeapArenaSystem, cfl_heap_arena_system_t;

/* System creation - allocates from cfl_perm (persistent) 
 * Pre-allocates all control blocks and creates allocator 0 automatically with allocator_0_size from perm
 * (allocator 0 is permanent and cannot be destroyed)
 */
CflHeapArenaSystem* cfl_heap_arena_system_create(struct CflPerm* perm, cfl_heap_t* heap, 
                                                  uint16_t max_allocator_count, uint16_t total_node_count,
                                                  uint16_t allocator_0_size);

/* Reset system - destroys allocators 1-253, resets allocator 0, reinitializes node arrays */
void cfl_heap_arena_system_reset(CflHeapArenaSystem* sys);

/* Arena creation - returns allocator ID 
 * Uses pre-allocated control block, allocates data buffer from heap
 */
cfl_heap_allocator_id_t cfl_heap_arena_create(CflHeapArenaSystem* sys, 
                                               uint16_t owner_node_id, 
                                               uint16_t size_bytes);

/* Arena destruction - frees heap memory (EXCEPTION if attempting to destroy allocator 0) */
void cfl_heap_arena_destroy(CflHeapArenaSystem* sys, 
                            cfl_heap_allocator_id_t id, 
                            uint16_t owner_node_id);

/* Allocator context management (for tree walker execution) */
void cfl_heap_arena_set_active_allocator(CflHeapArenaSystem* sys, uint16_t owner_node_id);
void cfl_heap_arena_set_active_allocator_id(CflHeapArenaSystem* sys, cfl_heap_allocator_id_t allocator_id);
void cfl_heap_arena_set_node_allocator(CflHeapArenaSystem* sys, uint16_t requesting_node_id);

/* Allocation from arena system (node-based, updates node_memory_index) */
void* cfl_arena_system_alloc(CflHeapArenaSystem* sys, uint16_t requesting_node_id, uint16_t size_bytes);
void* cfl_arena_system_alloc_aligned(CflHeapArenaSystem* sys, uint16_t requesting_node_id, uint16_t size_bytes, uint8_t alignment);

/* Additional allocation from node's arena (does NOT update node_memory_index) */
void* cfl_arena_additional_alloc(CflHeapArenaSystem* sys, uint16_t node_index, uint16_t size_bytes);
void* cfl_arena_additional_alloc_aligned(CflHeapArenaSystem* sys, uint16_t node_index, uint16_t size_bytes, uint8_t alignment);

/* Allocation from active allocator context (updates node_memory_index and assigns allocator to node) */
void* cfl_arena_alloc_from_active(CflHeapArenaSystem* sys, uint16_t node_index, uint16_t size_bytes);
void* cfl_arena_alloc_from_active_aligned(CflHeapArenaSystem* sys, uint16_t node_index, uint16_t size_bytes, uint8_t alignment);

/* Get pointer to node's allocated memory */
void* cfl_heap_arena_get_node_ptr(CflHeapArenaSystem* sys, uint16_t node_id);

/* Node array accessors */
cfl_heap_allocator_id_t cfl_heap_arena_get_node_allocator_id(CflHeapArenaSystem* sys, uint16_t node_id);
void cfl_heap_arena_set_node_allocator_id(CflHeapArenaSystem* sys, uint16_t node_id, cfl_heap_allocator_id_t allocator_id);

uint16_t cfl_heap_arena_get_node_memory_index(CflHeapArenaSystem* sys, uint16_t node_id);
void cfl_heap_arena_set_node_memory_index(CflHeapArenaSystem* sys, uint16_t node_id, uint16_t memory_idx);

/* Diagnostics */
uint16_t cfl_heap_arena_used_bytes(CflHeapArenaSystem* sys, cfl_heap_allocator_id_t id);
uint16_t cfl_heap_arena_free_bytes(CflHeapArenaSystem* sys, cfl_heap_allocator_id_t id);
CflHeapArenaStats cfl_heap_arena_dump_stats(CflHeapArenaSystem* sys);

#endif /* CFL_HEAP_ARENA_ALLOCATE_H */