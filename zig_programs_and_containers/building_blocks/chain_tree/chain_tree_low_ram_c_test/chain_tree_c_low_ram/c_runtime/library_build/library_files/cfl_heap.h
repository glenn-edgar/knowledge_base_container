/* ============= REENTRANT cfl_heap.h ============= */
#ifndef CFL_HEAP_H
#define CFL_HEAP_H

#ifdef __cplusplus
extern "C" {
#endif

#include <stdint.h>
#include <stdbool.h>

#define INVALID_HEAP_IDX  0xFFFF
#define NODE_ID_NONE      0xFFFF

/* Heap statistics structure */
typedef struct {
    uint16_t total_allocations;
    uint16_t total_frees;
    uint16_t current_blocks;
    uint16_t current_used_bytes;
    uint16_t peak_used_bytes;
    uint16_t largest_free_block;
    uint16_t free_blocks;
    uint16_t allocated_blocks;
} CflHeapStats;

/* Heap instance structure */
typedef struct CflHeap {
    uint8_t*  pool;          /* Pointer to heap memory pool */
    uint16_t  pool_size;     /* Total size of heap pool */
    bool      initialized;    /* Initialization flag */
    bool      owns_pool;      /* True if heap owns the pool memory */
    CflHeapStats stats;       /* Runtime statistics */
} CflHeap, cfl_heap_t;

/* Forward declaration */
struct CflPerm;

/* Initialize heap - allocates from cfl_perm and returns initialized heap */
CflHeap* cfl_heap_init(struct CflPerm* perm, uint16_t buffer_size);

/* Reset heap to initial state */
void cfl_heap_reset(CflHeap* heap);

/* Allocation - returns index */
uint16_t cfl_heap_malloc(CflHeap* heap, uint16_t size_bytes);
void     cfl_heap_free(CflHeap* heap, uint16_t idx);
void*    cfl_heap_ptr(CflHeap* heap, uint16_t idx);
uint16_t cfl_heap_ptr_to_idx(CflHeap* heap, void* ptr);

/* Allocation - returns pointer */
void*    cfl_heap_malloc_pointer(CflHeap* heap, uint16_t size_bytes);
void     cfl_heap_free_pointer(CflHeap* heap, void* ptr);

/* Arena allocation with node tracking and custom alignment */
uint16_t cfl_heap_arena_alloc_aligned(CflHeap* heap, uint16_t requesting_node_id, 
                                       uint16_t size_bytes, uint16_t alignment);

/* Diagnostics */
uint16_t cfl_heap_used_bytes(CflHeap* heap);
uint16_t cfl_heap_free_bytes(CflHeap* heap);
void     cfl_heap_get_stats(CflHeap* heap, CflHeapStats* stats);
void     cfl_heap_dump_stats(CflHeap* heap);

/* Validation */
bool     cfl_heap_validate(CflHeap* heap);
void     cfl_heap_walk(CflHeap* heap, void (*callback)(void* block_ptr, uint16_t size, bool allocated, uint16_t node_id));

/* Get node ID of allocated block */
uint16_t cfl_heap_get_node_id(CflHeap* heap, uint16_t idx);

/* Helper macros */
#define CFL_HEAP_TOTAL_SIZE(buffer_size) \
    (sizeof(CflHeap) + (buffer_size))

#define CFL_HEAP_DEFINE_STATIC(name, size) \
    static uint8_t name##_storage[CFL_HEAP_TOTAL_SIZE(size)] __attribute__((aligned(4))); \
    static CflHeap* name = (CflHeap*)name##_storage

#ifdef __cplusplus
}
#endif

#endif /* CFL_HEAP_H */