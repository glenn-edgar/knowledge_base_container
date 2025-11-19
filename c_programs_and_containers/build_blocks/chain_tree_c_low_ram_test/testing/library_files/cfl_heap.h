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

/**
 * @brief Initialize heap - allocates from cfl_perm and returns initialized heap
 * 
 * @param perm Permanent allocator to use for heap structure and pool
 * @param buffer_size Size of heap pool in bytes
 * @return Pointer to initialized heap (volatile for runtime handle compatibility)
 * 
 * @note Returns volatile pointer for use with volatile runtime handles
 */
volatile CflHeap* cfl_heap_init(struct CflPerm* perm, uint16_t buffer_size);

/**
 * @brief Reset heap to initial state
 * 
 * @param heap Pointer to heap (accepts volatile)
 */
void cfl_heap_reset(volatile CflHeap* heap);

/**
 * @brief Allocate memory block - returns index
 * 
 * @param heap Pointer to heap (accepts volatile)
 * @param size_bytes Size to allocate in bytes
 * @return Index to allocated memory, or INVALID_HEAP_IDX on failure
 */
uint16_t cfl_heap_malloc(volatile CflHeap* heap, uint16_t size_bytes);

/**
 * @brief Free memory block by index
 * 
 * @param heap Pointer to heap (accepts volatile)
 * @param idx Index to memory block
 */
void cfl_heap_free(volatile CflHeap* heap, uint16_t idx);

/**
 * @brief Convert index to pointer
 * 
 * @param heap Pointer to heap (accepts volatile)
 * @param idx Index to convert
 * @return Pointer to memory at index
 */
void* cfl_heap_ptr(volatile CflHeap* heap, uint16_t idx);

/**
 * @brief Convert pointer to index
 * 
 * @param heap Pointer to heap (accepts volatile)
 * @param ptr Pointer to convert
 * @return Index of pointer in heap
 */
uint16_t cfl_heap_ptr_to_idx(volatile CflHeap* heap, void* ptr);

/**
 * @brief Allocate memory block - returns pointer
 * 
 * @param heap Pointer to heap (accepts volatile)
 * @param size_bytes Size to allocate in bytes
 * @return Pointer to allocated memory, or NULL on failure
 */
void* cfl_heap_malloc_pointer(volatile CflHeap* heap, uint16_t size_bytes);

/**
 * @brief Free memory block by pointer
 * 
 * @param heap Pointer to heap (accepts volatile)
 * @param ptr Pointer to memory block
 */
void cfl_heap_free_pointer(volatile CflHeap* heap, void* ptr);

/**
 * @brief Arena allocation with node tracking and custom alignment
 * 
 * @param heap Pointer to heap (accepts volatile)
 * @param requesting_node_id ID of requesting node/component
 * @param size_bytes Size to allocate in bytes
 * @param alignment Alignment requirement (must be power of 2)
 * @return Index to aligned allocated memory, or INVALID_HEAP_IDX on failure
 */
uint16_t cfl_heap_arena_alloc_aligned(volatile CflHeap* heap, uint16_t requesting_node_id, 
                                       uint16_t size_bytes, uint16_t alignment);

/**
 * @brief Get currently used bytes
 * 
 * @param heap Pointer to heap (accepts volatile)
 * @return Number of bytes currently allocated
 */
uint16_t cfl_heap_used_bytes(volatile CflHeap* heap);

/**
 * @brief Get available free bytes
 * 
 * @param heap Pointer to heap (accepts volatile)
 * @return Number of bytes currently free
 */
uint16_t cfl_heap_free_bytes(volatile CflHeap* heap);

/**
 * @brief Get heap statistics
 * 
 * @param heap Pointer to heap (accepts volatile)
 * @param stats Pointer to stats structure to fill (accepts volatile)
 */
void cfl_heap_get_stats(volatile CflHeap* heap, volatile CflHeapStats* stats);

/**
 * @brief Update and store heap statistics
 * 
 * @param heap Pointer to heap (accepts volatile)
 */
void cfl_heap_dump_stats(volatile CflHeap* heap);

/**
 * @brief Validate heap integrity
 * 
 * @param heap Pointer to heap (accepts volatile)
 * @return true if heap is valid, false if corrupted
 */
bool cfl_heap_validate(volatile CflHeap* heap);

/**
 * @brief Walk through all heap blocks
 * 
 * @param heap Pointer to heap (accepts volatile)
 * @param callback Function to call for each block
 */
void cfl_heap_walk(volatile CflHeap* heap, 
                   void (*callback)(void* block_ptr, uint16_t size, bool allocated, uint16_t node_id));

/**
 * @brief Get node ID of allocated block
 * 
 * @param heap Pointer to heap (accepts volatile)
 * @param idx Index to memory block
 * @return Node ID that allocated the block, or NODE_ID_NONE
 */
uint16_t cfl_heap_get_node_id(volatile CflHeap* heap, uint16_t idx);

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