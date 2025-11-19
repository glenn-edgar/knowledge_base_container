/* ============= REENTRANT cfl_perm.h ============= */
#ifndef CFL_PERM_H
#define CFL_PERM_H

#ifdef __cplusplus
extern "C" {
#endif

#include <stdint.h>
#include <stdbool.h>

#define INVALID_PERM_IDX  0xFFFF

/* Permanent allocator statistics */
typedef struct {
    uint16_t total_allocations;
    uint16_t current_used_bytes;
    uint16_t peak_used_bytes;
    uint16_t largest_allocation;
    uint16_t smallest_allocation;
} CflPermStats;

/* Permanent allocator instance - NO global state */
typedef struct CflPerm {
    uint8_t*  pool;          /* Pointer to memory pool */
    uint16_t  pool_size;     /* Total size of pool */
    uint16_t  used;          /* Bump pointer (bytes used) */
    bool      initialized;   /* Initialization flag */
    bool      owns_pool;     /* True if allocator owns the pool memory */
    CflPermStats stats;      /* Runtime statistics */
} CflPerm, cfl_perm_t;

/* Allocator management */
CflPerm* cfl_perm_create(void);
void cfl_perm_destroy(volatile CflPerm* perm);

void cfl_perm_set_instance(volatile cfl_perm_t* perm);
cfl_perm_t* cfl_perm_malloc_create(uint16_t size);
void cfl_perm_malloc_destroy(volatile cfl_perm_t* perm);

/* Initialize with external buffer */
void cfl_perm_init(volatile CflPerm* perm, void* buffer, uint16_t buffer_size);

/* Reset to initial state (all allocations lost) */
void cfl_perm_reset(volatile CflPerm* perm);

/* Allocation - returns index (NO FREE - permanent allocations) */
uint16_t cfl_perm_alloc(volatile CflPerm* perm, uint16_t size_bytes);
uint16_t cfl_perm_alloc_aligned(volatile CflPerm* perm, uint16_t size_bytes, uint16_t alignment);

/* Allocation - returns pointer (NO FREE - permanent allocations) */
void* cfl_perm_alloc_pointer(volatile CflPerm* perm, uint16_t size_bytes);
void* cfl_perm_alloc_pointer_aligned(volatile CflPerm* perm, uint16_t size_bytes, uint16_t alignment);

/* Index/pointer conversion */
void* cfl_perm_ptr(volatile CflPerm* perm, uint16_t idx);
uint16_t cfl_perm_ptr_to_idx(volatile CflPerm* perm, void* ptr);

/* Diagnostics */
uint16_t cfl_perm_used_bytes(volatile CflPerm* perm);
uint16_t cfl_perm_free_bytes(volatile CflPerm* perm);
void cfl_perm_get_stats(volatile CflPerm* perm, CflPermStats* stats);
bool cfl_perm_validate(volatile CflPerm* perm);

/* Helper macros */
#define CFL_PERM_TOTAL_SIZE(buffer_size) \
    (sizeof(CflPerm) + (buffer_size))

#define CFL_PERM_DEFINE_STATIC(name, size) \
    static uint8_t name##_storage[CFL_PERM_TOTAL_SIZE(size)] __attribute__((aligned(4))); \
    static CflPerm* name = (CflPerm*)name##_storage

#ifdef __cplusplus
}
#endif

#endif /* CFL_PERM_H */