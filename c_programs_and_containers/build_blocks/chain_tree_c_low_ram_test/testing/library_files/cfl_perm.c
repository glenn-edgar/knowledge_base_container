/* ============= REENTRANT cfl_perm.c ============= */
#include "cfl_perm.h"
#include "cfl_exception.h"
#include <string.h>
#include "cfl_global_definitions.h"
#include <stdlib.h>

#ifndef PERM_ALIGNMENT 
#define PERM_ALIGNMENT     4
#endif

#ifndef MIN_ALLOC_SIZE
#define MIN_ALLOC_SIZE     4
#endif

#if defined(CFL_PERM_DEBUG)
#include <stdio.h>
#define DEBUG_PRINT(fmt, ...) printf(fmt, ##__VA_ARGS__)
#else
#define DEBUG_PRINT(fmt, ...)
#endif

/* Magic number for validation */
#define PERM_MAGIC         0x5045  /* 'PE' for PErmanent */

/* ============= INTERNAL HELPERS ============= */

static inline uint16_t align_up(uint16_t value, uint16_t alignment) {
    if (alignment == 0 || (alignment & (alignment - 1)) != 0) {
        EXCEPTION("align_up: Alignment must be power of 2");
    }
    return (value + alignment - 1) & ~(alignment - 1);
}

static inline bool is_power_of_2(uint16_t value) {
    return value != 0 && (value & (value - 1)) == 0;
}

static inline uint16_t ptr_to_idx(volatile CflPerm* perm, void* ptr) {
    if (!ptr) {
        EXCEPTION("ptr_to_idx: NULL pointer");
    }
    return (uint16_t)((uint8_t*)ptr - perm->pool);
}

static inline void* idx_to_ptr(volatile CflPerm* perm, uint16_t idx) {
    if (idx >= perm->pool_size) {
        EXCEPTION("idx_to_ptr: Index out of bounds");
    }
    return &perm->pool[idx];
}

/* Update statistics */
static void update_stats(volatile CflPerm* perm, uint16_t allocated_size) {
    perm->stats.total_allocations++;
    perm->stats.current_used_bytes = perm->used;
    
    if (perm->used > perm->stats.peak_used_bytes) {
        perm->stats.peak_used_bytes = perm->used;
    }
    
    if (allocated_size > perm->stats.largest_allocation) {
        perm->stats.largest_allocation = allocated_size;
    }
    
    if (perm->stats.total_allocations == 1 || allocated_size < perm->stats.smallest_allocation) {
        perm->stats.smallest_allocation = allocated_size;
    }
}

/* ============= PUBLIC API ============= */

void cfl_perm_set_instance(volatile cfl_perm_t* perm) {
    memset((void*)perm, 0, sizeof(CflPerm));
    perm->initialized = false;
    perm->owns_pool = false;
}

CflPerm* cfl_perm_create(void) {
    CflPerm* perm = (CflPerm*)malloc(sizeof(CflPerm));
    if (!perm) {
        EXCEPTION("cfl_perm_create: Failed to allocate memory");
    }
    
    /* Initialize structure to safe state */
    memset(perm, 0, sizeof(CflPerm));
    perm->initialized = false;
    perm->owns_pool = false;
    
    return perm;
}

void cfl_perm_destroy(volatile CflPerm* perm) {
    if (!perm) {
        EXCEPTION("cfl_perm_destroy: NULL perm pointer");
    }
    
    /* Free pool if we own it */
    if (perm->owns_pool && perm->pool) {
        free(perm->pool);
    }
    
    free((void*)perm);
}

cfl_perm_t* cfl_perm_malloc_create(uint16_t size) {
    cfl_perm_t* perm = (cfl_perm_t*)malloc(sizeof(cfl_perm_t));
    if (!perm) {
        EXCEPTION("cfl_perm_malloc_create: Failed to allocate perm structure");
    }
    
    perm->pool = (uint8_t*)malloc(size);
    if (!perm->pool) {
        free(perm);
        EXCEPTION("cfl_perm_malloc_create: Failed to allocate pool memory");
    }
    
    cfl_perm_init(perm, perm->pool, size);
    perm->owns_pool = true;
    
    return perm;
}

void cfl_perm_malloc_destroy(volatile cfl_perm_t* perm) {
    if (!perm) {
        EXCEPTION("cfl_perm_malloc_destroy: NULL perm pointer");
    }
    
    if (perm->pool) {
        free(perm->pool);
    }
    free((void*)perm);
}

void cfl_perm_init(volatile CflPerm* perm, void* buffer, uint16_t buffer_size) {
    if (!perm) {
        EXCEPTION("cfl_perm_init: NULL perm pointer");
    }
    if (!buffer) {
        EXCEPTION("cfl_perm_init: NULL buffer pointer");
    }
    if (buffer_size < MIN_ALLOC_SIZE) {
        EXCEPTION("cfl_perm_init: Buffer too small");
    }
    
    perm->pool = (uint8_t*)buffer;
    perm->pool_size = buffer_size;
    perm->used = 0;
    perm->owns_pool = false;
    
    /* Clear statistics */
    memset((void*)&perm->stats, 0, sizeof(CflPermStats));
    
    /* Zero the pool for safety */
    memset(perm->pool, 0, perm->pool_size);
    
    perm->initialized = true;
}

void cfl_perm_reset(volatile CflPerm* perm) {
    if (!perm) {
        EXCEPTION("cfl_perm_reset: NULL perm pointer");
    }
    
    if (!perm->pool) {
        EXCEPTION("cfl_perm_reset: NULL pool pointer");
    }
    
    /* Clear statistics */
    memset((void*)&perm->stats, 0, sizeof(CflPermStats));
    
    /* Reset bump pointer */
    perm->used = 0;
    
    /* Zero the pool */
    memset(perm->pool, 0, perm->pool_size);
    
    perm->initialized = true;
}

uint16_t cfl_perm_alloc(volatile CflPerm* perm, uint16_t size_bytes) {
    return cfl_perm_alloc_aligned(perm, size_bytes, PERM_ALIGNMENT);
}

uint16_t cfl_perm_alloc_aligned(volatile CflPerm* perm, uint16_t size_bytes, uint16_t alignment) {
    if (!perm) {
        EXCEPTION("cfl_perm_alloc_aligned: NULL perm pointer");
    }
    if (!perm->initialized) {
        EXCEPTION("cfl_perm_alloc_aligned: Allocator not initialized");
    }
    if (size_bytes == 0) {
        EXCEPTION("cfl_perm_alloc_aligned: Zero size allocation");
    }
    if (alignment == 0 || !is_power_of_2(alignment)) {
        EXCEPTION("cfl_perm_alloc_aligned: Alignment must be power of 2");
    }
    
    /* Align size */
    size_bytes = align_up(size_bytes, PERM_ALIGNMENT);
    
    if (size_bytes < MIN_ALLOC_SIZE) {
        size_bytes = MIN_ALLOC_SIZE;
    }
    
    /* Calculate aligned position */
    uintptr_t current_addr = (uintptr_t)(perm->pool + perm->used);
    uintptr_t aligned_addr = (current_addr + alignment - 1) & ~(uintptr_t)(alignment - 1);
    uint16_t padding = (uint16_t)(aligned_addr - current_addr);
    
    uint16_t total_needed = padding + size_bytes;
    
    /* Check if we have space */
    if (perm->used + total_needed > perm->pool_size) {
        EXCEPTION("cfl_perm_alloc_aligned: Out of memory");
    }
    
    /* Calculate return index */
    uint16_t ret_idx = perm->used + padding;
    
    /* Update bump pointer */
    perm->used += total_needed;
    
    /* Update statistics */
    update_stats(perm, size_bytes);
    
    return ret_idx;
}

void* cfl_perm_alloc_pointer(volatile CflPerm* perm, uint16_t size_bytes) {
    uint16_t idx = cfl_perm_alloc(perm, size_bytes);
    return idx_to_ptr(perm, idx);
}

void* cfl_perm_alloc_pointer_aligned(volatile CflPerm* perm, uint16_t size_bytes, uint16_t alignment) {
    uint16_t idx = cfl_perm_alloc_aligned(perm, size_bytes, alignment);
    return idx_to_ptr(perm, idx);
}

void* cfl_perm_ptr(volatile CflPerm* perm, uint16_t idx) {
    if (!perm) {
        EXCEPTION("cfl_perm_ptr: NULL perm pointer");
    }
    if (!perm->initialized) {
        EXCEPTION("cfl_perm_ptr: Allocator not initialized");
    }
    return idx_to_ptr(perm, idx);
}

uint16_t cfl_perm_ptr_to_idx(volatile CflPerm* perm, void* ptr) {
    if (!perm) {
        EXCEPTION("cfl_perm_ptr_to_idx: NULL perm pointer");
    }
    if (!perm->initialized) {
        EXCEPTION("cfl_perm_ptr_to_idx: Allocator not initialized");
    }
    if (!ptr) {
        EXCEPTION("cfl_perm_ptr_to_idx: NULL pointer");
    }
    
    /* Validate pointer is within pool bounds */
    if ((uint8_t*)ptr < perm->pool || (uint8_t*)ptr >= perm->pool + perm->pool_size) {
        EXCEPTION("cfl_perm_ptr_to_idx: Pointer out of bounds");
    }
    
    return (uint16_t)((uint8_t*)ptr - perm->pool);
}

uint16_t cfl_perm_used_bytes(volatile CflPerm* perm) {
    if (!perm || !perm->initialized) return 0;
    return perm->used;
}

uint16_t cfl_perm_free_bytes(volatile CflPerm* perm) {
    if (!perm || !perm->initialized) return 0;
    return perm->pool_size - perm->used;
}

void cfl_perm_get_stats(volatile CflPerm* perm, CflPermStats* stats) {
    if (!perm || !perm->initialized || !stats) {
        EXCEPTION("cfl_perm_get_stats: Invalid parameters");
    }
    
    memcpy(stats, (void*)&perm->stats, sizeof(CflPermStats));
}

bool cfl_perm_validate(volatile CflPerm* perm) {
    if (!perm || !perm->initialized) {
        return false;
    }
    
    /* Check bump pointer is within bounds */
    if (perm->used > perm->pool_size) {
        DEBUG_PRINT("Validation failed: used > pool_size\n");
        return false;
    }
    
    /* Check statistics are consistent */
    if (perm->stats.current_used_bytes != perm->used) {
        DEBUG_PRINT("Validation failed: stats mismatch\n");
        return false;
    }
    
    if (perm->stats.peak_used_bytes < perm->used) {
        DEBUG_PRINT("Validation failed: peak < current\n");
        return false;
    }
    
    return true;
}