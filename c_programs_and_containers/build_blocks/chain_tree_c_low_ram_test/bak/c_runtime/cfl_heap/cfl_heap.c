/* ============= REENTRANT cfl_heap.c ============= */
#include "cfl_heap.h"
#include "cfl_exception.h"
#include <string.h>
#include "cfl_global_definitions.h"

#ifndef  BLOCK_ALIGNMENT 
#define BLOCK_ALIGNMENT     4
#endif



#ifndef MIN_BLOCK_SIZE
#define MIN_BLOCK_SIZE      8
#endif

if (defined(CFL_HEAP_DEBUG))
#define DEBUG_PRINT(fmt, ...) printf(fmt, ##__VA_ARGS__)
#endif

/* Configuration */


/* Block header */
typedef struct BlockHeader {
    uint16_t size;
    uint16_t flags;
} BlockHeader;

#define HEADER_SIZE         sizeof(BlockHeader)
#define FLAG_ALLOCATED      0x0001

/* ============= INTERNAL HELPERS ============= */

static inline uint16_t align_up(uint16_t value, uint16_t alignment) {
    return (value + alignment - 1) & ~(alignment - 1);
}

static inline bool is_allocated(BlockHeader* block) {
    return (block->flags & FLAG_ALLOCATED) != 0;
}

static inline void mark_allocated(BlockHeader* block) {
    block->flags |= FLAG_ALLOCATED;
}

static inline void mark_free(BlockHeader* block) {
    block->flags &= ~FLAG_ALLOCATED;
}

static inline BlockHeader* get_next_block(CflHeap* heap, BlockHeader* block) {
    uint8_t* ptr = (uint8_t*)block;
    ptr += HEADER_SIZE + block->size;
    
    if (ptr >= heap->pool + heap->pool_size) {
        return NULL;  // Normal end of heap - not an error
    }
    
    return (BlockHeader*)ptr;
}

static inline uint16_t ptr_to_idx(CflHeap* heap, void* ptr) {
    if (!ptr) {
        EXCEPTION("ptr_to_idx: NULL pointer");
    }
    return (uint16_t)((uint8_t*)ptr - heap->pool);
}

static inline void* idx_to_ptr(CflHeap* heap, uint16_t idx) {
    if (idx >= heap->pool_size) {
        EXCEPTION("idx_to_ptr: Index out of bounds");
    }
    return &heap->pool[idx];
}

static inline void* block_to_data_ptr(BlockHeader* block) {
    return (uint8_t*)block + HEADER_SIZE;
}

static inline BlockHeader* data_ptr_to_block(void* ptr) {
    return (BlockHeader*)((uint8_t*)ptr - HEADER_SIZE);
}

/* Coalesce adjacent free blocks */
static void coalesce_free_blocks(CflHeap* heap) {
    BlockHeader* block = (BlockHeader*)heap->pool;
    
    while (block != NULL) {
        BlockHeader* next = get_next_block(heap, block);
        
        if (!is_allocated(block) && next != NULL && !is_allocated(next)) {
            block->size += HEADER_SIZE + next->size;
        } else {
            block = next;
        }
    }
}

/* ============= PUBLIC API ============= */

void cfl_heap_init(CflHeap* heap, void* buffer, uint16_t buffer_size) {
    if (!heap) {
        EXCEPTION("cfl_heap_init: NULL heap pointer");
    }
    if (!buffer) {
        EXCEPTION("cfl_heap_init: NULL buffer pointer");
    }
    if (buffer_size < HEADER_SIZE + MIN_BLOCK_SIZE) {
        EXCEPTION("cfl_heap_init: Buffer too small");
    }
    
    heap->pool = (uint8_t*)buffer;
    heap->pool_size = buffer_size;
    heap->owns_pool = false;
    heap->initialized = false;
    
    memset(heap->pool, 0, heap->pool_size);
    
    BlockHeader* initial = (BlockHeader*)heap->pool;
    initial->size = heap->pool_size - HEADER_SIZE;
    initial->flags = 0;
    
    heap->initialized = true;
}

void cfl_heap_init_embedded(CflHeap* heap, uint16_t buffer_size) {
    if (!heap) {
        EXCEPTION("cfl_heap_init_embedded: NULL heap pointer");
    }
    if (buffer_size < HEADER_SIZE + MIN_BLOCK_SIZE) {
        EXCEPTION("cfl_heap_init_embedded: Buffer too small");
    }
    
    /* Buffer immediately follows the CflHeap structure */
    uint8_t* buffer = (uint8_t*)heap + sizeof(CflHeap);
    
    heap->pool = buffer;
    heap->pool_size = buffer_size;
    heap->owns_pool = true;
    heap->initialized = false;
    
    memset(heap->pool, 0, heap->pool_size);
    
    BlockHeader* initial = (BlockHeader*)heap->pool;
    initial->size = heap->pool_size - HEADER_SIZE;
    initial->flags = 0;
    
    heap->initialized = true;
}

void cfl_heap_reset(CflHeap* heap) {
    if (!heap) return;
    
    if (heap->initialized && heap->pool) {
        memset(heap->pool, 0, heap->pool_size);
        
        BlockHeader* initial = (BlockHeader*)heap->pool;
        initial->size = heap->pool_size - HEADER_SIZE;
        initial->flags = 0;
    }
    
    heap->initialized = false;
}

uint16_t cfl_heap_malloc(CflHeap* heap, uint16_t size_bytes) {
    if (!heap) {
        EXCEPTION("cfl_heap_malloc: NULL heap pointer");
    }
    if (!heap->initialized) {
        EXCEPTION("cfl_heap_malloc: Heap not initialized");
    }
    if (size_bytes == 0) {
        EXCEPTION("cfl_heap_malloc: Zero size allocation");
    }
    
    size_bytes = align_up(size_bytes, BLOCK_ALIGNMENT);
    
    if (size_bytes < MIN_BLOCK_SIZE) {
        size_bytes = MIN_BLOCK_SIZE;
    }
    
    BlockHeader* block = (BlockHeader*)heap->pool;
    
    while (block != NULL) {
        if (!is_allocated(block) && block->size >= size_bytes) {
            uint16_t remainder = block->size - size_bytes;
            if (remainder >= HEADER_SIZE + MIN_BLOCK_SIZE) {
                BlockHeader* new_block = (BlockHeader*)((uint8_t*)block + HEADER_SIZE + size_bytes);
                new_block->size = remainder - HEADER_SIZE;
                new_block->flags = 0;
                
                block->size = size_bytes;
            }
            
            mark_allocated(block);
            return ptr_to_idx(heap, block_to_data_ptr(block));
        }
        
        block = get_next_block(heap, block);
    }
    
    EXCEPTION("cfl_heap_malloc: Out of memory");
}

void cfl_heap_free(CflHeap* heap, uint16_t idx) {
    if (!heap) {
        EXCEPTION("cfl_heap_free: NULL heap pointer");
    }
    if (!heap->initialized) {
        EXCEPTION("cfl_heap_free: Heap not initialized");
    }
    if (idx >= heap->pool_size) {
        EXCEPTION("cfl_heap_free: Invalid heap index");
    }
    
    void* ptr = idx_to_ptr(heap, idx);
    BlockHeader* block = data_ptr_to_block(ptr);
    
    if ((uint8_t*)block < heap->pool || 
        (uint8_t*)block >= heap->pool + heap->pool_size) {
        EXCEPTION("cfl_heap_free: Block pointer out of bounds");
    }
    
    mark_free(block);
    coalesce_free_blocks(heap);
}

void* cfl_heap_ptr(CflHeap* heap, uint16_t idx) {
    if (!heap) {
        EXCEPTION("cfl_heap_ptr: NULL heap pointer");
    }
    if (!heap->initialized) {
        EXCEPTION("cfl_heap_ptr: Heap not initialized");
    }
    return idx_to_ptr(heap, idx);
}

uint16_t cfl_heap_ptr_to_idx(CflHeap* heap, void* ptr) {
    if (!heap) {
        EXCEPTION("cfl_heap_ptr_to_idx: NULL heap pointer");
    }
    if (!heap->initialized) {
        EXCEPTION("cfl_heap_ptr_to_idx: Heap not initialized");
    }
    if (!ptr) {
        EXCEPTION("cfl_heap_ptr_to_idx: NULL pointer");
    }
    
    /* Validate pointer is within heap bounds */
    if ((uint8_t*)ptr < heap->pool || (uint8_t*)ptr >= heap->pool + heap->pool_size) {
        EXCEPTION("cfl_heap_ptr_to_idx: Pointer out of bounds");
    }
    
    return (uint16_t)((uint8_t*)ptr - heap->pool);
}

uint16_t cfl_heap_used_bytes(CflHeap* heap) {
    if (!heap || !heap->initialized) return 0;
    
    uint16_t used = 0;
    BlockHeader* block = (BlockHeader*)heap->pool;
    
    while (block != NULL) {
        if (is_allocated(block)) {
            used += HEADER_SIZE + block->size;
        }
        block = get_next_block(heap, block);
    }
    
    return used;
}

uint16_t cfl_heap_free_bytes(CflHeap* heap) {
    if (!heap || !heap->initialized) return 0;
    return heap->pool_size - cfl_heap_used_bytes(heap);
}

void cfl_heap_dump_stats(CflHeap* heap) {
    if (!heap || !heap->initialized) return;
    
    uint16_t total_blocks = 0;
    uint16_t free_blocks = 0;
    uint16_t allocated_blocks = 0;
    uint16_t largest_free = 0;
    
    BlockHeader* block = (BlockHeader*)heap->pool;
    
    while (block != NULL) {
        total_blocks++;
        
        if (is_allocated(block)) {
            allocated_blocks++;
        } else {
            free_blocks++;
            if (block->size > largest_free) {
                largest_free = block->size;
            }
        }
        
        block = get_next_block(heap, block);
    }
    
    /* Stats are now calculated but not printed - caller can access via other APIs */
}