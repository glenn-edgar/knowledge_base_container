/* ============= REENTRANT cfl_heap.h ============= */
#ifndef CFL_HEAP_H
#define CFL_HEAP_H

#include <stdint.h>
#include <stdbool.h>






#define INVALID_HEAP_IDX  0xFFFF

/* Forward declaration */
typedef struct CflHeap CflHeap;

/* Heap instance structure - opaque handle */
struct CflHeap {
    uint8_t*  pool;          /* Pointer to heap memory pool */
    uint16_t  pool_size;     /* Total size of heap pool */
    bool      initialized;    /* Initialization flag */
    bool      owns_pool;      /* True if heap owns the pool memory */
};

/* Initialization options */
typedef enum {
    CFL_HEAP_EMBEDDED,       /* Use embedded buffer in structure */
    CFL_HEAP_EXTERNAL        /* Use externally provided buffer */
} CflHeapMode;

/* Initialize heap with external buffer */
void cfl_heap_init(CflHeap* heap, void* buffer, uint16_t buffer_size);

/* Initialize heap with embedded buffer (caller allocates CflHeap + buffer) */
void cfl_heap_init_embedded(CflHeap* heap, uint16_t buffer_size);

/* Reset/deinitialize heap */
void cfl_heap_reset(CflHeap* heap);

/* Small-block allocator - returns index instead of pointer */
uint16_t cfl_heap_malloc(CflHeap* heap, uint16_t size_bytes);
void     cfl_heap_free(CflHeap* heap, uint16_t idx);
void*    cfl_heap_ptr(CflHeap* heap, uint16_t idx);
uint16_t cfl_heap_ptr_to_idx(CflHeap* heap, void* ptr);

/* Diagnostics */
uint16_t cfl_heap_used_bytes(CflHeap* heap);
uint16_t cfl_heap_free_bytes(CflHeap* heap);
void     cfl_heap_dump_stats(CflHeap* heap);

/* Helper macro to calculate total size needed for embedded heap */
#define CFL_HEAP_TOTAL_SIZE(buffer_size) \
    (sizeof(CflHeap) + (buffer_size))

/* Helper macro to statically allocate heap with embedded buffer */
#define CFL_HEAP_DEFINE_STATIC(name, size) \
    static uint8_t name##_storage[CFL_HEAP_TOTAL_SIZE(size)] __attribute__((aligned(4))); \
    static CflHeap* name = (CflHeap*)name##_storage

#endif /* CFL_HEAP_H */