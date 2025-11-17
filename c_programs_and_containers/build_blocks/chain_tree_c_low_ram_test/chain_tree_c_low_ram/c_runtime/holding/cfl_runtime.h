#ifndef CFL_RUNTIME_H
#define CFL_RUNTIME_H

#ifdef __cplusplus
extern "C" {
#endif


#include "cfl_global_definitions.h"
#include "cfl_exception.h"
#include "cfl_common_functions.h"
#include "cfl_heap.h"
#include "cfl_heap_arena_allocate.h"

typedef struct CFL_RUNTIME_HANDLE {
    cfl_heap_t *heap;      /* Pointer to heap */
    CflHeapArenaSystem* arena_system;
} cfl_runtime_handle_t;


cfl_runtime_handle_t* cfl_runtime_create(void);
void cfl_runtime_destroy(cfl_runtime_handle_t* handle);

#ifdef __cplusplus
}
#endif

#endif 