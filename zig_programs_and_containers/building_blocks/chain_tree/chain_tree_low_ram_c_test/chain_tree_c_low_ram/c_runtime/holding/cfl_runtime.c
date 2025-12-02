
#include <stdlib.h>
#include "cfl_runtime.h"


cfl_runtime_handle_t* cfl_runtime_create(void) {
    cfl_runtime_handle_t* handle = (cfl_runtime_handle_t*)malloc(sizeof(cfl_runtime_handle_t));
    if (!handle) {
        EXCEPTION("cfl_runtime_create: Failed to allocate memory");
    }
    handle->heap = NULL;
    return handle;
}

void cfl_runtime_destroy(cfl_runtime_handle_t* handle) {
    if (!handle) {
        EXCEPTION("cfl_runtime_destroy: NULL handle pointer");
    }
    if (handle->heap) {
        cfl_heap_destroy(handle->heap);
    }
    free(handle);
}