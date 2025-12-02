
#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <stdbool.h>
#include "cfl_runtime.h"


cfl_runtime_create_params_t* cfl_runtime_create_params_create(void);
void cfl_runtime_create_params_destroy(cfl_runtime_create_params_t* params);

cfl_runtime_create_params_t* cfl_runtime_create_params_create(void) {
    cfl_runtime_create_params_t* params = (cfl_runtime_create_params_t*)calloc(1, sizeof(cfl_runtime_create_params_t));
    if (!params) {
        EXCEPTION("cfl_runtime_create_params_create: Failed to allocate memory for params");
    }
    memset(params, 0, sizeof(cfl_runtime_create_params_t));
    return params;
}

void cfl_runtime_create_params_destroy(cfl_runtime_create_params_t* params) {
    if (!params) {
        EXCEPTION("cfl_runtime_create_params_destroy: NULL params pointer");
    }
    free(params);
}

cfl_runtime_handle_t* cfl_runtime_create( cfl_perm_t* perm, cfl_runtime_create_params_t* params) {
    
    cfl_perm_set_instance(perm);
    cfl_perm_init(perm, params->perm_buffer, params->perm_buffer_size);
    cfl_runtime_handle_t *handle = (cfl_runtime_handle_t*)cfl_perm_alloc_pointer(perm,(uint16_t) sizeof(cfl_runtime_handle_t));
    if (!handle) {
        EXCEPTION("cfl_runtime_create: Failed to allocate memory for handle");
    }
    handle->perm = perm;
    handle->heap = cfl_heap_init(perm, params->heap_size);
    uint16_t perm_used_bytes = cfl_perm_used_bytes(perm);
    uint16_t perm_free_bytes = cfl_perm_free_bytes(perm);
    printf("perm_used_bytes after heap init: %d, perm_free_bytes: %d\n", perm_used_bytes, perm_free_bytes);
    handle->arena_system = cfl_heap_arena_system_create(perm, handle->heap, params->max_allocator_count, params->total_node_count, params->allocator_0_size);
    perm_used_bytes = cfl_perm_used_bytes(perm);
    perm_free_bytes = cfl_perm_free_bytes(perm);
    printf("perm_used_bytes after arena system init: %d, perm_free_bytes: %d\n", perm_used_bytes, perm_free_bytes);
    handle->event_queue = cfl_create_event_queue(params->event_queue_high_priority_size, params->event_queue_low_priority_size, perm);
    perm_used_bytes = cfl_perm_used_bytes(perm);
    perm_free_bytes = cfl_perm_free_bytes(perm);
    printf("perm_used_bytes after event queue init: %d, perm_free_bytes: %d\n", perm_used_bytes, perm_free_bytes);
    handle->flags = (uint8_t*)cfl_perm_alloc_pointer(perm, (uint16_t) (sizeof(uint8_t) * params->total_node_count));
    perm_used_bytes = cfl_perm_used_bytes(perm);
    perm_free_bytes = cfl_perm_free_bytes(perm);
    printf("perm_used_bytes after flags init: %d, perm_free_bytes: %d\n", perm_used_bytes, perm_free_bytes);
    handle->timer_handle = cfl_timer_create(params->delta_time, perm);
    handle->delta_time = params->delta_time;
    perm_used_bytes = cfl_perm_used_bytes(perm);
    perm_free_bytes = cfl_perm_free_bytes(perm);
    printf("perm_used_bytes after timer handle init: %d, perm_free_bytes: %d\n", perm_used_bytes, perm_free_bytes);
    return handle;
}

void cfl_runtime_reset(cfl_runtime_handle_t* handle) {
    if (!handle) {
        EXCEPTION("cfl_runtime_reset: NULL handle pointer");
    }
    
    cfl_heap_arena_system_reset(handle->arena_system);
    cfl_clear_queue(handle->event_queue);
    
}

static cfl_tick_result_t result;

bool cfl_runtime_run(cfl_runtime_handle_t* handle) {
    char buffer[1024];
    if (!handle) {
        EXCEPTION("cfl_runtime_run: NULL handle pointer");
    }
    
    for( int i = 0; i < 1000; i++) {
        cfl_timer_wait(
            handle->timer_handle,
            handle->delta_time,
            &result
        );
        cfl_timer_format_tick_result(&result, buffer, sizeof(buffer));
        printf("Timer wait result: %s\n", buffer);
        printf("Timer wait result: %d\n", result.changed_mask);
    }
    return true;

}