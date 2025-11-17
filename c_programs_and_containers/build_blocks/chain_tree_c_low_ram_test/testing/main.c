/* test of runtime system */
#include <stdio.h>
#include <stdlib.h>
#include <stdbool.h>
#include "cfl_runtime.h"
#include "chaintree_support.h"

static cfl_perm_t perm;
static char perm_buffer[0xffff];

extern const chaintree_handle_t g_test_header;

int main(void) {
    cfl_runtime_create_params_t* params = (cfl_runtime_create_params_t*)malloc(sizeof(cfl_runtime_create_params_t));
    if (!params) {
        printf("Failed to allocate memory for params\n");
        return -1;
    }
    const chaintree_handle_t *test_handle = &g_test_header;
    params->perm = &perm;
    params->perm_buffer = perm_buffer;
    params->perm_buffer_size = (uint16_t) sizeof(perm_buffer);
    params->heap_size = (uint16_t) 0x1000;   
    params->max_allocator_count = (uint16_t) 2;
    params->total_node_count = test_handle->node_count;
    printf("total_node_count: %d\n", params->total_node_count);
    params->allocator_0_size = (uint16_t)  test_handle->node_count * 20;
    params->event_queue_high_priority_size = (uint16_t) 8;
    params->event_queue_low_priority_size = (uint16_t) 64;
    params->delta_time = (double) 0.1;
    cfl_runtime_handle_t *handle = cfl_runtime_create(&perm, params, test_handle);
    cfl_runtime_create_params_destroy(params);
    if (!handle) {
        printf("Failed to create runtime handle\n");
        return -1;
    }
   
    cfl_runtime_reset(handle);
    cfl_add_test_by_index(handle, 0);
    bool result = cfl_runtime_run(handle);
    printf("Runtime run result: %d\n", result);
    return 0;
}
