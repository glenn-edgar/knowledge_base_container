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
    const chaintree_handle_t *test_handle = &g_test_header;
    
    /* Validate test_handle */
    if (!test_handle) {
        printf("Error: test_handle is NULL\n");
        return -1;
    }
    
    /* Validate test index is within bounds */
    const uint16_t test_index = 3;
    if (test_index >= test_handle->kb_count) {
        printf("Error: test_index %d >= kb_count %d\n", test_index, test_handle->kb_count);
        return -1;
    }
    
    /* Use the provided API function instead of malloc */
    cfl_runtime_create_params_t* params = cfl_runtime_create_params_create();
    if (!params) {
        printf("Failed to allocate memory for params\n");
        return -1;
    }
    
    params->perm = &perm;
    params->perm_buffer = perm_buffer;
    params->perm_buffer_size = (uint16_t) sizeof(perm_buffer);
    params->heap_size = (uint16_t)  4096;
    params->max_allocator_count = cfl_calculate_arrena_number(test_handle);
    params->total_node_count = test_handle->node_count;
    printf("total_node_count: %d\n", params->total_node_count);
    
    /* Check for overflow in allocator_0_size calculation */
    size_t allocator_size = (size_t)50;
    if (allocator_size > 65535) {
        printf("Error: allocator_0_size calculation overflow: %zu > 65535\n", allocator_size);
        cfl_runtime_create_params_destroy(params);
        return -1;
    }
    params->allocator_0_size = (uint16_t)allocator_size;
    
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
    
    #if 0
    if (!cfl_add_test_by_index(handle, test_index)) {
        printf("Failed to add test at index %d\n", test_index);
        /* Note: Should have a cfl_runtime_destroy(handle) here if it exists */
        return -1;
    }
    #endif

    //cfl_add_test_by_index(handle, 0); //first test
    //cfl_add_test_by_index(handle, 1); //second test
    //cfl_add_test_by_index(handle, 2);//fourth test
    //cfl_add_test_by_index(handle, 3); //fifth test
    //cfl_add_test_by_index(handle, 4); //sixth test
    //cfl_add_test_by_index(handle, 5); //seventh test
    //cfl_add_test_by_index(handle, 6); //eighth test
    cfl_add_test_by_index(handle, 7); //ninth test
    
    printf("heap used bytes: %d\n", cfl_heap_used_bytes(handle->heap));
    printf("heap free bytes: %d\n", cfl_heap_free_bytes(handle->heap));
    bool result = cfl_runtime_run(handle);
    printf("Runtime run result: %d\n", result);
    
    /* Note: If there's a cfl_runtime_destroy() function, call it here:
     * cfl_runtime_destroy(handle);
     */
    
    return result ? 0 : -1;
}