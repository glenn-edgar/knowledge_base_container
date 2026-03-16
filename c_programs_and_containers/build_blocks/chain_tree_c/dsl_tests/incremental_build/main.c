/* test of runtime system */
#include <stdio.h>
#include <stdlib.h>
#include <stdbool.h>
#include "cfl_runtime.h"
#include "chaintree_support.h"
#include "cfl_exception.h"
#include "incr/chaintree_handle.h"


static cfl_perm_t perm;
static char perm_buffer[0xffff];


int main(void) {
    const chaintree_handle_t *test_handle = &g_chaintree_handle;
    
    /* Validate test_handle */
    if (!test_handle) {
        printf("Error: test_handle is NULL\n");
        return -1;
    }
    
    /* Validate test index is within bounds */
    const uint16_t test_index = 0;
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
    printf("heap_size: %d\n", params->heap_size);
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
    printf("runtime handle created\n");
    cfl_runtime_reset(handle);
    

   
    
    
 

    //cfl_add_test_by_index(handle, 0); //first test
    //cfl_add_test_by_index(handle, 1); //second test
    //cfl_add_test_by_index(handle, 2);//fourth test
    //cfl_add_test_by_index(handle, 3); //fifth test
    //cfl_add_test_by_index(handle, 4); //sixth test
    //cfl_add_test_by_index(handle, 5); //seventh test
    //cfl_add_test_by_index(handle, 6); //eighth test
    //cfl_add_test_by_index(handle, 7); //ninth test
    //cfl_add_test_by_index(handle, 8); //tenth test
    //cfl_add_test_by_index(handle, 9); //eleventh test
    //cfl_add_test_by_index(handle, 10); //twelfth test
    //cfl_add_test_by_index(handle, 11); //thirteenth test
    //cfl_add_test_by_index(handle, 12); //fourteenth test
    //cfl_add_test_by_index(handle, 13); //seventeenth test
    //cfl_add_test_by_index(handle, 14); //eighteenth test
    //cfl_add_test_by_index(handle, 15); //nineteenth test
    //cfl_add_test_by_index(handle, 16); //twentieth test
    //cfl_add_test_by_index(handle, 17); //twenty-first test
    cfl_add_test_by_index(handle, 18); //twenty-second test
    cfl_add_test_by_index(handle, 19); //twenty-third test

    //cfl_add_test_by_index(handle, 20); //twenty-fourth test
    //cfl_add_test_by_index(handle, 21); //twenty-fifth test
    //cfl_add_test_by_index(handle, 22); //twenty-sixth test
    //cfl_add_test_by_index(handle, 23); //twenty-seventh test
    //cfl_add_test_by_index(handle, 24); //twenty-eighth test
    //cfl_add_test_by_index(handle, 25); //twenty-ninth test
   //cfl_add_test_by_index(handle, 26); //thirty test
    //cfl_add_test_by_index(handle, 27); //thirty-one test
    //cfl_add_test_by_index(handle, 28); //thirty-two test
     //thirty-three test
    printf("heap used bytes: %d\n", cfl_heap_used_bytes(handle->heap));
    printf("heap free bytes: %d\n", cfl_heap_free_bytes(handle->heap));
    
    bool result = cfl_runtime_run(handle);
    printf("Runtime run result: %d\n", result);
    
    /* Note: If there's a cfl_runtime_destroy() function, call it here:
     * cfl_runtime_destroy(handle);
     */
    
    return result ? 0 : -1;
}