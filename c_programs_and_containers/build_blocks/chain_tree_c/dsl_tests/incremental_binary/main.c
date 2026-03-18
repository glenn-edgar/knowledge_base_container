/* test of runtime system — binary image loader path */
#include <stdio.h>
#include <stdlib.h>
#include <stdbool.h>
#include "cfl_runtime.h"
#include "cfl_chaintree_support.h"
#include "cfl_file_loader.h"
#include "cfl_function_loader.h"
#include "cfl_exception.h"
#include "chaintree_handle_image.h"
#include "user_one_shot_functions.h"
#include "user_boolean_functions.h"
#include "user_main_functions.h"
static cfl_perm_t perm;
static char perm_buffer[0xffff];

int main(void) {
   
   
#if 0
    /* ---- Load binary image ---- */
    cfl_image_loader_t img;
    int rc = cfl_file_load(ctb_path, &img);
    if (rc != CFL_IMAGE_OK) {
        printf("Error: failed to load %s (error %d)\n", ctb_path, rc);
        return -1;
    }
    printf("Loaded: %s\n", ctb_path);
#endif
    cfl_image_loader_t img;
    int rc = cfl_embedded_load(chaintree_handle_image, CHAINTREE_HANDLE_IMAGE_SIZE, &img);
    if (rc != CFL_IMAGE_OK) {
        printf("Error: failed to load embedded image (error %d)\n", rc);
        return -1;
    }
    printf("Loaded embedded image (%u bytes)\n", CHAINTREE_HANDLE_IMAGE_SIZE);
    cfl_register_all_functions(&img);
    cfl_register_user_one_shot_functions(&img);
    cfl_register_user_boolean_functions(&img);
    cfl_register_user_main_functions(&img);
    int missing = cfl_image_validate(&img);
    if (missing > 0) {
        printf("Warning: %d function(s) not registered\n", missing);
        cfl_image_free(&img);
        return -1;
    }
    printf("Functions registered (missing: %d)\n", missing);

    /* ---- Get the handle — identical to .h/.c g_incr from here on ---- */
    const cfl_chaintree_handle_t *test_handle = cfl_image_get_handle(&img);
    if (!test_handle) {
        printf("Error: test_handle is NULL\n");
        cfl_file_unload(&img);
        return -1;
    }

    printf("unique_id: %s\n", test_handle->unique_id);
    printf("node_count: %d\n", test_handle->node_count);
    printf("kb_count: %d\n", test_handle->kb_count);

   

   /* ---- Create runtime (same as .h/.c path from here) ---- */
    cfl_runtime_create_params_t *params = cfl_runtime_create_params_create();
    if (!params) {
        printf("Failed to allocate memory for params\n");
        cfl_image_free(&img);
        return -1;
    }
    
    params->perm = &perm;
    params->perm_buffer = perm_buffer;
    params->perm_buffer_size = (uint16_t)sizeof(perm_buffer);
    params->heap_size = (uint16_t)4096;
    printf("heap_size: %d\n", params->heap_size);
    params->max_allocator_count = cfl_calculate_arrena_number(test_handle);
    params->total_node_count = test_handle->node_count;
    printf("total_node_count: %d\n", params->total_node_count);

    size_t allocator_size = (size_t)50;
    if (allocator_size > 65535) {
        printf("Error: allocator_0_size calculation overflow: %zu > 65535\n", allocator_size);
        cfl_runtime_create_params_destroy(params);
        cfl_image_free(&img);
        return -1;
    }
    params->allocator_0_size = (uint16_t)allocator_size;

    params->event_queue_high_priority_size = (uint16_t)8;
    params->event_queue_low_priority_size = (uint16_t)64;
    params->delta_time = (double)0.1;

    cfl_runtime_handle_t *handle = cfl_runtime_create(&perm, params, test_handle);
    cfl_runtime_create_params_destroy(params);

    if (!handle) {
        printf("Failed to create runtime handle\n");
        cfl_image_free(&img);
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
    cfl_add_test_by_index(handle, 14); //eighteenth test
    //cfl_add_test_by_index(handle, 15); //nineteenth test
    //cfl_add_test_by_index(handle, 16); //twentieth test
    //cfl_add_test_by_index(handle, 17); //twenty-first test
    //cfl_add_test_by_index(handle, 18); //twenty-second test
    //cfl_add_test_by_index(handle, 19); //twenty-third test
    //cfl_add_test_by_index(handle, 20); //twenty-fourth test
    //cfl_add_test_by_index(handle, 20); //twenty-fourth test
    //cfl_add_test_by_index(handle, 21); //twenty-fifth test
    //cfl_add_test_by_index(handle, 22); //twenty-sixth test
    //cfl_add_test_by_index(handle, 23); //twenty-seventh test
    //cfl_add_test_by_index(handle, 24); //twenty-eighth test
    //cfl_add_test_by_index(handle, 25); //twenty-ninth test
   //cfl_add_test_by_index(handle, 26); //thirty test
    //cfl_add_test_by_index(handle, 27); //thirty-one test
    //cfl_add_test_by_index(handle, 28); //thirty-two test

    printf("heap used bytes: %d\n", cfl_heap_used_bytes(handle->heap));
    printf("heap free bytes: %d\n", cfl_heap_free_bytes(handle->heap));

    bool result = cfl_runtime_run(handle);
    printf("Runtime run result: %d\n", result);

    /* Clean up — free RAM arrays, then free file buffer */
    cfl_image_free(&img);

    return result ? 0 : -1;
}
