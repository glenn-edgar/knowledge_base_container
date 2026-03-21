/* S-Engine integration test — binary image loader path */
#include <stdio.h>
#include <stdlib.h>
#include <stdbool.h>
#include <string.h>
#include "cfl_runtime.h"
#include "cfl_chaintree_support.h"
#include "cfl_file_loader.h"
#include "cfl_function_loader.h"
#include "cfl_exception.h"
#include "cfl_blackboard.h"
#include "chaintree_handle_image.h"
#include "chaintree_handle_blackboard.h"

/* S-Engine module registry */
#include "cfl_se_module_registry.h"

/* Generated s-engine module definition (ROM) */
#include "se_test_module.h"
#include "se_test_module_bin_32.h"

/* ====================================================================
 * user_register_s_functions — ChainTree boolean function
 *
 * Called during module load to register user s-engine functions.
 * For this test the generated module has no user functions, so this
 * is a no-op. Return value is ignored.
 * ==================================================================== */

static bool user_register_s_functions_fn(void *handle, unsigned node_index,
                                         unsigned event_type, unsigned event_id,
                                         void *event_data)
{
    (void)handle; (void)node_index;
    (void)event_type; (void)event_id; (void)event_data;
    printf("user_register_s_functions: called (no user functions to register)\n");
    return false;
}

/* ====================================================================
 * Memory
 * ==================================================================== */

static cfl_perm_t perm;
static char perm_buffer[0xffff];

/* ====================================================================
 * Main
 * ==================================================================== */

int main(void) {
    /* ---- Load ChainTree binary image ---- */
    cfl_image_loader_t img;
    int rc = cfl_embedded_load(chaintree_handle_image,
                               CHAINTREE_HANDLE_IMAGE_SIZE, &img);
    if (rc != CFL_IMAGE_OK) {
        EXCEPTION("failed to load embedded ChainTree image");
    }
    printf("Loaded ChainTree embedded image (%u bytes)\n",
           CHAINTREE_HANDLE_IMAGE_SIZE);

    /* ---- Register ChainTree + SE runtime functions ---- */
    cfl_register_all_functions(&img);

    /* Register user boolean functions (test-specific) */
    cfl_image_register_boolean(&img, "user_register_s_functions_boolean",
                               user_register_s_functions_fn);
    cfl_image_register_boolean(&img, "se_custom_bb_load_boolean",
                               user_register_s_functions_fn);  /* dummy for now */

    int missing = cfl_image_validate(&img);
    if (missing > 0) {
        printf("Warning: %d function(s) not registered\n", missing);
        EXCEPTION("ChainTree function registration incomplete");
    }
    printf("All ChainTree functions registered\n");

    /* ---- Get the handle ---- */
    const cfl_chaintree_handle_t *test_handle = cfl_image_get_handle(&img);
    if (!test_handle) {
        EXCEPTION("cfl_image_get_handle returned NULL");
    }
    printf("unique_id: %s\n", test_handle->unique_id);
    printf("node_count: %d\n", test_handle->node_count);
    printf("kb_count: %d\n", test_handle->kb_count);

    /* ---- Create runtime ---- */
    cfl_runtime_create_params_t *params = cfl_runtime_create_params_create();
    if (!params) {
        EXCEPTION("cfl_runtime_create_params_create failed");
    }
    params->perm = &perm;
    params->perm_buffer = perm_buffer;
    params->perm_buffer_size = (uint16_t)sizeof(perm_buffer);
    params->heap_size = (uint16_t)8192;
    params->max_allocator_count = cfl_calculate_arrena_number(test_handle);
    params->total_node_count = test_handle->node_count;
    params->allocator_0_size = (uint16_t)128;
    params->event_queue_high_priority_size = (uint16_t)8;
    params->event_queue_low_priority_size = (uint16_t)64;
    params->delta_time = (double)0.1;

    cfl_runtime_handle_t *handle = cfl_runtime_create(&perm, params, test_handle);
    cfl_runtime_create_params_destroy(params);
    if (!handle) {
        EXCEPTION("cfl_runtime_create failed");
    }
    printf("Runtime handle created\n");

    /* ---- Create S-Engine module registry (app extension) ---- */
    cfl_se_module_registry_t *reg = cfl_se_registry_create(handle);
    if (!reg) {
        EXCEPTION("cfl_se_registry_create failed");
    }
    cfl_set_app_extensions(handle, reg);
    printf("S-Engine registry created\n");

    /* ---- Associate s-engine module names with their compiled binaries ---- */
    cfl_se_registry_register_def(reg, "se_test_module",
        se_test_module_module_bin_32, SE_TEST_MODULE_MODULE_BIN_32_SIZE);

    /* ---- Reset and run ---- */
    cfl_runtime_reset(handle);
    cfl_add_test_by_index(handle, 0); /* se_basic_load_test */

    printf("heap used: %d bytes\n", cfl_heap_used_bytes(handle->heap));
    printf("heap free: %d bytes\n", cfl_heap_free_bytes(handle->heap));

    bool result = cfl_runtime_run(handle);
    printf("Runtime run result: %d\n", result);

    /* ---- Cleanup ---- */
    cfl_se_registry_destroy(reg);
    cfl_set_app_extensions(handle, NULL);
    cfl_image_free(&img);

    return result ? 0 : -1;
}
