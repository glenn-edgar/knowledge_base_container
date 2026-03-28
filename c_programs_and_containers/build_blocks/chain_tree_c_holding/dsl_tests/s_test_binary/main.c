/* ====================================================================
 * S-Engine Integration Test 1 — se_tick leaf node
 *
 * Tests: se_module_load + se_tree_load + se_tick pipeline,
 *        state machine tree with tick delays.
 *
 * Usage: ./main [test_index]
 *        Comment/uncomment cfl_add_test_by_index lines below to
 *        select which test(s) to run.
 * ==================================================================== */

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

/* ---- S-Engine support ---- */
#include "cfl_se_module_registry.h"

/* ---- Generated s-engine module (ROM binary) ---- */
#include "state_machine_test.h"
#include "state_machine_test_bin_32.h"

/* ====================================================================
 * User boolean functions
 * ==================================================================== */

/* Dummy — used by se_module_load when no user s-engine functions needed */
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
 * S-Engine user function registration
 *
 * No user functions in this module — registration is empty.
 * If user functions are added, provide a wrapper here and use
 * cfl_se_registry_register_def_with_user() in section 5.
 * ==================================================================== */

/* (none) */

/* ====================================================================
 * Memory
 * ==================================================================== */

static cfl_perm_t perm;
static char perm_buffer[0xffff];

/* ====================================================================
 * Main
 * ==================================================================== */

int main(int argc, char *argv[]) {

    /* ================================================================
     * 1. Load ChainTree binary image (embedded C array)
     * ================================================================ */

    cfl_image_loader_t img;
    int rc = cfl_embedded_load(chaintree_handle_image,
                               CHAINTREE_HANDLE_IMAGE_SIZE, &img);
    if (rc != CFL_IMAGE_OK) {
        EXCEPTION("failed to load embedded ChainTree image");
    }
    printf("Loaded ChainTree embedded image (%u bytes)\n",
           CHAINTREE_HANDLE_IMAGE_SIZE);

    /* ================================================================
     * 2. Register functions
     *    - Core ChainTree + CFL bridge functions (automatic)
     *    - User boolean functions (test-specific)
     * ================================================================ */

    cfl_register_all_functions(&img);

    /* -- User boolean functions -- */
    cfl_image_register_boolean(&img, "user_register_s_functions_boolean",
                               user_register_s_functions_fn);
    cfl_image_register_boolean(&img, "se_custom_bb_load_boolean",
                               user_register_s_functions_fn);  /* dummy */

    /* -- Validate all functions resolved -- */
    int missing = cfl_image_validate(&img);
    if (missing > 0) {
        printf("Warning: %d function(s) not registered\n", missing);
        EXCEPTION("ChainTree function registration incomplete");
    }
    printf("All ChainTree functions registered\n");

    /* ================================================================
     * 3. Get the handle and print summary
     * ================================================================ */

    const cfl_chaintree_handle_t *test_handle = cfl_image_get_handle(&img);
    if (!test_handle) {
        EXCEPTION("cfl_image_get_handle returned NULL");
    }
    printf("unique_id: %s\n", test_handle->unique_id);
    printf("node_count: %d\n", test_handle->node_count);
    printf("kb_count: %d\n", test_handle->kb_count);

    /* ================================================================
     * 4. Create runtime
     * ================================================================ */

    cfl_runtime_create_params_t *params = cfl_runtime_create_params_create();
    if (!params) {
        EXCEPTION("cfl_runtime_create_params_create failed");
    }
    params->perm                          = &perm;
    params->perm_buffer                   = perm_buffer;
    params->perm_buffer_size              = (uint16_t)sizeof(perm_buffer);
    params->heap_size                     = (uint16_t)8192;
    params->max_allocator_count           = cfl_calculate_arrena_number(test_handle);
    params->total_node_count              = test_handle->node_count;
    params->allocator_0_size              = (uint16_t)128;
    params->event_queue_high_priority_size = (uint16_t)8;
    params->event_queue_low_priority_size = (uint16_t)64;
    params->delta_time                    = (double)0.1;

    cfl_runtime_handle_t *handle = cfl_runtime_create(&perm, params, test_handle);
    cfl_runtime_create_params_destroy(params);
    if (!handle) {
        EXCEPTION("cfl_runtime_create failed");
    }
    printf("Runtime handle created\n");

    /* ================================================================
     * 5. Create S-Engine module registry and register module binaries
     *
     * No user functions — use cfl_se_registry_register_def().
     * If user functions are needed, switch to
     * cfl_se_registry_register_def_with_user() with a wrapper.
     * ================================================================ */

    cfl_se_module_registry_t *reg = cfl_se_registry_create(handle);
    if (!reg) {
        EXCEPTION("cfl_se_registry_create failed");
    }
    cfl_set_app_extensions(handle, reg);

    cfl_se_registry_register_def(reg, "state_machine_test",
        state_machine_test_module_bin_32, STATE_MACHINE_TEST_MODULE_BIN_32_SIZE);

    printf("S-Engine registry created\n");

    /* ================================================================
     * 6. Reset and select test(s)
     *
     * Comment/uncomment lines to select which test KB(s) to run.
     * Use ./main <index> to override from command line.
     * ================================================================ */

    cfl_runtime_reset(handle);

    if (argc > 1) {
        /* Command-line override */
        int test_index = atoi(argv[1]);
        printf("Running test index: %d\n", test_index);
        cfl_add_test_by_index(handle, test_index);
    } else {
        /* Incremental test selection — uncomment to run */
        cfl_add_test_by_index(handle, 0);    /* se_basic_load_test — state machine tree */
        //cfl_add_test_by_index(handle, 1);  /* se_multi_tree_test — two trees */
        //cfl_add_test_by_index(handle, 2);  /* se_custom_bb_test — custom bb loader */
    }

    /* ================================================================
     * 7. Run
     * ================================================================ */

    printf("heap used: %d bytes\n", cfl_heap_used_bytes(handle->heap));
    printf("heap free: %d bytes\n", cfl_heap_free_bytes(handle->heap));

    bool result = cfl_runtime_run(handle);
    printf("Runtime run result: %d\n", result);

    /* ================================================================
     * 8. Cleanup
     * ================================================================ */

    cfl_se_registry_destroy(reg);
    cfl_set_app_extensions(handle, NULL);
    cfl_image_free(&img);

    return result ? 0 : -1;
}
