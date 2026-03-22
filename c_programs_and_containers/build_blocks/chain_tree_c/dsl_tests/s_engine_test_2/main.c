/* ====================================================================
 * S-Engine Integration Test 2 — se_engine composite + se_engine_link
 *
 * Tests: bitmask triggers, state machines with child columns,
 *        field dispatch, event dispatch, nested fields, pointers,
 *        linked lists, JSON reads, constant copies, external init.
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
#include "chain_flow_dsl_tests.h"
#include "chain_flow_dsl_tests_bin_32.h"

/* ====================================================================
 * User boolean functions
 *
 * Register any ChainTree boolean functions needed by the DSL here.
 * Each function is registered by its typed name (lowercase + _boolean).
 * ==================================================================== */

/* Dummy — used by se_module_load when no user s-engine functions needed */
static bool user_register_s_functions_fn(void *handle, unsigned node_index,
                                         unsigned event_type, unsigned event_id,
                                         void *event_data)
{
    (void)handle; (void)node_index;
    (void)event_type; (void)event_id; (void)event_data;
    return false;
}

/* TEST_39: External init — sets up blackboard before tree runs */
extern bool test_39_set_init_data_boolean_fn(void *, unsigned, unsigned, unsigned, void *);

/* ====================================================================
 * S-Engine user function registration
 *
 * The s-engine compiler generates a _user_registration.c that we
 * override manually to exclude CFL bridge functions (already in
 * cfl_se_get_*_table()). Only true user functions are registered here.
 * ==================================================================== */

extern void chain_flow_dsl_tests_register_all(s_expr_module_t *module);

static void user_register_wrapper(s_expr_module_t *mod, void *ctx) {
    (void)ctx;
    chain_flow_dsl_tests_register_all(mod);
}

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
    cfl_image_register_boolean(&img, "test_39_set_init_data_boolean",
                               test_39_set_init_data_boolean_fn);

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
    params->heap_size                     = (uint16_t)32768;
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
     * ================================================================ */

    cfl_se_module_registry_t *reg = cfl_se_registry_create(handle);
    if (!reg) {
        EXCEPTION("cfl_se_registry_create failed");
    }
    cfl_set_app_extensions(handle, reg);

    /* Register s-engine module binary + user function callback */
    cfl_se_registry_register_def_with_user(reg, "chain_flow_dsl_tests",
        chain_flow_dsl_tests_module_bin_32,
        CHAIN_FLOW_DSL_TESTS_MODULE_BIN_32_SIZE,
        user_register_wrapper, NULL);

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
        //cfl_add_test_by_index(handle, 0);  /* twenty_ninth_test — bitmask triggers */
        //cfl_add_test_by_index(handle, 1);  /* thirty_test — state machine + child columns */
        //cfl_add_test_by_index(handle, 2);  /* thirty_one_test — field dispatch + event dispatch */
        cfl_add_test_by_index(handle, 3);    /* thirty_two_test — sequential link tests 10-16 */
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
