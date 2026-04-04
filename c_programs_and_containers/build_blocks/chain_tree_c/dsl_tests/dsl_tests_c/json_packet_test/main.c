/* ========================================================================
 * main.c — JSON Packet Pipeline Integration Test
 *
 * Loads the chaintree_handle.ctb binary image (embedded), registers all
 * built-in CFL functions plus user boolean callbacks, creates the runtime,
 * runs the json_packet_pipeline_test KB, and prints results.
 *
 * Expected output:
 *   5 telemetry sinks (seq 0,2,4,6,7)
 *   1 command sink (seq 1)
 *   1 config sink (seq 3)
 *   1 default sink (seq 5, type "diagnostics")
 *   1 smoothed output (average of first 3 telemetry packets)
 * ======================================================================== */

#include <stdio.h>
#include <stdlib.h>
#include <stdbool.h>
#include "cfl_runtime.h"
#include "cfl_chaintree_support.h"
#include "cfl_file_loader.h"
#include "cfl_function_loader.h"
#include "cfl_image_loader.h"
#include "cfl_exception.h"
#include "chaintree_handle_image.h"
#include "chaintree_handle_blackboard.h"
#include "user_json_functions.h"
#include "user_json_drone_functions.h"

/* ── User boolean function registration ── */

static void register_user_boolean_functions(cfl_image_loader_t *img) {
    int rc;
    #define REG_BOOL(name_str, sym) do { \
        rc = cfl_image_register_boolean(img, (name_str), (sym)); \
        if (rc < 0) fprintf(stderr, "WARN: boolean '%s' not found in image\n", name_str); \
    } while(0)

    REG_BOOL("json_telem_sink_boolean",        json_telem_sink_boolean_fn);
    REG_BOOL("json_verify_x_range_boolean",  json_verify_x_range_boolean_fn);
    REG_BOOL("json_verified_sink_boolean",   json_verified_sink_boolean_fn);

    /* JSON drone control — server monitors */
    REG_BOOL("json_fly_straight_monitor_boolean", json_fly_straight_monitor_boolean_fn);
    REG_BOOL("json_fly_arc_monitor_boolean",      json_fly_arc_monitor_boolean_fn);
    REG_BOOL("json_fly_up_monitor_boolean",       json_fly_up_monitor_boolean_fn);
    REG_BOOL("json_fly_down_monitor_boolean",     json_fly_down_monitor_boolean_fn);

    /* JSON drone control — client completions */
    REG_BOOL("json_on_fly_straight_complete_boolean", json_on_fly_straight_complete_boolean_fn);
    REG_BOOL("json_on_fly_arc_complete_boolean",      json_on_fly_arc_complete_boolean_fn);
    REG_BOOL("json_on_fly_up_complete_boolean",       json_on_fly_up_complete_boolean_fn);
    REG_BOOL("json_on_fly_down_complete_boolean",     json_on_fly_down_complete_boolean_fn);

    /* JSON drone control — exception handler */
    REG_BOOL("json_drone_exception_catch_boolean", json_drone_exception_catch_boolean_fn);

    #undef REG_BOOL
}

/* ── User one-shot function registration ── */

static void register_user_one_shot_functions(cfl_image_loader_t *img) {
    int rc;
    #define REG_OS(name_str, sym) do { \
        rc = cfl_image_register_one_shot(img, (name_str), (sym)); \
        if (rc < 0) fprintf(stderr, "WARN: one_shot '%s' not found in image\n", name_str); \
    } while(0)

    REG_OS("json_fly_straight_final_one_shot", json_fly_straight_final_one_shot_fn);
    REG_OS("json_fly_arc_final_one_shot",      json_fly_arc_final_one_shot_fn);
    REG_OS("json_fly_up_final_one_shot",       json_fly_up_final_one_shot_fn);
    REG_OS("json_fly_down_final_one_shot",     json_fly_down_final_one_shot_fn);

    #undef REG_OS
}

/* ── Memory ── */

static cfl_perm_t perm;
static char perm_buffer[0xffff];

/* ── Main ── */

int main(int argc, char *argv[]) {
    printf("=== JSON Packet Pipeline Integration Test ===\n\n");

    /* 1. Load embedded binary image */
    cfl_image_loader_t img;
    int rc = cfl_embedded_load(chaintree_handle_image,
                               CHAINTREE_HANDLE_IMAGE_SIZE, &img);
    if (rc != CFL_IMAGE_OK) {
        printf("Error: failed to load embedded image (error %d)\n", rc);
        return -1;
    }
    printf("Loaded embedded image (%u bytes)\n", CHAINTREE_HANDLE_IMAGE_SIZE);

    /* 2. Register all functions */
    cfl_register_all_functions(&img);
    register_user_boolean_functions(&img);
    register_user_one_shot_functions(&img);

    int missing = cfl_image_validate(&img);
    if (missing > 0) {
        printf("Error: %d function(s) not registered\n", missing);
        cfl_image_free(&img);
        return -1;
    }
    printf("All functions registered (missing: %d)\n", missing);

    /* 3. Get handle */
    const cfl_chaintree_handle_t *test_handle = cfl_image_get_handle(&img);
    if (!test_handle) {
        printf("Error: test_handle is NULL\n");
        cfl_image_free(&img);
        return -1;
    }

    printf("unique_id: %s\n", test_handle->unique_id);
    printf("node_count: %d\n", test_handle->node_count);
    printf("kb_count: %d\n", test_handle->kb_count);

    /* 4. Create runtime */
    cfl_runtime_create_params_t *params = cfl_runtime_create_params_create();
    if (!params) {
        printf("Failed to allocate params\n");
        cfl_image_free(&img);
        return -1;
    }

    params->perm = &perm;
    params->perm_buffer = perm_buffer;
    params->perm_buffer_size = (uint16_t)sizeof(perm_buffer);
    params->heap_size = (uint16_t)32000;
    params->max_allocator_count = 3;
    params->total_node_count = test_handle->node_count;
    params->allocator_0_size = (uint16_t)2048;
    params->event_queue_high_priority_size = (uint16_t)8;
    params->event_queue_low_priority_size = (uint16_t)64;
    params->delta_time = (double)0.1;

    cfl_runtime_handle_t *handle = cfl_runtime_create(&perm, params, test_handle);
    cfl_runtime_create_params_destroy(params);

    if (!handle) {
        printf("Failed to create runtime\n");
        cfl_image_free(&img);
        return -1;
    }
    printf("Runtime created\n");
    cfl_runtime_reset(handle);

    /* 5. Select test KB */
    int test_index = 0;  /* json_packet_pipeline_test */
    if (argc > 1) {
        test_index = atoi(argv[1]);
    }
    printf("Running KB index: %d\n", test_index);
    cfl_add_test_by_index(handle, test_index);

    printf("heap used: %d bytes, free: %d bytes\n\n",
           cfl_heap_used_bytes(handle->heap),
           cfl_heap_free_bytes(handle->heap));

    /* 6. Run */
    printf("--- Pipeline Output ---\n");
    bool result = cfl_runtime_run(handle);
    printf("--- End Pipeline Output ---\n\n");

    printf("Runtime result: %s\n", result ? "PASS" : "FAIL");

    /* 7. Cleanup */
    cfl_image_free(&img);

    return result ? 0 : -1;
}
