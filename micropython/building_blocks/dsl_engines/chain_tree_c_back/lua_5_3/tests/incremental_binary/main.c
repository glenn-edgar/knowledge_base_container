/*
 * Lua 5.3+ bridge test — incremental_binary first_test
 *
 * Same as the pure-C incremental_binary test, but all user functions
 * are implemented in Lua via the cfl_lua53_bridge.
 *
 * Built-in CFL functions (CFL_WAIT, CFL_VERIFY, CFL_LOG, etc.)
 * remain in C from runtime_functions.
 */

#include <stdio.h>
#include <stdlib.h>
#include <stdbool.h>

#include <lua.h>
#include <lauxlib.h>
#include <lualib.h>

#include "cfl_runtime.h"
#include "cfl_chaintree_support.h"
#include "cfl_file_loader.h"
#include "cfl_function_loader.h"
#include "cfl_image_loader.h"
#include "cfl_lua53_bridge.h"

/* Embedded binary image from the DSL compiler */
#include "chaintree_handle_image.h"

static cfl_perm_t perm;
static char perm_buffer[0xffff];

/* ========================================================================
 * Register all user functions from Lua
 *
 * Loads user_functions.lua, then registers each named Lua function
 * with the image loader via the bridge trampoline table.
 * ======================================================================== */

static int register_lua_user_functions(cfl_image_loader_t *img, lua_State *L)
{
    int rc;
    int missing = 0;

    /* Helper macro: get global Lua function, register as one-shot */
    #define REG_OS(cfl_name, lua_name) do { \
        lua_getglobal(L, lua_name); \
        if (!lua_isfunction(L, -1)) { \
            fprintf(stderr, "  WARN: Lua function '%s' not found\n", lua_name); \
            lua_pop(L, 1); missing++; \
        } else { \
            rc = cfl_lua_bridge_register_one_shot(img, cfl_name, L); \
            if (rc < 0) { fprintf(stderr, "  WARN: '%s' not in image\n", cfl_name); missing++; } \
        } \
    } while(0)

    #define REG_MAIN(cfl_name, lua_name) do { \
        lua_getglobal(L, lua_name); \
        if (!lua_isfunction(L, -1)) { \
            fprintf(stderr, "  WARN: Lua function '%s' not found\n", lua_name); \
            lua_pop(L, 1); missing++; \
        } else { \
            rc = cfl_lua_bridge_register_main(img, cfl_name, L); \
            if (rc < 0) { fprintf(stderr, "  WARN: '%s' not in image\n", cfl_name); missing++; } \
        } \
    } while(0)

    #define REG_BOOL(cfl_name, lua_name) do { \
        lua_getglobal(L, lua_name); \
        if (!lua_isfunction(L, -1)) { \
            fprintf(stderr, "  WARN: Lua function '%s' not found\n", lua_name); \
            lua_pop(L, 1); missing++; \
        } else { \
            rc = cfl_lua_bridge_register_boolean(img, cfl_name, L); \
            if (rc < 0) { fprintf(stderr, "  WARN: '%s' not in image\n", cfl_name); missing++; } \
        } \
    } while(0)

    /* --- One-shot user functions --- */
    REG_OS("activate_valve_one_shot",                  "activate_valve_one_shot");
    REG_OS("wait_for_event_error_one_shot",            "wait_for_event_error_one_shot");
    REG_OS("verify_error_one_shot",                    "verify_error_one_shot");
    REG_OS("initialize_sequence_one_shot",             "initialize_sequence_one_shot");
    REG_OS("display_sequence_till_result_one_shot",    "display_sequence_till_result_one_shot");
    REG_OS("display_sequence_result_one_shot",         "display_sequence_result_one_shot");
    REG_OS("display_failure_window_result_one_shot",   "display_failure_window_result_one_shot");
    REG_OS("watch_dog_time_out_one_shot",              "watch_dog_time_out_one_shot");
    REG_OS("exception_logging_one_shot",               "exception_logging_one_shot");
    REG_OS("sm_event_filtering_init_one_shot",         "sm_event_filtering_init_one_shot");
    REG_OS("while_bitmask_failure_one_shot",           "while_bitmask_failure_one_shot");
    REG_OS("verify_bitmask_failure_one_shot",          "verify_bitmask_failure_one_shot");
    REG_OS("wait_for_test_complete_error_one_shot",    "wait_for_test_complete_error_one_shot");
    REG_OS("verify_tests_active_error_one_shot",       "verify_tests_active_error_one_shot");
    REG_OS("generate_avro_packet_one_shot",            "generate_avro_packet_one_shot");
    REG_OS("avro_verify_packet_init_one_shot",         "avro_verify_packet_init_one_shot");
    REG_OS("packet_generator_one_shot",                "packet_generator_one_shot");
    REG_OS("generate_const_avro_packet_one_shot",      "generate_const_avro_packet_one_shot");
    REG_OS("avro_verify_const_packet_init_one_shot",   "avro_verify_const_packet_init_one_shot");
    REG_OS("update_fly_straight_final_one_shot",       "update_fly_straight_final_one_shot");
    REG_OS("update_fly_arc_final_one_shot",            "update_fly_arc_final_one_shot");
    REG_OS("update_fly_up_final_one_shot",             "update_fly_up_final_one_shot");
    REG_OS("update_fly_down_final_one_shot",           "update_fly_down_final_one_shot");

    /* Blackboard test one-shots */
    REG_OS("bb_init_fields_one_shot",                  "bb_init_fields_one_shot");
    REG_OS("bb_verify_basic_fields_one_shot",          "bb_verify_basic_fields_one_shot");
    REG_OS("bb_verify_nested_fields_one_shot",         "bb_verify_nested_fields_one_shot");
    REG_OS("bb_verify_const_record_one_shot",          "bb_verify_const_record_one_shot");
    REG_OS("bb_verify_ptr64_field_one_shot",           "bb_verify_ptr64_field_one_shot");

    /* --- Main user functions --- */
    REG_MAIN("sm_event_filtering_main_main",           "sm_event_filtering_main_main");
    REG_MAIN("avro_verify_const_packet_main",          "avro_verify_const_packet_main");
    REG_MAIN("avro_verify_packet_main",                "avro_verify_packet_main");

    /* --- Boolean user functions --- */
    REG_BOOL("while_test_boolean",                     "while_test_boolean");
    REG_BOOL("catch_all_exception_boolean",            "catch_all_exception_boolean");
    REG_BOOL("exception_filter_boolean",               "exception_filter_boolean");
    REG_BOOL("user_skip_condition_boolean",            "user_skip_condition_boolean");
    REG_BOOL("packet_filter_boolean",                  "packet_filter_boolean");
    REG_BOOL("packet_sink_a_boolean",                  "packet_sink_a_boolean");
    REG_BOOL("packet_sink_b_boolean",                  "packet_sink_b_boolean");
    REG_BOOL("packet_tap_boolean",                     "packet_tap_boolean");
    REG_BOOL("packet_transform_boolean",               "packet_transform_boolean");
    REG_BOOL("packet_collector_boolean",               "packet_collector_boolean");
    REG_BOOL("packet_collector_sink_boolean",          "packet_collector_sink_boolean");
    REG_BOOL("packet_verify_x_range_boolean",          "packet_verify_x_range_boolean");
    REG_BOOL("packet_verified_sink_boolean",           "packet_verified_sink_boolean");
    REG_BOOL("fly_straight_monitor_boolean",           "fly_straight_monitor_boolean");
    REG_BOOL("fly_arc_monitor_boolean",                "fly_arc_monitor_boolean");
    REG_BOOL("fly_up_monitor_boolean",                 "fly_up_monitor_boolean");
    REG_BOOL("fly_down_monitor_boolean",               "fly_down_monitor_boolean");
    REG_BOOL("on_fly_straight_complete_boolean",       "on_fly_straight_complete_boolean");
    REG_BOOL("on_fly_arc_complete_boolean",            "on_fly_arc_complete_boolean");
    REG_BOOL("on_fly_up_complete_boolean",             "on_fly_up_complete_boolean");
    REG_BOOL("on_fly_down_complete_boolean",           "on_fly_down_complete_boolean");
    REG_BOOL("drone_control_exception_catch_boolean",  "drone_control_exception_catch_boolean");

    #undef REG_OS
    #undef REG_MAIN
    #undef REG_BOOL

    return missing;
}

/* ========================================================================
 * Main
 * ======================================================================== */

int main(int argc, char *argv[])
{
    int test_index = 3;  /* first_test */
    if (argc > 1)
        test_index = atoi(argv[1]);

    /* ---- Create Lua state ---- */
    lua_State *L = luaL_newstate();
    if (!L) {
        fprintf(stderr, "Failed to create Lua state\n");
        return -1;
    }
    luaL_openlibs(L);

    /* Initialize bridge (creates "cfl" module in Lua) */
    cfl_lua_bridge_init(L);

    /* Load user function definitions */
    if (luaL_dofile(L, "user_functions.lua") != LUA_OK) {
        fprintf(stderr, "Error loading user_functions.lua: %s\n",
                lua_tostring(L, -1));
        lua_close(L);
        return -1;
    }
    printf("Loaded user_functions.lua\n");

    /* ---- Load binary image ---- */
    cfl_image_loader_t img;
    int rc = cfl_embedded_load(chaintree_handle_image,
                                CHAINTREE_HANDLE_IMAGE_SIZE, &img);
    if (rc != CFL_IMAGE_OK) {
        fprintf(stderr, "Error: failed to load embedded image (error %d)\n", rc);
        lua_close(L);
        return -1;
    }
    printf("Loaded embedded image (%u bytes)\n", CHAINTREE_HANDLE_IMAGE_SIZE);

    /* ---- Register built-in CFL functions (C) ---- */
    cfl_register_all_functions(&img);

    /* ---- Register user functions (Lua via bridge) ---- */
    int lua_missing = register_lua_user_functions(&img, L);
    printf("Lua user functions registered (missing: %d)\n", lua_missing);

    /* ---- Validate ---- */
    int missing = cfl_image_validate(&img);
    if (missing > 0) {
        fprintf(stderr, "Warning: %d function(s) not registered\n", missing);
        cfl_image_free(&img);
        lua_close(L);
        return -1;
    }
    printf("All functions registered (missing: %d)\n", missing);

    /* ---- Get handle ---- */
    const cfl_chaintree_handle_t *test_handle = cfl_image_get_handle(&img);
    if (!test_handle) {
        fprintf(stderr, "Error: test_handle is NULL\n");
        cfl_image_free(&img);
        lua_close(L);
        return -1;
    }

    printf("unique_id: %s\n", test_handle->unique_id);
    printf("node_count: %d\n", test_handle->node_count);
    printf("kb_count: %d\n", test_handle->kb_count);

    /* ---- Create runtime ---- */
    cfl_runtime_create_params_t *params = cfl_runtime_create_params_create();
    if (!params) {
        fprintf(stderr, "Failed to allocate params\n");
        cfl_image_free(&img);
        lua_close(L);
        return -1;
    }

    params->perm = &perm;
    params->perm_buffer = perm_buffer;
    params->perm_buffer_size = (uint16_t)sizeof(perm_buffer);
    params->heap_size = (uint16_t)4096;
    params->max_allocator_count = cfl_calculate_arrena_number(test_handle);
    params->total_node_count = test_handle->node_count;
    params->allocator_0_size = (uint16_t)50;
    params->event_queue_high_priority_size = (uint16_t)8;
    params->event_queue_low_priority_size = (uint16_t)64;
    params->delta_time = (double)0.1;

    cfl_runtime_handle_t *handle = cfl_runtime_create(&perm, params, test_handle);
    cfl_runtime_create_params_destroy(params);

    if (!handle) {
        fprintf(stderr, "Failed to create runtime handle\n");
        cfl_image_free(&img);
        lua_close(L);
        return -1;
    }
    printf("Runtime handle created\n");

    /* ---- Attach Lua state to runtime ---- */
    cfl_lua_bridge_attach(handle, L);

    cfl_runtime_reset(handle);

    /* ---- Run test ---- */
    printf("Adding test by index: %d\n", test_index);
    cfl_add_test_by_index(handle, test_index);

    printf("heap used bytes: %d\n", cfl_heap_used_bytes(handle->heap));
    printf("heap free bytes: %d\n", cfl_heap_free_bytes(handle->heap));

    bool result = cfl_runtime_run(handle);
    printf("Runtime run result: %d\n", result);

    /* ---- Cleanup ---- */
    cfl_image_free(&img);
    lua_close(L);

    return result ? 0 : -1;
}
