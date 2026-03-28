/*
 * Lua 5.3+ bridge test — incremental_binary
 *
 * Non-avro user functions implemented in Lua via cfl_lua53_bridge.
 * Avro/streaming/drone functions remain in C.
 * Built-in CFL functions from runtime_functions library.
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

#include "chaintree_handle_image.h"

/* ========================================================================
 * C-only user functions (avro/streaming/drone)
 * These are compiled from .c files in this directory.
 * ======================================================================== */

/* user_avro_test_file.c */
extern void generate_avro_packet_one_shot(void *, unsigned);
extern void generate_const_avro_packet_one_shot(void *, unsigned);
extern void avro_verify_packet_init_one_shot_fn(void *, unsigned);
extern void avro_verify_const_packet_init_one_shot_fn(void *, unsigned);
extern unsigned avro_verify_packet_main_fn(void *, unsigned, unsigned, unsigned, unsigned, void *);
extern unsigned avro_verify_const_packet_main_fn(void *, unsigned, unsigned, unsigned, unsigned, void *);

/* user_streaming_boolean.c */
extern void packet_generator_one_shot_fn(void *, unsigned);
extern bool packet_filter_boolean_fn(void *, unsigned, unsigned, unsigned, void *);
extern bool packet_sink_a_boolean_fn(void *, unsigned, unsigned, unsigned, void *);
extern bool packet_sink_b_boolean_fn(void *, unsigned, unsigned, unsigned, void *);
extern bool packet_tap_boolean_fn(void *, unsigned, unsigned, unsigned, void *);
extern bool packet_transform_boolean_fn(void *, unsigned, unsigned, unsigned, void *);
extern bool packet_collector_boolean_fn(void *, unsigned, unsigned, unsigned, void *);
extern bool packet_collector_sink_boolean_fn(void *, unsigned, unsigned, unsigned, void *);
extern bool packet_verify_x_range_boolean_fn(void *, unsigned, unsigned, unsigned, void *);
extern bool packet_verified_sink_boolean_fn(void *, unsigned, unsigned, unsigned, void *);

/* user_node_control_boolean_fns.c */
extern bool on_fly_arc_complete_boolean_fn(void *, unsigned, unsigned, unsigned, void *);
extern bool on_fly_down_complete_boolean_fn(void *, unsigned, unsigned, unsigned, void *);
extern bool on_fly_straight_complete_boolean_fn(void *, unsigned, unsigned, unsigned, void *);
extern bool on_fly_up_complete_boolean_fn(void *, unsigned, unsigned, unsigned, void *);
extern bool fly_arc_monitor_boolean_fn(void *, unsigned, unsigned, unsigned, void *);
extern bool fly_down_monitor_boolean_fn(void *, unsigned, unsigned, unsigned, void *);
extern bool fly_straight_monitor_boolean_fn(void *, unsigned, unsigned, unsigned, void *);
extern bool fly_up_monitor_boolean_fn(void *, unsigned, unsigned, unsigned, void *);
extern void update_fly_arc_final_one_shot_fn(void *, unsigned);
extern void update_fly_down_final_one_shot_fn(void *, unsigned);
extern void update_fly_straight_final_one_shot_fn(void *, unsigned);
extern void update_fly_up_final_one_shot_fn(void *, unsigned);
extern bool drone_control_exception_catch_boolean_fn(void *, unsigned, unsigned, unsigned, void *);

static cfl_perm_t perm;
static char perm_buffer[0xffff];

/* ========================================================================
 * Register C-only avro/streaming/drone functions
 * ======================================================================== */

static void register_c_avro_functions(cfl_image_loader_t *img)
{
    int rc;
    #define REG_C_OS(n, f) do { rc = cfl_image_register_one_shot(img, n, f); \
        if (rc < 0) fprintf(stderr, "  WARN: C one_shot '%s' not in image\n", n); } while(0)
    #define REG_C_MAIN(n, f) do { rc = cfl_image_register_main(img, n, f); \
        if (rc < 0) fprintf(stderr, "  WARN: C main '%s' not in image\n", n); } while(0)
    #define REG_C_BOOL(n, f) do { rc = cfl_image_register_boolean(img, n, f); \
        if (rc < 0) fprintf(stderr, "  WARN: C boolean '%s' not in image\n", n); } while(0)

    /* Avro */
    REG_C_OS("generate_avro_packet_one_shot",          generate_avro_packet_one_shot);
    REG_C_OS("generate_const_avro_packet_one_shot",    generate_const_avro_packet_one_shot);
    REG_C_OS("avro_verify_packet_init_one_shot",       avro_verify_packet_init_one_shot_fn);
    REG_C_OS("avro_verify_const_packet_init_one_shot", avro_verify_const_packet_init_one_shot_fn);
    REG_C_MAIN("avro_verify_packet_main",              avro_verify_packet_main_fn);
    REG_C_MAIN("avro_verify_const_packet_main",        avro_verify_const_packet_main_fn);

    /* Streaming */
    REG_C_OS("packet_generator_one_shot",              packet_generator_one_shot_fn);
    REG_C_BOOL("packet_filter_boolean",                packet_filter_boolean_fn);
    REG_C_BOOL("packet_sink_a_boolean",                packet_sink_a_boolean_fn);
    REG_C_BOOL("packet_sink_b_boolean",                packet_sink_b_boolean_fn);
    REG_C_BOOL("packet_tap_boolean",                   packet_tap_boolean_fn);
    REG_C_BOOL("packet_transform_boolean",             packet_transform_boolean_fn);
    REG_C_BOOL("packet_collector_boolean",             packet_collector_boolean_fn);
    REG_C_BOOL("packet_collector_sink_boolean",        packet_collector_sink_boolean_fn);
    REG_C_BOOL("packet_verify_x_range_boolean",        packet_verify_x_range_boolean_fn);
    REG_C_BOOL("packet_verified_sink_boolean",         packet_verified_sink_boolean_fn);

    /* Drone control */
    REG_C_BOOL("on_fly_arc_complete_boolean",          on_fly_arc_complete_boolean_fn);
    REG_C_BOOL("on_fly_down_complete_boolean",         on_fly_down_complete_boolean_fn);
    REG_C_BOOL("on_fly_straight_complete_boolean",     on_fly_straight_complete_boolean_fn);
    REG_C_BOOL("on_fly_up_complete_boolean",           on_fly_up_complete_boolean_fn);
    REG_C_BOOL("fly_arc_monitor_boolean",              fly_arc_monitor_boolean_fn);
    REG_C_BOOL("fly_down_monitor_boolean",             fly_down_monitor_boolean_fn);
    REG_C_BOOL("fly_straight_monitor_boolean",         fly_straight_monitor_boolean_fn);
    REG_C_BOOL("fly_up_monitor_boolean",               fly_up_monitor_boolean_fn);
    REG_C_OS("update_fly_arc_final_one_shot",          update_fly_arc_final_one_shot_fn);
    REG_C_OS("update_fly_down_final_one_shot",         update_fly_down_final_one_shot_fn);
    REG_C_OS("update_fly_straight_final_one_shot",     update_fly_straight_final_one_shot_fn);
    REG_C_OS("update_fly_up_final_one_shot",           update_fly_up_final_one_shot_fn);
    REG_C_BOOL("drone_control_exception_catch_boolean", drone_control_exception_catch_boolean_fn);

    #undef REG_C_OS
    #undef REG_C_MAIN
    #undef REG_C_BOOL
}

/* ========================================================================
 * Register Lua user functions (non-avro)
 * ======================================================================== */

static int register_lua_user_functions(cfl_image_loader_t *img, lua_State *L)
{
    int rc;
    int missing = 0;

    #define REG_OS(cfl_name, lua_name) do { \
        lua_getglobal(L, lua_name); \
        if (!lua_isfunction(L, -1)) { lua_pop(L, 1); missing++; \
            fprintf(stderr, "  WARN: Lua '%s' not found\n", lua_name); \
        } else { \
            rc = cfl_lua_bridge_register_one_shot(img, cfl_name, L); \
            if (rc < 0) { fprintf(stderr, "  WARN: '%s' not in image\n", cfl_name); missing++; } \
        } \
    } while(0)

    #define REG_MAIN(cfl_name, lua_name) do { \
        lua_getglobal(L, lua_name); \
        if (!lua_isfunction(L, -1)) { lua_pop(L, 1); missing++; \
            fprintf(stderr, "  WARN: Lua '%s' not found\n", lua_name); \
        } else { \
            rc = cfl_lua_bridge_register_main(img, cfl_name, L); \
            if (rc < 0) { fprintf(stderr, "  WARN: '%s' not in image\n", cfl_name); missing++; } \
        } \
    } while(0)

    #define REG_BOOL(cfl_name, lua_name) do { \
        lua_getglobal(L, lua_name); \
        if (!lua_isfunction(L, -1)) { lua_pop(L, 1); missing++; \
            fprintf(stderr, "  WARN: Lua '%s' not found\n", lua_name); \
        } else { \
            rc = cfl_lua_bridge_register_boolean(img, cfl_name, L); \
            if (rc < 0) { fprintf(stderr, "  WARN: '%s' not in image\n", cfl_name); missing++; } \
        } \
    } while(0)

    /* One-shot (Lua) */
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
    REG_OS("bb_init_fields_one_shot",                  "bb_init_fields_one_shot");
    REG_OS("bb_verify_basic_fields_one_shot",          "bb_verify_basic_fields_one_shot");
    REG_OS("bb_verify_nested_fields_one_shot",         "bb_verify_nested_fields_one_shot");
    REG_OS("bb_verify_const_record_one_shot",          "bb_verify_const_record_one_shot");
    REG_OS("bb_verify_ptr64_field_one_shot",           "bb_verify_ptr64_field_one_shot");

    /* Main (Lua) */
    REG_MAIN("sm_event_filtering_main_main",           "sm_event_filtering_main_main");

    /* Boolean (Lua) */
    REG_BOOL("while_test_boolean",                     "while_test_boolean");
    REG_BOOL("catch_all_exception_boolean",            "catch_all_exception_boolean");
    REG_BOOL("exception_filter_boolean",               "exception_filter_boolean");
    REG_BOOL("user_skip_condition_boolean",            "user_skip_condition_boolean");

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
    lua_State *L = luaL_newstate();
    if (!L) { fprintf(stderr, "Failed to create Lua state\n"); return -1; }
    luaL_openlibs(L);
    cfl_lua_bridge_init(L);

    if (luaL_dofile(L, "user_functions.lua") != LUA_OK) {
        fprintf(stderr, "Error loading user_functions.lua: %s\n", lua_tostring(L, -1));
        lua_close(L); return -1;
    }
    printf("Loaded user_functions.lua\n");

    cfl_image_loader_t img;
    int rc = cfl_embedded_load(chaintree_handle_image, CHAINTREE_HANDLE_IMAGE_SIZE, &img);
    if (rc != CFL_IMAGE_OK) {
        fprintf(stderr, "Error: failed to load embedded image (error %d)\n", rc);
        lua_close(L); return -1;
    }
    printf("Loaded embedded image (%u bytes)\n", CHAINTREE_HANDLE_IMAGE_SIZE);

    /* Register: builtins (C) → avro/streaming/drone (C) → rest (Lua) */
    cfl_register_all_functions(&img);
    register_c_avro_functions(&img);
    int lua_missing = register_lua_user_functions(&img, L);
    printf("Lua user functions registered (missing: %d)\n", lua_missing);

    int missing = cfl_image_validate(&img);
    if (missing > 0) {
        fprintf(stderr, "Warning: %d function(s) not registered\n", missing);
        cfl_image_free(&img); lua_close(L); return -1;
    }
    printf("All functions registered (missing: %d)\n", missing);

    const cfl_chaintree_handle_t *test_handle = cfl_image_get_handle(&img);
    if (!test_handle) {
        fprintf(stderr, "Error: test_handle is NULL\n");
        cfl_image_free(&img); lua_close(L); return -1;
    }
    printf("unique_id: %s\n", test_handle->unique_id);
    printf("node_count: %d\n", test_handle->node_count);
    printf("kb_count: %d\n", test_handle->kb_count);

    cfl_runtime_create_params_t *params = cfl_runtime_create_params_create();
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
        cfl_image_free(&img); lua_close(L); return -1;
    }
    printf("Runtime handle created\n");

    cfl_lua_bridge_attach(handle, L);
    cfl_runtime_reset(handle);

    if (argc > 1) {
        int test_index = atoi(argv[1]);
        printf("Running test index (from command line): %d\n", test_index);
        cfl_add_test_by_index(handle, test_index);
    } else {
        cfl_add_test_by_index(handle, 3);
    }

    printf("heap used bytes: %d\n", cfl_heap_used_bytes(handle->heap));
    printf("heap free bytes: %d\n", cfl_heap_free_bytes(handle->heap));

    bool result = cfl_runtime_run(handle);
    printf("Runtime run result: %d\n", result);

    cfl_image_free(&img);
    lua_close(L);
    return result ? 0 : -1;
}
