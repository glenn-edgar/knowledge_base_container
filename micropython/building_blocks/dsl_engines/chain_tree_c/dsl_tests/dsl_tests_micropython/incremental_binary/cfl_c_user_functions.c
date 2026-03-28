/*
 * cfl_c_user_functions.c — Registration wrapper for C user functions
 *
 * Exposes cfl.register_c_user_functions() to Python so that
 * avro/streaming/drone functions stay in C while non-avro
 * user functions are implemented in Python via the bridge.
 *
 * Mirrors the lua5.3 hybrid pattern:
 *   C: avro, streaming, drone control
 *   Python: valve, sequence, exception, blackboard, etc.
 */

#include "py/runtime.h"
#include "py/obj.h"
#include "cfl_image_loader.h"

#include <stdio.h>
#include <stdbool.h>

/* ========================================================================
 * C user function externs (from .c files compiled into this binary)
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

/* ========================================================================
 * Registration — called from Python after cfl.load_embedded_image()
 * ======================================================================== */

void cfl_register_c_user_functions(cfl_image_loader_t *img)
{
    int rc;

    #define REG_OS(n, f) do { rc = cfl_image_register_one_shot(img, n, f); \
        if (rc < 0) fprintf(stderr, "  WARN: C one_shot '%s' not in image\n", n); } while(0)
    #define REG_MAIN(n, f) do { rc = cfl_image_register_main(img, n, f); \
        if (rc < 0) fprintf(stderr, "  WARN: C main '%s' not in image\n", n); } while(0)
    #define REG_BOOL(n, f) do { rc = cfl_image_register_boolean(img, n, f); \
        if (rc < 0) fprintf(stderr, "  WARN: C boolean '%s' not in image\n", n); } while(0)

    /* Avro */
    REG_OS("generate_avro_packet_one_shot",          generate_avro_packet_one_shot);
    REG_OS("generate_const_avro_packet_one_shot",    generate_const_avro_packet_one_shot);
    REG_OS("avro_verify_packet_init_one_shot",       avro_verify_packet_init_one_shot_fn);
    REG_OS("avro_verify_const_packet_init_one_shot", avro_verify_const_packet_init_one_shot_fn);
    REG_MAIN("avro_verify_packet_main",              avro_verify_packet_main_fn);
    REG_MAIN("avro_verify_const_packet_main",        avro_verify_const_packet_main_fn);

    /* Streaming */
    REG_OS("packet_generator_one_shot",              packet_generator_one_shot_fn);
    REG_BOOL("packet_filter_boolean",                packet_filter_boolean_fn);
    REG_BOOL("packet_sink_a_boolean",                packet_sink_a_boolean_fn);
    REG_BOOL("packet_sink_b_boolean",                packet_sink_b_boolean_fn);
    REG_BOOL("packet_tap_boolean",                   packet_tap_boolean_fn);
    REG_BOOL("packet_transform_boolean",             packet_transform_boolean_fn);
    REG_BOOL("packet_collector_boolean",             packet_collector_boolean_fn);
    REG_BOOL("packet_collector_sink_boolean",        packet_collector_sink_boolean_fn);
    REG_BOOL("packet_verify_x_range_boolean",        packet_verify_x_range_boolean_fn);
    REG_BOOL("packet_verified_sink_boolean",         packet_verified_sink_boolean_fn);

    /* Drone control */
    REG_BOOL("on_fly_arc_complete_boolean",          on_fly_arc_complete_boolean_fn);
    REG_BOOL("on_fly_down_complete_boolean",         on_fly_down_complete_boolean_fn);
    REG_BOOL("on_fly_straight_complete_boolean",     on_fly_straight_complete_boolean_fn);
    REG_BOOL("on_fly_up_complete_boolean",           on_fly_up_complete_boolean_fn);
    REG_BOOL("fly_arc_monitor_boolean",              fly_arc_monitor_boolean_fn);
    REG_BOOL("fly_down_monitor_boolean",             fly_down_monitor_boolean_fn);
    REG_BOOL("fly_straight_monitor_boolean",         fly_straight_monitor_boolean_fn);
    REG_BOOL("fly_up_monitor_boolean",               fly_up_monitor_boolean_fn);
    REG_OS("update_fly_arc_final_one_shot",          update_fly_arc_final_one_shot_fn);
    REG_OS("update_fly_down_final_one_shot",         update_fly_down_final_one_shot_fn);
    REG_OS("update_fly_straight_final_one_shot",     update_fly_straight_final_one_shot_fn);
    REG_OS("update_fly_up_final_one_shot",           update_fly_up_final_one_shot_fn);
    REG_BOOL("drone_control_exception_catch_boolean", drone_control_exception_catch_boolean_fn);

    #undef REG_OS
    #undef REG_MAIN
    #undef REG_BOOL
}
