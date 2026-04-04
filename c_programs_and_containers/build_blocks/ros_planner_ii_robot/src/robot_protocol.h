/**
 * @file robot_protocol.h
 * @brief Packet type constants, energy costs, and action durations.
 *
 * Matches the LuaJIT planner protocol exactly so the C robot is
 * a drop-in replacement for the LuaJIT MQTT robot.
 */

#ifndef ROBOT_PROTOCOL_H
#define ROBOT_PROTOCOL_H

/* ================================================================== */
/*  Packet types (hub -> robot RPC commands)                           */
/* ================================================================== */

#define TYPE_INIT_CHECK       1
#define TYPE_PATH_SPLINE      2
#define TYPE_PATH_LINE        3
#define TYPE_PATH_WALL        4
#define TYPE_PATH_ROTATE      5
#define TYPE_DELIVER_PART     6
#define TYPE_PAINT_SAMPLE     7
#define TYPE_LOAD_SHIPPING    8
#define TYPE_PASS_GATE        9
#define TYPE_INSPECTION_SCAN  10
#define TYPE_IDLE             11
#define TYPE_RECHARGE         12
#define TYPE_SHUTDOWN         255

#define TYPE_COUNT            13   /* number of action types (1-12) */

/* ================================================================== */
/*  Energy costs per action type                                       */
/* ================================================================== */

static const int energy_costs[] __attribute__((unused)) = {
    [0]                    = 0,
    [TYPE_INIT_CHECK]      = 50,
    [TYPE_PATH_SPLINE]     = 200,
    [TYPE_PATH_LINE]       = 200,
    [TYPE_PATH_WALL]       = 200,
    [TYPE_PATH_ROTATE]     = 50,
    [TYPE_DELIVER_PART]    = 100,
    [TYPE_PAINT_SAMPLE]    = 100,
    [TYPE_LOAD_SHIPPING]   = 100,
    [TYPE_PASS_GATE]       = 80,
    [TYPE_INSPECTION_SCAN] = 30,
    [TYPE_IDLE]            = 10,
    [TYPE_RECHARGE]        = 0,
};

/* ================================================================== */
/*  Action durations (ticks per command type)                          */
/* ================================================================== */

static const int action_durations[] __attribute__((unused)) = {
    [0]                    = 0,
    [TYPE_INIT_CHECK]      = 15,
    [TYPE_PATH_SPLINE]     = 25,
    [TYPE_PATH_LINE]       = 25,
    [TYPE_PATH_WALL]       = 25,
    [TYPE_PATH_ROTATE]     = 15,
    [TYPE_DELIVER_PART]    = 20,
    [TYPE_PAINT_SAMPLE]    = 20,
    [TYPE_LOAD_SHIPPING]   = 20,
    [TYPE_PASS_GATE]       = 15,
    [TYPE_INSPECTION_SCAN] = 12,
    [TYPE_IDLE]            = 5,
    [TYPE_RECHARGE]        = 30,
};

/* ================================================================== */
/*  Packet type name lookup                                            */
/* ================================================================== */

static const char *packet_type_names[] __attribute__((unused)) = {
    [0]                    = "unknown",
    [TYPE_INIT_CHECK]      = "init_check",
    [TYPE_PATH_SPLINE]     = "path_spline",
    [TYPE_PATH_LINE]       = "path_line",
    [TYPE_PATH_WALL]       = "path_wall",
    [TYPE_PATH_ROTATE]     = "path_rotate",
    [TYPE_DELIVER_PART]    = "deliver_part",
    [TYPE_PAINT_SAMPLE]    = "paint_sample",
    [TYPE_LOAD_SHIPPING]   = "load_shipping",
    [TYPE_PASS_GATE]       = "pass_gate",
    [TYPE_INSPECTION_SCAN] = "inspection_scan",
    [TYPE_IDLE]            = "idle",
    [TYPE_RECHARGE]        = "recharge",
};

#endif /* ROBOT_PROTOCOL_H */
