--[[
    command_packets.lua -- AVRC command packet definitions (hub → remote).

    One packet type per virtual node. Robot-independent.
    The packet describes WHAT to do, not HOW to do it.
    The robot maps the command to its hardware via ROBOT_HW config.
]]

local ffi = require("ffi")

ffi.cdef[[
    /* Common header */
    typedef struct {
        uint32_t packet_type;
        uint32_t seq;
        uint16_t test_id;
        uint16_t flags;
    } cmd_header_t;

    /* init_check: preflight self-test */
    typedef struct {
        cmd_header_t header;
    } cmd_init_check_t;

    /* path_spline: follow spline path between virtual nodes */
    typedef struct {
        cmd_header_t header;
        float        from_x, from_y;
        float        to_x, to_y;
        float        speed;
        float        distance;
        uint16_t     segment_index;
        uint16_t     total_segments;
    } cmd_path_spline_t;

    /* path_line: line follow between virtual nodes */
    typedef struct {
        cmd_header_t header;
        float        from_x, from_y;
        float        to_x, to_y;
        float        speed;
        float        distance;
    } cmd_path_line_t;

    /* path_wall: wall ride between virtual nodes */
    typedef struct {
        cmd_header_t header;
        float        from_x, from_y;
        float        to_x, to_y;
        float        speed;
        float        distance;
        float        wall_standoff;
    } cmd_path_wall_t;

    /* path_rotate: turn in place to heading */
    typedef struct {
        cmd_header_t header;
        float        from_heading;
        float        to_heading;
    } cmd_path_rotate_t;

    /* deliver_part: deliver payload at assembly station */
    typedef struct {
        cmd_header_t header;
        float        arm_target;
        float        arm_speed;
        float        arm_return;
        uint8_t      payload_type;
    } cmd_deliver_part_t;

    /* paint_sample: paint operation at painting station */
    typedef struct {
        cmd_header_t header;
        float        arm_target;
        float        arm_speed;
        float        arm_return;
        float        hold_time;
    } cmd_paint_sample_t;

    /* load_shipping: load container at shipping station */
    typedef struct {
        cmd_header_t header;
        float        arm_target;
        float        arm_speed;
        float        arm_return;
        uint8_t      payload_type;
    } cmd_load_shipping_t;

    /* pass_gate: open gate, drive through, close gate */
    typedef struct {
        cmd_header_t header;
        uint32_t     rpc_open_hash;
        uint32_t     rpc_close_hash;
        float        drive_through;
    } cmd_pass_gate_t;

    /* inspection_scan: read sensor at inspection station */
    typedef struct {
        cmd_header_t header;
        uint8_t      sensor_port;
        uint8_t      sensor_type;
    } cmd_inspection_scan_t;

    /* idle: park robot */
    typedef struct {
        cmd_header_t header;
    } cmd_idle_t;

    /* shutdown: terminate remote */
    typedef struct {
        cmd_header_t header;
    } cmd_shutdown_t;
]]

local M = {}

-- Packet type IDs: one per virtual node
M.TYPE_INIT_CHECK       = 1
M.TYPE_PATH_SPLINE      = 2
M.TYPE_PATH_LINE        = 3
M.TYPE_PATH_WALL        = 4
M.TYPE_PATH_ROTATE      = 5
M.TYPE_DELIVER_PART     = 6
M.TYPE_PAINT_SAMPLE     = 7
M.TYPE_LOAD_SHIPPING    = 8
M.TYPE_PASS_GATE        = 9
M.TYPE_INSPECTION_SCAN  = 10
M.TYPE_IDLE             = 11
M.TYPE_RECHARGE         = 12
M.TYPE_OPERATION        = 20
M.TYPE_SHUTDOWN         = 255

-- Type name lookup
M.type_names = {
    [M.TYPE_INIT_CHECK]      = "init_check",
    [M.TYPE_PATH_SPLINE]     = "path_spline",
    [M.TYPE_PATH_LINE]       = "path_line",
    [M.TYPE_PATH_WALL]       = "path_wall",
    [M.TYPE_PATH_ROTATE]     = "path_rotate",
    [M.TYPE_DELIVER_PART]    = "deliver_part",
    [M.TYPE_PAINT_SAMPLE]    = "paint_sample",
    [M.TYPE_LOAD_SHIPPING]   = "load_shipping",
    [M.TYPE_PASS_GATE]       = "pass_gate",
    [M.TYPE_INSPECTION_SCAN] = "inspection_scan",
    [M.TYPE_IDLE]            = "idle",
    [M.TYPE_RECHARGE]        = "recharge",
    [M.TYPE_OPERATION]       = "operation",
    [M.TYPE_SHUTDOWN]        = "shutdown",
}

return M
