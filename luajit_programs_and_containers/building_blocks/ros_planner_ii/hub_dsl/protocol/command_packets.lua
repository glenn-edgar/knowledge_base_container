--[[
    command_packets.lua -- AVRC command packet definitions (hub → remote).

    FFI struct definitions for every virtual node command type.
    These are the wire format — the FFI struct IS the packet.
    Both hub and remote include this file to speak the same protocol.
]]

local ffi = require("ffi")

ffi.cdef[[
    /* Common header for all command packets */
    typedef struct {
        uint32_t packet_type;     /* command type ID */
        uint32_t seq;             /* sequence number */
        uint16_t test_id;         /* which KB issued this */
        uint16_t flags;           /* reserved */
    } cmd_header_t;

    /* init_check: run self-test, report bitmap */
    typedef struct {
        cmd_header_t header;
        uint8_t      check_battery;
        uint8_t      check_motors;
        uint8_t      check_sensors;
        uint8_t      check_comms;
    } cmd_init_check_t;

    /* path_segment: follow one segment (spline, line, or wall) */
    typedef struct {
        cmd_header_t header;
        float        from_x, from_y;
        float        to_x, to_y;
        float        speed;
        float        distance;
        uint16_t     segment_index;
        uint16_t     total_segments;
        uint8_t      nav_method;    /* 0=spline, 1=line, 2=wall */
    } cmd_path_segment_t;

    /* rotate: turn in place */
    typedef struct {
        cmd_header_t header;
        float        from_heading;
        float        to_heading;
    } cmd_rotate_t;

    /* arm: arm operation at station */
    typedef struct {
        cmd_header_t header;
        float        arm_target;
        float        arm_speed;
        float        arm_return;
        uint8_t      payload_type;   /* 0=none, 1=part, 2=container */
    } cmd_arm_t;

    /* rpc_request: gate, actuator, etc. */
    typedef struct {
        cmd_header_t header;
        uint32_t     rpc_hash;
        float        param1;
        float        param2;
    } cmd_rpc_request_t;

    /* sensor_read: read a sensor */
    typedef struct {
        cmd_header_t header;
        uint8_t      sensor_port;
        uint8_t      sensor_type;    /* 0=color, 1=distance, 2=force */
    } cmd_sensor_read_t;

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

-- Packet type IDs
M.TYPE_INIT_CHECK    = 1
M.TYPE_PATH_SEGMENT  = 2
M.TYPE_ROTATE        = 3
M.TYPE_ARM           = 4
M.TYPE_RPC_REQUEST   = 5
M.TYPE_SENSOR_READ   = 6
M.TYPE_IDLE          = 7
M.TYPE_SHUTDOWN      = 255

-- Nav method codes
M.NAV_SPLINE = 0
M.NAV_LINE   = 1
M.NAV_WALL   = 2

-- Payload types
M.PAYLOAD_NONE      = 0
M.PAYLOAD_PART      = 1
M.PAYLOAD_CONTAINER = 2

-- Sensor types
M.SENSOR_COLOR    = 0
M.SENSOR_DISTANCE = 1
M.SENSOR_FORCE    = 2

-- Sequence counter
local seq = 0
local function next_seq()
    seq = seq + 1
    return seq
end

---------------------------------------------------------------------------
-- Packet generators
---------------------------------------------------------------------------

function M.make_init_check(test_id)
    local pkt = ffi.new("cmd_init_check_t")
    pkt.header.packet_type = M.TYPE_INIT_CHECK
    pkt.header.seq = next_seq()
    pkt.header.test_id = test_id or 1
    pkt.check_battery = 1
    pkt.check_motors  = 1
    pkt.check_sensors = 1
    pkt.check_comms   = 1
    return pkt, ffi.sizeof(pkt)
end

function M.make_path_segment(test_id, from_x, from_y, to_x, to_y,
                             speed, distance, seg_idx, total_segs, nav_method)
    local pkt = ffi.new("cmd_path_segment_t")
    pkt.header.packet_type = M.TYPE_PATH_SEGMENT
    pkt.header.seq = next_seq()
    pkt.header.test_id = test_id or 2
    pkt.from_x = from_x or 0
    pkt.from_y = from_y or 0
    pkt.to_x   = to_x or 0
    pkt.to_y   = to_y or 0
    pkt.speed    = speed or 100
    pkt.distance = distance or 0
    pkt.segment_index  = seg_idx or 0
    pkt.total_segments = total_segs or 1
    pkt.nav_method     = nav_method or M.NAV_SPLINE
    return pkt, ffi.sizeof(pkt)
end

function M.make_rotate(test_id, from_heading, to_heading)
    local pkt = ffi.new("cmd_rotate_t")
    pkt.header.packet_type = M.TYPE_ROTATE
    pkt.header.seq = next_seq()
    pkt.header.test_id = test_id or 5
    pkt.from_heading = from_heading or 0
    pkt.to_heading   = to_heading or 0
    return pkt, ffi.sizeof(pkt)
end

function M.make_arm(test_id, arm_target, arm_speed, arm_return, payload_type)
    local pkt = ffi.new("cmd_arm_t")
    pkt.header.packet_type = M.TYPE_ARM
    pkt.header.seq = next_seq()
    pkt.header.test_id = test_id or 6
    pkt.arm_target   = arm_target or 0
    pkt.arm_speed    = arm_speed or 80
    pkt.arm_return   = arm_return or 0
    pkt.payload_type = payload_type or M.PAYLOAD_NONE
    return pkt, ffi.sizeof(pkt)
end

function M.make_rpc_request(test_id, rpc_hash, param1, param2)
    local pkt = ffi.new("cmd_rpc_request_t")
    pkt.header.packet_type = M.TYPE_RPC_REQUEST
    pkt.header.seq = next_seq()
    pkt.header.test_id = test_id or 9
    pkt.rpc_hash = rpc_hash or 0
    pkt.param1   = param1 or 0
    pkt.param2   = param2 or 0
    return pkt, ffi.sizeof(pkt)
end

function M.make_sensor_read(test_id, sensor_port, sensor_type)
    local pkt = ffi.new("cmd_sensor_read_t")
    pkt.header.packet_type = M.TYPE_SENSOR_READ
    pkt.header.seq = next_seq()
    pkt.header.test_id = test_id or 10
    pkt.sensor_port = sensor_port or 0
    pkt.sensor_type = sensor_type or M.SENSOR_COLOR
    return pkt, ffi.sizeof(pkt)
end

function M.make_idle(test_id)
    local pkt = ffi.new("cmd_idle_t")
    pkt.header.packet_type = M.TYPE_IDLE
    pkt.header.seq = next_seq()
    pkt.header.test_id = test_id or 11
    return pkt, ffi.sizeof(pkt)
end

function M.make_shutdown()
    local pkt = ffi.new("cmd_shutdown_t")
    pkt.header.packet_type = M.TYPE_SHUTDOWN
    pkt.header.seq = next_seq()
    return pkt, ffi.sizeof(pkt)
end

---------------------------------------------------------------------------
-- Packet reading: extract header from raw bytes
---------------------------------------------------------------------------

function M.read_header(data, len)
    if len < ffi.sizeof("cmd_header_t") then return nil end
    local hdr = ffi.cast("cmd_header_t*", data)
    return {
        packet_type = hdr.packet_type,
        seq         = hdr.seq,
        test_id     = hdr.test_id,
        flags       = hdr.flags,
    }
end

-- Type name lookup
M.type_names = {
    [M.TYPE_INIT_CHECK]   = "init_check",
    [M.TYPE_PATH_SEGMENT] = "path_segment",
    [M.TYPE_ROTATE]       = "rotate",
    [M.TYPE_ARM]          = "arm",
    [M.TYPE_RPC_REQUEST]  = "rpc_request",
    [M.TYPE_SENSOR_READ]  = "sensor_read",
    [M.TYPE_IDLE]         = "idle",
    [M.TYPE_SHUTDOWN]     = "shutdown",
}

return M
