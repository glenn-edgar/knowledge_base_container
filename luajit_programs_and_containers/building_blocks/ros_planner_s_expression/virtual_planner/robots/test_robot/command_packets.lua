--[[
    command_packets.lua -- AVRC command packet definitions for test_robot.

    Defines FFI structs for each command type sent hub → remote.
    One-shot functions decode current_test_json, fill the appropriate
    struct, and write a pointer to the blackboard command_packet slot.

    The transport layer sends this binary packet to the remote.
    On a real robot, these map directly to C structs on the wire.
]]

local ffi = require("ffi")

ffi.cdef[[
    /* Common header for all command packets */
    typedef struct {
        uint32_t packet_type;     /* command type hash */
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

    /* path_spline: follow spline segment */
    typedef struct {
        cmd_header_t header;
        float        from_x, from_y;
        float        to_x, to_y;
        float        speed;
        float        distance;
        uint16_t     segment_index;
        uint16_t     total_segments;
    } cmd_path_segment_t;

    /* path_rotate: turn in place */
    typedef struct {
        cmd_header_t header;
        float        from_heading;
        float        to_heading;
    } cmd_rotate_t;

    /* arm_command: arm operation at station */
    typedef struct {
        cmd_header_t header;
        float        arm_target;
        float        arm_speed;
        float        arm_return;
        uint8_t      payload_type;   /* 0=none, 1=part, 2=container */
    } cmd_arm_t;

    /* rpc_request: gate open/close, sensor read, etc. */
    typedef struct {
        cmd_header_t header;
        uint32_t     rpc_hash;       /* hash of RPC function name */
        float        param1;
        float        param2;
    } cmd_rpc_request_t;

    /* sensor_command: read sensor */
    typedef struct {
        cmd_header_t header;
        uint8_t      sensor_port;
        uint8_t      sensor_type;    /* 0=color, 1=distance, 2=force */
    } cmd_sensor_read_t;

    /* idle: park robot */
    typedef struct {
        cmd_header_t header;
    } cmd_idle_t;
]]

local M = {}

-- Packet type hashes (FNV-1a of type name, matches avro_dsl convention)
M.PACKET_INIT_CHECK    = 0x01
M.PACKET_PATH_SEGMENT  = 0x02
M.PACKET_ROTATE        = 0x03
M.PACKET_ARM           = 0x04
M.PACKET_RPC_REQUEST   = 0x05
M.PACKET_SENSOR_READ   = 0x06
M.PACKET_IDLE          = 0x07

-- Sequence counter
local seq = 0
local function next_seq()
    seq = seq + 1
    return seq
end

-- Payload type lookup
local payload_types = { none = 0, part = 1, container = 2 }

-- Sensor type lookup
local sensor_types = { color = 0, distance = 1, force = 2 }

---------------------------------------------------------------------------
-- Packet generators: JSON action table → FFI struct pointer
---------------------------------------------------------------------------

function M.generate_init_check(action)
    local pkt = ffi.new("cmd_init_check_t")
    pkt.header.packet_type = M.PACKET_INIT_CHECK
    pkt.header.seq = next_seq()
    pkt.header.test_id = action.test_id or 1
    pkt.check_battery = 1
    pkt.check_motors  = 1
    pkt.check_sensors = 1
    pkt.check_comms   = 1
    return pkt
end

function M.generate_path_segment(action, seg_index, segment)
    local pkt = ffi.new("cmd_path_segment_t")
    pkt.header.packet_type = M.PACKET_PATH_SEGMENT
    pkt.header.seq = next_seq()
    pkt.header.test_id = action.test_id or 2
    pkt.from_x   = segment.from and segment.from.x or 0
    pkt.from_y   = segment.from and segment.from.y or 0
    pkt.to_x     = segment.to and segment.to.x or 0
    pkt.to_y     = segment.to and segment.to.y or 0
    pkt.speed    = segment.speed or action.speed or 100
    pkt.distance = segment.distance or 0
    pkt.segment_index  = seg_index - 1
    pkt.total_segments = action.segments and #action.segments or 1
    return pkt
end

function M.generate_rotate(action)
    local pkt = ffi.new("cmd_rotate_t")
    pkt.header.packet_type = M.PACKET_ROTATE
    pkt.header.seq = next_seq()
    pkt.header.test_id = action.test_id or 5
    pkt.from_heading = action.from_heading or 0
    pkt.to_heading   = action.to_heading or 0
    return pkt
end

function M.generate_arm(action)
    local pkt = ffi.new("cmd_arm_t")
    pkt.header.packet_type = M.PACKET_ARM
    pkt.header.seq = next_seq()
    pkt.header.test_id = action.test_id or 6
    local params = action.params or action
    pkt.arm_target = params.arm_target or 0
    pkt.arm_speed  = params.arm_speed or 80
    pkt.arm_return = params.arm_return or 0
    pkt.payload_type = payload_types[params.payload or "none"] or 0
    return pkt
end

function M.generate_rpc_request(action, rpc_name)
    local pkt = ffi.new("cmd_rpc_request_t")
    pkt.header.packet_type = M.PACKET_RPC_REQUEST
    pkt.header.seq = next_seq()
    pkt.header.test_id = action.test_id or 9
    -- Simple hash of rpc name
    local hash = 0
    for i = 1, #rpc_name do hash = hash * 31 + rpc_name:byte(i) end
    pkt.rpc_hash = hash
    local params = action.params or action
    pkt.param1 = params.drive_through or 0
    pkt.param2 = 0
    return pkt
end

function M.generate_sensor_read(action)
    local pkt = ffi.new("cmd_sensor_read_t")
    pkt.header.packet_type = M.PACKET_SENSOR_READ
    pkt.header.seq = next_seq()
    pkt.header.test_id = action.test_id or 10
    local params = action.params or action
    pkt.sensor_type = sensor_types[params.sensor_type or "color"] or 0
    return pkt
end

function M.generate_idle(action)
    local pkt = ffi.new("cmd_idle_t")
    pkt.header.packet_type = M.PACKET_IDLE
    pkt.header.seq = next_seq()
    pkt.header.test_id = action.test_id or 11
    return pkt
end

return M
