--[[
    stream_packets.lua -- Stream packet definitions (remote → hub).

    FFI struct definitions for streaming data from robot back to hub.
    Bitmap state, odometry, sensor readings.
]]

local ffi = require("ffi")

ffi.cdef[[
    /* Stream header */
    typedef struct {
        uint32_t packet_type;
        uint32_t seq;
        uint16_t test_id;
        uint16_t flags;
    } stream_header_t;

    /* Bitmap state update */
    typedef struct {
        stream_header_t header;
        uint8_t  seg_complete;
        uint8_t  action_complete;
        uint8_t  obstacle;
        uint8_t  motor_fault;
        uint8_t  action_fault;
        uint8_t  _pad[3];
    } stream_bitmap_t;

    /* Odometry update */
    typedef struct {
        stream_header_t header;
        float    distance_x;
        float    distance_y;
        float    heading;
        float    speed;
    } stream_odometry_t;

    /* Sensor reading */
    typedef struct {
        stream_header_t header;
        uint8_t  sensor_port;
        uint8_t  sensor_type;
        uint16_t _pad;
        float    value;
    } stream_sensor_t;

    /* Command ack (loopback/confirmation) */
    typedef struct {
        stream_header_t header;
        uint32_t ack_seq;         /* seq of the command being acked */
        uint8_t  status;          /* 0=ok, 1=error, 2=unsupported */
        uint8_t  _pad[3];
    } stream_ack_t;
]]

local M = {}

-- Stream packet type IDs
M.TYPE_BITMAP   = 101
M.TYPE_ODOMETRY = 102
M.TYPE_SENSOR   = 103
M.TYPE_ACK      = 104

-- Ack status codes
M.ACK_OK          = 0
M.ACK_ERROR       = 1
M.ACK_UNSUPPORTED = 2

-- Sequence counter
local seq = 0
local function next_seq()
    seq = seq + 1
    return seq
end

---------------------------------------------------------------------------
-- Packet generators
---------------------------------------------------------------------------

function M.make_bitmap(test_id, fields)
    local pkt = ffi.new("stream_bitmap_t")
    pkt.header.packet_type = M.TYPE_BITMAP
    pkt.header.seq = next_seq()
    pkt.header.test_id = test_id or 0
    if fields then
        pkt.seg_complete    = fields.seg_complete and 1 or 0
        pkt.action_complete = fields.action_complete and 1 or 0
        pkt.obstacle        = fields.obstacle and 1 or 0
        pkt.motor_fault     = fields.motor_fault and 1 or 0
        pkt.action_fault    = fields.action_fault and 1 or 0
    end
    return pkt, ffi.sizeof(pkt)
end

function M.make_odometry(test_id, dx, dy, heading, speed)
    local pkt = ffi.new("stream_odometry_t")
    pkt.header.packet_type = M.TYPE_ODOMETRY
    pkt.header.seq = next_seq()
    pkt.header.test_id = test_id or 0
    pkt.distance_x = dx or 0
    pkt.distance_y = dy or 0
    pkt.heading    = heading or 0
    pkt.speed      = speed or 0
    return pkt, ffi.sizeof(pkt)
end

function M.make_sensor(test_id, port, sensor_type, value)
    local pkt = ffi.new("stream_sensor_t")
    pkt.header.packet_type = M.TYPE_SENSOR
    pkt.header.seq = next_seq()
    pkt.header.test_id = test_id or 0
    pkt.sensor_port = port or 0
    pkt.sensor_type = sensor_type or 0
    pkt.value       = value or 0
    return pkt, ffi.sizeof(pkt)
end

function M.make_ack(test_id, ack_seq, status)
    local pkt = ffi.new("stream_ack_t")
    pkt.header.packet_type = M.TYPE_ACK
    pkt.header.seq = next_seq()
    pkt.header.test_id = test_id or 0
    pkt.ack_seq = ack_seq or 0
    pkt.status  = status or M.ACK_OK
    return pkt, ffi.sizeof(pkt)
end

---------------------------------------------------------------------------
-- Packet reading
---------------------------------------------------------------------------

function M.read_header(data, len)
    if len < ffi.sizeof("stream_header_t") then return nil end
    local hdr = ffi.cast("stream_header_t*", data)
    return {
        packet_type = hdr.packet_type,
        seq         = hdr.seq,
        test_id     = hdr.test_id,
    }
end

function M.read_ack(data, len)
    if len < ffi.sizeof("stream_ack_t") then return nil end
    local pkt = ffi.cast("stream_ack_t*", data)
    return {
        packet_type = pkt.header.packet_type,
        seq         = pkt.header.seq,
        test_id     = pkt.header.test_id,
        ack_seq     = pkt.ack_seq,
        status      = pkt.status,
    }
end

M.type_names = {
    [M.TYPE_BITMAP]   = "bitmap",
    [M.TYPE_ODOMETRY] = "odometry",
    [M.TYPE_SENSOR]   = "sensor",
    [M.TYPE_ACK]      = "ack",
}

return M
