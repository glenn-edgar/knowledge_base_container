-- drive_base_ffi.lua
-- Lua-side mirror of libcomm/drive_base_robot.h. Provides catalogue
-- codes, packed-payload builders, and event decoders that the master
-- HAL (dongle_hal.lua) uses to drive a robot_sim-hosted drive_base
-- via libcomm's comm_submit / comm_poll. Hand-written; bump in
-- lockstep with the C header.
--
-- Usage:
--   local db = require("drive_base_ffi")
--   local payload, len = db.build_push_line(0, 0, 1, 0, 0, 0, 0.5)
--   comm_ffi.C.comm_submit(mcu, db.CMD.PUSH_LINE, payload, len, err_ref)

local ffi = require("ffi")

local M = {}

-- ============ CATALOGUE CODES ============

M.CMD = {
    PUSH_LINE        = 0x1001,
    PUSH_SPLINE      = 0x1002,
    PUSH_ROTATE      = 0x1003,
    STOP             = 0x1010,
    RESUME           = 0x1011,
    ABORT            = 0x1012,
    TELEMETRY_ON     = 0x1020,
    TELEMETRY_OFF    = 0x1021,
    GET_TELEMETRY    = 0x1031,    -- request-response: master poll (L5)
    GET_TOOL_STATUS  = 0x1032,    -- L6: poll tool slot (resp 28 B)
    GET_STATION      = 0x1033,    -- L6: poll station_at_pose (resp 4 B)
    BEGIN_GRIP       = 0x1040,    -- L6: payload u8 slot
    BEGIN_RELEASE    = 0x1041,    -- L6: payload u8 slot
    BEGIN_DOCK       = 0x1042,    -- L6: payload u8 slot
    BEGIN_CHARGE     = 0x1043,    -- L6: payload u8 slot, f32 target_j
    TOOL_MOVE        = 0x1044,    -- L6: payload u8 slot, f32 target, f32 speed
}

-- Tool flag bits (mirror physics_pipe.h TOOL_F_*).
M.TOOL_F = {
    AT_TARGET = 0x01,
    GRASPED   = 0x02,
    RELEASED  = 0x04,
    DOCKED    = 0x08,
    CHARGING  = 0x10,
    FAULT     = 0x80,
}

-- TOOL_KIND / STATION_KIND mirror physics_ffi values. The IDs are
-- enumerated in physics_core.c; we use the same numeric encoding.
M.TOOL_KIND = {
    arm = 1, gripper = 2, charge_port = 3,
}
M.STATION_KIND = {
    none = 0, charger = 1, load_dock = 2,
    paint_fixture = 3, assembly_fixture = 4,
}

M.EVT = {
    TELEMETRY      = 0x1080,
    SEG_DONE       = 0x1081,
    FAULT          = 0x1082,
}

-- ============ PATH FLAGS ============
-- Mirrors libphysics's PATH_F so master-side code can interpret
-- DRV_EVT_TELEMETRY.flags without re-importing physics_ffi.

M.PATH_F = {
    TRACKING    = 0x01,
    STOPPED     = 0x02,
    QUEUE_EMPTY = 0x04,
    FAULT       = 0x08,
    SEG_DONE    = 0x10,
}

-- ============ PAYLOAD BUFFERS ============
-- comm_submit takes (uint8_t *payload, uint16_t payload_len). Each
-- builder returns a uint8_t buffer (FFI ctype) plus the byte length.
-- Buffer is freshly allocated per call; caller passes it through to
-- comm_submit and discards.

local function alloc(n)
    return ffi.new("uint8_t[?]", n)
end

local function put_f32(buf, off, f)
    -- LuaJIT FFI: cast a uint8_t* to float*, write, no endianness
    -- issue on little-endian targets (x86_64, aarch64). The C side
    -- reads back via memcpy + bit-shift so the byte order in the
    -- buffer must be little-endian — which is what direct float
    -- writes produce on x86/ARM.
    local fp = ffi.cast("float*", buf + off)
    fp[0] = f
end

local function put_le_u32(buf, off, v)
    buf[off    ] = bit.band(v, 0xFF)
    buf[off + 1] = bit.band(bit.rshift(v,  8), 0xFF)
    buf[off + 2] = bit.band(bit.rshift(v, 16), 0xFF)
    buf[off + 3] = bit.band(bit.rshift(v, 24), 0xFF)
end

local function put_le_u16(buf, off, v)
    buf[off    ] = bit.band(v, 0xFF)
    buf[off + 1] = bit.band(bit.rshift(v, 8), 0xFF)
end

local function get_f32(buf, off)
    local fp = ffi.cast("const float*", buf + off)
    return fp[0]
end

local function get_le_u32(buf, off)
    return            buf[off]
         + bit.lshift(buf[off + 1],  8)
         + bit.lshift(buf[off + 2], 16)
         + bit.lshift(buf[off + 3], 24)
end

local function get_le_u16(buf, off)
    return buf[off] + bit.lshift(buf[off + 1], 8)
end

-- 28-byte payload: fx fy tx ty h_from h_to speed.
function M.build_push_line(fx, fy, tx, ty, h_from, h_to, speed)
    local b = alloc(28)
    put_f32(b,  0, fx)
    put_f32(b,  4, fy)
    put_f32(b,  8, tx)
    put_f32(b, 12, ty)
    put_f32(b, 16, h_from)
    put_f32(b, 20, h_to)
    put_f32(b, 24, speed)
    return b, 28
end

function M.build_push_spline(fx, fy, tx, ty, h_from, h_to, speed)
    return M.build_push_line(fx, fy, tx, ty, h_from, h_to, speed)
end

-- 12-byte payload: from_h to_h rate.
function M.build_push_rotate(from_h, to_h, rate)
    local b = alloc(12)
    put_f32(b, 0, from_h)
    put_f32(b, 4, to_h)
    put_f32(b, 8, rate)
    return b, 12
end

-- Empty payload.
function M.build_simple()
    return nil, 0
end

-- L6 tool builders. Slot is u8; floats are LE f32. Buffer caller-discardable.

function M.build_slot_only(slot)
    local b = alloc(1)
    b[0] = bit.band(slot, 0xFF)
    return b, 1
end

function M.build_begin_charge(slot, target_j)
    local b = alloc(5)
    b[0] = bit.band(slot, 0xFF)
    put_f32(b, 1, target_j)
    return b, 5
end

function M.build_tool_move(slot, target, speed)
    local b = alloc(9)
    b[0] = bit.band(slot, 0xFF)
    put_f32(b, 1, target)
    put_f32(b, 5, speed)
    return b, 9
end

function M.build_kind_only(kind)
    local b = alloc(1)
    b[0] = bit.band(kind, 0xFF)
    return b, 1
end

-- ============ EVENT DECODERS ============
-- Take a comm_event_t (from comm_poll) and decode the payload. cmd
-- is matched against M.EVT.*. Return nil on mismatch.

-- comm_event_t.payload[] is uint8_t[128]; we read it as a raw byte
-- buffer.

function M.decode_telemetry(event)
    if event.cmd ~= M.EVT.TELEMETRY or event.payload_len ~= 32 then
        return nil
    end
    local p = event.payload
    return {
        x                 = get_f32   (p,  0),
        y                 = get_f32   (p,  4),
        heading           = get_f32   (p,  8),
        v                 = get_f32   (p, 12),
        omega             = get_f32   (p, 16),
        energy_used_total = get_f32   (p, 20),
        active_seg_id     = get_le_u32(p, 24),
        queue_depth       = get_le_u16(p, 28),
        flags             = get_le_u16(p, 30),
    }
end

function M.decode_seg_done(event)
    if event.cmd ~= M.EVT.SEG_DONE or event.payload_len ~= 8 then
        return nil
    end
    local p = event.payload
    return {
        seg_id              = get_le_u32(p, 0),  -- master_seq, 0..255 (zero-extended)
        energy_at_complete  = get_f32   (p, 4),
    }
end

-- L6 GET_TOOL_STATUS response decoder. 28-byte payload mirrors
-- phys_tool_status_t with doubles downcast to floats. Layout matches
-- libcomm/drive_base_robot.c GET_TOOL_STATUS handler.
function M.decode_tool_status(event)
    if event.cmd ~= M.CMD.GET_TOOL_STATUS or event.payload_len ~= 28 then
        return nil
    end
    local p = event.payload
    return {
        flags              = get_le_u32(p,  0),
        kind               = get_le_u32(p,  4),
        value              = get_f32   (p,  8),
        target             = get_f32   (p, 12),
        payload_mass       = get_f32   (p, 16),
        battery_j          = get_f32   (p, 20),
        battery_capacity_j = get_f32   (p, 24),
    }
end

-- L6 GET_STATION response decoder. 4-byte payload = i32 station index
-- (-1 = none).
function M.decode_station(event)
    if event.cmd ~= M.CMD.GET_STATION or event.payload_len ~= 4 then
        return nil
    end
    -- get_le_u32 returns unsigned; sign-extend manually since the wire
    -- value is i32 (and -1 is the "none" sentinel).
    local u = get_le_u32(event.payload, 0)
    if u >= 0x80000000 then u = u - 0x100000000 end
    return u
end

return M
