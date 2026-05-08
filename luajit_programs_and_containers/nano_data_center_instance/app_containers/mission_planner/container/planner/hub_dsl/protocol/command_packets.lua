--[[
    command_packets.lua -- AVRC command packet definitions (hub → remote).

    One packet type per virtual node. Robot-independent.
    The packet describes WHAT to do, not HOW to do it.
    The robot maps the command to its hardware via ROBOT_HW config.

    Two coexisting packet families during the Phase 2-5 transition:

    1. Legacy fixed-size FFI packets (cmd_path_*, cmd_deliver_part, ...).
       FFI cdefs below; one packet type ID per virtual node. Removed in
       Phase 5 once the route_builder rewrite is complete.

    2. New composite packets (cmd_drive_t, cmd_activate_action_t).
       Schema-driven Lua tables, CBOR-encoded on the wire. cmd_drive_t
       collapses spline/line/wall/rotate into a single packet whose
       `segments` list chains sub-segment shapes. cmd_activate_action_t
       carries a virtual-action invocation (action_id + opaque params).

    Header convention (locked Phase 2 C1):
        packet_type   uint32  enum tag (TYPE_*)
        packet_id     uint32  monotonic, robot ACKs by this value
        mission_id    uint32  optional; for log correlation
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

-- Phase 2 composite packet types (CBOR on wire; not FFI structs).
-- Reserved range 30+ so legacy ids 1-20 stay stable during transition.
M.TYPE_DRIVE            = 30   -- cmd_drive_t (chained motion sub-segments)
M.TYPE_ACTIVATE_ACTION  = 31   -- cmd_activate_action_t (Phase 2 C3)

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
    [M.TYPE_DRIVE]           = "drive",
    [M.TYPE_ACTIVATE_ACTION] = "activate_action",
}

-- =========================================================================
-- Phase 2 C1: cmd_drive_t composite packet schema + validators
-- =========================================================================
--
-- Wire shape (CBOR-encoded Lua table):
--   {
--     packet_type   = M.TYPE_DRIVE,
--     packet_id     = uint32,
--     mission_id    = uint32 | nil,
--     start_pos     = { x = float, y = float, heading = float },
--     default_speed = float,
--     stop_at_end   = bool,
--     segments      = { <sub-segment>, <sub-segment>, ... },
--   }
--
-- Sub-segments chain: each starts where the previous one ended (or at
-- start_pos for the first). speed defaults to default_speed if omitted.
-- direction defaults to "forward".
--
-- Sub-segment shapes (C1 ships 3 simple kinds; C2 adds wall_follow +
-- line_follow composites):
--
--   straight_line: { kind = "straight_line",
--                    end_pos = {x,y},
--                    speed = float | nil,
--                    direction = "forward" | "reverse" | nil }
--
--   spline:        { kind = "spline",
--                    end_pos = {x,y},
--                    end_heading = float,
--                    speed = float | nil,
--                    direction = "forward" | "reverse" | nil }
--                  -- Hermite-form on wire: endpoint + tangent angle.
--                  -- Robot reconstructs cubic-Bezier control points
--                  -- via the existing distance/3 rule in physics_core.c.
--
--   rotate:        { kind = "rotate",
--                    end_heading = float,
--                    speed = float | nil }
--                  -- In-place heading change. No translation, no
--                  -- direction field (rotation sense is signed by
--                  -- start->end heading delta).
--
-- Per-PACKET completion (not per-segment): a 5-segment cmd_drive_t
-- produces ONE ACK after all 5 finish. Lock-step is at packet boundary.

M.DIRECTIONS = { forward = true, reverse = true }

-- The set of valid sub-segment kinds. C2 adds wall_follow + line_follow.
M.SUB_SEG_KINDS = {
  straight_line = true,
  spline        = true,
  rotate        = true,
}

-- Per-kind validators. Each returns (true) on success; raises on failure
-- with a path-prefixed error so the caller can pin the bad field.
-- Validators are intentionally STRICT: unknown fields are rejected so a
-- typo in the DSL compiler doesn't slip past as a no-op.

local function err(path, msg)
  error(string.format("%s: %s", path, msg), 0)
end

local function check_number(path, v, name)
  if type(v) ~= "number" then
    err(path, string.format("%s required (number; got %s)", name, type(v)))
  end
end

local function check_pos2(path, p, name)
  if type(p) ~= "table" then
    err(path, string.format("%s required (table {x,y}; got %s)", name, type(p)))
  end
  check_number(path .. "." .. name, p.x, "x")
  check_number(path .. "." .. name, p.y, "y")
  for k, _ in pairs(p) do
    if k ~= "x" and k ~= "y" then
      err(path .. "." .. name, "unknown field " .. tostring(k))
    end
  end
end

local function check_optional_speed(path, v)
  if v == nil then return end
  if type(v) ~= "number" then
    err(path, string.format("speed must be number when present (got %s)", type(v)))
  end
  if v <= 0 then
    err(path, string.format("speed must be > 0 (got %s)", tostring(v)))
  end
end

local function check_optional_direction(path, v)
  if v == nil then return end
  if not M.DIRECTIONS[v] then
    err(path, string.format(
      "direction must be \"forward\" or \"reverse\" (got %q)", tostring(v)))
  end
end

-- Allowed-field whitelist per kind. Any key on the segment NOT in this
-- set is a strict-mode error. `kind` is implicitly allowed.
local SUB_SEG_ALLOWED = {
  straight_line = { end_pos = true, speed = true, direction = true },
  spline        = { end_pos = true, end_heading = true,
                    speed = true, direction = true },
  rotate        = { end_heading = true, speed = true },
}

local function check_unknown_fields(path, seg, kind)
  local allowed = SUB_SEG_ALLOWED[kind]
  for k, _ in pairs(seg) do
    if k ~= "kind" and not allowed[k] then
      err(path, string.format("unknown field %q for kind=%s",
        tostring(k), kind))
    end
  end
end

local function validate_straight_line(path, seg)
  check_pos2(path, seg.end_pos, "end_pos")
  check_optional_speed(path, seg.speed)
  check_optional_direction(path, seg.direction)
  check_unknown_fields(path, seg, "straight_line")
end

local function validate_spline(path, seg)
  check_pos2(path, seg.end_pos, "end_pos")
  check_number(path, seg.end_heading, "end_heading")
  check_optional_speed(path, seg.speed)
  check_optional_direction(path, seg.direction)
  check_unknown_fields(path, seg, "spline")
end

local function validate_rotate(path, seg)
  check_number(path, seg.end_heading, "end_heading")
  check_optional_speed(path, seg.speed)
  check_unknown_fields(path, seg, "rotate")
end

-- Dispatch table -- C2 extends this with composites; C3 leaves it alone.
M._SUB_SEG_VALIDATORS = {
  straight_line = validate_straight_line,
  spline        = validate_spline,
  rotate        = validate_rotate,
}

-- Validate a single sub-segment. Used by validate_drive() and reusable
-- by C2's composite validators (wall_follow / line_follow nest a base
-- straight_line or spline).
function M.validate_sub_segment(path, seg)
  if type(seg) ~= "table" then
    err(path, string.format("segment must be table (got %s)", type(seg)))
  end
  local kind = seg.kind
  if type(kind) ~= "string" or kind == "" then
    err(path, "segment.kind required (non-empty string)")
  end
  if not M.SUB_SEG_KINDS[kind] then
    err(path, string.format("unknown segment kind %q", kind))
  end
  local fn = M._SUB_SEG_VALIDATORS[kind]
  if not fn then
    err(path, string.format("no validator registered for kind=%q", kind))
  end
  fn(path, seg)
end

-- Validate a cmd_drive_t packet end-to-end. Raises on first failure.
-- Returns true on success so callers can write `assert(M.validate_drive(p))`.
function M.validate_drive(packet)
  if type(packet) ~= "table" then
    err("cmd_drive", string.format("packet must be table (got %s)",
      type(packet)))
  end
  if packet.packet_type ~= M.TYPE_DRIVE then
    err("cmd_drive", string.format(
      "packet_type must be %d (TYPE_DRIVE); got %s",
      M.TYPE_DRIVE, tostring(packet.packet_type)))
  end
  check_number("cmd_drive", packet.packet_id, "packet_id")
  if packet.mission_id ~= nil then
    check_number("cmd_drive", packet.mission_id, "mission_id")
  end

  local sp = packet.start_pos
  if type(sp) ~= "table" then
    err("cmd_drive", string.format(
      "start_pos required (table {x,y,heading}; got %s)", type(sp)))
  end
  check_number("cmd_drive.start_pos", sp.x, "x")
  check_number("cmd_drive.start_pos", sp.y, "y")
  check_number("cmd_drive.start_pos", sp.heading, "heading")
  for k, _ in pairs(sp) do
    if k ~= "x" and k ~= "y" and k ~= "heading" then
      err("cmd_drive.start_pos", "unknown field " .. tostring(k))
    end
  end

  check_number("cmd_drive", packet.default_speed, "default_speed")
  if packet.default_speed <= 0 then
    err("cmd_drive", "default_speed must be > 0")
  end

  if type(packet.stop_at_end) ~= "boolean" then
    err("cmd_drive", string.format(
      "stop_at_end required (boolean; got %s)", type(packet.stop_at_end)))
  end

  if type(packet.segments) ~= "table" then
    err("cmd_drive", string.format(
      "segments required (list; got %s)", type(packet.segments)))
  end
  if #packet.segments == 0 then
    err("cmd_drive", "segments must have at least one entry")
  end

  for i, seg in ipairs(packet.segments) do
    M.validate_sub_segment("cmd_drive.segments[" .. i .. "]", seg)
  end

  -- Reject extra top-level fields so DSL typos surface immediately.
  local DRIVE_ALLOWED = {
    packet_type = true, packet_id = true, mission_id = true,
    start_pos = true, default_speed = true, stop_at_end = true,
    segments = true,
  }
  for k, _ in pairs(packet) do
    if not DRIVE_ALLOWED[k] then
      err("cmd_drive", "unknown top-level field " .. tostring(k))
    end
  end

  return true
end

return M
