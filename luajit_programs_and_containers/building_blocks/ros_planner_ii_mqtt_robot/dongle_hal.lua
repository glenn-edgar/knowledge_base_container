-- dongle_hal.lua
-- Master-side HAL backed by libcomm + robot_sim. Mirrors the surface
-- of physics_ffi (robot_hal's "sim" mode) so robot_controller and
-- mqtt_robot_main don't change. The substitution is at the HAL
-- boundary: HAL_MODE=dongle (or robot_hal_mod.new{ mode = "dongle" })
-- selects this backend.
--
-- Architecture this connects to (see continue.md, project memory):
--   master process (this Lua):
--     comm_submit(DRV_CMD_PUSH_LINE, payload) → libcomm m2s frame → pty
--   robot_sim process:
--     ext_bus → dongle_manager → internal_bus → drive_base → libphysics
--     drive_base.tick → DRV_EVT_TELEMETRY / SEG_DONE → ext_bus → pty
--   master polls events via comm_poll, caches latest telemetry,
--   matches SEG_DONE wire_seq to push_* return values.
--
-- The HAL surface (paths_only subset for L5; tools deferred):
--   step(dt)                         -- no-op (physics autonomous)
--   sim_time()                       -- wall-clock based
--   read_pose() / read_pose_truth()  -- from cached telemetry
--   read_path_status()               -- from cached telemetry + tracking
--   push_line / push_spline / push_rotate  -- comm_submit + ACK wait
--   request_stop / release_stop / abort_path / is_stopped
--   queue_depth / active_seg_id / last_done_seg_id
--   PATH_F constants                 -- mirrored from drive_base_ffi
--
-- Tool methods raise an error — drive_base catalogue doesn't yet
-- expose them and paths_only e2e doesn't need them.

local ffi      = require("ffi")
local bit      = require("bit")
local comm_ffi = require("comm_ffi")
local ct_comm  = require("ct_comm")
local db       = require("drive_base_ffi")

local C = comm_ffi.C
local R = comm_ffi.RESULT

local M = {}

-- ============ MANIFEST BUILD ============
-- Built at runtime to avoid a separate baked .lua file. One dongle,
-- one bus, one slave per opts. Mirrors test_comm_pty_multi_dongle's
-- pattern but with a single dongle.

ffi.cdef[[
typedef struct __attribute__((packed)) {
    double      timestamp;
    uint32_t    schema_hash;
    uint16_t    seq;
    uint16_t    source_node;
} comm_manifest_wire_header_t;
typedef struct __attribute__((packed)) {
    uint8_t dongle_uuid[16];
    uint8_t bus_count;
    uint8_t bus_local_ids[8];
} manifest_dongle_t;
typedef struct __attribute__((packed)) {
    uint8_t  max_miss;
    uint16_t tick_period_ms;
    uint16_t join_timeout_ms;
} manifest_tunables_t;
typedef struct __attribute__((packed)) {
    uint8_t             bus_id;
    manifest_tunables_t tunables;
} manifest_bus_t;
typedef struct __attribute__((packed)) {
    uint8_t  mcu;
    uint8_t  dongle_idx;
    uint8_t  bus_id;
    uint8_t  addr;
    uint32_t physics_model_id;
} manifest_slave_t;
typedef struct __attribute__((packed)) {
    uint8_t version;
    uint8_t dongle_count;
    uint8_t bus_count;
    uint8_t slave_count;
    manifest_dongle_t dongles[4];
    manifest_bus_t    buses[8];
    manifest_slave_t  slaves[64];
} comm_manifest_v1_wire_t;
typedef struct __attribute__((packed)) {
    comm_manifest_wire_header_t header;
    comm_manifest_v1_wire_t     data;
} comm_manifest_v1_packet_t;
]]

local SCHEMA_HASH = 0x79046205
local PACKET_SIZE = ffi.sizeof("comm_manifest_v1_packet_t")

local function set_uuid_identity(uuid, dongle_type, dongle_instance)
    uuid[0] = bit.band(dongle_type, 0xFF)
    uuid[1] = bit.band(bit.rshift(dongle_type, 8), 0xFF)
    uuid[2] = bit.band(dongle_instance, 0xFF)
    uuid[3] = bit.band(bit.rshift(dongle_instance, 8), 0xFF)
    -- Bytes 4..15 stay zero.
end

local function build_one_dongle_packet(dongle_type, dongle_instance, slave_addr, mcu)
    local pkt = ffi.new("comm_manifest_v1_packet_t")
    pkt.header.schema_hash = SCHEMA_HASH
    pkt.data.version       = 1
    pkt.data.dongle_count  = 1
    pkt.data.bus_count     = 1
    pkt.data.slave_count   = 1
    set_uuid_identity(pkt.data.dongles[0].dongle_uuid, dongle_type, dongle_instance)
    pkt.data.dongles[0].bus_count        = 1
    pkt.data.dongles[0].bus_local_ids[0] = 0
    pkt.data.buses[0].bus_id                   = 0
    pkt.data.buses[0].tunables.max_miss        = 3
    pkt.data.buses[0].tunables.tick_period_ms  = 20
    pkt.data.buses[0].tunables.join_timeout_ms = 500
    pkt.data.slaves[0].mcu              = mcu
    pkt.data.slaves[0].dongle_idx       = 0
    pkt.data.slaves[0].bus_id           = 0
    pkt.data.slaves[0].addr             = slave_addr
    pkt.data.slaves[0].physics_model_id = 0
    return pkt
end

-- ============ HELPERS ============

local function now_ms()
    return ct_comm.now_ms()
end

-- ============ HAL HANDLE ============

local hal_mt = {}
hal_mt.__index = hal_mt

-- Update the cached state from a freshly-arrived event.
function hal_mt:_apply_event(e)
    local cmd = e.cmd
    if cmd == db.EVT.TELEMETRY then
        local t = db.decode_telemetry(e)
        if t then
            self._pose.x         = t.x
            self._pose.y         = t.y
            self._pose.heading   = t.heading
            self._pose.v         = t.v
            self._pose.omega     = t.omega
            self._path.flags             = t.flags
            self._path.active_seg_id     = t.active_seg_id
            self._path.queue_depth       = t.queue_depth
            self._path.energy_used_total = t.energy_used_total
        end
    elseif cmd == db.EVT.SEG_DONE then
        local d = db.decode_seg_done(e)
        if d then
            self._path.last_done_seg_id = d.seg_id
        end
    end
end

-- Drain any pending events so cache stays current. Called from each
-- HAL method that observes state.
--
-- libcomm surfaces each slot at most once per poll; if we call
-- comm_poll without applying the events, they're lost. Every place
-- that drives comm_poll routes through here.
function hal_mt:_drain()
    local events = ct_comm.poll(comm_ffi.HANDLES_MAX)
    if events then
        for i = 1, #events do
            self:_apply_event(events[i])
        end
    end
end

-- Wait for handle h to terminate (ACK / NAK / fault / timeout). Each
-- poll iteration drains telemetry into the cache so a long-running
-- ACK wait doesn't drop telemetry events.
function hal_mt:_poll_for_completion(h, timeout_ms)
    local deadline = now_ms() + (timeout_ms or 200)
    while now_ms() < deadline do
        self:_drain()
        local s = C.comm_status(h)
        if s == R.OK then
            local e, _rc = ct_comm.claim(h)
            return s, e
        end
        if s == R.ERR_NAK or s == R.ERR_TIMEOUT or s == R.ERR_FAULT then
            return s, nil
        end
        ffi.C.usleep(1000)
    end
    return C.comm_status(h), nil
end

-- step() is a no-op on the dongle path: physics runs autonomously
-- inside robot_sim at whatever phys_step cadence drive_base owns.
-- Matters only on the sim path.
function hal_mt:step(_dt)
    self:_drain()
end

function hal_mt:sim_time()
    -- TELEMETRY doesn't carry sim_t today; fall back to wall-clock
    -- elapsed since hal:new.
    return (now_ms() - self._t0_ms) / 1000.0
end

function hal_mt:read_pose()
    self:_drain()
    return {
        x       = self._pose.x,
        y       = self._pose.y,
        heading = self._pose.heading,
        v       = self._pose.v,
        omega   = self._pose.omega,
        sim_t   = self:sim_time(),
    }
end

-- Same as read_pose on the dongle path: drive_base's TELEMETRY is the
-- noisy reading already (gps_sigma_* configured in tunables). We don't
-- have a separate truth pipe over the wire.
function hal_mt:read_pose_truth() return self:read_pose() end

function hal_mt:read_path_status()
    self:_drain()
    return {
        flags             = self._path.flags,
        active_seg_id     = self._path.active_seg_id,
        last_done_seg_id  = self._path.last_done_seg_id,
        queue_depth       = self._path.queue_depth,
        active_progress   = 0.0,    -- not in TELEMETRY today
        cross_track_err   = 0.0,
        heading_err       = 0.0,
        energy_used_total = self._path.energy_used_total,
        v_cmd             = 0.0,
    }
end

function hal_mt:read_tool_status(_slot)
    error("dongle_hal: tools not in drive_base catalogue (paths_only mode)")
end

-- ============ COMMANDS ============
-- Each push_* synchronously waits for ACK_BARE so we can return the
-- wire seq as the seg_id. ~1-3 ms per call over pty; the workload
-- emits paths at human cadence so the latency is invisible.

function hal_mt:_submit_push(cmd, buf, len)
    local h, err = ct_comm.submit(self.mcu, cmd, buf, len)
    if h == 0 then
        error(string.format("dongle_hal: comm_submit failed (cmd=0x%04x err=%d)",
                            cmd, err))
    end
    local s, e = self:_poll_for_completion(h, 200)
    if s ~= R.OK or e == nil then
        error(string.format("dongle_hal: cmd 0x%04x not ACKed (status=%d)",
                            cmd, s))
    end
    -- Wire seq is the seg_id we hand back.
    return tonumber(e.seq)
end

function hal_mt:push_line(fx, fy, tx, ty, h_from, h_to, speed)
    local b, n = db.build_push_line(fx, fy, tx, ty, h_from, h_to, speed)
    return self:_submit_push(db.CMD.PUSH_LINE, b, n)
end

function hal_mt:push_spline(fx, fy, tx, ty, h_from, h_to, speed)
    local b, n = db.build_push_spline(fx, fy, tx, ty, h_from, h_to, speed)
    return self:_submit_push(db.CMD.PUSH_SPLINE, b, n)
end

function hal_mt:push_rotate(from_h, to_h, rate)
    local b, n = db.build_push_rotate(from_h, to_h, rate)
    return self:_submit_push(db.CMD.PUSH_ROTATE, b, n)
end

-- ABORT / STOP / RESUME are fire-and-forget (don't need to return a
-- seg_id). We still wait briefly for ACK so the master sees ordering
-- against subsequent commands.
function hal_mt:_submit_simple(cmd)
    local h, err = ct_comm.submit(self.mcu, cmd, nil, 0)
    if h == 0 then
        error(string.format("dongle_hal: comm_submit simple failed (cmd=0x%04x err=%d)",
                            cmd, err))
    end
    self:_poll_for_completion(h, 200)
end

function hal_mt:request_stop()  self:_submit_simple(db.CMD.STOP)   end
function hal_mt:release_stop()  self:_submit_simple(db.CMD.RESUME) end
function hal_mt:abort_path()    self:_submit_simple(db.CMD.ABORT)
                                self._path.last_done_seg_id = 0   -- match abort semantics
                                end

function hal_mt:is_stopped()
    self:_drain()
    return bit.band(self._path.flags, db.PATH_F.STOPPED) ~= 0
end

function hal_mt:queue_depth()       self:_drain(); return self._path.queue_depth end
function hal_mt:active_seg_id()     self:_drain(); return self._path.active_seg_id end
function hal_mt:last_done_seg_id()  self:_drain(); return self._path.last_done_seg_id end

-- Tools — stubs.
function hal_mt:begin_tool_move(_slot, _target, _speed)
    error("dongle_hal: begin_tool_move not in v1 catalogue")
end
function hal_mt:begin_grip(_slot)    error("dongle_hal: begin_grip not in v1 catalogue") end
function hal_mt:begin_release(_slot) error("dongle_hal: begin_release not in v1 catalogue") end
function hal_mt:begin_dock(_slot)    error("dongle_hal: begin_dock not in v1 catalogue") end
function hal_mt:begin_charge(_slot)  error("dongle_hal: begin_charge not in v1 catalogue") end

-- ============ CONSTANTS ============
-- Mirror what physics_ffi exposes so robot_controller doesn't care
-- which HAL it has. PATH_F is real (used by is_stopped + path-status
-- flag bits). TOOL_F / KIND / STATION_KIND are stubs since tool path
-- isn't in v1.

local PATH_F       = { TRACKING = 0x01, STOPPED = 0x02, QUEUE_EMPTY = 0x04,
                       FAULT = 0x08, SEG_DONE = 0x10 }
local TOOL_F       = {}
local TOOL_KIND    = {}
local STATION_KIND = {}

-- ============ FACTORY ============

function M.new(opts)
    opts = opts or {}
    local pty_path = opts.pty_path or os.getenv("ROBOT_SIM_PTY")
    if not pty_path or pty_path == "" then
        error("dongle_hal: opts.pty_path or ROBOT_SIM_PTY env var required")
    end
    local dongle_type     = opts.dongle_type     or 1   -- DRIVE_BASE
    local dongle_instance = opts.dongle_instance or 1
    local slave_addr      = opts.slave_addr      or 1
    local mcu             = opts.mcu             or 1

    local pkt   = build_one_dongle_packet(dongle_type, dongle_instance,
                                          slave_addr, mcu)
    local specs = ffi.new("comm_dongle_attach_t[1]")
    specs[0].path             = pty_path
    specs[0].dongle_type      = dongle_type
    specs[0].dongle_instance  = dongle_instance

    local rc = C.comm_init_with_dongles(
                    ffi.cast("const uint8_t*", pkt),
                    PACKET_SIZE,
                    specs, 1)
    if rc ~= R.OK then
        error(string.format("dongle_hal: comm_init_with_dongles rc=%d "
                          .."(pty=%s type=%d instance=%d)",
                            rc, pty_path, dongle_type, dongle_instance))
    end

    -- Hold pty_path string alive for the lifetime of comm — the spec
    -- struct holds a raw pointer.
    local hal = setmetatable({
        mode        = "dongle",
        mcu         = mcu,
        slave_addr  = slave_addr,
        _t0_ms      = now_ms(),
        _pty_holder = pty_path,
        _pose       = { x = 0, y = 0, heading = 0, v = 0, omega = 0 },
        _path       = { flags = 0, active_seg_id = 0, last_done_seg_id = 0,
                        queue_depth = 0, energy_used_total = 0 },
        PATH_F       = PATH_F,
        TOOL_F       = TOOL_F,
        TOOL_KIND    = TOOL_KIND,
        STATION_KIND = STATION_KIND,
    }, hal_mt)

    -- Enable telemetry so read_pose / read_path_status see live data.
    hal:_submit_simple(db.CMD.TELEMETRY_ON)

    return hal
end

function M.shutdown()
    C.comm_shutdown()
end

return M
