--[[
    physics_ffi.lua -- LuaJIT FFI wrapper around libphysics.so.

    Loads the C library from the same directory as this script and applies
    physics_config.json + sim_map.json. Returns a handle with a small OO API.
]]

local ffi = require("ffi")

ffi.cdef[[
typedef struct phys_s phys_t;

typedef struct {
    double x, y, heading;
    double v, omega;
    double sim_t;
} phys_pose_t;

typedef struct {
    uint32_t flags;
    uint32_t active_seg_id;
    uint32_t last_done_seg_id;
    int32_t  queue_depth;
    double   active_progress;
    double   cross_track_err;
    double   heading_err;
    double   energy_used_total;
    double   v_cmd;
} phys_path_status_t;

typedef struct {
    uint32_t flags;
    int32_t  kind;
    double   value;
    double   target;
    double   payload_mass;
    double   battery_j;
    double   battery_capacity_j;
} phys_tool_status_t;

phys_t*  phys_create(void);
void     phys_destroy(phys_t*);

void     phys_set_chassis(phys_t*, double, double, double, double, double, double);
void     phys_set_motors(phys_t*, double, double, double, double, double, double);
void     phys_set_follower(phys_t*, double, double, double, double, double, double, double, double);
void     phys_set_sensors(phys_t*, double, double, double, double);
void     phys_set_battery(phys_t*, double, double);
void     phys_set_initial_pose(phys_t*, double, double, double);
void     phys_set_seed(phys_t*, uint64_t);

int      phys_add_tool(phys_t*, int, int, double, double, double, double);
int      phys_add_station(phys_t*, int, double, double, double, double, double, double, double);

double   phys_sim_time(phys_t*);
void     phys_step(phys_t*, double);

uint32_t phys_push_line  (phys_t*, double, double, double, double, double, double, double);
uint32_t phys_push_spline(phys_t*, double, double, double, double, double, double, double);
uint32_t phys_push_rotate(phys_t*, double, double, double);

void     phys_request_stop(phys_t*);
void     phys_release_stop(phys_t*);
int      phys_is_stopped(phys_t*);
void     phys_abort_path(phys_t*);

int      phys_queue_depth(phys_t*);
uint32_t phys_active_seg_id(phys_t*);
uint32_t phys_last_done_seg_id(phys_t*);

void     phys_read_pose         (phys_t*, phys_pose_t*);
void     phys_read_pose_truth   (phys_t*, phys_pose_t*);
void     phys_read_path_status  (phys_t*, phys_path_status_t*);
void     phys_read_tool_status  (phys_t*, int, phys_tool_status_t*);

int      phys_begin_tool_move(phys_t*, int, double, double);
int      phys_begin_grip     (phys_t*, int);
int      phys_begin_release  (phys_t*, int);
int      phys_begin_dock     (phys_t*, int);
int      phys_begin_charge   (phys_t*, int, double);

int      phys_station_at_pose(phys_t*, int);
double   phys_payload_mass(phys_t*);
double   phys_battery_j(phys_t*);
]]

-- Path flags
local PATH_F = {
    TRACKING    = 0x01,
    STOPPED     = 0x02,
    QUEUE_EMPTY = 0x04,
    FAULT       = 0x08,
    SEG_DONE    = 0x10,
}
local TOOL_F = {
    AT_TARGET = 0x01,
    GRASPED   = 0x02,
    RELEASED  = 0x04,
    DOCKED    = 0x08,
    CHARGING  = 0x10,
    FAULT     = 0x80,
}
local TOOL_KIND = {
    revolute_1dof = 1,
    binary        = 2,
    passive_dock  = 3,
}
local STATION_KIND = {
    charger          = 1,
    load_dock        = 2,
    paint_fixture    = 3,
    assembly_fixture = 4,
}

local M = {}
M.PATH_F = PATH_F
M.TOOL_F = TOOL_F
M.TOOL_KIND = TOOL_KIND
M.STATION_KIND = STATION_KIND

-- Library loader -- tries local dir, then global
local function load_lib()
    local info = debug.getinfo(1, "S")
    local src = info.source:match("@(.*)")
    local script_dir = src and src:match("(.*/)") or "./"
    local candidates = {
        script_dir .. "libphysics.so",
        "./libphysics.so",
        "libphysics",
    }
    for _, c in ipairs(candidates) do
        local ok, lib = pcall(ffi.load, c)
        if ok then return lib end
    end
    error("physics_ffi: cannot load libphysics.so (tried " .. table.concat(candidates, ", ") .. ")")
end

local L = load_lib()
M.L = L

-- OO handle
local H = {}
H.__index = H

local function deg2rad(d) return d * math.pi / 180.0 end

function M.new(physics_config, sim_map, opts)
    opts = opts or {}
    local p = L.phys_create()
    if p == nil then error("phys_create failed") end
    ffi.gc(p, L.phys_destroy)

    -- Apply config
    local ch = physics_config.chassis or {}
    local mo = physics_config.motors  or {}
    local fw = physics_config.path_follower or {}
    local sn = physics_config.sensors or {}
    local bt = physics_config.battery or {}
    local ip = physics_config.initial_pose or {x=0,y=0,heading=0}
    local pid = mo.pid or {}

    L.phys_set_chassis(p,
        ch.wheelbase_m    or 0.30,
        ch.wheel_radius_m or 0.04,
        ch.mass_kg        or 8.0,
        ch.inertia_kg_m2  or 0.12,
        ch.lin_friction   or 0.8,
        ch.ang_friction   or 0.4)

    L.phys_set_motors(p,
        mo.max_torque_nm   or 1.5,
        mo.max_wheel_rad_s or 30.0,
        pid.kp or 8.0, pid.ki or 4.0, pid.kd or 0.05,
        mo.energy_k or 1.0)

    L.phys_set_follower(p,
        fw.lookahead_min_m    or 0.20,
        fw.lookahead_k_v      or 0.40,
        fw.arrival_tol_m      or 0.05,
        fw.heading_tol_rad    or 0.05,
        fw.max_linear_accel   or 1.0,
        fw.max_angular_accel  or 3.0,
        fw.inner_dt_s         or 0.005,
        fw.cross_track_abort_m or 5.0)

    local gps = sn.gps or {}
    local imu = sn.imu or {}
    local bat = sn.battery or {}
    L.phys_set_sensors(p,
        gps.pose_sigma_xy or 0.0,
        gps.pose_sigma_h  or 0.0,
        imu.yaw_rate_sigma or 0.0,
        bat.noise_pct or 0.0)

    L.phys_set_battery(p, bt.capacity_j or 100000.0, bt.initial_j or bt.capacity_j or 100000.0)
    L.phys_set_initial_pose(p, ip.x or 0, ip.y or 0, ip.heading or 0)

    if opts.seed then L.phys_set_seed(p, ffi.new("uint64_t", opts.seed)) end

    -- Tools
    local tool_slot_by_name = {}
    for _, t in ipairs(physics_config.tools or {}) do
        local kind = TOOL_KIND[t.kind] or 0
        local lim_min, lim_max = 0.0, 0.0
        local max_speed = 0.0
        if t.limits_deg then
            lim_min = deg2rad(t.limits_deg[1])
            lim_max = deg2rad(t.limits_deg[2])
        end
        if t.max_speed_dps then max_speed = deg2rad(t.max_speed_dps) end
        local trans_s = (t.transition_ms or 400) / 1000.0
        L.phys_add_tool(p, t.slot, kind, lim_min, lim_max, max_speed, trans_s)
        tool_slot_by_name[t.name] = t.slot
    end

    -- Stations
    local station_idx_by_id = {}
    for _, st in ipairs((sim_map and sim_map.stations) or {}) do
        local kind = STATION_KIND[st.kind] or 0
        local dp = st.dock_pose
        local param1, param2 = 0.0, 0.0
        if st.kind == "charger"        then param1 = st.charge_rate_w or 0.0 end
        if st.kind == "load_dock"      then param1 = (st.payload and st.payload.mass_kg) or 0.0
                                            param2 = deg2rad(st.pickup_arm_angle_deg or 0.0) end
        if st.kind == "paint_fixture"  then param1 = deg2rad(st.paint_arm_angle_deg or 0.0) end
        if st.kind == "assembly_fixture" then param1 = deg2rad(st.deliver_arm_angle_deg or 0.0) end
        local idx = L.phys_add_station(p, kind, dp[1], dp[2], dp[3] or 0.0,
                                       st.tolerance_m or 0.25,
                                       deg2rad(st.tolerance_h_deg or 20.0),
                                       param1, param2)
        station_idx_by_id[st.id] = idx
    end

    local self = setmetatable({
        p = p, L = L,
        tool_slot_by_name = tool_slot_by_name,
        station_idx_by_id = station_idx_by_id,
        _pose        = ffi.new("phys_pose_t"),
        _path_status = ffi.new("phys_path_status_t"),
        _tool_status = ffi.new("phys_tool_status_t"),
    }, H)
    return self
end

function H:step(dt_sim) L.phys_step(self.p, dt_sim) end
function H:sim_time()   return tonumber(L.phys_sim_time(self.p)) end

function H:read_pose()
    L.phys_read_pose(self.p, self._pose)
    return {
        x = tonumber(self._pose.x), y = tonumber(self._pose.y),
        heading = tonumber(self._pose.heading),
        v = tonumber(self._pose.v), omega = tonumber(self._pose.omega),
        sim_t = tonumber(self._pose.sim_t),
    }
end

function H:read_pose_truth()
    L.phys_read_pose_truth(self.p, self._pose)
    return {
        x = tonumber(self._pose.x), y = tonumber(self._pose.y),
        heading = tonumber(self._pose.heading),
        v = tonumber(self._pose.v), omega = tonumber(self._pose.omega),
        sim_t = tonumber(self._pose.sim_t),
    }
end

function H:read_path_status()
    L.phys_read_path_status(self.p, self._path_status)
    local s = self._path_status
    return {
        flags             = tonumber(s.flags),
        active_seg_id     = tonumber(s.active_seg_id),
        last_done_seg_id  = tonumber(s.last_done_seg_id),
        queue_depth       = tonumber(s.queue_depth),
        active_progress   = tonumber(s.active_progress),
        cross_track_err   = tonumber(s.cross_track_err),
        heading_err       = tonumber(s.heading_err),
        energy_used_total = tonumber(s.energy_used_total),
        v_cmd             = tonumber(s.v_cmd),
    }
end

function H:read_tool_status(slot)
    L.phys_read_tool_status(self.p, slot, self._tool_status)
    local s = self._tool_status
    return {
        flags              = tonumber(s.flags),
        kind               = tonumber(s.kind),
        value              = tonumber(s.value),
        target             = tonumber(s.target),
        payload_mass       = tonumber(s.payload_mass),
        battery_j          = tonumber(s.battery_j),
        battery_capacity_j = tonumber(s.battery_capacity_j),
    }
end

-- Segment pushes
function H:push_line  (fx, fy, tx, ty, h_from, h_to, speed)
    return tonumber(L.phys_push_line(self.p, fx, fy, tx, ty, h_from, h_to, speed))
end
function H:push_spline(fx, fy, tx, ty, h_from, h_to, speed)
    return tonumber(L.phys_push_spline(self.p, fx, fy, tx, ty, h_from, h_to, speed))
end
function H:push_rotate(from_h, to_h, rate)
    return tonumber(L.phys_push_rotate(self.p, from_h, to_h, rate))
end

function H:request_stop() L.phys_request_stop(self.p) end
function H:release_stop() L.phys_release_stop(self.p) end
function H:is_stopped()   return L.phys_is_stopped(self.p) ~= 0 end
function H:abort_path()   L.phys_abort_path(self.p) end
function H:queue_depth()  return tonumber(L.phys_queue_depth(self.p)) end
function H:active_seg_id()    return tonumber(L.phys_active_seg_id(self.p)) end
function H:last_done_seg_id() return tonumber(L.phys_last_done_seg_id(self.p)) end

-- Tool ops (accept slot number OR tool name)
local function resolve_slot(self, slot_or_name)
    if type(slot_or_name) == "string" then
        return self.tool_slot_by_name[slot_or_name] or error("unknown tool: " .. slot_or_name)
    end
    return slot_or_name
end

function H:begin_tool_move(slot, target_rad, speed_rad_s)
    return L.phys_begin_tool_move(self.p, resolve_slot(self, slot), target_rad, speed_rad_s or 0.0)
end
function H:begin_grip(slot)    return L.phys_begin_grip   (self.p, resolve_slot(self, slot)) end
function H:begin_release(slot) return L.phys_begin_release(self.p, resolve_slot(self, slot)) end
function H:begin_dock(slot)    return L.phys_begin_dock   (self.p, resolve_slot(self, slot)) end
function H:begin_charge(slot, target_j)
    return L.phys_begin_charge(self.p, resolve_slot(self, slot), target_j or 0.0)
end

function H:station_at_pose(kind) return tonumber(L.phys_station_at_pose(self.p, STATION_KIND[kind] or 0)) end
function H:battery_j()           return tonumber(L.phys_battery_j(self.p)) end
function H:payload_mass()        return tonumber(L.phys_payload_mass(self.p)) end

return M
