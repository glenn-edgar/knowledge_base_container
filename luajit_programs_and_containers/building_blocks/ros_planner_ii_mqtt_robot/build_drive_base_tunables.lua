-- build_drive_base_tunables.lua
-- Convert physics_config.json → drive_base_tunables.bin (a packed
-- struct readable by robot_sim's --tunables option). Mirrors Q3's
-- NVS commissioning blob: same struct, same schema version, just
-- file-backed instead of NVS-backed.
--
-- Usage:
--   luajit build_drive_base_tunables.lua physics_config.json out.bin

local ffi = require("ffi")

ffi.cdef[[
typedef struct __attribute__((packed)) {
    uint16_t schema_version;
    uint16_t _pad0;

    float wheelbase_m;
    float wheel_radius_m;
    float mass_kg;
    float inertia_kg_m2;
    float lin_friction;
    float ang_friction;

    float max_torque_nm;
    float max_wheel_rad_s;
    float pid_kp, pid_ki, pid_kd;
    float energy_k;

    float lookahead_min_m;
    float lookahead_k_v;
    float arrival_tol_m;
    float heading_tol_rad;
    float max_linear_accel;
    float max_angular_accel;
    float cross_track_abort_m;
    float inner_dt_s;

    float gps_sigma_xy;
    float gps_sigma_h;
    float imu_sigma;
    float battery_noise_pct;

    float battery_capacity_j;
    float battery_initial_j;

    float init_x, init_y, init_heading;

    uint64_t seed;
} drive_base_tunables_t;
]]

local SCHEMA_VERSION = 1

local args = arg or {}
if #args < 2 then
    io.stderr:write("Usage: luajit build_drive_base_tunables.lua "
                 .. "<physics_config.json> <out.bin>\n")
    os.exit(2)
end
local in_path  = args[1]
local out_path = args[2]

local function load_json(path)
    local ok, json_util = pcall(require, "json_util")
    if not ok then
        io.stderr:write("build_drive_base_tunables: json_util not on LUA_PATH\n")
        os.exit(1)
    end
    local f = assert(io.open(path, "r"), "cannot open "..path)
    local s = f:read("*a"); f:close()
    return json_util.decode(s)
end

local cfg = load_json(in_path)

local tun = ffi.new("drive_base_tunables_t")
tun.schema_version = SCHEMA_VERSION
tun._pad0          = 0

local chassis  = cfg.chassis      or {}
local motors   = cfg.motors       or {}
local pid      = motors.pid       or {}
local follower = cfg.path_follower or {}
local sensors  = cfg.sensors      or {}
local gps      = sensors.gps      or {}
local imu      = sensors.imu      or {}
local battnoise= sensors.battery  or {}
local battery  = cfg.battery      or {}
local pose     = cfg.initial_pose or {}

tun.wheelbase_m         = chassis.wheelbase_m     or 0.30
tun.wheel_radius_m      = chassis.wheel_radius_m  or 0.04
tun.mass_kg             = chassis.mass_kg         or 8.0
tun.inertia_kg_m2       = chassis.inertia_kg_m2   or 0.12
tun.lin_friction        = chassis.lin_friction    or 0.8
tun.ang_friction        = chassis.ang_friction    or 0.4

tun.max_torque_nm       = motors.max_torque_nm    or 1.5
tun.max_wheel_rad_s     = motors.max_wheel_rad_s  or 30.0
tun.pid_kp              = pid.kp                  or 8.0
tun.pid_ki              = pid.ki                  or 4.0
tun.pid_kd              = pid.kd                  or 0.05
tun.energy_k            = motors.energy_k         or 1.0

tun.lookahead_min_m     = follower.lookahead_min_m     or 0.20
tun.lookahead_k_v       = follower.lookahead_k_v       or 0.40
tun.arrival_tol_m       = follower.arrival_tol_m       or 0.05
tun.heading_tol_rad     = follower.heading_tol_rad     or 0.05
tun.max_linear_accel    = follower.max_linear_accel    or 1.0
tun.max_angular_accel   = follower.max_angular_accel   or 3.0
tun.cross_track_abort_m = follower.cross_track_abort_m or 20.0
tun.inner_dt_s          = follower.inner_dt_s          or 0.005

tun.gps_sigma_xy        = gps.pose_sigma_xy            or 0.0
tun.gps_sigma_h         = gps.pose_sigma_h             or 0.0
tun.imu_sigma           = imu.yaw_rate_sigma           or 0.0
tun.battery_noise_pct   = battnoise.noise_pct          or 0.0

tun.battery_capacity_j  = battery.capacity_j or 100000.0
tun.battery_initial_j   = battery.initial_j  or tun.battery_capacity_j

tun.init_x              = pose.x       or 0.0
tun.init_y              = pose.y       or 0.0
tun.init_heading        = pose.heading or 0.0

-- 64-bit seed: caller may set "sim_seed" in cfg or override at runtime.
tun.seed = cfg.sim_seed and ffi.cast("uint64_t", cfg.sim_seed) or 0

local out = io.open(out_path, "wb")
if not out then
    io.stderr:write("cannot open "..out_path.." for write\n")
    os.exit(1)
end
out:write(ffi.string(tun, ffi.sizeof("drive_base_tunables_t")))
out:close()

io.stderr:write(string.format(
    "build_drive_base_tunables: wrote %d bytes to %s\n",
    ffi.sizeof("drive_base_tunables_t"), out_path))
