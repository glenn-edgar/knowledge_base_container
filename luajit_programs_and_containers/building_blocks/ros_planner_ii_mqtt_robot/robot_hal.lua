--[[
    robot_hal.lua -- Hardware abstraction. Selects sim or real backend.

    Today only "sim" exists (backed by physics_ffi + libphysics.so).
    HAL_MODE env var or opts.mode picks the backend.

    HAL surface (mirrors physics_ffi, minus C-specific plumbing):
      step(dt)                               -- sim only; real HAL ignores
      sim_time() -> sec
      read_pose() -> { x, y, heading, v, omega, sim_t }
      read_pose_truth() -> same, ground-truth
      read_path_status() -> { flags, active_seg_id, last_done_seg_id, queue_depth,
                              active_progress, cross_track_err, heading_err,
                              energy_used_total, v_cmd }
      read_tool_status(slot) -> { flags, kind, value, target, payload_mass,
                                  battery_j, battery_capacity_j }
      push_line / push_spline / push_rotate -> seg_id
      request_stop / release_stop / is_stopped
      abort_path / queue_depth / active_seg_id / last_done_seg_id
      begin_tool_move / begin_grip / begin_release / begin_dock / begin_charge
      PATH_F / TOOL_F / TOOL_KIND / STATION_KIND  -- flag constants
]]

local M = {}

local function load_json(path)
    local ok, json_util = pcall(require, "json_util")
    if not ok then error("robot_hal: json_util not on path") end
    local f, err = io.open(path, "r")
    if not f then error("robot_hal: cannot open " .. path .. ": " .. tostring(err)) end
    local content = f:read("*a"); f:close()
    return json_util.decode(content)
end

function M.new(opts)
    opts = opts or {}
    local mode = opts.mode or os.getenv("HAL_MODE") or "sim"
    if mode ~= "sim" then
        error("robot_hal: mode '" .. mode .. "' not supported yet (only 'sim')")
    end

    local physics_ffi = require("physics_ffi")
    local physics_config = opts.physics_config or load_json(opts.physics_config_path
        or (opts.dir and (opts.dir .. "/physics_config.json"))
        or "physics_config.json")
    local sim_map = opts.sim_map or load_json(opts.sim_map_path
        or (opts.dir and (opts.dir .. "/sim_map.json"))
        or "sim_map.json")

    local hal = physics_ffi.new(physics_config, sim_map, { seed = opts.seed })

    -- Expose flag constants on the returned handle
    hal.PATH_F       = physics_ffi.PATH_F
    hal.TOOL_F       = physics_ffi.TOOL_F
    hal.TOOL_KIND    = physics_ffi.TOOL_KIND
    hal.STATION_KIND = physics_ffi.STATION_KIND
    hal.mode         = mode
    return hal
end

return M
