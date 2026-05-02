-- =============================================================================
-- user_functions.lua -- robot_base controller registry.
--
-- M.build(ctx) returns the chain-tree user-function map. ctx fields:
--   env                  ENV snapshot (validated by env_validate.lua)
--   class_root           /opt/apps/<ROBOT_CLASS>
--   run_dir              /run/robot
--   class_processes      decoded class_processes.json
--   children             { [name] = { argv, pid, stdout_fd, started_ts,
--                                     ready, env, role } }
--   sim                  shortcut: children["robot_sim"]
--   mqtt                 shortcut: children["mqtt_robot"]
--   shutdown_requested_getter
--   upward_peer
--   log
--   cfl_rt / chain_tree
-- =============================================================================

local eq_mod = require("cfl_event_queue")
local defs   = require("cfl_definitions")
local sm_mod = require("cfl_state_machine")

local ptime  = require("posix_time")
local pp     = require("process_primitives")
local ph     = require("process_helpers")
local er     = require("env_validate")
local cr     = require("config_render")
local up     = require("upward_peer")

local M = {}

local function now_ns() return math.floor(ptime.now_sec() * 1e9) end

local function read_file(path)
    local f = io.open(path, "rb")
    if not f then return nil end
    local d = f:read("*a"); f:close(); return d
end

local function terminate_system(ctx, reason)
    ctx.terminate_reason = reason or "unspecified"
    local h = ctx.chain_tree.handle
    eq_mod.send_null(h.event_queue, defs.CFL_TERMINATE_SYSTEM_EVENT)
end

-- Build env passed to a child. Inherits validated rover env, adds role-
-- specific overlay (e.g. ROBOT_SIM_PTY for mqtt_robot).
local function child_env(ctx, overlay)
    local e = {}
    for k, v in pairs(ctx.env) do e[k] = v end
    if overlay then
        for k, v in pairs(overlay) do e[k] = v end
    end
    return e
end

function M.build(ctx)
    local log = ctx.log
    local R = {}

    ----------------------------------------------------------------------
    -- VALIDATE_ENV (also re-checks ROBOT_CLASS_BAKED match)
    ----------------------------------------------------------------------
    R.VALIDATE_ENV = function(_h, _n)
        local env, err = er.gather()
        if not env then
            log("robot", "ENV invalid: " .. err)
            terminate_system(ctx, err)
            return
        end
        ctx.env         = env
        ctx.class_root  = "/opt/apps/" .. env.ROBOT_CLASS
        ctx.run_dir     = "/run/robot"
        ctx.upward_peer = up.new(ctx)

        os.execute("mkdir -p " .. ctx.run_dir)

        local cp_path = ctx.class_root .. "/class_processes.json"
        local cp_data = read_file(cp_path)
        if not cp_data then
            log("robot", "missing " .. cp_path)
            terminate_system(ctx, "no class_processes.json")
            return
        end
        local json = require("json_util")
        local ok, decoded = pcall(json.decode, cp_data)
        if not ok or not decoded then
            log("robot", "bad class_processes.json: " .. tostring(decoded))
            terminate_system(ctx, "bad class_processes.json")
            return
        end
        ctx.class_processes = decoded
        ctx.children        = {}

        log("robot", string.format(
            "identity: id=%s class=%s dongle=%s site=%s",
            env.ROBOT_ID, env.ROBOT_CLASS,
            env.DONGLE_INSTANCE, env.VMRT_KB_SITE))
    end

    ----------------------------------------------------------------------
    -- RENDER_CONFIG: template -> /run/robot/config.json
    ----------------------------------------------------------------------
    R.RENDER_CONFIG = function(_h, _n)
        local tpl = ctx.class_root .. "/config.template.json"
        local tgt = ctx.run_dir   .. "/config.json"
        -- Render env contains both ENV and class_root for path tokens.
        local renv = {}
        for k, v in pairs(ctx.env) do renv[k] = v end
        renv.CLASS_ROOT = ctx.class_root
        renv.RUN_DIR    = ctx.run_dir
        local out, err = cr.render(tpl, tgt, renv)
        if not out then
            log("robot", "RENDER_CONFIG failed: " .. err)
            terminate_system(ctx, "config render failed")
            return
        end
        log("robot", "rendered " .. tgt)
    end

    ----------------------------------------------------------------------
    -- SPAWN_ROBOT_SIM: starts the C dongle simulator with stdout pipe.
    ----------------------------------------------------------------------
    R.SPAWN_ROBOT_SIM = function(_h, _n)
        local sim_spec = ctx.class_processes.robot_sim
        if not sim_spec then
            log("robot", "class_processes.json missing robot_sim entry")
            terminate_system(ctx, "no robot_sim spec")
            return
        end
        local argv = {}
        for _, a in ipairs(sim_spec.argv) do
            argv[#argv + 1] = a:gsub("%${([A-Za-z_][A-Za-z0-9_]*)}",
                function(k) return ctx.env[k] or "" end)
        end
        local pid, fd_or_err = ph.spawn_with_stdout_pipe(
            argv, child_env(ctx, sim_spec.env))
        if not pid then
            log("robot", "spawn robot_sim FAILED: " .. tostring(fd_or_err))
            terminate_system(ctx, "robot_sim spawn failed")
            return
        end
        ctx.children.robot_sim = {
            name       = "robot_sim",
            argv       = argv,
            pid        = pid,
            stdout_fd  = fd_or_err,
            started_ts = now_ns(),
            ready      = false,
            pty_path   = nil,
            role       = "robot_sim",
        }
        ctx.sim = ctx.children.robot_sim
        log("robot", string.format("spawn robot_sim pid=%d argv=%s",
            pid, table.concat(argv, " ")))
    end

    ----------------------------------------------------------------------
    -- VERIFY_SIM_READY: drain stdout, look for PTY=… and READY.
    ----------------------------------------------------------------------
    -- asm_wait semantics: bool=false halts (stay in state), bool=true
    -- advances. Drain stdout on every event so PTY/READY are captured
    -- regardless of which event woke us up.
    R.VERIFY_SIM_READY = function(_h, _n, _et, _event_id, _ed)
        local s = ctx.sim
        if not s or not s.stdout_fd then return false end
        local lines = ph.read_lines_nonblocking(s.stdout_fd)
        for _, line in ipairs(lines) do
            log("sim", line)
            local p = line:match("^PTY=(.+)$")
            if p then s.pty_path = p end
            if line == "READY" then s.ready = true end
        end
        return s.pty_path ~= nil and s.ready
    end

    R.ERR_SIM_NOT_READY = function(_h, _n)
        log("robot", "ERR_SIM_NOT_READY -- robot_sim never published READY")
        terminate_system(ctx, "robot_sim not ready")
    end

    ----------------------------------------------------------------------
    -- SPAWN_MQTT_ROBOT: starts the Lua robot main with sim pty wired in.
    ----------------------------------------------------------------------
    R.SPAWN_MQTT_ROBOT = function(_h, _n)
        local spec = ctx.class_processes.mqtt_robot
        if not spec then
            log("robot", "class_processes.json missing mqtt_robot entry")
            terminate_system(ctx, "no mqtt_robot spec")
            return
        end
        local argv = {}
        for _, a in ipairs(spec.argv) do
            argv[#argv + 1] = a:gsub("%${([A-Za-z_][A-Za-z0-9_]*)}",
                function(k) return ctx.env[k] or "" end)
        end
        local overlay = {}
        for k, v in pairs(spec.env or {}) do overlay[k] = v end
        overlay.ROBOT_SIM_PTY = ctx.sim.pty_path
        overlay.HAL_MODE      = ctx.env.HAL_MODE

        local pid, fd_or_err = ph.spawn_with_stdout_pipe(
            argv, child_env(ctx, overlay))
        if not pid then
            log("robot", "spawn mqtt_robot FAILED: " .. tostring(fd_or_err))
            terminate_system(ctx, "mqtt_robot spawn failed")
            return
        end
        ctx.children.mqtt_robot = {
            name       = "mqtt_robot",
            argv       = argv,
            pid        = pid,
            stdout_fd  = fd_or_err,
            started_ts = now_ns(),
            ready      = true,
            role       = "mqtt_robot",
        }
        ctx.mqtt = ctx.children.mqtt_robot
        log("robot", string.format(
            "spawn mqtt_robot pid=%d sim_pty=%s argv=%s",
            pid, ctx.sim.pty_path, table.concat(argv, " ")))
    end

    ----------------------------------------------------------------------
    -- REGISTER_WITH_PEER: Phase 1 no-op
    ----------------------------------------------------------------------
    R.REGISTER_WITH_PEER = function(_h, _n)
        local ok, why = ctx.upward_peer:register()
        log("robot", "upward_peer:register -> " .. tostring(why or ok))
    end

    ----------------------------------------------------------------------
    -- monitor column handlers
    ----------------------------------------------------------------------

    -- Drain stdout from BOTH children every tick so log lines flow
    -- through the supervisor (and so EOF is observable in liveness).
    R.DRAIN_SIM_STDOUT = function(_h, _n)
        for _, c in pairs(ctx.children or {}) do
            if c.stdout_fd then
                local lines = ph.read_lines_nonblocking(c.stdout_fd)
                for _, line in ipairs(lines) do log(c.name, line) end
            end
        end
    end

    R.VERIFY_CHILDREN_ALIVE = function(_h, _n, _et, event_id, _ed)
        if event_id ~= defs.CFL_TIMER_EVENT then return true end
        if not ctx.children then return true end
        for name, c in pairs(ctx.children) do
            if c.pid then
                local r = pp.waitpid_nohang(c.pid)
                if r then
                    log("robot", string.format(
                        "child %s exited: pid=%d code=%s",
                        name, c.pid, tostring(r.exit_code)))
                    c.last_exit = r.exit_code
                    c.pid = nil
                    return false
                end
            else
                return false
            end
        end
        return true
    end

    R.ERR_CHILD_DIED = function(handle, _n)
        log("robot", "ERR_CHILD_DIED -> request_shutdown (fail-stop)")
        local s = handle.flash_handle.sm_by_name
                  and handle.flash_handle.sm_by_name["robot_sm"]
        if not s then
            terminate_system(ctx, "child died (no sm_by_name)")
            return
        end
        local idx = s.states["request_shutdown"]
        if not idx then
            terminate_system(ctx, "child died (no request_shutdown state)")
            return
        end
        sm_mod.change_state(handle, 0, s.node_id, idx, nil)
    end

    R.VERIFY_NO_SHUTDOWN_SIGNAL = function(_h, _n, _et, event_id, _ed)
        if event_id ~= defs.CFL_TIMER_EVENT then return true end
        if ctx.shutdown_requested_getter and ctx.shutdown_requested_getter() then
            log("robot", "SIGTERM/SIGINT caught -- requesting teardown")
            return false
        end
        return true
    end

    R.ERR_TEARDOWN_REQUESTED = function(handle, _n)
        log("robot", "ERR_TEARDOWN_REQUESTED -> request_shutdown")
        local s = handle.flash_handle.sm_by_name
                  and handle.flash_handle.sm_by_name["robot_sm"]
        if not s then
            terminate_system(ctx, "teardown requested (no sm_by_name)")
            return
        end
        local idx = s.states["request_shutdown"]
        sm_mod.change_state(handle, 0, s.node_id, idx, nil)
    end

    R.UPWARD_PEER_TICK = function(_h, _n)
        if ctx.upward_peer then ctx.upward_peer:tick() end
    end

    ----------------------------------------------------------------------
    -- request_shutdown / teardown
    ----------------------------------------------------------------------
    R.SIGTERM_ALL_CHILDREN = function(_h, _n)
        for name, c in pairs(ctx.children or {}) do
            if c.pid then
                pp.kill(c.pid, pp.signals.SIGTERM)
                log("robot", string.format("SIGTERM %s pid=%d", name, c.pid))
            end
        end
    end

    R.VERIFY_ALL_CHILDREN_EXITED = function(_h, _n, _et, event_id, _ed)
        if event_id ~= defs.CFL_TIMER_EVENT then return true end
        for _, c in pairs(ctx.children or {}) do
            if c.pid then
                local r = pp.waitpid_nohang(c.pid)
                if r then c.pid = nil
                else return false end
            end
        end
        return true
    end

    R.ERR_FORCE_TEARDOWN = function(_h, _n)
        log("robot", "ERR_FORCE_TEARDOWN -- grace window expired")
    end

    R.SIGKILL_ALL_CHILDREN = function(_h, _n)
        for name, c in pairs(ctx.children or {}) do
            if c.pid then
                pp.kill(c.pid, pp.signals.SIGKILL)
                log("robot", string.format("SIGKILL %s pid=%d", name, c.pid))
                for _ = 1, 20 do
                    if pp.waitpid_nohang(c.pid) then break end
                    ptime.sleep_for(0.05)
                end
                c.pid = nil
            end
            if c.stdout_fd then
                ph.close_fd(c.stdout_fd); c.stdout_fd = nil
            end
        end
    end

    R.UPWARD_PEER_SHUTDOWN = function(_h, _n)
        if ctx.upward_peer then ctx.upward_peer:on_shutdown() end
    end

    return R
end

return M
