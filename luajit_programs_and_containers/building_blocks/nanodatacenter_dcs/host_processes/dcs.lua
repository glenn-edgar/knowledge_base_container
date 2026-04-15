#!/usr/bin/env luajit
-- =============================================================================
-- dcs.lua -- DCS host-process harness.
--
-- Reusable module exposing the lifecycle of a DCS host process:
--   open bootstrap.db -> init chain-tree -> activate initial KB -> tick loop.
--
-- User-function implementations live in host_processes/user_functions.lua
-- (a separate module). They are injected via the context object so this
-- harness stays free of project-specific policy.
--
-- Direct invocation (from start.sh) runs M.main(arg).
-- Module mode: `local dcs = require("dcs"); dcs.main(...)` or exercise the
-- helpers (open_bootstrap, init_chain_tree, run_loop) from a test driver.
-- =============================================================================

local M = {}

local h    = require("sqlite3_helpers")
local json = h.json

local cfl_rt   = require("cfl_runtime")
local loader   = require("cfl_json_loader")
local builtins = require("cfl_builtins")
local sm       = require("cfl_state_machine")

local user_functions   = require("user_functions")
local ptime            = require("posix_time")
local docker           = require("docker")
local pg_connector     = require("pg_connector")
local bit_mask_helpers = require("bit_mask_helpers")
local kb_status        = require("kb_status")
local kb_exception     = require("kb_exception")

---------------------------------------------------------------------------
-- paths
---------------------------------------------------------------------------

local function script_dir()
  local src = debug.getinfo(1, "S").source
  if src:sub(1,1) == "@" then src = src:sub(2) end
  return src:match("(.*/)") or "./"
end
M.script_dir = script_dir

---------------------------------------------------------------------------
-- logger (stderr; start.sh redirects to error.log)
---------------------------------------------------------------------------

function M.make_logger()
  return function(half, msg)
    io.stderr:write(string.format("%s [%s] %s\n",
      os.date("!%Y-%m-%dT%H:%M:%SZ"), half, msg))
    io.stderr:flush()
  end
end

---------------------------------------------------------------------------
-- default context shared by user functions
---------------------------------------------------------------------------

function M.default_context()
  return {
    cfg        = nil,
    bootstrap  = { path = nil, db = nil },
    log        = M.make_logger(),
    connectors = { pg = nil, nats = nil, mqtt = nil },
    process_globals = {
      node_control_operational      = false,
      node_control_heartbeat_ts     = 0,
      node_control_teardown_request = false,
      node_control_stopped          = false,
      system_ready_current          = false,
    },
    system_control_globals = {},
    node_control_globals   = {},
    chain_tree = { flash = nil, handle = nil, kb_indexes = {} },
    -- cfl_rt is referenced by user functions that activate/deactivate KBs
    cfl_rt     = cfl_rt,
    -- docker primitives (stop_labelled, run_from_spec, stop, is_running)
    docker     = docker,
    -- pg connect helper (consumed by VERIFY_PG)
    pg_connector = pg_connector,
    -- direct-SQL helpers for bit_mask_table + status_table writes
    bit_mask_helpers = bit_mask_helpers,
    kb_status        = kb_status,
    kb_exception     = kb_exception,
    -- tunable settings (override before M.main, or inject via ctx builder).
    -- Default: sim time matches wall time so chain-tree verify_timeout(30s)
    -- actually waits ~30 real seconds. One chain-tree tick per outer loop;
    -- outer loop paces via CLOCK_MONOTONIC at 1s.
    settings = {
      tick_interval_s     = 1.0,   -- outer-loop pacing
      cfl_delta_time      = 1.0,   -- chain-tree seconds per tick
      cfl_max_ticks       = 1,     -- chain-tree ticks per outer burst
      heartbeat_fresh_s   = 10,    -- VERIFY_NODE_CTRL_HEARTBEAT_FRESH window
                                   -- (must exceed publish cadence; monitor
                                   --  hb_col publishes every 5s)
    },
  }
end

---------------------------------------------------------------------------
-- open bootstrap.db (read-only) and parse bootstrap.config
---------------------------------------------------------------------------

function M.open_bootstrap(path)
  local ffi = require("ffi")
  local sl  = h.sqlite3_lib
  pcall(ffi.cdef, [[
    int sqlite3_open_v2(const char *filename, void **ppDb,
                        int flags, const char *zVfs);
  ]])
  local SQLITE_OPEN_READONLY = 0x00000001
  local pp = ffi.new("void*[1]")
  if sl.sqlite3_open_v2(path, pp, SQLITE_OPEN_READONLY, nil) ~= 0 then
    error("cannot open " .. path .. " (read-only)")
  end
  local db = pp[0]
  local rows = h.sql_query(db,
    "SELECT data FROM knowledge_base WHERE label='bootstrap' AND name='config'", {})
  if #rows == 0 then error("bootstrap.config not found in " .. path) end
  local cfg = json.decode(rows[1].data)
  return db, cfg
end

---------------------------------------------------------------------------
-- load all container_definition build.spec rows into a map keyed by def name
-- (consumed by user functions to drive docker.run_from_spec)
---------------------------------------------------------------------------

function M.load_build_specs(bdb)
  local rows = h.sql_query(bdb, [[
    SELECT path, data FROM knowledge_base
     WHERE label = 'build' AND name = 'spec'
       AND path LIKE 'system.container_definition.%'
  ]], {})
  local specs = {}
  for _, r in ipairs(rows) do
    -- path = system.container_definition.<def>.build.spec
    local def_name = r.path:match("^system%.container_definition%.([^%.]+)%.build%.spec$")
    if def_name then
      specs[def_name] = json.decode(r.data)
    end
  end
  return specs
end

---------------------------------------------------------------------------
-- load chain-tree JSON IR + register user functions + validate
---------------------------------------------------------------------------

function M.init_chain_tree(json_path, user_fns, settings)
  settings = settings or { cfl_delta_time = 0.1, cfl_max_ticks = 50 }

  local flash = loader.load(json_path)
  loader.register_functions(flash, builtins, sm, user_fns)
  local ok, missing = loader.validate(flash)
  if not ok then
    local list = {}
    for _, m in ipairs(missing) do
      list[#list + 1] = "  " .. m.kind .. ": " .. m.name
    end
    error("chain-tree has unresolved user functions:\n" ..
          table.concat(list, "\n"))
  end

  local kb_indexes = {}
  for i, kb in ipairs(flash.kb_table) do
    kb_indexes[kb.name] = i - 1   -- 0-based
  end

  local handle = cfl_rt.create({
    delta_time = settings.cfl_delta_time,
    max_ticks  = settings.cfl_max_ticks,
  }, flash)
  cfl_rt.reset(handle)

  return flash, handle, kb_indexes
end

---------------------------------------------------------------------------
-- activate initial KB on master/slave policy
---------------------------------------------------------------------------

function M.activate_initial_kb(ctx)
  local kb_idx = ctx.chain_tree.kb_indexes
  local h_     = ctx.chain_tree.handle
  if ctx.cfg.is_master == 1 then
    ctx.log("dcs", "activating system_control KB (master)")
    cfl_rt.add_test(h_, kb_idx.system_control)
  else
    ctx.log("dcs", "activating node_control KB (non-master)")
    cfl_rt.add_test(h_, kb_idx.node_control)
  end
end

---------------------------------------------------------------------------
-- tick loop (stops on SIGTERM/SIGINT via watchdog)
---------------------------------------------------------------------------

-- Return true if any KB is still registered as active.
local function any_active(handle)
  for _ in pairs(handle.active_tests) do return true end
  return false
end

function M.run_loop(ctx)
  local interval = (ctx.settings and ctx.settings.tick_interval_s) or 1.0
  ctx.log("dcs", string.format(
    "entering chain-tree tick loop (interval=%.3fs, CLOCK_MONOTONIC)",
    interval))

  local handle   = ctx.chain_tree.handle
  local burst    = 0
  local deadline = ptime.now_sec()

  while true do
    burst = burst + 1
    local t0 = ptime.now_sec()
    cfl_rt.run(handle)
    local run_ms = (ptime.now_sec() - t0) * 1000.0

    ctx.log("dcs", string.format(
      "burst=%d ticks=%d run=%.1fms sys_ready=%s node_op=%s",
      burst, handle.tick_count or 0, run_ms,
      tostring(ctx.process_globals.system_ready_current),
      tostring(ctx.process_globals.node_control_operational)))

    -- Terminate detection: either an error handler pumped
    -- CFL_TERMINATE_SYSTEM_EVENT (runtime set cfl_engine_flag=false and
    -- cleared active_tests) or all KBs finished normally. In both cases
    -- no KB remains; exit so watchdog restarts.
    if not any_active(handle) then
      local reason = ctx.terminate_reason or "no active tests"
      ctx.log("dcs", "exiting cleanly: " .. reason)
      os.exit(1)
    end

    -- Drift-free pacing: deadline advances by interval regardless of run time.
    -- If a burst overruns, skip ahead (don't stack backlog).
    deadline = deadline + interval
    local now = ptime.now_sec()
    if deadline < now then deadline = now end
    ptime.sleep_until(deadline)
  end
end

---------------------------------------------------------------------------
-- entry point
---------------------------------------------------------------------------

function M.main(args)
  local bootstrap_path = args[1]
    or error("missing argv[1] (bootstrap.db path)")

  local ctx = M.default_context()
  ctx.bootstrap.path = bootstrap_path

  local bdb, cfg = M.open_bootstrap(bootstrap_path)
  ctx.bootstrap.db = bdb
  ctx.cfg          = cfg

  ctx.log("dcs", "opened " .. bootstrap_path .. " (read-only)")
  ctx.log("dcs", string.format("identity: site=%s cpu=%s master=%s",
    cfg.site, cfg.cpu_id, tostring(cfg.is_master == 1)))

  -- Load build specs once at boot; user functions consume via
  -- ctx.system_control_globals.build_specs[def_name].
  ctx.system_control_globals.build_specs = M.load_build_specs(bdb)
  local spec_names = {}
  for name, _ in pairs(ctx.system_control_globals.build_specs) do
    spec_names[#spec_names + 1] = name
  end
  table.sort(spec_names)
  ctx.log("dcs", "build specs loaded: " .. table.concat(spec_names, ", "))

  -- Build user_fns over ctx (closures capture connectors, globals, etc.)
  local user_fns = user_functions.build(ctx)

  local json_path = script_dir() .. "../chain_tree/dcs.json"
  ctx.log("dcs", "loading chain-tree IR from " .. json_path)
  local flash, handle, kb_indexes =
    M.init_chain_tree(json_path, user_fns, ctx.settings)
  ctx.chain_tree.flash      = flash
  ctx.chain_tree.handle     = handle
  ctx.chain_tree.kb_indexes = kb_indexes
  ctx.log("dcs", string.format("KB indexes: system_control=%d node_control=%d",
    kb_indexes.system_control or -1, kb_indexes.node_control or -1))

  M.activate_initial_kb(ctx)
  M.run_loop(ctx)
end

---------------------------------------------------------------------------
-- direct invocation
---------------------------------------------------------------------------

if arg and (arg[0] or ""):match("dcs%.lua$") then
  M.main(arg)
end

return M
