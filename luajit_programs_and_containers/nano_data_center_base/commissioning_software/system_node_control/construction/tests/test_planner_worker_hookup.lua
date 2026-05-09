#!/usr/bin/env luajit
-- =============================================================================
-- test_planner_worker_hookup.lua -- Phase 5b worker hookup acceptance.
--
-- Two layers of coverage:
--
--  1. The on_tick contract in action_server:serve():
--     - called once per cycle with the cycle index (1-based)
--     - errors inside on_tick are caught (pcall) so a transient
--       heartbeat-side blip doesn't take down mission execution
--     - max_cycles + on_tick interact correctly (callback fires
--       max_cycles times, then loop exits)
--     - on_tick=nil works (back-compat with existing callers)
--
--  2. main.lua parse-load + structural sanity:
--     - file parses cleanly (no syntax errors after the migration)
--     - jq_observer / drain_observer references are gone
--     - on_tick + serve({drain_nats=true}) wiring present
--     - heartbeat-only fallback present
--
-- The on_tick test exercises the REAL action_server:serve loop body
-- with a fake instance that stubs out network/IO dependencies, so we
-- get end-to-end semantics for the hook without needing a live NATS.
-- Requires LD_LIBRARY_PATH pointing at the planner container's
-- prebuilt_libs/ so libnats_*.so loads at action_server require time.
-- =============================================================================

local SCRIPT_DIR = arg[0]:match("(.*/)") or "./"
local REPO_ROOT  = SCRIPT_DIR .. "../../../../../"
local PLANNER    = REPO_ROOT
    .. "nano_data_center_instance/app_containers/mission_planner/container/planner"

-- Resolve the action_server require chain (mirrors main.lua's package.path).
-- chain_tree/lua_dsl/luajit_pipeline/json_util.lua is installed at
-- /usr/local/share/lua/5.1 inside the container; host-side it lives
-- under luajit_base/container/prebuilt_lua_share/.
local LUAJIT_BASE = REPO_ROOT .. "nano_data_center_base/luajit/luajit_base"
package.path = PLANNER .. "/lib/?.lua;" ..
               PLANNER .. "/?.lua;" ..
               PLANNER .. "/hub_dsl/?.lua;" ..
               PLANNER .. "/hub_dsl/hub_functions/?.lua;" ..
               PLANNER .. "/hub_dsl/protocol/?.lua;" ..
               PLANNER .. "/hub_dsl/kb_construct/?.lua;" ..
               PLANNER .. "/hub_dsl/kb/?.lua;" ..
               LUAJIT_BASE .. "/container/prebuilt_lua_share/?.lua;" ..
               LUAJIT_BASE .. "/container/prebuilt_lua_share/" ..
                  "chain_tree/lua_dsl/luajit_pipeline/?.lua;" ..
               package.path

local pass, fail = 0, 0
local function ok(name, cond, detail)
  if cond then pass = pass + 1; print("  ok  " .. name)
  else fail = fail + 1; print("  FAIL " .. name .. (detail and " -- " .. detail or "")) end
end

local function read_file(path)
  local f = io.open(path, "rb"); if not f then return nil end
  local s = f:read("*a"); f:close(); return s
end

------------------------------------------------------------------------
-- 1. on_tick contract
------------------------------------------------------------------------

print("== action_server:serve on_tick contract ==")

local M = require("lib.action_server")
ok("action_server module loaded", type(M) == "table")
ok("M:serve method exists", type(M.serve) == "function")

-- Build a minimal fake instance: just the fields serve() touches
-- when there are no pending missions, no link_mgr, no mqtt_hub. The
-- only real cost per cycle is the ffi.C.usleep(50000) idle sleep, so
-- max_cycles=3 -> ~150ms test duration.
local function make_fake_srv()
  return {
    mission_count   = 0,
    missions        = {},
    pending         = {},
    link_mgr        = nil,
    _link_kv_writer = nil,
    mqtt_hub        = nil,
    tick_usleep     = 1000,
    _drain_nats_queue = function(self) end,  -- no-op stub
  }
end

-- Test 1: on_tick called exactly max_cycles times with cycle indices
do
  local fake = make_fake_srv()
  local seen_indices = {}
  M.serve(fake, {
    drain_nats = true,
    max_cycles = 3,
    on_tick    = function(idx) seen_indices[#seen_indices + 1] = idx end,
  })
  ok("on_tick called 3 times for max_cycles=3",
     #seen_indices == 3,
     "got " .. #seen_indices)
  ok("cycle indices are 1-based and sequential",
     seen_indices[1] == 1 and seen_indices[2] == 2 and seen_indices[3] == 3,
     "got " .. table.concat(seen_indices, ","))
end

-- Test 2: on_tick error is caught (loop continues to max_cycles)
do
  local fake = make_fake_srv()
  local hits = 0
  M.serve(fake, {
    drain_nats = true,
    max_cycles = 3,
    on_tick    = function(idx)
      hits = hits + 1
      if idx == 2 then error("boom from on_tick") end
    end,
  })
  ok("on_tick raised on cycle 2 -- loop still completes 3 cycles",
     hits == 3, "hits=" .. hits)
end

-- Test 3: on_tick=nil is accepted (back-compat)
do
  local fake = make_fake_srv()
  local ok_call = pcall(function()
    M.serve(fake, { drain_nats = true, max_cycles = 2 })
  end)
  ok("serve() with no on_tick still works (back-compat)", ok_call)
end

-- Test 4: max_cycles bound respected when on_tick present
do
  local fake = make_fake_srv()
  local hits = 0
  M.serve(fake, {
    drain_nats = true,
    max_cycles = 5,
    on_tick    = function() hits = hits + 1 end,
  })
  ok("on_tick called exactly max_cycles=5 times", hits == 5,
     "hits=" .. hits)
end

------------------------------------------------------------------------
-- 2. main.lua structural sanity
------------------------------------------------------------------------

print()
print("== main.lua structural sanity ==")

local main_src = read_file(PLANNER .. "/main.lua")
ok("main.lua readable", main_src ~= nil)

local chunk, perr = loadfile(PLANNER .. "/main.lua")
ok("main.lua parses cleanly",
   chunk ~= nil, perr and tostring(perr) or "")

if main_src then
  -- Old observer wiring is gone
  ok("jq_observer reference removed",
     main_src:find("jq_observer", 1, true) == nil,
     "still references jq_observer")
  ok("drain_observer reference removed",
     main_src:find("drain_observer", 1, true) == nil,
     "still references drain_observer")
  ok("logged_only marker removed (was the A.3.6 stub)",
     main_src:find("logged_only", 1, true) == nil,
     "still references logged_only")

  -- New hookup wiring is present
  ok("calls action_srv:serve",
     main_src:find("action_srv:serve", 1, true) ~= nil)
  ok("passes drain_nats=true",
     main_src:find("drain_nats = true", 1, true) ~= nil)
  ok("passes on_tick callback",
     main_src:find("on_tick%s*=%s*on_tick") ~= nil)

  -- Heartbeat closure restructure
  ok("fire_heartbeat function defined",
     main_src:find("function fire_heartbeat", 1, true) ~= nil
     or main_src:find("local function fire_heartbeat", 1, true) ~= nil)
  ok("on_tick gates heartbeat by wall time",
     main_src:find("os.time() - hb_state.last_at", 1, true) ~= nil)
  ok("hb_state singleton tracks tick + last_at",
     main_src:find("hb_state", 1, true) ~= nil)

  -- Fallback path for missing action_server
  ok("heartbeat-only fallback present",
     main_src:find("heartbeat%-only fallback") ~= nil
     or main_src:find("action_server unavailable") ~= nil)
  ok("fallback runs nanosleep loop",
     main_src:find("ffi.C.nanosleep", 1, true) ~= nil)
end

------------------------------------------------------------------------
print()
print(string.format("SUMMARY: %d passed, %d failed", pass, fail))
os.exit(fail > 0 and 1 or 0)
