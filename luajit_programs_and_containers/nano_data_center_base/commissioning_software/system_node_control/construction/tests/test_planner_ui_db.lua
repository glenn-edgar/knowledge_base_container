#!/usr/bin/env luajit
-- =============================================================================
-- test_planner_ui_db.lua -- Phase 5b C2 acceptance for db.lua SQL
-- helpers.
--
-- Coverage:
--   db.boards_namespace: pulls APP_SYSTEM/APP_SITE; errors on missing
--   db.connect: stubbed via package.preload (pgmoon) so we don't need
--     a real pg or OpenResty cosocket
--   db.list_boards: SQL targets fs_node + fs_blob, namespace built
--     from env, hex-encodes sha256, kind='file' filter
--   db.get_board:
--     - input validation (name required, must match [%w_]+)
--     - SQL for the join targets the right path
--     - found row -> returns content + sha256_hex
--     - missing row -> "board not found: <name>"
--   db.list_active_nodes: SQL targets knowledge_base table with
--     label='active_node_def' filter under the right namespace
--
-- The handlers (api_*.lua) require ngx at runtime; verify parse-load
-- only here. Full HTTP behavior is verified by the cluster smoke
-- (curl /api/* against a built container).
-- =============================================================================

local SCRIPT_DIR = arg[0]:match("(.*/)") or "./"
local REPO_ROOT  = SCRIPT_DIR .. "../../../../../"
local PUI        = REPO_ROOT
    .. "nano_data_center_instance/app_containers/mission_planner/container/planner_ui"
package.path = PUI .. "/lua/?.lua;" .. package.path

local pass, fail = 0, 0
local function ok(name, cond, detail)
  if cond then pass = pass + 1; print("  ok  " .. name)
  else fail = fail + 1; print("  FAIL " .. name .. (detail and " -- " .. detail or "")) end
end

------------------------------------------------------------------------
-- env manipulation
------------------------------------------------------------------------
local ffi = require("ffi")
pcall(ffi.cdef, [[
  int setenv(const char *name, const char *value, int overwrite);
  int unsetenv(const char *name);
]])
local function set_env(name, val)  pcall(ffi.C.setenv, name, val, 1)  end
local function clear_env(name)     pcall(ffi.C.unsetenv, name)        end
local function clear_pg_envs()
  for _, n in ipairs({ "APP_SYSTEM", "APP_SITE", "PG_HOST", "PG_PORT",
                       "PG_DB", "PG_USER", "PG_PASSWORD" }) do
    clear_env(n)
  end
end

------------------------------------------------------------------------
-- pgmoon stub: records queries + lets each test inject a result.
-- Installed via package.preload so the first require("pgmoon") in
-- db.lua picks it up.
------------------------------------------------------------------------
local STUB = { last_sql = nil, next_result = nil, next_err = nil,
               connected = false, keepalive_called = false }

local function reset_stub()
  STUB.last_sql        = nil
  STUB.next_result     = nil
  STUB.next_err        = nil
  STUB.connected       = false
  STUB.keepalive_called = false
end

package.preload["pgmoon"] = function()
  return {
    new = function(_opts)
      return {
        connect = function(self) STUB.connected = true; return true end,
        keepalive = function(self) STUB.keepalive_called = true; return true end,
        query = function(self, sql)
          STUB.last_sql = sql
          if STUB.next_err then return nil, STUB.next_err end
          return STUB.next_result or {}
        end,
        -- pgmoon's escape_literal wraps in single quotes + doubles any
        -- embedded single quotes. Mirror that for stub correctness.
        escape_literal = function(self, s)
          local esc = tostring(s):gsub("'", "''")
          return "'" .. esc .. "'"
        end,
      }
    end,
  }
end

local db = require("db")

------------------------------------------------------------------------
print("== db.boards_namespace ==")
------------------------------------------------------------------------

do
  clear_pg_envs()
  clear_env("PLANNER_NAMESPACE")
  local ns, err = db.boards_namespace()
  ok("missing env -> nil + err", ns == nil and err ~= nil,
     "got ns=" .. tostring(ns) .. " err=" .. tostring(err))

  -- APP_SYSTEM/APP_SITE present but PLANNER_NAMESPACE missing -> err
  set_env("APP_SYSTEM", "ros_planner_ii")
  set_env("APP_SITE",   "moonbase.alpha.surface_ops")
  ns, err = db.boards_namespace()
  ok("APP_* set but PLANNER_NAMESPACE missing -> err",
     ns == nil and err and err:find("PLANNER_NAMESPACE"),
     "got ns=" .. tostring(ns) .. " err=" .. tostring(err))

  set_env("PLANNER_NAMESPACE", "mission_planner_01")
  local ns2 = db.boards_namespace()
  ok("env set -> namespace string includes planner.<ns>",
     ns2 == "system.ros_planner_ii.site.moonbase.alpha.surface_ops" ..
            ".planner.mission_planner_01.boards",
     "got " .. tostring(ns2))
end

------------------------------------------------------------------------
print()
print("== db.connect ==")
------------------------------------------------------------------------

do
  reset_stub()
  local pg, err = db.connect()
  ok("connect succeeds via stub", pg ~= nil, err)
  ok("stub recorded the connect", STUB.connected == true)
end

------------------------------------------------------------------------
print()
print("== db.list_boards ==")
------------------------------------------------------------------------

do
  reset_stub()
  set_env("APP_SYSTEM", "ros_planner_ii")
  set_env("APP_SITE",   "moonbase.alpha.surface_ops")
  STUB.next_result = {
    { name = "landing_zone", sha256_hex = "abc123",
      updated_at = "2026-05-09T10:00:00", size = 1234 },
    { name = "habitat",      sha256_hex = "def456",
      updated_at = "2026-05-09T10:01:00", size = 5678 },
  }
  local pg = db.connect()
  local rows, err = db.list_boards(pg)
  ok("list_boards returned rows", rows ~= nil, err)
  ok("two rows", #rows == 2)
  ok("first row carries expected fields",
     rows[1].name == "landing_zone" and rows[1].sha256_hex == "abc123")

  -- SQL shape checks: targets the right tables + filters
  ok("SQL references knowledge_base_fs_node",
     STUB.last_sql:find("knowledge_base_fs_node", 1, true) ~= nil)
  ok("SQL references knowledge_base_fs_blob",
     STUB.last_sql:find("knowledge_base_fs_blob", 1, true) ~= nil)
  ok("SQL filters kind='file'",
     STUB.last_sql:find("kind = 'file'", 1, true) ~= nil)
  ok("SQL hex-encodes sha256",
     STUB.last_sql:find("encode(b.sha256, 'hex')", 1, true) ~= nil)
  ok("SQL embeds the boards namespace (with planner.<ns>)",
     STUB.last_sql:find(
       "system.ros_planner_ii.site.moonbase.alpha.surface_ops" ..
       ".planner.mission_planner_01.boards",
       1, true) ~= nil,
     "SQL: " .. (STUB.last_sql or "(nil)"):sub(1, 300))
end

do
  -- error path
  reset_stub()
  STUB.next_err = "connection lost"
  local pg = db.connect()
  local rows, err = db.list_boards(pg)
  ok("list_boards error -> nil + err",
     rows == nil and err and err:find("list_boards"))
end

------------------------------------------------------------------------
print()
print("== db.get_board: input validation ==")
------------------------------------------------------------------------

do
  reset_stub()
  local pg = db.connect()

  local r, err = db.get_board(pg, nil)
  ok("nil name rejected", r == nil and err == "get_board: name required")

  r, err = db.get_board(pg, "")
  ok("empty name rejected", r == nil and err == "get_board: name required")

  r, err = db.get_board(pg, "../etc/passwd")
  ok("path-traversal rejected (slash)",
     r == nil and err and err:find("invalid name"))

  r, err = db.get_board(pg, "name with space")
  ok("space rejected",
     r == nil and err and err:find("invalid name"))

  r, err = db.get_board(pg, "valid_name_123")
  -- valid name reaches the SQL stage; STUB.next_result is nil so the
  -- helper returns "board not found"
  ok("valid name reaches SQL (no validation rejection)",
     r == nil and err and err:find("board not found"))
end

------------------------------------------------------------------------
print()
print("== db.get_board: SQL + result handling ==")
------------------------------------------------------------------------

do
  reset_stub()
  set_env("APP_SYSTEM", "ros_planner_ii")
  set_env("APP_SITE",   "moonbase.alpha.surface_ops")
  STUB.next_result = {
    { content    = '{"schema_version":1,"name":"x"}',
      sha256_hex = "deadbeef",
      size       = 32 },
  }
  local pg = db.connect()
  local content, sha = db.get_board(pg, "landing_zone")
  ok("get_board returned content",
     content == '{"schema_version":1,"name":"x"}', content)
  ok("get_board returned sha256_hex",
     sha == "deadbeef", "got " .. tostring(sha))

  ok("SQL targets the specific board path (with planner.<ns>)",
     STUB.last_sql:find(
       "system.ros_planner_ii.site.moonbase.alpha.surface_ops" ..
       ".planner.mission_planner_01.boards.landing_zone",
       1, true) ~= nil,
     "SQL: " .. (STUB.last_sql or "(nil)"):sub(1, 300))
end

do
  -- missing board
  reset_stub()
  set_env("APP_SYSTEM", "ros_planner_ii")
  set_env("APP_SITE",   "moonbase.alpha.surface_ops")
  STUB.next_result = {}  -- no row matched
  local pg = db.connect()
  local content, err = db.get_board(pg, "nonexistent")
  ok("missing board -> nil + 'board not found'",
     content == nil and err and err:find("board not found"))
end

------------------------------------------------------------------------
print()
print("== db.list_active_nodes ==")
------------------------------------------------------------------------

do
  reset_stub()
  set_env("APP_SYSTEM", "ros_planner_ii")
  set_env("APP_SITE",   "moonbase.alpha.surface_ops")
  STUB.next_result = {
    { path = "system.ros_planner_ii.site.moonbase.alpha.surface_ops"
          .. ".infrastructure.registry.active_node_def.dock_v1",
      name = "dock_v1",
      data = '{"action_id":"recharge"}' },
  }
  local pg = db.connect()
  local rows, err = db.list_active_nodes(pg)
  ok("list_active_nodes returned rows", rows ~= nil, err)
  ok("one row", #rows == 1)
  ok("name extracted", rows[1].name == "dock_v1")

  ok("SQL targets knowledge_base table",
     STUB.last_sql:find("FROM knowledge_base", 1, true) ~= nil)
  ok("SQL filters label='active_node_def'",
     STUB.last_sql:find("label = 'active_node_def'", 1, true) ~= nil)
  ok("SQL embeds the registry namespace",
     STUB.last_sql:find(
       "infrastructure.registry.active_node_def", 1, true) ~= nil)
end

------------------------------------------------------------------------
print()
print("== handler files: parse-load (no execute) ==")
------------------------------------------------------------------------

do
  for _, name in ipairs({
    "api.lua", "api_boards.lua", "api_board.lua", "api_active_nodes.lua",
  }) do
    local chunk, err = loadfile(PUI .. "/lua/" .. name)
    ok(name .. " parses cleanly",
       chunk ~= nil, err and tostring(err) or "")
  end

  -- Cluster-smoke regression guard (2026-05-10): pgmoon over the
  -- OpenResty cosocket needs an nginx `resolver` directive to
  -- resolve hostnames like host.docker.internal / pg-vector. Without
  -- it, every /api/* endpoint returns 503 with "no resolver defined".
  -- Stub-based tests don't catch this -- it surfaces only on a real
  -- container boot. Assert the directive is present so the
  -- regression can't slip again.
  local f = io.open(PUI .. "/conf/nginx.conf", "rb")
  local nginx_conf = f and f:read("*a") or ""
  if f then f:close() end
  ok("nginx.conf has `resolver` directive (cluster-smoke regression)",
     nginx_conf:find("resolver%s+%d", 1, false) ~= nil
     or nginx_conf:find("resolver 127.0.0.11", 1, true) ~= nil,
     "missing -- pgmoon cosocket connect will 503 on hostnames")
end

------------------------------------------------------------------------
print()
print(string.format("SUMMARY: %d passed, %d failed", pass, fail))
os.exit(fail > 0 and 1 or 0)
