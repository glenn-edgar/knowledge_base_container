#!/usr/bin/env luajit
-- =============================================================================
-- test_driver.lua -- unit tests for apps_builder_framework/driver.lua.
--
-- Backed by the in-tree sqlite Construct_KB (no pg required). Tests:
--   1. happy path: 2 apps build cleanly under app_containers.<name>.*
--   2. spec validation: malformed container_spec aborts before kb_build runs
--   3. atomic failure: erroring kb_build halts further builds
--   4. scope confinement: kb_build that opens a header without leaving
--      causes a scope-corruption error from the driver
--   5. duplicate instance_id rejected
--   6. idempotence: re-running with a fresh DB produces identical paths
--
-- Run:
--   cd nano_data_center_base/commissioning_software/apps_builder_framework
--   luajit tests/test_driver.lua
-- =============================================================================

local SCRIPT_DIR = (arg[0] or ""):match("(.+)/[^/]+$") or "."
package.path = SCRIPT_DIR .. "/../?.lua;"
            .. SCRIPT_DIR .. "/../../kb/sqlite3/construct_kb/?.lua;"
            .. package.path

local Construct_KB = require("construct_kb")
local driver       = require("driver")

---------------------------------------------------------------------------
-- helpers
---------------------------------------------------------------------------

local TMP_DB = "/tmp/test_apps_builder_framework.db"

local function fresh_kb()
  os.remove(TMP_DB)
  local ltree_so = SCRIPT_DIR .. "/../../kb/sqlite3/construct_kb/ltree"
  local kb = Construct_KB.new(TMP_DB, "knowledge_base", ltree_so, false)
  -- Same shape build_kb.lua uses post-M-2: prefix = { "system", SYSTEM_NAME }
  -- so paths root at system.<sys>.*; logical kb_name stays "system".
  kb:add_kb("system", "test KB", { "system", "moon_base" })
  kb:select_kb("system")
  -- Open the site scope so the driver writes under
  -- system.moon_base.site.moon_base_alpha.app_containers.<name>.*.
  kb:add_header_node("site", "moon_base_alpha",
    { site_type = "dcs" }, {}, "site")
  return kb
end

local function close_kb(kb)
  -- Close the site header before disconnect. with_header in tests would
  -- pop automatically, but here we open it imperatively.
  pcall(function() kb:leave_header_node("site", "moon_base_alpha") end)
  kb:disconnect()
end

local function rows_with_path_prefix(kb, prefix)
  local sdb = kb:get_db_objects()
  local h = require("sqlite3_helpers")
  return h.sql_query(sdb,
    "SELECT path, label, name FROM knowledge_base WHERE path LIKE ? ORDER BY path",
    { prefix .. "%" })
end

local pass_count, fail_count = 0, 0

local function expect(cond, msg)
  if cond then
    pass_count = pass_count + 1
    print("  PASS: " .. msg)
  else
    fail_count = fail_count + 1
    print("  FAIL: " .. msg)
  end
end

---------------------------------------------------------------------------
-- fixtures
---------------------------------------------------------------------------

local SPEC_OK = {
  class = "irrigation_controller",
  image = "nanodatacenter/irrigation_controller:latest",
  kind  = "application",
  port_spec = {
    ui = { internal = 8080, protocol = "tcp", purpose = "ui",
           description = "operator UI" },
  },
  env_required = { "PG_HOST", "PG_USER" },
  volumes = {},
}

local SPEC_BAD_KIND = {
  class = "x", image = "x", kind = "bogus",
}

-- A simple kb_build that emits one status field under the anchor.
local function kb_build_simple(ctx)
  -- Should write under app_containers.<instance_id>.*
  ctx.kb:add_info_node("KB_STATUS_FIELD", "version",
    { class = ctx.app_class }, { value = "1.0.0" }, "version stamp")
end

-- A kb_build that opens a header but forgets to leave (scope corruption).
local function kb_build_leaks_scope(ctx)
  ctx.kb:add_header_node("section", "leaky", {}, {}, "")
  -- intentionally no leave_header_node
end

-- A kb_build that errors.
local function kb_build_errors(ctx)
  error("intentional failure for atomic-fail test")
end

---------------------------------------------------------------------------
-- 1. happy path
---------------------------------------------------------------------------

print("=== 1. happy path: 2 apps build cleanly ===")
do
  local kb = fresh_kb()
  local placements = {
    { instance_id = "irrigation_01", app_class = "irrigation_controller",
      cpu = "cpu_02", role = "active", spec = SPEC_OK,
      kb_build = kb_build_simple },
    { instance_id = "irrigation_02", app_class = "irrigation_controller",
      cpu = "cpu_02", role = "passive", spec = SPEC_OK,
      kb_build = kb_build_simple },
  }
  local ok, err, count = driver.drive(kb, kb, placements)
  expect(ok, "drive returns ok")
  expect(err == nil, "no error: " .. tostring(err))
  expect(count == 2, "built 2 placements (got " .. tostring(count) .. ")")

  local rows = rows_with_path_prefix(kb,
    "system.moon_base.site.moon_base_alpha.app_containers.")
  -- Each placement emits: 1 anchor header + 1 info_node = 2 rows
  expect(#rows == 4, "got 4 rows under app_containers (2 headers + 2 fields), saw " .. #rows)
  local saw_01, saw_02
  for _, r in ipairs(rows) do
    if r.path:match("app_containers%.irrigation_01%.KB_STATUS_FIELD%.version$") then saw_01 = true end
    if r.path:match("app_containers%.irrigation_02%.KB_STATUS_FIELD%.version$") then saw_02 = true end
  end
  expect(saw_01, "irrigation_01 version field present")
  expect(saw_02, "irrigation_02 version field present")

  close_kb(kb)
end

---------------------------------------------------------------------------
-- 2. spec validation
---------------------------------------------------------------------------

print("=== 2. spec validation: bad spec aborts before kb_build runs ===")
do
  local kb = fresh_kb()
  local kb_build_ran = false
  local placements = {
    { instance_id = "bad_app_01", app_class = "x",
      cpu = "cpu_02", spec = SPEC_BAD_KIND,
      kb_build = function() kb_build_ran = true end },
  }
  local ok, err = driver.drive(kb, kb, placements)
  expect(not ok, "drive fails on bad spec")
  expect(err and err:find("kind must be one of"),
    "error mentions invalid kind (got: " .. tostring(err) .. ")")
  expect(not kb_build_ran, "kb_build was NOT invoked for the bad placement")
  close_kb(kb)
end

---------------------------------------------------------------------------
-- 3. atomic failure: erroring kb_build halts subsequent builds
---------------------------------------------------------------------------

print("=== 3. atomic failure: erroring kb_build halts further builds ===")
do
  local kb = fresh_kb()
  local third_ran = false
  local placements = {
    { instance_id = "good_app", app_class = "irrigation_controller",
      cpu = "cpu_02", spec = SPEC_OK, kb_build = kb_build_simple },
    { instance_id = "bad_app", app_class = "irrigation_controller",
      cpu = "cpu_02", spec = SPEC_OK, kb_build = kb_build_errors },
    { instance_id = "third_app", app_class = "irrigation_controller",
      cpu = "cpu_02", spec = SPEC_OK,
      kb_build = function() third_ran = true end },
  }
  local ok, err, count = driver.drive(kb, kb, placements)
  expect(not ok, "drive fails on erroring kb_build")
  expect(err and err:find("intentional failure"),
    "error propagates the original message (got: " .. tostring(err) .. ")")
  expect(count == 1, "exactly 1 placement built before failure (got " .. tostring(count) .. ")")
  expect(not third_ran, "third app was NOT invoked after the failure")
  close_kb(kb)
end

---------------------------------------------------------------------------
-- 4. scope confinement: kb_build that opens a header without leaving
--    triggers a scope-corruption error from the driver post-check.
---------------------------------------------------------------------------

print("=== 4. scope confinement: header-leak detected ===")
do
  local kb = fresh_kb()
  local placements = {
    { instance_id = "leaky_app", app_class = "x",
      cpu = "cpu_02", spec = SPEC_OK, kb_build = kb_build_leaks_scope },
  }
  local ok, err = driver.drive(kb, kb, placements)
  expect(not ok, "drive fails on scope-corrupting kb_build")
  -- The error may be either the depth-mismatch we explicitly check, or
  -- the underlying with_header's leave_header_node failing because the
  -- top of the stack doesn't match. EITHER one means scope confinement
  -- is enforced.
  expect(err ~= nil, "got an error message")
  if err then
    print("    (scope-error message: " .. err:sub(1, 120) .. ")")
  end
  close_kb(kb)
end

---------------------------------------------------------------------------
-- 5. duplicate instance_id rejected pre-build
---------------------------------------------------------------------------

print("=== 5. duplicate instance_id rejected ===")
do
  local kb = fresh_kb()
  local invocations = 0
  local placements = {
    { instance_id = "dup_app", app_class = "irrigation_controller",
      cpu = "cpu_02", spec = SPEC_OK,
      kb_build = function() invocations = invocations + 1 end },
    { instance_id = "dup_app", app_class = "irrigation_controller",
      cpu = "cpu_02", spec = SPEC_OK,
      kb_build = function() invocations = invocations + 1 end },
  }
  local ok, err = driver.drive(kb, kb, placements)
  expect(not ok, "drive fails on duplicate instance_id")
  expect(err and err:find("duplicate instance_id"),
    "error mentions duplicate (got: " .. tostring(err) .. ")")
  expect(invocations == 0,
    "no kb_build invoked when duplicate detected pre-build (saw " .. invocations .. ")")
  close_kb(kb)
end

---------------------------------------------------------------------------
-- 6. idempotence: a fresh build produces identical paths
---------------------------------------------------------------------------

print("=== 6. idempotence: fresh build is path-identical ===")
do
  local function build_and_collect()
    local kb = fresh_kb()
    local placements = {
      { instance_id = "irrigation_01", app_class = "irrigation_controller",
        cpu = "cpu_02", role = "active", spec = SPEC_OK,
        kb_build = kb_build_simple },
    }
    local ok, err = driver.drive(kb, kb, placements)
    assert(ok, "build failed: " .. tostring(err))
    local rows = rows_with_path_prefix(kb,
      "system.moon_base.site.moon_base_alpha.app_containers.")
    local paths = {}
    for _, r in ipairs(rows) do paths[#paths + 1] = r.path end
    table.sort(paths)
    close_kb(kb)
    return table.concat(paths, "\n")
  end
  local snap1 = build_and_collect()
  local snap2 = build_and_collect()
  expect(snap1 == snap2, "two fresh builds emit identical paths")
end

---------------------------------------------------------------------------
-- summary
---------------------------------------------------------------------------

print("")
print(string.format("=== %d passed, %d failed ===", pass_count, fail_count))
os.remove(TMP_DB)
os.exit(fail_count == 0 and 0 or 1)
