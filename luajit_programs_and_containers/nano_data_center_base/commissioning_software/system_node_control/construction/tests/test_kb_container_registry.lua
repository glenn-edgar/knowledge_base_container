#!/usr/bin/env luajit
-- =============================================================================
-- test_kb_container_registry.lua -- Roundtrip smoke test for the registry.
--
-- Uses topology.lua for pg connect info. Writes two fake rows under the
-- master CPU's CONTAINER_REGISTRY namespace, lists/verifies them, deletes
-- one, verifies one remains, then RECONCILE'd with an empty expected-set
-- which must delete the other. Any stray rows from a previous crashed run
-- are cleaned on startup and shutdown.
--
-- Run via ./run_test_kb_container_registry.sh (sets up LUA_PATH + secrets).
-- =============================================================================

local pgc  = require("pg_connector")
local kbcr = require("kb_container_registry")

local function load_catalog(filename)
  local src = debug.getinfo(1, "S").source
  if src:sub(1, 1) == "@" then src = src:sub(2) end
  local dir = src:match("(.*/)") or "./"
  -- tests/ -> ../catalogs/<filename>
  local chunk, err = loadfile(dir .. "../catalogs/" .. filename)
  if not chunk then error("load " .. filename .. ": " .. tostring(err)) end
  return chunk()
end

local TOPOLOGY = load_catalog("topology.lua")
local SITE     = TOPOLOGY.site
local CPU      = TOPOLOGY.master
assert(SITE and CPU, "topology.site / topology.master missing")

local password = os.getenv("POSTGRES_PASSWORD") or os.getenv("PG_PASSWORD")
assert(password and password ~= "",
       "POSTGRES_PASSWORD not in env -- source secrets.env first")

local cfg = {
  pg_host = TOPOLOGY.pg_connect.host,
  pg_port = TOPOLOGY.pg_connect.port,
  pg_db   = TOPOLOGY.pg_connect.dbname,
  pg_user = TOPOLOGY.pg_connect.user,
}
local conn, err = pgc.try_connect(cfg, password)
assert(conn, "pg connect: " .. tostring(err))
print(string.format("connected: %s@%s:%s/%s",
                    cfg.pg_user, cfg.pg_host, cfg.pg_port, cfg.pg_db))

local NAMES = { "test_kbcr_a", "test_kbcr_b" }

-- Precautionary cleanup in case a prior run crashed mid-test.
local function cleanup()
  for _, n in ipairs(NAMES) do
    kbcr.deregister(conn, SITE, CPU, n)
  end
end
cleanup()

local function run()
  -- REGISTER A with real port records.
  local ok, rerr = kbcr.register(conn, SITE, CPU, "test_kbcr_a",
    { definition = "test_app", category = "application" },
    { host = "cpu_01",
      image = "nanodatacenter/test-app:latest",
      ports = {
        { slot        = "exceptions_ui",
          internal    = 8080,
          external    = 19001,
          protocol    = "tcp",
          purpose     = "ui",
          description = "Exception aggregation viewer" },
        { slot        = "logs_ui",
          internal    = 8081,
          external    = 19002,
          protocol    = "tcp",
          purpose     = "ui",
          description = "Log aggregation viewer" },
      },
      description = "smoke test A" })
  assert(ok, "register A: " .. tostring(rerr))

  -- REGISTER B minimal.
  ok, rerr = kbcr.register(conn, SITE, CPU, "test_kbcr_b",
    { definition = "test_app", category = "application" },
    { host = "cpu_01", ports = {}, description = "smoke test B" })
  assert(ok, "register B: " .. tostring(rerr))

  -- LIST should return both.
  local rows, lerr = kbcr.list_by_cpu(conn, SITE, CPU)
  assert(rows, "list_by_cpu 1: " .. tostring(lerr))
  local by_name = {}
  for _, r in ipairs(rows) do by_name[r.name] = r end
  assert(by_name["test_kbcr_a"],     "A missing from list")
  assert(by_name["test_kbcr_b"],     "B missing from list")
  assert(by_name["test_kbcr_a"].properties.cpu_id     == CPU,         "A.cpu_id")
  assert(by_name["test_kbcr_a"].properties.definition == "test_app",  "A.def")
  assert(by_name["test_kbcr_a"].data.ports
         and by_name["test_kbcr_a"].data.ports[1]
         and by_name["test_kbcr_a"].data.ports[1].slot == "exceptions_ui",
         "A.ports[1].slot mismatch")
  assert(by_name["test_kbcr_a"].data.registered_at,
         "A.registered_at not set")
  print("register + list_by_cpu: ok (2 rows)")

  -- DEREGISTER A.
  ok, rerr = kbcr.deregister(conn, SITE, CPU, "test_kbcr_a")
  assert(ok, "deregister A: " .. tostring(rerr))

  rows, lerr = kbcr.list_by_cpu(conn, SITE, CPU)
  assert(rows, "list_by_cpu 2: " .. tostring(lerr))
  by_name = {}
  for _, r in ipairs(rows) do by_name[r.name] = r end
  assert(not by_name["test_kbcr_a"], "A still present after deregister")
  assert(by_name["test_kbcr_b"],     "B gone after deregister of A")
  print("deregister: ok (A gone, B remains)")

  -- RECONCILE with empty expected-set: deletes B.
  local deleted, rcerr = kbcr.reconcile(conn, SITE, CPU, {})
  assert(deleted, "reconcile: " .. tostring(rcerr))
  assert(deleted == 1,
         string.format("reconcile expected 1 delete, got %d", deleted))
  print("reconcile: ok (1 row cleaned)")

  rows = kbcr.list_by_cpu(conn, SITE, CPU)
  for _, r in ipairs(rows) do
    if r.name == "test_kbcr_a" or r.name == "test_kbcr_b" then
      error("row " .. r.name .. " not cleaned by reconcile")
    end
  end
end

local ok, terr = pcall(run)
cleanup()
conn:close()

if not ok then
  io.stderr:write("FAIL: " .. tostring(terr) .. "\n")
  os.exit(1)
end
print("PASS: kb_container_registry roundtrip clean")
