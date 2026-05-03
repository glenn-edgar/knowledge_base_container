#!/usr/bin/env luajit
-- =============================================================================
-- test_infra_discovery.lua -- unit tests for the app-side helper.
--
-- Uses a minimal mock pg connection (table-based) instead of a live db.
-- Tests cover: happy lookup, missing row, unhealthy gating, staleness
-- gating, opts.require_healthy=false bypass, NATS/MQTT convenience helpers.
-- =============================================================================

local SCRIPT_DIR = (arg[0] or ""):match("(.+)/[^/]+$") or "."
package.path = SCRIPT_DIR .. "/../prebuilt_lua_share/?.lua;"
            .. "/usr/local/share/lua/5.1/?.lua;"
            .. package.path

local cjson = require("dkjson")
local ndc_paths = require("ndc_paths")
ndc_paths.configure{ system_name = "moon_base" }

local infra = require("infra_discovery")

local pass, fail = 0, 0
local function expect(cond, msg)
  if cond then pass = pass + 1; print("  PASS: " .. msg)
  else fail = fail + 1; print("  FAIL: " .. msg) end
end

---------------------------------------------------------------------------
-- Mock pg connection that returns canned rows keyed by SQL substring.
---------------------------------------------------------------------------

local function make_mock(rows)
  -- rows: array of { match = <substring>, data = <table> }
  return {
    prepare = function(self, sql)
      local row = nil
      for _, r in ipairs(rows) do
        if sql:find(r.match, 1, true) then
          row = r.data
          break
        end
      end
      return {
        execute = function() return true end,
        fetch   = function() return row and { data = cjson.encode(row) } or nil end,
        close   = function() end,
      }
    end,
  }
end

---------------------------------------------------------------------------
-- 1. happy lookup (healthy NATS, fresh)
---------------------------------------------------------------------------
print("=== 1. healthy NATS ===")
do
  local now = os.time()
  local pg = make_mock({
    { match = "nats.KB_STATUS_FIELD.host",      data = { value = "nats-js-ram" } },
    { match = "nats.KB_STATUS_FIELD.port",      data = { value = 4222 } },
    { match = "nats.KB_STATUS_FIELD.protocol",  data = { value = "tcp" } },
    { match = "nats.KB_STATUS_FIELD.healthy",   data = { value = true } },
    { match = "nats.KB_STATUS_FIELD.last_seen", data = { value = now - 2 } },
  })
  local r, err = infra.lookup(pg, "moon_base_alpha", "nats")
  expect(r ~= nil, "lookup returns table (err=" .. tostring(err) .. ")")
  if r then
    expect(r.host == "nats-js-ram", "host = nats-js-ram (got " .. tostring(r.host) .. ")")
    expect(r.port == 4222, "port = 4222")
    expect(r.protocol == "tcp", "protocol = tcp")
    expect(r.healthy == true, "healthy = true")
    expect(r.age_s <= 5, "age_s reasonable: " .. r.age_s)
  end
end

---------------------------------------------------------------------------
-- 2. unhealthy service rejected by default
---------------------------------------------------------------------------
print("=== 2. unhealthy NATS rejected (default require_healthy=true) ===")
do
  local pg = make_mock({
    { match = "nats.KB_STATUS_FIELD.host",      data = { value = "nats-js-ram" } },
    { match = "nats.KB_STATUS_FIELD.port",      data = { value = 4222 } },
    { match = "nats.KB_STATUS_FIELD.protocol",  data = { value = "tcp" } },
    { match = "nats.KB_STATUS_FIELD.healthy",   data = { value = false } },
    { match = "nats.KB_STATUS_FIELD.last_seen", data = { value = 0 } },
  })
  local r, err = infra.lookup(pg, "moon_base_alpha", "nats")
  expect(r == nil, "rejected (returns nil)")
  expect(err and err:find("not healthy", 1, true), "err mentions 'not healthy': " .. tostring(err))
end

---------------------------------------------------------------------------
-- 3. unhealthy bypass with require_healthy=false
---------------------------------------------------------------------------
print("=== 3. unhealthy bypass with require_healthy=false ===")
do
  local pg = make_mock({
    { match = "nats.KB_STATUS_FIELD.host",      data = { value = "" } },
    { match = "nats.KB_STATUS_FIELD.port",      data = { value = 4222 } },
    { match = "nats.KB_STATUS_FIELD.protocol",  data = { value = "tcp" } },
    { match = "nats.KB_STATUS_FIELD.healthy",   data = { value = false } },
    { match = "nats.KB_STATUS_FIELD.last_seen", data = { value = 0 } },
  })
  local r, err = infra.lookup(pg, "moon_base_alpha", "nats",
    { require_healthy = false })
  expect(r ~= nil, "returned despite unhealthy")
  if r then expect(r.healthy == false, "healthy still reported as false") end
end

---------------------------------------------------------------------------
-- 4. stale (last_seen too old)
---------------------------------------------------------------------------
print("=== 4. stale entry rejected (max_age_s=10) ===")
do
  local now = os.time()
  local pg = make_mock({
    { match = "nats.KB_STATUS_FIELD.host",      data = { value = "nats-js-ram" } },
    { match = "nats.KB_STATUS_FIELD.port",      data = { value = 4222 } },
    { match = "nats.KB_STATUS_FIELD.protocol",  data = { value = "tcp" } },
    { match = "nats.KB_STATUS_FIELD.healthy",   data = { value = true } },
    { match = "nats.KB_STATUS_FIELD.last_seen", data = { value = now - 60 } },
  })
  local r, err = infra.lookup(pg, "moon_base_alpha", "nats",
    { max_age_s = 10 })
  expect(r == nil, "rejected as stale")
  expect(err and err:find("stale", 1, true), "err mentions 'stale': " .. tostring(err))
end

---------------------------------------------------------------------------
-- 5. missing schema row
---------------------------------------------------------------------------
print("=== 5. unregistered service ===")
do
  local pg = make_mock({})  -- no rows; everything misses
  local r, err = infra.lookup(pg, "moon_base_alpha", "redis")
  expect(r == nil, "rejected (registry row missing)")
  expect(err and err:find("row not found", 1, true),
    "err mentions 'row not found': " .. tostring(err))
end

---------------------------------------------------------------------------
-- 6. nats_url convenience
---------------------------------------------------------------------------
print("=== 6. nats_url convenience helper ===")
do
  local now = os.time()
  local pg = make_mock({
    { match = "nats.KB_STATUS_FIELD.host",      data = { value = "nats-js-ram" } },
    { match = "nats.KB_STATUS_FIELD.port",      data = { value = 4222 } },
    { match = "nats.KB_STATUS_FIELD.protocol",  data = { value = "tcp" } },
    { match = "nats.KB_STATUS_FIELD.healthy",   data = { value = true } },
    { match = "nats.KB_STATUS_FIELD.last_seen", data = { value = now } },
  })
  local url, err = infra.nats_url(pg, "moon_base_alpha")
  expect(url == "nats://nats-js-ram:4222",
    "url = nats://nats-js-ram:4222 (got " .. tostring(url) .. ")")
end

---------------------------------------------------------------------------
-- 7. mqtt_addr convenience
---------------------------------------------------------------------------
print("=== 7. mqtt_addr convenience helper ===")
do
  local now = os.time()
  local pg = make_mock({
    { match = "mqtt.KB_STATUS_FIELD.host",      data = { value = "mosquitto-ram-ws_main" } },
    { match = "mqtt.KB_STATUS_FIELD.port",      data = { value = 1883 } },
    { match = "mqtt.KB_STATUS_FIELD.protocol",  data = { value = "tcp" } },
    { match = "mqtt.KB_STATUS_FIELD.healthy",   data = { value = true } },
    { match = "mqtt.KB_STATUS_FIELD.last_seen", data = { value = now } },
  })
  local host, port = infra.mqtt_addr(pg, "moon_base_alpha")
  expect(host == "mosquitto-ram-ws_main", "host = mosquitto-ram-ws_main")
  expect(port == 1883, "port = 1883")
end

print("")
print(string.format("=== %d passed, %d failed ===", pass, fail))
os.exit(fail == 0 and 0 or 1)
