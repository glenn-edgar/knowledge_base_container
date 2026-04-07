--[[
    bootstrap_container.lua -- Container startup: discover infra, build KB, launch planner.

    Reads the extracted SQLite DB (bind-mounted from host) to discover
    infrastructure services (NATS, MQTT host:port) and domain site name.
    Builds runtime KB, exports to NATS, then execs planner_server.lua.

    The extracted SQLite is the single source of truth for infrastructure.
    Env vars are optional overrides.

    Usage:
      SQLITE_DB=/data/surface_ops.db luajit bootstrap_container.lua
]]

local kb_query = require("kb_query")

local extracted_db = os.getenv("SQLITE_DB")
if not extracted_db then
    io.stderr:write("ERROR: SQLITE_DB env var not set\n")
    os.exit(1)
end

local f = io.open(extracted_db, "r")
if not f then
    io.stderr:write("ERROR: SQLite DB not found: " .. extracted_db .. "\n")
    os.exit(1)
end
f:close()

---------------------------------------------------------------------------
-- Query extracted SQLite for infrastructure
---------------------------------------------------------------------------
local q = kb_query.new(extracted_db)
local infra = q:get_infrastructure()
local domain = q:get_domain()
q:close()

local site = os.getenv("VMRT_KB_SITE") or (domain and domain.site)
local mqtt_host = os.getenv("MQTT_HOST") or (infra.mqtt and infra.mqtt.host)
local mqtt_port = os.getenv("MQTT_PORT") or (infra.mqtt and tostring(infra.mqtt.port))
local nats_host = infra.nats and infra.nats.host
local nats_port = infra.nats and infra.nats.port
local nats_server = os.getenv("NATS_SERVER")
    or (nats_host and nats_port and string.format("nats://%s:%d", nats_host, nats_port))

if not site then io.stderr:write("ERROR: KB missing domain site\n"); os.exit(1) end
if not mqtt_host then io.stderr:write("ERROR: KB missing MQTT host\n"); os.exit(1) end
if not mqtt_port then io.stderr:write("ERROR: KB missing MQTT port\n"); os.exit(1) end
if not nats_server then io.stderr:write("ERROR: KB missing NATS server\n"); os.exit(1) end

io.stdout:setvbuf("line")
print("=== ROS Planner Container ===")
print(string.format("  Site: %s", site))
print(string.format("  NATS: %s", nats_server))
print(string.format("  MQTT: %s:%s", mqtt_host, mqtt_port))
print(string.format("  DB:   %s", extracted_db))
io.stdout:flush()

---------------------------------------------------------------------------
-- Planner data is now in the extracted SQLite DB (merged by master_build.lua).
-- No separate runtime KB build needed.
---------------------------------------------------------------------------
local script_dir = debug.getinfo(1, "S").source:match("^@(.*/)")  or "./"

local function file_exists(path)
    local fh = io.open(path, "r")
    if fh then fh:close(); return true end
    return false
end

---------------------------------------------------------------------------
-- Build remote.json if missing (hub.json no longer needed — state machine)
---------------------------------------------------------------------------
if not file_exists(script_dir .. "mqtt_robot/remote.json") then
    print("Building remote.json...")
    os.execute(string.format("cd '%smqtt_robot' && luajit remote_dsl.lua remote.json", script_dir))
end

---------------------------------------------------------------------------
-- Export KB to NATS KV (retry up to 3 times — NATS may still be starting)
---------------------------------------------------------------------------
print("\nExporting KB to NATS...")
local ffi = require("ffi")
ffi.cdef[[
    int setenv(const char *name, const char *value, int overwrite);
    unsigned int sleep(unsigned int seconds);
]]

local kb_exporter = require("kb_exporter")
local export_ok, stats
for attempt = 1, 3 do
    export_ok, stats = pcall(kb_exporter.export, {
        db_file     = extracted_db,
        nats_server = nats_server,
        bucket      = "kb_export",
    })
    if export_ok and stats then
        print(string.format("Exported: %d keys", stats.keys_written or 0))
        break
    end
    if attempt < 3 then
        print(string.format("  Export attempt %d failed, retrying in 2s...", attempt))
        ffi.C.sleep(2)
    else
        print("WARNING: KB export failed after 3 attempts (NATS may not be available)")
    end
end

---------------------------------------------------------------------------
-- Set env vars and run planner_server.lua in-process
---------------------------------------------------------------------------
print("\nStarting planner server...")
io.stdout:flush()

-- Set env vars so planner_server.lua sees the discovered infrastructure
ffi.C.setenv("NATS_SERVER", nats_server, 0)
ffi.C.setenv("MQTT_HOST", mqtt_host, 0)
ffi.C.setenv("MQTT_PORT", mqtt_port, 0)
ffi.C.setenv("VMRT_KB_SITE", site, 0)

dofile(script_dir .. "planner_server.lua")
