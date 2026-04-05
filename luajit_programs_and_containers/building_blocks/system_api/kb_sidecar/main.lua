--[[
  main.lua — KB query sidecar entry point

  Standalone LuaJIT process that:
    1. Connects to Postgres via kb_data_structures (existing DBI facade)
    2. Starts a NATS RPC server listening on system_api.kb.query.*
    3. Routes requests to handlers that query Postgres and return JSON

  The sidecar keeps OpenResty non-blocking — all Postgres access goes
  through here over NATS request/reply.

  Usage:
    cd system_api/kb_sidecar
    luajit main.lua

  NATS subjects (prefix: system_api.kb.query):
    .domains     — list all domains (tab discovery)
    .robots      — list robots for a domain
    .containers  — list containers on a CPU
    .services    — list services for a container
    .node        — get a single node by path
    .subtree     — get all nodes under a path prefix

  Environment:
    NATS_URL      — NATS server (default nats://127.0.0.1:4222)
    PG_HOST       — Postgres host (default localhost)
    PG_PORT       — Postgres port (default 5432)
    PG_DBNAME     — database name (default knowledge_base)
    PG_USER       — database user (default gedgar)
    PG_PASSWORD   — database password (default "")
]]

-----------------------------------------------------------------------
-- Package paths
-----------------------------------------------------------------------
local kb_nats_lib = "../../knowledge_base/nats/?.lua"
local kb_nats_lib2 = "../../knowledge_base/nats/lib/?.lua"
local kb_pg_ds = "../../knowledge_base/postgres/data_structures/?.lua"
package.path = kb_nats_lib .. ";" .. kb_nats_lib2 .. ";" .. kb_pg_ds .. ";" .. package.path

-- C library path: LD_LIBRARY_PATH must include the nats .so directory
-- Use run.sh to launch with the correct LD_LIBRARY_PATH

-----------------------------------------------------------------------
-- Config
-----------------------------------------------------------------------
local nats_url = os.getenv("NATS_URL") or "nats://127.0.0.1:4222"

local pg_config = {
  host     = os.getenv("PG_HOST") or "localhost",
  port     = os.getenv("PG_PORT") or "5432",
  dbname   = os.getenv("PG_DBNAME") or "knowledge_base",
  database = os.getenv("PG_DBNAME") or "knowledge_base",
  user     = os.getenv("PG_USER") or "gedgar",
  password = os.getenv("PG_PASSWORD") or os.getenv("POSTGRES_PASSWORD") or "",
}

local RPC_PREFIX = "system_api.kb.query"

-----------------------------------------------------------------------
-- Connect to Postgres
-----------------------------------------------------------------------
print("KB Sidecar starting...")
print("  NATS: " .. nats_url)
print("  Postgres: " .. pg_config.host .. ":" .. pg_config.port
  .. "/" .. pg_config.dbname)

local KB_Data_Structures = require("kb_data_structures")
local kb = KB_Data_Structures.new(pg_config)
print("  Postgres connected.")

-----------------------------------------------------------------------
-- Create handlers
-----------------------------------------------------------------------
local handlers_mod = require("handlers")
local handlers = handlers_mod.new(kb)

-----------------------------------------------------------------------
-- Start NATS RPC server
-----------------------------------------------------------------------
local nats_rpc = require("nats_rpc_server")

local server = nats_rpc.RpcServer.new({
  server      = nats_url,
  instance_id = "kb_sidecar_01",
})

-- Register each handler
local method_count = 0
for method, handler_fn in pairs(handlers) do
  server:register(method, handler_fn)
  method_count = method_count + 1
  print("  Registered: " .. RPC_PREFIX .. "." .. method)
end

print(string.format("\nKB Sidecar ready — %d methods on prefix '%s'",
  method_count, RPC_PREFIX))
print("Press Ctrl+C to stop.\n")

-- Start subscribes (non-blocking), then wait blocks until stopped
server:start(RPC_PREFIX)
server:wait()

-- Cleanup
server:stop()
server:destroy()
kb:disconnect()
print("KB Sidecar stopped.")
