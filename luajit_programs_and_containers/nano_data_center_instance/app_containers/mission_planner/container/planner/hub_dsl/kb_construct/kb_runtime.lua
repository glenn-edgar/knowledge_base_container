--[[
    kb_runtime.lua -- Runtime KB reader/writer for status + stream tables.

    Used by the controller (remote) and hub to update robot state
    during execution. Separate from kb_query (which is read-only for config).

    NOTE: Robot instances are no longer pre-created in the KB. All writes
    are best-effort — if the path doesn't exist, writes are silently
    skipped. Live telemetry flows via NATS KV instead.

    Status table: single row per robot, current snapshot
    Stream table: circular buffer of heartbeat records

    A.3.5 SIGNATURE NOTE: arg 1 is now a pg_conn TABLE (not a sqlite path
    string) so KBM.new in v3 (pg-only) accepts it. The BODY of this module
    is still sqlite-coded (self.kb.db, sqlite3_helpers); first mission
    dispatch will crash at status/stream writes. Porting this body to pg
    KBM ltree writes is deferred — it gates the V-heavy completion path
    (B.2.A.5) but not A.3.5's instantiation + NATS-subscribe smoke.

    Usage:
        local kb_rt = require("kb_runtime")
        local rt = kb_rt.new(pg_conn, "moonbase.alpha.surface_ops", "rover_1")

        -- Update status (overwrites current snapshot)
        rt:update_status({
            active_kb = "init_check",
            active_worker = "worker_init_check",
            global_x = 800, global_y = 0, global_heading = 0,
            connected = true,
        })

        -- Read current status
        local status = rt:read_status()

        rt:close()
]]

local KBM = require("knowledge_base_manager")
local h = require("sqlite3_helpers")
local sql_query = h.sql_query
local sql_exec  = h.sql_exec
local json      = h.json

local M = {}
M.__index = M

function M.new(pg_conn, site, instance_name, database)
    assert(type(pg_conn) == "table",
           "kb_runtime.new: pg_conn must be a table " ..
           "{host, port, dbname, user, password}")
    local self = setmetatable({}, M)
    database = database or "knowledge_base"
    self.kb = KBM.new(database, pg_conn, true)  -- upload_flag=true (read-only schema)
    self.db = self.kb.db   -- TODO(A.3.5 deferred): pg KBM has no .db; body still sqlite-coded
    self.database = database
    self.site = site
    self.instance = instance_name
    self.status_table = database .. "_status"
    self.stream_table = database .. "_stream"

    -- Pre-compute paths (lowercase namespace convention)
    self.status_path = site .. ".robots." .. instance_name ..
        ".status.state"
    self.connection_path = site .. ".robots." .. instance_name ..
        ".status.connection"
    self.stream_path = site .. ".robots." .. instance_name ..
        ".stream.telemetry"

    return self
end

function M:close()
    self.kb:disconnect()
end

---------------------------------------------------------------------------
-- STATUS: read/write current robot state
---------------------------------------------------------------------------

function M:read_status()
    local rows = sql_query(self.db,
        string.format("SELECT data FROM %s WHERE path = ?", self.status_table),
        { self.status_path })
    if #rows > 0 and rows[1].data then
        local ok, d = pcall(json.decode, rows[1].data)
        if ok then return d end
    end
    return {}
end

function M:update_status(state_data)
    local json_str = json.encode(state_data)
    sql_exec(self.db, string.format(
        "UPDATE %s SET data = '%s' WHERE path = '%s'",
        self.status_table, json_str:gsub("'", "''"), self.status_path))
end

-- Merge fields into existing status (partial update)
function M:merge_status(fields)
    local current = self:read_status()
    for k, v in pairs(fields) do
        current[k] = v
    end
    self:update_status(current)
end

function M:read_connection()
    local rows = sql_query(self.db,
        string.format("SELECT data FROM %s WHERE path = ?", self.status_table),
        { self.connection_path })
    if #rows > 0 and rows[1].data then
        local ok, d = pcall(json.decode, rows[1].data)
        if ok then return d end
    end
    return {}
end

---------------------------------------------------------------------------
-- STREAM: circular buffer of heartbeat records
---------------------------------------------------------------------------

function M:write_heartbeat(heartbeat_data)
    -- Find the oldest row (lowest recorded_at) with valid=0, or oldest valid=1
    -- Update it with new data — circular buffer pattern
    local rows = sql_query(self.db,
        string.format([[
            SELECT id FROM %s
            WHERE path = ?
            ORDER BY
                valid ASC,
                recorded_at ASC
            LIMIT 1
        ]], self.stream_table),
        { self.stream_path })

    if #rows > 0 then
        local json_str = json.encode(heartbeat_data)
        sql_exec(self.db, string.format(
            "UPDATE %s SET data = '%s', valid = 1, recorded_at = datetime('now') WHERE id = %d",
            self.stream_table, json_str:gsub("'", "''"), rows[1].id))
    end
end

function M:read_heartbeats(count)
    count = count or 10
    local rows = sql_query(self.db,
        string.format([[
            SELECT data, recorded_at FROM %s
            WHERE path = ? AND valid = 1
            ORDER BY recorded_at DESC
            LIMIT ?
        ]], self.stream_table),
        { self.stream_path, count })

    local result = {}
    for _, row in ipairs(rows) do
        if row.data then
            local ok, d = pcall(json.decode, row.data)
            if ok then
                d._recorded_at = row.recorded_at
                result[#result + 1] = d
            end
        end
    end
    return result
end

-- Get count of valid heartbeat records
function M:heartbeat_count()
    local rows = sql_query(self.db,
        string.format("SELECT COUNT(*) as cnt FROM %s WHERE path = ? AND valid = 1",
            self.stream_table),
        { self.stream_path })
    if #rows > 0 then return rows[1].cnt end
    return 0
end

return M
