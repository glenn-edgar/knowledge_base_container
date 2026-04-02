--[[
    kb_runtime.lua -- Runtime KB reader/writer for status + stream tables.

    Used by the controller (remote) and hub to update robot state
    during execution. Separate from kb_query (which is read-only for config).

    Status table: single row per robot, current snapshot
    Stream table: circular buffer of heartbeat records

    Usage:
        local kb_rt = require("kb_runtime")
        local rt = kb_rt.new("surface_ops.db", "moonbase.alpha.surface_ops", "rover_1")

        -- Update status (overwrites current snapshot)
        rt:update_status({
            active_kb = "init_check",
            active_worker = "worker_init_check",
            global_x = 800, global_y = 0, global_heading = 0,
            connected = true,
        })

        -- Write heartbeat to stream (circular buffer)
        rt:write_heartbeat({
            delta_x = 50, delta_y = 0, delta_heading = 0,
            watchdog_ticks = 12, worker = "worker_path_segment",
        })

        -- Read current status
        local status = rt:read_status()

        -- Read last N heartbeats
        local beats = rt:read_heartbeats(10)

        rt:close()
]]

local KBM = require("knowledge_base_manager")
local h = require("sqlite3_helpers")
local sql_query = h.sql_query
local sql_exec  = h.sql_exec
local json      = h.json

local M = {}
M.__index = M

function M.new(db_file, site, instance_name, database)
    local self = setmetatable({}, M)
    database = database or "knowledge_base"
    self.kb = KBM.new(database, db_file, nil, true)  -- upload_flag=true (read-only schema)
    self.db = self.kb.db
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
