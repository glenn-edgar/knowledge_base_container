--[[
    mission.lua -- Mission observability: NATS JetStream for live, SQLite for durable bookends.

    Live telemetry flows through NATS JetStream KV:
      - Status snapshots → KeyStore key (current state, any tool can read)
      - Heartbeat stream → KeyStore circular buffer (persistent, survives restarts)
        Uses the same slot/meta pattern as nats_stream.StreamBuffer so external
        consumers can read with StreamBuffer from a separate process.

    Durable records go to SQLite via kb_runtime:
      - First heartbeat per virtual node (VN started)
      - Final heartbeat per virtual node (VN completed/failed)
      - Final mission status

    NATS subjects/keys match SQLite KB paths from DSL tree:
      status:    {site}.ROBOT_INSTANCE.{robot_id}.KB_STATUS_FIELD.state
      stream:    {site}.ROBOT_INSTANCE.{robot_id}.KB_STREAM_FIELD.telemetry

    Usage:
        local mission = require("mission")
        local m = mission.new({
            robot_id     = "rover_1",
            db_file      = "surface_ops.db",
            site         = "moonbase.alpha.surface_ops",
            nats_server  = "nats://127.0.0.1:4222",
            route_length = 18,
            stream_size  = 200,   -- circular buffer capacity (default 200)
        })

        m:start()
        m:action_start(1, action, pose)
        m:heartbeat(1, "path_spline", pose)
        m:action_complete(1, action, pose)
        m:finish(result)
        m:close()
]]

local json_util = require("json_util")

local M = {}
M.__index = M

---------------------------------------------------------------------------
-- Constructor
---------------------------------------------------------------------------
function M.new(opts)
    local self = setmetatable({}, M)

    self.robot_id     = opts.robot_id     or error("mission: robot_id required")
    local db_file     = opts.db_file      or error("mission: db_file required")
    local site        = opts.site         or error("mission: site required")
    local nats_server = opts.nats_server  or "nats://127.0.0.1:4222"
    self.route_length = opts.route_length or 0
    self.site         = site
    self.nats_server  = nats_server

    -- Build KB paths (same strings used as NATS keys)
    self.status_path = site .. ".ROBOT_INSTANCE." .. self.robot_id ..
        ".KB_STATUS_FIELD.state"
    self.stream_path = site .. ".ROBOT_INSTANCE." .. self.robot_id ..
        ".KB_STREAM_FIELD.telemetry"

    -- SQLite kb_runtime (durable bookends)
    local kb_runtime = require("kb_runtime")
    self.kb_rt = kb_runtime.new(db_file, site, self.robot_id)

    -- NATS KeyStore (status + abort signaling)
    local ks_lib = require("lib.nats_key_store")
    self.ks = ks_lib.KeyStore.new({
        server        = nats_server,
        bucket        = "mission_" .. self.robot_id,
        description   = "Mission status and abort signaling for " .. self.robot_id,
        create_bucket = true,
        history       = 1,
        client_name   = "mission_ks_" .. self.robot_id,
    })
    self.ks:connect()

    -- NATS KeyStore for stream (separate bucket, persistent circular buffer)
    -- Uses same slot/meta key pattern as nats_stream.StreamBuffer so external
    -- consumers can read with StreamBuffer.new(pubsub, ks, subject, size)
    self.stream_size = opts.stream_size or 200
    self.stream_ks = ks_lib.KeyStore.new({
        server        = nats_server,
        bucket        = "stream_" .. self.robot_id,
        description   = "Mission heartbeat stream for " .. self.robot_id,
        create_bucket = true,
        history       = 1,
        client_name   = "mission_stream_" .. self.robot_id,
    })
    self.stream_ks:connect()

    -- Stream circular buffer state (mirrors StreamBuffer internals)
    self._stream_prefix = self.stream_path:gsub("%.", "_")
    self._stream_head   = 0
    self._stream_count  = 0
    self._stream_total  = 0

    -- Abort key
    self.abort_key = "planner." .. self.robot_id .. ".abort"

    -- Clear any stale abort request
    pcall(function() self.ks:delete(self.abort_key) end)

    -- Heartbeat counter
    self.heartbeat_count = 0

    -- Mission start time
    self.start_time = nil

    return self
end

---------------------------------------------------------------------------
-- Stream circular buffer (write-through to JetStream KV)
-- Same key pattern as nats_stream.StreamBuffer for compatibility
---------------------------------------------------------------------------

function M:_stream_meta_key()
    return self._stream_prefix .. ".__meta"
end

function M:_stream_slot_key(slot_0based)
    return self._stream_prefix .. ".__" .. tostring(slot_0based)
end

function M:_stream_write(json_str)
    local slot_0 = self._stream_head % self.stream_size
    self._stream_head  = self._stream_head + 1
    self._stream_total = self._stream_total + 1
    if self._stream_count < self.stream_size then
        self._stream_count = self._stream_count + 1
    end

    -- Write slot data
    self.stream_ks:put(self:_stream_slot_key(slot_0), json_str)

    -- Write meta (compatible with StreamBuffer._load)
    self.stream_ks:put(self:_stream_meta_key(), string.format(
        '{"head":%d,"count":%d,"total":%d,"size":%d}',
        self._stream_head, self._stream_count,
        self._stream_total, self.stream_size))
end

function M:_stream_purge()
    pcall(function() self.stream_ks:delete(self:_stream_meta_key()) end)
    for i = 0, self.stream_size - 1 do
        pcall(function() self.stream_ks:delete(self:_stream_slot_key(i)) end)
    end
end

---------------------------------------------------------------------------
-- Mission lifecycle
---------------------------------------------------------------------------

function M:start()
    self.start_time = os.clock()

    -- Clear previous stream data
    self:_stream_purge()

    -- SQLite: write initial mission status
    self.kb_rt:merge_status({
        mission_active   = true,
        route_length     = self.route_length,
        actions_complete = 0,
        started_at       = os.date("!%Y-%m-%dT%H:%M:%SZ"),
        success          = nil,
    })

    -- NATS KeyStore: write live status
    local start_payload = json_util.encode({
        type             = "mission_start",
        robot_id         = self.robot_id,
        route_length     = self.route_length,
        timestamp        = os.date("!%Y-%m-%dT%H:%M:%SZ"),
    })
    self.ks:put(self.status_path, start_payload)

    -- NATS stream: persist to JetStream KV
    self:_stream_write(start_payload)
end

---------------------------------------------------------------------------
-- Action lifecycle
---------------------------------------------------------------------------

function M:action_start(index, action, pose)
    pose = pose or {}

    local payload = json_util.encode({
        type             = "action_start",
        robot_id         = self.robot_id,
        action_index     = index,
        kb_name          = action.kb_name,
        global_x         = pose.x or 0,
        global_y         = pose.y or 0,
        global_heading   = pose.heading or 0,
        global_arm_angle = pose.arm_angle or 0,
        timestamp        = os.date("!%Y-%m-%dT%H:%M:%SZ"),
    })

    -- SQLite bookend: initial heartbeat for this VN
    self.kb_rt:write_heartbeat({
        type          = "action_start",
        action_index  = index,
        kb_name       = action.kb_name,
        global_x      = pose.x or 0,
        global_y      = pose.y or 0,
        global_heading = pose.heading or 0,
        global_arm_angle = pose.arm_angle or 0,
        timestamp     = os.date("!%Y-%m-%dT%H:%M:%SZ"),
    })

    -- NATS KeyStore: update live status
    self.ks:put(self.status_path, json_util.encode({
        type          = "action_active",
        robot_id      = self.robot_id,
        action_index  = index,
        kb_name       = action.kb_name,
        global_x      = pose.x or 0,
        global_y      = pose.y or 0,
        global_heading = pose.heading or 0,
        timestamp     = os.date("!%Y-%m-%dT%H:%M:%SZ"),
    }))

    -- NATS stream: persist
    self:_stream_write(payload)
end

function M:heartbeat(index, kb_name, pose)
    pose = pose or {}
    self.heartbeat_count = self.heartbeat_count + 1

    -- NATS stream only (persistent via JetStream KV, no SQLite)
    self:_stream_write(json_util.encode({
        type             = "heartbeat",
        robot_id         = self.robot_id,
        action_index     = index,
        kb_name          = kb_name,
        heartbeat_seq    = self.heartbeat_count,
        global_x         = pose.x or 0,
        global_y         = pose.y or 0,
        global_heading   = pose.heading or 0,
        global_arm_angle = pose.arm_angle or 0,
        timestamp        = os.date("!%Y-%m-%dT%H:%M:%SZ"),
    }))
end

function M:action_complete(index, action, pose)
    pose = pose or {}

    local payload = json_util.encode({
        type             = "action_complete",
        robot_id         = self.robot_id,
        action_index     = index,
        kb_name          = action.kb_name,
        success          = true,
        global_x         = pose.x or 0,
        global_y         = pose.y or 0,
        global_heading   = pose.heading or 0,
        global_arm_angle = pose.arm_angle or 0,
        timestamp        = os.date("!%Y-%m-%dT%H:%M:%SZ"),
    })

    -- SQLite bookend: final heartbeat for this VN
    self.kb_rt:write_heartbeat({
        type             = "action_complete",
        action_index     = index,
        kb_name          = action.kb_name,
        success          = true,
        global_x         = pose.x or 0,
        global_y         = pose.y or 0,
        global_heading   = pose.heading or 0,
        global_arm_angle = pose.arm_angle or 0,
        timestamp        = os.date("!%Y-%m-%dT%H:%M:%SZ"),
    })

    -- NATS KeyStore: update live status
    self.ks:put(self.status_path, payload)

    -- NATS stream: persist
    self:_stream_write(payload)
end

function M:action_failed(index, action, reason)
    local payload = json_util.encode({
        type          = "action_failed",
        robot_id      = self.robot_id,
        action_index  = index,
        kb_name       = action.kb_name,
        fault_reason  = reason,
        timestamp     = os.date("!%Y-%m-%dT%H:%M:%SZ"),
    })

    -- SQLite bookend: fault record for this VN
    self.kb_rt:write_heartbeat({
        type          = "action_failed",
        action_index  = index,
        kb_name       = action.kb_name,
        success       = false,
        fault_reason  = reason,
        timestamp     = os.date("!%Y-%m-%dT%H:%M:%SZ"),
    })

    -- NATS KeyStore: update live status
    self.ks:put(self.status_path, payload)

    -- NATS stream: persist
    self:_stream_write(payload)
end

---------------------------------------------------------------------------
-- Mission completion
---------------------------------------------------------------------------

function M:finish(result)
    local elapsed_ms = math.floor((os.clock() - (self.start_time or os.clock())) * 1000)

    -- SQLite: write final mission status
    self.kb_rt:merge_status({
        mission_active   = false,
        success          = result.success,
        actions_complete = result.completed or 0,
        route_length     = result.total or self.route_length,
        elapsed_ms       = elapsed_ms,
        final_x          = result.final_pose and result.final_pose.x or 0,
        final_y          = result.final_pose and result.final_pose.y or 0,
        final_heading    = result.final_pose and result.final_pose.heading or 0,
        final_arm_angle  = result.final_pose and result.final_pose.arm_angle or 0,
        completed_at     = os.date("!%Y-%m-%dT%H:%M:%SZ"),
    })

    local event_type = result.success and "mission_complete" or "mission_failed"

    local payload = json_util.encode({
        type             = event_type,
        robot_id         = self.robot_id,
        success          = result.success,
        completed        = result.completed or 0,
        total            = result.total or self.route_length,
        elapsed_ms       = elapsed_ms,
        heartbeat_count  = self.heartbeat_count,
        final_x          = result.final_pose and result.final_pose.x or 0,
        final_y          = result.final_pose and result.final_pose.y or 0,
        final_heading    = result.final_pose and result.final_pose.heading or 0,
        final_arm_angle  = result.final_pose and result.final_pose.arm_angle or 0,
        fault            = result.fault,
        timestamp        = os.date("!%Y-%m-%dT%H:%M:%SZ"),
    })

    -- NATS KeyStore: final status
    self.ks:put(self.status_path, payload)

    -- NATS stream: persist
    self:_stream_write(payload)
end

---------------------------------------------------------------------------
-- Convenience reader
---------------------------------------------------------------------------

--- Read the persistent stream directly from JetStream KV.
-- No PubSub subscription needed — reads slot/meta keys directly.
-- @param count  number of most recent entries to return (default all)
-- @return array of decoded JSON tables, newest first
function M:read_stream(count)
    local meta_json = self.stream_ks:get(self:_stream_meta_key())
    if not meta_json then return {} end

    local head  = tonumber(meta_json:match('"head":(-?%d+)'))
    local stored = tonumber(meta_json:match('"count":(%d+)'))
    local size  = tonumber(meta_json:match('"size":(%d+)'))
    if not head or not stored or not size then return {} end

    count = count or stored
    if count > stored then count = stored end

    local result = {}
    -- Walk backwards from most recent
    for i = 0, count - 1 do
        local slot_0 = (head - 1 - i) % size
        local data = self.stream_ks:get(self:_stream_slot_key(slot_0))
        if data then
            local ok, decoded = pcall(json_util.decode, data)
            if ok then
                result[#result + 1] = decoded
            end
        end
    end
    return result
end

--- Read current live status from KeyStore.
-- @return table or nil
function M:read_status()
    local val = self.ks:get(self.status_path)
    if val then
        local ok, decoded = pcall(json_util.decode, val)
        if ok then return decoded end
    end
    return nil
end

---------------------------------------------------------------------------
-- Abort signaling
---------------------------------------------------------------------------

function M:is_abort_requested()
    local val = self.ks:get(self.abort_key)
    if val and val == "true" then
        return true
    end
    return false
end

---------------------------------------------------------------------------
-- Cleanup
---------------------------------------------------------------------------

function M:close()
    self.ks:disconnect()
    self.ks:destroy()
    self.stream_ks:disconnect()
    self.stream_ks:destroy()
    self.kb_rt:close()
end

return M
