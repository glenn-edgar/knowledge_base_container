--[[
    mqtt_hub_transport.lua -- Shared MQTT client for the planner.

    Single PubSub connection handles ALL robots. Wildcard subscriptions
    route messages by robot_id parsed from the topic. Replaces both
    nats_transport.hub_side and the separate mqtt_bridge process.

    Usage:
        local hub_tx = require("mqtt_hub_transport")
        local hub = hub_tx.new(mqtt_host, mqtt_port, site)
        hub:connect()

        -- In tick loop:
        local msgs = hub:poll()    -- {robot_id, msg_type, payload, topic}
        hub:send_rpc("rover_1", json_str)

        -- Shutdown:
        hub:close()
]]

local json_util = require("json_util")

local M = {}
M.__index = M

---------------------------------------------------------------------------
-- Constructor
---------------------------------------------------------------------------

function M.new(host, port, site, opts)
    opts = opts or {}
    local site_path = site:gsub("%.", "/")

    return setmetatable({
        host      = host or error("mqtt_hub_transport: host required"),
        port      = port or error("mqtt_hub_transport: port required"),
        site      = site,
        site_path = site_path,
        -- Phase 7 multi-tenant: client_id must be unique across all
        -- planners attached to the same broker; otherwise MQTT 3.1.1
        -- kicks out the older connection (rc=7 disconnect). The third
        -- positional arg in v3 (planner_namespace) used to be optional
        -- and absent in the original call sites; we accept it via opts
        -- for backward compatibility.
        namespace = opts.namespace,
        ps        = nil,  -- PubSub handle, set on connect()
        -- Topic prefixes
        robots_prefix     = site_path .. "/robots/",
        robots_prefix_len = #(site_path .. "/robots/"),
        -- Per-robot stream buffers: robot_id → {payload, ...}
        stream_buffers = {},
        -- Per-robot CBOR codec: robot_id → cbor_mod or nil
        wire_formats   = {},
        -- Link handler: set via set_link_handler() to route link messages
        link_handler   = nil,
    }, M)
end

---------------------------------------------------------------------------
-- Connect and subscribe to wildcard topics
---------------------------------------------------------------------------

function M:connect()
    local ffi = require("ffi")
    local pubsub = require("lib.mqtt_pubsub")
    -- Phase 7: per-tenant client_id so multiple planners share a broker
    -- without kicking each other out (MQTT 3.1.1: client_id is a global
    -- identifier, second connection wins). Falls back to site-only when
    -- no namespace was supplied (single-tenant deployments).
    local ns_suffix = self.namespace
        and ("_" .. self.namespace:gsub("[^%w]", "_"))
        or ""
    local client_id = "planner_" .. (self.site:gsub("%.", "_")) .. ns_suffix
    -- Cache for reconnect.
    self._client_id = client_id

    -- Reconnect backoff state. Polling helpers call _ensure_connected
    -- before any broker I/O; on a confirmed disconnect (rc=7 from a
    -- broker reset, peer kickout, or vpnkit hiccup) we re-dial with
    -- exponential backoff 1s→30s and re-subscribe on success. Initialized
    -- BEFORE the first connect attempt so an initial failure still
    -- enters the same backoff machinery.
    self._reconnect_backoff_s    = 1
    self._reconnect_backoff_max  = 30
    self._next_reconnect_at      = 0

    self.ps = pubsub.PubSub.new(self.host, self.port, client_id)
    -- Connect may return asynchronously on some platforms (WSL2 containers)
    local ok, err = pcall(self.ps.connect, self.ps, 5000)
    if not ok then
        -- Async connect — wait briefly for it to establish
        for _ = 1, 50 do
            if self.ps:is_connected() then break end
            ffi.C.usleep(100000)  -- 100ms
        end
    end
    if not self.ps:is_connected() then
        -- Initial connect failed. Don't throw — leave the planner with
        -- mqtt_hub wired so action_server can still bind link_manager;
        -- _ensure_connected on the next poll will retry. Log so ops can
        -- see the broker was unreachable at boot.
        io.stderr:write(string.format(
            "mqtt_hub_transport: initial connect failed (%s); " ..
            "will retry on first poll\n", tostring(err)))
        io.stderr:flush()
        return
    end

    self:_subscribe_all()

    -- Drain stale retained messages (limited attempts to avoid infinite loop)
    for _ = 1, 20 do
        local stale = self.ps:poll(50)
        if #stale == 0 then break end
    end
end

function M:_subscribe_all()
    -- Subscribe to all robot streams, status, and link protocol.
    -- Idempotent: safe to call after reconnect; the broker re-applies
    -- the wildcard subscriptions for the same client_id.
    pcall(self.ps.subscribe, self.ps, self.site_path .. "/robots/+/stream_bus", 1)
    pcall(self.ps.subscribe, self.ps, self.site_path .. "/robots/+/status/#", 1)
    pcall(self.ps.subscribe, self.ps, self.site_path .. "/robots/+/link", 1)
end

--- Re-establish the broker connection if it's gone. Idempotent.
-- Returns true if the connection is good (either was already connected,
-- or reconnect succeeded just now); false if the connection is still
-- down and we're inside the backoff window. Callers should treat
-- "false" as "skip this poll cycle, try again next tick".
function M:_ensure_connected()
    if self.ps and self.ps:is_connected() then
        -- Healthy — reset backoff so the next outage starts at 1s.
        if self._reconnect_backoff_s ~= 1 then
            self._reconnect_backoff_s = 1
        end
        return true
    end

    local now_s = os.time()
    if now_s < (self._next_reconnect_at or 0) then
        return false   -- inside backoff window
    end

    -- The pubsub handle may not exist yet (initial connect failed at
    -- M:connect time; we deferred giving up so this poll path can
    -- recover when the broker comes back). Re-create it lazily.
    if not self.ps then
        local ok_new, err_new = pcall(function()
            local pubsub = require("lib.mqtt_pubsub")
            self.ps = pubsub.PubSub.new(self.host, self.port, self._client_id)
        end)
        if not ok_new then
            local backoff = self._reconnect_backoff_s or 1
            self._next_reconnect_at = now_s + backoff
            self._reconnect_backoff_s = math.min(
                backoff * 2, self._reconnect_backoff_max or 30)
            io.stderr:write(string.format(
                "mqtt_hub_transport: PubSub.new failed (%s), retry in %ds\n",
                tostring(err_new), backoff))
            io.stderr:flush()
            return false
        end
    end

    -- Try to (re)connect. pubsub:connect is synchronous on the happy
    -- path and may throw on failure; pcall it so a transient broker
    -- outage doesn't kill the planner's serve loop.
    local ok = pcall(function()
        self.ps:connect(5000)
    end)
    if ok and self.ps:is_connected() then
        io.stderr:write(string.format(
            "mqtt_hub_transport: connected (client_id=%s)\n",
            tostring(self._client_id)))
        io.stderr:flush()
        self:_subscribe_all()
        self._reconnect_backoff_s = 1
        return true
    end

    -- Failed — exponential backoff up to _reconnect_backoff_max.
    local backoff = self._reconnect_backoff_s or 1
    self._next_reconnect_at = now_s + backoff
    self._reconnect_backoff_s = math.min(
        backoff * 2, self._reconnect_backoff_max or 30)
    io.stderr:write(string.format(
        "mqtt_hub_transport: reconnect failed, retry in %ds\n", backoff))
    io.stderr:flush()
    return false
end

---------------------------------------------------------------------------
-- Topic parsing
---------------------------------------------------------------------------

function M:parse_robot_id(topic)
    if topic:sub(1, self.robots_prefix_len) ~= self.robots_prefix then
        return nil, nil
    end
    local rest = topic:sub(self.robots_prefix_len + 1)
    local slash = rest:find("/")
    if not slash then return nil, nil end

    local robot_id = rest:sub(1, slash - 1)
    local suffix = rest:sub(slash + 1)  -- "stream_bus", "status/heartbeat", etc.
    return robot_id, suffix
end

---------------------------------------------------------------------------
-- Poll: drain all MQTT messages, return parsed list
---------------------------------------------------------------------------

function M:poll(timeout_ms)
    timeout_ms = timeout_ms or 1
    if not self:_ensure_connected() then return {} end
    local msgs = self.ps:poll(timeout_ms)
    local result = {}

    for _, msg in ipairs(msgs) do
        if #msg.payload == 0 then goto continue end  -- skip empty retained clears

        local robot_id, suffix = self:parse_robot_id(msg.topic)
        if not robot_id then goto continue end

        local msg_type
        if suffix == "stream_bus" then
            msg_type = "stream"
        elseif suffix == "link" then
            msg_type = "link"
        elseif suffix == "status/heartbeat" then
            msg_type = "heartbeat"
        elseif suffix == "status/state" then
            msg_type = "status_state"
        elseif suffix == "status/energy" then
            msg_type = "status_energy"
        elseif suffix == "status/bitmask" then
            msg_type = "status_bitmask"
        else
            msg_type = "unknown"
        end

        -- Decode CBOR → JSON string for robots with wire_format=cbor
        local payload = msg.payload
        if self.wire_formats[robot_id] == "cbor" then
            local cbor = require("lib.lua_cbor")
            local ok, decoded = pcall(cbor.decode, payload)
            if ok then
                payload = decoded  -- cbor.decode returns a JSON string
            end
        end

        result[#result + 1] = {
            robot_id = robot_id,
            msg_type = msg_type,
            payload  = payload,
            topic    = msg.topic,
            retain   = msg.retain,
        }

        ::continue::
    end

    return result
end

---------------------------------------------------------------------------
-- Per-robot stream buffer (for queue_monitor compatibility)
---------------------------------------------------------------------------

function M:buffer_stream(robot_id, payload)
    if not self.stream_buffers[robot_id] then
        self.stream_buffers[robot_id] = {}
    end
    local buf = self.stream_buffers[robot_id]
    buf[#buf + 1] = payload
end

function M:recv_stream(robot_id)
    local buf = self.stream_buffers[robot_id]
    if not buf or #buf == 0 then return nil end
    return table.remove(buf, 1)
end

---------------------------------------------------------------------------
-- Send RPC command to a specific robot
---------------------------------------------------------------------------

-- Safe-publish helper: pcall around publish + an is_connected gate so
-- the planner doesn't crash when the broker is briefly gone. Drops the
-- message silently; the poll loop's _ensure_connected handles reconnect
-- + re-subscribe, and the protocol re-sends (link heartbeats, ack
-- retries from the robot's announce loop) cover the gap.
function M:_safe_publish(topic, payload, qos, retain)
    if not self.ps or not self.ps:is_connected() then return false end
    local ok, err = pcall(self.ps.publish, self.ps, topic, payload,
        qos or 1, retain == true)
    if not ok then
        io.stderr:write(string.format(
            "mqtt_hub_transport: publish failed on %s: %s\n",
            tostring(topic), tostring(err)))
        io.stderr:flush()
    end
    return ok
end

function M:send_rpc(robot_id, json_str)
    local wire = json_str
    local fmt = self.wire_formats[robot_id]
    if fmt == "cbor" then
        local cbor = require("lib.lua_cbor")
        local ok, result = pcall(cbor.encode, json_str)
        if ok then
            wire = result
        else
            io.stderr:write("CBOR encode error: " .. tostring(result) ..
                "\n  JSON (" .. #json_str .. " bytes): " .. json_str:sub(1, 200) .. "\n")
            -- Fallback to JSON
        end
    end
    local topic = self.site_path .. "/robots/" .. robot_id .. "/rpc"
    self:_safe_publish(topic, wire, 1, false)
    return "ok"
end

---------------------------------------------------------------------------
-- Raw publish: caller-supplied bytes go on the wire as-is.
--
-- Used by the Phase 5 drive-packet path: encoder.encode_drive() already
-- produces CBOR bytes, so passing them through send_rpc would
-- double-encode them when the per-robot wire_format is "cbor".
-- send_rpc_raw bypasses wire_format conversion entirely.
---------------------------------------------------------------------------

function M:send_rpc_raw(robot_id, bytes)
    local topic = self.site_path .. "/robots/" .. robot_id .. "/rpc"
    self:_safe_publish(topic, bytes, 1, false)
    return "ok"
end

---------------------------------------------------------------------------
-- Publish to planner/ control topics (ack, heartbeat, disconnect)
---------------------------------------------------------------------------

function M:send_planner_ack(robot_id, json_str)
    local topic = self.site_path .. "/robots/" .. robot_id .. "/planner/ack"
    self:_safe_publish(topic, json_str, 1, false)
end

function M:send_planner_heartbeat(robot_id, json_str)
    local topic = self.site_path .. "/robots/" .. robot_id .. "/planner/heartbeat"
    self:_safe_publish(topic, json_str, 1, false)
end

function M:send_planner_disconnect(robot_id, json_str)
    local topic = self.site_path .. "/robots/" .. robot_id .. "/planner/disconnect"
    self:_safe_publish(topic, json_str, 1, false)
end

---------------------------------------------------------------------------
-- Wire format management
---------------------------------------------------------------------------

function M:set_wire_format(robot_id, format)
    self.wire_formats[robot_id] = format
end

---------------------------------------------------------------------------
-- Per-robot transport adapter (queue_monitor compatible)
--
-- Returns a lightweight handle with send_rpc/recv_stream for one robot.
-- Backed by the shared connection.
---------------------------------------------------------------------------

function M:robot_transport(robot_id)
    local hub = self
    local tx = {}

    function tx:send_rpc(json_str)
        return hub:send_rpc(robot_id, json_str)
    end

    -- Phase 5 drive-packet path: caller has already CBOR-encoded the
    -- payload; bypass wire_format conversion to avoid double-encoding.
    function tx:send_rpc_raw(bytes)
        return hub:send_rpc_raw(robot_id, bytes)
    end

    function tx:recv_stream()
        return hub:recv_stream(robot_id)
    end

    function tx:flush()
        hub.stream_buffers[robot_id] = {}
    end

    function tx:close()
        hub.stream_buffers[robot_id] = nil
    end

    return tx
end

---------------------------------------------------------------------------
-- Clear retained status topics for a robot
---------------------------------------------------------------------------

function M:clear_retained(robot_id)
    local prefix = self.site_path .. "/robots/" .. robot_id .. "/status/"
    for _, suffix in ipairs({"state", "energy", "bitmask"}) do
        self.ps:publish(prefix .. suffix, "", 1, true)
    end
end

---------------------------------------------------------------------------
-- Publish to kv-bridge container (fire-and-forget, never blocks)
--
-- The kv-bridge subscribes to MQTT_TOPIC (default: kv_bridge/write) and
-- writes to NATS KV asynchronously. This keeps all KV writes off the
-- hub tick path.
---------------------------------------------------------------------------

function M:publish_kv(bucket, key, value)
    local payload = '{"bucket":"' .. bucket .. '","key":"' .. key .. '","value":' .. value .. '}'
    self.ps:publish("kv_bridge/write", payload, 0, false)
end

function M:delete_kv(bucket, key)
    local payload = '{"bucket":"' .. bucket .. '","key":"' .. key .. '","op":"delete"}'
    self.ps:publish("kv_bridge/write", payload, 0, false)
end

---------------------------------------------------------------------------
-- Poll and route: poll MQTT, auto-buffer stream messages, return the rest
--
-- Convenience for tick loops. Stream messages go into per-robot buffers
-- (readable via robot_transport:recv_stream()). Non-stream messages
-- (heartbeats, status) are returned for link_manager / caller handling.
---------------------------------------------------------------------------

--- Set a link handler to receive link protocol messages.
-- The handler receives (robot_id, payload) for each link message.
-- Typically this is a function that dispatches to link_manager.
function M:set_link_handler(handler)
    self.link_handler = handler
end

function M:poll_and_route(timeout_ms)
    local msgs = self:poll(timeout_ms)
    local non_stream = {}
    for _, msg in ipairs(msgs) do
        if msg.msg_type == "stream" then
            self:buffer_stream(msg.robot_id, msg.payload)
        elseif msg.msg_type == "link" and self.link_handler then
            self.link_handler(msg.robot_id, msg.payload)
        else
            non_stream[#non_stream + 1] = msg
        end
    end
    return non_stream
end

---------------------------------------------------------------------------
-- Shutdown
---------------------------------------------------------------------------

function M:close()
    if self.ps then
        pcall(function() self.ps:disconnect() end)
        pcall(function() self.ps:destroy() end)
        self.ps = nil
    end
end

return M
