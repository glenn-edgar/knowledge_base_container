--[[
    mqtt_bridge.lua -- Multi-robot NATS↔MQTT bidirectional bridge.

    Single bridge process handles all MQTT robots. Discovers robots
    dynamically from MQTT status topics. Each robot can independently
    use JSON or CBOR wire format.

    Three channels per robot:
      1. NATS rpc → MQTT rpc (commands to robot)
      2. MQTT stream → NATS stream (responses from robot)
      3. MQTT status → NATS KeyStore (energy, bitmask, state)

    MQTT subscriptions (wildcards):
      {site}/robots/+/stream_bus   — all robot responses
      {site}/robots/+/status/#     — all robot status

    Usage: luajit mqtt_bridge.lua [mqtt_host] [mqtt_port]

    Environment:
      NATS_SERVER     NATS URL (default: nats://127.0.0.1:4222)
      MQTT_HOST       MQTT broker host (default: localhost)
      MQTT_PORT       MQTT broker port (default: 1883)
      VMRT_KB_SITE    KB site name (default: moonbase.alpha.surface_ops)
]]

local ffi = require("ffi")
ffi.cdef[[ int usleep(unsigned int usec); ]]

local json_util = require("json_util")

local mqtt_host = arg and arg[1] or os.getenv("MQTT_HOST") or "localhost"
local mqtt_port = tonumber(arg and arg[2] or os.getenv("MQTT_PORT") or "1883")
local server    = os.getenv("NATS_SERVER") or "nats://127.0.0.1:4222"
local site      = os.getenv("VMRT_KB_SITE") or "moonbase.alpha.surface_ops"

io.stderr:write(string.format(
    "BRIDGE: NATS=%s, MQTT=%s:%d, site=%s\n", server, mqtt_host, mqtt_port, site))

local site_path   = site:gsub("%.", "/")
local site_bucket = site:gsub("%.", "_")

---------------------------------------------------------------------------
-- NATS KeyStore for status mirroring (shared across all robots)
---------------------------------------------------------------------------
local ks_lib = require("lib.nats_key_store")
local status_ks = ks_lib.KeyStore.new({
    server        = server,
    bucket        = site_bucket .. "_robot_status",
    description   = "Bridge status mirror",
    create_bucket = true,
    history       = 1,
    client_name   = "bridge_status",
})
status_ks:connect()

---------------------------------------------------------------------------
-- MQTT side: single PubSub with wildcard subscriptions
---------------------------------------------------------------------------
local pubsub_mod = require("lib.mqtt_pubsub")

local mqtt_ps = pubsub_mod.PubSub.new(mqtt_host, mqtt_port, "mqtt_bridge")
mqtt_ps:connect(5000)

-- Subscribe to stream first, flush stale messages
local stream_wildcard = site_path .. "/robots/+/stream_bus"
local status_wildcard = site_path .. "/robots/+/status/#"

mqtt_ps:subscribe(stream_wildcard, 1)
repeat
    local stale = mqtt_ps:poll(10)
until #stale == 0

-- Then subscribe to status (retained messages delivered fresh)
mqtt_ps:subscribe(status_wildcard, 1)

---------------------------------------------------------------------------
-- Topic parsing
---------------------------------------------------------------------------

-- Extract robot_id from topic: {site}/robots/{robot_id}/...
local robots_prefix = site_path .. "/robots/"
local robots_prefix_len = #robots_prefix

local function parse_robot_id(topic)
    if topic:sub(1, robots_prefix_len) ~= robots_prefix then return nil end
    local rest = topic:sub(robots_prefix_len + 1)
    local slash = rest:find("/")
    if not slash then return nil end
    return rest:sub(1, slash - 1)
end

local function is_stream_topic(topic)
    return topic:match("/stream_bus$") ~= nil
end

local function is_status_topic(topic)
    return topic:match("/status/") ~= nil
end

local function mqtt_topic_to_nats_key(topic)
    return topic:gsub("/", ".")
end

---------------------------------------------------------------------------
-- Per-robot state: NATS transport + wire format
---------------------------------------------------------------------------
local nats_transport = require("nats_transport")
local cbor_mod = nil  -- loaded on demand

local robots = {}  -- robot_id → { nats_tx, wire_format, rpc_topic }

local function get_or_create_robot(robot_id)
    if robots[robot_id] then return robots[robot_id] end

    -- Create NATS remote_side for this robot
    local nats_tx = nats_transport.remote_side(robot_id, server, site)
    nats_tx:flush()

    local rpc_topic = site_path .. "/robots/" .. robot_id .. "/rpc"

    robots[robot_id] = {
        nats_tx     = nats_tx,
        wire_format = "json",  -- updated when status/state arrives
        rpc_topic   = rpc_topic,
    }

    io.stderr:write(string.format("BRIDGE: registered robot '%s' (wire=json)\n", robot_id))
    return robots[robot_id]
end

local function update_wire_format(robot_id, new_format)
    local robot = robots[robot_id]
    if not robot then return end
    if robot.wire_format == new_format then return end

    robot.wire_format = new_format
    if new_format == "cbor" and not cbor_mod then
        cbor_mod = require("lib.lua_cbor")
    end
    io.stderr:write(string.format("BRIDGE: robot '%s' wire_format → %s\n",
        robot_id, new_format))
end

---------------------------------------------------------------------------
-- Discovery: poll for retained status messages to find existing robots
---------------------------------------------------------------------------
io.stderr:write("BRIDGE: discovering robots...\n")

for _ = 1, 40 do
    local msgs = mqtt_ps:poll(50)
    for _, msg in ipairs(msgs) do
        local rid = parse_robot_id(msg.topic)
        if rid and is_status_topic(msg.topic) then
            get_or_create_robot(rid)

            -- Check for wire_format in state topic
            local ok, parsed = pcall(json_util.decode, msg.payload)
            if ok and parsed and parsed.wire_format then
                update_wire_format(rid, parsed.wire_format)
            end

            -- Mirror to NATS KV
            local nats_key = mqtt_topic_to_nats_key(msg.topic)
            pcall(status_ks.put, status_ks, nats_key, msg.payload)
        end
    end
    if #msgs == 0 then break end
end

local robot_count = 0
for _ in pairs(robots) do robot_count = robot_count + 1 end
io.stderr:write(string.format("BRIDGE: running (%d robots discovered)\n", robot_count))
io.stderr:flush()

---------------------------------------------------------------------------
-- Bridge tick loop
---------------------------------------------------------------------------
local running = true
local fwd_nats_to_mqtt = 0
local fwd_mqtt_to_nats = 0
local fwd_status = 0

while running do
    local did_work = false

    -- Direction 1: NATS rpc → MQTT rpc (per robot)
    for robot_id, robot in pairs(robots) do
        for _ = 1, 10 do
            local rpc_payload = robot.nats_tx:recv_rpc()
            if not rpc_payload then break end

            local wire_payload = rpc_payload
            if robot.wire_format == "cbor" and cbor_mod then
                wire_payload = cbor_mod.encode(rpc_payload)
            end
            mqtt_ps:publish(robot.rpc_topic, wire_payload, 1, false)
            fwd_nats_to_mqtt = fwd_nats_to_mqtt + 1
            did_work = true
        end
    end

    -- Direction 2: MQTT → NATS (stream + status, all robots)
    local msgs = mqtt_ps:poll(1)
    for _, msg in ipairs(msgs) do
        local rid = parse_robot_id(msg.topic)
        if not rid then goto continue end

        if is_status_topic(msg.topic) then
            -- Status: always JSON → mirror to NATS KV
            local nats_key = mqtt_topic_to_nats_key(msg.topic)
            pcall(status_ks.put, status_ks, nats_key, msg.payload)
            fwd_status = fwd_status + 1

            -- Auto-register new robots from status messages
            local robot = get_or_create_robot(rid)

            -- Detect wire_format changes
            local ok, parsed = pcall(json_util.decode, msg.payload)
            if ok and parsed and parsed.wire_format then
                update_wire_format(rid, parsed.wire_format)
            end

            -- Detect disconnection
            if ok and parsed and parsed.connected == false then
                io.stderr:write(string.format("BRIDGE: robot '%s' disconnected\n", rid))
            end

        elseif is_stream_topic(msg.topic) then
            -- Stream: translate if needed, forward to NATS
            local robot = robots[rid]
            if robot then
                local payload = msg.payload
                if robot.wire_format == "cbor" and cbor_mod then
                    payload = cbor_mod.decode(payload)
                end
                robot.nats_tx:send_stream(payload)
                fwd_mqtt_to_nats = fwd_mqtt_to_nats + 1
            end
        end

        did_work = true
        ::continue::
    end

    if not did_work then
        ffi.C.usleep(500)
    end
end

---------------------------------------------------------------------------
-- Cleanup
---------------------------------------------------------------------------
local robot_names = {}
for rid in pairs(robots) do robot_names[#robot_names + 1] = rid end

io.stderr:write(string.format(
    "BRIDGE: shutdown (robots: %s, fwd: %d NATS→MQTT, %d MQTT→NATS, %d status)\n",
    table.concat(robot_names, ","), fwd_nats_to_mqtt, fwd_mqtt_to_nats, fwd_status))
io.stderr:flush()

pcall(function() mqtt_ps:disconnect(); mqtt_ps:destroy() end)
pcall(function() status_ks:disconnect(); status_ks:destroy() end)
for _, robot in pairs(robots) do
    pcall(function() robot.nats_tx:close() end)
end
