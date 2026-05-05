--[[
    mqtt_pubsub.lua — LuaJIT FFI binding for streaming MQTT pub/sub.

    Subscribe once, messages buffer via C ring buffer, poll() drains them.
    No re-subscribe per read. Designed for low-latency robot bridges.

    Usage:
        local ps = PubSub.new("localhost", 1883, "my_client")
        ps:connect(5000)
        ps:subscribe("robots/rover_1/stream_bus", 1)

        -- In tick loop:
        local msgs = ps:poll(10)  -- 10ms timeout
        for _, m in ipairs(msgs) do
            print(m.topic, m.payload)
        end

        ps:publish("robots/rover_1/rpc", '{"cmd":"go"}', 1, false)
        ps:disconnect()
        ps:destroy()
]]

local ffi = require("ffi")

ffi.cdef[[
    /* Message slot (fixed-size, ring buffer element) */
    typedef struct {
        char     topic[256];
        char     payload[8192];
        uint32_t payload_len;
        int      qos;
        bool     retain;
        bool     valid;
    } MqpsMsgSlot;

    /* Stats */
    typedef struct {
        uint64_t published;
        uint64_t received;
        uint64_t dropped;
        uint64_t polled;
        uint32_t ring_used;
    } MqpsStats;

    /* Opaque handle */
    typedef struct MqttPubSubStream MqttPubSubStream;

    MqttPubSubStream *mqps_create(const char *host, int port,
                                   const char *client_id);
    void              mqps_destroy(MqttPubSubStream *ps);

    bool mqps_connect(MqttPubSubStream *ps, int timeout_ms);
    void mqps_disconnect(MqttPubSubStream *ps);
    bool mqps_is_connected(MqttPubSubStream *ps);

    bool mqps_publish(MqttPubSubStream *ps, const char *topic,
                      const void *payload, int payload_len,
                      int qos, bool retain);

    bool mqps_subscribe(MqttPubSubStream *ps, const char *topic, int qos);
    bool mqps_unsubscribe(MqttPubSubStream *ps, const char *topic);

    int mqps_poll(MqttPubSubStream *ps, int timeout_ms,
                  MqpsMsgSlot *out_msgs, int max_msgs);

    int mqps_drain(MqttPubSubStream *ps,
                   MqpsMsgSlot *out_msgs, int max_msgs);

    void mqps_get_stats(MqttPubSubStream *ps, MqpsStats *out);
]]

local C = ffi.load("mqtt_pubsub")

-- Pre-allocate poll buffer (reused across calls)
local MAX_POLL = 64
local poll_buf = ffi.new("MqpsMsgSlot[?]", MAX_POLL)

local PubSub = {}
PubSub.__index = PubSub

function PubSub.new(host, port, client_id)
    local h = C.mqps_create(host or "localhost", port or 1883, client_id)
    if h == nil then error("Failed to create MQTT PubSub", 2) end
    return setmetatable({ _h = h }, PubSub)
end

function PubSub:destroy()
    if self._h then
        C.mqps_destroy(self._h)
        self._h = nil
    end
end

function PubSub:connect(timeout_ms)
    if not C.mqps_connect(self._h, timeout_ms or 5000) then
        error("MQTT PubSub connect failed", 2)
    end
end

function PubSub:disconnect()
    C.mqps_disconnect(self._h)
end

function PubSub:is_connected()
    return C.mqps_is_connected(self._h)
end

function PubSub:publish(topic, payload, qos, retain)
    local len = payload and #payload or 0
    return C.mqps_publish(self._h, topic, payload, len, qos or 1, retain or false)
end

function PubSub:subscribe(topic, qos)
    if not C.mqps_subscribe(self._h, topic, qos or 1) then
        error("MQTT PubSub subscribe failed: " .. topic, 2)
    end
end

function PubSub:unsubscribe(topic)
    return C.mqps_unsubscribe(self._h, topic)
end

--- Poll for messages. Runs mosquitto loop for timeout_ms, then drains buffer.
--- Returns a Lua table of { topic=string, payload=string, qos=int, retain=bool }.
function PubSub:poll(timeout_ms, max_msgs)
    max_msgs = max_msgs or MAX_POLL
    if max_msgs > MAX_POLL then max_msgs = MAX_POLL end

    local count = C.mqps_poll(self._h, timeout_ms or 10, poll_buf, max_msgs)
    local result = {}
    for i = 0, count - 1 do
        local slot = poll_buf[i]
        result[#result + 1] = {
            topic   = ffi.string(slot.topic),
            payload = ffi.string(slot.payload, slot.payload_len),
            qos     = slot.qos,
            retain  = slot.retain,
        }
    end
    return result
end

--- Non-blocking drain: return already-buffered messages without waiting.
function PubSub:drain(max_msgs)
    max_msgs = max_msgs or MAX_POLL
    if max_msgs > MAX_POLL then max_msgs = MAX_POLL end

    local count = C.mqps_drain(self._h, poll_buf, max_msgs)
    local result = {}
    for i = 0, count - 1 do
        local slot = poll_buf[i]
        result[#result + 1] = {
            topic   = ffi.string(slot.topic),
            payload = ffi.string(slot.payload, slot.payload_len),
            qos     = slot.qos,
            retain  = slot.retain,
        }
    end
    return result
end

--- Get stats: { published, received, dropped, polled, ring_used }
function PubSub:get_stats()
    local s = ffi.new("MqpsStats")
    C.mqps_get_stats(self._h, s)
    return {
        published = tonumber(s.published),
        received  = tonumber(s.received),
        dropped   = tonumber(s.dropped),
        polled    = tonumber(s.polled),
        ring_used = tonumber(s.ring_used),
    }
end

return { PubSub = PubSub, _C = C }
