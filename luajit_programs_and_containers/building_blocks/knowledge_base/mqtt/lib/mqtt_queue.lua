--[[
  mqtt_queue.lua — LuaJIT FFI binding for MQTT Queue (publisher + reader)
  Timeouts in MILLISECONDS (matching original C API).
]]

local ffi = require("ffi")
pcall(ffi.cdef, "void free(void *ptr);")

ffi.cdef[[
typedef struct MqttPublisher MqttPublisher;
MqttPublisher *mqtt_pub_create(const char *host, int port, const char *client_id);
void mqtt_pub_destroy(MqttPublisher *p);
bool mqtt_pub_connect(MqttPublisher *p, int timeout_ms);
void mqtt_pub_disconnect(MqttPublisher *p);
bool mqtt_pub_is_connected(MqttPublisher *p);
bool mqtt_pub_publish(MqttPublisher *p, const char *topic,
                      const char *payload, int qos, bool retain);

typedef struct MqttQueueReader MqttQueueReader;
typedef struct MqttQueueMsg {
    char *topic; char *payload; struct MqttQueueMsg *next;
} MqttQueueMsg;
MqttQueueReader *mqtt_qr_create(const char *host, int port,
                                 const char *client_id, bool clean_session);
void mqtt_qr_destroy(MqttQueueReader *r);
bool mqtt_qr_connect(MqttQueueReader *r, int timeout_ms);
void mqtt_qr_disconnect(MqttQueueReader *r);
bool mqtt_qr_is_connected(MqttQueueReader *r);
bool mqtt_qr_subscribe(MqttQueueReader *r, const char *topic, int qos, int timeout_ms);
MqttQueueMsg *mqtt_qr_read(MqttQueueReader *r, const char *topic,
                            int qos, int timeout_ms, int *out_count);
void mqtt_qr_free_msgs(MqttQueueMsg *head);
]]

local C = ffi.load("mqtt_luajit_adapter")

local Publisher = {}; Publisher.__index = Publisher
function Publisher.new(host, port, client_id)
    local h = C.mqtt_pub_create(host or "localhost", port or 1883, client_id)
    if h == nil then error("Failed to create publisher", 2) end
    return setmetatable({ _h = h }, Publisher)
end
function Publisher:destroy()      if self._h then C.mqtt_pub_destroy(self._h); self._h = nil end end
function Publisher:connect(ms)    if not C.mqtt_pub_connect(self._h, ms or 5000) then error("Publisher connect failed", 2) end end
function Publisher:disconnect()   C.mqtt_pub_disconnect(self._h) end
function Publisher:is_connected() return C.mqtt_pub_is_connected(self._h) end
function Publisher:publish(topic, payload, qos, retain)
    return C.mqtt_pub_publish(self._h, topic, payload, qos or 1, retain or false)
end

local QueueReader = {}; QueueReader.__index = QueueReader
function QueueReader.new(host, port, client_id, clean_session)
    if clean_session == nil then clean_session = false end
    local h = C.mqtt_qr_create(host or "localhost", port or 1883, client_id, clean_session)
    if h == nil then error("Failed to create queue reader", 2) end
    return setmetatable({ _h = h }, QueueReader)
end
function QueueReader:destroy()      if self._h then C.mqtt_qr_destroy(self._h); self._h = nil end end
function QueueReader:connect(ms)    if not C.mqtt_qr_connect(self._h, ms or 5000) then error("Reader connect failed", 2) end end
function QueueReader:disconnect()   C.mqtt_qr_disconnect(self._h) end
function QueueReader:is_connected() return C.mqtt_qr_is_connected(self._h) end
function QueueReader:subscribe(topic, qos, ms)
    if not C.mqtt_qr_subscribe(self._h, topic, qos or 1, ms or 2000) then error("Subscribe failed", 2) end
end
function QueueReader:read(topic, qos, ms)
    local cnt = ffi.new("int[1]")
    local head = C.mqtt_qr_read(self._h, topic, qos or 1, ms or 3000, cnt)
    local r = {}; local cur = head
    while cur ~= nil do
        r[#r+1] = {
            topic   = cur.topic ~= nil and ffi.string(cur.topic) or "",
            payload = cur.payload ~= nil and ffi.string(cur.payload) or "",
        }
        cur = cur.next
    end
    if head ~= nil then C.mqtt_qr_free_msgs(head) end
    return r
end

return { Publisher = Publisher, Reader = QueueReader, _C = C }

