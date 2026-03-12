--[[
  mqtt_kv_store.lua — LuaJIT FFI binding for MQTT KV Store (writer + reader)
  Timeouts in SECONDS (matching original C API).
]]

local ffi = require("ffi")
pcall(ffi.cdef, "void free(void *ptr);")

ffi.cdef[[
void mqtt_adapter_lib_init(void);
void mqtt_adapter_lib_cleanup(void);

typedef struct MqttKvWriter MqttKvWriter;
MqttKvWriter *mqtt_kvw_create(const char *host, int port, const char *client_id);
void mqtt_kvw_destroy(MqttKvWriter *w);
bool mqtt_kvw_connect(MqttKvWriter *w, double timeout_sec);
void mqtt_kvw_disconnect(MqttKvWriter *w);
bool mqtt_kvw_is_connected(MqttKvWriter *w);
bool mqtt_kvw_write(MqttKvWriter *w, const char *topic, const char *value,
                    int qos, bool retain, double timeout_sec);
bool mqtt_kvw_delete(MqttKvWriter *w, const char *topic, double timeout_sec);
bool mqtt_kvw_update(MqttKvWriter *w, const char *topic, const char *value,
                     double timeout_sec);

typedef struct MqttKvReader MqttKvReader;
typedef struct { char topic[256]; char value[4096]; bool active; } MqttKvEntry;
MqttKvReader *mqtt_kvr_create(const char *host, int port, const char *client_id);
void mqtt_kvr_destroy(MqttKvReader *r);
bool mqtt_kvr_connect(MqttKvReader *r, double timeout_sec);
void mqtt_kvr_disconnect(MqttKvReader *r);
bool mqtt_kvr_is_connected(MqttKvReader *r);
int  mqtt_kvr_read_pattern(MqttKvReader *r, const char *pattern, int qos,
                           double timeout_sec, MqttKvEntry *out, int max);
bool mqtt_kvr_read_single(MqttKvReader *r, const char *topic,
                          double timeout_sec, char *out, int out_len);
int  mqtt_kvr_read_all(MqttKvReader *r, const char *base, double timeout_sec,
                       MqttKvEntry *out, int max);
]]

local C = ffi.load("mqtt_luajit_adapter")

local Writer = {}; Writer.__index = Writer
function Writer.new(host, port, client_id)
    local h = C.mqtt_kvw_create(host or "localhost", port or 1883, client_id)
    if h == nil then error("Failed to create KV writer", 2) end
    return setmetatable({ _h = h }, Writer)
end
function Writer:destroy()      if self._h then C.mqtt_kvw_destroy(self._h); self._h = nil end end
function Writer:connect(t)     if not C.mqtt_kvw_connect(self._h, t or 5.0) then error("KV writer connect failed", 2) end end
function Writer:disconnect()   C.mqtt_kvw_disconnect(self._h) end
function Writer:is_connected() return C.mqtt_kvw_is_connected(self._h) end
function Writer:write(topic, value, qos, retain, t)
    if retain == nil then retain = true end
    return C.mqtt_kvw_write(self._h, topic, value, qos or 1, retain, t or 5.0)
end
function Writer:delete(topic, t) return C.mqtt_kvw_delete(self._h, topic, t or 5.0) end
function Writer:update(topic, value, t) return C.mqtt_kvw_update(self._h, topic, value, t or 5.0) end

local Reader = {}; Reader.__index = Reader
function Reader.new(host, port, client_id)
    local h = C.mqtt_kvr_create(host or "localhost", port or 1883, client_id)
    if h == nil then error("Failed to create KV reader", 2) end
    return setmetatable({ _h = h }, Reader)
end
function Reader:destroy()      if self._h then C.mqtt_kvr_destroy(self._h); self._h = nil end end
function Reader:connect(t)     if not C.mqtt_kvr_connect(self._h, t or 5.0) then error("KV reader connect failed", 2) end end
function Reader:disconnect()   C.mqtt_kvr_disconnect(self._h) end
function Reader:is_connected() return C.mqtt_kvr_is_connected(self._h) end
function Reader:read_pattern(pattern, qos, t, max)
    max = max or 256; local e = ffi.new("MqttKvEntry[?]", max)
    local n = C.mqtt_kvr_read_pattern(self._h, pattern, qos or 1, t or 5.0, e, max)
    local r = {}; for i = 0, n-1 do if e[i].active then r[#r+1] = { topic = ffi.string(e[i].topic), value = ffi.string(e[i].value) } end end
    return r
end
function Reader:read_single(topic, t)
    local buf = ffi.new("char[4096]")
    if C.mqtt_kvr_read_single(self._h, topic, t or 5.0, buf, 4096) then return ffi.string(buf) end
    return nil
end
function Reader:read_all(base, t, max)
    max = max or 256; local e = ffi.new("MqttKvEntry[?]", max)
    local n = C.mqtt_kvr_read_all(self._h, base or "#", t or 5.0, e, max)
    local r = {}; for i = 0, n-1 do if e[i].active then r[#r+1] = { topic = ffi.string(e[i].topic), value = ffi.string(e[i].value) } end end
    return r
end

return {
    Writer = Writer, Reader = Reader,
    lib_init    = function() C.mqtt_adapter_lib_init() end,
    lib_cleanup = function() C.mqtt_adapter_lib_cleanup() end,
    _C = C,
}
