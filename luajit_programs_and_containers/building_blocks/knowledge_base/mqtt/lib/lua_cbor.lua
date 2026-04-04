--[[
    lua_cbor.lua — LuaJIT FFI binding for JSON↔CBOR codec.

    Usage:
        local cbor = require("lib.lua_cbor")
        local bytes = cbor.encode('{"packet_type":1,"seq":42}')
        local json  = cbor.decode(bytes)
]]

local ffi = require("ffi")

ffi.cdef[[
    int cbor_from_json(const char *json, uint8_t *out, int max_out);
    int cbor_to_json(const uint8_t *cbor, int cbor_len, char *out, int max_out);
]]

local C = ffi.load("lua_cbor")

local MAX_BUF = 8192
local cbor_buf = ffi.new("uint8_t[?]", MAX_BUF)
local json_buf = ffi.new("char[?]", MAX_BUF)

local M = {}

--- Encode JSON string → CBOR bytes (returns Lua string of raw bytes)
function M.encode(json_str)
    local n = C.cbor_from_json(json_str, cbor_buf, MAX_BUF)
    if n < 0 then error("CBOR encode failed", 2) end
    return ffi.string(cbor_buf, n)
end

--- Decode CBOR bytes → JSON string
function M.decode(cbor_bytes)
    local len = #cbor_bytes
    local n = C.cbor_to_json(cbor_bytes, len, json_buf, MAX_BUF)
    if n < 0 then error("CBOR decode failed", 2) end
    return ffi.string(json_buf, n)
end

return M
