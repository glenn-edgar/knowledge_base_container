--[[
    fnv1a.lua — LuaJIT FFI wrapper for libfnv1a.so

    Proper uint32_t arithmetic via C. No integer overflow concerns.

    Usage:
        local fnv = require("fnv1a")
        local hash = fnv.hash("hello")           -- uint32
        local hash = fnv.hash_bytes(ptr, len)     -- raw bytes
        local sh   = fnv.schema_hash("file", "record")  -- signed int32
]]

local ffi = require("ffi")

ffi.cdef[[
    uint32_t fnv1a_32(const char *data, size_t len);
    uint32_t fnv1a_32_str(const char *str);
    int32_t  fnv1a_schema_hash(const char *file_name, const char *record_name);
]]

-- Load from same directory as this script, or LD_LIBRARY_PATH
local script_dir = debug.getinfo(1, "S").source:match("^@(.*/)")  or "./"
local ok, C = pcall(ffi.load, script_dir .. "../fnv1a/libfnv1a.so")
if not ok then
    C = ffi.load("fnv1a")
end

local M = {}

--- Hash a string, returns unsigned 32-bit integer.
function M.hash(str)
    return tonumber(C.fnv1a_32(str, #str))
end

--- Hash raw bytes (cdata pointer + length).
function M.hash_bytes(data, len)
    return tonumber(C.fnv1a_32(data, len))
end

--- Hash a null-terminated string.
function M.hash_str(str)
    return tonumber(C.fnv1a_32_str(str))
end

--- Schema hash: fnv1a("{file}.h:{record}"), returns signed int32.
function M.schema_hash(file_name, record_name)
    return tonumber(C.fnv1a_schema_hash(file_name, record_name))
end

M._C = C

return M
