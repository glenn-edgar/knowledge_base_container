-- ble_port.lua
-- BLE GATT write/read wrapper for LuaJIT.
--
-- Two backends:
--   1. Native:  FFI into ble_helper.dll  (preferred)
--   2. Bridge:  named-pipe to a .NET/Python BLE helper process (fallback)
--
-- The bridge fallback uses a simple line-based protocol over
-- a Win32 named pipe, so we can leverage .NET's BLE stack without
-- fighting WinRT from C.

local ffi = require("ffi")

---------------------------------------------------------------------------
-- FFI declarations (matches ble_helper.h)
---------------------------------------------------------------------------
ffi.cdef[[
typedef struct ble_conn* ble_handle_t;

int  ble_open(uint64_t address_u64, ble_handle_t* out_handle);
int  ble_open_by_name(const char* device_name, uint32_t timeout_ms,
                       ble_handle_t* out_handle);
int  ble_write(ble_handle_t h, const char* service_uuid,
               const char* char_uuid, const uint8_t* data,
               uint32_t data_len, int write_type);
int  ble_read(ble_handle_t h, const char* service_uuid,
              const char* char_uuid, uint8_t* buf, uint32_t buf_len);
void ble_close(ble_handle_t h);
const char* ble_last_error(void);
]]

---------------------------------------------------------------------------
-- Try loading the native DLL
---------------------------------------------------------------------------
local ble_lib
local HAS_NATIVE = false

local ok, lib = pcall(ffi.load, "ble_helper")
if ok then
    ble_lib  = lib
    HAS_NATIVE = true
end

---------------------------------------------------------------------------
-- BLE Port object
---------------------------------------------------------------------------
local BlePort = {}
BlePort.__index = BlePort

--- Open a BLE connection.
-- @param opts table {
--   address      = 0xAABBCCDDEEFF,   -- BT address (uint64) OR
--   device_name  = "MyDevice",        -- scan by name
--   scan_timeout = 5000,              -- ms (for name scan)
--   service_uuid = "6e40...",         -- target GATT service
--   char_uuid    = "6e41...",         -- target GATT characteristic
--   write_type   = 0,                 -- 0=with_response, 1=without
--   label        = "BLE-MyDevice",    -- logging label
--   backend      = "native",          -- "native" or "bridge"
-- }
function BlePort.open(opts)
    assert(opts, "BlePort.open requires opts table")
    assert(opts.service_uuid, "service_uuid required")
    assert(opts.char_uuid,    "char_uuid required")

    local backend = opts.backend or (HAS_NATIVE and "native" or "bridge")

    local self = setmetatable({
        _service_uuid = opts.service_uuid,
        _char_uuid    = opts.char_uuid,
        _write_type   = opts.write_type or 0,
        _label        = opts.label or "BLE",
        _backend      = backend,
        _handle       = nil,
        _pipe         = nil,
        _closed       = false,
    }, BlePort)

    if backend == "native" then
        if not HAS_NATIVE then
            return nil, "ble_helper.dll not found"
        end
        local h = ffi.new("ble_handle_t[1]")
        local rc
        if opts.address then
            rc = ble_lib.ble_open(opts.address, h)
        elseif opts.device_name then
            rc = ble_lib.ble_open_by_name(opts.device_name,
                                           opts.scan_timeout or 5000, h)
        else
            return nil, "address or device_name required"
        end
        if rc ~= 0 then
            return nil, string.format("ble_open failed (%d): %s",
                                      rc, ffi.string(ble_lib.ble_last_error()))
        end
        self._handle = h[0]

    elseif backend == "bridge" then
        -- Named-pipe bridge to external process
        local pipe_name = opts.pipe_name or "\\\\.\\pipe\\ble_bridge"
        local pipe, err = self:_open_pipe(pipe_name)
        if not pipe then
            return nil, "bridge pipe open failed: " .. (err or "unknown")
        end
        self._pipe = pipe

        -- Send CONNECT command
        local connect_cmd
        if opts.address then
            connect_cmd = string.format("CONNECT addr=%016X\n", opts.address)
        elseif opts.device_name then
            connect_cmd = string.format("CONNECT name=%s timeout=%d\n",
                                         opts.device_name,
                                         opts.scan_timeout or 5000)
        end
        self:_pipe_write(connect_cmd)
        local resp = self:_pipe_read_line()
        if not resp or not resp:find("^OK") then
            return nil, "bridge CONNECT failed: " .. (resp or "no response")
        end
    else
        return nil, "unknown backend: " .. tostring(backend)
    end

    return self
end

--- Write data bytes through the BLE characteristic.
function BlePort:write(data)
    if self._closed then return nil, "closed" end

    if self._backend == "native" then
        local buf = ffi.new("uint8_t[?]", #data, data)
        local rc = ble_lib.ble_write(self._handle,
                                      self._service_uuid,
                                      self._char_uuid,
                                      buf, #data,
                                      self._write_type)
        if rc ~= 0 then
            return nil, string.format("ble_write failed (%d): %s",
                                      rc, ffi.string(ble_lib.ble_last_error()))
        end
        return #data

    elseif self._backend == "bridge" then
        -- Hex-encode the data for the text protocol
        local hex = {}
        for i = 1, #data do
            hex[i] = string.format("%02X", data:byte(i))
        end
        local cmd = string.format("WRITE svc=%s chr=%s type=%d data=%s\n",
                                   self._service_uuid,
                                   self._char_uuid,
                                   self._write_type,
                                   table.concat(hex))
        self:_pipe_write(cmd)
        local resp = self:_pipe_read_line()
        if not resp or not resp:find("^OK") then
            return nil, "bridge WRITE failed: " .. (resp or "no response")
        end
        return #data
    end
end

--- Read from the BLE characteristic.
function BlePort:read(max_bytes)
    max_bytes = max_bytes or 512
    if self._closed then return nil, "closed" end

    if self._backend == "native" then
        local buf = ffi.new("uint8_t[?]", max_bytes)
        local rc = ble_lib.ble_read(self._handle,
                                     self._service_uuid,
                                     self._char_uuid,
                                     buf, max_bytes)
        if rc < 0 then
            return nil, string.format("ble_read failed (%d)", rc)
        end
        return ffi.string(buf, rc)

    elseif self._backend == "bridge" then
        local cmd = string.format("READ svc=%s chr=%s max=%d\n",
                                   self._service_uuid,
                                   self._char_uuid,
                                   max_bytes)
        self:_pipe_write(cmd)
        local resp = self:_pipe_read_line()
        if not resp or not resp:find("^OK") then
            return nil, "bridge READ failed: " .. (resp or "no response")
        end
        -- Parse hex data after "OK data="
        local hex_str = resp:match("data=(%x+)")
        if not hex_str then return "" end
        local bytes = {}
        for i = 1, #hex_str, 2 do
            bytes[#bytes + 1] = string.char(tonumber(hex_str:sub(i, i+1), 16))
        end
        return table.concat(bytes)
    end
end

function BlePort:flush()
    -- BLE writes are inherently flushed per-write
end

function BlePort:close()
    if self._closed then return end
    self._closed = true
    if self._backend == "native" and self._handle then
        ble_lib.ble_close(self._handle)
        self._handle = nil
    elseif self._backend == "bridge" and self._pipe then
        pcall(self._pipe_write, self, "CLOSE\n")
        self:_close_pipe()
    end
end

function BlePort:label()
    return self._label
end

function BlePort:is_open()
    return not self._closed
end

---------------------------------------------------------------------------
-- Named-pipe helpers (bridge backend)
---------------------------------------------------------------------------
local kernel32_pipe

local function ensure_pipe_ffi()
    if kernel32_pipe then return end
    -- Reuse the kernel32 FFI from win32_serial or declare minimal set
    kernel32_pipe = ffi.load("kernel32")
end

function BlePort:_open_pipe(pipe_name)
    ensure_pipe_ffi()
    local h = kernel32_pipe.CreateFileA(
        pipe_name,
        0xC0000000,  -- GENERIC_READ | GENERIC_WRITE
        0, nil, 3,   -- OPEN_EXISTING
        0, nil
    )
    local INVALID = ffi.cast("HANDLE", ffi.new("uintptr_t", 0xFFFFFFFFFFFFFFFFULL))
    if h == INVALID then
        return nil, "cannot open pipe"
    end
    return h
end

function BlePort:_pipe_write(str)
    local buf = ffi.new("char[?]", #str, str)
    local written = ffi.new("DWORD[1]")
    kernel32_pipe.WriteFile(self._pipe, buf, #str, written, nil)
end

function BlePort:_pipe_read_line()
    -- Simple byte-at-a-time read until \n (adequate for control protocol)
    local chars = {}
    local one = ffi.new("char[1]")
    local nr  = ffi.new("DWORD[1]")
    for _ = 1, 4096 do
        local ok = kernel32_pipe.ReadFile(self._pipe, one, 1, nr, nil)
        if ok == 0 or nr[0] == 0 then break end
        local c = string.char(one[0])
        if c == "\n" then break end
        chars[#chars + 1] = c
    end
    return table.concat(chars)
end

function BlePort:_close_pipe()
    if self._pipe then
        kernel32_pipe.CloseHandle(self._pipe)
        self._pipe = nil
    end
end

return BlePort