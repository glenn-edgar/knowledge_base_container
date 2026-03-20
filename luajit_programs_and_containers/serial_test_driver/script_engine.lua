-- script_engine.lua
-- Message script engine for the serial test driver.
--
-- Script format: a Lua table (array) of entries.  Each entry is one of:
--
--   { type="msg",   data="HELLO\r\n" }          -- raw bytes
--   { type="msg",   data="...", hex=true }       -- hex-encoded bytes
--   { type="delay", ms=1000 }                    -- pause N milliseconds
--   { type="msg",   data="...", port="usb" }     -- target specific port
--   { type="label", name="start_block" }         -- named label (for logging)
--   { type="set",   key="seq", value=0 }         -- set a script variable
--   { type="template", fmt="SEQ=%d\r\n",         -- formatted message
--                       args={"seq"} }            -- references script vars
--
-- Messages are sent *continuously* (no inter-message gap) until a "delay"
-- entry is hit.  After the delay, sending resumes.  When the script is
-- exhausted it loops back to the beginning.

local ffi = require("ffi")

ffi.cdef[[
void Sleep(DWORD dwMilliseconds);
typedef unsigned long DWORD;

typedef union {
    struct {
        DWORD LowPart;
        long  HighPart;
    };
    int64_t QuadPart;
} LARGE_INTEGER;
BOOL QueryPerformanceCounter(LARGE_INTEGER* lpPerformanceCount);
BOOL QueryPerformanceFrequency(LARGE_INTEGER* lpFrequency);
]]

local kernel32 = ffi.load("kernel32")

---------------------------------------------------------------------------
-- High-resolution timer
---------------------------------------------------------------------------
local qpc_freq = ffi.new("LARGE_INTEGER")
kernel32.QueryPerformanceFrequency(qpc_freq)
local freq = tonumber(qpc_freq.QuadPart)

local function now_ms()
    local t = ffi.new("LARGE_INTEGER")
    kernel32.QueryPerformanceCounter(t)
    return tonumber(t.QuadPart) * 1000.0 / freq
end

local function sleep_ms(ms)
    if ms > 0 then
        kernel32.Sleep(ms)
    end
end

---------------------------------------------------------------------------
-- Script loader
---------------------------------------------------------------------------
local ScriptEngine = {}
ScriptEngine.__index = ScriptEngine

--- Create a new script engine.
-- @param script   table   array of script entries
-- @param ports    table   { name = port_object, ... }
--                          port_object must have :write(data) and :label()
-- @param opts     table   { log=print, loop=true, verbose=false,
--                           default_ports={"usb","bt","ble"} }
function ScriptEngine.new(script, ports, opts)
    opts = opts or {}
    local self = setmetatable({
        _script   = script,
        _ports    = ports,
        _index    = 1,
        _loop     = (opts.loop == nil) and true or opts.loop,
        _running  = false,
        _vars     = {},
        _log      = opts.log or print,
        _verbose  = opts.verbose or false,
        _cycle    = 0,
        _msg_count = 0,
        _err_count = 0,
        _default_ports = opts.default_ports,   -- nil = all ports
        _on_send  = opts.on_send,    -- callback(port_name, data, entry)
        _on_error = opts.on_error,   -- callback(port_name, err, entry)
        _on_delay = opts.on_delay,   -- callback(ms)
        _on_cycle = opts.on_cycle,   -- callback(cycle_number)
    }, ScriptEngine)

    -- Build default port list if not specified
    if not self._default_ports then
        self._default_ports = {}
        for name, _ in pairs(ports) do
            self._default_ports[#self._default_ports + 1] = name
        end
        table.sort(self._default_ports)
    end

    return self
end

--- Resolve which ports an entry targets.
local function resolve_ports(self, entry)
    if entry.port then
        -- Single named port
        if type(entry.port) == "string" then
            return { entry.port }
        end
        -- List of port names
        return entry.port
    end
    return self._default_ports
end

--- Expand a hex string to raw bytes.
local function hex_to_bytes(hex_str)
    return (hex_str:gsub("%s+", ""):gsub("..", function(h)
        return string.char(tonumber(h, 16))
    end))
end

--- Resolve message data from an entry.
local function resolve_data(self, entry)
    if entry.type == "template" then
        local args = {}
        for i, key in ipairs(entry.args or {}) do
            args[i] = self._vars[key] or 0
        end
        return string.format(entry.fmt, unpack(args))
    end

    local data = entry.data
    if entry.hex then
        data = hex_to_bytes(data)
    end
    return data
end

--- Send a message entry to all target ports.
local function send_entry(self, entry)
    local data = resolve_data(self, entry)
    local target_ports = resolve_ports(self, entry)

    for _, port_name in ipairs(target_ports) do
        local port = self._ports[port_name]
        if port and port:is_open() then
            local ok, err = port:write(data)
            if ok then
                self._msg_count = self._msg_count + 1
                if self._on_send then
                    self._on_send(port_name, data, entry)
                end
                if self._verbose then
                    self._log(string.format("[%s] TX %d bytes: %s",
                        port:label(), #data, self:_format_preview(data)))
                end
            else
                self._err_count = self._err_count + 1
                if self._on_error then
                    self._on_error(port_name, err, entry)
                end
                self._log(string.format("[%s] ERR: %s",
                    port:label(), tostring(err)))
            end
        end
    end
end

--- Format a data preview for logging.
function ScriptEngine:_format_preview(data, max_len)
    max_len = max_len or 40
    local preview = data:gsub("[%c]", function(c)
        if c == "\r" then return "\\r"
        elseif c == "\n" then return "\\n"
        elseif c == "\t" then return "\\t"
        else return string.format("\\x%02X", c:byte())
        end
    end)
    if #preview > max_len then
        preview = preview:sub(1, max_len) .. "..."
    end
    return preview
end

--- Process a single script entry.  Returns true if the engine should
--  continue immediately, false if it paused (delay) or stopped.
local function process_entry(self)
    local entry = self._script[self._index]
    if not entry then return false end

    local etype = entry.type

    if etype == "msg" or etype == "template" then
        send_entry(self, entry)

    elseif etype == "delay" then
        local ms = entry.ms or 0
        if self._verbose then
            self._log(string.format("[engine] delay %d ms", ms))
        end
        if self._on_delay then
            self._on_delay(ms)
        end
        sleep_ms(ms)

    elseif etype == "set" then
        self._vars[entry.key] = entry.value
        if self._verbose then
            self._log(string.format("[engine] set %s = %s",
                tostring(entry.key), tostring(entry.value)))
        end

    elseif etype == "label" then
        if self._verbose then
            self._log(string.format("[engine] --- %s ---", entry.name or ""))
        end
    else
        self._log(string.format("[engine] unknown entry type: %s",
            tostring(etype)))
    end

    -- Advance
    self._index = self._index + 1
    if self._index > #self._script then
        self._cycle = self._cycle + 1
        if self._on_cycle then
            self._on_cycle(self._cycle)
        end
        if self._loop then
            self._index = 1
            if self._verbose then
                self._log(string.format(
                    "[engine] === cycle %d complete, looping ===",
                    self._cycle))
            end
        else
            self._running = false
            self._log("[engine] script complete (no loop)")
            return false
        end
    end

    return true
end

--- Run the script engine.  Blocks until stopped or script ends (if no loop).
-- @param max_cycles  number  stop after N cycles (nil = unlimited)
function ScriptEngine:run(max_cycles)
    self._running = true
    self._index   = 1
    self._cycle   = 0

    self._log(string.format(
        "[engine] starting: %d entries, %d ports, loop=%s",
        #self._script, self:_count_ports(), tostring(self._loop)))

    while self._running do
        if max_cycles and self._cycle >= max_cycles then
            self._log(string.format(
                "[engine] reached max_cycles=%d, stopping", max_cycles))
            break
        end
        if not process_entry(self) and not self._loop then
            break
        end
    end

    self._log(string.format(
        "[engine] stopped: %d msgs sent, %d errors, %d cycles",
        self._msg_count, self._err_count, self._cycle))
end

--- Stop the engine (call from another coroutine or signal handler).
function ScriptEngine:stop()
    self._running = false
end

--- Reset counters and position.
function ScriptEngine:reset()
    self._index     = 1
    self._cycle     = 0
    self._msg_count = 0
    self._err_count = 0
    self._vars      = {}
end

--- Get stats.
function ScriptEngine:stats()
    return {
        messages = self._msg_count,
        errors   = self._err_count,
        cycles   = self._cycle,
        index    = self._index,
    }
end

function ScriptEngine:_count_ports()
    local n = 0
    for _ in pairs(self._ports) do n = n + 1 end
    return n
end

---------------------------------------------------------------------------
-- Script builder helpers
---------------------------------------------------------------------------
local S = {}

function S.msg(data, opts)
    opts = opts or {}
    return { type = "msg", data = data,
             hex = opts.hex, port = opts.port }
end

function S.hex(hex_str, opts)
    opts = opts or {}
    return { type = "msg", data = hex_str, hex = true, port = opts.port }
end

function S.delay(ms)
    return { type = "delay", ms = ms }
end

function S.label(name)
    return { type = "label", name = name }
end

function S.set(key, value)
    return { type = "set", key = key, value = value }
end

function S.template(fmt, args, opts)
    opts = opts or {}
    return { type = "template", fmt = fmt, args = args, port = opts.port }
end

ScriptEngine.S = S

return ScriptEngine