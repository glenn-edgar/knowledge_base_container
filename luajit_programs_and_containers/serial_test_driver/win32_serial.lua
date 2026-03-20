-- win32_serial.lua
-- Win32 COM port wrapper for LuaJIT FFI
-- Handles USB Serial and Bluetooth SPP (both appear as COMn on Windows)

local ffi = require("ffi")
local bit = require("bit")

ffi.cdef[[
// --- Win32 types ---
typedef void*          HANDLE;
typedef unsigned long  DWORD;
typedef int            BOOL;
typedef unsigned char  BYTE;
typedef unsigned short WORD;
typedef const char*    LPCSTR;
typedef void*          LPVOID;
typedef DWORD*         LPDWORD;
typedef void*          LPOVERLAPPED;

static const DWORD GENERIC_READ       = 0x80000000;
static const DWORD GENERIC_WRITE      = 0x40000000;
static const DWORD OPEN_EXISTING      = 3;
static const DWORD FILE_ATTRIBUTE_NORMAL = 0x80;

static const DWORD INVALID_HANDLE_VALUE_DWORD = 0xFFFFFFFF;

// --- DCB (serial config) ---
typedef struct {
    DWORD DCBlength;
    DWORD BaudRate;
    DWORD fBinary          :1;
    DWORD fParity          :1;
    DWORD fOutxCtsFlow     :1;
    DWORD fOutxDsrFlow     :1;
    DWORD fDtrControl      :2;
    DWORD fDsrSensitivity  :1;
    DWORD fTXContinueOnXoff:1;
    DWORD fOutX            :1;
    DWORD fInX             :1;
    DWORD fErrorChar       :1;
    DWORD fNull            :1;
    DWORD fRtsControl      :2;
    DWORD fAbortOnError    :1;
    DWORD fDummy2          :17;
    WORD  wReserved;
    WORD  XonLim;
    WORD  XoffLim;
    BYTE  ByteSize;
    BYTE  Parity;
    BYTE  StopBits;
    char  XonChar;
    char  XoffChar;
    char  ErrorChar;
    char  EofChar;
    char  EvtChar;
    WORD  wReserved1;
} DCB;

// --- COMMTIMEOUTS ---
typedef struct {
    DWORD ReadIntervalTimeout;
    DWORD ReadTotalTimeoutMultiplier;
    DWORD ReadTotalTimeoutConstant;
    DWORD WriteTotalTimeoutMultiplier;
    DWORD WriteTotalTimeoutConstant;
} COMMTIMEOUTS;

// --- Win32 API ---
HANDLE CreateFileA(
    LPCSTR lpFileName,
    DWORD  dwDesiredAccess,
    DWORD  dwShareMode,
    void*  lpSecurityAttributes,
    DWORD  dwCreationDisposition,
    DWORD  dwFlagsAndAttributes,
    HANDLE hTemplateFile
);

BOOL CloseHandle(HANDLE hObject);

BOOL WriteFile(
    HANDLE       hFile,
    const void*  lpBuffer,
    DWORD        nNumberOfBytesToWrite,
    LPDWORD      lpNumberOfBytesWritten,
    LPOVERLAPPED lpOverlapped
);

BOOL ReadFile(
    HANDLE       hFile,
    void*        lpBuffer,
    DWORD        nNumberOfBytesToRead,
    LPDWORD      lpNumberOfBytesRead,
    LPOVERLAPPED lpOverlapped
);

BOOL GetCommState(HANDLE hFile, DCB* lpDCB);
BOOL SetCommState(HANDLE hFile, DCB* lpDCB);
BOOL SetCommTimeouts(HANDLE hFile, COMMTIMEOUTS* lpCommTimeouts);
BOOL PurgeComm(HANDLE hFile, DWORD dwFlags);
BOOL FlushFileBuffers(HANDLE hFile);
DWORD GetLastError(void);

void Sleep(DWORD dwMilliseconds);
]]

local C = ffi.C
local kernel32 = ffi.load("kernel32")

-- Purge flags
local PURGE_TXABORT = 0x0001
local PURGE_RXABORT = 0x0002
local PURGE_TXCLEAR = 0x0004
local PURGE_RXCLEAR = 0x0008

local INVALID_HANDLE = ffi.cast("HANDLE", ffi.new("uintptr_t", 0xFFFFFFFFFFFFFFFFULL))

-- Parity constants
local PARITY = { none = 0, odd = 1, even = 2, mark = 3, space = 4 }
-- Stop bit constants
local STOPBITS = { ["1"] = 0, ["1.5"] = 1, ["2"] = 2 }

---------------------------------------------------------------------------
-- Serial Port object
---------------------------------------------------------------------------
local SerialPort = {}
SerialPort.__index = SerialPort

--- Open a COM port.
-- @param port_name  string  e.g. "COM3" or "\\\\.\\COM12"
-- @param opts       table   { baud=115200, databits=8, parity="none",
--                              stopbits="1", read_timeout_ms=100,
--                              write_timeout_ms=100 }
-- @return SerialPort handle, or nil + error string
function SerialPort.open(port_name, opts)
    opts = opts or {}
    local baud       = opts.baud           or 115200
    local databits   = opts.databits       or 8
    local parity     = opts.parity         or "none"
    local stopbits   = opts.stopbits       or "1"
    local rd_timeout = opts.read_timeout_ms  or 100
    local wr_timeout = opts.write_timeout_ms or 100

    -- Prefix for COM ports > 9
    local dev_path = port_name
    if not dev_path:find("^\\\\") then
        dev_path = "\\\\.\\" .. dev_path
    end

    local h = kernel32.CreateFileA(
        dev_path,
        bit.bor(0x80000000, 0x40000000),  -- GENERIC_READ | GENERIC_WRITE
        0,       -- no sharing
        nil,     -- no security attrs
        3,       -- OPEN_EXISTING
        0x80,    -- FILE_ATTRIBUTE_NORMAL
        nil
    )

    if h == INVALID_HANDLE then
        return nil, string.format("CreateFileA failed for %s (err=%d)",
                                  port_name, kernel32.GetLastError())
    end

    -- Configure DCB
    local dcb = ffi.new("DCB")
    dcb.DCBlength = ffi.sizeof("DCB")
    if kernel32.GetCommState(h, dcb) == 0 then
        kernel32.CloseHandle(h)
        return nil, "GetCommState failed"
    end

    dcb.BaudRate  = baud
    dcb.ByteSize  = databits
    dcb.Parity    = PARITY[parity] or 0
    dcb.StopBits  = STOPBITS[tostring(stopbits)] or 0
    dcb.fBinary   = 1
    dcb.fParity   = (parity ~= "none") and 1 or 0

    if kernel32.SetCommState(h, dcb) == 0 then
        kernel32.CloseHandle(h)
        return nil, "SetCommState failed"
    end

    -- Timeouts
    local timeouts = ffi.new("COMMTIMEOUTS")
    timeouts.ReadIntervalTimeout         = 50
    timeouts.ReadTotalTimeoutMultiplier  = 0
    timeouts.ReadTotalTimeoutConstant    = rd_timeout
    timeouts.WriteTotalTimeoutMultiplier = 0
    timeouts.WriteTotalTimeoutConstant   = wr_timeout

    if kernel32.SetCommTimeouts(h, timeouts) == 0 then
        kernel32.CloseHandle(h)
        return nil, "SetCommTimeouts failed"
    end

    -- Flush any stale data
    kernel32.PurgeComm(h, bit.bor(PURGE_TXCLEAR, PURGE_RXCLEAR,
                                   PURGE_TXABORT, PURGE_RXABORT))

    local self = setmetatable({
        _handle    = h,
        _port_name = port_name,
        _closed    = false,
        _label     = opts.label or port_name,
    }, SerialPort)

    return self
end

--- Write raw bytes.
-- @return bytes_written or nil + error
function SerialPort:write(data)
    if self._closed then return nil, "port closed" end
    local buf = ffi.new("char[?]", #data, data)
    local written = ffi.new("DWORD[1]")
    local ok = kernel32.WriteFile(self._handle, buf, #data, written, nil)
    if ok == 0 then
        return nil, string.format("WriteFile failed (err=%d)", kernel32.GetLastError())
    end
    return tonumber(written[0])
end

--- Read up to max_bytes (subject to timeout).
-- @return string (may be empty on timeout) or nil + error
function SerialPort:read(max_bytes)
    max_bytes = max_bytes or 1024
    if self._closed then return nil, "port closed" end
    local buf  = ffi.new("char[?]", max_bytes)
    local nread = ffi.new("DWORD[1]")
    local ok = kernel32.ReadFile(self._handle, buf, max_bytes, nread, nil)
    if ok == 0 then
        return nil, string.format("ReadFile failed (err=%d)", kernel32.GetLastError())
    end
    return ffi.string(buf, tonumber(nread[0]))
end

--- Flush transmit buffer.
function SerialPort:flush()
    if not self._closed then
        kernel32.FlushFileBuffers(self._handle)
    end
end

--- Purge rx/tx buffers.
function SerialPort:purge()
    if not self._closed then
        kernel32.PurgeComm(self._handle, bit.bor(PURGE_TXCLEAR, PURGE_RXCLEAR))
    end
end

--- Close the port.
function SerialPort:close()
    if not self._closed then
        kernel32.CloseHandle(self._handle)
        self._closed = true
    end
end

--- Port label for logging.
function SerialPort:label()
    return self._label
end

function SerialPort:is_open()
    return not self._closed
end

return SerialPort