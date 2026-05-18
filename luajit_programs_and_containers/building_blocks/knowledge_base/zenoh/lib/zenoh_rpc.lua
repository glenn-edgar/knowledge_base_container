--[[
  zenoh_rpc.lua — LuaJIT FFI binding for libzenoh_rpc (client side only)

  Scope:
    Client-side z_get / RPC call. The synchronous _call() pattern is safe
    for LuaJIT because the C library blocks the calling thread on a
    pthread_cond_timedwait inside zenoh_rpc_client_call() — no Lua
    callback runs on a foreign thread.

  Server-side queryables are NOT bound here. zenoh-pico would invoke a
  Lua handler from its internal read thread, hitting the same
  cross-thread-callback risk that drove the pub_sub queue+poll API.
  To add server-side later, mirror the pub_sub queue pattern in the
  C library (query → queue → main-thread reply API).

  Usage:
    local zrpc = require("lib.zenoh_rpc")
    local zt   = require("lib.zenoh_token")

    local cli = zrpc.Client.new({ locators = { "udp/127.0.0.1:7447" } })
    cli:connect()

    local reply = cli:call(zt.hash("math.add"), '{"a":5,"b":3}', 5000)
    print(reply)         -- response payload as string

    cli:disconnect()
    cli:destroy()
]]

local ffi = require("ffi")

ffi.cdef[[
typedef enum {
    ZRPC_OK = 0,
    ZRPC_ERR_INVALID_ARG,
    ZRPC_ERR_CONNECTION,
    ZRPC_ERR_TIMEOUT,
    ZRPC_ERR_MEMORY,
    ZRPC_ERR_NOT_CONNECTED,
    ZRPC_ERR_NO_REPLY,
    ZRPC_ERR_HANDLER,
    ZRPC_ERR_ZENOH
} zrpc_status_t;

const char *zrpc_status_str(zrpc_status_t st);

typedef struct {
    const char *const *locators;
    size_t             n_locators;
    const char        *mode;
    bool               enable_scout;
    const char        *client_name;
} ZenohRpcConfig;

void zenoh_rpc_config_defaults(ZenohRpcConfig *cfg);

typedef struct ZenohRpcClient ZenohRpcClient;

zrpc_status_t zenoh_rpc_client_create(ZenohRpcClient **out, const ZenohRpcConfig *cfg);
void          zenoh_rpc_client_destroy(ZenohRpcClient *cli);
zrpc_status_t zenoh_rpc_client_connect(ZenohRpcClient *cli);
zrpc_status_t zenoh_rpc_client_disconnect(ZenohRpcClient *cli);

zrpc_status_t zenoh_rpc_client_call(ZenohRpcClient *cli,
                                    uint32_t token,
                                    const uint8_t *req, size_t req_len,
                                    uint32_t timeout_ms,
                                    uint8_t **resp, size_t *resp_len);

void  free(void *);
]]

local C = ffi.load("zenoh_rpc")

local function check(st, where)
    if st ~= C.ZRPC_OK then
        error((where or "zenoh_rpc") .. ": " ..
              ffi.string(C.zrpc_status_str(st)), 2)
    end
end

local Client = {}
Client.__index = Client

function Client.new(opts)
    opts = opts or {}
    local self = setmetatable({}, Client)
    self._keep = {}

    local function build_cstr_array(strs)
        if not strs or #strs == 0 then return nil, 0 end
        local arr = ffi.new("const char *[?]", #strs)
        for i = 1, #strs do
            local s = strs[i]
            local cs = ffi.cast("const char *", s)
            arr[i - 1] = cs
            table.insert(self._keep, s)
            table.insert(self._keep, cs)
        end
        table.insert(self._keep, arr)
        return arr, #strs
    end

    local cfg = ffi.new("ZenohRpcConfig")
    C.zenoh_rpc_config_defaults(cfg)
    local locs_arr, locs_n = build_cstr_array(opts.locators)
    cfg.locators     = locs_arr
    cfg.n_locators   = locs_n
    cfg.mode         = opts.mode         or "client"
    cfg.enable_scout = opts.enable_scout or false
    cfg.client_name  = opts.client_name
    table.insert(self._keep, cfg.mode)
    if cfg.client_name then table.insert(self._keep, cfg.client_name) end

    local h = ffi.new("ZenohRpcClient*[1]")
    check(C.zenoh_rpc_client_create(h, cfg), "Client.new")
    self._handle = h[0]
    self._connected = false
    return self
end

function Client:connect()
    check(C.zenoh_rpc_client_connect(self._handle), "Client:connect")
    self._connected = true
end

function Client:disconnect()
    if self._connected then
        check(C.zenoh_rpc_client_disconnect(self._handle), "Client:disconnect")
        self._connected = false
    end
end

function Client:destroy()
    if self._handle ~= nil then
        if self._connected then self:disconnect() end
        C.zenoh_rpc_client_destroy(self._handle)
        self._handle = nil
    end
end

--- Synchronous RPC call.
-- @param token       uint32 method token
-- @param req         request payload string (may be "" or nil)
-- @param timeout_ms  timeout in milliseconds (default 5000)
-- @return            reply payload string, or raises on timeout / error
function Client:call(token, req, timeout_ms)
    if type(token) ~= "number" then
        error("Client:call: token must be a number", 2)
    end
    req = req or ""
    if type(req) ~= "string" then
        error("Client:call: req must be a string", 2)
    end
    timeout_ms = timeout_ms or 5000

    local req_buf = (#req > 0) and ffi.cast("const uint8_t *", req) or nil
    local resp_pp = ffi.new("uint8_t*[1]")
    local resp_lp = ffi.new("size_t[1]")
    local st = C.zenoh_rpc_client_call(self._handle, token,
                                       req_buf, #req,
                                       timeout_ms,
                                       resp_pp, resp_lp)
    if st == C.ZRPC_ERR_TIMEOUT then
        if resp_pp[0] ~= nil then C.free(resp_pp[0]) end
        error("Client:call: timeout after " .. timeout_ms .. "ms", 2)
    end
    if st == C.ZRPC_ERR_HANDLER then
        -- Server returned an error reply; payload carries the error string.
        local emsg = resp_pp[0] ~= nil and ffi.string(resp_pp[0], resp_lp[0]) or ""
        if resp_pp[0] ~= nil then C.free(resp_pp[0]) end
        error("Client:call: handler error: " .. emsg, 2)
    end
    check(st, "Client:call")

    local payload = (resp_pp[0] ~= nil and resp_lp[0] > 0)
                       and ffi.string(resp_pp[0], resp_lp[0])
                       or ""
    if resp_pp[0] ~= nil then C.free(resp_pp[0]) end
    return payload
end

return { Client = Client, _C = C }
