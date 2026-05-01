--[[
  nats_rpc.lua — LuaJIT FFI binding for libnats_rpc

  Usage:
    local rpc = require("lib.nats_rpc")

    -- Server
    local srv = rpc.RpcServer.new({ server = "nats://127.0.0.1:4222" })
    srv:register("math.add", function(request_json)
        -- parse, compute, return JSON string
        return '{"result":8}'
    end)
    srv:start("rpc")  -- blocks; run in a coroutine or thread

    -- Client
    local cli = rpc.RpcClient.new({ server = "nats://127.0.0.1:4222" })
    cli:connect()
    local result = cli:call("rpc.math.add", '{"a":5,"b":3}', 5.0)
    cli:disconnect()
    cli:destroy()
]]

local ffi = require("ffi")

-- C stdlib (may already be declared if key_store loaded first)
pcall(ffi.cdef, "void free(void *ptr); void *malloc(size_t size);")

-- ----------------------------------------------------------------
--  C declarations
-- ----------------------------------------------------------------

ffi.cdef[[
/* ---- RPC Status codes ---- */
typedef enum {
    RPC_OK = 0,
    RPC_ERR_INVALID_ARG,
    RPC_ERR_CONNECTION,
    RPC_ERR_TIMEOUT,
    RPC_ERR_ENCODE,
    RPC_ERR_DECODE,
    RPC_ERR_MEMORY,
    RPC_ERR_HANDLER,
    RPC_ERR_NOT_FOUND,
    RPC_ERR_NATS,
} rpc_status_t;

const char *rpc_status_str(rpc_status_t st);

/* ---- Configuration ---- */
typedef struct {
    const char *server;
    const char *namespace_;
    const char *instance_id;
    bool        enable_health;
} RpcConfig;

void rpc_config_defaults(RpcConfig *cfg);

/* ---- RPC Server ---- */
typedef struct RpcServer RpcServer;

/* Handler callback: receives request JSON, must return response JSON.
   The returned string is owned by the caller (library will free it). */
typedef char *(*rpc_handler_fn)(const char *request_json,
                                void *user_data);

rpc_status_t rpc_server_create(RpcServer **out, const RpcConfig *cfg);
void         rpc_server_destroy(RpcServer *srv);
rpc_status_t rpc_server_register(RpcServer *srv, const char *method,
                                 rpc_handler_fn handler, void *user_data);
rpc_status_t rpc_server_start(RpcServer *srv, const char *prefix);
rpc_status_t rpc_server_stop(RpcServer *srv);

/* ---- Handler stats ---- */
typedef struct {
    const char *method;
    uint64_t    calls;
    uint64_t    errors;
    double      avg_ms;
} RpcHandlerStats;

rpc_status_t rpc_server_get_stats(const RpcServer *srv,
                                  RpcHandlerStats **stats,
                                  size_t *count);
void         rpc_server_free_stats(RpcHandlerStats *stats, size_t count);

/* ---- RPC Client ---- */
typedef struct RpcClient RpcClient;

rpc_status_t rpc_client_create(RpcClient **out, const RpcConfig *cfg);
void         rpc_client_destroy(RpcClient *cli);
rpc_status_t rpc_client_connect(RpcClient *cli);
rpc_status_t rpc_client_disconnect(RpcClient *cli);

rpc_status_t rpc_client_call(RpcClient *cli,
                             const char *method,
                             const char *request_json,
                             double timeout_s,
                             char **response_json);

/* ---- Batch calls ---- */
typedef struct {
    const char *method;
    const char *request_json;
    char       *response_json;   /* filled on success; caller must free */
    rpc_status_t status;
} RpcBatchItem;

rpc_status_t rpc_client_batch(RpcClient *cli,
                              RpcBatchItem *items,
                              size_t count,
                              double timeout_s);
]]

-- ----------------------------------------------------------------
--  Load shared library
-- ----------------------------------------------------------------

local C = ffi.load("nats_rpc")

-- ----------------------------------------------------------------
--  Helpers
-- ----------------------------------------------------------------

local RPC_OK = 0

local function check(st)
    if st == RPC_OK then return true end
    error("RPC error: " .. ffi.string(C.rpc_status_str(st)), 2)
end

-- ----------------------------------------------------------------
--  Keep Lua callbacks alive (prevent GC)
-- ----------------------------------------------------------------

local _callback_refs = {}

-- ----------------------------------------------------------------
--  RpcServer class
-- ----------------------------------------------------------------

local RpcServer = {}
RpcServer.__index = RpcServer

--- Create a new RPC server.
-- @param opts table  { server, namespace_, instance_id, enable_health }
function RpcServer.new(opts)
    opts = opts or {}
    local cfg = ffi.new("RpcConfig")
    C.rpc_config_defaults(cfg)
    if opts.server       then cfg.server      = opts.server end
    if opts.namespace_   then cfg.namespace_   = opts.namespace_ end
    if opts.instance_id  then cfg.instance_id  = opts.instance_id end
    if opts.enable_health ~= nil then cfg.enable_health = opts.enable_health end

    local handle = ffi.new("RpcServer*[1]")
    check(C.rpc_server_create(handle, cfg))
    local self = setmetatable({}, RpcServer)
    self._handle = handle[0]
    self._callbacks = {}
    return self
end

--- Destroy the server.
function RpcServer:destroy()
    if self._handle ~= nil then
        C.rpc_server_destroy(self._handle)
        -- Remove callback refs
        for _, ref in pairs(self._callbacks) do
            _callback_refs[ref] = nil
        end
        self._handle = nil
    end
end

--- Register a method handler.
-- @param method string  Method name (e.g. "math.add")
-- @param handler function(request_json) -> response_json
function RpcServer:register(method, handler)
    -- Create a C callback that wraps the Lua function.
    -- The returned string must be malloc'd (library frees it).
    local cb = ffi.cast("rpc_handler_fn", function(req_json, _ud)
        local ok, result = pcall(handler, ffi.string(req_json))
        if not ok then
            result = '{"error":"' .. tostring(result):gsub('"', '\\"') .. '"}'
        end
        -- Must return a malloc'd copy
        local len = #result
        local buf = ffi.C.malloc(len + 1)
        ffi.copy(buf, result, len + 1)
        return ffi.cast("char*", buf)
    end)

    -- Keep reference alive
    local ref_id = tostring(cb)
    _callback_refs[ref_id] = cb
    self._callbacks[method] = ref_id

    check(C.rpc_server_register(self._handle, method, cb, nil))
end

--- Start the server (blocks, subscribes to method subjects).
-- @param prefix string  Subject prefix (e.g. "rpc")
function RpcServer:start(prefix)
    check(C.rpc_server_start(self._handle, prefix))
end

--- Stop the server.
function RpcServer:stop()
    check(C.rpc_server_stop(self._handle))
end

--- Get handler statistics.
-- @return table of { method, calls, errors, avg_ms }
function RpcServer:get_stats()
    local stats = ffi.new("RpcHandlerStats*[1]")
    local cnt   = ffi.new("size_t[1]")
    check(C.rpc_server_get_stats(self._handle, stats, cnt))
    local result = {}
    for i = 0, tonumber(cnt[0]) - 1 do
        result[#result + 1] = {
            method = ffi.string(stats[0][i].method),
            calls  = tonumber(stats[0][i].calls),
            errors = tonumber(stats[0][i].errors),
            avg_ms = stats[0][i].avg_ms,
        }
    end
    C.rpc_server_free_stats(stats[0], cnt[0])
    return result
end

-- ----------------------------------------------------------------
--  RpcClient class
-- ----------------------------------------------------------------

local RpcClient = {}
RpcClient.__index = RpcClient

--- Create a new RPC client.
-- @param opts table  { server, namespace_, instance_id }
function RpcClient.new(opts)
    opts = opts or {}
    local cfg = ffi.new("RpcConfig")
    C.rpc_config_defaults(cfg)
    if opts.server      then cfg.server      = opts.server end
    if opts.namespace_  then cfg.namespace_   = opts.namespace_ end
    if opts.instance_id then cfg.instance_id  = opts.instance_id end

    local handle = ffi.new("RpcClient*[1]")
    check(C.rpc_client_create(handle, cfg))
    local self = setmetatable({}, RpcClient)
    self._handle = handle[0]
    return self
end

--- Destroy the client.
function RpcClient:destroy()
    if self._handle ~= nil then
        C.rpc_client_destroy(self._handle)
        self._handle = nil
    end
end

--- Connect to NATS.
function RpcClient:connect()
    check(C.rpc_client_connect(self._handle))
end

--- Disconnect from NATS.
function RpcClient:disconnect()
    check(C.rpc_client_disconnect(self._handle))
end

--- Call a remote method.
-- @param method       string  Full method path (e.g. "rpc.math.add")
-- @param request_json string  JSON request payload
-- @param timeout_s    number  Timeout in seconds (default 5.0)
-- @return response_json string
function RpcClient:call(method, request_json, timeout_s)
    timeout_s = timeout_s or 5.0
    local resp = ffi.new("char*[1]")
    check(C.rpc_client_call(self._handle, method, request_json, timeout_s, resp))
    local result = ffi.string(resp[0])
    ffi.C.free(resp[0])
    return result
end

--- Batch call multiple methods.
-- @param items table of { method=string, request_json=string }
-- @param timeout_s number (default 5.0)
-- @return table of { method, request_json, response_json, status, status_str }
function RpcClient:batch(items, timeout_s)
    timeout_s = timeout_s or 5.0
    local n = #items
    local c_items = ffi.new("RpcBatchItem[?]", n)
    for i = 1, n do
        c_items[i - 1].method       = items[i].method
        c_items[i - 1].request_json = items[i].request_json
        c_items[i - 1].response_json = nil
        c_items[i - 1].status        = 0
    end

    check(C.rpc_client_batch(self._handle, c_items, n, timeout_s))

    local results = {}
    for i = 0, n - 1 do
        local resp = nil
        if c_items[i].response_json ~= nil then
            resp = ffi.string(c_items[i].response_json)
            ffi.C.free(c_items[i].response_json)
        end
        results[#results + 1] = {
            method       = ffi.string(c_items[i].method),
            request_json = ffi.string(c_items[i].request_json),
            response_json = resp,
            status        = tonumber(c_items[i].status),
            status_str    = ffi.string(C.rpc_status_str(c_items[i].status)),
        }
    end
    return results
end

-- ----------------------------------------------------------------
--  Module
-- ----------------------------------------------------------------

return {
    RpcServer = RpcServer,
    RpcClient = RpcClient,
    status_str = function(st)
        return ffi.string(C.rpc_status_str(st))
    end,
    RPC_OK              = 0,
    RPC_ERR_INVALID_ARG = 1,
    RPC_ERR_CONNECTION  = 2,
    RPC_ERR_TIMEOUT     = 3,
    RPC_ERR_ENCODE      = 4,
    RPC_ERR_DECODE      = 5,
    RPC_ERR_MEMORY      = 6,
    RPC_ERR_HANDLER     = 7,
    RPC_ERR_NOT_FOUND   = 8,
    RPC_ERR_NATS        = 9,
    _C = C,
}