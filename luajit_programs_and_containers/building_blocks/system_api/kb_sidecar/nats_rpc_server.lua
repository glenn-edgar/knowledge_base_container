--[[
  nats_rpc_server.lua — Local RPC server wrapper with correct FFI bindings

  The shared nats_rpc.lua has a stale FFI declaration that doesn't match
  the current libnats_rpc.so header. This module provides a corrected
  server-only binding for the sidecar.
]]

local ffi = require("ffi")

pcall(ffi.cdef, "void free(void *ptr); void *malloc(size_t size);")

ffi.cdef[[
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

  typedef struct {
    const char *server;
    const char *namespace_;
    const char *instance_id;
    bool        enable_health;
  } RpcConfig;

  void rpc_config_defaults(RpcConfig *cfg);

  typedef struct RpcServer RpcServer;

  /* Correct handler signature: returns status, result via out-pointer */
  typedef rpc_status_t (*rpc_handler_fn)(const char *params_json,
                                         void       *user_data,
                                         char      **result_json);

  rpc_status_t rpc_server_create(RpcServer **out, const RpcConfig *cfg);
  void         rpc_server_destroy(RpcServer *srv);

  rpc_status_t rpc_server_register(RpcServer *srv, const char *method,
                                   rpc_handler_fn handler, void *user_data,
                                   bool instance_specific);

  rpc_status_t rpc_server_start(RpcServer *srv, const char *prefix);
  rpc_status_t rpc_server_stop(RpcServer *srv);
  void         rpc_server_wait(RpcServer *srv);
]]

local C = ffi.load("nats_rpc")

local RPC_OK = 0

local function check(st)
  if st == RPC_OK then return true end
  error("RPC error: " .. ffi.string(C.rpc_status_str(st)), 2)
end

-- prevent GC of callbacks
local _callback_refs = {}

local RpcServer = {}
RpcServer.__index = RpcServer

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

function RpcServer:destroy()
  if self._handle ~= nil then
    C.rpc_server_destroy(self._handle)
    for _, ref in pairs(self._callbacks) do
      _callback_refs[ref] = nil
    end
    self._handle = nil
  end
end

function RpcServer:register(method, handler)
  local cb = ffi.cast("rpc_handler_fn", function(params_json, _ud, result_out)
    local ok, result = pcall(handler, ffi.string(params_json))
    if not ok then
      result = '{"error":"' .. tostring(result):gsub('"', '\\"') .. '"}'
    end
    local len = #result
    local buf = ffi.C.malloc(len + 1)
    ffi.copy(buf, result, len + 1)
    result_out[0] = ffi.cast("char*", buf)
    return ok and 0 or 7  -- RPC_OK or RPC_ERR_HANDLER
  end)

  local ref_id = tostring(cb)
  _callback_refs[ref_id] = cb
  self._callbacks[method] = ref_id

  check(C.rpc_server_register(self._handle, method, cb, nil, false))
end

function RpcServer:start(prefix)
  check(C.rpc_server_start(self._handle, prefix))
end

function RpcServer:wait()
  C.rpc_server_wait(self._handle)
end

function RpcServer:stop()
  check(C.rpc_server_stop(self._handle))
end

return { RpcServer = RpcServer, _C = C }
