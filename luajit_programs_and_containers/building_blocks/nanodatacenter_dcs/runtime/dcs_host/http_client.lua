-- =============================================================================
-- http_client.lua -- minimal synchronous JSON HTTP client for dcs.lua.
--
-- Used by broker_client.lua mutation methods (start/stop/run/rm) to POST to
-- the docker_host_broker. Synchronous-blocking is acceptable here: every
-- caller sits on a control-plane edge (sys_sm sync_st boot, sys_sm
-- teardown_st shutdown, node_sm setup_st per-assignment) where the work
-- is already inherently slow and bounded in volume. We are not on the
-- chain-tree tick hot path.
--
-- Returns three-valued results so the caller can distinguish transport
-- errors (broker dead → status=nil) from HTTP errors (status set + body):
--
--   local status, body, err = http_client.post_json("/v1/cmd/start", {...})
--
--   * status == nil           → transport error (broker unreachable, timeout,
--                                bad URL); err describes. Callers fail closed.
--   * status == 202           → broker accepted; body has cmd_id/accepted_at
--   * status == 404|409|5xx   → broker rejected; body carries `error` string
-- =============================================================================

local socket_http = require("socket.http")
local ltn12       = require("ltn12")
local dkjson      = require("dkjson")

local M = {}

local cfg = {
  base_url  = nil,    -- e.g. "http://127.0.0.1:9100"
  timeout_s = 10,     -- per-request connect + I/O budget
}

function M.configure(opts)
  if opts.base_url  then cfg.base_url  = opts.base_url  end
  if opts.timeout_s then cfg.timeout_s = tonumber(opts.timeout_s) or cfg.timeout_s end
end

function M.is_configured() return cfg.base_url ~= nil end

-- POST a JSON body to <base_url><path>. Returns (status_code, body_table, err).
function M.post_json(path, body)
  if not cfg.base_url then return nil, nil, "http_client: not configured" end

  local payload = dkjson.encode(body or {})
  socket_http.TIMEOUT = cfg.timeout_s

  local resp = {}
  local ok, _, code = pcall(socket_http.request, {
    url     = cfg.base_url .. path,
    method  = "POST",
    headers = {
      ["Content-Type"]   = "application/json",
      ["Content-Length"] = tostring(#payload),
    },
    source = ltn12.source.string(payload),
    sink   = ltn12.sink.table(resp),
  })

  if not ok then return nil, nil, "http_client: pcall: " .. tostring(_) end
  -- socket.http returns code as the second value; on transport errors that
  -- value is a string (e.g. "connection refused", "timeout") rather than a
  -- numeric HTTP status.
  if type(code) ~= "number" then return nil, nil, "http_client: " .. tostring(code) end

  local raw = table.concat(resp)
  local decoded
  if #raw > 0 then decoded = dkjson.decode(raw) end
  return code, decoded or {}, nil
end

return M
