-- =============================================================================
-- infra_publisher.lua -- system_control's writer for the runtime infra registry.
--
-- Reads the per-service registry headers planted at build_kb time
-- (`subsystems/infrastructure_registry.lua`) and the broker snapshot
-- (via broker_client), then writes runtime addressing into:
--
--   system.<sys>.site.<S>.infrastructure.registry.service.<service_type>.
--     KB_STATUS_FIELD.{ host, port, healthy, last_seen }
--
-- Each tick is independent: re-read registry headers (cached), poll
-- broker_client, write status fields. Idempotent.
--
-- Caller invokes M.publish_once(conn, opts) once per chain-tree tick.
-- =============================================================================

local broker_client = require("broker_client")
local kb_status     = require("kb_status")
local ndc_paths     = require("ndc_paths")
local cjson         = require("dkjson")

local M = {}

local function escape(s) return tostring(s):gsub("'", "''") end

---------------------------------------------------------------------------
-- Registry-header reader (cached after first hit)
---------------------------------------------------------------------------

local registry_cache = nil  -- { { service_type, def_name, contract_port, contract_protocol } }

local function load_registry(conn, site)
  if registry_cache then return registry_cache end

  local prefix = ndc_paths.site_path(site,
    "infrastructure.registry.service")
  local sql = string.format([[
    SELECT name, properties::text AS props
      FROM knowledge_base
     WHERE label = 'service'
       AND path <@ '%s'::ltree
       AND nlevel(path) = nlevel('%s'::ltree) + 1
     ORDER BY name
  ]], escape(prefix), escape(prefix))

  local sth, err = conn:prepare(sql)
  if not sth then return nil, "prepare: " .. tostring(err) end
  local ok, eerr = sth:execute()
  if not ok then sth:close(); return nil, "execute: " .. tostring(eerr) end

  local out = {}
  while true do
    local row = sth:fetch(true)
    if not row then break end
    local props = cjson.decode(row.props or "{}") or {}
    out[#out + 1] = {
      service_type      = row.name,
      def_name          = props.def_name,
      contract_port     = tonumber(props.contract_port) or 0,
      contract_protocol = props.contract_protocol or "tcp",
    }
  end
  sth:close()

  registry_cache = out
  return out
end

-- Exposed for tests / reset on container respawn.
function M.reset_cache()
  registry_cache = nil
end

---------------------------------------------------------------------------
-- Broker-snapshot lookup: find a running container whose `definition`
-- (set by node_control / kb_assignments at run time) OR `image` matches
-- the given def_name.
---------------------------------------------------------------------------

-- Map def_name -> a substring expected to appear in the container's image
-- tag. (broker snapshot exposes `image` per container; node_control does
-- not currently publish `definition` into the snapshot, so we match on
-- image for stable behavior.)
local DEF_IMAGE_MATCH = {
  postgres  = "pgvector/pgvector",
  nats      = "nats-js-ram",
  mosquitto = "mosquitto-ram-ws",
  kv_bridge = "kv-bridge",
}

-- Map def_name -> the canonical container name we expect on planner-net
-- (mirror of topology.lua infra block — the names are stable across
-- deployments per the v3 platform locked design).
local DEF_NAME_MATCH = {
  postgres  = "pg-vector",
  nats      = "nats-js-ram",
  mosquitto = "mosquitto-ram-ws_main",
  kv_bridge = "kv-bridge",
}

-- For one registry entry, return { host=, healthy=, last_seen= } from the
-- current broker snapshot. host is the container name (resolves via
-- planner-net DNS); healthy is true iff broker observed state="running".
local function resolve_one(conn, entry)
  local name = DEF_NAME_MATCH[entry.def_name]
  if not name then
    return { host = "", healthy = false, last_seen = 0,
             reason = "no DEF_NAME_MATCH for def=" .. tostring(entry.def_name) }
  end

  local ci = broker_client.get_container(conn, name)
  if not ci then
    return { host = name, healthy = false, last_seen = 0,
             reason = "container not in broker snapshot" }
  end

  local healthy = (ci.state == "running")
  -- Belt-and-braces image check: warn (but don't fail-publish) if image
  -- doesn't match the def's expected substring. Catches a mis-named
  -- container (e.g. operator ran something else under our slot).
  local img_substr = DEF_IMAGE_MATCH[entry.def_name]
  if healthy and img_substr and ci.image and not ci.image:find(img_substr, 1, true) then
    healthy = false
    return { host = name, healthy = false, last_seen = 0,
             reason = string.format("image mismatch: expected %q in %q",
               img_substr, tostring(ci.image)) }
  end

  return {
    host      = name,
    healthy   = healthy,
    last_seen = healthy and os.time() or 0,
    reason    = healthy and "ok" or ("state=" .. tostring(ci.state)),
  }
end

---------------------------------------------------------------------------
-- Public: one publish pass over every registered service.
--
-- Returns { service_type -> { host, healthy, last_seen, reason } } for
-- callers that want to log a summary; errors are logged to stderr but
-- never bubble up (this runs on the system_control monitor loop and
-- must not crash the supervisor).
---------------------------------------------------------------------------

function M.publish_once(conn, opts)
  opts = opts or {}
  local site   = opts.site   or error("publish_once: site required")
  local logger = opts.logger or function() end

  if not conn then
    logger("infra_publisher: no pg conn; skipping")
    return nil
  end

  local registry, err = load_registry(conn, site)
  if not registry then
    logger("infra_publisher: load_registry failed: " .. tostring(err))
    return nil
  end
  if #registry == 0 then
    logger("infra_publisher: registry empty (build_kb did not emit?)")
    return {}
  end

  -- Refresh broker snapshot once per pass; resolve_one reads from cache.
  local rok, rerr = broker_client.refresh(conn)
  if not rok then
    logger("infra_publisher: broker_client.refresh failed: " .. tostring(rerr))
    -- Continue: resolve_one will report each service as unhealthy.
  end

  local out = {}
  for _, entry in ipairs(registry) do
    local r = resolve_one(conn, entry)
    out[entry.service_type] = r

    local base = ndc_paths.site_path(site,
      "infrastructure.registry.service." .. entry.service_type
      .. ".KB_STATUS_FIELD")

    -- Each field is its own status row; write all five.
    -- kb_status.set_status_data UPDATEs the pre-allocated row from
    -- infrastructure_registry subsystem. Wrapped {value = X} matches
    -- the shape add_status_field laid down at build_kb time.
    -- port + protocol come from the contract at build time and stay
    -- stable across ticks; we still write them every tick so the
    -- knowledge_base_status row carries the runtime view (build_kb
    -- defaults live in knowledge_base, not knowledge_base_status, so
    -- without these writes the status row stays {} and readers see
    -- port=nil).
    local ok1 = pcall(kb_status.set_status_data, conn,
      base .. ".host",      { value = r.host })
    local ok2 = pcall(kb_status.set_status_data, conn,
      base .. ".port",      { value = entry.contract_port })
    local ok3 = pcall(kb_status.set_status_data, conn,
      base .. ".protocol",  { value = entry.contract_protocol })
    local ok4 = pcall(kb_status.set_status_data, conn,
      base .. ".healthy",   { value = r.healthy })
    local ok5 = pcall(kb_status.set_status_data, conn,
      base .. ".last_seen", { value = r.last_seen })
    if not (ok1 and ok2 and ok3 and ok4 and ok5) then
      logger(string.format(
        "infra_publisher: write failed for %s",
        entry.service_type))
    end
  end

  return out
end

return M
