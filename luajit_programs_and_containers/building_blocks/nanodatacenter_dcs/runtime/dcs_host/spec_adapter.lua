-- =============================================================================
-- spec_adapter.lua -- convert build_specs[def_name] entries into the
-- wire-protocol RunSpec table that broker_client.run() expects.
--
-- The bootstrap.db catalog format (see construction/catalogs/definitions.lua)
-- carries Lua-shaped fields:
--
--   image, ports[], volumes[], env_defaults{}, env_required[],
--   labels{}, restart_policy, entrypoint[], network?
--
-- The broker's wire format flattens this into:
--
--   { name, image, env{}, ports[], volumes[], restart_policy,
--     labels{}, network, extra_hosts[], entrypoint[] }
--
-- This module performs four normalizations the broker can't do for us:
--
--   1. env_required → os.getenv at call time (secrets live in the
--      operator env, never in the catalog)
--   2. ~/foo in volume host paths → $HOME/foo (broker sees only the
--      docker daemon's filesystem; ~ would be daemon-relative, wrong)
--   3. legacy port shapes ({external,internal} | {host,cont} | C) → the
--      broker's single shape {host_port, container_port, proto}
--   4. inject the global "nanodatacenter=true" discovery label and the
--      "host.docker.internal:host-gateway" extra host (these are DCS
--      conventions, not broker primitives — broker stays generic)
-- =============================================================================

local M = {}

-- ~ expansion. Mirrors docker.lua's expand_home: only the leading "~/"
-- or bare "~" are rewritten. Embedded ~ stays literal.
local function expand_home(path)
  if type(path) ~= "string" then return path end
  if path == "~" then return os.getenv("HOME") or "" end
  if path:sub(1, 2) == "~/" then
    return (os.getenv("HOME") or "") .. path:sub(2)
  end
  return path
end

-- Collapse a port record from any of the catalog's accepted shapes onto
-- the wire-protocol shape. Returns nil if the record is malformed.
local function normalize_port(p)
  local hp, cp, proto
  if type(p) == "table" then
    if p.external or p.internal then
      hp = p.external or p.internal
      cp = p.internal or p.external
    else
      hp = p.host or p.cont
      cp = p.cont or p.host
    end
    proto = p.protocol or p.proto or "tcp"
  elseif type(p) == "number" or type(p) == "string" then
    hp, cp, proto = p, p, "tcp"
  else
    return nil
  end
  hp, cp = tonumber(hp), tonumber(cp)
  if not hp or not cp then return nil end
  return { host_port = hp, container_port = cp, proto = proto }
end

--- Build a wire-protocol RunSpec table.
---
--- @param name      string   container instance name (e.g. "test_app_01")
--- @param spec      table    build_specs[def_name] entry
--- @param extra_env table?   optional env merged in last (CPU_ID, SITE, ...)
---
--- Returns (run_spec, nil) on success, or (nil, err) when a required
--- env var is unresolved or the spec is malformed.
function M.build_run_spec(name, spec, extra_env)
  if type(name) ~= "string" or name == "" then
    return nil, "spec_adapter: name required"
  end
  if type(spec) ~= "table" or not spec.image then
    return nil, "spec_adapter: spec.image required"
  end

  -- env: env_defaults + os.getenv(env_required) + extra_env. Last write wins.
  local env = {}
  for k, v in pairs(spec.env_defaults or {}) do env[k] = tostring(v) end
  for _, k in ipairs(spec.env_required or {}) do
    local val = os.getenv(k)
    if not val then
      return nil, "spec_adapter: env_required not set: " .. tostring(k)
    end
    env[k] = val
  end
  for k, v in pairs(extra_env or {}) do env[k] = tostring(v) end

  -- ports: normalize every record; reject the spec on the first bad one
  -- so misconfigurations surface at the supervisor edge, not at the
  -- docker daemon.
  local ports = {}
  for i, p in ipairs(spec.ports or {}) do
    local np = normalize_port(p)
    if not np then
      return nil, string.format("spec_adapter: bad port[%d] in %s", i, name)
    end
    ports[#ports + 1] = np
  end

  -- volumes: ~ expansion on host path; container path is daemon-side
  -- so it must stay verbatim.
  local volumes = {}
  for _, v in ipairs(spec.volumes or {}) do
    if type(v) == "table" and v.host and v.cont then
      volumes[#volumes + 1] = {
        host      = expand_home(v.host),
        container = v.cont,
        read_only = v.read_only and true or false,
      }
    end
  end

  -- labels: copy + inject the global discovery label so docker-stop-by-label
  -- sweeps and any other label-driven query find DCS-managed containers.
  local labels = {}
  for k, v in pairs(spec.labels or {}) do labels[k] = tostring(v) end
  labels["nanodatacenter"] = "true"

  -- restart_policy: catalog uses "no" as the don't-restart sentinel; the
  -- wire protocol expects "" (omitted) for the same meaning, so docker
  -- doesn't reject the empty-string case.
  local restart_policy = spec.restart_policy
  if not restart_policy or restart_policy == "no" or restart_policy == "" then
    restart_policy = ""
  end

  return {
    name           = name,
    image          = spec.image,
    env            = env,
    ports          = ports,
    volumes        = volumes,
    restart_policy = restart_policy,
    labels         = labels,
    network        = spec.network or "",
    -- Always inject host.docker.internal so app containers can reach
    -- pg/nats on the docker host. Required on Docker Desktop, harmless
    -- on Linux-native (where the gateway alias just points at the host).
    extra_hosts    = { "host.docker.internal:host-gateway" },
    entrypoint     = spec.entrypoint or {},
  }, nil
end

return M
