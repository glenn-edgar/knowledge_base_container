-- =============================================================================
-- docker.lua -- Docker CLI primitives for the DCS host process.
--
-- Shell out to `docker` for container lifecycle. All docker run invocations
-- include the `nanodatacenter=true` label so `docker_stop_labelled` can
-- clean-slate the host without touching other workloads.
--
-- Minimal surface (Build 2b):
--   M.stop_labelled(label?)     -- stop + rm every container matching label
--   M.is_running(name)          -- true if a container with this name is up
--   M.run_from_spec(name, spec, extra_env?)  -- docker run from a build.spec
--   M.stop(name)                -- docker stop <name>
--
-- spec shape (from container_definition.<def>.build.spec in bootstrap.db):
--   runtime        = "docker"
--   image          = "pgvector/pgvector:pg17"
--   env_defaults   = { K = V, ... }
--   env_required   = { "POSTGRES_PASSWORD", ... }    -- resolved from os.getenv
--   ports          = { { host = 5432, cont = 5432 }, ... }
--   volumes        = { { host = "~/Data", cont = "/var/lib/..." }, ... }
--   labels         = { K = V, ... }
--   restart_policy = "always" | "no" | "unless-stopped" | ...
-- =============================================================================

local M = {}

local DEFAULT_LABEL = "nanodatacenter=true"

---------------------------------------------------------------------------
-- shell helpers
---------------------------------------------------------------------------

local function shell_escape(s)
  return "'" .. tostring(s):gsub("'", "'\\''") .. "'"
end

local function capture(cmd)
  local p = io.popen(cmd .. " 2>&1")
  if not p then return nil, "popen failed" end
  local out = p:read("*a") or ""
  local ok, _, code = p:close()
  return out, ok, code
end

local function run_exit(cmd)
  local _, ok, code = capture(cmd)
  return ok and 0 or (code or 1)
end

local function expand_home(path)
  if not path then return path end
  if path:sub(1, 1) == "~" then
    return (os.getenv("HOME") or "") .. path:sub(2)
  end
  return path
end

---------------------------------------------------------------------------
-- stop every container labelled as ours (clean slate at boot)
---------------------------------------------------------------------------

function M.stop_labelled(label)
  label = label or DEFAULT_LABEL
  local ids_out = capture(string.format(
    "docker ps -aq --filter %s", shell_escape("label=" .. label)))
  if not ids_out or ids_out == "" then return 0 end
  local ids = {}
  for id in ids_out:gmatch("%S+") do ids[#ids + 1] = id end
  if #ids == 0 then return 0 end
  capture("docker rm -f " .. table.concat(ids, " "))
  return #ids
end

---------------------------------------------------------------------------
-- is a named container currently running?
---------------------------------------------------------------------------

function M.is_running(name)
  local out = capture(string.format(
    "docker inspect -f '{{.State.Running}}' %s", shell_escape(name)))
  if not out then return false end
  return out:match("true") ~= nil
end

---------------------------------------------------------------------------
-- docker run from a build.spec
---------------------------------------------------------------------------

function M.run_from_spec(name, spec, extra_env)
  assert(name and name ~= "", "run_from_spec: name required")
  assert(spec and spec.image, "run_from_spec: spec.image required")

  -- Belt-and-braces: a stopped container under the same name from a
  -- prior run would make `docker run --name` fail with "name in use".
  -- DCS owns application containers, so removing any stale instance
  -- before starting is the right default.
  capture("docker rm -f " .. shell_escape(name))

  local parts = { "docker", "run", "-d", "--name", shell_escape(name) }

  -- our global clean-slate label
  parts[#parts + 1] = "--label"
  parts[#parts + 1] = shell_escape(DEFAULT_LABEL)

  -- host.docker.internal resolution: required on Docker Desktop, harmless
  -- on Linux-native (where the gateway alias just points at the host).
  -- Containers that connect back to pg/NATS on the host use this name.
  parts[#parts + 1] = "--add-host"
  parts[#parts + 1] = shell_escape("host.docker.internal:host-gateway")

  -- per-spec labels
  for k, v in pairs(spec.labels or {}) do
    parts[#parts + 1] = "--label"
    parts[#parts + 1] = shell_escape(k .. "=" .. tostring(v))
  end

  -- restart policy
  if spec.restart_policy and spec.restart_policy ~= "no" then
    parts[#parts + 1] = "--restart"
    parts[#parts + 1] = shell_escape(spec.restart_policy)
  end

  -- port mappings. Accepted record shapes:
  --   { external = H, internal = C, slot, protocol, purpose, ... }  -- new
  --   { host = H, cont = C }                                         -- legacy
  --   { C }  |  C                                                    -- short legacy
  -- New-style records come from port_spec-driven definitions (per
  -- construct_dcs_kb.lua's resolve_instance_ports); legacy records still
  -- carry the old host/cont shape for infrastructure defs.
  for _, p in ipairs(spec.ports or {}) do
    local hp, cp
    if type(p) == "table" then
      if p.external or p.internal then
        hp = p.external or p.internal
        cp = p.internal or p.external
      else
        hp = p.host or p.cont
        cp = p.cont or p.host
      end
    else
      hp, cp = p, p
    end
    parts[#parts + 1] = "-p"
    parts[#parts + 1] = shell_escape(tostring(hp) .. ":" .. tostring(cp))
  end

  -- volume mounts (expand ~)
  for _, v in ipairs(spec.volumes or {}) do
    parts[#parts + 1] = "-v"
    parts[#parts + 1] = shell_escape(expand_home(v.host) .. ":" .. v.cont)
  end

  -- env_defaults (non-secret)
  for k, v in pairs(spec.env_defaults or {}) do
    parts[#parts + 1] = "-e"
    parts[#parts + 1] = shell_escape(k .. "=" .. tostring(v))
  end

  -- env_required (resolved from host env; secrets live there)
  for _, k in ipairs(spec.env_required or {}) do
    local val = os.getenv(k)
    if not val then
      return false, "env_required not set: " .. k
    end
    parts[#parts + 1] = "-e"
    parts[#parts + 1] = shell_escape(k .. "=" .. val)
  end

  -- extra env injected by the caller (e.g., CPU_ID, SITE)
  for k, v in pairs(extra_env or {}) do
    parts[#parts + 1] = "-e"
    parts[#parts + 1] = shell_escape(k .. "=" .. tostring(v))
  end

  parts[#parts + 1] = shell_escape(spec.image)

  -- entrypoint overrides (positional args at end)
  for _, arg in ipairs(spec.entrypoint or {}) do
    parts[#parts + 1] = shell_escape(arg)
  end

  local cmd = table.concat(parts, " ")
  local out, ok = capture(cmd)
  if not ok then
    return false, "docker run failed: " .. tostring(out)
  end
  return true, out:gsub("%s+$", "")   -- container id
end

---------------------------------------------------------------------------
-- docker stop <name> (SIGTERM + grace; rm -f after).
-- Used for app containers DCS itself created via run_from_spec.
---------------------------------------------------------------------------

function M.stop(name)
  capture("docker stop " .. shell_escape(name))
  capture("docker rm -f " .. shell_escape(name))
end

---------------------------------------------------------------------------
-- docker start <name> for pre-existing (laptop-placed) containers.
-- DCS does NOT create infra containers; it only starts/stops them.
-- Returns (true, "") on success, (false, err) on failure (e.g. no
-- container with that name exists).
---------------------------------------------------------------------------

function M.start_existing(name)
  local out, ok = capture("docker start " .. shell_escape(name))
  if not ok then
    return false, "docker start failed: " .. tostring(out)
  end
  return true, out:gsub("%s+$", "")
end

---------------------------------------------------------------------------
-- docker stop <name> WITHOUT rm. For pre-placed containers DCS supervises
-- but doesn't own. The container stays around to be started again.
---------------------------------------------------------------------------

function M.stop_only(name)
  local out, ok = capture("docker stop " .. shell_escape(name))
  if not ok then
    return false, "docker stop failed: " .. tostring(out)
  end
  return true, out:gsub("%s+$", "")
end

return M
