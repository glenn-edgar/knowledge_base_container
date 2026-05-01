--[[
  graph.lua — Container dependency graph from Postgres KB

  Queries the system KB for container nodes, extracts depends_on edges,
  and provides topological sort for startup (forward) and shutdown (reverse).

  Service discovery: the orchestrator resolves service endpoints from
  dependency containers' service nodes in the KB. No hardcoded endpoints
  in container env — Postgres KB is the single source of truth.

  Each container node is stored at:
    system.site.<site>.cpu.<cpu>.container.<name>
  with data fields: image, docker_name, depends_on, ports, volumes,
                    tmpfs, env, network, restart, links, cmd

  Service nodes are children of container nodes:
    system.site.<site>.cpu.<cpu>.container.<name>.service.<svc_name>
  with data fields: protocol-specific (host, port, dbname, etc.)
]]

local dkjson = require("dkjson")

local M = {}

---------------------------------------------------------------------------
-- Query Postgres KB for container graph + services
---------------------------------------------------------------------------

function M.load_from_postgres(pg_opts)
  local KB_Search = require("kb_search")
  local kb = KB_Search.new({
    host     = pg_opts.host or "127.0.0.1",
    port     = pg_opts.port or 5432,
    dbname   = pg_opts.dbname or "knowledge_base",
    user     = pg_opts.user or "gedgar",
    password = pg_opts.password or "",
    database = "knowledge_base",
  })

  -- Find all container nodes in the system KB
  kb:search_kb("system")
  kb:search_label("container")
  local rows = kb:execute_query()

  local containers = {}
  for _, row in ipairs(rows) do
    local data = dkjson.decode(row.data or "{}")
    local props = dkjson.decode(row.properties or "{}")
    containers[row.name] = {
      name        = row.name,
      path        = row.path,
      docker_name = data.docker_name or row.name,
      depends_on  = data.depends_on or {},
      type        = props.type or "unknown",
      image       = data.image,
      -- Run spec
      ports       = data.ports,
      volumes     = data.volumes,
      tmpfs       = data.tmpfs,
      env         = data.env or {},
      env_names   = data.env_names or {},
      network     = data.network,
      restart     = data.restart,
      links       = data.links,
      cmd         = data.cmd,
      -- Services provided by this container (populated below)
      services    = {},
    }
  end

  -- Find all service nodes
  kb:clear_filters()
  kb:search_kb("system")
  kb:search_label("service")
  local svc_rows = kb:execute_query()

  for _, row in ipairs(svc_rows) do
    local props = dkjson.decode(row.properties or "{}")
    local data  = dkjson.decode(row.data or "{}")
    -- Match service to its parent container by path prefix
    for _, ctr in pairs(containers) do
      if ctr.path and row.path:find(ctr.path, 1, true) == 1 then
        table.insert(ctr.services, {
          name     = row.name,
          protocol = props.protocol,
          data     = data,
        })
        break
      end
    end
  end

  return containers
end

---------------------------------------------------------------------------
-- Service discovery: build env vars from dependency services
--
-- Only injects env for protocols the consumer actually needs — determined
-- by the consumer having a service with role="client" for that protocol.
--
-- Protocol → default env var mapping (overridable via env_names):
--   nats     → NATS_SERVER=nats://<docker_name>:<port>
--   mqtt     → MQTT_HOST=<docker_name>, MQTT_PORT=<port>
--   postgres → PG_HOST=<docker_name>, PG_PORT=<port>,
--              PG_DBNAME=<dbname>, PG_USER=<user>
--   http     → HTTP_HOST=<docker_name>, HTTP_PORT=<port>
--
-- env_names overrides: { nats = "NATS_URL" } → uses NATS_URL instead of NATS_SERVER
---------------------------------------------------------------------------

function M.resolve_service_env(ctr, containers)
  -- Build set of protocols this container consumes (role=client/subscriber)
  local needs = {}
  for _, svc in ipairs(ctr.services) do
    local role = svc.data and svc.data.role
    if role == "client" or role == "subscriber" then
      needs[svc.protocol] = true
    end
  end

  local names = ctr.env_names or {}
  local env = {}

  for _, dep_name in ipairs(ctr.depends_on) do
    local dep = containers[dep_name]
    if dep then
      for _, svc in ipairs(dep.services) do
        local d = svc.data or {}

        if svc.protocol == "nats" and needs.nats then
          local key = names.nats or "NATS_SERVER"
          env[key] = string.format("nats://%s:%d",
            dep.docker_name, d.port or 4222)
        elseif svc.protocol == "mqtt" and needs.mqtt then
          env[names.mqtt_host or "MQTT_HOST"] = dep.docker_name
          env[names.mqtt_port or "MQTT_PORT"] = tostring(d.port or 1883)
        elseif svc.protocol == "postgres" and needs.postgres then
          env[names.pg_host   or "PG_HOST"]   = dep.docker_name
          env[names.pg_port   or "PG_PORT"]   = tostring(d.port or 5432)
          env[names.pg_dbname or "PG_DBNAME"] = d.dbname or "knowledge_base"
          env[names.pg_user   or "PG_USER"]   = "gedgar"
        elseif svc.protocol == "http" and needs.http then
          env[names.http_host or "HTTP_HOST"] = dep.docker_name
          env[names.http_port or "HTTP_PORT"] = tostring(d.port or 8080)
        end
      end
    end
  end

  return env
end

---------------------------------------------------------------------------
-- Merge env tables: container-own env + resolved service env
-- Container-own env wins on conflict (explicit override).
---------------------------------------------------------------------------

function M.merge_env(container_env, service_env)
  local merged = {}
  for k, v in pairs(service_env) do merged[k] = v end
  for k, v in pairs(container_env) do merged[k] = v end
  return merged
end

---------------------------------------------------------------------------
-- Resolve $ENV_VAR references in env values
---------------------------------------------------------------------------

function M.resolve_env_refs(env)
  local resolved = {}
  for k, v in pairs(env) do
    if type(v) == "string" and v:sub(1, 1) == "$" then
      resolved[k] = os.getenv(v:sub(2)) or ""
    else
      resolved[k] = tostring(v)
    end
  end
  return resolved
end

---------------------------------------------------------------------------
-- Topological sort (Kahn's algorithm)
--
-- Returns ordered list of container names.
-- forward=true  → roots first (startup order)
-- forward=false → leaves first (shutdown order)
---------------------------------------------------------------------------

function M.topo_sort(containers, forward)
  local in_degree = {}
  local dependents = {}

  for name, _ in pairs(containers) do
    in_degree[name] = 0
    dependents[name] = {}
  end

  for name, ctr in pairs(containers) do
    for _, dep in ipairs(ctr.depends_on) do
      if containers[dep] then
        in_degree[name] = in_degree[name] + 1
        table.insert(dependents[dep], name)
      end
    end
  end

  local queue = {}
  for name, deg in pairs(in_degree) do
    if deg == 0 then
      table.insert(queue, name)
    end
  end
  table.sort(queue)

  local order = {}
  local head = 1
  while head <= #queue do
    local name = queue[head]
    head = head + 1
    table.insert(order, name)

    for _, child in ipairs(dependents[name]) do
      in_degree[child] = in_degree[child] - 1
      if in_degree[child] == 0 then
        table.insert(queue, child)
      end
    end
  end

  if #order ~= #queue then
    error("Dependency cycle detected in container graph")
  end

  if not forward then
    local rev = {}
    for i = #order, 1, -1 do
      table.insert(rev, order[i])
    end
    return rev
  end

  return order
end

---------------------------------------------------------------------------
-- Group by topological level (for parallel operations)
---------------------------------------------------------------------------

function M.topo_levels(containers, forward)
  local levels = {}

  local function compute_level(name, visited)
    if levels[name] then return levels[name] end
    if visited[name] then error("Cycle: " .. name) end
    visited[name] = true

    local max_dep = -1
    for _, dep in ipairs(containers[name].depends_on) do
      if containers[dep] then
        max_dep = math.max(max_dep, compute_level(dep, visited))
      end
    end

    levels[name] = max_dep + 1
    return levels[name]
  end

  for name in pairs(containers) do
    compute_level(name, {})
  end

  local max_level = 0
  for _, lvl in pairs(levels) do
    max_level = math.max(max_level, lvl)
  end

  local grouped = {}
  for i = 0, max_level do
    grouped[i + 1] = {}
  end

  for name, lvl in pairs(levels) do
    table.insert(grouped[lvl + 1], name)
  end

  for _, group in ipairs(grouped) do
    table.sort(group)
  end

  if not forward then
    local rev = {}
    for i = #grouped, 1, -1 do
      table.insert(rev, grouped[i])
    end
    return rev
  end

  return grouped
end

---------------------------------------------------------------------------
-- Docker helpers
---------------------------------------------------------------------------

function M.docker_exists(docker_name)
  local cmd = string.format("docker inspect %s >/dev/null 2>&1", docker_name)
  return os.execute(cmd) == 0
end

function M.docker_is_running(docker_name)
  local cmd = string.format(
    "docker inspect -f '{{.State.Running}}' %s 2>/dev/null", docker_name)
  local handle = io.popen(cmd)
  local output = handle:read("*a"):gsub("%s+$", "")
  handle:close()
  return output == "true"
end

-- LuaJIT io.popen:close() always returns true — detect errors via output
local function run_cmd(cmd)
  local handle = io.popen(cmd)
  local output = handle:read("*a"):gsub("%s+$", "")
  handle:close()
  local ok = not output:find("^Error") and not output:find("^error")
  return ok, output
end

function M.docker_start(docker_name)
  return run_cmd(string.format("docker start %s 2>&1", docker_name))
end

function M.docker_stop(docker_name)
  return run_cmd(string.format("docker stop %s 2>&1", docker_name))
end

function M.docker_rm(docker_name)
  return run_cmd(string.format("docker rm -f %s 2>&1", docker_name))
end

function M.docker_wait_healthy(docker_name, timeout_s)
  timeout_s = timeout_s or 30
  for _ = 1, timeout_s do
    if M.docker_is_running(docker_name) then
      return true
    end
    os.execute("sleep 1")
  end
  return false
end

---------------------------------------------------------------------------
-- Build `docker run` command from run spec
--
-- env_table is the fully merged + resolved env (container + service discovery)
---------------------------------------------------------------------------

function M.build_run_cmd(ctr, env_table)
  local parts = { "docker", "run", "-d" }

  table.insert(parts, "--name")
  table.insert(parts, ctr.docker_name)

  -- Restart policy
  if ctr.restart then
    table.insert(parts, "--restart")
    table.insert(parts, ctr.restart)
  end

  -- Network
  if ctr.network then
    table.insert(parts, "--network")
    table.insert(parts, ctr.network)
  end

  -- Ports
  for _, p in ipairs(ctr.ports or {}) do
    table.insert(parts, "-p")
    table.insert(parts, p)
  end

  -- Volumes
  for _, v in ipairs(ctr.volumes or {}) do
    table.insert(parts, "-v")
    table.insert(parts, v)
  end

  -- Tmpfs
  for _, t in ipairs(ctr.tmpfs or {}) do
    table.insert(parts, "--tmpfs")
    table.insert(parts, t)
  end

  -- Links
  for _, l in ipairs(ctr.links or {}) do
    table.insert(parts, "--link")
    table.insert(parts, l)
  end

  -- Environment variables (sorted for determinism)
  local env_keys = {}
  for k in pairs(env_table) do table.insert(env_keys, k) end
  table.sort(env_keys)
  for _, k in ipairs(env_keys) do
    table.insert(parts, "-e")
    table.insert(parts, k .. "=" .. env_table[k])
  end

  -- Image
  table.insert(parts, ctr.image)

  -- CMD override
  if ctr.cmd then
    table.insert(parts, ctr.cmd)
  end

  return table.concat(parts, " ")
end

---------------------------------------------------------------------------
-- Inspect existing container config for stale detection
-- Returns { image = "...", env = { K=V, ... } } or nil
---------------------------------------------------------------------------

function M.docker_inspect_config(docker_name)
  -- Get image
  local h1 = io.popen(string.format(
    "docker inspect %s --format '{{.Config.Image}}' 2>/dev/null", docker_name))
  local image = h1:read("*a"):gsub("%s+$", "")
  h1:close()
  if image == "" then return nil end

  -- Get env vars as KEY=VALUE lines
  local h2 = io.popen(string.format(
    "docker inspect %s --format '{{range .Config.Env}}{{println .}}{{end}}' 2>/dev/null",
    docker_name))
  local env_raw = h2:read("*a")
  h2:close()

  local env = {}
  for line in env_raw:gmatch("[^\n]+") do
    local k, v = line:match("^([^=]+)=(.*)")
    if k then env[k] = v end
  end

  return { image = image, env = env }
end

---------------------------------------------------------------------------
-- Check if existing container matches KB spec (image + env)
-- Returns true if the container needs to be recreated
---------------------------------------------------------------------------

function M.is_stale(ctr, containers)
  local current = M.docker_inspect_config(ctr.docker_name)
  if not current then return false end  -- doesn't exist, not stale

  -- Check image
  if current.image ~= ctr.image then return true end

  -- Build expected env
  local svc_env = M.resolve_service_env(ctr, containers)
  local merged  = M.merge_env(ctr.env, svc_env)
  local expected = M.resolve_env_refs(merged)

  -- Check expected env vars are present with correct values
  for k, v in pairs(expected) do
    if current.env[k] ~= v then return true end
  end

  return false
end

---------------------------------------------------------------------------
-- docker_ensure — create-or-start a container
--
-- If container doesn't exist: docker run (full create from KB spec)
-- If container exists but config differs from KB: rm + docker run
-- If container exists and matches: docker start
-- If container is running and matches: no-op
-- If container is running but stale: stop + rm + docker run
---------------------------------------------------------------------------

function M.docker_ensure(ctr, containers)
  local docker_name = ctr.docker_name

  if M.docker_is_running(docker_name) then
    if M.is_stale(ctr, containers) then
      M.docker_stop(docker_name)
      M.docker_rm(docker_name)
      -- fall through to docker run below
    else
      return "running", docker_name .. " already running"
    end
  elseif M.docker_exists(docker_name) then
    if M.is_stale(ctr, containers) then
      M.docker_rm(docker_name)
      -- fall through to docker run below
    else
      local ok, out = M.docker_start(docker_name)
      return ok and "started" or "failed", out
    end
  end

  -- Container doesn't exist — full docker run
  local svc_env = M.resolve_service_env(ctr, containers)
  local merged  = M.merge_env(ctr.env, svc_env)
  local final   = M.resolve_env_refs(merged)

  -- Ensure network exists
  if ctr.network then
    os.execute(string.format("docker network create %s 2>/dev/null", ctr.network))
  end

  local cmd = M.build_run_cmd(ctr, final)
  local ok, output = run_cmd(cmd .. " 2>&1")

  return ok and "created" or "failed", output
end

return M
