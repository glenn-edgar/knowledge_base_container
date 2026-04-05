--[[
  handlers.lua — NATS RPC handlers for KB queries

  Each handler receives a JSON request string, queries Postgres via
  kb_data_structures, and returns a JSON response string.

  All handlers use the same KB facade instance passed at init time.
]]

local json = require("dkjson")

local M = {}

--- Initialize handlers with a kb_data_structures instance.
-- @param kb  KB_Data_Structures instance (connected to Postgres)
-- @return table of { method_name = handler_fn }
function M.new(kb)
  local handlers = {}

  -----------------------------------------------------------------
  -- domains — list all domains (web UI tab discovery)
  -----------------------------------------------------------------
  handlers["domains"] = function(request_json)
    kb:clear_filters()
    kb:search_kb("subsystems")
    kb:search_label("domain")
    local rows = kb:execute_kb_search()

    local domains = {}
    for _, row in ipairs(rows) do
      local data = row.data
      if type(data) == "string" then data = json.decode(data) or {} end
      local props = row.properties
      if type(props) == "string" then props = json.decode(props) or {} end

      domains[#domains + 1] = {
        name        = row.name,
        path        = row.path,
        description = props.description or "",
        nats_prefix = data.nats_prefix or "",
        container   = data.container or "",
      }
    end

    return json.encode({ domains = domains })
  end

  -----------------------------------------------------------------
  -- robots — list robots for a domain
  -----------------------------------------------------------------
  handlers["robots"] = function(request_json)
    local req = json.decode(request_json) or {}
    local domain = req.domain
    if not domain then
      return json.encode({ error = "missing 'domain' parameter" })
    end

    kb:clear_filters()
    kb:search_kb("subsystems")
    kb:search_label("robot")
    kb:search_starting_path("subsystems.domain." .. domain)
    local rows = kb:execute_kb_search()

    local robots = {}
    for _, row in ipairs(rows) do
      local data = row.data
      if type(data) == "string" then data = json.decode(data) or {} end
      local props = row.properties
      if type(props) == "string" then props = json.decode(props) or {} end

      robots[#robots + 1] = {
        name         = row.name,
        path         = row.path,
        transport    = props.transport or "",
        wire_format  = props.wire_format or "",
        capabilities = data.capabilities or {},
      }
    end

    return json.encode({ robots = robots })
  end

  -----------------------------------------------------------------
  -- containers — list containers on a CPU
  -----------------------------------------------------------------
  handlers["containers"] = function(request_json)
    local req = json.decode(request_json) or {}
    local cpu = req.cpu or "cpu_01"

    kb:clear_filters()
    kb:search_kb("system")
    kb:search_label("container")
    kb:search_starting_path("system.site.main.cpu." .. cpu)
    local rows = kb:execute_kb_search()

    local containers = {}
    for _, row in ipairs(rows) do
      local data = row.data
      if type(data) == "string" then data = json.decode(data) or {} end
      local props = row.properties
      if type(props) == "string" then props = json.decode(props) or {} end

      containers[#containers + 1] = {
        name        = row.name,
        path        = row.path,
        type        = props.type or "",
        description = props.description or "",
        image       = data.image or "",
        sqlite_db   = data.sqlite_db,
      }
    end

    return json.encode({ containers = containers })
  end

  -----------------------------------------------------------------
  -- services — list services for a container
  -----------------------------------------------------------------
  handlers["services"] = function(request_json)
    local req = json.decode(request_json) or {}
    local container_path = req.container_path
    if not container_path then
      return json.encode({ error = "missing 'container_path' parameter" })
    end

    kb:clear_filters()
    kb:search_kb("system")
    kb:search_label("service")
    kb:search_starting_path(container_path)
    local rows = kb:execute_kb_search()

    local services = {}
    for _, row in ipairs(rows) do
      local data = row.data
      if type(data) == "string" then data = json.decode(data) or {} end
      local props = row.properties
      if type(props) == "string" then props = json.decode(props) or {} end

      services[#services + 1] = {
        name     = row.name,
        path     = row.path,
        protocol = props.protocol or "",
        data     = data,
      }
    end

    return json.encode({ services = services })
  end

  -----------------------------------------------------------------
  -- node — get a single node by exact path
  -----------------------------------------------------------------
  handlers["node"] = function(request_json)
    local req = json.decode(request_json) or {}
    local path = req.path
    local kb_name = req.kb or "system"
    if not path then
      return json.encode({ error = "missing 'path' parameter" })
    end

    kb:clear_filters()
    kb:search_kb(kb_name)
    kb:search_path(path)
    local rows = kb:execute_kb_search()

    if #rows == 0 then
      return json.encode({ error = "not found", path = path })
    end

    local row = rows[1]
    local data = row.data
    if type(data) == "string" then data = json.decode(data) or {} end
    local props = row.properties
    if type(props) == "string" then props = json.decode(props) or {} end

    return json.encode({
      name       = row.name,
      label      = row.label,
      path       = row.path,
      properties = props,
      data       = data,
    })
  end

  -----------------------------------------------------------------
  -- subtree — get all nodes under a path prefix
  -----------------------------------------------------------------
  handlers["subtree"] = function(request_json)
    local req = json.decode(request_json) or {}
    local path = req.path
    local kb_name = req.kb or "system"
    if not path then
      return json.encode({ error = "missing 'path' parameter" })
    end

    kb:clear_filters()
    kb:search_kb(kb_name)
    kb:search_starting_path(path)
    local rows = kb:execute_kb_search()

    local nodes = {}
    for _, row in ipairs(rows) do
      local data = row.data
      if type(data) == "string" then data = json.decode(data) or {} end
      local props = row.properties
      if type(props) == "string" then props = json.decode(props) or {} end

      nodes[#nodes + 1] = {
        name       = row.name,
        label      = row.label,
        path       = row.path,
        properties = props,
        data       = data,
      }
    end

    return json.encode({ nodes = nodes })
  end

  return handlers
end

return M
