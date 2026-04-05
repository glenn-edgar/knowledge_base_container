--[[
  software_tree.lua — Subsystem/domain definitions (data-driven)

  Domains are logical groupings (mission planners, robot controllers).
  Each domain references its container in the physical tree via ltree path.
]]

local M = {}

function M.build(kb, config)
  kb:add_kb("subsystems", "Software subsystem definitions")
  kb:select_kb("subsystems")

  for _, domain in ipairs(config.domains) do
    local container_path = "system.site." .. config.site_name
      .. ".cpu." .. domain.cpu
      .. ".container." .. domain.container

    local has_robots = domain.robots and #domain.robots > 0

    if has_robots then
      kb:add_header_node("domain", domain.name,
        { type = "domain", description = domain.description },
        { nats_prefix = domain.nats_prefix,
          container   = container_path })

      kb:add_header_node("robots", "registry",
        { type = "robot_registry" },
        {})

      for _, robot in ipairs(domain.robots) do
        kb:add_info_node("robot", robot.name,
          { type = "robot",
            transport   = robot.transport,
            wire_format = robot.wire_format },
          { capabilities = robot.capabilities })
      end

      kb:leave_header_node("robots", "registry")
      kb:leave_header_node("domain", domain.name)
    else
      kb:add_info_node("domain", domain.name,
        { type = "domain", description = domain.description },
        { nats_prefix = domain.nats_prefix,
          container   = container_path })
    end
  end
end

return M
