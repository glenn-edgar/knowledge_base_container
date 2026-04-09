--[[
  physical_tree.lua — Hardware topology tree (data-driven)

  Hierarchy: site → CPU → container → service
  Each CPU has a master leaf. Master holds Postgres.
  CPU controllers read their subtree to know what to instantiate.

  Container data includes the full run spec (deployment manifest):
    image, docker_name, depends_on, ports, volumes, tmpfs, env,
    network, restart, links, cmd, params, sqlite_db
]]

local M = {}

function M.build(kb, config)
  kb:add_kb("system", "Embedded data center physical topology")
  kb:select_kb("system")

  kb:add_header_node("site", config.site_name,
    { type = "site", description = "Main site" },
    {})

  for _, cpu in ipairs(config.cpus) do
    kb:add_header_node("cpu", cpu.name,
      { type = cpu.type, description = cpu.description },
      {})

    -- Master leaf
    kb:add_info_node("role", "master",
      { type = "role" },
      { is_master = cpu.master })

    -- Containers under this CPU
    for _, ctr in ipairs(cpu.containers) do
      local ctr_data = {
        -- Identity
        image       = ctr.image,
        docker_name = ctr.docker_name,
        -- Lifecycle
        depends_on  = ctr.depends_on,
        restart     = ctr.restart,
        -- Run spec
        ports       = ctr.ports,
        volumes     = ctr.volumes,
        tmpfs       = ctr.tmpfs,
        env         = ctr.env,
        env_names   = ctr.env_names,
        network     = ctr.network,
        links       = ctr.links,
        cmd         = ctr.cmd,
        -- Application
        params      = ctr.params,
        sqlite_db   = ctr.sqlite_db,
      }

      if #ctr.services > 0 then
        kb:add_header_node("container", ctr.name,
          { type = ctr.type, description = ctr.description },
          ctr_data)

        for _, svc in ipairs(ctr.services) do
          kb:add_info_node("service", svc.name,
            { type = "service", protocol = svc.protocol },
            svc.data)
        end

        kb:leave_header_node("container", ctr.name)
      else
        kb:add_info_node("container", ctr.name,
          { type = ctr.type, description = ctr.description },
          ctr_data)
      end
    end

    kb:leave_header_node("cpu", cpu.name)
  end

  kb:leave_header_node("site", config.site_name)
end

return M
