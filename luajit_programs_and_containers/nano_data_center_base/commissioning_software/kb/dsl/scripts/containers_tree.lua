--[[
  containers_tree.lua — Container definitions (data-driven)

  First-class deployable units. Each container node carries everything
  a CPU controller needs to instantiate it: image, deps, env, volumes, params.

  CPU controllers query bindings to find their containers, then read
  the container node for instantiation parameters.
]]

local M = {}

function M.build(kb, config)
  kb:add_kb("containers", "Deployable container definitions")
  kb:select_kb("containers")

  for _, ctr in ipairs(config.containers) do
    kb:add_info_node("container", ctr.name,
      { type        = ctr.type,
        description = ctr.description },
      { image   = ctr.image,
        deps    = ctr.deps,
        env     = ctr.env,
        volumes = ctr.volumes,
        params  = ctr.params })
  end
end

return M
