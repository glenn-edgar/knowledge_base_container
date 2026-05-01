--[[
  bindings.lua — Attach containers to CPUs (data-driven)

  CPU controllers query: "what containers do I run?"
  Then read the container definition for instantiation parameters.
]]

local M = {}

function M.build(kb, config)
  kb:add_kb("bindings", "Container-to-CPU instantiation bindings")
  kb:select_kb("bindings")

  for _, bind in ipairs(config.bindings) do
    kb:add_info_node("instance", bind.container,
      { type = "binding" },
      { container_def = "containers.container." .. bind.container,
        target_cpu    = "system.site." .. config.site_name
                        .. ".cpu." .. bind.target_cpu })
  end
end

return M
