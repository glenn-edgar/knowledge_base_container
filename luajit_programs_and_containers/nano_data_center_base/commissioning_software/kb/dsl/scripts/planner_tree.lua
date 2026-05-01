--[[
  planner_tree.lua — Planner KB tree data in Postgres

  Adds planner-specific tree nodes (boards, VNs, robot classes, robots)
  to Postgres. Uses only construct_kb API (add_header_node, add_info_node).

  Status/stream/bitmask tables are built on the SQLite side via CDT
  in sqlite_extract.lua, which calls the planner_data module with
  full CDT support.
]]

local M = {}

function M.build(pg_kb, config)
  for _, domain in ipairs(config.domains) do
    if domain.planner_data then
      local site = domain.site
      print(string.format("  Planner KB: %s (%s)", site, domain.planner_data))

      pg_kb:add_kb(site, "Planner data for " .. domain.name)
      pg_kb:select_kb(site)

      -- Load the planner data builder and call it.
      -- The Postgres construct_kb doesn't have CDT methods (add_status_field etc.)
      -- so wrap missing methods to fall back to add_info_node.
      local wrapper = setmetatable({}, {
        __index = function(_, key)
          local fn = pg_kb[key]
          if fn then return function(_, ...) return fn(pg_kb, ...) end end
          -- Fallback: status/stream fields → add_info_node
          if key == "add_status_field" then
            return function(_, name, props, desc, data)
              pg_kb:add_info_node("status", name, props, data or {}, desc)
            end
          elseif key == "add_stream_field" then
            return function(_, name, size, desc)
              pg_kb:add_info_node("stream", name, { stream_size = size }, {}, desc)
            end
          elseif key == "create_bit_mask_entry" then
            return function() end  -- no-op on Postgres side
          elseif key == "clear_bit_mask_flags" or key == "add_bit_mask_flag" then
            return function() end
          elseif key == "check_installation" then
            return function() end
          end
        end,
      })

      local builder = require(domain.planner_data)
      builder(wrapper, site, domain)

      print(string.format("    -> %s OK", site))
    end
  end
end

return M
