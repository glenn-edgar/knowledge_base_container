-- =============================================================================
-- subsystems/container_definitions.lua
--
-- Emits container_definition.<def>.build.spec rows at the system-KB root
-- (outside any site.<SITE>). One row per entry in catalogs/definitions.lua.
-- Consumed at runtime by node_control when starting an instance.
-- =============================================================================

return {

  install_system_kb = function(ctx)
    local kb = ctx.kb
    for def_name, def in pairs(ctx.DEFINITIONS) do
      kb:with_header("container_definition", def_name,
        { kind = def.kind, runtime = def.runtime or "docker" },
        {}, "Container definition " .. def_name,
      function()
        kb:add_info_node("build", "spec",
          { kind = def.kind },
          {
            runtime        = def.runtime or "docker",
            image          = def.image,
            build_ctx      = def.build_ctx,
            entrypoint     = def.entrypoint     or {},
            env_defaults   = def.env_defaults   or {},
            env_required   = def.env_required   or {},
            default_cfg    = def.default_cfg    or {},
            ports          = def.ports          or {},
            port_spec      = def.port_spec      or {},
            volumes        = def.volumes        or {},
            labels         = def.labels         or {},
            restart_policy = def.restart_policy or "no",
            cli_databases  = def.cli_databases  or {},
          },
          "Build + runtime spec")
      end)
    end
  end,

}
