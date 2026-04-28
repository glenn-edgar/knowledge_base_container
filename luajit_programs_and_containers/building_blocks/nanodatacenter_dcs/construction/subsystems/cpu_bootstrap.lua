-- =============================================================================
-- subsystems/cpu_bootstrap.lua
--
-- Per-CPU bootstrap.config info-node: identity (cpu_id, is_master, kb_root)
-- + pg connect. Sliced into deployment/<cpu>/bootstrap.db by slice_bootstrap.
-- Read once at host-process startup.
-- =============================================================================

return {

  install_cpu = function(ctx, cpu_id, cpu_cfg)
    local kb = ctx.kb
    local PG = ctx.TOPOLOGY.pg_connect
    -- Phase 6.1: master needs the list of slave cpu_ids to round-robin
    -- their inboxes; slaves need the master's cpu_id to address its
    -- inbox. Cheap to bake in here and avoid runtime KB lookups.
    local peers = {}
    for other_id, _ in pairs(ctx.TOPOLOGY.cpus) do
      if other_id ~= cpu_id then peers[#peers + 1] = other_id end
    end
    table.sort(peers)  -- deterministic round-robin order
    kb:add_info_node("bootstrap", "config",
      { kind = "bootstrap" },
      {
        site               = ctx.SITE,
        cpu_id             = cpu_id,
        is_master          = (cpu_id == ctx.MASTER_CPU) and 1 or 0,
        master_cpu         = ctx.MASTER_CPU,
        peers              = peers,   -- Phase 6.1
        kb_root            = string.format("system.site.%s.cpu.%s",
                                           ctx.SITE, cpu_id),
        pg_host            = PG.host,
        pg_port            = PG.port,
        pg_db              = PG.dbname,
        pg_user            = PG.user,
        bit_index          = cpu_cfg.bit_index,
        expected_cpu_count = ctx.CPU_COUNT,
        -- pg_password sourced from env; never baked into the KB.
      },
      "Bootstrap config for host processes")
  end,

}
