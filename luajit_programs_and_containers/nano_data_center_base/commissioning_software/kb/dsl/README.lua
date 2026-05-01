--[[
  kb_dsl/ — Knowledge Base DSL Build Directory

  Purpose:
    Single source of truth for the embedded data center KB.
    One master DSL script populates both Postgres (system controller)
    and per-domain SQLite extracts (node controllers).

  Directory layout:
    scripts/          DSL build scripts
      master_build.lua        Entry point — builds everything
      physical_tree.lua       Hardware topology (CPUs, services)
      software_tree.lua       Subsystem definitions (domains, containers)
      bindings.lua            Maps container defs -> CPU nodes
      sqlite_extract.lua      Extracts per-domain SQLite DBs from Postgres

    sqlite_dbs/       Generated SQLite databases (one per domain)
      surface_ops.db
      fleet.db
      ...

    templates/        Reusable node templates (robot types, service configs)

  Usage:
    cd kb_dsl
    luajit scripts/master_build.lua

  Down the road this directory becomes a container volume.
]]
