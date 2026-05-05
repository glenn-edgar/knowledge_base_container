-- =============================================================================
-- subsystems/apps_builder.lua
--
-- Phase B Layer I -- in-process apps-builder driver. Walks topology
-- placements, finds those with a per-app package under
--   <NDC_INSTANCE_BASE>/app_containers/<def_name>/
-- and drives the apps_builder_framework's `driver.drive()` to mirror
-- each app's manifest.lua into KB rows under
--   system.<sys>.site.<S>.app_containers.<instance_id>.spec.manifest.*
--
-- Why a subsystem instead of a one-shot container: build_kb.sh already
-- has POSTGRES_PASSWORD + pg connection + LUA_PATH set up; per-app
-- kb_build files are pure Lua; running in-process means atomic-fail
-- propagates as a single Lua error (the framework's atomic-fail
-- invariant). A separate container would duplicate the connection +
-- LUA_PATH setup with no native-deps justification.
--
-- Auto-discovery: an app gets picked up iff its package dir contains
-- ALL THREE of manifest.lua, container_spec.lua, kb_build.lua. New
-- apps just drop a package dir (and a topology placement) -- no edit
-- here required.
-- =============================================================================

local function file_exists(path)
  local f = io.open(path, "r")
  if f then f:close(); return true end
  return false
end

local function load_app_package(pkg_dir)
  -- Returns container_spec, manifest, kb_build (all loaded fresh) or
  -- nil, err. Each load uses dofile so package.loaded isn't polluted.
  local cs_path = pkg_dir .. "/container_spec.lua"
  local mf_path = pkg_dir .. "/manifest.lua"
  local kb_path = pkg_dir .. "/kb_build.lua"

  for _, p in ipairs({ cs_path, mf_path, kb_path }) do
    if not file_exists(p) then
      return nil, "missing required file: " .. p
    end
  end

  local ok_cs, container_spec = pcall(dofile, cs_path)
  if not ok_cs then return nil, "container_spec.lua: " .. tostring(container_spec) end
  if type(container_spec) ~= "table" then
    return nil, "container_spec.lua must return a table"
  end

  local ok_mf, manifest = pcall(dofile, mf_path)
  if not ok_mf then return nil, "manifest.lua: " .. tostring(manifest) end
  if type(manifest) ~= "table" then
    return nil, "manifest.lua must return a table"
  end

  local ok_kb, kb_build = pcall(dofile, kb_path)
  if not ok_kb then return nil, "kb_build.lua: " .. tostring(kb_build) end
  if type(kb_build) ~= "function" then
    return nil, "kb_build.lua must return a function"
  end

  return container_spec, manifest, kb_build
end

return {

  install_site = function(ctx)
    local instance_base = os.getenv("NDC_INSTANCE_BASE")
    if not instance_base or instance_base == "" then
      print("  [apps_builder] NDC_INSTANCE_BASE unset -- skipping (no apps to build)")
      return
    end
    local app_root = instance_base .. "/app_containers"

    local driver = require("driver")
    local placements = {}
    local skipped    = {}

    -- Walk topology in deterministic order so build output is stable.
    local cpu_ids = {}
    for cpu_id, _ in pairs(ctx.TOPOLOGY.cpus) do
      cpu_ids[#cpu_ids + 1] = cpu_id
    end
    table.sort(cpu_ids)

    for _, cpu_id in ipairs(cpu_ids) do
      local cpu_cfg = ctx.TOPOLOGY.cpus[cpu_id]
      for _, inst in ipairs(cpu_cfg.instances or {}) do
        local def = ctx.DEFINITIONS[inst.def]
        if def and def.kind == "application" then
          local pkg_dir = app_root .. "/" .. inst.def
          local cs, mf, kb_fn = load_app_package(pkg_dir)
          if cs then
            placements[#placements + 1] = {
              instance_id = inst.name,
              app_class   = cs.class,
              cpu         = cpu_id,
              role        = inst.role or "active",
              spec        = cs,
              manifest    = mf,
              kb_build    = kb_fn,
            }
          else
            -- Not a fatal error: an "application" def may legitimately
            -- not have ported under apps-builder yet (e.g. test_app,
            -- robot_manager pre-Layer-A). Record + continue.
            skipped[#skipped + 1] = string.format(
              "%s (def=%q on %s): %s", inst.name, inst.def, cpu_id, tostring(mf))
          end
        end
      end
    end

    if #placements == 0 then
      print(string.format(
        "  [apps_builder] no apps with packages found under %s", app_root))
      for _, s in ipairs(skipped) do
        print("    skipped: " .. s)
      end
      return
    end

    print(string.format(
      "  [apps_builder] driving %d app(s):", #placements))
    for _, p in ipairs(placements) do
      print(string.format("    - %s (class=%s, cpu=%s)",
        p.instance_id, p.app_class, p.cpu))
    end
    for _, s in ipairs(skipped) do
      print("    skipped: " .. s)
    end

    local ok, err, count = driver.drive(ctx.kb, ctx.kb, placements)
    if not ok then
      error(string.format(
        "apps_builder: %d/%d apps committed before failure: %s",
        count, #placements, err))
    end
    print(string.format("  [apps_builder] %d apps committed", count))
  end,

}
