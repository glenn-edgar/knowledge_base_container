-- =============================================================================
-- driver.lua -- apps-builder driver. Walks placement table, calls each
-- container's kb_build(ctx) under a scoped path stack.
--
-- Scope discipline (Phase B Layer F):
--   For each placement entry, the driver:
--     1. Validates the container_spec (see container_spec_validator.lua).
--     2. Records the kb's path-stack depth at entry.
--     3. Opens with_header("app_containers", placement.instance_id, ...)
--        so any kb method called inside writes under
--        `system.<sys>.site.<S>.app_containers.<instance_id>.*`.
--     4. Calls placement.kb_build(ctx)  -- per-app spec.* writes.
--     5. Emits placement.current.KB_STATUS_FIELD.{cpu, role}  -- framework-
--        owned, derived from the placement table; consumed at runtime by
--        node_control to decide what to start on each CPU (Phase B Layer N).
--     6. Pre-allocates runtime.heartbeat.KB_STATUS_FIELD.snapshot with
--        empty defaults; the container UPDATEs this row each tick at
--        runtime so observability + dcs_console + a future runtime.cpu
--        drift detector can see liveness uniformly across all apps
--        (Phase B.2).
--     7. Verifies the path stack returned to the entry depth (catches
--        apps that forgot to leave a header). Re-raises if mismatched.
--     8. with_header pops automatically (also on error).
--   Atomic failure: any kb_build error propagates; the build aborts and
--   the calling code is responsible for transaction rollback.
--
-- The driver does NOT execute container-level commission steps (image
-- builds, port allocation, runtime placement). Those are decided by site
-- config and emitted by node_control / build_kb's existing subsystems.
-- The driver's only job is invoking each app's kb_build under the right
-- anchor and stamping placement assignment.
-- =============================================================================

local validator   = require("container_spec_validator")
local ctx_builder = require("ctx_builder")

local M = {}

local function path_depth(kb)
  -- construct_kb (postgres + sqlite) keeps kb.path[<working_kb>] as the
  -- live path-stack array. We snapshot its length at entry and verify
  -- it returns to the same length after kb_build returns. With_header
  -- guarantees the SEGMENTS pushed by it are popped (it pcalls the body
  -- and unconditionally pops on the way out), so any mismatch means the
  -- app called add_header_node without a matching leave -- corrupted
  -- state.
  --
  -- The kb may be either a bare Construct_KB (Layer F unit-test usage)
  -- OR a Construct_Data_Tables facade (production build_kb.lua). The
  -- facade aliases .path to its inner kb but does NOT mirror
  -- .working_kb on every select_kb call -- so we fall through to the
  -- inner kb's working_kb when the outer is unset.
  local working_kb = kb.working_kb
  if working_kb == nil and kb.kb then working_kb = kb.kb.working_kb end
  local stack = kb.path and kb.path[working_kb]
  if type(stack) ~= "table" then
    error("driver: kb has no path stack for working_kb=" ..
          tostring(working_kb))
  end
  return #stack
end

--- Drive one app build.
--- @param kb table        active KB handle (Construct_KB, sqlite or pg)
--- @param read_kb table   unscoped reader (typically the same handle, used
---                         for cross-container discovery during build).
--- @param placement table  one entry: { instance_id, app_class, cpu, role,
---                          spec, kb_build (function) }
--- @return boolean, string?  ok, err_message
local function drive_one(kb, read_kb, placement)
  -- 1. Spec validation.
  local ok, err = validator.validate(placement.spec, placement.instance_id)
  if not ok then return false, err end

  -- 2. Required fields beyond container_spec.
  if type(placement.kb_build) ~= "function" then
    return false, string.format(
      "placement[%s] missing kb_build function", placement.instance_id)
  end

  -- 3. Snapshot path depth + open the anchor scope.
  local entry_depth = path_depth(kb)
  local ctx = ctx_builder.build(placement, kb, read_kb)

  -- Detect facade-vs-bare kb the same way mission_planner's kb_build does:
  -- production build_kb runs against Construct_Data_Tables (facade with
  -- add_status_field); framework unit tests run against bare Construct_KB
  -- (no satellite tables, only add_info_node).
  local use_facade = type(kb.add_status_field) == "function"

  local function emit_placement()
    -- Sub-header: app_containers.<i>.placement.current
    -- Two-segment shape mirrors spec.manifest (forced by add_header_node's
    -- link+name pair contract); "placement" is the namespace marker,
    -- "current" is the active assignment record. Future "placement.preferred"
    -- (load-balancer hints) or "placement.history" (audit) can coexist.
    kb:with_header("placement", "current",
      { source = "topology", class = placement.app_class },
      {},
      "Placement assignment for " .. placement.instance_id,
      function()
        if use_facade then
          kb:add_status_field("cpu", {},
            "CPU id this app is assigned to (placement source of truth; " ..
            "node_control reads this to decide what to start)",
            { value = placement.cpu })
          kb:add_status_field("role", {},
            "Placement role (active | passive | ...)",
            { value = placement.role or "active" })
        else
          kb:add_info_node("KB_STATUS_FIELD", "cpu", {},
            { value = placement.cpu },
            "CPU id this app is assigned to")
          kb:add_info_node("KB_STATUS_FIELD", "role", {},
            { value = placement.role or "active" },
            "Placement role")
        end
      end)
  end

  local function emit_runtime_skeleton()
    -- Sub-header: app_containers.<i>.runtime.heartbeat
    -- Pre-allocates the runtime liveness snapshot every app must update at
    -- startup + each tick. Single status field "snapshot" (JSON-shaped):
    --   { at = unix_ms, host = container_dns, cpu = APP_CPU_ID, ui_port = N }
    -- App-side updater: kb_status.set_status_data(pg, path, {value = {...}})
    -- Consumers: dcs_console liveness probe; observability tree;
    -- node_control runtime.cpu vs placement.cpu drift detection (future).
    -- App-specific runtime sub-namespaces (runtime.metrics, runtime.connections)
    -- are added by the app's own kb_build under runtime.* if it needs them.
    kb:with_header("runtime", "heartbeat", { kind = "runtime" }, {},
      "Runtime liveness snapshot (container fills each tick)",
      function()
        local default = { at = 0, host = "", cpu = "", ui_port = 0 }
        if use_facade then
          kb:add_status_field("snapshot", {},
            "App runtime snapshot updated each tick by the container",
            default)
        else
          kb:add_info_node("KB_STATUS_FIELD", "snapshot", {},
            default, "App runtime snapshot")
        end
      end)
  end

  -- Gap-1 fix (post-ROBSIM C3 smoke 2026-05-10): emit per-instance
  -- topology params as a KB jsonb blob so node_control's launcher
  -- (kb_assignments.lua + user_functions.lua) can read them at
  -- container-spawn time and inject as env vars (uppercase keys,
  -- scalar values only). Skipped when placement has no params.
  -- Path: app_containers.<inst>.spec.params.KB_JSONB_FIELD.params
  local function emit_spec_params()
    if not placement.params or not next(placement.params) then return end
    kb:with_header("spec", "params",
      { kind = "placement_params", class = placement.app_class },
      {},
      "Topology instance params for " .. placement.instance_id,
      function()
        if use_facade then
          kb:add_jsonb_field("params", "placement_params",
            "Per-instance topology params (robot_id, " ..
            "planner_namespace, capabilities, ...) -- node_control's " ..
            "launch_assignment lifts scalar values into the container's " ..
            "docker run env (uppercase keys).",
            placement.params)
        else
          kb:add_info_node("KB_JSONB_FIELD", "params",
            { doc_type = "placement_params" },
            placement.params,
            "Per-instance topology params")
        end
      end)
  end

  local body_ok, body_err = pcall(function()
    kb:with_header("app_containers", placement.instance_id,
      { class = placement.app_class, kind = placement.spec.kind },
      {},
      "App container anchor: " .. placement.instance_id,
      function()
        local fn_ok, fn_err = placement.kb_build(ctx)
        if fn_ok == false then
          error(string.format(
            "kb_build returned ok=false: %s", tostring(fn_err)), 0)
        end
        emit_placement()
        emit_spec_params()
        emit_runtime_skeleton()
      end)
  end)

  -- 4. Verify path stack returned to entry depth (catches malformed apps
  -- that opened a header inside kb_build but never closed it).
  local exit_depth = path_depth(kb)
  if exit_depth ~= entry_depth then
    return false, string.format(
      "placement[%s] kb_build left path stack at depth %d (entry was %d) -- " ..
      "missing leave_header_node? scope corruption",
      placement.instance_id, exit_depth, entry_depth)
  end

  if not body_ok then
    return false, string.format(
      "placement[%s] kb_build failed: %s",
      placement.instance_id, tostring(body_err))
  end

  return true
end
M._drive_one = drive_one  -- exposed for unit tests

--- Drive the full placement table. Atomic-fail: stops at the first
--- failing app.
--- @param kb table         active KB handle (already in select_kb scope)
--- @param read_kb table    unscoped reader (commonly == kb)
--- @param placements table  array of placement entries (see drive_one)
--- @return boolean, string?, integer  ok, err_message, count_built
function M.drive(kb, read_kb, placements)
  assert(type(kb) == "table",          "drive: kb required")
  assert(type(read_kb) == "table",     "drive: read_kb required")
  assert(type(placements) == "table",  "drive: placements must be an array")

  -- Detect duplicate instance_id BEFORE any pg writes.
  local seen = {}
  for i, p in ipairs(placements) do
    if type(p) ~= "table" or type(p.instance_id) ~= "string" then
      return false, string.format(
        "placements[%d] missing instance_id", i), 0
    end
    if seen[p.instance_id] then
      return false, string.format(
        "duplicate instance_id %q at placements[%d] and [%d]",
        p.instance_id, seen[p.instance_id], i), 0
    end
    seen[p.instance_id] = i
  end

  local built = 0
  for _, p in ipairs(placements) do
    local ok, err = drive_one(kb, read_kb, p)
    if not ok then
      return false, err, built
    end
    built = built + 1
  end
  return true, nil, built
end

return M
