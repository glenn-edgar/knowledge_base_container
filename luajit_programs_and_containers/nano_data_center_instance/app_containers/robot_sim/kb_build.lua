-- =============================================================================
-- kb_build.lua -- robot_sim KB build function (Phase 7 ROBSIM C2).
--
-- Invoked by apps_builder_framework/driver.lua with ctx.kb already
-- scoped to app_containers.<instance_id>. Mirrors mission_planner's
-- shape but simpler -- robot_sim has no mission_log / runtime ring,
-- only the manifest catalog.
--
-- Per-tenant robot identity (the operator-facing "robot row") is
-- emitted by a separate subsystem (construction/subsystems/robots.lua)
-- under planner.<ns>.robots.<robot_id>. That row is NOT created here;
-- this kb_build.lua only mirrors the manifest catalog under the
-- container's own app_containers.<instance_id>.spec.manifest path.
-- =============================================================================

return function(ctx)
    local manifest = ctx.manifest
    assert(type(manifest) == "table",
        "robot_sim kb_build: ctx.manifest missing -- " ..
        "apps-builder subsystem must load manifest.lua before drive()")

    -- Detect facade-vs-bare KB at runtime (same pattern as
    -- mission_planner/kb_build.lua).
    local use_facade = type(ctx.kb.add_status_field) == "function"
                   and type(ctx.kb.add_jsonb_field)  == "function"

    local function emit_status(name, value)
        if use_facade then
            ctx.kb:add_status_field(
                name, {}, "Manifest scalar: " .. name, { value = value })
        else
            ctx.kb:add_info_node(
                "KB_STATUS_FIELD", name, {}, { value = value },
                "Manifest scalar: " .. name)
        end
    end

    local function emit_jsonb(key, blob)
        if use_facade then
            ctx.kb:add_jsonb_field(
                key, "manifest_blob", "Manifest JSONB: " .. key, blob)
        else
            ctx.kb:add_info_node(
                "KB_JSONB_FIELD", key, { doc_type = "manifest_blob" },
                blob, "Manifest JSONB: " .. key)
        end
    end

    ctx.kb:with_header("spec", "manifest",
        { class = ctx.app_class, kind = ctx.spec.kind },
        {},
        "Manifest catalog for " .. ctx.instance_id,
        function()
            for name, value in pairs(manifest.status or {}) do
                emit_status(name, value)
            end
            for key, blob in pairs(manifest.jsonb or {}) do
                emit_jsonb(key, blob)
            end
        end)
end
