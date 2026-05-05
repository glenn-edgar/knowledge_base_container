-- =============================================================================
-- kb_build.lua -- mission_planner KB build function.
--
-- Invoked by apps_builder_framework/driver.lua with ctx.kb already scoped
-- to app_containers.<instance_id>. This function pushes a "spec/manifest"
-- sub-header (so paths land under app_containers.<i>.spec.manifest.*) and
-- mirrors manifest.lua into KB rows:
--
--   .spec.manifest.KB_STATUS_FIELD.<name>   (scalars: version, class)
--   .spec.manifest.KB_JSONB_FIELD.<key>     (blobs:   capabilities, virtual_nodes,
--                                                     wire_formats, ui_protocol,
--                                                     nats_protocol, mqtt_protocol,
--                                                     streams)
--
-- The two-segment "spec/manifest" push is forced by add_header_node's
-- link+name pair shape; "spec" is the namespace marker, "manifest" is the
-- name of this particular spec catalog. Future "spec.tunables" or
-- "spec.placement_hints" can coexist.
-- =============================================================================

-- manifest is provided in ctx by the apps-builder subsystem (it loads
-- each app's manifest.lua via dofile and stores it on the placement
-- before driver.drive). This avoids package.loaded["manifest"] cache
-- collisions between multiple apps building in the same process.

return function(ctx)
    local manifest = ctx.manifest
    assert(type(manifest) == "table",
        "mission_planner kb_build: ctx.manifest missing -- " ..
        "apps-builder subsystem must load manifest.lua before drive()")

    -- Detect facade-vs-bare KB at runtime so this works in both contexts:
    --   - Construct_Data_Tables facade (production build_kb.sh): use
    --     add_status_field / add_jsonb_field so satellite tables get
    --     properly seeded.
    --   - Bare Construct_KB (framework unit tests): use add_info_node;
    --     no satellite tables exist in test context.
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

    -- Sub-header: app_containers.<instance>.spec.manifest
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
