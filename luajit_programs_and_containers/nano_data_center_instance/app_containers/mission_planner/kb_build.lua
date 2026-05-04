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

-- manifest.lua is loaded from the same package directory. The apps-builder
-- driver sets up package.path to include each app's package root before
-- invoking kb_build, so a plain require works.
local manifest = require("manifest")

return function(ctx)
    -- Sub-header: app_containers.<instance>.spec.manifest
    --
    -- Uses add_info_node directly (rather than the facade's add_status_field
    -- / add_jsonb_field) so this works against both bare Construct_KB (used
    -- by the framework's unit tests) AND Construct_Data_Tables (used by
    -- production build_kb.sh). Satellite-table reconciliation is the
    -- facade's job at check_installation time, NOT kb_build's.
    ctx.kb:with_header("spec", "manifest",
        { class = ctx.app_class, kind = ctx.spec.kind },
        {},
        "Manifest catalog for " .. ctx.instance_id,
        function()
            -- KB_STATUS_FIELD scalars.
            for name, value in pairs(manifest.status or {}) do
                ctx.kb:add_info_node(
                    "KB_STATUS_FIELD",
                    name,
                    {},                                              -- properties
                    { value = value },                               -- data
                    "Manifest scalar: " .. name)                     -- description
            end

            -- KB_JSONB_FIELD blobs (doc_type carried in properties).
            for key, blob in pairs(manifest.jsonb or {}) do
                ctx.kb:add_info_node(
                    "KB_JSONB_FIELD",
                    key,
                    { doc_type = "manifest_blob" },                  -- properties
                    blob,                                            -- data
                    "Manifest JSONB: " .. key)                       -- description
            end
        end)
end
