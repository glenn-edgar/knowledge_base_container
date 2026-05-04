-- =============================================================================
-- container_spec.lua -- mission_planner docker container shape.
--
-- Validated by apps_builder_framework/container_spec_validator.lua before
-- kb_build.lua is invoked. Defines what `docker run` needs.
--
-- Convention: APP_SITE / APP_SYSTEM / APP_NAME / CONTAINER_NAME / CPU_ID
-- are injected unconditionally by node_control's container launcher and
-- do NOT appear in env_required. Only declare app-specific env vars here.
-- =============================================================================

return {
    class = "mission_planner",
    image = "nanodatacenter/mission-planner:latest",
    kind  = "application",

    port_spec = {
        ui = {
            internal    = 8090,
            protocol    = "tcp",
            purpose     = "ui",
            description = "planner_ui HTTP surface (matches manifest ui_protocol.port_internal)",
        },
    },

    -- NATS / MQTT broker addresses are looked up at runtime via
    -- infra_discovery.lua (Layer A-pre). No env-var injection chain
    -- for these.
    env_required = {},

    volumes = {},
}
