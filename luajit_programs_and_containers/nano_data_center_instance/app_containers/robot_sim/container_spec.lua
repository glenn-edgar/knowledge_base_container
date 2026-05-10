-- =============================================================================
-- container_spec.lua -- robot_sim docker container shape (Phase 7 ROBSIM C2).
--
-- Validated by apps_builder_framework/container_spec_validator.lua before
-- kb_build.lua is invoked. Defines what `docker run` needs.
--
-- Convention: APP_SITE / APP_SYSTEM / APP_NAME / CONTAINER_NAME / CPU_ID
-- are injected unconditionally by node_control's container launcher and
-- do NOT appear in env_required. Only declare app-specific env vars here.
--
-- Per-instance robot identity (ROBOT_ID, PLANNER_NAMESPACE, capabilities)
-- comes from topology.cpus.<cpu>.instances[i].params -- node_control
-- lifts those into env vars when launching. Until that mapping lands,
-- the operator passes them via `-e` flags during cluster smoke (see
-- continue.md Step B recipe).
-- =============================================================================

return {
    class = "robot_sim",
    image = "nanodatacenter/robot-sim:latest",
    kind  = "application",

    -- No port_spec: headless single-process container. node_control's
    -- HTTP watchdog therefore won't poll it; reconcile loop watches by
    -- container name only (sufficient for v1).
    port_spec = {},

    -- App-specific env vars sourced from topology instance params.
    env_required = {
        "ROBOT_ID",            -- e.g. "rover_1"
        "PLANNER_NAMESPACE",   -- which tenant owns this robot (must match
                               -- a mission_planner instance's namespace)
        "MQTT_HOST",           -- e.g. "mosquitto-ram-ws_main" (planner-net)
        "MQTT_PORT",           -- e.g. "1883"
    },

    -- Future: capabilities, energy_max, energy_remaining could become
    -- env vars too; for now the simulator hardcodes class="lunar_rover"
    -- + energy_max=10000 (matches mock_mqtt_robot.lua defaults).

    volumes = {},
}
