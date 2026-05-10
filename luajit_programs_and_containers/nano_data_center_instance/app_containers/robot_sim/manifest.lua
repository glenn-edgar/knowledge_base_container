-- =============================================================================
-- manifest.lua -- robot_sim manifest data (Phase 7 ROBSIM C2).
--
-- Mirrored to KB by kb_build.lua under
--   app_containers.<instance_id>.spec.manifest.KB_STATUS_FIELD.*  (scalars)
--   app_containers.<instance_id>.spec.manifest.KB_JSONB_FIELD.*   (blobs)
--
-- Pure data; no behavior. The simulator's runtime container reads its
-- robot identity (robot_id, planner_namespace, capabilities) from env
-- vars injected at `docker run` time -- see container_spec.env_required.
-- =============================================================================

return {

    ---------------------------------------------------------------------------
    -- KB_STATUS_FIELD scalars
    ---------------------------------------------------------------------------

    status = {
        version = "1.0",
        class   = "robot_sim",
    },

    ---------------------------------------------------------------------------
    -- KB_JSONB_FIELD blobs
    ---------------------------------------------------------------------------

    jsonb = {

        capabilities = {
            "urlp_link_protocol",
            "drive_packet_echo",
            "activate_action_echo",
            "blind_success",       -- responds kb_done success=true for any
                                   -- action; no real dock interaction
        },

        -- robot_sim has no UI surface and no NATS surface. It speaks
        -- ONLY MQTT (planner-net to mosquitto-ram-ws_main). The
        -- ui_protocol / nats_protocol blocks are absent on purpose.
        --
        -- MQTT topics (per-robot; site_path is dots->slashes of APP_SITE):
        --   {site_path}/rpc/{robot}            inbound from planner
        --   {site_path}/stream_bus/{robot}/    outbound ack + kb_done
        --   {site_path}/link/{robot}           link protocol multiplex
        mqtt_protocol = {
            port = 1883,
            topics = {
                { name = "rpc",
                  topic = "{site_path}/rpc/{robot}",
                  purpose = "inbound RPC commands (drive packets + activate)" },
                { name = "stream_bus",
                  topic = "{site_path}/stream_bus/{robot}/{stream}",
                  purpose = "outbound responses (drive_ack, drive_done, ack, kb_done)" },
                { name = "link_out",
                  topic = "{site_path}/link/{robot}",
                  purpose = "URLP link multiplex (announce, confirm, heartbeat, disconnect)" },
            },
        },

        wire_formats = { "json" },

        streams = {},

    },

}
