-- =============================================================================
-- manifest.lua -- mission_planner manifest data (locked Phase B Layer A).
--
-- Mirrored to KB by kb_build.lua under
--   app_containers.<instance_id>.spec.manifest.KB_STATUS_FIELD.*  (scalars)
--   app_containers.<instance_id>.spec.manifest.KB_JSONB_FIELD.*   (blobs)
--
-- Pure data; no behavior. Edit this file to change what gets advertised.
-- The container itself reads its own KB rows at startup via kb_query.lua;
-- external schedulers read peer apps' rows through the same surface.
-- =============================================================================

return {

    ---------------------------------------------------------------------------
    -- KB_STATUS_FIELD scalars (one value per row, queryable individually)
    ---------------------------------------------------------------------------

    status = {
        version = "1.0",
        class   = "mission_planner",
    },

    ---------------------------------------------------------------------------
    -- KB_JSONB_FIELD blobs (structured catalogs; queryable via JSONB ops)
    ---------------------------------------------------------------------------

    jsonb = {

        capabilities = {
            "path_planning",
            "energy_budget",
            "transit",
            "drive_base",
        },

        virtual_nodes = {
            "init_check",
            "path_spline",
            "path_line",
            "operation",
            "idle",
            "error_recovery",
        },

        wire_formats = { "json", "cbor" },

        -- HTTP UI -- first-class peer of NATS/MQTT. External schedulers /
        -- monitors can use HTTP without a NATS client. Every NATS subject
        -- has an HTTP equivalent where reasonable. WebSocket / SSE / auth
        -- deferred to v1.1+.
        ui_protocol = {
            scheme        = "http",
            port_internal = 8090,
            endpoints = {
                { path = "/",                       method = "GET",  purpose = "landing" },
                { path = "/api/missions",           method = "POST", purpose = "submit_mission" },
                { path = "/api/missions",           method = "GET",  purpose = "list_missions" },
                { path = "/api/missions/:id",       method = "GET",  purpose = "mission_status" },
                { path = "/api/robots",             method = "GET",  purpose = "list_robots" },
                { path = "/api/blackboard",         method = "GET",  purpose = "blackboard_view" },
            },
        },

        -- NATS -- runtime real-time fan-out. Subjects are absolute (the
        -- `{site}` placeholder is the v3 site name; resolved at startup).
        nats_protocol = {
            port = 4222,
            subjects = {
                { name = "mission_submit",   subject = "{site}.action_server.missions",                purpose = "inbound mission submit" },
                { name = "mission_status",   subject = "{site}.action_server.{robot}.status",          purpose = "per-robot status KV" },
                { name = "mission_result",   subject = "{site}.action_server.{robot}.result",          purpose = "completion / error" },
                { name = "mission_log",      subject = "{site}.action_server.{robot}.log",             purpose = "per-robot log lines" },
                { name = "robot_status_kv",  subject = "{site}.robots.{robot}.status.>",               purpose = "robot self-announce + state" },
                { name = "blackboard_kv",    subject = "{site}.action_server.blackboard.>",            purpose = "shared blackboard fan-out" },
            },
        },

        -- MQTT -- remote-bus / robot transport. Slashes are MQTT-native
        -- (site dots → slashes); kb_build does NOT translate.
        mqtt_protocol = {
            port = 1883,
            topics = {
                { name = "rpc",            topic = "{site_path}/rpc/{robot}",                         purpose = "request/response" },
                { name = "stream_bus",     topic = "{site_path}/stream_bus/{robot}/{stream}",         purpose = "high-rate streams" },
                { name = "link",           topic = "{site_path}/link/{robot}",                        purpose = "link-protocol multiplex" },
                { name = "status_state",   topic = "{site_path}/robots/{robot}/status/state",         purpose = "connected / started_at" },
                { name = "status_energy",  topic = "{site_path}/robots/{robot}/status/energy",        purpose = "energy_max / remaining" },
                { name = "status_bitmask", topic = "{site_path}/robots/{robot}/status/bitmask",       purpose = "raw + heartbeat bit" },
                { name = "status_health",  topic = "{site_path}/robots/{robot}/status/health",        purpose = "self-reported health" },
            },
        },

        -- streams = {} for v1.0 -- planner stays NATS-only for runtime
        -- state. Adding KB-resident streams later bumps to v1.1 once we
        -- know what's worth persisting.
        streams = {},

    },

}
