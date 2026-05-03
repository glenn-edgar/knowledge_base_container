-- =============================================================================
-- definitions.lua -- Container definition catalog (class blueprints).
--
-- One entry per container *type*. Instances reference these by name from
-- topology.lua. Code-versioned; edit means code review.
--
-- Schema per entry:
--   kind          : "infrastructure" | "application" | "host_process"
--   runtime       : container backend ("docker" today; "virtual" planned
--                   for pi-zero software-virtual containers)
--   image         : pre-built image name (infrastructure: never rebuilt)
--   build_ctx     : build-context dir (control/application: rebuilt)
--   entrypoint    : { argv... } the controller will exec
--   env_defaults  : { KEY = "value", ... } non-secret env
--   env_required  : { "POSTGRES_PASSWORD", ... } names resolved from operator
--                   secrets.env at docker run time (NEVER baked into image)
--   default_cfg   : opaque table written into the instance's service info_node
--   ports         : LEGACY host/cont list used by infrastructure defs.
--                   { { host = X, cont = Y }, ... } or { Y, ... } (same).
--                   Mutually exclusive with port_spec.
--   port_spec     : NEW -- per-image named-slot declaration for applications.
--                   Each slot describes an INTERNAL port the image binds on;
--                   topology.lua's instance-level `ports = { <slot> = <ext> }`
--                   supplies the external port numbers author-managed (no
--                   auto-assignment). Slot names are the stable identifier
--                   used by the gateway for routing and by the registry.
--                   Shape:
--                     port_spec = {
--                       <slot_name> = {
--                         internal    = 8080,          -- required, int
--                         protocol    = "tcp",         -- default "tcp"
--                         purpose     = "ui",          -- "ui" | "service" | ...
--                         description = "human text",  -- optional
--                         probe       = {              -- optional; default-off
--                           path           = "/health",-- required when probe present
--                           expect_status  = 200,      -- default 200
--                           interval_s     = 5,        -- default 5
--                           timeout_ms     = 2000,     -- default 2000
--                         },
--                       },
--                       ...
--                     }
--                   Construct-time: def-level uniqueness of `internal` across
--                   slots within a port_spec is enforced; per-CPU uniqueness of
--                   EXTERNAL ports across all instances (port_spec + legacy) is
--                   enforced; every slot must have a matching entry in the
--                   instance's topology-level `ports` table.
--
--                   `probe` block (Phase 4 broker-active HTTP probes):
--                   When present, dcs_host's spec_adapter emits Docker labels
--                   `nanodatacenter.probe.<slot>.{path,expect_status,
--                   interval_s,timeout_ms,internal_port}` at run time. The
--                   docker_host_broker reads those labels, issues HTTP GETs
--                   against the container's internal bridge IP (NOT through
--                   host port forwarding -- bypasses vpnkit), and publishes
--                   per-container probe state in containers.snapshot. dcs.lua
--                   WATCHDOG trips when probe.fail_streak crosses
--                   WATCHDOG_FAIL_THRESHOLD. See WIRE_PROTOCOL.md
--                   "Broker-active HTTP probes" for the full wire shape.
--   volumes       : { { host = "~/Postgres_Data", cont = "/var/lib/..." }, ... }
--                   host-side paths resolved against $HOME at run time
--   labels        : { key = value, ... } -- "nanodatacenter=true" added
--                   implicitly for docker stop-labelled semantics
--   restart_policy: docker restart policy ("no" default, "always" for pg)
--   cli_databases : { "name1", "name2", ... } per-app-CLI db files
--
-- Infrastructure entries match the existing planner-net containers.
-- =============================================================================

return {

  ----------------------------------------------------------------------
  -- Infrastructure (pre-built images; system_control starts/stops)
  ----------------------------------------------------------------------

  -- service_contract on infrastructure defs declares the abstract service
  -- name + port that system_control's INFRA_PUBLISH state advertises into
  -- system.<sys>.site.<S>.infrastructure.<service_type>.KB_STATUS_FIELD.*.
  -- App containers query that path via /opt/apps/lib/infra_discovery.lua
  -- (no env-injection chain for these endpoints; pg is the only
  -- rendez-vous point).

  postgres = {
    kind          = "infrastructure",
    runtime       = "docker",
    image         = "pgvector/pgvector:pg17",
    ports         = { { host = 5432, cont = 5432 } },
    env_defaults  = {
      POSTGRES_USER = "gedgar",
      POSTGRES_DB   = "knowledge_base",
    },
    env_required  = { "POSTGRES_PASSWORD" },
    volumes       = {
      { host = "~/Postgres_Data/vector", cont = "/var/lib/postgresql/data" },
    },
    restart_policy = "always",
    default_cfg    = { db_name = "knowledge_base", user = "gedgar" },
    service_contract = {
      service_type = "postgres",
      port         = 5432,
      protocol     = "tcp",
    },
  },

  nats = {
    kind    = "infrastructure",
    runtime = "docker",
    image   = "nanodatacenter/nats-js-ram:latest",
    ports   = {
      { host = 4222, cont = 4222 },
      { host = 9222, cont = 9222 },
    },
    restart_policy = "always",
    service_contract = {
      service_type = "nats",
      port         = 4222,
      protocol     = "tcp",
    },
  },

  mosquitto = {
    kind    = "infrastructure",
    runtime = "docker",
    image   = "nanodatacenter/mosquitto-ram-ws:latest",
    -- 1883 = MQTT, 9001 = MQTT-over-WebSocket (install script binds both).
    -- Declared here so construct-time per-CPU conflict detection catches
    -- collisions with application ports.
    ports   = { { host = 1883, cont = 1883 },
                { host = 9001, cont = 9001 } },
    restart_policy = "always",
    service_contract = {
      service_type = "mqtt",
      port         = 1883,
      protocol     = "tcp",
    },
  },

  kv_bridge = {
    kind    = "infrastructure",
    runtime = "docker",
    image   = "nanodatacenter/kv-bridge:latest",
    ports   = { { host = 8080, cont = 8080 } },
    restart_policy = "always",
    service_contract = {
      service_type = "kv_bridge",
      port         = 8080,
      protocol     = "tcp",
    },
  },

  -- NOTE: system_control + node_control are NOT containers. They run as
  -- one DCS host process (host_processes/dcs.lua) managed by
  -- build_output/<cpu>/start.sh. They don't appear in this catalog.

  ----------------------------------------------------------------------
  -- Applications
  ----------------------------------------------------------------------

  -- test_app: 4-process shell container used to exercise the full
  -- registration/deregistration path through node_control + gateway.
  -- Image is built out-of-tree in building_blocks/test_app/. Two
  -- supervised web processes bind internal ports 8080 + 8081; two
  -- supervised lua worker processes have no ports.
  test_app = {
    kind          = "application",
    runtime       = "docker",
    image         = "nanodatacenter/test-app:latest",
    restart_policy = "unless-stopped",
    port_spec = {
      exceptions_ui = {
        internal    = 8080,
        protocol    = "tcp",
        purpose     = "ui",
        description = "Exception aggregation viewer (shell)",
        probe = {
          path = "/health",
        },
      },
      logs_ui = {
        internal    = 8081,
        protocol    = "tcp",
        purpose     = "ui",
        description = "Log aggregation viewer (shell)",
      },
    },
  },

  -- dcs_console: two-web-server pod. gateway slot will become the
  -- site-wide reverse proxy; admin slot will become the real operator
  -- UI (pg reads + htmx). Both start as shells; slot names are final so
  -- filling them in doesn't churn CONTAINER_REGISTRY rows.
  dcs_console = {
    kind          = "application",
    runtime       = "docker",
    image         = "nanodatacenter/dcs-console:latest",
    restart_policy = "unless-stopped",
    port_spec = {
      gateway = {
        internal    = 8080,
        protocol    = "tcp",
        purpose     = "ui",
        description = "Site-wide reverse proxy (shell)",
      },
      admin = {
        internal    = 8081,
        protocol    = "tcp",
        purpose     = "ui",
        description = "DCS operator admin UI (shell)",
      },
    },
  },

  -- ros_mission_planner_ii: two-process pod (worker + UI). Shell for
  -- now; real planner logic gets ported in a later session.
  ros_mission_planner_ii = {
    kind          = "application",
    runtime       = "docker",
    image         = "nanodatacenter/ros-mission-planner-ii:latest",
    restart_policy = "unless-stopped",
    port_spec = {
      planner_ui = {
        internal    = 8080,
        protocol    = "tcp",
        purpose     = "ui",
        description = "Mission planner operator UI (shell)",
      },
    },
  },

  -- robot_manager: two-process pod (worker + UI). Shell for now; real
  -- fleet-manager logic fills this in alongside ros_fleet_manager later.
  robot_manager = {
    kind          = "application",
    runtime       = "docker",
    image         = "nanodatacenter/robot-manager:latest",
    restart_policy = "unless-stopped",
    port_spec = {
      manager_ui = {
        internal    = 8080,
        protocol    = "tcp",
        purpose     = "ui",
        description = "Robot fleet manager operator UI (shell)",
      },
    },
  },

  -- observability: Task 4 SCADA-style observability hub. Singleton on
  -- master. Four supervised processes:
  --   exception_analyzer (lua)  -- SYS_EXCEPTION janitor
  --   log_analyzer       (lua)  -- KB_LOG ingest + rule eval + rollups
  --   exception_web      (ors)  -- alarm ops UI (internal 8080)
  --   log_web            (ors)  -- strip charts + rule inventory (internal 8081)
  -- Phase 5 is a shell build; Phases 6-8 fill in real logic.
  observability = {
    kind          = "application",
    runtime       = "docker",
    image         = "nanodatacenter/observability:latest",
    restart_policy = "unless-stopped",
    -- Analyzer + web servers all talk to the master Postgres. node_control
    -- resolves these from its host env at `docker run` time.
    env_required  = { "POSTGRES_PASSWORD" },
    env_defaults  = {
      PG_HOST = "host.docker.internal",
      PG_PORT = "5432",
      PG_DB   = "knowledge_base",
      PG_USER = "gedgar",
    },
    port_spec = {
      exception_web = {
        internal    = 8080,
        protocol    = "tcp",
        purpose     = "ui",
        description = "SCADA alarm operations UI (shell -> phase 8)",
      },
      log_web = {
        internal    = 8081,
        protocol    = "tcp",
        purpose     = "ui",
        description = "Log strip charts + rule inventory (shell -> phase 8)",
      },
    },
  },

}
