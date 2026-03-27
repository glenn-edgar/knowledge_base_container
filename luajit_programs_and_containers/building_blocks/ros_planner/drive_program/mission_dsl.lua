-- mission_dsl.lua — Missions with ChainTree exception handling
--
-- Each mission maps to a ChainTree exception_handler with three phases:
--   MAIN     — the primary action sequence
--   RECOVERY — iterative step recovery (max_steps retries + skip condition)
--   FINALIZE — cleanup that runs regardless of outcome
--
-- The match-level supervisor manages mission-level failures using
-- Erlang one-for-one strategy with leaky-bucket rate limiting.
--
-- RPC calls to the hub activate hidden nodes (client/server pattern).
-- Nested exception handlers allow inner faults (e.g., RPC) to be caught
-- before outer handlers (e.g., navigation).
--
-- Recovery action types:
--   "reroute"    — bidirectional Dijkstra re-search from current position
--   "sequence"   — execute a list of actions
--   "rpc_retry"  — re-activate the named RPC service hidden node

missions = {
  time_limit = 150,  -- seconds (2.5 min match)

  ---------------------------------------------------------------------------
  -- Match-level supervisor
  ---------------------------------------------------------------------------
  -- Maps to CFL_SUPERVISOR_MAIN:
  --   strategy     → termination_type (0=one_for_one, 1=one_for_all, 2=rest_for_one)
  --   max_failures → max_reset_number (leaky bucket capacity)
  --   reset_window → reset_window (ms, bucket leak interval)
  --   on_all_failed → finalize_function_id (called when bucket overflows)
  supervisor = {
    strategy      = "one_for_one",
    max_failures  = 3,
    reset_window  = 30000,
    on_all_failed = "return_home",
  },

  ---------------------------------------------------------------------------
  -- M1: Deliver Part
  ---------------------------------------------------------------------------
  M1 = {
    name             = "deliver_part",
    target_node      = "assembly",
    approach_heading = 270,
    points           = 15,
    preconditions    = {},

    -- MAIN phase: navigate to target, then execute actions
    actions = {
      { "drive_straight", 80 },
      { "arm_to_angle", -45 },
      { "drive_straight", -80 },
    },

    -- RECOVERY phase: iterative step recovery
    -- max_steps maps to cfl_exception_support_data_t.max_steps
    -- skip_condition maps to boolean aux_function on recovery column
    recovery = {
      max_steps      = 2,
      skip_condition = "at_target_node",
      steps = {
        { type = "reroute", scope = "local" },
        { type = "sequence", actions = {
            { "drive_straight", -100 },
            { "reroute", "local" },
        }},
      },
    },

    -- FINALIZE phase: always runs
    finalize = {
      { "kb_write", "mission.M1.status", "attempted" },
    },
  },

  ---------------------------------------------------------------------------
  -- M2: Paint Sample
  ---------------------------------------------------------------------------
  M2 = {
    name             = "paint_sample",
    target_node      = "painting",
    approach_heading = 270,
    points           = 20,
    preconditions    = {},

    actions = {
      { "drive_straight", 60 },
      { "arm_to_angle", -60 },
      { "wait", 500 },
      { "arm_to_angle", 0 },
      { "drive_straight", -60 },
    },

    recovery = {
      max_steps      = 1,
      skip_condition = nil,
      steps = {
        { type = "reroute", scope = "local" },
      },
    },

    finalize = {
      { "kb_write", "mission.M2.status", "attempted" },
    },
  },

  ---------------------------------------------------------------------------
  -- M3: Pass Gate (uses RPC + nested exception handler)
  ---------------------------------------------------------------------------
  -- This mission has a nested handler structure:
  --
  --   Mission M3 (outer exception_handler)
  --     MAIN column:
  --       navigate_to(gate_zone)
  --       Gate Actions (inner exception_handler)
  --         MAIN: [rpc_call, rpc_expect, drive, rpc_call]
  --         RECOVERY: [rpc_retry, then mark_blocked+reroute]
  --         FINALIZE: [kb_write]
  --     RECOVERY column: [reroute to alternative]
  --     FINALIZE column: [kb_write]
  --
  -- RPC faults caught at inner level; navigation faults bubble to outer.
  M3 = {
    name             = "pass_gate",
    target_node      = "gate_zone",
    approach_heading = 0,
    points           = 25,
    preconditions    = {},

    -- rpc_call activates hidden server node via request_port
    -- rpc_expect checks response_port return value
    actions = {
      { "rpc_call", "gate.open" },
      { "rpc_expect", "status", "open" },
      { "drive_straight", 200 },
      { "rpc_call", "gate.close" },
    },

    recovery = {
      max_steps      = 2,
      skip_condition = "gate_is_open",
      steps = {
        { type = "rpc_retry", service = "gate.open" },
        { type = "sequence", actions = {
            { "kb_write", "connection.gate_zone.shipping.status", "blocked" },
            { "reroute", "local" },
        }},
      },
    },

    finalize = {
      { "kb_write", "mission.M3.status", "attempted" },
    },
  },

  ---------------------------------------------------------------------------
  -- M4: Load Shipping
  ---------------------------------------------------------------------------
  M4 = {
    name             = "load_shipping",
    target_node      = "shipping",
    approach_heading = 0,
    points           = 20,
    preconditions    = { "M3" },  -- must pass gate first

    actions = {
      { "drive_straight", 100 },
      { "arm_to_angle", -30 },
      { "drive_straight", 50 },
      { "arm_to_angle", 0 },
      { "drive_straight", -150 },
    },

    recovery = {
      max_steps      = 1,
      skip_condition = nil,
      steps = {
        { type = "reroute", scope = "local" },
      },
    },

    finalize = {
      { "kb_write", "mission.M4.status", "attempted" },
    },
  },

  ---------------------------------------------------------------------------
  -- M5: Inspection Scan
  ---------------------------------------------------------------------------
  M5 = {
    name             = "inspection_scan",
    target_node      = "inspection",
    approach_heading = 0,
    points           = 20,
    preconditions    = {},

    actions = {
      { "align_to_line" },
      { "color_read", "inspect" },
      { "kb_write", "mission.M5.result", "$color" },
      { "drive_straight", -80 },
    },

    recovery = {
      max_steps      = 2,
      skip_condition = nil,
      steps = {
        { type = "sequence", actions = {
            { "drive_straight", -40 },
            { "align_to_line" },
        }},
        { type = "reroute", scope = "local" },
      },
    },

    finalize = {
      { "kb_write", "mission.M5.status", "attempted" },
    },
  },

  ---------------------------------------------------------------------------
  -- Strategy: mission ordering and match boundaries
  ---------------------------------------------------------------------------
  strategy = {
    ordering_constraints = {
      { "M3", "before", "M4" },
    },
    start_node = "launch",
    end_node   = "launch",
  },
}

return missions
