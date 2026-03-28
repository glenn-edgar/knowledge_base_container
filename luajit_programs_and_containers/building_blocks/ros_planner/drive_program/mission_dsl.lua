-- mission_dsl.lua — Missions defined as virtual node sequences
--
-- Each mission is a sequence of virtual nodes to visit.
-- The global planner chains Dijkstra segments between them.
-- The local planner compiles each edge into hidden node patterns.
-- The local tracker monitors execution via scan tree bitmaps.
--
-- Exception handling:
--   Hub side  — monitors bitmaps, triggers replan on failure
--   Robot side — final handler halts, waits for new download
--
-- Supervisor: Erlang one-for-one with leaky-bucket rate limiting.

missions = {
  time_limit = 150,  -- seconds (2.5 min match)

  ---------------------------------------------------------------------------
  -- Match-level supervisor
  ---------------------------------------------------------------------------
  supervisor = {
    strategy      = "one_for_one",
    max_failures  = 3,
    reset_window  = 30000,
    on_all_failed = "return_home",
  },

  ---------------------------------------------------------------------------
  -- M1: Deliver Part
  ---------------------------------------------------------------------------
  -- Drive to assembly, lower arm to deliver, raise arm, done.
  M1 = {
    name   = "deliver_part",
    points = 15,
    visit  = { "assembly", "arm_deliver", "arm_home" },
    preconditions = {},
  },

  ---------------------------------------------------------------------------
  -- M2: Paint Sample
  ---------------------------------------------------------------------------
  -- Drive to painting, lower arm (apply paint), raise arm, done.
  M2 = {
    name   = "paint_sample",
    points = 20,
    visit  = { "painting", "arm_paint_down", "arm_paint_up" },
    preconditions = {},
  },

  ---------------------------------------------------------------------------
  -- M3: Pass Gate (uses RPC to hub)
  ---------------------------------------------------------------------------
  -- Drive to gate_zone. Gate open/close is handled by hub RPC —
  -- the local planner inserts RPC hidden nodes around the path segment.
  M3 = {
    name   = "pass_gate",
    points = 25,
    visit  = { "gate_zone" },
    rpc    = { "gate.open", "gate.close" },
    preconditions = {},
  },

  ---------------------------------------------------------------------------
  -- M4: Load Shipping
  ---------------------------------------------------------------------------
  -- Must pass gate first. Drive to shipping, lower arm to load, raise arm.
  M4 = {
    name   = "load_shipping",
    points = 20,
    visit  = { "shipping", "arm_load", "arm_home" },
    preconditions = { "M3" },
  },

  ---------------------------------------------------------------------------
  -- M5: Inspection Scan
  ---------------------------------------------------------------------------
  -- Drive to inspection. Sensor read is scan tree input, not an action.
  -- The hub reads the color bitmap from the streaming channel.
  M5 = {
    name   = "inspection_scan",
    points = 20,
    visit  = { "inspection" },
    preconditions = {},
  },

  ---------------------------------------------------------------------------
  -- Strategy
  ---------------------------------------------------------------------------
  strategy = {
    ordering_constraints = {
      { "M3", "before", "M4" },
    },
    start_node  = "launch",
    end_node    = "launch",
    use_greedy  = false,
  },
}

return missions
