-- mission_dsl.lua -- Match strategy
--
-- Defines which catalog entries to execute and in what order.
-- The catalog (in virtual_node_dsl.lua) defines what each mission does.
-- This file defines the match-level strategy: which missions, time limit,
-- and supervisor configuration.

local strategy = {
  time_limit = 150,  -- seconds (2.5 min match)

  -- Which catalog entries to execute (order is advisory --
  -- the planner respects preconditions from the catalog)
  missions = {
    "deliver_part",
    "paint_sample",
    "pass_gate",
    "load_shipping",
    "inspection_scan",
  },

  start_node = "launch",
  end_node   = "launch",

  -- Match-level supervisor
  supervisor = {
    strategy      = "one_for_one",
    max_failures  = 3,
    reset_window  = 30000,
    on_all_failed = "return_home",
  },
}

return strategy
