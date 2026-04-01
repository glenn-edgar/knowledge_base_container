-- main.lua -- Obstacle recovery test
--
-- Single path with obstacle injection at a specific tick.

local ct_runtime       = require("ct_runtime")
local tree_builder     = require("tree_builder")
local fn_registry      = require("fn_registry")
local path_template    = require("path_template")
local mission_template = require("mission_template")
local sequencer_mod    = require("sequencer")
local channels_mod     = require("channels")
local global_planner   = require("global_planner")
local defs             = require("ct_definitions")
local engine           = require("ct_engine")

-- Load config
local config = dofile("config.lua")
local boards_dir = os.getenv("VP_BOARDS") or "../../boards/"
local models_dir = os.getenv("VP_MODELS") or "../../remote_models/"
local remote_template = dofile(models_dir .. config.remote_model)

-- Build hub
local hub_tb = tree_builder.new("hub")
path_template.build_tree(hub_tb)
mission_template.build_tree(hub_tb)
local hub_data = hub_tb:build()
fn_registry.register_functions(hub_data, path_template.registry, mission_template.registry)
assert(fn_registry.validate(hub_data))
local hub_handle = ct_runtime.create({ delta_time = 0.1 }, hub_data)

-- Build remote
local remote_tb = tree_builder.new("remote")
remote_template.build_tree(remote_tb)
local remote_data = remote_tb:build()
fn_registry.register_functions(remote_data, remote_template.registry)
assert(fn_registry.validate(remote_data))
local remote_handle = ct_runtime.create({ delta_time = 0.1 }, remote_data)

engine.init_test(remote_handle, remote_template.kb_name)
remote_handle.active_tests[remote_template.kb_name] = true
remote_handle.active_test_count = 1

-- Channels
local channels = channels_mod.new(hub_handle, remote_handle)

-- Single path plan with 2 segments
local plan = {
  actions = {
    {
      action_type    = "path",
      from           = "launch",
      to             = "assembly",
      nav_method     = "spline_follow",
      speed          = 150,
      max_distance   = 2000,
      total_distance = 1600,
      step           = 1,
      segments       = {
        { from = "launch", to = "gate_zone", nav = "spline_follow",
          speed = 150, distance = 800 },
        { from = "gate_zone", to = "assembly", nav = "spline_follow",
          speed = 150, distance = 800 },
      },
    },
  },
  mission_order = {},
  total_cost = 1600,
  virtual_route = { "launch", "assembly" },
}

local template_map = {
  path    = path_template.kb_name,
  mission = mission_template.kb_name,
}
local seq = sequencer_mod.new(plan, hub_handle, template_map)

-- Helper
local function tick_remote()
  local kb = remote_handle.kb_table[remote_template.kb_name]
  if kb then
    table.insert(remote_handle.event_queue, {
      node_id  = kb.root_node,
      event_id = defs.CFL_TIMER_EVENT,
    })
  end
  while #remote_handle.event_queue > 0 do
    local event = table.remove(remote_handle.event_queue, 1)
    engine.execute_event(remote_handle, event.node_id,
      event.event_id, event.event_data, event.event_type)
  end
end

---------------------------------------------------------------------------
-- Run with obstacle at tick 6
---------------------------------------------------------------------------
print("=== Obstacle Recovery Test ===\n")
seq:start()

local obstacle_tick = 6
local tick = 0

while seq.status == "running" and tick < 100 do
  tick = tick + 1

  if tick == obstacle_tick then
    remote_handle.blackboard.sim_obstacle = true
    print(string.format("  tick %2d: INJECTING OBSTACLE", tick))
  end

  channels:tick()
  tick_remote()
  channels:tick()
  seq:tick()

  local rbb = remote_handle.blackboard
  if rbb.last_action_started then
    print(string.format("  tick %2d: remote started [%s]", tick, rbb.last_action_started))
    rbb.last_action_started = nil
  end
  if rbb.last_action_completed then
    print(string.format("  tick %2d: remote completed [%s]", tick, rbb.last_action_completed))
    rbb.last_action_completed = nil
  end
end

print(string.format("\n--- Results ---"))
print(string.format("Status: %s", seq.status))
print(string.format("Ticks: %d", tick))
channels:print_stats()

if seq.status == "complete" then
  print("\nPASSED")
else
  print("\nFAILED")
  os.exit(1)
end
