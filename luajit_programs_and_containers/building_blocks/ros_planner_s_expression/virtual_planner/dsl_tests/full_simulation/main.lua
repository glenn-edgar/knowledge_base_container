-- main.lua -- Full system simulation test
--
-- Runs hub + remote ChainTree runtimes connected via channels.
-- Loads board, strategy, and remote model from config.lua.

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
local boards_dir  = os.getenv("VP_BOARDS") or "../../boards/"
local models_dir  = os.getenv("VP_MODELS") or "../../remote_models/"

-- Load remote model
local remote_template = dofile(models_dir .. config.remote_model)

-- Load board and strategy
local vn       = dofile(boards_dir .. config.board)
local strategy = dofile(boards_dir .. config.strategy)

-- Generate global plan
local plan = global_planner.plan(strategy, vn)
print(string.format("Global plan: %d virtual actions\n", #plan.actions))
global_planner.print_plan(plan)
print()

---------------------------------------------------------------------------
-- Build HUB runtime
---------------------------------------------------------------------------
local hub_tb = tree_builder.new("hub")
path_template.build_tree(hub_tb)
mission_template.build_tree(hub_tb)
local hub_data = hub_tb:build()
fn_registry.register_functions(hub_data, path_template.registry, mission_template.registry)
assert(fn_registry.validate(hub_data))

local hub_handle = ct_runtime.create({ delta_time = 0.1 }, hub_data)

---------------------------------------------------------------------------
-- Build REMOTE runtime
---------------------------------------------------------------------------
local remote_tb = tree_builder.new("remote")
remote_template.build_tree(remote_tb)
local remote_data = remote_tb:build()
fn_registry.register_functions(remote_data, remote_template.registry)
assert(fn_registry.validate(remote_data))

local remote_handle = ct_runtime.create({ delta_time = 0.1 }, remote_data)

-- Activate remote
engine.init_test(remote_handle, remote_template.kb_name)
remote_handle.active_tests[remote_template.kb_name] = true
remote_handle.active_test_count = 1

---------------------------------------------------------------------------
-- Channels and sequencer
---------------------------------------------------------------------------
local channels = channels_mod.new(hub_handle, remote_handle)
local template_map = {
  path    = path_template.kb_name,
  mission = mission_template.kb_name,
}
local seq = sequencer_mod.new(plan, hub_handle, template_map)

---------------------------------------------------------------------------
-- Helper: tick remote
---------------------------------------------------------------------------
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
-- Run
---------------------------------------------------------------------------
print("=== Running Full Simulation ===\n")
seq:start()

local max_ticks = 1000
local tick = 0
local prev_index = 0

while seq.status == "running" and tick < max_ticks do
  tick = tick + 1
  channels:tick()
  tick_remote()
  channels:tick()
  seq:tick()

  if seq.current_index ~= prev_index then
    local completed = plan.actions[prev_index]
    if completed then
      local label
      if completed.action_type == "path" then
        label = string.format("path %s -> %s (%d segs)",
          completed.from, completed.to,
          completed.segments and #completed.segments or 0)
      else
        label = string.format("mission %s at %s",
          completed.catalog_key, completed.board_node)
      end
      print(string.format("  tick %3d: completed [%s]", tick, label))
    end
    prev_index = seq.current_index
  end
end

print(string.format("\n--- Results ---"))
print(string.format("Status: %s", seq.status))
print(string.format("Ticks: %d", tick))
print(string.format("Actions: %d / %d",
  math.min(seq.current_index - 1, #plan.actions), #plan.actions))
channels:print_stats()

-- Write plan output
global_planner.write_json(plan, "global_plan.json")
global_planner.write_yaml(plan, "global_plan.yaml")

if seq.status == "complete" then
  print("\nPASSED")
else
  print("\nFAILED")
  os.exit(1)
end
