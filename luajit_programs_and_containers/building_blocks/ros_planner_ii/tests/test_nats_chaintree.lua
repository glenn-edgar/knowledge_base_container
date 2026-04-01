--[[
    test_nats_chaintree.lua -- Full stack: Hub ChainTree + Remote ChainTree over NATS.

    Features tested:
      - KeyStore-backed blackboard (observable state)
      - Per-KB bitmasks
      - Active KB tracking with elapsed time
      - Delta pose streaming → global coordinate translation
      - Event-driven queue monitors on both sides
]]

local ffi = require("ffi")
ffi.cdef[[
    int usleep(unsigned int usec);
    typedef void (*sighandler_t)(int);
    sighandler_t signal(int signum, sighandler_t handler);
]]
ffi.C.signal(13, ffi.cast("sighandler_t", 1))  -- ignore SIGPIPE

local json_util      = require("json_util")
local cmd_packets    = require("command_packets")
local event_ids      = require("event_ids")
local nats_transport = require("nats_transport")
local queue_monitor  = require("queue_monitor")
local ks_blackboard  = require("ks_blackboard")
local hub_control    = require("hub_control")

local ct_runtime    = require("ct_runtime")
local ct_loader     = require("ct_loader_pure")
local builtins      = require("ct_builtins")
local fn_registry   = require("fn_registry")
local defs          = require("ct_definitions")
local engine        = require("ct_engine")
local packet_mapper = require("packet_mapper")

local robot_id = os.getenv("ROBOT_ID") or "test_robot_1"
local server   = os.getenv("NATS_SERVER") or "nats://127.0.0.1:4222"

local script_dir  = debug.getinfo(1, "S").source:match("^@(.*/)")  or "./"
local root_dir    = script_dir .. "../"
local hub_json    = root_dir .. "hub_dsl/hub.json"

print("=== NATS ChainTree Full Test ===\n")
print(string.format("Robot: %s, Server: %s\n", robot_id, server))

---------------------------------------------------------------------------
-- NATS transport + KeyStore blackboard
---------------------------------------------------------------------------
local tx = nats_transport.hub_side(robot_id, server)
tx:flush()
print("Hub connected to NATS (queues flushed).")

-- KeyStore-backed blackboard
local bb = ks_blackboard.new(robot_id, server)
print("KeyStore blackboard connected.")

-- Give remote time to connect
ffi.C.usleep(1000000)

---------------------------------------------------------------------------
-- Load KB plugins
---------------------------------------------------------------------------
local hub_dsl_mod = require("hub_dsl")
local plugins = hub_dsl_mod.plugins

local kb_by_name = {}
for _, p in ipairs(plugins) do
    kb_by_name[p.name] = p
end

---------------------------------------------------------------------------
-- Load hub ChainTree
---------------------------------------------------------------------------
print("Loading hub: " .. hub_json)
local hub_data = ct_loader.load(hub_json)

fn_registry.register_functions(hub_data, builtins)

local mapper_fns = { one_shot = {} }
for _, plugin in ipairs(plugins) do
    local name, fn = packet_mapper.make_one_shot(plugin, json_util.decode)
    mapper_fns.one_shot[name] = fn
end
fn_registry.register_functions(hub_data, mapper_fns)

local planner_chain = require("planner_start_next_test")
planner_chain.set_plugins(plugins)
fn_registry.register_functions(hub_data, planner_chain.registry)

local event_handler_fns = require("event_handlers")
fn_registry.register_functions(hub_data, event_handler_fns.registry)

local error_recovery_fns = require("error_recovery")
fn_registry.register_functions(hub_data, error_recovery_fns.registry)

local hub_handle = ct_runtime.create({delta_time = 0.1, max_ticks = 50000}, hub_data)

-- Replace in-memory blackboard with KeyStore-backed one
hub_handle.blackboard = bb

-- Queue monitor
local monitor = queue_monitor.new({
    handle    = hub_handle,
    transport = tx,
    direction = "hub",
})

-- Set initial global pose
hub_control.set_global_pose({ x = 0, y = 0, z = 0, heading = 0, arm_angle = 0 })

print("")

---------------------------------------------------------------------------
-- KB management
---------------------------------------------------------------------------
local function activate_kb(kb_name)
    local kb = hub_handle.kb_table[kb_name]
    if not kb then return false end
    for _, nid in ipairs(kb.node_ids) do
        local n = hub_handle.nodes[nid]
        if n then
            n.ct_control.enabled = false
            n.ct_control.initialized = false
        end
        hub_handle.node_state[nid] = nil
    end
    bb.command_packet = nil
    bb.command_packet_size = 0
    bb.command_packet_json = nil

    -- Track active KB
    local plugin = kb_by_name[kb_name]
    hub_control.on_kb_start(bb, kb_name, plugin)

    engine.init_test(hub_handle, kb_name)
    hub_handle.active_tests[kb_name] = true
    hub_handle.active_test_count = (hub_handle.active_test_count or 0) + 1
    return true
end

local function kb_is_complete(kb_name)
    local kb = hub_handle.kb_table[kb_name]
    if not kb then return true end
    return not engine.node_is_enabled(hub_handle, kb.root_node)
end

local function deactivate_kb(kb_name)
    if hub_handle.active_tests[kb_name] then
        hub_handle.active_tests[kb_name] = nil
        hub_handle.active_test_count = hub_handle.active_test_count - 1
    end
    -- kb_done data was already applied by the event interceptor
    hub_control.on_kb_done(bb, kb_name, nil)
end

---------------------------------------------------------------------------
-- Tick hub
---------------------------------------------------------------------------
local function tick_hub()
    monitor:tick()
    hub_control.on_tick(bb)

    for kb_name, _ in pairs(hub_handle.active_tests) do
        local kb = hub_handle.kb_table[kb_name]
        if kb then
            table.insert(hub_handle.event_queue, {
                node_id  = kb.root_node,
                event_id = defs.CFL_TIMER_EVENT,
            })
        end
    end

    while #hub_handle.event_queue > 0 do
        local ev = table.remove(hub_handle.event_queue, 1)
        -- All event handling done inside ChainTree nodes
        -- (HUB_ACK_HANDLER, HUB_BITMAP_HANDLER, HUB_KB_DONE_HANDLER)
        engine.execute_event(hub_handle, ev.node_id,
            ev.event_id, ev.event_data, ev.event_type)
    end

    monitor:flush_outbound()
    bb.flush()
end

---------------------------------------------------------------------------
-- Test runner
---------------------------------------------------------------------------
local pass_count = 0
local fail_count = 0

local function check(name, condition, msg)
    if condition then
        pass_count = pass_count + 1
    else
        fail_count = fail_count + 1
        print(string.format("  FAIL: %s — %s", name, msg or ""))
    end
end

local function test_virtual_node(plugin, action_json)
    local kb_name = plugin.name

    bb.current_test_json = json_util.encode(action_json)

    local activated = activate_kb(kb_name)
    check(kb_name .. " activate", activated, "KB not found")
    if not activated then return end

    -- Verify active_kb tracking
    check(kb_name .. " active_kb", bb.active_kb == kb_name,
        "expected " .. kb_name .. ", got " .. tostring(bb.active_kb))

    local max_ticks = 500
    local completed = false
    for tick = 1, max_ticks do
        tick_hub()

        if kb_is_complete(kb_name) then
            completed = true
            deactivate_kb(kb_name)
            break
        end

        ffi.C.usleep(2000)
    end

    check(kb_name .. " complete", completed,
        "KB did not complete in " .. max_ticks .. " ticks")

    -- Verify success flag
    if completed then
        local success = bb.kb_done_success
        check(kb_name .. " success", success == true,
            "kb_done_success: " .. tostring(success))

        local pose = hub_control.get_global_pose()
        print(string.format("  PASS: %-18s  global: x=%.0f y=%.0f heading=%.0f arm=%.0f",
            kb_name, pose.x, pose.y, pose.heading, pose.arm_angle))
    end
end

---------------------------------------------------------------------------
-- Test each virtual node
---------------------------------------------------------------------------

test_virtual_node(kb_by_name["init_check"], {
    test_id = 1, next_test = 2,
})

test_virtual_node(kb_by_name["path_spline"], {
    test_id = 2, next_test = 3,
    from_x = 0, from_y = 0, to_x = 800, to_y = 0,
    speed = 150, distance = 800, segment_index = 0, total_segments = 1,
    nav_method = 0,
})

test_virtual_node(kb_by_name["path_line"], {
    test_id = 3, next_test = 4,
    from_x = 800, from_y = 0, to_x = 1600, to_y = 0,
    speed = 120, distance = 800, segment_index = 0, total_segments = 1,
    nav_method = 1,
})

test_virtual_node(kb_by_name["path_wall"], {
    test_id = 4, next_test = 5,
    from_x = 1600, from_y = 0, to_x = 1600, to_y = 400,
    speed = 100, distance = 400, segment_index = 0, total_segments = 1,
    nav_method = 2,
})

test_virtual_node(kb_by_name["path_rotate"], {
    test_id = 5, next_test = 6,
    from_heading = 0, to_heading = 90,
})

test_virtual_node(kb_by_name["deliver_part"], {
    test_id = 6, next_test = 7,
    params = { arm_target = -45, arm_speed = 80, arm_return = 0, payload = "part" },
})

test_virtual_node(kb_by_name["paint_sample"], {
    test_id = 7, next_test = 8,
    params = { arm_target = -60, arm_speed = 60, arm_return = 0 },
})

test_virtual_node(kb_by_name["load_shipping"], {
    test_id = 8, next_test = 9,
    params = { arm_target = -30, arm_speed = 80, arm_return = 0, payload = "container" },
})

test_virtual_node(kb_by_name["pass_gate"], {
    test_id = 9, next_test = 10,
    params = { rpc_hash = 12345, drive_through = 200 },
})

test_virtual_node(kb_by_name["inspection_scan"], {
    test_id = 10, next_test = 11,
    params = { sensor_port = 0, sensor_type = "color" },
})

test_virtual_node(kb_by_name["idle"], {
    test_id = 11, next_test = 0,
})

---------------------------------------------------------------------------
-- Shutdown
---------------------------------------------------------------------------
tx:send_rpc(json_util.encode({
    packet_type = cmd_packets.TYPE_SHUTDOWN,
    seq = 99,
}))
ffi.C.usleep(500000)
tx:close()
bb.close()

---------------------------------------------------------------------------
-- Results
---------------------------------------------------------------------------
print(string.format("\n--- Results ---"))
print(string.format("Passed: %d", pass_count))
print(string.format("Failed: %d", fail_count))

local final_pose = hub_control.get_global_pose()
print(string.format("Final global pose: x=%.0f y=%.0f heading=%.0f arm=%.0f",
    final_pose.x, final_pose.y, final_pose.heading, final_pose.arm_angle))

if fail_count == 0 then
    print("\nPASSED")
else
    print("\nFAILED")
    os.exit(1)
end
