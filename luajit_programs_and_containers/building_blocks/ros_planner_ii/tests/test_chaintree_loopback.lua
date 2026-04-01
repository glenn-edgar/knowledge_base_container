--[[
    test_chaintree_loopback.lua -- Step 1: Hub ChainTree + remote loopback.

    Spawns:
      - Hub thread with real ChainTree runtime (loads hub.json, runs KBs)
      - Remote loopback process (acks packets, no ChainTree)

    For each virtual node type:
      1. Test stages current_test_json on hub blackboard
      2. Hub KB fires: one-shot reads JSON, generic mapper creates FFI packet
      3. Hub bridge sends packet over RPC socket to remote
      4. Remote loopback acks + sends bitmap over stream socket
      5. Hub bridge reads stream, writes bitmap to blackboard
      6. Hub KB completes (root disables)
      7. Test reads COMPLETE from hub

    Proves: DSL → ChainTree → generic mapper → binary packet → wire → ack
]]

local ffi = require("ffi")
local vmrt           = require("vmrt_ffi")
local json_util      = require("json_util")
local cmd_packets    = require("command_packets")
local stream_packets = require("stream_packets")

-- ChainTree runtime (for direct hub handle access in test harness)
local ct_runtime  = require("ct_runtime")
local ct_loader   = require("ct_loader_pure")
local builtins    = require("ct_builtins")
local fn_registry = require("fn_registry")
local defs        = require("ct_definitions")
local engine      = require("ct_engine")
local packet_mapper = require("packet_mapper")

ffi.cdef[[ int usleep(unsigned int usec); ]]

print("=== ChainTree Loopback Test (Step 1) ===\n")

---------------------------------------------------------------------------
-- Paths
---------------------------------------------------------------------------
local script_dir  = debug.getinfo(1, "S").source:match("^@(.*/)")  or "./"
local root_dir    = script_dir .. "../"
local runtime_dir = root_dir .. "runtime/"
local hub_dsl_dir = root_dir .. "hub_dsl/"
local robot_dir   = root_dir .. "robots/test_robot/"

local hub_json      = hub_dsl_dir .. "hub.json"
local remote_script = robot_dir .. "remote_loopback.lua"

-- LUA_PATH for remote process
local remote_lua_path = table.concat({
    root_dir .. "hub_dsl/protocol/?.lua",
    runtime_dir .. "?.lua",
    "?.lua", "",
}, ";")

---------------------------------------------------------------------------
-- Load KB plugins
---------------------------------------------------------------------------
local hub_dsl_mod = require("hub_dsl")
local plugins = hub_dsl_mod.plugins

local kb_by_index = {}
local kb_by_name = {}
for _, p in ipairs(plugins) do
    kb_by_index[p.index] = p
    kb_by_name[p.name] = p
end

---------------------------------------------------------------------------
-- Load hub ChainTree (in test process, not in a thread)
-- For step 1, we run the hub inline to have direct blackboard access.
---------------------------------------------------------------------------
print("Loading hub: " .. hub_json)
local hub_data = ct_loader.load(hub_json)

-- Register builtins
fn_registry.register_functions(hub_data, builtins)

-- Register generic mapper one-shots
local mapper_fns = { one_shot = {} }
for _, plugin in ipairs(plugins) do
    local name, fn = packet_mapper.make_one_shot(plugin, json_util.decode)
    mapper_fns.one_shot[name] = fn
end
fn_registry.register_functions(hub_data, mapper_fns)

-- Register chaining
local planner_chain = require("planner_start_next_test")
planner_chain.set_plugins(plugins)
fn_registry.register_functions(hub_data, planner_chain.registry)

local event_handler_fns = require("event_handlers")
fn_registry.register_functions(hub_data, event_handler_fns.registry)

local error_recovery_fns = require("error_recovery")
fn_registry.register_functions(hub_data, error_recovery_fns.registry)

local hub_handle = ct_runtime.create({delta_time = 0.1, max_ticks = 50000}, hub_data)

---------------------------------------------------------------------------
-- Create channel pair and spawn remote process
---------------------------------------------------------------------------
local cp = vmrt.channel_pair_create()
local remote = vmrt.remote_spawn(remote_script, remote_lua_path, cp)
ffi.C.usleep(100000)  -- let remote start

local rpc_sock    = cp.rpc_hub
local stream_sock = cp.stream_hub

print("Remote process spawned.\n")

---------------------------------------------------------------------------
-- Bridge helpers
---------------------------------------------------------------------------
local STREAM_BUF_SIZE = 4096
local stream_buf = ffi.new("uint8_t[?]", STREAM_BUF_SIZE)

local function bridge_commands()
    local bb = hub_handle.blackboard
    local pkt = bb.command_packet
    local size = bb.command_packet_size or 0
    if pkt ~= nil and pkt ~= 0 and size > 0 then
        vmrt.lib.vmrt_socket_send(rpc_sock, pkt, size)
        bb.command_packet = nil
        bb.command_packet_size = 0
    end
end

local function bridge_streams()
    while true do
        local len = vmrt.lib.vmrt_socket_recv_nb(stream_sock, stream_buf, STREAM_BUF_SIZE)
        if not len or len <= 0 then break end

        local hdr = stream_packets.read_header(stream_buf, len)
        if not hdr then break end

        local bb = hub_handle.blackboard
        if hdr.packet_type == stream_packets.TYPE_BITMAP then
            local pkt = ffi.cast("stream_bitmap_t*", stream_buf)
            if pkt.seg_complete ~= 0    then bb.bitmap_seg_complete    = true end
            if pkt.action_complete ~= 0 then bb.bitmap_action_complete = true end
        elseif hdr.packet_type == stream_packets.TYPE_ACK then
            local ack = stream_packets.read_ack(stream_buf, len)
            if ack then
                bb.last_ack_seq    = ack.ack_seq
                bb.last_ack_status = ack.status
            end
        end
    end
end

local function tick_hub()
    for kb_name, _ in pairs(hub_handle.active_tests) do
        local kb = hub_handle.kb_table[kb_name]
        if kb then
            table.insert(hub_handle.event_queue, {
                node_id = kb.root_node, event_id = defs.CFL_TIMER_EVENT,
            })
        end
    end
    while #hub_handle.event_queue > 0 do
        local ev = table.remove(hub_handle.event_queue, 1)
        engine.execute_event(hub_handle, ev.node_id,
            ev.event_id, ev.event_data, ev.event_type)
    end
end

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
    -- Clear bitmap state
    hub_handle.blackboard.bitmap_seg_complete    = false
    hub_handle.blackboard.bitmap_action_complete = false
    hub_handle.blackboard.command_packet = nil
    hub_handle.blackboard.command_packet_size = 0

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
    local test_name = kb_name

    -- Stage JSON on blackboard
    hub_handle.blackboard.current_test_json = json_util.encode(action_json)

    -- Activate KB
    local activated = activate_kb(kb_name)
    check(test_name .. " activate", activated, "KB not found")
    if not activated then return end

    -- Run tick loop: tick hub, bridge, wait for KB to complete
    local max_ticks = 100
    local completed = false
    for tick = 1, max_ticks do
        tick_hub()
        bridge_commands()
        ffi.C.usleep(1000)  -- give remote time
        bridge_streams()

        if kb_is_complete(kb_name) then
            completed = true
            deactivate_kb(kb_name)
            break
        end
    end

    check(test_name .. " complete", completed, "KB did not complete in " .. max_ticks .. " ticks")

    -- Verify we got an ack
    local bb = hub_handle.blackboard
    check(test_name .. " ack", bb.last_ack_status == stream_packets.ACK_OK,
        "ack status: " .. tostring(bb.last_ack_status))

    if completed then
        print(string.format("  PASS: %s", test_name))
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
    from_x = 0, from_y = 800, to_x = 0, to_y = 400,
    speed = 120, distance = 400, segment_index = 0, total_segments = 1,
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
-- Shutdown remote
---------------------------------------------------------------------------
local shutdown_pkt, shutdown_size = cmd_packets.make_shutdown()
vmrt.lib.vmrt_socket_send(rpc_sock, shutdown_pkt, shutdown_size)
ffi.C.usleep(50000)

local exit_status = vmrt.remote_wait(remote)
check("remote exit", exit_status == 0, "exit status " .. tostring(exit_status))

vmrt.channel_pair_close(cp)
vmrt.cleanup()

---------------------------------------------------------------------------
-- Results
---------------------------------------------------------------------------
print(string.format("\n--- Results ---"))
print(string.format("Passed: %d", pass_count))
print(string.format("Failed: %d", fail_count))

if fail_count == 0 then
    print("\nPASSED")
else
    print("\nFAILED")
    os.exit(1)
end
