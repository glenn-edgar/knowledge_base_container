--[[
    test_chaintree_full.lua -- Step 2: Hub ChainTree + Remote ChainTree.

    Both sides run real ChainTree runtimes communicating over sockets.

    Hub: loads hub.json, KB one-shots generate AVRC packets via schema mapper
    Remote: loads remote.json, receiver/executor/streamer columns process packets

    For each virtual node:
      1. Stage current_test_json on hub blackboard
      2. Hub KB fires one-shot → generic mapper → FFI packet → RPC socket
      3. Remote ChainTree receiver reads packet → executor simulates → streamer sends ack+bitmap
      4. Hub bridge reads stream → bitmap on blackboard → KB completes
]]

local ffi = require("ffi")
local vmrt           = require("vmrt_ffi")
local json_util      = require("json_util")
local cmd_packets    = require("command_packets")
local stream_packets = require("stream_packets")

local ct_runtime  = require("ct_runtime")
local ct_loader   = require("ct_loader_pure")
local builtins    = require("ct_builtins")
local fn_registry = require("fn_registry")
local defs        = require("ct_definitions")
local engine      = require("ct_engine")
local packet_mapper = require("packet_mapper")

ffi.cdef[[
    int usleep(unsigned int usec);
    typedef void (*sighandler_t)(int);
    sighandler_t signal(int signum, sighandler_t handler);
]]
-- Ignore SIGPIPE (broken pipe kills process silently otherwise)
ffi.C.signal(13, ffi.cast("sighandler_t", 1))  -- SIG_IGN = 1, SIGPIPE = 13

print("=== ChainTree Full Test (Step 2) ===\n")

---------------------------------------------------------------------------
-- Paths
---------------------------------------------------------------------------
local script_dir  = debug.getinfo(1, "S").source:match("^@(.*/)")  or "./"
local root_dir    = script_dir .. "../"
local runtime_dir = root_dir .. "runtime/"
local hub_dsl_dir = root_dir .. "hub_dsl/"
local robot_dir   = root_dir .. "robots/test_robot/"

local hub_json       = hub_dsl_dir .. "hub.json"
local remote_json    = robot_dir .. "remote.json"
local remote_script  = robot_dir .. "remote_chaintree.lua"

-- LUA_PATH for remote process (needs protocol, runtime, robot, CT runtime)
local ct_base = root_dir .. "../chain_tree_luajit/"
local remote_lua_path = table.concat({
    root_dir .. "hub_dsl/protocol/?.lua",
    runtime_dir .. "?.lua",
    robot_dir .. "?.lua",
    ct_base .. "runtime_dict/?.lua",
    ct_base .. "lua_dsl/luajit_pipeline/?.lua",
    "?.lua", "",
}, ";")

---------------------------------------------------------------------------
-- Create channels and spawn remote FIRST (before loading hub ChainTree)
-- This ensures socketpair fds are not corrupted by ChainTree file loading
---------------------------------------------------------------------------
ffi.cdef[[ int setenv(const char *name, const char *value, int overwrite); ]]
ffi.C.setenv("VMRT_REMOTE_JSON", remote_json, 1)

local cp = vmrt.channel_pair_create()
local remote = vmrt.remote_spawn(remote_script, remote_lua_path, cp)
ffi.C.usleep(200000)  -- 200ms for remote ChainTree to load + init

-- Capture socket fds AFTER spawn (parent's ends are still valid)
local rpc_sock    = ffi.new("vmrt_socket_t", { fd = cp.rpc_hub.fd })
local stream_sock = ffi.new("vmrt_socket_t", { fd = cp.stream_hub.fd })

print("Remote ChainTree process spawned (pid=" .. tostring(remote.pid) .. ").")
print(string.format("  Hub sockets: rpc_fd=%d stream_fd=%d", rpc_sock.fd, stream_sock.fd))

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

local wait_fns = require("wait_for_response")
fn_registry.register_functions(hub_data, wait_fns.registry)

local hub_handle = ct_runtime.create({delta_time = 0.1, max_ticks = 50000}, hub_data)

print("")

-- Check if remote is still alive
ffi.cdef[[ int kill(int pid, int sig); ]]
local alive = ffi.C.kill(remote.pid, 0)
io.write(string.format("  DEBUG remote alive check: kill(0) returned %d\n", alive))
io.flush()

---------------------------------------------------------------------------
-- Bridge helpers (same as step 1)
---------------------------------------------------------------------------
local STREAM_BUF_SIZE = 4096
local stream_buf = ffi.new("uint8_t[?]", STREAM_BUF_SIZE)

local bridge_send_count = 0
local bridge_debug_count = 0
local function bridge_commands()
    local bb = hub_handle.blackboard
    local size = bb.command_packet_size
    local pkt = bb.command_packet

    -- Debug first few calls
    bridge_debug_count = bridge_debug_count + 1
    if bridge_debug_count <= 5 then
        io.write(string.format("  DEBUG bridge check: size=%s pkt_type=%s pkt=%s rpc_fd=%d stream_fd=%d\n",
            tostring(size), type(pkt), tostring(pkt),
            rpc_sock.fd, stream_sock.fd))
        io.flush()
    end

    if size and size > 0 and pkt then
        -- Use a temporary socket struct with the fd to ensure proper pointer passing
        local sock = ffi.new("vmrt_socket_t", { fd = rpc_sock.fd })
        io.write(string.format("  DEBUG about to send: fd=%d size=%d\n", sock.fd, size))
        io.flush()
        local rc = vmrt.lib.vmrt_socket_send(sock, pkt, size)
        bridge_send_count = bridge_send_count + 1
        io.write(string.format("  DEBUG bridge_send #%d: size=%d rc=%d\n",
            bridge_send_count, size, rc))
        io.flush()
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
            if pkt.obstacle ~= 0        then bb.bitmap_obstacle        = true end
            if pkt.motor_fault ~= 0     then bb.bitmap_motor_fault     = true end
            if pkt.action_fault ~= 0    then bb.bitmap_action_fault    = true end
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

    hub_handle.blackboard.current_test_json = json_util.encode(action_json)
    hub_handle.blackboard.last_ack_seq = -1
    hub_handle.blackboard.last_ack_status = -1

    local activated = activate_kb(kb_name)
    check(kb_name .. " activate", activated, "KB not found")
    if not activated then return end

    -- Tick loop: hub ticks, bridge, wait for remote to process
    local max_ticks = 200
    local completed = false
    for tick = 1, max_ticks do
        tick_hub()

        -- Debug: check if packet was generated
        local bb = hub_handle.blackboard
        if tick == 1 then
            local has_pkt = bb.command_packet ~= nil and bb.command_packet ~= 0
            local pkt_size = bb.command_packet_size or 0
            if not has_pkt and pkt_size == 0 then
                io.write(string.format("  DEBUG %s tick %d: no packet generated\n", kb_name, tick))
            else
                io.write(string.format("  DEBUG %s tick %d: packet size=%d\n", kb_name, tick, pkt_size))
            end
            io.flush()
        end

        bridge_commands()
        ffi.C.usleep(2000)  -- give remote time to tick
        bridge_streams()

        -- Debug: check stream state on early ticks
        if tick <= 5 then
            local bb2 = hub_handle.blackboard
            if bb2.last_ack_seq and bb2.last_ack_seq >= 0 then
                io.write(string.format("  DEBUG %s tick %d: ack_seq=%s ack_status=%s action_complete=%s seg_complete=%s\n",
                    kb_name, tick,
                    tostring(bb2.last_ack_seq), tostring(bb2.last_ack_status),
                    tostring(bb2.bitmap_action_complete), tostring(bb2.bitmap_seg_complete)))
                io.flush()
            end
        end

        if kb_is_complete(kb_name) then
            completed = true
            deactivate_kb(kb_name)
            break
        end
    end

    check(kb_name .. " complete", completed,
        "KB did not complete in " .. max_ticks .. " ticks")

    local bb = hub_handle.blackboard
    check(kb_name .. " ack", bb.last_ack_status == stream_packets.ACK_OK,
        "ack status: " .. tostring(bb.last_ack_status))

    if completed then
        print(string.format("  PASS: %s", kb_name))
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
ffi.C.usleep(100000)

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
