--[[
    hub_vm.lua -- Hub thread entry point.

    Runs in its own LuaJIT VM (pthread). Responsibilities:
      - Load hub ChainTree JSON IR
      - Register generic packet mapper one-shots from KB plugins
      - Tick hub runtime
      - Bridge: bb.command_packet → RPC socket (send binary packet)
      - Bridge: stream socket → bb bitmap fields (receive updates)
      - Report action completion to main via stdout protocol

    Globals set by C launcher:
      _VMRT_RPC_FD     — socket fd for RPC channel (hub writes)
      _VMRT_STREAM_FD  — socket fd for stream channel (hub reads)
      _VMRT_HUB        — hub control struct pointer

    Environment:
      VMRT_HUB_JSON    — path to compiled hub.json
]]

local ffi = require("ffi")

ffi.cdef[[
    int usleep(unsigned int usec);
    typedef struct { int fd; } vmrt_socket_t;
    int     vmrt_socket_send(vmrt_socket_t *sock, const void *data, uint32_t len);
    int32_t vmrt_socket_recv(vmrt_socket_t *sock, void *buf, uint32_t buf_size);
    int32_t vmrt_socket_recv_nb(vmrt_socket_t *sock, void *buf, uint32_t buf_size);
    void    vmrt_socket_close(vmrt_socket_t *sock);

    typedef struct {
        vmrt_socket_t rpc_fd;
        vmrt_socket_t stream_fd;
        char          script_path[512];
        char          lua_path[2048];
        volatile int  running;
        volatile int  ready;
        volatile int  exited;
        int           exit_code;
        char          error_msg[256];
    } vmrt_hub_t;
]]

local script_dir = debug.getinfo(1, "S").source:match("^@(.*/)")  or "./"
local lib = ffi.load(script_dir .. "libvmrt.so")

-- Get socket fds and hub control from C launcher
local rpc_sock    = ffi.new("vmrt_socket_t", { fd = _VMRT_RPC_FD })
local stream_sock = ffi.new("vmrt_socket_t", { fd = _VMRT_STREAM_FD })
local hub_ctrl    = ffi.cast("vmrt_hub_t *", _VMRT_HUB)

---------------------------------------------------------------------------
-- Load ChainTree runtime
---------------------------------------------------------------------------
local ct_runtime  = require("ct_runtime")
local ct_loader   = require("ct_loader_pure")
local builtins    = require("ct_builtins")
local fn_registry = require("fn_registry")
local defs        = require("ct_definitions")
local engine      = require("ct_engine")
local json_util   = require("json_util")

-- Load KB plugins and packet mapper
local packet_mapper = require("packet_mapper")
local hub_dsl_mod   = require("hub_dsl")
local plugins       = hub_dsl_mod.plugins

-- Load chaining function
local planner_chain = require("planner_start_next_test")
planner_chain.set_plugins(plugins)

---------------------------------------------------------------------------
-- Load hub tree
---------------------------------------------------------------------------
local hub_json = os.getenv("VMRT_HUB_JSON") or ""
if hub_json == "" then
    hub_ctrl.exit_code = 1
    return
end

local hub_data = ct_loader.load(hub_json)

-- Register builtins + chaining
fn_registry.register_functions(hub_data, builtins, planner_chain.registry)

-- Register generic packet mapper one-shots from plugins
local mapper_fns = { one_shot = {} }
for _, plugin in ipairs(plugins) do
    local name, fn = packet_mapper.make_one_shot(plugin, json_util.decode)
    mapper_fns.one_shot[name] = fn
end
fn_registry.register_functions(hub_data, mapper_fns)

local hub_handle = ct_runtime.create({delta_time = 0.1, max_ticks = 50000}, hub_data)

---------------------------------------------------------------------------
-- KB index map
---------------------------------------------------------------------------
local kb_by_index = {}
for _, p in ipairs(plugins) do
    kb_by_index[p.index] = p.name
end

---------------------------------------------------------------------------
-- Tick hub
---------------------------------------------------------------------------
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
-- Bridge: bb.command_packet → RPC socket
---------------------------------------------------------------------------
local function bridge_commands()
    local bb = hub_handle.blackboard
    local pkt = bb.command_packet
    local size = bb.command_packet_size or 0
    if pkt ~= nil and pkt ~= 0 and size > 0 then
        lib.vmrt_socket_send(rpc_sock, pkt, size)
        bb.command_packet = nil
        bb.command_packet_size = 0
    end
end

---------------------------------------------------------------------------
-- Bridge: stream socket → bb bitmap fields
---------------------------------------------------------------------------
local STREAM_BUF_SIZE = 4096
local stream_buf = ffi.new("uint8_t[?]", STREAM_BUF_SIZE)

local stream_packets = require("stream_packets")

local function bridge_streams()
    while true do
        local len = lib.vmrt_socket_recv_nb(stream_sock, stream_buf, STREAM_BUF_SIZE)
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
        elseif hdr.packet_type == stream_packets.TYPE_ODOMETRY then
            local pkt = ffi.cast("stream_odometry_t*", stream_buf)
            bb.local_distance_x = pkt.distance_x
            bb.local_distance_y = pkt.distance_y
            bb.current_heading  = pkt.heading
            bb.current_speed    = pkt.speed
        elseif hdr.packet_type == stream_packets.TYPE_ACK then
            -- Ack received — KB can check this
            local ack = stream_packets.read_ack(stream_buf, len)
            if ack then
                bb.last_ack_seq    = ack.ack_seq
                bb.last_ack_status = ack.status
            end
        end
    end
end

---------------------------------------------------------------------------
-- KB management
---------------------------------------------------------------------------
local current_action_idx = 0
local active_kb = nil

local function activate_kb(test_id)
    local kb_name = kb_by_index[test_id]
    if not kb_name then return false end
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

    engine.init_test(hub_handle, kb_name)
    hub_handle.active_tests[kb_name] = true
    hub_handle.active_test_count = (hub_handle.active_test_count or 0) + 1
    active_kb = kb_name
    return true
end

local function check_kb_complete()
    if not active_kb then return false end
    local kb = hub_handle.kb_table[active_kb]
    if not kb then return false end
    return not engine.node_is_enabled(hub_handle, kb.root_node)
end

local function deactivate_kb()
    if not active_kb then return end
    if hub_handle.active_tests[active_kb] then
        hub_handle.active_tests[active_kb] = nil
        hub_handle.active_test_count = hub_handle.active_test_count - 1
    end
    active_kb = nil
end

---------------------------------------------------------------------------
-- Main↔Hub protocol: read JSON commands from stdin pipe
-- (For step 1, the test harness writes to hub's blackboard directly
--  via environment. The hub reads current_test_json at KB activation.)
---------------------------------------------------------------------------

-- Signal ready
hub_ctrl.ready = 1

---------------------------------------------------------------------------
-- Main loop
---------------------------------------------------------------------------
while hub_ctrl.running ~= 0 do
    -- Bridge streams from remote
    bridge_streams()

    -- Tick hub ChainTree
    tick_hub()

    -- Bridge commands to remote
    bridge_commands()

    -- Check if current KB completed
    if active_kb and check_kb_complete() then
        -- Write completion marker to stdout for test harness
        io.write(string.format("COMPLETE:%d:%s\n", current_action_idx, active_kb))
        io.flush()
        deactivate_kb()
    end

    ffi.C.usleep(100)
end

-- Send shutdown to remote
local cmd_pkts = require("command_packets")
local shutdown_pkt, shutdown_size = cmd_pkts.make_shutdown()
lib.vmrt_socket_send(rpc_sock, shutdown_pkt, shutdown_size)
