--[[
    remote_vm.lua -- Remote (robot) thread entry point for multi-VM runtime.

    Runs in its own LuaJIT VM (pthread). Responsibilities:
      - Load remote ChainTree JSON IR
      - Tick remote runtime independently
      - Receive RPC commands from hub via transport pipe
      - Send streaming bitmap/sensor data back to hub via transport pipe

    The transport pipe is generic — same API for ringbuffer (simulation)
    or serial/network (real hardware). When real hardware is introduced,
    this file stays the same; only the transport backend changes.

    Communication protocol (pipe messages are JSON strings):
      Hub → Remote (RPC):
        {"type":"path_segment","speed":150,...}   -- motor command
        {"type":"arm_command","target":-45,...}    -- arm command
        {"type":"rpc_request","rpc":"gate.open"}  -- RPC call
        {"type":"shutdown"}                       -- stop thread

      Remote → Hub (Stream):
        {"seg_complete":true}
        {"action_complete":true}
        {"obstacle":true}
        {"motor_fault":true}
        {"action_fault":true}

    Globals set by C launcher:
      _VMRT_PIPE  — pointer to vmrt_pipe_t (hub↔remote pipe)
      _VMRT_HUB   — pointer to vmrt_hub_t (thread control)
]]

local ffi = require("ffi")

ffi.cdef[[
    int usleep(unsigned int usec);

    typedef struct {
        uint8_t  *buf;
        uint32_t  capacity;
        uint32_t  head;
        uint32_t  tail;
    } vmrt_ringbuf_t;

    typedef struct {
        vmrt_ringbuf_t *to_hub;
        vmrt_ringbuf_t *from_hub;
    } vmrt_pipe_t;

    typedef struct {
        vmrt_pipe_t *pipe;
        char         script_path[512];
        char         lua_path[2048];
        volatile int running;
        volatile int ready;
        volatile int exited;
        int          exit_code;
        char         error_msg[256];
    } vmrt_hub_t;

    int             vmrt_ringbuf_write(vmrt_ringbuf_t *rb, const void *data, uint32_t len);
    int32_t         vmrt_ringbuf_read(vmrt_ringbuf_t *rb, void *buf, uint32_t buf_size);
    uint32_t        vmrt_ringbuf_available(const vmrt_ringbuf_t *rb);
]]

local script_dir = debug.getinfo(1, "S").source:match("^@(.*/)")  or "./"
local lib = ffi.load(script_dir .. "libvmrt.so")

-- Get pointers from C launcher
local pipe = ffi.cast("vmrt_pipe_t *", _VMRT_PIPE)
local hub_ctrl = ffi.cast("vmrt_hub_t *", _VMRT_HUB)

---------------------------------------------------------------------------
-- Transport (remote side of hub↔remote pipe)
---------------------------------------------------------------------------
local transport = require("transport")
local tx = transport.remote_side_ringbuf(pipe, lib)

---------------------------------------------------------------------------
-- JSON
---------------------------------------------------------------------------
local json_util = require("json_util")

---------------------------------------------------------------------------
-- Load ChainTree remote runtime
---------------------------------------------------------------------------
local ct_runtime = require("ct_runtime")
local ct_loader  = require("ct_loader_pure")
local builtins   = require("ct_builtins")
local fn_registry = require("fn_registry")
local defs       = require("ct_definitions")
local engine     = require("ct_engine")

local remote_json = os.getenv("VMRT_REMOTE_JSON") or ""
if remote_json == "" then
    hub_ctrl.exit_code = 1
    return
end

local remote_data = ct_loader.load(remote_json)

---------------------------------------------------------------------------
-- Remote user functions (simulated)
-- In real hardware, these would be C functions driving motors/sensors.
---------------------------------------------------------------------------
local remote_fns = {main = {}, one_shot = {}, boolean = {}}

remote_fns.main.REMOTE_RECV_MAIN = function(handle, bool_fn, node, event_id)
    if event_id ~= defs.CFL_TIMER_EVENT then return defs.CFL_CONTINUE end
    local bb = handle.blackboard

    -- Check transport for incoming RPC command
    local msg_str = tx:recv_rpc()
    if msg_str then
        local ok, cmd = pcall(json_util.decode, msg_str)
        if ok and cmd then
            if cmd.type == "shutdown" then
                bb.shutdown_requested = true
                return defs.CFL_CONTINUE
            end
            bb.active_command = cmd
            bb.exec_start = true
            bb.stream_seg_complete    = false
            bb.stream_action_complete = false
            bb.stream_obstacle        = false
            bb.stream_motor_fault     = false
            bb.stream_action_fault    = false
        end
    end
    return defs.CFL_CONTINUE
end
remote_fns.one_shot.REMOTE_RECV_INIT = function() end

-- Action durations (ticks per command type)
local action_durations = {
    path_segment    = 3,  arm_command       = 4,
    rpc_request     = 2,  drive_command     = 3,
    sensor_command  = 1,  wait              = 2,
    circumnavigation = 5, rotate            = 2,
    init_check      = 2,
}

remote_fns.main.REMOTE_EXEC_MAIN = function(handle, bool_fn, node, event_id)
    if event_id ~= defs.CFL_TIMER_EVENT then return defs.CFL_CONTINUE end
    local node_id = node.label_dict.ltree_name
    local ns = handle.node_state[node_id]
    if not ns then return defs.CFL_CONTINUE end
    local bb = handle.blackboard

    if bb.exec_start then
        bb.exec_start = false
        local cmd = bb.active_command
        if cmd then
            ns.command_type = cmd.type or "unknown"
            ns.ticks_remaining = action_durations[ns.command_type] or 2
            ns.active = true
        end
    end

    if ns.active then
        ns.ticks_remaining = ns.ticks_remaining - 1
        if ns.ticks_remaining <= 0 then
            -- Action completed — set stream flags
            if ns.command_type == "path_segment" or ns.command_type == "circumnavigation" then
                bb.stream_seg_complete = true
            else
                bb.stream_action_complete = true
            end
            ns.active = false
        end
    end
    return defs.CFL_CONTINUE
end

remote_fns.one_shot.REMOTE_EXEC_INIT = function(handle, node)
    local node_id = node.label_dict.ltree_name
    handle.node_state[node_id] = {ticks_remaining = 0, active = false}
end
remote_fns.one_shot.REMOTE_EXEC_TERM = function(handle, node)
    local node_id = node.label_dict.ltree_name
    handle.node_state[node_id] = nil
end
remote_fns.boolean.REMOTE_EXEC_CHECK = function(handle, node)
    local node_id = node.label_dict.ltree_name
    local ns = handle.node_state[node_id]
    if not ns then return false end
    return ns.active and ns.ticks_remaining <= 0
end

remote_fns.main.REMOTE_STREAM_MAIN = function(handle, bool_fn, node, event_id)
    if event_id ~= defs.CFL_TIMER_EVENT then return defs.CFL_CONTINUE end
    local bb = handle.blackboard

    -- Send stream updates via transport
    local update = {}
    local has_update = false

    if bb.stream_seg_complete then
        update.seg_complete = true; has_update = true
    end
    if bb.stream_action_complete then
        update.action_complete = true; has_update = true
    end
    if bb.stream_obstacle then
        update.obstacle = true; has_update = true
    end
    if bb.stream_motor_fault then
        update.motor_fault = true; has_update = true
    end
    if bb.stream_action_fault then
        update.action_fault = true; has_update = true
    end

    if has_update then
        tx:send_stream(json_util.encode(update))
    end

    return defs.CFL_CONTINUE
end
remote_fns.one_shot.REMOTE_STREAM_INIT = function() end

---------------------------------------------------------------------------
-- Create runtime
---------------------------------------------------------------------------
fn_registry.register_functions(remote_data, builtins, remote_fns)
local ok, missing = fn_registry.validate(remote_data)
if not ok then
    hub_ctrl.exit_code = 1
    return
end

local remote_handle = ct_runtime.create({delta_time = 0.1, max_ticks = 50000}, remote_data)

-- Activate remote_unit KB
engine.init_test(remote_handle, "remote_unit")
remote_handle.active_tests["remote_unit"] = true
remote_handle.active_test_count = 1

---------------------------------------------------------------------------
-- Tick loop
---------------------------------------------------------------------------
hub_ctrl.ready = 1

while hub_ctrl.running ~= 0 do
    -- Check for shutdown
    if remote_handle.blackboard.shutdown_requested then
        break
    end

    -- Tick remote runtime
    local kb = remote_handle.kb_table["remote_unit"]
    if kb then
        table.insert(remote_handle.event_queue, {
            node_id = kb.root_node,
            event_id = defs.CFL_TIMER_EVENT,
        })
    end
    while #remote_handle.event_queue > 0 do
        local ev = table.remove(remote_handle.event_queue, 1)
        engine.execute_event(remote_handle, ev.node_id,
            ev.event_id, ev.event_data, ev.event_type)
    end

    -- Yield CPU
    ffi.C.usleep(100)
end
