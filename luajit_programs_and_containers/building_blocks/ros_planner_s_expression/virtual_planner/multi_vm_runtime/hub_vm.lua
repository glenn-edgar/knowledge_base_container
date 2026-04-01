--[[
    hub_vm.lua -- Hub thread entry point for multi-VM runtime.

    Runs in its own LuaJIT VM (pthread). Responsibilities:
      - Load hub ChainTree JSON IR
      - Tick hub runtime independently
      - Receive action JSON from main VM via main↔hub pipe
      - Send/receive RPC and streaming data to/from remote via hub↔remote transport
      - Send status/completion back to main VM via main↔hub pipe

    The hub does NOT load the remote runtime — that runs in its own VM.
    Communication with the remote is through the transport layer, which
    is the same abstraction that will be swapped for serial/network
    when real hardware is introduced.

    Globals set by C launcher:
      _VMRT_PIPE  — pointer to vmrt_pipe_t (main↔hub pipe)
      _VMRT_HUB   — pointer to vmrt_hub_t (thread control)

    Environment variables:
      VMRT_HUB_JSON        — path to hub.json
      VMRT_REMOTE_PIPE_PTR — hex address of hub↔remote pipe (set by main_vm)

    Communication protocol (main↔hub pipe, JSON strings):
      Main → Hub:
        {"cmd":"stage","current":{...},"next":{...}}
        {"cmd":"shutdown"}

      Hub → Main:
        {"event":"ready"}
        {"event":"action_complete","action_index":N,"kb":"..."}
        {"event":"error","msg":"..."}
        {"event":"shutdown"}
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

-- Get pointers from C launcher (main↔hub pipe)
local main_pipe = ffi.cast("vmrt_pipe_t *", _VMRT_PIPE)
local hub_ctrl  = ffi.cast("vmrt_hub_t *", _VMRT_HUB)

---------------------------------------------------------------------------
-- Main↔Hub pipe helpers
---------------------------------------------------------------------------
local read_buf = ffi.new("uint8_t[?]", 8192)

local function main_recv()
    local len = lib.vmrt_ringbuf_read(main_pipe.to_hub, nil, 0)
    if len <= 0 then return nil end
    if len > 8192 then
        read_buf = ffi.new("uint8_t[?]", len)
    end
    local got = lib.vmrt_ringbuf_read(main_pipe.to_hub, read_buf, ffi.sizeof(read_buf))
    if got <= 0 then return nil end
    return ffi.string(read_buf, got)
end

local function main_send(msg)
    return lib.vmrt_ringbuf_write(main_pipe.from_hub, msg, #msg) == 0
end

local json_util = require("json_util")

local function send_event(tbl)
    main_send(json_util.encode(tbl))
end

---------------------------------------------------------------------------
-- Hub↔Remote transport
---------------------------------------------------------------------------
local transport = require("transport")

-- Get hub↔remote pipe pointer from environment (set by main_vm)
local remote_pipe_str = os.getenv("VMRT_REMOTE_PIPE_PTR") or ""
if remote_pipe_str == "" then
    send_event({event = "error", msg = "VMRT_REMOTE_PIPE_PTR required"})
    hub_ctrl.exit_code = 1
    return
end

local remote_pipe_addr = tonumber(remote_pipe_str)
local remote_pipe = ffi.cast("vmrt_pipe_t *", remote_pipe_addr)
local tx = transport.hub_side_ringbuf(remote_pipe, lib)

---------------------------------------------------------------------------
-- Load hub ChainTree runtime
---------------------------------------------------------------------------
local ct_runtime = require("ct_runtime")
local ct_loader  = require("ct_loader_pure")
local builtins   = require("ct_builtins")
local fn_registry = require("fn_registry")
local defs       = require("ct_definitions")
local engine     = require("ct_engine")

local hub_json = os.getenv("VMRT_HUB_JSON") or ""
if hub_json == "" then
    send_event({event = "error", msg = "VMRT_HUB_JSON required"})
    hub_ctrl.exit_code = 1
    return
end

local hub_data = ct_loader.load(hub_json)
fn_registry.register_functions(hub_data, builtins)
local hub_handle = ct_runtime.create({delta_time = 0.1, max_ticks = 50000}, hub_data)

---------------------------------------------------------------------------
-- KB name ↔ index mapping
---------------------------------------------------------------------------
local kb_by_index = {
    [1]  = "init_check",    [2]  = "path_spline",
    [3]  = "path_line",     [4]  = "path_wall",
    [5]  = "path_rotate",   [6]  = "deliver_part",
    [7]  = "paint_sample",  [8]  = "load_shipping",
    [9]  = "pass_gate",     [10] = "inspection_scan",
    [11] = "idle",
}

---------------------------------------------------------------------------
-- Tick hub runtime
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
-- Hub↔Remote bridge: read streams, send commands
---------------------------------------------------------------------------
local function bridge_streams()
    -- Read streaming updates from remote and apply to hub blackboard
    while true do
        local msg_str = tx:recv_stream()
        if not msg_str then break end
        local ok, update = pcall(json_util.decode, msg_str)
        if ok and update then
            local bb = hub_handle.blackboard
            if update.seg_complete    then bb.bitmap_seg_complete    = true end
            if update.action_complete then bb.bitmap_action_complete = true end
            if update.obstacle        then bb.bitmap_obstacle        = true end
            if update.motor_fault     then bb.bitmap_motor_fault     = true end
            if update.action_fault    then bb.bitmap_action_fault    = true end
        end
    end
end

local function bridge_commands()
    -- Send pending commands from hub blackboard to remote
    local bb = hub_handle.blackboard
    if bb.command_sent and bb.current_command then
        tx:send_rpc(json_util.encode(bb.current_command))
        bb.command_sent = false
    end
end

---------------------------------------------------------------------------
-- KB state management
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
-- Main loop
---------------------------------------------------------------------------
hub_ctrl.ready = 1
send_event({event = "ready"})

while hub_ctrl.running ~= 0 do
    -- Check for messages from main VM
    local msg_str = main_recv()
    if msg_str then
        local ok, msg = pcall(json_util.decode, msg_str)
        if ok and msg then
            if msg.cmd == "stage" then
                if msg.current then
                    hub_handle.blackboard.current_test_json = json_util.encode(msg.current)
                end
                if msg.next then
                    hub_handle.blackboard.next_test_json = json_util.encode(msg.next)
                end
                if msg.current and msg.current.test_id then
                    current_action_idx = msg.action_index or 0
                    deactivate_kb()
                    activate_kb(msg.current.test_id)
                end
            elseif msg.cmd == "shutdown" then
                -- Tell remote to shut down too
                tx:send_rpc(json_util.encode({type = "shutdown"}))
                send_event({event = "shutdown"})
                break
            end
        end
    end

    -- Bridge: hub blackboard ↔ remote transport
    bridge_streams()
    bridge_commands()

    -- Tick hub
    tick_hub()

    -- Check if current KB completed
    if active_kb and check_kb_complete() then
        send_event({
            event = "action_complete",
            action_index = current_action_idx,
            kb = active_kb,
        })
        deactivate_kb()
    end

    ffi.C.usleep(100)
end
