--[[
    main_vm.lua -- Main VM orchestrator for multi-VM runtime.

    Runs the global planner and local planner in the main thread.
    Spawns one hub VM thread per robot. Communicates via pipes.

    Usage:
      luajit main_vm.lua <board_dir> <strategy_file>

    Example:
      luajit main_vm.lua workshop_floor strategy_full.lua
]]

local ffi  = require("ffi")
local vmrt = require("vmrt_ffi")
local json_util = require("json_util")
local global_planner = require("global_planner")

ffi.cdef[[
    int usleep(unsigned int usec);
    char *realpath(const char *path, char *resolved_path);
    int setenv(const char *name, const char *value, int overwrite);
    typedef unsigned long uintptr_t;
]]

---------------------------------------------------------------------------
-- Config
---------------------------------------------------------------------------
-- Resolve to absolute paths so hub VM threads can find modules
local script_dir_rel = debug.getinfo(1, "S").source:match("^@(.*/)")  or "./"
local function resolve(path)
    local buf = ffi.new("char[?]", 4096)
    local r = ffi.C.realpath(path, buf)
    if r ~= nil then return ffi.string(r) end
    return path
end

local script_dir = resolve(script_dir_rel) .. "/"
local vp_root    = resolve(script_dir_rel .. "../") .. "/"
local boards_dir = vp_root .. "boards/"

local board_name    = arg and arg[1] or "workshop_floor"
local strategy_file = arg and arg[2] or "strategy_full.lua"

local robot_name  = arg and arg[3] or "test_robot"
local robot_dir   = vp_root .. "robots/" .. robot_name .. "/"
local hub_json    = robot_dir .. "hub.json"
local remote_json = robot_dir .. "remote.json"
local hub_script  = script_dir .. "hub_vm.lua"

---------------------------------------------------------------------------
-- KB name ↔ index mapping
---------------------------------------------------------------------------
local kb_name_to_index = {
    init_check = 1, path_spline = 2, path_line = 3, path_wall = 4,
    path_rotate = 5, deliver_part = 6, paint_sample = 7,
    load_shipping = 8, pass_gate = 9, inspection_scan = 10, idle = 11,
}

local function action_to_kb(action)
    if action.action_type == "init_check"  then return "init_check" end
    if action.action_type == "path_rotate" then return "path_rotate" end
    if action.action_type == "path" then
        local nav = action.nav_method
        if nav == "spline_follow" then return "path_spline" end
        if nav == "line_follow"   then return "path_line" end
        if nav == "wall_ride"     then return "path_wall" end
        return "path_spline"
    end
    if action.action_type == "mission" then return action.catalog_key end
    return nil
end

local function action_to_json(actions, idx)
    local action = actions[idx]
    if not action then return nil end
    local kb_name = action_to_kb(action)
    if not kb_name then return nil end

    local blob = {}
    for k, v in pairs(action) do blob[k] = v end
    blob.test_id = kb_name_to_index[kb_name] or 0

    local next_idx = idx + 1
    if next_idx <= #actions then
        local next_kb = action_to_kb(actions[next_idx])
        blob.next_test = kb_name_to_index[next_kb] or 0
    else
        blob.next_test = kb_name_to_index["idle"]
    end
    return blob
end

---------------------------------------------------------------------------
-- Poll helper
---------------------------------------------------------------------------
local function recv_wait(pipe, timeout_ms)
    timeout_ms = timeout_ms or 2000
    local elapsed = 0
    while elapsed < timeout_ms do
        local msg = vmrt.recv(pipe)
        if msg then
            local ok, tbl = pcall(json_util.decode, msg)
            if ok then return tbl end
        end
        ffi.C.usleep(1000)
        elapsed = elapsed + 1
    end
    return nil
end

---------------------------------------------------------------------------
-- Global plan
---------------------------------------------------------------------------
local board_dir = boards_dir .. board_name .. "/"
local vn       = dofile(board_dir .. "board.lua")
local strategy = dofile(board_dir .. strategy_file)
local plan     = global_planner.plan(strategy, vn)

print(string.format("Board: %s", board_name))
print(string.format("Global plan: %d virtual actions\n", #plan.actions))
global_planner.print_plan(plan)
print()

---------------------------------------------------------------------------
-- Spawn hub + remote threads
---------------------------------------------------------------------------
local ct_runtime_dir = resolve(vp_root .. "../../chain_tree_luajit/runtime_dict")
local ct_json_dir    = resolve(vp_root .. "../../chain_tree_luajit/lua_dsl/luajit_pipeline")

-- Build LUA_PATH for child VMs
local child_lua_path = table.concat({
    vp_root .. "lib/?.lua",
    vp_root .. "hub_functions/?.lua",
    robot_dir .. "?.lua",
    vp_root .. "hub_templates/?.lua",
    ct_runtime_dir .. "/?.lua",
    ct_json_dir .. "/?.lua",
    script_dir .. "?.lua",
    "?.lua",
    "",
}, ";")

-- Set env vars (child VMs inherit environment)
ffi.C.setenv("VMRT_HUB_JSON", hub_json, 1)
ffi.C.setenv("VMRT_REMOTE_JSON", remote_json, 1)

-- Create pipes
local main_hub_pipe    = vmrt.pipe_create()   -- main ↔ hub
local hub_remote_pipe  = vmrt.pipe_create()   -- hub ↔ remote

-- Pass hub↔remote pipe address to hub VM via environment
-- (the pipe is in C heap, visible to all threads in the process)
local remote_pipe_addr = tonumber(ffi.cast("uintptr_t",
    ffi.cast("void *", hub_remote_pipe)))
ffi.C.setenv("VMRT_REMOTE_PIPE_PTR", string.format("%.0f", remote_pipe_addr), 1)

-- Spawn remote thread first (so it's ready when hub starts sending)
local remote_script = script_dir .. "remote_vm.lua"
local remote_thread = vmrt.hub_spawn(remote_script, child_lua_path, hub_remote_pipe)

-- Wait for remote ready
local timeout_r = 0
while not vmrt.hub_is_ready(remote_thread) do
    ffi.C.usleep(1000)
    timeout_r = timeout_r + 1
    if timeout_r > 5000 then
        print("ERROR: remote did not become ready")
        local err = vmrt.hub_error(remote_thread)
        if err then print("  remote error: " .. err) end
        vmrt.hub_stop(remote_thread)
        vmrt.hub_destroy(remote_thread)
        vmrt.pipe_destroy(main_hub_pipe)
        vmrt.pipe_destroy(hub_remote_pipe)
        os.exit(1)
    end
end
print("Remote thread ready.")

-- Spawn hub thread
local hub = vmrt.hub_spawn(hub_script, child_lua_path, main_hub_pipe)

-- Wait for hub ready
local ready_msg = recv_wait(main_hub_pipe.from_hub, 5000)
if not ready_msg or ready_msg.event ~= "ready" then
    print("ERROR: hub did not become ready")
    local err = vmrt.hub_error(hub)
    if err then print("  hub error: " .. err) end
    vmrt.hub_stop(hub)
    vmrt.hub_stop(remote_thread)
    vmrt.hub_destroy(hub)
    vmrt.hub_destroy(remote_thread)
    vmrt.pipe_destroy(main_hub_pipe)
    vmrt.pipe_destroy(hub_remote_pipe)
    os.exit(1)
end
print("Hub thread ready.\n")

---------------------------------------------------------------------------
-- Local planner: sequence actions through the hub
---------------------------------------------------------------------------
print("=== Running Multi-VM Simulation ===\n")

local completed = 0
local total = #plan.actions

for i = 1, total do
    local current = action_to_json(plan.actions, i)
    local next_action = action_to_json(plan.actions, i + 1)

    -- If no next action, point to idle
    if not next_action then
        next_action = {test_id = kb_name_to_index["idle"], next_test = 0}
    end

    -- Send stage command to hub
    local stage_msg = json_util.encode({
        cmd = "stage",
        action_index = i,
        current = current,
        next = next_action,
    })
    vmrt.send(main_hub_pipe.to_hub, stage_msg)

    -- Wait for action completion
    local result = recv_wait(main_hub_pipe.from_hub, 10000)
    if not result then
        print(string.format("  action %d: TIMEOUT", i))
        break
    end

    if result.event == "action_complete" then
        local action = plan.actions[i]
        local label = action.action_type
        if action.from_name then
            label = label .. " " .. action.from_name .. " -> " .. (action.to_name or "")
        elseif action.catalog_key then
            label = label .. " " .. action.catalog_key
        end
        print(string.format("  action %2d: completed [%s] (KB: %s)",
            i, label, result.kb or "?"))
        completed = completed + 1
    elseif result.event == "error" then
        print(string.format("  action %d: ERROR - %s", i, result.msg or "unknown"))
        break
    end
end

---------------------------------------------------------------------------
-- Shutdown
---------------------------------------------------------------------------
-- Tell hub to shut down (hub will forward shutdown to remote via transport)
vmrt.send(main_hub_pipe.to_hub, json_util.encode({cmd = "shutdown"}))
local shutdown_msg = recv_wait(main_hub_pipe.from_hub, 2000)

local hub_exit = vmrt.hub_join(hub)
local remote_exit = vmrt.hub_join(remote_thread)

print(string.format("\n--- Results ---"))
print(string.format("Actions: %d / %d", completed, total))
print(string.format("Hub exit: %d, Remote exit: %d", hub_exit, remote_exit))

-- Write plan output
global_planner.write_json(plan, board_dir .. "global_plan.json")
global_planner.write_yaml(plan, board_dir .. "global_plan.yaml")

vmrt.hub_destroy(hub)
vmrt.hub_destroy(remote_thread)
vmrt.pipe_destroy(main_hub_pipe)
vmrt.pipe_destroy(hub_remote_pipe)

if completed == total then
    print("\nPASSED")
else
    print("\nFAILED")
    os.exit(1)
end
