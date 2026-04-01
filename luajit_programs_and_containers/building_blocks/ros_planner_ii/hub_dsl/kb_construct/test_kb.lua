--[[
    test_kb.lua -- Build and query the surface ops KB.

    Usage:
        luajit test_kb.lua

    Requires SQLite KB library on LUA_PATH.
]]

local json = require("sqlite3_helpers").json

local DB_FILE = "surface_ops.db"

-- =====================================================================
-- Step 1: Build the KB
-- =====================================================================
print("=== Building Surface Ops KB ===\n")

-- Remove old DB
os.remove(DB_FILE)

-- Set arg for construct script (it reads arg[1] for db_file)
local saved_arg = arg
arg = { DB_FILE }
dofile("construct_surface_ops.lua")
arg = saved_arg

-- =====================================================================
-- Step 2: Query the KB
-- =====================================================================
print("\n=== Querying Surface Ops KB ===\n")

local kb_query = require("kb_query")
local q = kb_query.new(DB_FILE, "knowledge_base")

-- Site
local site = q:get_site()
print("Site: " .. tostring(site))

-- Boards
local boards = q:list_boards()
print("\nBoards:")
for _, b in ipairs(boards) do
    print("  " .. b)
    local board = q:get_board(b)
    if board then
        print("    Nodes: " .. #board.nodes)
        print("    Edges: " .. #board.edges)
    end
end

-- Robot classes
local classes = q:list_robot_classes()
print("\nRobot classes:")
for _, c in ipairs(classes) do
    print("  " .. c)
    local infra = q:get_robot_infra(c)
    if infra then
        print("    Virtual nodes: " .. table.concat(infra.virtual_nodes, ", "))
        print("    Tick rate: " .. tostring(infra.tick_rate_ms) .. "ms")
    end
end

-- Robot instances
local robots = q:list_robots()
print("\nRobot instances:")
for _, r in ipairs(robots) do
    print("  " .. r)

    local caps = q:get_capabilities(r)
    print("    Capabilities: " .. table.concat(caps, ", "))

    local hw = q:get_hardware(r)
    if hw then
        if hw.calibration then
            print("    Calibration: " .. json.encode(hw.calibration))
        end
        if hw.pose_dofs then
            print("    Pose DOFs: " .. table.concat(hw.pose_dofs, ", "))
        end
    end

    local conn = q:get_connection(r)
    if conn then
        print("    NATS: " .. conn.rpc_topic .. " / " .. conn.stream_topic)
    end

    local state = q:get_robot_state(r)
    if state then
        print("    State: " .. json.encode(state))
    end
end

-- Planner
local planner = q:get_planner_state()
print("\nPlanner state: " .. json.encode(planner))

-- Full site config (what startup would read)
print("\n--- Full Site Config ---")
local config = q:get_site_config()
print("Site: " .. config.site)
print("Boards: " .. table.concat(config.boards, ", "))
print("Robots:")
for name, rc in pairs(config.robots) do
    print(string.format("  %s: %d capabilities, nats=%s",
        name, #rc.capabilities, rc.nats and rc.nats.rpc_topic or "none"))
end

-- Cleanup
q:close()

print("\n=== PASSED ===")
