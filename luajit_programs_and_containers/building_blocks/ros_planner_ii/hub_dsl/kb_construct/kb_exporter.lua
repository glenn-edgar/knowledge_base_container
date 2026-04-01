--[[
    kb_exporter.lua -- Export SQLite KB to NATS KeyStore at startup.

    Reads robot instances, virtual nodes, board data from SQLite and
    publishes to NATS KeyStore. Any process (bridge, dashboard, etc.)
    can read from NATS KV without SQLite access.

    NATS KV keys mirror SQLite KB paths:
      {site}.ROBOT_INSTANCE.{name}.connection  → connection JSON
      {site}.ROBOT_INSTANCE.{name}.capabilities → virtual_nodes array
      {site}.VIRTUAL_NODES.{name}              → VN definition JSON
      {site}.BOARD.{name}                      → board data JSON

    Usage:
        local kb_exporter = require("kb_exporter")
        local stats = kb_exporter.export({
            db_file     = "surface_ops.db",
            nats_server = "nats://127.0.0.1:4222",
            bucket      = "kb_export",
        })
        print(stats.keys_written)
]]

local json_util = require("json_util")
local kb_query  = require("kb_query")
local ks_lib    = require("lib.nats_key_store")

local M = {}

--- Export SQLite KB data to NATS KeyStore.
-- @param opts  table:
--   db_file      string: path to SQLite KB
--   nats_server  string: NATS URL (default nats://127.0.0.1:4222)
--   bucket       string: KeyStore bucket name (default "kb_export")
--   ltree_path   string: path to ltree extension (default /usr/local/lib/ltree)
-- @return stats table: { keys_written, site, robots, boards, virtual_nodes }
function M.export(opts)
    local db_file     = opts.db_file     or error("kb_exporter: db_file required")
    local nats_server = opts.nats_server or "nats://127.0.0.1:4222"
    local bucket      = opts.bucket      or "kb_export"
    local ltree_path  = opts.ltree_path  or "/usr/local/lib/ltree"

    -- Open SQLite KB
    local q = kb_query.new(db_file, "knowledge_base", ltree_path)
    local site = q:get_site()

    -- Open NATS KeyStore
    local ks = ks_lib.KeyStore.new({
        server        = nats_server,
        bucket        = bucket,
        description   = "KB export: " .. site,
        create_bucket = true,
        history       = 1,
        client_name   = "kb_exporter",
    })
    ks:connect()

    local keys_written = 0

    -- Export robot instances
    local robots = q:list_robots()
    for _, robot_name in ipairs(robots) do
        local config = q:get_robot_config(robot_name)

        -- Connection info (includes comm_type)
        if config.connection then
            local key = site .. ".ROBOT_INSTANCE." .. robot_name .. ".connection"
            ks:put(key, json_util.encode(config.connection))
            keys_written = keys_written + 1
        end

        -- Capabilities
        if config.capabilities then
            local key = site .. ".ROBOT_INSTANCE." .. robot_name .. ".capabilities"
            ks:put(key, json_util.encode(config.capabilities))
            keys_written = keys_written + 1
        end

        -- NATS topics
        if config.nats then
            local key = site .. ".ROBOT_INSTANCE." .. robot_name .. ".nats"
            ks:put(key, json_util.encode(config.nats))
            keys_written = keys_written + 1
        end

        -- Hardware
        if config.hardware then
            local key = site .. ".ROBOT_INSTANCE." .. robot_name .. ".hardware"
            ks:put(key, json_util.encode(config.hardware))
            keys_written = keys_written + 1
        end
    end

    -- Export virtual node definitions
    local vn_all = q:get_all_virtual_nodes()
    local vn_count = 0
    for vn_name, vn_def in pairs(vn_all) do
        local key = site .. ".VIRTUAL_NODES." .. vn_name
        ks:put(key, json_util.encode(vn_def))
        keys_written = keys_written + 1
        vn_count = vn_count + 1
    end

    -- Export boards
    local boards = q:list_boards()
    for _, board_name in ipairs(boards) do
        local board_data = q:get_board(board_name)
        if board_data then
            local key = site .. ".BOARD." .. board_name
            ks:put(key, json_util.encode(board_data))
            keys_written = keys_written + 1
        end
    end

    -- Export planner state
    local planner = q:get_planner_state()
    if planner then
        local key = site .. ".PLANNER.state"
        ks:put(key, json_util.encode(planner))
        keys_written = keys_written + 1
    end

    -- Cleanup
    q:close()
    ks:disconnect()
    ks:destroy()

    return {
        keys_written  = keys_written,
        site          = site,
        robots        = #robots,
        boards        = #boards,
        virtual_nodes = vn_count,
    }
end

--- Read exported data from NATS KeyStore (for any process without SQLite).
-- @param opts  table: { nats_server, bucket }
-- @return reader object with get methods
function M.reader(opts)
    local nats_server = opts.nats_server or "nats://127.0.0.1:4222"
    local bucket      = opts.bucket      or "kb_export"

    local ks = ks_lib.KeyStore.new({
        server      = nats_server,
        bucket      = bucket,
        history     = 1,
        client_name = "kb_reader",
    })
    ks:connect()

    local R = {}

    function R:get(key)
        local val = ks:get(key)
        if val then
            local ok, decoded = pcall(json_util.decode, val)
            if ok then return decoded end
        end
        return nil
    end

    function R:get_robot_connection(site, robot_name)
        return self:get(site .. ".ROBOT_INSTANCE." .. robot_name .. ".connection")
    end

    function R:get_robot_capabilities(site, robot_name)
        return self:get(site .. ".ROBOT_INSTANCE." .. robot_name .. ".capabilities")
    end

    function R:get_virtual_node(site, vn_name)
        return self:get(site .. ".VIRTUAL_NODES." .. vn_name)
    end

    function R:get_board(site, board_name)
        return self:get(site .. ".BOARD." .. board_name)
    end

    function R:close()
        ks:disconnect()
        ks:destroy()
    end

    return R
end

return M
