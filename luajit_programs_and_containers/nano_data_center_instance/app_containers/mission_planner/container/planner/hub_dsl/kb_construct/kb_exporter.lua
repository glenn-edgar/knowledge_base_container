--[[
    kb_exporter.lua -- Export SQLite KB to NATS KeyStore at startup.

    Reads robot classes, virtual nodes, board data from SQLite and
    publishes to NATS KeyStore. Any process (bridge, dashboard, etc.)
    can read from NATS KV without SQLite access.

    Robot instances are NOT exported — they register dynamically via
    the link protocol.

    NATS KV keys mirror SQLite KB paths (lowercase namespace):
      {site}.robot_class.{name}.infra     → class config JSON
      {site}.virtual_nodes.{name}         → VN definition JSON
      {site}.boards.{name}                → board data JSON

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

    -- Export robot classes (infra only — no instances or hw profiles in KB)
    local classes = q:list_robot_classes()
    local class_count = 0
    for _, class_name in ipairs(classes) do
        local infra = q:get_robot_infra(class_name)
        if infra then
            local key = site .. ".robot_class." .. class_name .. ".infra"
            ks:put(key, json_util.encode(infra))
            keys_written = keys_written + 1
            class_count = class_count + 1
        end
    end

    -- Export virtual node definitions
    local vn_all = q:get_all_virtual_nodes()
    local vn_count = 0
    for vn_name, vn_def in pairs(vn_all) do
        local key = site .. ".virtual_nodes." .. vn_name
        ks:put(key, json_util.encode(vn_def))
        keys_written = keys_written + 1
        vn_count = vn_count + 1
    end

    -- Export boards
    local boards = q:list_boards()
    for _, board_name in ipairs(boards) do
        local board_data = q:get_board(board_name)
        if board_data then
            local key = site .. ".boards." .. board_name
            ks:put(key, json_util.encode(board_data))
            keys_written = keys_written + 1
        end
    end

    -- Export planner state
    local planner = q:get_planner_state()
    if planner then
        local key = site .. ".planner.state"
        ks:put(key, json_util.encode(planner))
        keys_written = keys_written + 1
    end

    -- Cleanup
    q:close()
    ks:disconnect()
    ks:destroy()

    return {
        keys_written   = keys_written,
        site           = site,
        boards         = #boards,
        virtual_nodes  = vn_count,
        robot_classes  = class_count,
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

    function R:get_class_infra(site, class_name)
        return self:get(site .. ".robot_class." .. class_name .. ".infra")
    end

    function R:get_virtual_node(site, vn_name)
        return self:get(site .. ".virtual_nodes." .. vn_name)
    end

    function R:get_board(site, board_name)
        return self:get(site .. ".boards." .. board_name)
    end

    function R:close()
        ks:disconnect()
        ks:destroy()
    end

    return R
end

return M
