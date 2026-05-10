--[[
    kb_query.lua -- Knowledge Base query helper for the mission_planner.

    Reads the v3 KB (Postgres-backed) via KnowledgeBaseManager's ltree
    queries. Reads the planner's own spec under the v3 anchor:

        system.<sys>.site.<S>.app_containers.<container_name>.spec.KB_*FIELD.*

    All paths are composed via ndc_paths (Phase B Layer M-1 discipline);
    no string literals here.

    Robot instances are NOT in the KB. Robots register dynamically via
    the link protocol and announce their class_name. Robot classes
    themselves are owned by robot_manager (not yet ported); until that
    lands, FALLBACK_ROBOT_CLASSES below provides drive_base just well
    enough for Layer V acceptance. The fallback engages only when the
    v3 path returns nil; the future cross-app read auto-supersedes it.

    Usage:
        local kb_query = require("kb_query")
        local pg_conn = { host="pg-vector", port=5432, dbname="knowledge_base",
                          user="gedgar",    password=os.getenv("PG_PASSWORD") }
        local q = kb_query.new(pg_conn,
                               "moon_base",          -- system_name
                               "moon_base_alpha",    -- site
                               "mission_planner_01") -- own_instance_id

        local caps    = q:get_class_capabilities("drive_base")
        local classes = q:list_robot_classes()
        local vns     = q:get_all_virtual_nodes()
        local blob    = q:get_app_spec_jsonb("mission_planner_01", "capabilities")

        q:close()
]]

local KBM   = require("knowledge_base_manager")
local json  = require("sqlite3_helpers").json
local paths = require("ndc_paths")

local M = {}
M.__index = M

---------------------------------------------------------------------------
-- FALLBACK_ROBOT_CLASSES (Q2.2 stop-gap; engages only when robot_manager's
-- v3 spec is absent). Single drive_base entry mirroring the drive-base
-- app catalogue capabilities.
--
-- TODO: remove when robot_manager ports under
--       app_containers.robot_manager_01.spec.KB_JSONB_FIELD.robot_classes
--       (the get_app_spec_jsonb call below will start succeeding and this
--       constant becomes unreachable).
---------------------------------------------------------------------------

local FALLBACK_ROBOT_CLASSES = {
    drive_base = {
        virtual_nodes    = { "init_check", "path_spline", "path_line",
                             "transit", "operation", "idle",
                             "error_recovery" },
        operation_types  = { "drive", "rotate", "follow_spline" },
        energy_rate      = 1.0,
        energy_max       = 10000,
        energy_infinite  = false,
        topics           = {
            rpc          = "rpc",
            stream_bus   = "stream_bus",
            link         = "link",
        },
        tick_rate_ms     = 100,
        worker_kbs       = {},
    },
}

---------------------------------------------------------------------------
-- Constructor / teardown
---------------------------------------------------------------------------

-- @param planner_namespace  optional 5th arg (Phase 5 C4). Tenant
--   identifier scoping which boards / robots / virtual nodes this
--   planner instance owns. Defaults to own_instance_id when not
--   provided so the single-tenant call sites keep working unchanged.
--   Multi-tenant query scoping (the consumer of this field) lands
--   with Phase 6/7 fixtures; for now it's threaded through but only
--   stored.
function M.new(pg_conn, system_name, site, own_instance_id, planner_namespace)
    assert(type(pg_conn) == "table",
           "kb_query.new: pg_conn must be a table " ..
           "{host, port, dbname, user, password}")
    assert(type(system_name) == "string" and system_name ~= "",
           "kb_query.new: system_name required (was a topology auto-detect in v2)")
    assert(type(site) == "string" and site ~= "",
           "kb_query.new: site required")
    assert(type(own_instance_id) == "string" and own_instance_id ~= "",
           "kb_query.new: own_instance_id required (this container's name, " ..
           "e.g. 'mission_planner_01')")

    -- Configure ndc_paths idempotently. The helper rejects conflicting
    -- system_name re-configures, so this is safe to call from any caller.
    if not paths.system_name() then
        paths.configure({ system_name = system_name })
    end

    local self = setmetatable({}, M)
    -- KBM.new(table_name, connection_params, upload_flag)
    -- upload_flag=true -> read-only mode (skip table creation).
    self.kb                = KBM.new("knowledge_base", pg_conn, true)
    self.system_name       = system_name
    self.site              = site
    self.own_instance_id   = own_instance_id
    self.planner_namespace = planner_namespace or own_instance_id
    return self
end

function M:close()
    self.kb:disconnect()
end

---------------------------------------------------------------------------
-- Boards (file_store-backed, content-addressable)
--
-- Reads navigation boards from pg's fs_blob/fs_node tables. Boards are
-- uploaded by the operator-side construction/scripts/upload_board.lua
-- CLI (writer="commissioning"); planner code reads via doc_get and is
-- never a writer.
--
-- Cache: keyed by sha256 hex (NOT by board name). Once a hash has been
-- parsed, the in-memory graph is reused for every subsequent fetch of
-- that hash. Updating a board on disk + re-uploading flips the fs_node
-- pointer to a new hash; next get_active_board() call sees the cache
-- miss and re-parses.
--
-- Mid-mission revision policy is (1) drain-then-flip: callers capture
-- the returned sha256_hex when starting a mission and re-check on each
-- tick. Drift handling is the sequencer's concern (see _check_board_drift
-- in sequencer.lua).
---------------------------------------------------------------------------

local function bytes_to_hex(bytes)
    if not bytes then return nil end
    local out = {}
    for i = 1, #bytes do out[#out + 1] = string.format("%02x", bytes:byte(i)) end
    return table.concat(out)
end

--- Get the currently active board by name.
-- @param name string  board name (e.g. "landing_zone")
-- @return table { graph_data = <parsed JSON>, sha256_hex = <64-char hex>,
--                 cache_hit = bool } on success, or nil + err on missing/invalid.
function M:get_active_board(name)
    assert(type(name) == "string" and name ~= "",
           "get_active_board: name required")
    assert(self.planner_namespace and self.planner_namespace ~= "",
           "get_active_board: planner_namespace required (Phase 7 per-tenant boards)")

    local doc_store = require("kb_doc_store")
    -- Phase 7: per-tenant board path (matches boards.lua subsystem +
    -- compile_board.lua's --planner-namespace flag).
    local path = string.format("system.%s.site.%s.planner.%s.boards.%s",
        self.system_name, self.site, self.planner_namespace, name)

    -- Cheap stat: fetches sha256 + size + mtime, NOT content. Lets us
    -- cache-hit without paying the bytea decode + JSON parse cost.
    local stat, serr = doc_store.doc_stat(self.kb.conn, "knowledge_base", path)
    if not stat then
        return nil, "board not found: " .. name
            .. (serr and (" (" .. serr .. ")") or "")
    end
    local sha_hex = bytes_to_hex(stat.sha256)
    if not sha_hex then
        return nil, "board has no sha256 (fs_node pointer null?): " .. name
    end

    self._board_cache = self._board_cache or {}
    local cached = self._board_cache[sha_hex]
    if cached then
        return { graph_data = cached, sha256_hex = sha_hex, cache_hit = true }
    end

    -- Cache miss: full fetch + parse.
    local row, gerr = doc_store.doc_get(self.kb.conn, "knowledge_base", path)
    if not row or not row.content then
        return nil, "doc_get returned no content for " .. path
            .. (gerr and (" (" .. gerr .. ")") or "")
    end
    local dkjson = require("dkjson")
    local graph, _, perr = dkjson.decode(row.content)
    if not graph then
        return nil, "board content not valid JSON: " .. tostring(perr)
    end

    -- Schema-version guard: refuse boards whose declared shape this reader
    -- does not understand (e.g. a v2 board with sub-paths uploaded against
    -- a v1 planner). Missing schema_version is treated as v1 for backward
    -- compatibility with pre-A.4f boards. See BOARD_FORMAT.md.
    local sv = graph.schema_version
    if sv ~= nil and sv ~= 1 then
        return nil, string.format(
            "board schema_version=%s not supported by this reader (expected 1); "
            .. "upgrade the planner or downgrade the board",
            tostring(sv))
    end

    self._board_cache[sha_hex] = graph
    return { graph_data = graph, sha256_hex = sha_hex, cache_hit = false }
end

--- Get just the active sha256_hex for a board (cheap drift-detection
-- helper for the sequencer's per-tick board_drift check).
-- @param name string
-- @return string sha256_hex on success, nil + err on missing.
function M:get_active_board_sha(name)
    assert(self.planner_namespace and self.planner_namespace ~= "",
           "get_active_board_sha: planner_namespace required (Phase 7)")
    local doc_store = require("kb_doc_store")
    -- Phase 7: per-tenant board path (matches get_active_board above).
    local path = string.format("system.%s.site.%s.planner.%s.boards.%s",
        self.system_name, self.site, self.planner_namespace, name)
    local stat = doc_store.doc_stat(self.kb.conn, "knowledge_base", path)
    if not stat then return nil, "board not found: " .. name end
    return bytes_to_hex(stat.sha256)
end

---------------------------------------------------------------------------
-- Helper: single-row exact-path lookup via raw SQL.
--
-- Phase 7 gap-4 fix (2026-05-10, surfaced by ROBSIM C3 cluster smoke):
-- the Postgres port of KnowledgeBaseManager is write-only by design --
-- only add_kb / add_node / add_link / add_link_mount exist. Read methods
-- (find_by_pattern, find_descendants) live only on the SQLite3 KBM and
-- were never ported. Three callers below (get_app_spec_jsonb,
-- get_app_spec_status, get_planner_state) were trying to call
-- self.kb:find_by_pattern -- always crashed at runtime with
-- "attempt to call method 'find_by_pattern' (a nil value)".
--
-- All three callers pass CONCRETE PATHS, not patterns. The fix is a
-- single-row lookup against the knowledge_base table (filtered by the
-- kb-name column + ltree path equality). Mirrors the kb_doc_store
-- prepare/execute/fetch pattern already used at lines 162, 179, 217
-- above for board fs_node / fs_blob queries.
---------------------------------------------------------------------------

local function pg_escape(s) return tostring(s):gsub("'", "''") end

local function fetch_row_by_path(conn, kb_name, path)
    local sql = string.format(
        "SELECT path::text AS path, label, name, " ..
        "properties::text AS properties, data::text AS data " ..
        "FROM knowledge_base " ..
        "WHERE knowledge_base = '%s' AND path = '%s'::ltree LIMIT 1",
        pg_escape(kb_name), pg_escape(path))
    local sth, perr = conn:prepare(sql)
    if not sth then return nil, "prepare: " .. tostring(perr) end
    local ok, eerr = sth:execute()
    if not ok then sth:close(); return nil, "execute: " .. tostring(eerr) end
    local row = sth:fetch(true)
    sth:close()
    return row
end

---------------------------------------------------------------------------
-- Helper: parse JSON fields from a row
---------------------------------------------------------------------------

local function parse_row(row)
    local r = {
        path  = row.path,
        label = row.label,
        name  = row.name,
    }
    if row.properties and row.properties ~= "" then
        local ok, p = pcall(json.decode, row.properties)
        r.properties = ok and p or {}
    else
        r.properties = {}
    end
    if row.data and row.data ~= "" then
        local ok, d = pcall(json.decode, row.data)
        r.data = ok and d or {}
    else
        r.data = {}
    end
    return r
end

---------------------------------------------------------------------------
-- Generic app_containers spec readers (v3)
---------------------------------------------------------------------------

--- Read a JSONB blob from another (or this) app's manifest namespace.
-- Path: app_containers.<container_name>.spec.manifest.KB_JSONB_FIELD.<key>
-- @param container_name string  the instance_id ("mission_planner_01")
-- @param key            string  the JSONB field name ("capabilities", ...)
-- @return decoded value or nil
function M:get_app_spec_jsonb(container_name, key)
    local path = paths.app_manifest_jsonb_path(self.site, container_name, key)
    local row = fetch_row_by_path(self.kb.conn, "system", path)
    if row then return parse_row(row).data end
    return nil
end

--- Read a scalar status field from another (or this) app's manifest namespace.
-- Path: app_containers.<container_name>.spec.manifest.KB_STATUS_FIELD.<name>
-- @param container_name string
-- @param name           string  the status field name ("version", "class")
-- @return decoded value or nil
function M:get_app_spec_status(container_name, name)
    local path = paths.app_manifest_status_path(self.site, container_name, name)
    local row = fetch_row_by_path(self.kb.conn, "system", path)
    if row then return parse_row(row).data end
    return nil
end

---------------------------------------------------------------------------
-- ROBOT_CLASS (sources: robot_manager spec when ported; FALLBACK until then)
---------------------------------------------------------------------------

local function load_robot_classes(self)
    if self._robot_classes then return self._robot_classes end
    local blob = self:get_app_spec_jsonb("robot_manager_01", "robot_classes")
    if blob == nil or next(blob) == nil then
        blob = FALLBACK_ROBOT_CLASSES
    end
    self._robot_classes = blob
    return blob
end

function M:get_robot_infra(class_name)
    local classes = load_robot_classes(self)
    return classes[class_name]
end

function M:list_robot_classes()
    local classes = load_robot_classes(self)
    local names = {}
    for name in pairs(classes) do
        names[#names + 1] = name
    end
    table.sort(names)
    return names
end

---------------------------------------------------------------------------
-- CLASS-BASED CAPABILITIES (robots register dynamically via link protocol)
---------------------------------------------------------------------------

function M:get_class_capabilities(class_name)
    local infra = self:get_robot_infra(class_name)
    if not infra then return {} end
    return infra.virtual_nodes or {}
end

function M:get_class_operation_types(class_name)
    local infra = self:get_robot_infra(class_name)
    if not infra then return {} end
    return infra.operation_types or {}
end

function M:get_class_energy_rate(class_name)
    local infra = self:get_robot_infra(class_name)
    if not infra then return nil end
    return infra.energy_rate or 1.0
end

function M:get_class_energy_max(class_name)
    local infra = self:get_robot_infra(class_name)
    if not infra then return nil end
    return infra.energy_max
end

function M:get_class_energy_infinite(class_name)
    local infra = self:get_robot_infra(class_name)
    if not infra then return false end
    return infra.energy_infinite == true
end

function M:get_class_config(class_name)
    local infra = self:get_robot_infra(class_name)
    if not infra then return nil end
    return {
        class_name      = class_name,
        capabilities    = infra.virtual_nodes or {},
        energy_max      = infra.energy_max,
        energy_infinite = infra.energy_infinite == true,
        topics          = infra.topics,
        tick_rate_ms    = infra.tick_rate_ms,
        worker_kbs      = infra.worker_kbs,
    }
end

---------------------------------------------------------------------------
-- PLANNER runtime state (own anchor)
---------------------------------------------------------------------------

--- Phase 5 C4: tenant identifier this kb_query is scoped to.
-- Defaults to own_instance_id when caller didn't supply one. Future
-- query methods (get_my_robots, list_my_boards) will scope by this.
function M:get_planner_namespace()
    return self.planner_namespace
end

function M:get_planner_state()
    -- runtime.planner.KB_STATUS_FIELD.state -- written by the planner worker
    -- under the "planner" runtime sub-namespace (state_name="planner",
    -- field name="state"). Add other runtime sub-namespaces (e.g.
    -- "ui", "transport") as the planner exposes them.
    local path = paths.app_runtime_status_path(
        self.site, self.own_instance_id, "planner", "state")
    local row = fetch_row_by_path(self.kb.conn, "system", path)
    if row then return parse_row(row).data end
    return nil
end

---------------------------------------------------------------------------
-- Virtual node definitions (read from own spec.KB_JSONB_FIELD.virtual_nodes)
---------------------------------------------------------------------------

local function load_virtual_nodes(self)
    if self._virtual_nodes ~= nil then return self._virtual_nodes end
    local blob = self:get_app_spec_jsonb(self.own_instance_id, "virtual_nodes")
    self._virtual_nodes = blob or {}
    return self._virtual_nodes
end

--- Get a single virtual node type definition.
-- @param vn_name string  e.g. "path_spline"
-- @return table { packet_type_id, description, json_schema, bitmask, pose_fields }
function M:get_virtual_node(vn_name)
    local vns = load_virtual_nodes(self)
    if type(vns) == "table" then
        if vns[vn_name] ~= nil then
            local v = vns[vn_name]
            if type(v) == "table" then
                v.name = vn_name
                return v
            end
        end
        for _, v in ipairs(vns) do
            if v == vn_name then
                return { name = vn_name }
            end
        end
    end
    return nil
end

--- List all virtual node type names.
function M:list_virtual_nodes()
    local vns = load_virtual_nodes(self)
    local names = {}
    if type(vns) == "table" then
        for k, v in pairs(vns) do
            if type(k) == "string" then
                names[#names + 1] = k
            elseif type(v) == "string" then
                names[#names + 1] = v
            end
        end
    end
    table.sort(names)
    return names
end

--- Get all virtual node definitions as a table keyed by name.
function M:get_all_virtual_nodes()
    local names = self:list_virtual_nodes()
    local result = {}
    for _, name in ipairs(names) do
        result[name] = self:get_virtual_node(name)
    end
    return result
end

---------------------------------------------------------------------------
-- Aggregate site-config snapshot
---------------------------------------------------------------------------

function M:get_site_config()
    local classes = self:list_robot_classes()
    local class_configs = {}
    for _, name in ipairs(classes) do
        class_configs[name] = self:get_class_config(name)
    end
    return {
        system_name    = self.system_name,
        site           = self.site,
        robot_classes  = class_configs,
        planner        = self:get_planner_state(),
        virtual_nodes  = self:get_all_virtual_nodes(),
    }
end

return M
