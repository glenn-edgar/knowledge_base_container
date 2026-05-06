--[[
    kb_runtime.lua -- Per-action durable persistence for mission lifecycle.

    Pushes mission/action events into a capped-FIFO ring at:
      system.<system>.site.<site>.app_containers.<container>.
        mission_log.actions.KB_STREAM_FIELD.samples

    Cap depth declared at build_kb time via mission_planner's kb_build
    (add_stream_field("samples", N, ...)). Pre-allocated rows in
    knowledge_base_stream are updated in oldest-first order on each
    push -- standard kb_stream circular-buffer pattern.

    Records are JSONB. Schema-on-read: any field can be added by future
    code without migration. The unified record shape used by mission.lua:

      {
        type             = "mission_start" | "action_start" |
                           "action_complete" | "action_failed" |
                           "mission_finish",
        robot_id         = string,        -- auto-injected from constructor
        mission_id       = string,        -- auto-injected from constructor
        action_index     = number?,       -- nil for mission events
        action_total     = number?,       -- nil for mission events
        kb_name          = string?,       -- nil for mission events
        success          = boolean?,      -- complete/failed/finish only
        fault_reason     = string?,       -- failed only
        fault            = table?,        -- finish only (full fault detail)
        global_x/y/heading/arm_angle = number?,  -- when pose available
        elapsed_ms       = number?,       -- finish only
        timestamp        = string,        -- ISO8601, auto-injected if absent
        ... (any extra fields the caller passes)
      }

    This module replaces the v2 sqlite-coded kb_runtime body. The status
    table semantic (single-row state snapshot via merge_status) is gone:
    nothing reads it, and live state already flows through NATS KV via
    mission.lua's :_publish_status path. Action history lives here.

    Robot_id is per-record, NOT per-path: all robots share one ring per
    planner instance. UI consumers filter by data->>'robot_id'.

    Usage:
        local kb_rt = require("kb_runtime")
        local rt = kb_rt.new({
            pg_conn        = { host=..., port=..., dbname=..., user=..., password=... },
            site           = "moon_base_alpha",
            system_name    = "moon_base",
            container_name = "mission_planner_01",
            robot_id       = "rover_1",
            mission_id     = "ab017494...",  -- from JobQueue job.id, or caller-generated
        })

        rt:push_event({ type = "mission_start", route_length = 6 })
        rt:push_event({ type = "action_start", action_index = 1, kb_name = "drive" })
        ...
        rt:close()
]]

local DBI    = require("DBI")
local dkjson = require("dkjson")

local M = {}
M.__index = M

---------------------------------------------------------------------------
-- Internal pg helpers (mirrors dcs_host/kb_stream.lua's direct-SQL push;
-- avoids the KB_Search dependency so this module stays self-contained).
---------------------------------------------------------------------------

local function escape(s) return tostring(s):gsub("'", "''") end

local function exec(conn, sql)
    local sth, err = conn:prepare(sql)
    if not sth then return nil, "prepare: " .. tostring(err) end
    local ok, eerr = sth:execute()
    if not ok then sth:close(); return nil, "execute: " .. tostring(eerr) end
    sth:close()
    return true
end

local function query_one(conn, sql)
    local sth, err = conn:prepare(sql)
    if not sth then return nil, "prepare: " .. tostring(err) end
    local ok, eerr = sth:execute()
    if not ok then sth:close(); return nil, "execute: " .. tostring(eerr) end
    local row = sth:fetch(true)
    sth:close()
    return row
end

---------------------------------------------------------------------------
-- Constructor
---------------------------------------------------------------------------

function M.new(opts)
    assert(type(opts) == "table",          "kb_runtime.new: opts table required")
    assert(type(opts.pg_conn) == "table",  "kb_runtime.new: opts.pg_conn must be a table")
    assert(type(opts.site)    == "string" and opts.site    ~= "", "kb_runtime.new: opts.site required")
    assert(type(opts.system_name)    == "string" and opts.system_name    ~= "", "kb_runtime.new: opts.system_name required")
    assert(type(opts.container_name) == "string" and opts.container_name ~= "", "kb_runtime.new: opts.container_name required")
    assert(type(opts.robot_id) == "string" and opts.robot_id ~= "", "kb_runtime.new: opts.robot_id required")
    assert(type(opts.mission_id) == "string" and opts.mission_id ~= "", "kb_runtime.new: opts.mission_id required (use JobQueue job.id or caller-generated)")
    assert(type(opts.board_name) == "string" and opts.board_name ~= "", "kb_runtime.new: opts.board_name required (for per-action record correlation)")
    assert(type(opts.board_sha256) == "string" and opts.board_sha256 ~= "", "kb_runtime.new: opts.board_sha256 required (file_store hash captured at planner build)")

    local self = setmetatable({}, M)
    self.site           = opts.site
    self.system_name    = opts.system_name
    self.container_name = opts.container_name
    self.robot_id       = opts.robot_id
    self.mission_id     = opts.mission_id
    self.board_name     = opts.board_name
    self.board_sha256   = opts.board_sha256

    -- Direct DBI connect (no KBM facade -- we only do the one SQL push).
    local pg = opts.pg_conn
    local conn, err = DBI.Connect("PostgreSQL", pg.dbname, pg.user, pg.password,
                                  pg.host, tostring(pg.port))
    if not conn then
        error("kb_runtime: pg connect failed: " .. tostring(err))
    end
    conn:autocommit(true)
    self.conn = conn

    -- Pre-compute the stream path (per planner instance, not per robot;
    -- robot_id lives in the JSON payload, not the ltree path).
    self.event_path = string.format(
        "system.%s.site.%s.app_containers.%s.mission_log.actions.KB_STREAM_FIELD.samples",
        opts.system_name, opts.site, opts.container_name)

    return self
end

function M:close()
    if self.conn then
        pcall(function() self.conn:close() end)
        self.conn = nil
    end
end

---------------------------------------------------------------------------
-- push_event(record)
--
-- Pushes a record into the action-history ring. Auto-injects robot_id,
-- mission_id, and timestamp if not already in the record. The ring is
-- pre-allocated at build_kb time; if no slot is found, returns nil+err
-- (loud failure -- means the kb_build add_stream_field declaration was
-- forgotten or the build_kb step didn't run).
---------------------------------------------------------------------------

function M:push_event(record)
    if type(record) ~= "table" then
        return nil, "push_event: record must be a table"
    end

    -- Auto-inject identifying fields. Caller can override by passing them.
    record.robot_id     = record.robot_id     or self.robot_id
    record.mission_id   = record.mission_id   or self.mission_id
    record.board_name   = record.board_name   or self.board_name
    record.board_sha256 = record.board_sha256 or self.board_sha256
    record.timestamp    = record.timestamp    or os.date("!%Y-%m-%dT%H:%M:%SZ")

    local json = dkjson.encode(record)

    -- Find the oldest row at this path (lowest recorded_at, breaking ties
    -- by id ASC). Pre-allocated empty rows have valid=FALSE; they sort
    -- first because their CURRENT_TIMESTAMP at allocation is older than
    -- any pushed record.
    local oldest, err = query_one(self.conn, string.format([[
        SELECT id FROM knowledge_base_stream
         WHERE path = '%s'::ltree
         ORDER BY recorded_at ASC, id ASC
         LIMIT 1
    ]], escape(self.event_path)))
    if err then return nil, err end
    if not oldest then
        return nil, string.format(
            "kb_runtime: no slots pre-allocated for path '%s' " ..
            "-- did mission_planner kb_build add_stream_field run?",
            self.event_path)
    end

    return exec(self.conn, string.format([[
        UPDATE knowledge_base_stream
           SET data = '%s'::jsonb,
               recorded_at = NOW(),
               valid = TRUE
         WHERE id = %s
    ]], escape(json), tostring(oldest.id)))
end

---------------------------------------------------------------------------
-- M.push_rejection(opts)  --  module-level helper, no instance needed.
--
-- Pushes a mission_rejected record into the same kb_stream ring used by
-- per-action events. Opens a short-lived pg connection, performs one
-- UPDATE, and closes. Used by action_server when a mission fails BEFORE
-- the per-mission kb_runtime instance is constructed (board_not_found,
-- planning_failed, insufficient_energy, etc.) -- in those cases there is
-- no board_sha256 yet, which is why this is a free function rather than
-- an instance method.
--
-- opts:
--   pg_conn        = { host, port, dbname, user, password }   required
--   system_name                                               required
--   site                                                      required
--   container_name (planner instance, e.g. "mission_planner_01") required
--   robot_id                                                  required
--   mission_id                                                required
--   reason         (e.g. "board_not_found")                   required
--   detail         (free-form string)                         optional
--   board_name                                                optional
--   board_sha256                                              optional
--   ... any other JSON fields the caller wants persisted      optional
--
-- Returns true on success, nil + err on failure (mirrors push_event).
---------------------------------------------------------------------------
function M.push_rejection(opts)
    if type(opts) ~= "table" then
        return nil, "push_rejection: opts table required"
    end
    for _, k in ipairs({"pg_conn", "system_name", "site", "container_name",
                        "robot_id", "mission_id", "reason"}) do
        if not opts[k] or opts[k] == "" then
            return nil, "push_rejection: opts." .. k .. " required"
        end
    end

    local pg = opts.pg_conn
    local conn, cerr = DBI.Connect("PostgreSQL", pg.dbname, pg.user,
                                   pg.password, pg.host, tostring(pg.port))
    if not conn then
        return nil, "push_rejection pg connect: " .. tostring(cerr)
    end
    conn:autocommit(true)

    local event_path = string.format(
        "system.%s.site.%s.app_containers.%s.mission_log.actions.KB_STREAM_FIELD.samples",
        opts.system_name, opts.site, opts.container_name)

    local record = {
        type         = "mission_rejected",
        robot_id     = opts.robot_id,
        mission_id   = opts.mission_id,
        reason       = opts.reason,
        detail       = opts.detail,
        board_name   = opts.board_name,
        board_sha256 = opts.board_sha256,
        timestamp    = os.date("!%Y-%m-%dT%H:%M:%SZ"),
    }
    -- Forward any extra opts the caller wants persisted (e.g. unsupported,
    -- energy_required, energy_remaining for insufficient_energy rejections).
    for k, v in pairs(opts) do
        if record[k] == nil and k ~= "pg_conn" and k ~= "system_name"
           and k ~= "site" and k ~= "container_name" then
            record[k] = v
        end
    end

    local json = dkjson.encode(record)

    local oldest, qerr = query_one(conn, string.format([[
        SELECT id FROM knowledge_base_stream
         WHERE path = '%s'::ltree
         ORDER BY recorded_at ASC, id ASC
         LIMIT 1
    ]], escape(event_path)))
    if qerr then
        pcall(function() conn:close() end)
        return nil, "push_rejection: " .. qerr
    end
    if not oldest then
        pcall(function() conn:close() end)
        return nil, "push_rejection: no slots pre-allocated for path " .. event_path
    end

    local ok, eerr = exec(conn, string.format([[
        UPDATE knowledge_base_stream
           SET data = '%s'::jsonb,
               recorded_at = NOW(),
               valid = TRUE
         WHERE id = %s
    ]], escape(json), tostring(oldest.id)))
    pcall(function() conn:close() end)
    if not ok then return nil, "push_rejection: " .. tostring(eerr) end
    return true
end

return M
