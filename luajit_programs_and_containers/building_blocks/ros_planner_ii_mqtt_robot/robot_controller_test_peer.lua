-- robot_controller_test_peer.lua -- In-process fixture for the
-- robot_controller contract (see docs/controller/contract.md).
--
-- Phase 2 plan: a real `robot_controller` service will own fleet
-- membership, KB-via-controller, and exception sink. Rover-side
-- `upward_peer.lua` (today a no-op stub) will become its client.
-- This module exposes the contract verbs as a Lua object so:
--
--   1. Contract tests can grade upward_peer / robot_controller pairs
--      against the same fixture without standing up a transport.
--   2. The test peer doubles as a documentation artifact: the surface
--      here IS the contract verb list.
--   3. When the real service ships, the same handler closures the
--      tests register here can be moved (or wrapped) into the real
--      controller.
--
-- Transport: synchronous, in-process. Rover client invokes
--   peer:handle(verb_table)
-- and gets back the response verb table (or nil + error_string for
-- contract violations). Controller-initiated verbs are produced via
-- :make_drain / :make_pause / :make_resume / :make_kb_invalidate
-- helpers and pushed into the rover via whatever channel the test
-- has wired up (typically a method on a paired upward_peer mock).
--
-- See `docs/controller/contract.md` for verb shapes + invariants.

local M = {}
local peer_mt = {}
peer_mt.__index = peer_mt

local DEFAULT_POLICY = {
    heartbeat_period_s = 10,
    drain_grace_s      = 30,
    kb_read_timeout_s  = 5,
}

local function default_session_id_factory()
    -- Monotonic + caller-supplied seed; not crypto. Tests can override.
    local n = 0
    return function()
        n = n + 1
        return string.format("01H%08X", n)
    end
end

local function default_kb_root_for(robot_id)
    return "rovers." .. tostring(robot_id)
end

-- ----- factory ---------------------------------------------------------

function M.new(opts)
    opts = opts or {}
    return setmetatable({
        sessions          = {},   -- session_id -> session record
        sessions_by_dkey  = {},   -- (class .. ":" .. dongle_instance) -> session_id
        session_id_factory = opts.session_id_factory or default_session_id_factory(),
        kb_root_for       = opts.kb_root_for or default_kb_root_for,
        policy            = opts.policy or DEFAULT_POLICY,
        version_accept    = opts.version_accept,    -- fn(client_version) -> ok, reason
        kb_store          = opts.kb_store or {},    -- in-memory KB for tests

        -- Logs (callers can drain for assertions).
        registers_seen    = {},
        heartbeats_seen   = {},
        exceptions_seen   = {},
        kb_reads_seen     = {},
        kb_writes_seen    = {},
        shutdowns_seen    = {},
        ack_log           = {},

        -- Optional override hooks set via :on_*.
        _on_register      = nil,
        _on_heartbeat     = nil,
        _on_exception     = nil,
        _on_kb_read       = nil,
        _on_kb_write      = nil,
        _on_shutdown      = nil,
    }, peer_mt)
end

-- ----- override hooks --------------------------------------------------

function peer_mt:on_register(fn)  self._on_register  = fn; return self end
function peer_mt:on_heartbeat(fn) self._on_heartbeat = fn; return self end
function peer_mt:on_exception(fn) self._on_exception = fn; return self end
function peer_mt:on_kb_read(fn)   self._on_kb_read   = fn; return self end
function peer_mt:on_kb_write(fn)  self._on_kb_write  = fn; return self end
function peer_mt:on_shutdown(fn)  self._on_shutdown  = fn; return self end

-- ----- internal helpers ------------------------------------------------

local function require_field(req, name)
    if req[name] == nil or req[name] == "" then
        return nil, "missing field: " .. name
    end
    return req[name]
end

local function require_session(self, req)
    local sid, err = require_field(req, "session_id")
    if not sid then return nil, err end
    local s = self.sessions[sid]
    if not s then return nil, "unknown session: " .. tostring(sid) end
    return s
end

local function dkey(class, dongle_instance)
    return tostring(class) .. ":" .. tostring(dongle_instance)
end

local function path_under(prefix, path)
    if not path or not prefix then return false end
    if path == prefix then return true end
    return path:sub(1, #prefix + 1) == (prefix .. ".")
end

local function deepcopy(v)
    if type(v) ~= "table" then return v end
    local o = {}
    for k, x in pairs(v) do o[k] = deepcopy(x) end
    return o
end

-- ----- handlers (rover-initiated verbs) --------------------------------

function peer_mt:_handle_register(req)
    self.registers_seen[#self.registers_seen + 1] = req

    for _, name in ipairs({
        "robot_id", "robot_class", "dongle_instance",
        "capabilities", "energy_max", "client_version",
    }) do
        local _, err = require_field(req, name)
        if err then
            return { type = "register_ack", ack = false, reason = err }
        end
    end
    if type(req.capabilities) ~= "table" then
        return { type = "register_ack", ack = false,
                 reason = "capabilities must be array" }
    end

    local k = dkey(req.robot_class, req.dongle_instance)
    local existing = self.sessions_by_dkey[k]
    if existing then
        return { type = "register_ack", ack = false,
                 reason = "dongle_collision",
                 conflict_session_id = existing }
    end

    if self.version_accept then
        local ok, why = self.version_accept(req.client_version)
        if not ok then
            return { type = "register_ack", ack = false,
                     reason = why or "version_incompatible" }
        end
    end

    -- Hook can override the default acceptance behavior entirely.
    if self._on_register then
        local ack = self._on_register(req)
        if ack then return ack end
    end

    local session_id = self.session_id_factory()
    local session = {
        session_id        = session_id,
        robot_id          = req.robot_id,
        robot_class       = req.robot_class,
        dongle_instance   = req.dongle_instance,
        capabilities      = deepcopy(req.capabilities),
        kb_root           = self.kb_root_for(req.robot_id),
        last_seq          = nil,
        live              = true,
        registered_at     = req.boot_ts,
        last_hb_seq       = 0,
        seen_exceptions   = {},
    }
    self.sessions[session_id]    = session
    self.sessions_by_dkey[k]     = session_id

    return {
        type       = "register_ack",
        ack        = true,
        session_id = session_id,
        fleet_seq  = #self.registers_seen,
        kb_root    = session.kb_root,
        policy     = deepcopy(self.policy),
    }
end

function peer_mt:_handle_heartbeat(req)
    local s, err = require_session(self, req)
    if not s then
        return { type = "heartbeat_ack", ok = false, reason = err }
    end
    self.heartbeats_seen[#self.heartbeats_seen + 1] = req
    s.last_hb_seq = req.seq or s.last_hb_seq

    if self._on_heartbeat then
        local r = self._on_heartbeat(req, s)
        if r then return r end
    end
    return { type = "heartbeat_ack", ok = true, seq = req.seq }
end

function peer_mt:_handle_exception(req)
    local s, err = require_session(self, req)
    if not s then
        return { type = "exception_ack", ok = false, reason = err }
    end
    local exc_id = req.exception_id
    if not exc_id then
        return { type = "exception_ack", ok = false,
                 reason = "missing exception_id" }
    end
    -- Idempotent — second send with the same id is a no-op success.
    if not s.seen_exceptions[exc_id] then
        s.seen_exceptions[exc_id] = req
        self.exceptions_seen[#self.exceptions_seen + 1] = req
        if self._on_exception then self._on_exception(req, s) end
    end
    return { type = "exception_ack", ok = true, exception_id = exc_id }
end

function peer_mt:_handle_kb_read(req)
    local s, err = require_session(self, req)
    if not s then
        return { type = "kb_read_response", ok = false,
                 request_id = req.request_id, error = err }
    end
    self.kb_reads_seen[#self.kb_reads_seen + 1] = req

    if self._on_kb_read then
        local r = self._on_kb_read(req, s)
        if r then return r end
    end

    local v = self.kb_store[req.path]
    if v == nil then
        return { type = "kb_read_response", ok = false,
                 request_id = req.request_id, error = "not_found" }
    end
    local out = deepcopy(v.value or v)
    if req.fields and type(req.fields) == "table" and type(out) == "table" then
        local picked = {}
        for _, f in ipairs(req.fields) do picked[f] = out[f] end
        out = picked
    end
    return {
        type       = "kb_read_response",
        ok         = true,
        request_id = req.request_id,
        value      = out,
        version    = (type(v) == "table" and v.version) or "v1",
    }
end

function peer_mt:_handle_kb_write(req)
    local s, err = require_session(self, req)
    if not s then
        return { type = "kb_write_response", ok = false,
                 request_id = req.request_id, error = err }
    end
    if not path_under(s.kb_root, req.path) then
        return { type = "kb_write_response", ok = false,
                 request_id = req.request_id, error = "denied" }
    end
    self.kb_writes_seen[#self.kb_writes_seen + 1] = req

    if self._on_kb_write then
        local r = self._on_kb_write(req, s)
        if r then return r end
    end

    local cur = self.kb_store[req.path]
    if req.if_match and cur and cur.version ~= req.if_match then
        return { type = "kb_write_response", ok = false,
                 request_id = req.request_id, error = "version_mismatch",
                 version = cur.version }
    end
    local new_version = string.format("v%d", (cur and (cur.version_n or 0) or 0) + 1)
    self.kb_store[req.path] = {
        value      = deepcopy(req.value),
        version    = new_version,
        version_n  = (cur and (cur.version_n or 0) or 0) + 1,
    }
    return { type = "kb_write_response", ok = true,
             request_id = req.request_id, version = new_version }
end

function peer_mt:_handle_shutdown_notice(req)
    self.shutdowns_seen[#self.shutdowns_seen + 1] = req
    local s = self.sessions[req.session_id]
    if s then
        s.live = false
        local k = dkey(s.robot_class, s.dongle_instance)
        if self.sessions_by_dkey[k] == s.session_id then
            self.sessions_by_dkey[k] = nil
        end
    end
    if self._on_shutdown then self._on_shutdown(req, s) end
    -- Best-effort; contract says ACK not required, but provide one
    -- for symmetry. Tests can ignore it.
    return { type = "shutdown_ack", ok = true }
end

-- ----- public API ------------------------------------------------------

-- Single entrypoint for rover-initiated verbs. Returns response table
-- (always — the contract requires ACK on every rover-initiated verb)
-- or nil + "unknown_verb: <type>" for verbs outside the catalogue.
function peer_mt:handle(verb)
    if type(verb) ~= "table" or not verb.type then
        return nil, "verb must have a type field"
    end
    local resp
    if     verb.type == "register"        then resp = self:_handle_register(verb)
    elseif verb.type == "heartbeat"       then resp = self:_handle_heartbeat(verb)
    elseif verb.type == "exception"       then resp = self:_handle_exception(verb)
    elseif verb.type == "kb_read"         then resp = self:_handle_kb_read(verb)
    elseif verb.type == "kb_write"        then resp = self:_handle_kb_write(verb)
    elseif verb.type == "shutdown_notice" then resp = self:_handle_shutdown_notice(verb)
    else
        return nil, "unknown_verb: " .. tostring(verb.type)
    end
    self.ack_log[#self.ack_log + 1] = { req = verb, resp = resp }
    return resp
end

-- ----- controller-initiated verb builders ------------------------------

function peer_mt:make_drain(session_id, deadline)
    return { type = "drain", session_id = session_id, deadline = deadline }
end

function peer_mt:make_pause(session_id, request_id)
    return { type = "pause", session_id = session_id, request_id = request_id }
end

function peer_mt:make_resume(session_id, request_id)
    return { type = "resume", session_id = session_id, request_id = request_id }
end

function peer_mt:make_kb_invalidate(session_id, paths)
    return { type = "kb_invalidate", session_id = session_id, paths = paths }
end

-- ----- introspection ---------------------------------------------------

function peer_mt:session(session_id) return self.sessions[session_id] end
function peer_mt:live_count()
    local n = 0
    for _, s in pairs(self.sessions) do
        if s.live then n = n + 1 end
    end
    return n
end

function peer_mt:reset()
    self.sessions, self.sessions_by_dkey = {}, {}
    self.registers_seen, self.heartbeats_seen   = {}, {}
    self.exceptions_seen, self.kb_reads_seen    = {}, {}
    self.kb_writes_seen, self.shutdowns_seen    = {}, {}
    self.ack_log = {}
end

return M
