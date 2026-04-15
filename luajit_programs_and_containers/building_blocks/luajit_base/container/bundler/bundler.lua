#!/usr/bin/env luajit
-- =============================================================================
-- bundler.lua -- Image-build-time tool.
--
-- Walks /opt/apps/*/app.manifest.json and emits /opt/luajit_base/controller.db
-- (sqlite) with one row per app in `command_map`.
--
-- Each manifest:
--   {
--     "name": "<short name, PRIMARY KEY>",
--     "argv": ["luajit", "/opt/apps/<name>/main.lua", ...],
--     "start_order": 10,              -- integer, optional
--     "restart_policy": "always",     -- always | on-failure | never
--     "kb_path": "/opt/apps/<name>/kb.sqlite"  -- optional, for apps that
--                                              --  need their own sqlite
--   }
--
-- The bundler is invoked by app Dockerfiles (typically via
-- /usr/local/bin/bundle_controller -> bundle.sh) after COPYing app
-- artifacts into /opt/apps/. It's idempotent: re-running rebuilds the
-- controller.db from whatever is under /opt/apps at call time.
-- =============================================================================

local ffi    = require("ffi")
local dkjson = require("dkjson")

local APPS_ROOT   = os.getenv("APPS_ROOT")    or "/opt/apps"
local OUTPUT_PATH = os.getenv("OUTPUT_PATH")  or "/opt/luajit_base/controller.db"

local function die(msg)
    io.stderr:write("bundler: " .. msg .. "\n")
    os.exit(1)
end

local function log(msg)
    io.stderr:write("bundler: " .. msg .. "\n")
end

---------------------------------------------------------------------------
-- sqlite via FFI (keep the bundler free of the sqlite3_helpers dependency
-- so it can run in the base image without pulling the full KB library).
---------------------------------------------------------------------------

pcall(ffi.cdef, [[
    typedef struct sqlite3 sqlite3;
    int sqlite3_open(const char *filename, sqlite3 **ppDb);
    int sqlite3_close(sqlite3 *);
    int sqlite3_exec(sqlite3*, const char *sql, void*, void*, char **errmsg);
    const char *sqlite3_errmsg(sqlite3*);
    void sqlite3_free(void*);
]])

local S = ffi.load("sqlite3")

local function sqlite_open(path)
    local pp = ffi.new("sqlite3*[1]")
    if S.sqlite3_open(path, pp) ~= 0 then
        die("cannot open sqlite at " .. path)
    end
    return pp[0]
end

local function sqlite_exec(db, sql)
    local errmsg = ffi.new("char*[1]")
    if S.sqlite3_exec(db, sql, nil, nil, errmsg) ~= 0 then
        local msg = "<unknown>"
        if errmsg[0] ~= nil then
            msg = ffi.string(errmsg[0])
            S.sqlite3_free(errmsg[0])
        end
        die("sqlite_exec failed:\n  sql: " .. sql .. "\n  err: " .. msg)
    end
end

local function sql_quote(s)
    return "'" .. tostring(s):gsub("'", "''") .. "'"
end

---------------------------------------------------------------------------
-- list apps
---------------------------------------------------------------------------

local function read_file(path)
    local f = io.open(path, "rb")
    if not f then return nil end
    local d = f:read("*a")
    f:close()
    return d
end

local function list_dirs(root)
    -- portable-ish: shell out to `ls -1`. Busybox-friendly.
    local out = {}
    local p = io.popen(string.format("ls -1 %q 2>/dev/null",
                                     root:gsub('"', '\\"')))
    if not p then return out end
    for line in p:lines() do
        if line ~= "" then out[#out + 1] = line end
    end
    p:close()
    return out
end

local function read_manifests(root)
    local entries = {}
    for _, name in ipairs(list_dirs(root)) do
        local mpath = root .. "/" .. name .. "/app.manifest.json"
        local data  = read_file(mpath)
        if data then
            local m, err = dkjson.decode(data)
            if not m then
                die("bad manifest at " .. mpath .. ": " .. tostring(err))
            end
            if not m.name or not m.argv or type(m.argv) ~= "table" then
                die("manifest missing name/argv: " .. mpath)
            end
            entries[#entries + 1] = m
        end
    end
    table.sort(entries, function(a, b)
        return (a.start_order or 0) < (b.start_order or 0)
    end)
    return entries
end

---------------------------------------------------------------------------
-- main
---------------------------------------------------------------------------

local entries = read_manifests(APPS_ROOT)
log(string.format("found %d app manifest(s) under %s", #entries, APPS_ROOT))

-- Remove any stale file so we always write a fresh db.
os.remove(OUTPUT_PATH)

local db = sqlite_open(OUTPUT_PATH)
sqlite_exec(db, [[
    CREATE TABLE command_map (
        name            TEXT PRIMARY KEY,
        argv            TEXT NOT NULL,
        start_order     INTEGER DEFAULT 0,
        restart_policy  TEXT    DEFAULT 'always',
        kb_path         TEXT    DEFAULT ''
    );
]])

for _, m in ipairs(entries) do
    local argv_json = dkjson.encode(m.argv)
    local sql = string.format(
        "INSERT INTO command_map (name, argv, start_order, restart_policy, kb_path) " ..
        "VALUES (%s, %s, %d, %s, %s);",
        sql_quote(m.name),
        sql_quote(argv_json),
        tonumber(m.start_order) or 0,
        sql_quote(m.restart_policy or "always"),
        sql_quote(m.kb_path or ""))
    sqlite_exec(db, sql)
    log(string.format("  + %s  start_order=%d policy=%s",
        m.name,
        tonumber(m.start_order) or 0,
        m.restart_policy or "always"))
end

S.sqlite3_close(db)
log("wrote " .. OUTPUT_PATH)
