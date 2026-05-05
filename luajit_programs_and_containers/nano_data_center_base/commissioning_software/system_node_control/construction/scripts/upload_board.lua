#!/usr/bin/env luajit
-- =============================================================================
-- upload_board.lua -- Upload (or revise) a navigation board into pg file_store.
--
-- Usage:
--   upload_board.lua --system <sys> --site <S> --name <board_name> --file <path.json>
--
-- Reads the JSON file, validates it parses, computes sha256, INSERTs a
-- knowledge_base_fs_blob row (no-op if the hash already exists -- content
-- is dedup'd), and UPSERTs the knowledge_base_fs_node pointer at
--
--   system.<sys>.site.<S>.boards.<name>
--
-- All inside one transaction. The class namespace
-- (system.<sys>.site.<S>.boards) must be registered first by the boards
-- subsystem (run via build_kb.sh).
--
-- Writer policy is commissioning_only: this CLI passes writer="commissioning"
-- when calling doc_put. Runtime code (mission_planner) uses the read-only
-- doc_get path and never invokes this tool.
--
-- Mid-mission revision policy is (1) drain-then-flip for now: existing
-- missions complete on the hash they captured at start; new missions see
-- the new hash. Future (3) replan-in-place builds on the same fs_node
-- pointer-flip event.
--
-- Audit log: every successful upload writes a row to
--   knowledge_base_status path = .boards.<name>.KB_STATUS_FIELD.last_upload
-- with { value = { sha256_hex, uploaded_at, uploaded_by, file_path } }.
-- The fs_blob retains every historical version by content-addressable
-- dedup; the audit row records WHO flipped to which hash, WHEN.
-- =============================================================================

local DBI    = require("DBI")
local dkjson = require("dkjson")

---------------------------------------------------------------------------
-- arg parsing
---------------------------------------------------------------------------

local function die(msg)
    io.stderr:write("upload_board: " .. msg .. "\n")
    os.exit(1)
end

local function usage()
    io.stderr:write([[
Usage: upload_board.lua --system <sys> --site <S> --name <board_name> --file <path.json>
                       [--pg-host <h>] [--pg-port <p>] [--pg-db <db>] [--pg-user <u>]
Environment fallback:
  PG_HOST, PG_PORT, PG_DB, PG_USER, PG_PASSWORD (or POSTGRES_PASSWORD)
]])
    os.exit(2)
end

local opts = { pg_port = "5432", pg_db = "knowledge_base", pg_user = "gedgar" }
local i = 1
while i <= #arg do
    local k, v = arg[i], arg[i + 1]
    if     k == "--system"  then opts.system  = v; i = i + 2
    elseif k == "--site"    then opts.site    = v; i = i + 2
    elseif k == "--name"    then opts.name    = v; i = i + 2
    elseif k == "--file"    then opts.file    = v; i = i + 2
    elseif k == "--pg-host" then opts.pg_host = v; i = i + 2
    elseif k == "--pg-port" then opts.pg_port = v; i = i + 2
    elseif k == "--pg-db"   then opts.pg_db   = v; i = i + 2
    elseif k == "--pg-user" then opts.pg_user = v; i = i + 2
    elseif k == "--help" or k == "-h" then usage()
    else die("unknown arg: " .. tostring(k)) end
end

if not (opts.system and opts.site and opts.name and opts.file) then usage() end

opts.pg_host     = opts.pg_host     or os.getenv("PG_HOST")     or "localhost"
opts.pg_password = os.getenv("PG_PASSWORD") or os.getenv("POSTGRES_PASSWORD")
if not opts.pg_password or opts.pg_password == "" then
    die("PG_PASSWORD or POSTGRES_PASSWORD env var required")
end

---------------------------------------------------------------------------
-- read + validate file
---------------------------------------------------------------------------

local f, ferr = io.open(opts.file, "rb")
if not f then die("cannot open " .. opts.file .. ": " .. tostring(ferr)) end
local content = f:read("*a"); f:close()
if not content or content == "" then die("empty file: " .. opts.file) end

-- JSON shape check (loud failure). Schema-on-read; no field validation.
local parsed, _, perr = dkjson.decode(content)
if not parsed then die("file is not valid JSON: " .. tostring(perr)) end

---------------------------------------------------------------------------
-- sha256 (shell out to sha256sum -- no crypto in luajit base image)
---------------------------------------------------------------------------

local function sha256_hex_of_file(path)
    local p = io.popen("sha256sum -b " .. ("'" .. path:gsub("'", "'\\''") .. "'") .. " | awk '{print $1}'")
    if not p then return nil end
    local hex = p:read("*l"); p:close()
    return hex
end

local hex = sha256_hex_of_file(opts.file)
if not hex or #hex ~= 64 then die("sha256sum failed for " .. opts.file) end

local function hex_to_bytes(h)
    local out = {}
    for j = 1, #h, 2 do out[#out + 1] = string.char(tonumber(h:sub(j, j + 1), 16)) end
    return table.concat(out)
end

---------------------------------------------------------------------------
-- pg connect + doc_put + audit
---------------------------------------------------------------------------

-- Locate kb_doc_store (relative to this script dir).
local script_dir = arg[0]:match("(.*/)") or "./"
package.path = script_dir .. "../../../kb/postgres/data_structures/?.lua;"
            .. script_dir .. "../../../kb/postgres/construct_kb/?.lua;"
            .. package.path
local doc_store = require("kb_doc_store")

local pg, err = DBI.Connect("PostgreSQL", opts.pg_db, opts.pg_user,
                            opts.pg_password, opts.pg_host, opts.pg_port)
if not pg then die("pg connect: " .. tostring(err)) end
pg:autocommit(false)

local class_ns = string.format("system.%s.site.%s.boards", opts.system, opts.site)
local board_path = class_ns .. "." .. opts.name

-- Confirm class is registered (build_kb must have run with the boards subsystem).
local sth = pg:prepare(string.format(
    "SELECT writer FROM knowledge_base_doc_class WHERE namespace = '%s'::ltree", class_ns))
sth:execute()
local crow = sth:fetch(true); sth:close()
if not crow then
    pg:rollback(); pg:close()
    die("doc class not registered: " .. class_ns
        .. " -- run build_kb.sh with the boards subsystem first")
end

-- doc_put with writer="commissioning" (class is commissioning_only).
local ok, perr = doc_store.doc_put(pg, opts.pg_db, board_path, {
    sha256   = hex_to_bytes(hex),
    filename = opts.name .. ".json",
    ext      = "json",
    content  = content,
    mime     = "application/json",
    writer   = "commissioning",
    meta     = { source_path = opts.file },
})
if not ok then
    pg:rollback(); pg:close()
    die("doc_put failed: " .. tostring(perr))
end

-- Audit row: who flipped to which hash, when.
local audit_path = board_path .. ".KB_STATUS_FIELD.last_upload"
local audit_data = dkjson.encode({
    value = {
        sha256_hex   = hex,
        size         = #content,
        uploaded_at  = os.date("!%Y-%m-%dT%H:%M:%SZ"),
        uploaded_by  = os.getenv("USER") or "unknown",
        file_path    = opts.file,
    }
})
local upd_sth = pg:prepare(string.format([[
    INSERT INTO knowledge_base_status (path, data) VALUES ('%s'::ltree, '%s'::json)
    ON CONFLICT (path) DO UPDATE SET data = EXCLUDED.data
]], audit_path, audit_data:gsub("'", "''")))
local audit_ok, audit_err = upd_sth:execute()
upd_sth:close()
if not audit_ok then
    -- Audit failure is non-fatal: the doc_put already landed; rolling back
    -- would lose the actual upload. Log and continue.
    io.stderr:write(string.format(
        "upload_board: WARNING audit row write failed: %s (board upload still committed)\n",
        tostring(audit_err)))
end

pg:commit()
pg:close()

io.stdout:write(string.format(
    "uploaded board=%s namespace=%s sha256=%s size=%d\n",
    opts.name, class_ns, hex, #content))
