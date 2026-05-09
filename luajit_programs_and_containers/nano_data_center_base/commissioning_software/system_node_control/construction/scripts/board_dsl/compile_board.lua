#!/usr/bin/env luajit
-- =============================================================================
-- compile_board.lua -- v2 board DSL compiler entry point.
--
-- Usage:
--   compile_board.lua --board <path-to-dsl.lua> --system <sys> --site <S>
--                     --planner-namespace <ns>  (or env PLANNER_NAMESPACE)
--                     [--out <path>]   [--name <board_name>]
--                     [--no-kb]        [--upload]
--                     [--pg-host <h>] [--pg-port <p>] [--pg-db <db>] [--pg-user <u>]
--
-- Required:
--   --board               path to operator-authored board.lua
--   --system              system name (e.g. moon_base) -- KB lookups + upload
--   --site                site name (e.g. alpha)        -- KB lookups + upload
--   --planner-namespace   Phase 7: tenant the board belongs to (e.g.
--                         mission_planner_01 / surface_ops). Required
--                         in KB-validating mode; the validator scopes
--                         every kb_ref against this tenant's subtree.
--                         Falls back to env PLANNER_NAMESPACE.
--
-- Defaults:
--   --out       <board file's basename>.json next to the .lua
--   --name      board file basename without .lua extension
--   pg-*        localhost:5432 / knowledge_base / gedgar
--                                             (ignored if --no-kb)
--   PG_PASSWORD or POSTGRES_PASSWORD env var required unless --no-kb
--
-- Modes:
--   default     run all 4 KB-driven checks; emit JSON; exit
--   --no-kb     run structural + capability-union checks only;
--                 useful for offline DSL development
--   --upload    after successful compile, hand off to upload_board.lua
--
-- Exit codes: 0 = success, 1 = compile error, 2 = usage error
-- =============================================================================

local function die(msg)
  io.stderr:write("compile_board: " .. msg .. "\n")
  os.exit(1)
end

local function usage()
  io.stderr:write([[
Usage: compile_board.lua --board <path> --system <sys> --site <S>
                         --planner-namespace <ns>  (or env PLANNER_NAMESPACE)
                         [--out <path>] [--name <board_name>]
                         [--no-kb] [--upload]
                         [--pg-host <h>] [--pg-port <p>]
                         [--pg-db <db>] [--pg-user <u>]
]])
  os.exit(2)
end

------------------------------------------------------------------------
-- arg parsing
------------------------------------------------------------------------

local opts = { pg_port = "5432", pg_db = "knowledge_base", pg_user = "gedgar" }

local i = 1
while i <= #arg do
  local k, v = arg[i], arg[i + 1]
  if     k == "--board"             then opts.board   = v; i = i + 2
  elseif k == "--system"            then opts.system  = v; i = i + 2
  elseif k == "--site"              then opts.site    = v; i = i + 2
  elseif k == "--planner-namespace" then opts.planner_namespace = v; i = i + 2
  elseif k == "--out"               then opts.out     = v; i = i + 2
  elseif k == "--name"              then opts.name    = v; i = i + 2
  elseif k == "--no-kb"             then opts.no_kb   = true; i = i + 1
  elseif k == "--upload"            then opts.upload  = true; i = i + 1
  elseif k == "--pg-host"           then opts.pg_host = v; i = i + 2
  elseif k == "--pg-port"           then opts.pg_port = v; i = i + 2
  elseif k == "--pg-db"             then opts.pg_db   = v; i = i + 2
  elseif k == "--pg-user"           then opts.pg_user = v; i = i + 2
  elseif k == "--help" or k == "-h" then usage()
  else die("unknown arg: " .. tostring(k)) end
end

-- Phase 7: planner_namespace is required for KB-validating compile.
-- Env var fallback so docker-compose / shell-export workflows don't
-- need to repeat the flag.
opts.planner_namespace = opts.planner_namespace
                       or os.getenv("PLANNER_NAMESPACE")

if not (opts.board and opts.system and opts.site) then usage() end
if not opts.no_kb and (not opts.planner_namespace or opts.planner_namespace == "") then
  die("--planner-namespace required (or env PLANNER_NAMESPACE) " ..
      "in KB-validating mode; pass --no-kb for offline structural " ..
      "checks only")
end

opts.pg_host = opts.pg_host or os.getenv("PG_HOST") or "localhost"
opts.pg_password = os.getenv("PG_PASSWORD") or os.getenv("POSTGRES_PASSWORD")
if not opts.no_kb and (not opts.pg_password or opts.pg_password == "") then
  die("PG_PASSWORD or POSTGRES_PASSWORD env var required (or pass --no-kb)")
end

-- Default --out and --name from --board basename.
local function basename_no_ext(p)
  local base = p:match("([^/]+)$") or p
  base = base:gsub("%.lua$", "")
  return base
end
opts.name = opts.name or basename_no_ext(opts.board)
opts.out  = opts.out  or (opts.board:gsub("%.lua$", ".json"))

------------------------------------------------------------------------
-- locate sibling modules
------------------------------------------------------------------------

local script_dir = arg[0]:match("(.*/)") or "./"
package.path = script_dir .. "?.lua;" .. package.path

local bd   = require("board_dsl")
local emit = require("emit")

------------------------------------------------------------------------
-- run the operator's board.lua
------------------------------------------------------------------------

io.stdout:write(string.format("compile_board: loading %s\n", opts.board))

local board_chunk, lerr = loadfile(opts.board)
if not board_chunk then die("loadfile " .. opts.board .. ": " .. tostring(lerr)) end

-- Inject `bd` into the board chunk's environment so the operator can
-- write `bd.straight_line{...}` without an explicit require. Standard
-- Lua scoping: a global `bd` is visible to the chunk because it's
-- being run in the same global env as this script.
_G.bd = bd

local ok, b = pcall(board_chunk)
if not ok then die("board.lua errored: " .. tostring(b)) end
if type(b) ~= "table" or type(b.build) ~= "function" then
  die("board.lua must `return b` (a board_dsl board object)")
end

------------------------------------------------------------------------
-- build (with or without KB)
------------------------------------------------------------------------

local build_opts = {}
local pg
if not opts.no_kb then
  local DBI = require("DBI")
  -- DSN-string form per feedback_dbd_postgres_dsn.md: positional args
  -- silently fall back to unix socket which doesn't exist in
  -- containers. Build the dbname-only positional form here since
  -- compile_board runs on the laptop host where unix-socket pg may
  -- work, but DSN keeps it explicit.
  io.stdout:write(string.format(
    "compile_board: connecting to pg %s@%s:%s/%s\n",
    opts.pg_user, opts.pg_host, opts.pg_port, opts.pg_db))
  local err
  pg, err = DBI.Connect("PostgreSQL",
    string.format("dbname=%s host=%s port=%s",
      opts.pg_db, opts.pg_host, opts.pg_port),
    opts.pg_user, opts.pg_password)
  if not pg then die("pg connect: " .. tostring(err)) end
  build_opts.kb_conn          = pg
  build_opts.system_name      = opts.system
  build_opts.site_name        = opts.site
  build_opts.planner_namespace = opts.planner_namespace
end

io.stdout:write(string.format(
  "compile_board: building board %q (%s mode)\n",
  opts.name, opts.no_kb and "offline" or "KB-validating"))

local ok2, board_table = pcall(b.build, b, build_opts)
if pg then pg:close() end
if not ok2 then die("build failed: " .. tostring(board_table)) end

------------------------------------------------------------------------
-- emit JSON
------------------------------------------------------------------------

local emit_ok, emit_err = pcall(emit.write_file, board_table, opts.out)
if not emit_ok then die("emit failed: " .. tostring(emit_err)) end

io.stdout:write(string.format(
  "compile_board: wrote %s (%d nodes, %d edges, %d capabilities)\n",
  opts.out, #board_table.nodes, #board_table.edges, #board_table.capabilities))

------------------------------------------------------------------------
-- optional upload
------------------------------------------------------------------------

if opts.upload then
  -- Locate upload_board.lua relative to this script (sibling under
  -- construction/scripts/).
  local upload_path = script_dir .. "../upload_board.lua"
  io.stdout:write(string.format("compile_board: uploading via %s\n", upload_path))
  local cmd = string.format(
    "%s --system %q --site %q --name %q --file %q " ..
    "--pg-host %q --pg-port %q --pg-db %q --pg-user %q",
    upload_path, opts.system, opts.site, opts.name, opts.out,
    opts.pg_host, opts.pg_port, opts.pg_db, opts.pg_user)
  local rc = os.execute(cmd)
  -- Lua 5.1 returns the raw shell rc as integer; LuaJIT same. 0 = ok.
  if rc ~= 0 and rc ~= true then
    die("upload_board.lua exited non-zero: " .. tostring(rc))
  end
end

os.exit(0)
