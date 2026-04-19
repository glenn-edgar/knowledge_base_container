--[[
  kb_doc_commissioning.lua

  Laptop-side commissioning loader/extractor for the doc store.

    - load_dir(conn, database, namespace, source_dir)
      Walks source_dir, sanitizes filenames into ltree labels, computes
      sha256, upserts blobs + nodes. Idempotent (re-runs touch only
      changed files because the blob insert is ON CONFLICT DO NOTHING
      and node upsert compares sha256).

    - extract_dir(conn, database, namespace, dest_dir)
      Reverses load_dir: walks fs_node rows under namespace, reconstructs
      directory tree using the original filename column, writes bytes
      from fs_blob.

  sha256 is computed by shelling out to `sha256sum`. Laptop hosts have
  coreutils; no lua crypto dependency. If you ever need this to run
  inside a container that lacks coreutils, swap the impl for
  resty.openssl.digest or ffi-libcrypto -- same 32-byte contract.

  Assumes:
    - The doc class for namespace is already registered via
      construct_doc_store:add_doc_class{...} during build_kb.
    - The writer policy allows 'commissioning' for this class
      (both 'runtime' and 'commissioning_only' accept writer='commissioning').
]]

local doc_store = require("kb_doc_store")

local M = {}

---------------------------------------------------------------------------
-- Filename sanitization (load-side)
---------------------------------------------------------------------------

-- Turn a filename into a single ltree label.
--   Dots (except the extension separator) + any [^A-Za-z0-9_] -> '_'.
--   Leading digit prefixed with 'l_'.
-- Extension is split off and returned separately (no dot, lowercased).
-- Returns (label, ext_lowercased_no_dot).
local function split_name_ext(fname)
  local base, ext = fname:match("^(.*)%.([^.]+)$")
  if not base or base == "" then
    base = fname
    ext = nil
  end
  local sane = base:lower():gsub("[^a-z0-9_]", "_")
  if sane:match("^[0-9]") then sane = "l_" .. sane end
  if ext then
    ext = ext:lower():gsub("[^a-z0-9_]", "_")
  end
  return sane, ext
end

---------------------------------------------------------------------------
-- File I/O helpers
---------------------------------------------------------------------------

local function read_file(path)
  local f, err = io.open(path, "rb")
  if not f then return nil, "open: " .. tostring(err) end
  local bytes = f:read("*a")
  f:close()
  return bytes
end

local function write_file(path, bytes)
  local f, err = io.open(path, "wb")
  if not f then return nil, "open: " .. tostring(err) end
  f:write(bytes)
  f:close()
  return true
end

-- sha256(bytes) -> 32 raw bytes, via sha256sum over a temp file.
-- Lua's io.popen doesn't portably support bidirectional pipes for
-- write-then-read, so tempfile is the simplest reliable path. Laptop
-- commissioning is rare, so the extra syscall cost is irrelevant.
local function sha256_bytes(bytes)
  local tmp = os.tmpname()
  local f = assert(io.open(tmp, "wb"))
  f:write(bytes)
  f:close()
  local h = assert(io.popen("sha256sum -b '" .. tmp .. "' | cut -c1-64"))
  local hex = h:read("*l")
  h:close()
  os.remove(tmp)
  if not hex or #hex ~= 64 then
    error("sha256sum failed or returned unexpected output: " .. tostring(hex))
  end
  local out = {}
  for i = 1, 64, 2 do
    out[#out + 1] = string.char(tonumber(hex:sub(i, i + 1), 16))
  end
  return table.concat(out)
end

-- Recursive directory walk. Returns list of { relpath, abs_path, is_dir }.
-- Uses `find` for portability + simplicity. Skips symlinks.
local function walk_dir(source_dir)
  local cmd = string.format(
    "find '%s' -mindepth 1 \\( -type f -o -type d \\) -not -type l -printf '%%y\\t%%P\\n'",
    source_dir:gsub("'", "'\\''"))
  local h, err = io.popen(cmd)
  if not h then return nil, "find: " .. tostring(err) end

  local entries = {}
  for line in h:lines() do
    local kind, rel = line:match("^(%a)\t(.+)$")
    if kind and rel then
      entries[#entries + 1] = {
        relpath = rel,
        abs     = source_dir .. "/" .. rel,
        is_dir  = (kind == "d"),
      }
    end
  end
  h:close()
  return entries
end

-- Make dirs recursively (mkdir -p).
local function mkdir_p(path)
  os.execute(string.format("mkdir -p '%s'", path:gsub("'", "'\\''")))
end

---------------------------------------------------------------------------
-- load_dir
---------------------------------------------------------------------------

--- Walk source_dir and upsert every file under `namespace` in the doc store.
--- Relative paths become ltree labels: "a/b-c/d.pdf" -> "<ns>.a.b_c.d".
--- Each file's `filename` column stores the original basename for lossless
--- extract.
---
--- @param conn       DBI connection
--- @param database   KB database name (e.g. "system")
--- @param namespace  ltree path already declared via add_doc_class
--- @param source_dir absolute path on the laptop
--- @param opts       optional { entity_key = "..." }
--- @return number of files loaded, or nil + err
function M.load_dir(conn, database, namespace, source_dir, opts)
  opts = opts or {}
  local entries, err = walk_dir(source_dir)
  if not entries then return nil, err end

  local n_loaded = 0
  for _, e in ipairs(entries) do
    -- Build ltree path segments from relpath.
    local segs = {}
    for seg in e.relpath:gmatch("[^/]+") do
      segs[#segs + 1] = seg
    end

    if e.is_dir then
      local dir_labels = {}
      for _, s in ipairs(segs) do
        local sane = s:lower():gsub("[^a-z0-9_]", "_")
        if sane:match("^[0-9]") then sane = "l_" .. sane end
        dir_labels[#dir_labels + 1] = sane
      end
      local path = namespace .. "." .. table.concat(dir_labels, ".")
      local ok, derr = doc_store.doc_put_dir(conn, database, path,
        { filename = segs[#segs] })
      if not ok then return nil, "put_dir " .. path .. ": " .. tostring(derr) end
    else
      -- File: parent labels + sanitized basename.
      local parent_labels = {}
      for i = 1, #segs - 1 do
        local sane = segs[i]:lower():gsub("[^a-z0-9_]", "_")
        if sane:match("^[0-9]") then sane = "l_" .. sane end
        parent_labels[#parent_labels + 1] = sane
      end
      local basename = segs[#segs]
      local label, ext = split_name_ext(basename)

      local parent = namespace
      if #parent_labels > 0 then
        parent = parent .. "." .. table.concat(parent_labels, ".")
      end
      local path = parent .. "." .. label

      local content, rerr = read_file(e.abs)
      if not content then return nil, "read " .. e.abs .. ": " .. tostring(rerr) end
      local sha = sha256_bytes(content)

      local ok, perr = doc_store.doc_put(conn, database, path, {
        sha256     = sha,
        content    = content,
        filename   = basename,
        ext        = ext,
        entity_key = opts.entity_key,
        writer     = "commissioning",
      })
      if not ok then return nil, "put " .. path .. ": " .. tostring(perr) end
      n_loaded = n_loaded + 1
    end
  end
  return n_loaded
end

---------------------------------------------------------------------------
-- extract_dir
---------------------------------------------------------------------------

--- Reverse of load_dir: reconstruct a directory tree under dest_dir
--- using the stored `filename` column (lossless round-trip).
---
--- @return number of files written, or nil + err
function M.extract_dir(conn, database, namespace, dest_dir)
  mkdir_p(dest_dir)
  local rows, err = doc_store.doc_list(conn, database, namespace)
  if not rows then return nil, err end

  -- Build a path-label -> filename map across ALL rows so we can
  -- reconstruct the original basenames, including parent directory
  -- names. For every row, split path into labels (after dropping the
  -- namespace prefix) and pair each label with either the stored
  -- `filename` (for the row's own last label) or the `filename` from
  -- the matching directory row for intermediate labels.
  local label_to_name = {}
  local ns_depth = 0
  for _ in namespace:gmatch("[^.]+") do ns_depth = ns_depth + 1 end

  for _, r in ipairs(rows) do
    local labels = {}
    for seg in r.path:gmatch("[^.]+") do labels[#labels + 1] = seg end
    -- The row names itself: last-label -> filename.
    local last = labels[#labels]
    if r.filename and last then
      label_to_name[table.concat(labels, ".", 1, #labels)] = r.filename
    end
  end

  local function rel_for_row(r)
    local labels = {}
    for seg in r.path:gmatch("[^.]+") do labels[#labels + 1] = seg end
    local parts = {}
    for i = ns_depth + 1, #labels do
      local key = table.concat(labels, ".", 1, i)
      local name = label_to_name[key] or labels[i]
      parts[#parts + 1] = name
    end
    return table.concat(parts, "/")
  end

  local n_written = 0
  for _, r in ipairs(rows) do
    if r.kind == "dir" then
      local rel = rel_for_row(r)
      if rel ~= "" then mkdir_p(dest_dir .. "/" .. rel) end
    else
      local rel = rel_for_row(r)
      if rel ~= "" then
        -- Ensure parent dir exists even if no dir row was recorded.
        local parent = rel:match("^(.*)/[^/]+$")
        if parent then mkdir_p(dest_dir .. "/" .. parent) end

        local full = doc_store.doc_get(conn, database, r.path)
        if full and full.content then
          local ok, werr = write_file(dest_dir .. "/" .. rel, full.content)
          if not ok then return nil, "write " .. rel .. ": " .. tostring(werr) end
          n_written = n_written + 1
        end
      end
    end
  end
  return n_written
end

return M
