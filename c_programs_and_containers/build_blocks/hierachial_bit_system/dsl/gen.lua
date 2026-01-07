-- dsl/gen.lua
--
-- Top-level generator entrypoint.
-- Usage:
--   luajit dsl/gen.lua <schema.lua> <profile> <outdir>
--
-- Example:
--   luajit dsl/gen.lua dsl/example_schema.lua mcu_32k out/mcu
--   luajit dsl/gen.lua dsl/example_schema.lua linux   out/linux
--

-- ------------------------------------------------------------
-- Requires
-- ------------------------------------------------------------
local compiler        = require("schema_compiler")
local emit_c          = require("emit_c")
local emit_bin        = require("emit_bin")
local emit_bin2c      = require("emit_bin2c")

local emit_cfg_records = require("emit_cfg_records")
print("emit_cfg_records loaded from:", debug.getinfo(emit_cfg_records.emit, "S").source)

local emit_cfg_json    = require("emit_cfg_json")

-- ------------------------------------------------------------
-- Helpers
-- ------------------------------------------------------------

local function die(msg)
  io.stderr:write("ERROR: " .. msg .. "\n")
  os.exit(1)
end

local function mkdir_p(path)
  -- portable enough for typical Linux / macOS / CI use
  os.execute("mkdir -p " .. path)
end

-- Assign numeric bitspace IDs deterministically (sorted by name)
local function assign_bitspace_ids(ir)
  local seen = {}
  for _,b in ipairs(ir.banks or {}) do
    seen[b.bitspace] = true
  end
  local names = {}
  for n,_ in pairs(seen) do names[#names+1] = n end
  table.sort(names)

  local ids = {}
  for i,n in ipairs(names) do
    ids[n] = i
  end
  return ids
end

-- ------------------------------------------------------------
-- Arguments
-- ------------------------------------------------------------

local schema_path = arg[1] or die("missing schema.lua argument")
local profile     = arg[2] or die("missing profile name")
local outdir      = arg[3] or ("out/" .. profile)

-- ------------------------------------------------------------
-- Load schema
-- ------------------------------------------------------------

local schema = dofile(schema_path)
if type(schema) ~= "table" then
  die("schema file did not return a table: " .. schema_path)
end

-- ------------------------------------------------------------
-- Compile schema IR
-- ------------------------------------------------------------

local ir = compiler.compile(schema, profile)

-- ------------------------------------------------------------
-- Prepare output directory
-- ------------------------------------------------------------

mkdir_p(outdir)

-- ------------------------------------------------------------
-- Emit core BitTree schema (.h/.c)
-- ------------------------------------------------------------

emit_c.emit(ir, outdir)

-- ------------------------------------------------------------
-- Emit packed binary schema blob + embedded header
-- ------------------------------------------------------------

local bitspace_id = assign_bitspace_ids(ir)

local MERGE = {
  OR             = 1,
  AND            = 2,
  PRIORITY       = 3,
  CLEAR_DOMINANT = 4,
  SET_DOMINANT   = 5,
}

local BOUND = {
  RESET = 1,
  COPY  = 2,
  LATCH = 3,
}

emit_bin.emit(
  ir,
  outdir .. "/schema_blob.bin",
  bitspace_id,
  MERGE,
  BOUND
)

emit_bin2c.emit(
  outdir .. "/schema_blob.bin",
  outdir .. "/schema_blob_embed.h",
  "g_schema_blob",
  { bytes_per_line = 12, static = true }
)

-- ------------------------------------------------------------
-- Profile handling (THIS IS STEP 4 + STEP 5)
-- ------------------------------------------------------------

-- Pull profile settings from DSL schema
local profiles = schema.profiles or {}
local prof = profiles[profile] or {}

-- ------------------------------------------------------------
-- Emit embedded JSON config (MCU + Linux)
-- ------------------------------------------------------------
print("CFG: emitting records; ir.config type =", type(ir.config))

-- This ALWAYS runs: MCU and Linux both get compiled config tables
emit_cfg_records.emit(ir, outdir, prof)

-- ------------------------------------------------------------
-- Emit human-readable JSON sidecar (Linux only)
-- ------------------------------------------------------------

-- Controlled entirely by profile flag in DSL
if prof.emit_json_sidecar then
  emit_cfg_json.emit(ir, outdir)
end

-- ------------------------------------------------------------
-- Done
-- ------------------------------------------------------------

print("--------------------------------------------------")
print("Generated schema for:")
print("  schema : " .. schema_path)
print("  profile: " .. profile)
print("  outdir : " .. outdir)
print("--------------------------------------------------")
