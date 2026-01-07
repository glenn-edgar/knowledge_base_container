-- dsl/emit_cfg_records.lua
-- Emits:
--   cfg_json_recs.h / cfg_json_recs.c   (json_record_t stream)
--   cfg_index.h / cfg_index.c           (hash -> record index)
--   cfg_hashes.h                        (convenience #defines)

local bit = require("bit")

local M = {}

-- ----------------------------
-- Utilities
-- ----------------------------

local function wfile(path, s)
  local f = assert(io.open(path, "wb"))
  print("***************************************wfile called with path: " .. path)
  f:write(s)
  f:close()
end

local function is_ltree_label(s)
  return type(s) == "string" and s:match("^[A-Za-z0-9_]+$") ~= nil
end

local function stable_key_sort(a, b)
  return tostring(a) < tostring(b)
end

local function fnv1a32(s)
  local h = 0x811c9dc5
  for i = 1, #s do
    h = bit.bxor(h, s:byte(i))
    h = (h * 0x01000193) % 2^32
  end
  return h
end

local function u32hex(u)
  return string.format("0x%08X", u % 2^32)
end

local function classify_number(n)
  if math.type and math.type(n) == "integer" then
    if n >= 0 and n <= 0xFFFFFFFF then return "UINT32" end
    if n >= -0x80000000 and n <= 0x7FFFFFFF then return "INT32" end
    return "FLOAT32"
  end
  if n == math.floor(n) then
    if n >= 0 and n <= 0xFFFFFFFF then return "UINT32" end
    if n >= -0x80000000 and n <= 0x7FFFFFFF then return "INT32" end
  end
  return "FLOAT32"
end

local function sanitize_macro(s)
  s = s:gsub("[:%.%-/| ]+", "_")
  s = s:gsub("[^A-Za-z0-9_]", "_")
  s = s:gsub("_+", "_")
  s = s:gsub("^_+", ""):gsub("_+$", "")
  if #s == 0 then s = "EMPTY" end
  return s:upper()
end

-- ----------------------------
-- Record constructors
-- ----------------------------

local function rec_null()         return { t = "JSON_TYPE_NULL" } end
local function rec_bool(v)        return { t = "JSON_TYPE_BOOL",        bool = v and 1 or 0 } end
local function rec_i32(v)         return { t = "JSON_TYPE_INT32",       i32 = v } end
local function rec_u32(v)         return { t = "JSON_TYPE_UINT32",      u32 = v } end
local function rec_f32(v)         return { t = "JSON_TYPE_FLOAT32",     f32 = v } end
local function rec_strhash(h)     return { t = "JSON_TYPE_STRING_HASH", hash = h } end
local function rec_array(n)       return { t = "JSON_TYPE_ARRAY",       count = n } end
local function rec_object(n)      return { t = "JSON_TYPE_OBJECT",      count = n } end

local function emit_record_c_initializer(r)
  if r.t == "JSON_TYPE_NULL" then
    return "{ JSON_TYPE_NULL, .value.u32_value = 0u }"
  elseif r.t == "JSON_TYPE_BOOL" then
    return string.format("{ JSON_TYPE_BOOL, .value.bool_value = %du }", r.bool)
  elseif r.t == "JSON_TYPE_INT32" then
    return string.format("{ JSON_TYPE_INT32, .value.i32_value = %d }", r.i32)
  elseif r.t == "JSON_TYPE_UINT32" then
    return string.format("{ JSON_TYPE_UINT32, .value.u32_value = %uu }", r.u32)
  elseif r.t == "JSON_TYPE_FLOAT32" then
    return string.format("{ JSON_TYPE_FLOAT32, .value.f32_value = %.9gf }", r.f32)
  elseif r.t == "JSON_TYPE_STRING_HASH" then
    return string.format("{ JSON_TYPE_STRING_HASH, .value.hash32 = %s }", u32hex(r.hash))
  elseif r.t == "JSON_TYPE_ARRAY" then
    return string.format("{ JSON_TYPE_ARRAY, .value.container_count = %uu }", r.count)
  elseif r.t == "JSON_TYPE_OBJECT" then
    return string.format("{ JSON_TYPE_OBJECT, .value.container_count = %uu }", r.count)
  else
    error("Unknown record type: " .. tostring(r.t))
  end
end

-- ----------------------------
-- Header emission (now part of module)
-- ----------------------------

function M.emit_headers(outdir)
  print("***************************************emit_headers called with outdir: " .. outdir)
  local h = {}
  h[#h+1] = "/* Auto-generated. Do not edit. */\n"
  h[#h+1] = "#pragma once\n"
  h[#h+1] = "#include <stdint.h>\n"
  h[#h+1] = "#include <stddef.h>\n\n"

  h[#h+1] = "#define FNV1A_32_INIT   0x811c9dc5U\n"
  h[#h+1] = "#define FNV1A_32_PRIME  0x01000193U\n\n"
  h[#h+1] = "typedef uint32_t json_hash32_t;\n\n"

  h[#h+1] = "typedef enum {\n"
  h[#h+1] = "  JSON_TYPE_STRING_HASH = 0,\n"
  h[#h+1] = "  JSON_TYPE_INT32       = 1,\n"
  h[#h+1] = "  JSON_TYPE_UINT32      = 2,\n"
  h[#h+1] = "  JSON_TYPE_FLOAT32     = 3,\n"
  h[#h+1] = "  JSON_TYPE_NULL        = 4,\n"
  h[#h+1] = "  JSON_TYPE_BOOL        = 5,\n"
  h[#h+1] = "  JSON_TYPE_ARRAY       = 6,\n"
  h[#h+1] = "  JSON_TYPE_OBJECT      = 7\n"
  h[#h+1] = "} json_type_t;\n\n"

  h[#h+1] = "typedef struct {\n"
  h[#h+1] = "  json_type_t object_type;\n"
  h[#h+1] = "  union {\n"
  h[#h+1] = "    json_hash32_t hash32;\n"
  h[#h+1] = "    int32_t       i32_value;\n"
  h[#h+1] = "    uint32_t      u32_value;\n"
  h[#h+1] = "    float         f32_value;\n"
  h[#h+1] = "    uint8_t       bool_value;\n"
  h[#h+1] = "    uint32_t      container_count;\n"
  h[#h+1] = "  } value;\n"
  h[#h+1] = "} json_record_t;\n\n"

  h[#h+1] = "typedef struct {\n"
  h[#h+1] = "  json_hash32_t path_hash;\n"
  h[#h+1] = "  uint32_t      rec_index;\n"
  h[#h+1] = "} json_path_index_t;\n\n"

  h[#h+1] = "extern const json_record_t g_cfg_recs[];\n"
  h[#h+1] = "extern const uint32_t      g_cfg_recs_len;\n\n"
  h[#h+1] = "extern const json_path_index_t g_cfg_index[];\n"
  h[#h+1] = "extern const uint32_t          g_cfg_index_len;\n\n"

  wfile(outdir .. "/cfg_json_recs.h", table.concat(h))
  wfile(outdir .. "/cfg_index.h", table.concat(h))
end

-- ----------------------------
-- Corrected walk_value
-- ----------------------------

-- Inside emit_cfg_records.lua

local function is_array(t)
  if type(t) ~= "table" then return false end
  local i = 0
  for _ in pairs(t) do
    i = i + 1
    if t[i] == nil then return false end
  end
  return true
end

local function sorted_keys(t)
  local ks = {}
  for k in pairs(t) do ks[#ks + 1] = k end
  table.sort(ks, stable_key_sort)
  return ks
end

local function walk_value(val, path_parts, records, index_entries, collision_guard, opts)
  local tv = type(val)

  -- Leaf types — emit record and index them
  if tv == "nil" then
    local idx = #records + 1
    records[idx] = rec_null()

  elseif tv == "boolean" then
    local idx = #records + 1
    records[idx] = rec_bool(val)

  elseif tv == "number" then
    local idx = #records + 1
    local kind = classify_number(val)
    if kind == "UINT32" then
      records[idx] = rec_u32(val)
    elseif kind == "INT32" then
      records[idx] = rec_i32(val)
    else
      records[idx] = rec_f32(val)
    end

  elseif tv == "string" then
    local idx = #records + 1
    local canon = val
    if opts and opts.str_lowercase then canon = canon:lower() end
    local h = fnv1a32("str:" .. canon)
    records[idx] = rec_strhash(h)

  elseif tv == "table" then
    local is_arr = is_array(val)
    local child_count = is_arr and #val or #sorted_keys(val)
    local container_idx = #records + 1

    if is_arr then
      -- Array
      records[container_idx] = rec_array(child_count)
      for i = 1, child_count do
        local child_path = {unpack(path_parts)}
        child_path[#child_path + 1] = tostring(i)
        walk_value(val[i], child_path, records, index_entries, collision_guard, opts)
      end
    else
      -- Object
      local keys = sorted_keys(val)
      records[container_idx] = rec_object(#keys)

      for _, k in ipairs(keys) do
        assert(type(k) == "string" and is_ltree_label(k),
          "Invalid config key at path " .. table.concat(path_parts, ".") .. "." .. tostring(k))

        -- Emit key name as hash record
        local kh = fnv1a32("key:" .. k)
        local key_idx = #records + 1
        records[key_idx] = rec_strhash(kh)

        -- Recurse into value
        local child_path = {unpack(path_parts)}
        child_path[#child_path + 1] = k
        walk_value(val[k], child_path, records, index_entries, collision_guard, opts)
      end
    end

    -- Containers are not indexed — only leaves are
    return container_idx
  else
    error("Unsupported config value type: " .. tv .. " at path " .. table.concat(path_parts, "."))
  end

  -- All leaf paths reach here: index the leaf record
  local cfg_path = "cfg:" .. table.concat(path_parts, ".")
  if #path_parts == 0 then cfg_path = "cfg:" end  -- root edge case

  local ph = fnv1a32(cfg_path)

  if collision_guard[ph] and collision_guard[ph] ~= cfg_path then
    error(string.format("FNV1a-32 collision:\n  hash=%s\n  existing=%q\n  new=%q",
      u32hex(ph), collision_guard[ph], cfg_path))
  end
  collision_guard[ph] = cfg_path

  index_entries[#index_entries + 1] = {
    path_hash = ph,
    rec_index = #records,        -- points directly to the leaf record
    cfg_path = cfg_path
  }

  return #records
end
-- ----------------------------
-- Main emit function
-- ----------------------------

function M.emit(ir, outdir, profile)
  print("***************************************emit_cfg_records.emit called with outdir: " .. outdir)

  outdir = outdir or "out"
  profile = profile or {}

  M.emit_headers(outdir)

  local cfg = ir.config or {}
  local records = {}
  local index_entries = {}
  local collision_guard = {}

  walk_value(cfg, {}, records, index_entries, collision_guard, profile)

  -- Sort index for binary search
  table.sort(index_entries, function(a, b) return a.path_hash < b.path_hash end)

  -- Emit cfg_json_recs.c
  local c1 = {}
  c1[#c1+1] = "/* Auto-generated. Do not edit. */\n"
  c1[#c1+1] = "#include \"cfg_json_recs.h\"\n\n"
  c1[#c1+1] = "const json_record_t g_cfg_recs[] = {\n"
  for _, r in ipairs(records) do
    c1[#c1+1] = "  " .. emit_record_c_initializer(r) .. ",\n"
  end
  c1[#c1+1] = "};\n\n"
  c1[#c1+1] = string.format("const uint32_t g_cfg_recs_len = %uu;\n", #records)
  wfile(outdir .. "/cfg_json_recs.c", table.concat(c1))

  -- Emit cfg_index.c
  local c2 = {}
  c2[#c2+1] = "/* Auto-generated. Do not edit. */\n"
  c2[#c2+1] = "#include \"cfg_index.h\"\n\n"
  c2[#c2+1] = "const json_path_index_t g_cfg_index[] = {\n"
  for _, e in ipairs(index_entries) do
    c2[#c2+1] = string.format("  { .path_hash = %s, .rec_index = %uu }, // %s\n",
      u32hex(e.path_hash), e.rec_index, e.cfg_path)
  end
  c2[#c2+1] = "};\n\n"
  c2[#c2+1] = string.format("const uint32_t g_cfg_index_len = %uu;\n", #index_entries)
  wfile(outdir .. "/cfg_index.c", table.concat(c2))

  -- Convenience hash macros
  local hm = {}
  hm[#hm+1] = "/* Auto-generated. Do not edit. */\n"
  hm[#hm+1] = "#pragma once\n#include <stdint.h>\n\n"
  for _, e in ipairs(index_entries) do
    local macro = "CFG_HASH_" .. sanitize_macro(e.cfg_path:gsub("^cfg:", ""))
    hm[#hm+1] = string.format("#define %-60s %s // %s\n",
      macro, u32hex(e.path_hash), e.cfg_path)
  end
  wfile(outdir .. "/cfg_hashes.h", table.concat(hm))
end

return M