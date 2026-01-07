-- dsl/emit_cfg_json.lua
-- Emits a stable, pretty JSON file from schema.config (Lua table).
-- Keys are sorted to ensure clean diffs.

local M = {}
print("***************************************emit_cfg_json loaded from:")

local function wfile(path, s)
  local f = assert(io.open(path, "wb"))
  f:write(s)
  f:close()
end

local function is_array(t)
  if type(t) ~= "table" then return false end
  local max_i = 0
  for k,_ in pairs(t) do
    if type(k) ~= "number" then return false end
    if k > max_i then max_i = k end
  end
  for i=1,max_i do
    if t[i] == nil then return false end
  end
  return true
end

local function sorted_keys(t)
  local ks = {}
  for k,_ in pairs(t) do
    ks[#ks+1] = k
  end
  table.sort(ks, function(a,b) return tostring(a) < tostring(b) end)
  return ks
end

local function esc(s)
  s = s:gsub("\\", "\\\\")
  s = s:gsub("\"", "\\\"")
  s = s:gsub("\n", "\\n")
  s = s:gsub("\r", "\\r")
  s = s:gsub("\t", "\\t")
  return s
end

local function emit_val(v, indent, out)
  local tv = type(v)
  if tv == "nil" then
    out[#out+1] = "null"
  elseif tv == "boolean" then
    out[#out+1] = v and "true" or "false"
  elseif tv == "number" then
    -- Keep numeric formatting stable
    if v == math.floor(v) then
      out[#out+1] = string.format("%d", v)
    else
      out[#out+1] = string.format("%.9g", v)
    end
  elseif tv == "string" then
    out[#out+1] = "\"" .. esc(v) .. "\""
  elseif tv == "table" then
    if is_array(v) then
      out[#out+1] = "[\n"
      local n = #v
      for i=1,n do
        out[#out+1] = string.rep("  ", indent+1)
        emit_val(v[i], indent+1, out)
        if i < n then out[#out+1] = "," end
        out[#out+1] = "\n"
      end
      out[#out+1] = string.rep("  ", indent) .. "]"
    else
      out[#out+1] = "{\n"
      local ks = sorted_keys(v)
      for i,k in ipairs(ks) do
        out[#out+1] = string.rep("  ", indent+1)
        out[#out+1] = "\"" .. esc(tostring(k)) .. "\": "
        emit_val(v[k], indent+1, out)
        if i < #ks then out[#out+1] = "," end
        out[#out+1] = "\n"
      end
      out[#out+1] = string.rep("  ", indent) .. "}"
    end
  else
    error("Unsupported JSON value type: "..tv)
  end
end

function M.emit(ir, outdir)
  outdir = outdir or "out"
  local cfg = ir.config or {}

  local out = {}
  emit_val(cfg, 0, out)
  out[#out+1] = "\n"

  wfile(outdir.."/config.json", table.concat(out))
end

return M

