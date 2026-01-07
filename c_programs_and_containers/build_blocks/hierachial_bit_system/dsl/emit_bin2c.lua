-- dsl/emit_bin2c.lua
local M = {}

local function read_file(path)
  local f = assert(io.open(path, "rb"))
  local data = f:read("*a")
  f:close()
  assert(type(data) == "string" and #data > 0, "empty file: " .. path)
  return data
end

local function write_file(path, s)
  local f = assert(io.open(path, "wb"))
  f:write(s)
  f:close()
end

local function basename(p)
  return (p:gsub("^.*/", ""))
end

-- Emits a C header similar to bin2c/xxd -i output, but with your symbol name.
-- Arguments:
--   in_bin_path: path to .bin
--   out_h_path : path to .h
--   symbol     : e.g. "g_schema_blob"
--   opts       : { bytes_per_line=12, static=true }
function M.emit(in_bin_path, out_h_path, symbol, opts)
  opts = opts or {}
  local bytes_per_line = opts.bytes_per_line or 12
  local make_static = (opts.static ~= false)

  assert(type(symbol) == "string" and #symbol > 0, "symbol must be non-empty string")

  local data = read_file(in_bin_path)
  local n = #data

  local out = {}
  out[#out+1] = string.format("/* Auto-generated from %s. Do not edit. */\n", basename(in_bin_path))
  out[#out+1] = "#pragma once\n"
  out[#out+1] = "#include <stdint.h>\n"
  out[#out+1] = "#include <stddef.h>\n\n"

  local prefix = make_static and "static " or ""
  out[#out+1] = string.format("%sconst uint8_t %s[%d] = {\n", prefix, symbol, n)

  -- Format bytes
  local col = 0
  for i = 1, n do
    local b = string.byte(data, i)
    if col == 0 then
      out[#out+1] = "  "
    end
    out[#out+1] = string.format("0x%02X", b)

    if i ~= n then
      out[#out+1] = ", "
    end

    col = col + 1
    if col >= bytes_per_line then
      out[#out+1] = "\n"
      col = 0
    end
  end
  if col ~= 0 then out[#out+1] = "\n" end
  out[#out+1] = "};\n\n"

  out[#out+1] = string.format("%sconst size_t %s_len = %d;\n", prefix, symbol, n)

  write_file(out_h_path, table.concat(out))
end

return M
