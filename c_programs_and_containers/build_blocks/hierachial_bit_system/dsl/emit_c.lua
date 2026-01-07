-- emit_c.lua
local M = {}

local function wfile(path, s)
  local f = assert(io.open(path, "wb"))
  f:write(s)
  f:close()
end

function M.emit(ir, outdir)
  outdir = outdir or "out"
  os.execute("mkdir -p " .. outdir)

  local h_ids = {}
  h_ids[#h_ids+1] = "/* Auto-generated. Do not edit. */\n"
  h_ids[#h_ids+1] = "#pragma once\n#include <stdint.h>\n\n"
  h_ids[#h_ids+1] = string.format("#define SCHEMA_FINGERPRINT_U32 0x%08Xu\n", ir.fingerprint_u32)
  h_ids[#h_ids+1] = string.format("#define SCHEMA_NODE_COUNT %d\n", #ir.nodes)
  h_ids[#h_ids+1] = string.format("#define SCHEMA_BANK_COUNT %d\n", #ir.banks)
  h_ids[#h_ids+1] = string.format("#define SCHEMA_BIT_COUNT %d\n\n", #ir.bits)
  wfile(outdir.."/schema_ids.h", table.concat(h_ids))

  local h_tbl = {}
  h_tbl[#h_tbl+1] = "/* Auto-generated. Do not edit. */\n"
  h_tbl[#h_tbl+1] = "#pragma once\n#include <stdint.h>\n#include \"schema_ids.h\"\n\n"
  h_tbl[#h_tbl+1] = "typedef struct {\n"
  h_tbl[#h_tbl+1] = "  uint16_t node_id;\n"
  h_tbl[#h_tbl+1] = "  uint16_t bitspace_id; /* generator assigns ids */\n"
  h_tbl[#h_tbl+1] = "  uint16_t bits;\n"
  h_tbl[#h_tbl+1] = "  uint8_t  merge;\n"
  h_tbl[#h_tbl+1] = "  uint8_t  boundary;\n"
  h_tbl[#h_tbl+1] = "} schema_bank_desc_t;\n\n"

  h_tbl[#h_tbl+1] = "typedef struct {\n"
  h_tbl[#h_tbl+1] = "  uint16_t bank_id;\n"
  h_tbl[#h_tbl+1] = "  uint16_t local_idx;\n"
  h_tbl[#h_tbl+1] = "} schema_bit_desc_t;\n\n"

  h_tbl[#h_tbl+1] = "extern const schema_bank_desc_t g_schema_banks[SCHEMA_BANK_COUNT];\n"
  h_tbl[#h_tbl+1] = "extern const schema_bit_desc_t  g_schema_bits[SCHEMA_BIT_COUNT];\n"
  h_tbl[#h_tbl+1] = "extern const uint16_t          g_schema_parents[SCHEMA_NODE_COUNT];\n"
  wfile(outdir.."/schema_tables.h", table.concat(h_tbl))

  -- Assign compact ids for bitspaces (stable by name sort)
  local bitspace_names = {}
  local bitspace_id = {}
  for _,b in ipairs(ir.banks) do bitspace_names[b.bitspace] = true end
  local names = {}
  for n,_ in pairs(bitspace_names) do names[#names+1] = n end
  table.sort(names)
  for i,n in ipairs(names) do bitspace_id[n] = i end

  local MERGE = { OR=1, AND=2, PRIORITY=3, CLEAR_DOMINANT=4, SET_DOMINANT=5 }
  local BOUND = { RESET=1, COPY=2, LATCH=3 }

  local c_tbl = {}
  c_tbl[#c_tbl+1] = "/* Auto-generated. Do not edit. */\n"
  c_tbl[#c_tbl+1] = "#include \"schema_tables.h\"\n\n"

  c_tbl[#c_tbl+1] = "const uint16_t g_schema_parents[SCHEMA_NODE_COUNT] = {\n"
  for i=1,#ir.parents do
    c_tbl[#c_tbl+1] = string.format("  %d,%s\n", ir.parents[i], (i%8==0 and "" or ""))
  end
  c_tbl[#c_tbl+1] = "};\n\n"

  c_tbl[#c_tbl+1] = "const schema_bank_desc_t g_schema_banks[SCHEMA_BANK_COUNT] = {\n"
  for i,b in ipairs(ir.banks) do
    c_tbl[#c_tbl+1] = string.format(
      "  { .node_id=%d, .bitspace_id=%d, .bits=%d, .merge=%d, .boundary=%d },\n",
      b.node_id, bitspace_id[b.bitspace], b.bits, MERGE[b.merge], BOUND[b.boundary]
    )
  end
  c_tbl[#c_tbl+1] = "};\n\n"

  c_tbl[#c_tbl+1] = "const schema_bit_desc_t g_schema_bits[SCHEMA_BIT_COUNT] = {\n"
  for i,d in ipairs(ir.bits) do
    c_tbl[#c_tbl+1] = string.format("  { .bank_id=%d, .local_idx=%d },\n", d.bank_id, d.local_idx)
  end
  c_tbl[#c_tbl+1] = "};\n"
  wfile(outdir.."/schema_tables.c", table.concat(c_tbl))

  -- Optional: emit symbols JSON for Linux profile
  if ir.symbols then
    local j = {}
    j[#j+1] = "{\n"
    j[#j+1] = string.format("  \"name\": %q,\n  \"version\": %q,\n  \"profile\": %q,\n", ir.name, ir.version or "", ir.profile)
    j[#j+1] = "  \"bits\": [\n"
    for i,sym in ipairs(ir.symbols) do
      j[#j+1] = string.format(
        "    {\"id\":%d,\"node\":%q,\"bitspace\":%q,\"name\":%q}%s\n",
        i, sym.node_path, sym.bitspace, sym.name, (i<#ir.symbols and "," or "")
      )
    end
    j[#j+1] = "  ]\n}\n"
    local f = io.open(outdir.."/schema_symbols.json","wb"); f:write(table.concat(j)); f:close()
  end
end

return M

