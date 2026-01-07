-- emit_bin.lua
local ffi = require("ffi")

ffi.cdef[[
typedef struct {
  uint32_t magic;      // 'SBT1'
  uint32_t fingerprint;
  uint16_t node_count;
  uint16_t bank_count;
  uint16_t bit_count;
  uint16_t reserved;
} blob_header_t;

typedef struct {
  uint16_t node_id;
  uint16_t bitspace_id;
  uint16_t bits;
  uint8_t  merge;
  uint8_t  boundary;
} blob_bank_t;

typedef struct {
  uint16_t bank_id;
  uint16_t local_idx;
} blob_bit_t;
]]

local M = {}

local function wbin(path, ptr, nbytes)
  local f = assert(io.open(path, "wb"))
  f:write(ffi.string(ptr, nbytes))
  f:close()
end

function M.emit(ir, outpath, bitspace_id, merge_map, bound_map)
  outpath = outpath or "out/schema_blob.bin"

  local header = ffi.new("blob_header_t[1]")
  header[0].magic = 0x31544253 -- 'SBT1' little-endian
  header[0].fingerprint = ir.fingerprint_u32
  header[0].node_count = #ir.nodes
  header[0].bank_count = #ir.banks
  header[0].bit_count  = #ir.bits

  local banks = ffi.new("blob_bank_t[?]", #ir.banks)
  for i,b in ipairs(ir.banks) do
    banks[i-1].node_id = b.node_id
    banks[i-1].bitspace_id = bitspace_id[b.bitspace]
    banks[i-1].bits = b.bits
    banks[i-1].merge = merge_map[b.merge]
    banks[i-1].boundary = bound_map[b.boundary]
  end

  local bits = ffi.new("blob_bit_t[?]", #ir.bits)
  for i,d in ipairs(ir.bits) do
    bits[i-1].bank_id = d.bank_id
    bits[i-1].local_idx = d.local_idx
  end

  local parent = ffi.new("uint16_t[?]", #ir.parents)
  for i,p in ipairs(ir.parents) do parent[i-1] = p end

  -- layout: header | parents | banks | bits
  local bytes_header = ffi.sizeof("blob_header_t")
  local bytes_parent = ffi.sizeof("uint16_t") * #ir.parents
  local bytes_banks  = ffi.sizeof("blob_bank_t") * #ir.banks
  local bytes_bits   = ffi.sizeof("blob_bit_t") * #ir.bits
  local total = bytes_header + bytes_parent + bytes_banks + bytes_bits

  local blob = ffi.new("uint8_t[?]", total)
  local off = 0
  ffi.copy(blob + off, header, bytes_header); off = off + bytes_header
  ffi.copy(blob + off, parent, bytes_parent); off = off + bytes_parent
  ffi.copy(blob + off, banks, bytes_banks);   off = off + bytes_banks
  ffi.copy(blob + off, bits, bytes_bits);     off = off + bytes_bits

  wbin(outpath, blob, total)
end

return M
