-- schema_compiler.lua
local ffi = require("ffi")
local bit = require("bit")

local M = {}

local function split_path(path)
  local parts = {}
  for p in string.gmatch(path, "([^.]+)") do parts[#parts+1] = p end
  return parts
end

local function stable_u32_hash(s)
  -- Deterministic, simple FNV-1a 32-bit (works everywhere; no external libs)
  local h = 2166136261
  for i=1,#s do
    h = bit.bxor(h, string.byte(s, i))
    h = (h * 16777619) % 2^32
  end
  return h
end

function M.compile(schema, profile_name)
  assert(type(schema)=="table", "schema must be table")
  local profile = (schema.profiles and schema.profiles[profile_name]) or {}
  local keep_names = profile.keep_names and true or false

  -- Index bitspaces by name
  local bitspaces = {}
  for _,bs in ipairs(schema.bitspaces or {}) do
    bitspaces[bs.name] = bs
  end

  -- Index classes by name
  local classes = {}
  for _,cl in ipairs(schema.classes or {}) do
    classes[cl.name] = cl
  end

  -- Build node tree (simple: just store full paths; parent = prefix)
  local nodes = {}
  local node_id_by_path = {}
  for i,n in ipairs(schema.nodes or {}) do
    assert(classes[n.class], "Unknown class: "..n.class)
    nodes[#nodes+1] = { path=n.path, class=n.class, parts=split_path(n.path) }
  end
  table.sort(nodes, function(a,b) return a.path < b.path end)
  for i,n in ipairs(nodes) do
    node_id_by_path[n.path] = i
  end

  -- Parent relationship (by longest prefix existing as a node)
  local parents = {}
  for i,n in ipairs(nodes) do
    local parent_id = 0
    for k=#n.parts-1,1,-1 do
      local prefix = table.concat(n.parts, ".", 1, k)
      local pid = node_id_by_path[prefix]
      if pid then parent_id = pid; break end
    end
    parents[i] = parent_id
  end

  -- Banks: each node instantiates its class banks (node × bitspace => bank)
  local banks = {}
  -- bank key: node_id .. ":" .. bitspace_name
  local bank_id_by_key = {}

  local function add_bank(node_id, bitspace_name, bits_per_bank)
    local key = node_id .. ":" .. bitspace_name
    if bank_id_by_key[key] then return bank_id_by_key[key] end
    local bs = assert(bitspaces[bitspace_name], "Unknown bitspace: "..bitspace_name)
    local bank_id = #banks + 1
    bank_id_by_key[key] = bank_id
    banks[bank_id] = {
      node_id = node_id,
      bitspace = bitspace_name,
      bits = bits_per_bank,
      merge = bs.merge,
      boundary = bs.boundary,
    }
    return bank_id
  end

  for node_id,n in ipairs(nodes) do
    local cl = classes[n.class]
    for bs_name,bits_per_bank in pairs(cl.banks) do
      add_bank(node_id, bs_name, bits_per_bank)
    end
  end

  -- Bit dictionary: map (node_id, bitspace, bitname) -> absolute bit index within its bank
  local bits = {}
  local bit_id_by_qualified = {}
  local symbols = keep_names and {} or nil

  local function define_bit(node_id, bs_name, bit_name, local_idx)
    local bank_id = assert(bank_id_by_key[node_id..":"..bs_name], "Missing bank: "..bs_name)
    local b = banks[bank_id]
    assert(local_idx >= 0 and local_idx < b.bits, "Bit idx out of bank range")
    local qname = node_id..":"..bs_name..":"..bit_name
    local bit_id = #bits + 1
    bit_id_by_qualified[qname] = bit_id
    bits[bit_id] = { bank_id=bank_id, local_idx=local_idx }
    if symbols then
      symbols[bit_id] = {
        node_path = nodes[node_id].path,
        bitspace = bs_name,
        name = bit_name,
      }
    end
    return bit_id
  end

  -- Instantiate bits per node based on class definition
  for node_id,n in ipairs(nodes) do
    local cl = classes[n.class]
    for bs_name,bitlist in pairs(cl.bits or {}) do
      for _,bd in ipairs(bitlist) do
        define_bit(node_id, bs_name, bd.name, bd.idx)
      end
    end
  end

  -- Exports resolution (child export token -> bit_id)
  local exports = {}
  for node_id,n in ipairs(nodes) do
    local cl = classes[n.class]
    for token,ref in pairs(cl.exports or {}) do
      local qname = node_id..":"..ref.bitspace..":"..ref.bit
      local bit_id = bit_id_by_qualified[qname]
      assert(bit_id, "Export refers to missing bit: "..token.." @ "..n.path)
      exports[node_id..":"..token] = bit_id
    end
  end

  -- Rollups compile: for each node, for each rollup def, build list of child source bit_ids
  -- (Design choice: rollups write into a dedicated summary bit in STATE bank, reserved index.)
  local rollup_ops = {}
  for _,r in ipairs(schema.rollups or {}) do
    -- Convention: rollup target bit exists in STATE bank at a reserved index you define per class,
    -- or generator can allocate in a "NodeSummary" pseudo-class. Here we keep it conceptual.
    rollup_ops[#rollup_ops+1] = {
      name=r.name, bitspace=r.bitspace, op=r.op, sources=r.sources
    }
  end

  -- Schema fingerprint to lock ID stability
  local fingerprint = stable_u32_hash(schema.name .. ":" .. (schema.version or "0") .. ":" .. profile_name)

  return {
    name = schema.name,
    version = schema.version,
    profile = profile_name,
    fingerprint_u32 = fingerprint,

    nodes = nodes,
    parents = parents,

    banks = banks,
    bits = bits,

    symbols = symbols,
    exports = exports,
    rollup_ops = rollup_ops,
    -- Pass config through to emitters
    config = schema.config or {},

  }
end

return M

