-- dsl_runtime.lua
local M = {}

-- Keep DSL pure/deterministic: no os.time(), no random(), no filesystem scanning.
-- Schema generator will load a single schema file that calls these functions.

local function assert_str(x, name)
  assert(type(x) == "string" and #x > 0, name .. " must be non-empty string")
end

local function assert_tbl(x, name)
  assert(type(x) == "table", name .. " must be table")
end

-- Bitspace definition
function M.Bitspace(def)
  assert_tbl(def, "Bitspace(def)")
  assert_str(def.name, "Bitspace.name")
  -- merge: "OR" | "AND" | "PRIORITY" | "CLEAR_DOMINANT" | "SET_DOMINANT"
  -- boundary: "RESET" | "COPY" | "LATCH"
  assert_str(def.merge, "Bitspace.merge")
  assert_str(def.boundary, "Bitspace.boundary")
  return { _kind="bitspace", name=def.name, merge=def.merge, boundary=def.boundary }
end

-- Device class definition
function M.DeviceClass(def)
  assert_tbl(def, "DeviceClass(def)")
  assert_str(def.name, "DeviceClass.name")
  assert_tbl(def.banks, "DeviceClass.banks") -- { [bitspace_name] = bits_per_bank }
  -- Bits: { [bitspace_name] = { {name="X", idx=12, attrs={...}}, ... } }
  def.bits = def.bits or {}
  return { _kind="class", name=def.name, banks=def.banks, bits=def.bits, exports=def.exports or {} }
end

-- Node instance (hierarchy)
function M.Node(def)
  assert_tbl(def, "Node(def)")
  assert_str(def.path, "Node.path")     -- "Plant.Line1.Cell3.Robot2"
  assert_str(def.class, "Node.class")   -- DeviceClass name
  def.params = def.params or {}
  return { _kind="node", path=def.path, class=def.class, params=def.params }
end

-- Rollup definition (simple reducers)
-- Example: parent summary bit = OR of child bank bits (by exported bit name)
function M.Rollup(def)
  assert_tbl(def, "Rollup(def)")
  assert_str(def.name, "Rollup.name")         -- "AnyAlarm"
  assert_str(def.bitspace, "Rollup.bitspace") -- "STATE" typically, but can be ALARM too
  assert_str(def.op, "Rollup.op")             -- "OR" | "AND"
  -- sources: list of exported bit names from children (e.g. {"ALARM.AnyActive"})
  assert_tbl(def.sources, "Rollup.sources")
  return { _kind="rollup", name=def.name, bitspace=def.bitspace, op=def.op, sources=def.sources }
end

-- Schema root
function M.Schema(def)
  assert_tbl(def, "Schema(def)")
  def.bitspaces = def.bitspaces or {}
  def.classes   = def.classes   or {}
  def.nodes     = def.nodes     or {}
  def.rollups   = def.rollups   or {}
  def.profiles  = def.profiles  or {}
  return def
end

return M

