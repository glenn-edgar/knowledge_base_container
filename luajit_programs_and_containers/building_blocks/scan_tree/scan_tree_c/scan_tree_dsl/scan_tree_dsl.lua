--[[
    Scan Tree DSL - Core Construction Engine (LuaJIT)

    Stack-based DSL for constructing hierarchical bitmap-driven behavior tree
    computational graphs. Outputs a JSON intermediate file for code generation.
--]]

local ffi = require("ffi")

-- =====================================================================
-- JSON encoder (minimal, no external dependencies)
-- =====================================================================

local json = {}

-- Marker to force a table to serialize as a JSON array (even when empty)
local ARRAY_MT = {}
function json.array(t)
    t = t or {}
    return setmetatable(t, ARRAY_MT)
end

local function is_json_array(t)
    if getmetatable(t) == ARRAY_MT then return true end
    -- Also detect natural arrays (consecutive integer keys starting at 1)
    local count = 0
    for _ in pairs(t) do count = count + 1 end
    return count == #t and #t > 0
end

local function json_encode_string(s)
    s = s:gsub('\\', '\\\\')
    s = s:gsub('"', '\\"')
    s = s:gsub('\n', '\\n')
    s = s:gsub('\r', '\\r')
    s = s:gsub('\t', '\\t')
    return '"' .. s .. '"'
end

local json_encode_value  -- forward declaration

local function json_encode_array(t, indent, depth)
    if #t == 0 then return "[]" end
    local parts = {}
    local pad = string.rep("  ", depth + 1)
    local pad_close = string.rep("  ", depth)
    for i, v in ipairs(t) do
        parts[i] = pad .. json_encode_value(v, indent, depth + 1)
    end
    return "[\n" .. table.concat(parts, ",\n") .. "\n" .. pad_close .. "]"
end

local function json_encode_object(t, indent, depth)
    -- Check if table is array-like
    if #t > 0 then
        return json_encode_array(t, indent, depth)
    end

    -- Collect keys and sort for deterministic output
    local keys = {}
    local has_keys = false
    for k, _ in pairs(t) do
        if type(k) == "string" then
            keys[#keys + 1] = k
            has_keys = true
        end
    end
    if not has_keys then return "{}" end

    table.sort(keys)

    local parts = {}
    local pad = string.rep("  ", depth + 1)
    local pad_close = string.rep("  ", depth)
    for _, k in ipairs(keys) do
        parts[#parts + 1] = pad .. json_encode_string(k) .. ": " ..
            json_encode_value(t[k], indent, depth + 1)
    end
    return "{\n" .. table.concat(parts, ",\n") .. "\n" .. pad_close .. "}"
end

function json_encode_value(v, indent, depth)
    local vtype = type(v)
    if v == nil then
        return "null"
    elseif vtype == "boolean" then
        return v and "true" or "false"
    elseif vtype == "number" then
        if v == math.floor(v) and v < 2^53 and v > -2^53 then
            return string.format("%d", v)
        else
            return string.format("%.17g", v)
        end
    elseif vtype == "string" then
        return json_encode_string(v)
    elseif vtype == "table" then
        if is_json_array(v) then
            return json_encode_array(v, indent, depth)
        end
        -- Empty table without array marker: default to object {}
        if next(v) == nil then
            return "{}"
        end
        return json_encode_object(v, indent, depth)
    else
        return json_encode_string(tostring(v))
    end
end

function json.encode(t)
    return json_encode_value(t, true, 0)
end

-- =====================================================================
-- Valid types
-- =====================================================================

local VALID_TYPES = {
    ["bool"] = true,
    ["uint8_t"] = true, ["uint16_t"] = true, ["uint32_t"] = true, ["uint64_t"] = true,
    ["int8_t"] = true, ["int16_t"] = true, ["int32_t"] = true, ["int64_t"] = true,
    ["float"] = true, ["double"] = true,
}

-- Scope kinds
local SCOPE_TREE     = "tree"
local SCOPE_LEVEL    = "level"
local SCOPE_SUBLEVEL = "sub_level"
local SCOPE_BUFFER   = "buffer"

-- =====================================================================
-- ScopeHandle
-- =====================================================================

local ScopeHandle = {}
ScopeHandle.__index = ScopeHandle

function ScopeHandle.new(kind, name, depth)
    local self = setmetatable({}, ScopeHandle)
    self.kind = kind
    self.name = name
    self.depth = depth
    self.closed = false
    self._buf_data = nil
    return self
end

function ScopeHandle:__tostring()
    local state = self.closed and "closed" or "open"
    return string.format("ScopeHandle(%s, '%s', depth=%d, %s)",
        self.kind, self.name, self.depth, state)
end

-- =====================================================================
-- ScanTreeDSL
-- =====================================================================

local ScanTreeDSL = {}
ScanTreeDSL.__index = ScanTreeDSL

function ScanTreeDSL.new()
    local self = setmetatable({}, ScanTreeDSL)
    self._scope_stack = {}
    self._tree_handle = nil
    self._finalized = false

    self._raw_buffers = {}
    self._all_buffers = {}
    self._levels = {}
    self._current_level = nil
    self._level_count = 0

    self._json_data = {
        raw_buffers = json.array(),
        levels = json.array(),
    }
    return self
end

-- ------------------------------------------------------------------
-- Path helpers
-- ------------------------------------------------------------------

function ScanTreeDSL:_current_path()
    local parts = {}
    for _, scope in ipairs(self._scope_stack) do
        if scope.kind ~= SCOPE_BUFFER then
            parts[#parts + 1] = scope.name
        end
    end
    return table.concat(parts, ".")
end

function ScanTreeDSL:_full_path(name)
    local path = self:_current_path()
    if path ~= "" then
        return path .. "." .. name
    end
    return name
end

-- ------------------------------------------------------------------
-- Scope helpers
-- ------------------------------------------------------------------

function ScanTreeDSL:_push_scope(kind, name)
    local depth = #self._scope_stack
    local handle = ScopeHandle.new(kind, name, depth)
    self._scope_stack[#self._scope_stack + 1] = handle
    return handle
end

function ScanTreeDSL:_pop_scope(handle, expected_kind)
    if #self._scope_stack == 0 then
        error("No open scope to close")
    end
    local top = self._scope_stack[#self._scope_stack]
    if top ~= handle then
        error(string.format("Scope mismatch: expected to close %s, got %s",
            tostring(top), tostring(handle)))
    end
    if top.kind ~= expected_kind then
        error(string.format("Scope kind mismatch: expected %s, got %s",
            expected_kind, top.kind))
    end
    if top.closed then
        error(string.format("Scope already closed: %s", tostring(handle)))
    end
    top.closed = true
    self._scope_stack[#self._scope_stack] = nil
end

function ScanTreeDSL:_current_scope_kind()
    if #self._scope_stack == 0 then
        return nil
    end
    return self._scope_stack[#self._scope_stack].kind
end

-- ------------------------------------------------------------------
-- Parent data lookup
-- ------------------------------------------------------------------

function ScanTreeDSL:_find_parent_data()
    if #self._levels == 0 then return nil end
    local current = self._levels[#self._levels]

    for _, scope in ipairs(self._scope_stack) do
        if scope.kind == SCOPE_SUBLEVEL and not scope.closed then
            local sub_levels = current.sub_levels or {}
            for _, sub in ipairs(sub_levels) do
                if sub.name == scope.name then
                    current = sub
                    break
                end
            end
        end
    end
    return current
end

-- ------------------------------------------------------------------
-- Tree start/end
-- ------------------------------------------------------------------

function ScanTreeDSL:SCAN_TREE_start(name)
    name = name or "scan_tree"
    if self._tree_handle then
        error("SCAN_TREE_start() already called")
    end
    self._tree_handle = self:_push_scope(SCOPE_TREE, name)
    self._json_data.name = name
    return self._tree_handle
end

function ScanTreeDSL:SCAN_TREE_end(handle)
    self:_pop_scope(handle, SCOPE_TREE)
    self._finalized = true
end

-- ------------------------------------------------------------------
-- Level start/end
-- ------------------------------------------------------------------

function ScanTreeDSL:SCAN_TREE_level_start(name)
    if self:_current_scope_kind() ~= SCOPE_TREE then
        error("SCAN_TREE_level_start() must be called at tree scope")
    end
    if not name then
        name = string.format("level_%d", self._level_count)
    end
    self._level_count = self._level_count + 1

    local level_data = {
        name = name,
        path = self:_full_path(name),
        level_number = self._level_count - 1,
        output_buffer = nil,
        sub_levels = json.array(),
        virtual_functions = json.array(),
    }
    self._levels[#self._levels + 1] = level_data
    self._current_level = level_data
    self._json_data.levels[#self._json_data.levels + 1] = level_data

    return self:_push_scope(SCOPE_LEVEL, name)
end

function ScanTreeDSL:SCAN_TREE_level_end(handle)
    self:_pop_scope(handle, SCOPE_LEVEL)
    self._current_level = nil
end

-- ------------------------------------------------------------------
-- Sub-level start/end
-- ------------------------------------------------------------------

function ScanTreeDSL:SCAN_TREE_sub_level(name)
    local enclosing = self:_current_scope_kind()
    if enclosing ~= SCOPE_LEVEL and enclosing ~= SCOPE_SUBLEVEL then
        error("SCAN_TREE_sub_level() must be called inside a level or sub-level")
    end

    if not name then
        local count = 0
        for _, s in ipairs(self._scope_stack) do
            if s.kind == SCOPE_SUBLEVEL then count = count + 1 end
        end
        name = string.format("sub_%d", count)
    end

    local sub_data = {
        name = name,
        path = self:_full_path(name),
        output_buffer = nil,
        sub_levels = json.array(),
        virtual_functions = json.array(),
    }

    local parent_data = self:_find_parent_data()
    if parent_data then
        parent_data.sub_levels[#parent_data.sub_levels + 1] = sub_data
    end

    return self:_push_scope(SCOPE_SUBLEVEL, name)
end

function ScanTreeDSL:SCAN_TREE_sub_level_end(handle)
    self:_pop_scope(handle, SCOPE_SUBLEVEL)
end

-- ------------------------------------------------------------------
-- Buffer definition
-- ------------------------------------------------------------------

function ScanTreeDSL:define_buffer(name, buf_type, number, description, json_properties)
    description = description or ""
    json_properties = json_properties or {}

    if not VALID_TYPES[buf_type] then
        error(string.format("Invalid buffer type: %s", buf_type))
    end
    if number <= 0 then
        error(string.format("Buffer size must be > 0, got %d", number))
    end

    local path = self:_full_path(name)
    local buf_data = {
        name = name,
        path = path,
        type = buf_type,
        size = number,
        description = description,
        json_properties = json_properties,
        pins = json.array(),
    }

    local scope_kind = self:_current_scope_kind()

    if scope_kind == SCOPE_TREE then
        self._raw_buffers[name] = buf_data
        self._json_data.raw_buffers[#self._json_data.raw_buffers + 1] = buf_data
        buf_data.category = "raw"
    elseif scope_kind == SCOPE_LEVEL or scope_kind == SCOPE_SUBLEVEL then
        if buf_type ~= "bool" then
            error(string.format("Layer buffer '%s' must be bool, got %s", name, buf_type))
        end
        local parent_data = self:_find_parent_data()
        if parent_data then
            parent_data.output_buffer = buf_data
        end
        buf_data.category = "layer"
    else
        error(string.format("Cannot define buffer in scope: %s", tostring(scope_kind)))
    end

    self._all_buffers[path] = buf_data

    local handle = self:_push_scope(SCOPE_BUFFER, name)
    handle._buf_data = buf_data
    return handle
end

function ScanTreeDSL:define_pin(handle, name, position, description)
    description = description or ""
    if handle.kind ~= SCOPE_BUFFER then
        error("define_pin requires a buffer handle")
    end
    if handle.closed then
        error(string.format("Buffer already closed: %s", tostring(handle)))
    end

    local buf_data = handle._buf_data
    if position < 0 or position >= buf_data.size then
        error(string.format("Pin position %d out of range for buffer '%s' (size %d)",
            position, buf_data.name, buf_data.size))
    end

    -- Check duplicates
    for _, pin in ipairs(buf_data.pins) do
        if pin.position == position then
            error(string.format("Duplicate pin position %d in buffer '%s'",
                position, buf_data.name))
        end
        if pin.name == name then
            error(string.format("Duplicate pin name '%s' in buffer '%s'",
                name, buf_data.name))
        end
    end

    buf_data.pins[#buf_data.pins + 1] = {
        name = name,
        position = position,
        description = description,
    }
end

function ScanTreeDSL:end_buffer(handle)
    self:_pop_scope(handle, SCOPE_BUFFER)
end

-- ------------------------------------------------------------------
-- Buffer lookup
-- ------------------------------------------------------------------

function ScanTreeDSL:_get_buffer(name)
    -- Try full path
    if self._all_buffers[name] then
        return self._all_buffers[name]
    end
    -- Try short name
    for path, buf in pairs(self._all_buffers) do
        if buf.name == name then
            return buf
        end
    end
    return nil
end

-- ------------------------------------------------------------------
-- Parameter parsing and validation
-- ------------------------------------------------------------------

function ScanTreeDSL:parse_param(param_str)
    local colon_pos = param_str:find(":")
    if not colon_pos then
        error(string.format(
            "Invalid parameter format: '%s'. Expected buffer_name:start_position-count",
            param_str))
    end

    local buf_name = param_str:sub(1, colon_pos - 1)
    local rest = param_str:sub(colon_pos + 1)

    local dash_pos = rest:find("-")
    if not dash_pos then
        error(string.format(
            "Invalid parameter format: '%s'. Expected buffer_name:start_position-count",
            param_str))
    end

    local start_str = rest:sub(1, dash_pos - 1)
    local count_str = rest:sub(dash_pos + 1)

    local start = tonumber(start_str)
    if not start then
        error(string.format("Invalid start position: '%s'", start_str))
    end

    local count
    if count_str == "*" then
        count = -1  -- Variable count marker
    else
        count = tonumber(count_str)
        if not count then
            error(string.format("Invalid count: '%s'", count_str))
        end
    end

    return buf_name, start, count
end

function ScanTreeDSL:validate_param(param_str, opts)
    opts = opts or {}
    local buf_name, start, count = self:parse_param(param_str)

    local buf = self:_get_buffer(buf_name)
    if not buf then
        error(string.format("Buffer not found: '%s'", buf_name))
    end

    if opts.require_bool and buf.type ~= "bool" then
        error(string.format("Buffer '%s' must be bool, got %s", buf_name, buf.type))
    end

    if opts.expected_type and buf.type ~= opts.expected_type then
        error(string.format("Buffer '%s' type mismatch: expected %s, got %s",
            buf_name, opts.expected_type, buf.type))
    end

    if start < 0 or start >= buf.size then
        error(string.format("Start position %d out of range for buffer '%s' (size %d)",
            start, buf_name, buf.size))
    end

    if count ~= -1 then  -- Not variable
        if count <= 0 then
            error(string.format("Count must be > 0, got %d", count))
        end
        if start + count > buf.size then
            error(string.format("Range %d-%d exceeds buffer '%s' size (%d)",
                start, start + count - 1, buf_name, buf.size))
        end
    end

    return buf, start, count
end

-- ------------------------------------------------------------------
-- Virtual function instantiation
-- ------------------------------------------------------------------

function ScanTreeDSL:instantiate_vft(vft_func, ...)
    local record = vft_func(self, ...)

    local parent_data = self:_find_parent_data()
    if parent_data then
        parent_data.virtual_functions[#parent_data.virtual_functions + 1] = record
    end

    return record
end

-- ------------------------------------------------------------------
-- JSON output
-- ------------------------------------------------------------------

function ScanTreeDSL:to_json()
    if not self._finalized then
        error("SCAN_TREE_end() must be called before generating JSON")
    end
    return json.encode(self._json_data)
end

function ScanTreeDSL:write_json(filepath)
    local f = io.open(filepath, "w")
    if not f then
        error(string.format("Cannot open file for writing: %s", filepath))
    end
    f:write(self:to_json())
    f:write("\n")
    f:close()
    print(string.format("JSON written to: %s", filepath))
end

function ScanTreeDSL:print_summary()
    local d = self._json_data
    print(string.format("\n=== Scan Tree: %s ===", d.name))
    print(string.format("Raw buffers: %d  Levels: %d", #d.raw_buffers, #d.levels))
    for _, lv in ipairs(d.levels) do
        print(string.format("  %s (L%d)", lv.name, lv.level_number))
        for _, sub in ipairs(lv.sub_levels) do
            print(string.format("    %s: %d VFTs", sub.name, #sub.virtual_functions))
            for _, v in ipairs(sub.virtual_functions) do
                print(string.format("      %s [%s]", v.vft_name, v.class))
            end
        end
        for _, v in ipairs(lv.virtual_functions) do
            print(string.format("    %s [%s]", v.vft_name, v.class))
        end
    end
end
-- =====================================================================
-- Module export
-- =====================================================================

local M = {
    ScanTreeDSL = ScanTreeDSL,
    VALID_TYPES = VALID_TYPES,
    json = json,
}

return M

