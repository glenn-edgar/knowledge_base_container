-- ============================================================================
-- se_builtins_dict.lua
-- Mirrors s_engine_builtins_dict.h / se_load_dictionary.c
--
-- Dictionary loading and value extraction oneshot functions.
-- All functions: fn(inst, node)  (oneshot, no return value)
--
-- In C, dictionaries are ROM binary structures (s_expr_param_t arrays).
-- In Lua, dictionaries are plain Lua tables stored in the blackboard:
--
--   {                              -- top-level dict
--     ["key_name"] = 42,           -- scalar value
--     [0xABCD1234] = 3.14,         -- hash-keyed scalar
--     ["sub"] = { ... },           -- nested dict
--   }
--
-- se_load_dictionary stores a reference (the Lua table itself) into a
-- blackboard field. se_dict_extract_* navigate the table and write values.
--
-- String-path functions (params[2] = str_idx):
--   Navigate using dot-separated keys  "level1.level2.key"
--
-- Hash-path functions (params[2..N-1] = str_hash):
--   Navigate using a sequence of hash keys
-- ============================================================================

local se_runtime = require("se_runtime")

local param_str        = se_runtime.param_str
local param_field_name = se_runtime.param_field_name
local field_get        = se_runtime.field_get

local M = {}

-- ============================================================================
-- Internal helpers
-- ============================================================================

-- Retrieve the dict table from a blackboard PTR field
local function get_dict_from_bb(inst, node, param_idx)
    local field_name = param_field_name(node, param_idx)
    local dict = inst.blackboard[field_name]
    assert(dict and type(dict) == "table",
        "se_dict: blackboard field is not a table: " .. tostring(field_name))
    return dict
end

-- Navigate a nested dict using a dot-separated string path
-- "a.b.c" -> dict["a"]["b"]["c"]
local function navigate_string_path(dict, path)
    local cur = dict
    for key in path:gmatch("[^%.]+") do
        if type(cur) ~= "table" then return nil end
        -- Try string key first, then numeric key
        cur = cur[key]
        if cur == nil then cur = cur and cur[tonumber(key)] end
    end
    return cur
end

-- Navigate a nested dict using an array of hash keys
local function navigate_hash_path(dict, hashes)
    local cur = dict
    for _, h in ipairs(hashes) do
        if type(cur) ~= "table" then return nil end
        cur = cur[h]
        if cur == nil then cur = cur and cur[tostring(h)] end
    end
    return cur
end

-- Extract scalar from dict entry; entry may be raw value or {value=V} table
local function extract_value(entry)
    if type(entry) == "table" and entry.value ~= nil then
        return entry.value
    end
    return entry
end

-- Collect hash key params from node.params[start_idx .. end_idx]
-- Returns array of hash numbers
local function collect_hashes(node, start_idx, end_idx)
    local hashes = {}
    for i = start_idx, end_idx do
        local p = (node.params or {})[i]
        if not p then break end
        hashes[#hashes + 1] = (type(p.value) == "table") and p.value.hash or p.value
    end
    return hashes
end

-- Find the index of the last field_ref param (used by hash-path functions)
local function last_field_param_idx(node)
    local params = node.params or {}
    for i = #params, 1, -1 do
        if params[i].type == "field_ref" or params[i].type == "nested_field_ref" then
            return i
        end
    end
    return nil
end

-- ============================================================================
-- SE_LOAD_DICTIONARY  (oneshot)
-- Stores a reference to a dictionary constant into a blackboard PTR field.
-- params[1] = field_ref  (destination PTR field)
-- params[2] = const_ref | str_idx | str_hash  (name/hash of the constant dict)
--
-- In C this stores a pointer into the binary module image.
-- In Lua we look up mod.module_data.constants and store the Lua table.
-- If params[2] is a list_start, use its items table directly (inline dict).
-- ============================================================================
M.se_load_dictionary = function(inst, node)
    assert(#(node.params or {}) >= 2,
        "se_load_dictionary: requires [field_ref dest] [const ref]")

    local field_name = param_field_name(node, 1)
    local p2 = node.params[2]
    local dict_ref

    local consts = inst.mod.module_data and inst.mod.module_data.constants

    if p2.type == "str_idx" or p2.type == "str_ptr" then
        -- String constant name: direct lookup
        dict_ref = consts and consts[p2.value]
    elseif p2.type == "const_ref" then
        dict_ref = consts and consts[p2.value]
    elseif p2.type == "str_hash" or p2.type == "uint" or p2.type == "int" then
        -- Hash: search constants table for name_hash match, or use as direct key
        local hash = (type(p2.value) == "table") and p2.value.hash or p2.value
        if consts then
            dict_ref = consts[hash]
            if not dict_ref then
                for _, v in pairs(consts) do
                    if type(v) == "table" and v.name_hash == hash then
                        dict_ref = v.data or v; break
                    end
                end
            end
        end
    elseif p2.type == "list_start" then
        -- Inline dict embedded directly in params
        dict_ref = p2.items or {}
    end

    assert(dict_ref, "se_load_dictionary: could not resolve dictionary constant")
    inst.blackboard[field_name] = dict_ref
end

-- ============================================================================
-- String-path extraction
-- params[1] = field_ref  (source dict in blackboard)
-- params[2] = str_idx    (dot-separated path string)
-- params[3] = field_ref  (destination field)
-- ============================================================================

M.se_dict_extract_int = function(inst, node)
    local dict  = get_dict_from_bb(inst, node, 1)
    local path  = param_str(node, 2)
    local entry = navigate_string_path(dict, path)
    local val   = entry and math.floor(tonumber(extract_value(entry)) or 0) or 0
    inst.blackboard[param_field_name(node, 3)] = val
end

M.se_dict_extract_uint = function(inst, node)
    local dict  = get_dict_from_bb(inst, node, 1)
    local path  = param_str(node, 2)
    local entry = navigate_string_path(dict, path)
    local val   = entry and math.floor(math.abs(tonumber(extract_value(entry)) or 0)) or 0
    inst.blackboard[param_field_name(node, 3)] = val
end

M.se_dict_extract_float = function(inst, node)
    local dict  = get_dict_from_bb(inst, node, 1)
    local path  = param_str(node, 2)
    local entry = navigate_string_path(dict, path)
    local val   = entry and ((tonumber(extract_value(entry)) or 0) + 0.0) or 0.0
    inst.blackboard[param_field_name(node, 3)] = val
end

M.se_dict_extract_bool = function(inst, node)
    local dict  = get_dict_from_bb(inst, node, 1)
    local path  = param_str(node, 2)
    local entry = navigate_string_path(dict, path)
    local raw   = entry and extract_value(entry) or nil
    local val   = (raw and raw ~= 0 and raw ~= false) and 1 or 0
    inst.blackboard[param_field_name(node, 3)] = val
end

M.se_dict_extract_hash = function(inst, node)
    local dict  = get_dict_from_bb(inst, node, 1)
    local path  = param_str(node, 2)
    local entry = navigate_string_path(dict, path)
    inst.blackboard[param_field_name(node, 3)] = entry and extract_value(entry) or 0
end

-- ============================================================================
-- Hash-path extraction
-- params[1]       = field_ref  (source dict)
-- params[2..N-1]  = str_hash   (path segment hashes)
-- params[N]       = field_ref  (destination field)  -- last field_ref param
-- ============================================================================

local function hash_path_extract(inst, node, converter)
    local dict     = get_dict_from_bb(inst, node, 1)
    local dest_idx = last_field_param_idx(node)
    -- dest_idx must be > 1 (at least one hash param between them)
    assert(dest_idx and dest_idx > 2,
        "se_dict_extract_h: missing hash path params or destination")
    local hashes = collect_hashes(node, 2, dest_idx - 1)
    local entry  = navigate_hash_path(dict, hashes)
    inst.blackboard[param_field_name(node, dest_idx)] =
        converter(entry and extract_value(entry) or nil)
end

M.se_dict_extract_int_h = function(inst, node)
    hash_path_extract(inst, node,
        function(v) return v and math.floor(tonumber(v) or 0) or 0 end)
end

M.se_dict_extract_uint_h = function(inst, node)
    hash_path_extract(inst, node,
        function(v) return v and math.floor(math.abs(tonumber(v) or 0)) or 0 end)
end

M.se_dict_extract_float_h = function(inst, node)
    hash_path_extract(inst, node,
        function(v) return v and ((tonumber(v) or 0) + 0.0) or 0.0 end)
end

M.se_dict_extract_bool_h = function(inst, node)
    hash_path_extract(inst, node,
        function(v) return (v and v ~= 0 and v ~= false) and 1 or 0 end)
end

M.se_dict_extract_hash_h = function(inst, node)
    hash_path_extract(inst, node, function(v) return v or 0 end)
end

-- ============================================================================
-- SE_DICT_STORE_PTR  (oneshot)
-- Stores a reference to a sub-table at a string path into a blackboard field.
-- params[1] = field_ref  (source dict)
-- params[2] = str_idx    (dot-separated path)
-- params[3] = field_ref  (destination PTR field)
-- ============================================================================
M.se_dict_store_ptr = function(inst, node)
    local dict = get_dict_from_bb(inst, node, 1)
    local path = param_str(node, 2)
    local sub  = navigate_string_path(dict, path)
    inst.blackboard[param_field_name(node, 3)] = sub   -- nil if not found
end

-- ============================================================================
-- SE_DICT_STORE_PTR_H  (oneshot)
-- Stores a sub-table reference navigated by hash path.
-- params[1]       = field_ref  (source dict)
-- params[2..N-1]  = str_hash   (path segments)
-- params[N]       = field_ref  (destination PTR field)
-- ============================================================================
M.se_dict_store_ptr_h = function(inst, node)
    local dict     = get_dict_from_bb(inst, node, 1)
    local dest_idx = last_field_param_idx(node)
    assert(dest_idx and dest_idx > 2,
        "se_dict_store_ptr_h: missing hash path params or destination")
    local hashes = collect_hashes(node, 2, dest_idx - 1)
    local sub    = navigate_hash_path(dict, hashes)
    inst.blackboard[param_field_name(node, dest_idx)] = sub
end

return M