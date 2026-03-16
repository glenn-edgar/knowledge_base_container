-- ============================================================================
-- json_lua_functions.lua
-- Lua 5.3 implementations of ALL dictionary operations
--
-- Replaces all SE_DICT_* and SE_LOAD_DICTIONARY builtins with Lua
-- implementations that call back into C via inst methods.
--
-- Bridge inst methods used:
--   inst:dict_extract_str(dict_off, path, dest_off, type)
--   inst:dict_extract_keys(dict_off, keys, dest_off, type)
--   inst:dict_store_ptr_str(dict_off, path, dest_off)
--   inst:dict_store_ptr_keys(dict_off, keys, dest_off)
--   inst:store_raw_param_ptr(raw_params, index, dest_off)
--
-- type: "int", "uint", "float", "bool", "hash"
-- ============================================================================

local bridge = se_bridge

-- ============================================================================
-- FNV-1a 32-bit hash
-- ============================================================================

local function fnv1a_32(str)
    local hash = 0x811c9dc5
    local prime = 0x01000193
    for i = 1, #str do
        hash = hash ~ string.byte(str, i)
        hash = (hash * prime) & 0xFFFFFFFF
    end
    return hash
end

-- ============================================================================
-- Param opcode constants
-- ============================================================================

local OPCODE = {
    INT       = 0x00,
    UINT      = 0x01,
    FLOAT     = 0x02,
    STR_HASH  = 0x03,
    FIELD     = 0x0B,
    STR_IDX   = 0x0D,
}

-- ============================================================================
-- HELPER: Parse string-path extraction params
-- Returns dict_offset, path_string, dest_offset or nil on error
-- ============================================================================

local function parse_str_extract(params, func_name, inst)
    local dict_param = params[1]
    local path_param = params[2]
    local dest_param = params[3]

    if not dict_param or dict_param.opcode ~= OPCODE.FIELD then
        print(string.format("[%s] ERROR: param[1] not FIELD", func_name))
        return nil
    end
    if not path_param or path_param.opcode ~= OPCODE.STR_IDX then
        print(string.format("[%s] ERROR: param[2] not STR_IDX", func_name))
        return nil
    end
    if not dest_param or dest_param.opcode ~= OPCODE.FIELD then
        print(string.format("[%s] ERROR: param[3] not FIELD", func_name))
        return nil
    end

    local path_str = inst:get_string(path_param.str_index)
    if not path_str then
        print(string.format("[%s] ERROR: string index %d invalid", func_name, path_param.str_index))
        return nil
    end

    return dict_param.field_offset, path_str, dest_param.field_offset
end

-- ============================================================================
-- HELPER: Parse hash-path extraction params
-- Returns dict_offset, hash_keys_table, dest_offset or nil on error
-- ============================================================================

local function parse_hash_extract(params, func_name)
    local n = #params
    if n < 3 then
        print(string.format("[%s] ERROR: need at least 3 params, got %d", func_name, n))
        return nil
    end

    local dict_param = params[1]
    if not dict_param or dict_param.opcode ~= OPCODE.FIELD then
        print(string.format("[%s] ERROR: param[1] not FIELD", func_name))
        return nil
    end

    local dest_param = params[n]
    if not dest_param or dest_param.opcode ~= OPCODE.FIELD then
        print(string.format("[%s] ERROR: param[%d] not FIELD", func_name, n))
        return nil
    end

    local hash_keys = {}
    for i = 2, n - 1 do
        local p = params[i]
        if not p or p.opcode ~= OPCODE.STR_HASH then
            print(string.format("[%s] ERROR: param[%d] not STR_HASH", func_name, i))
            return nil
        end
        hash_keys[#hash_keys + 1] = p.str_hash
    end

    return dict_param.field_offset, hash_keys, dest_param.field_offset
end

-- ============================================================================
-- LOAD DICTIONARY (string keys)
-- params[1] = FIELD (dest PTR64 offset)
-- params[2..] = OPEN_DICT structure in param stream
-- raw_params (6th arg) = C pointer to param array
-- ============================================================================

bridge.register(fnv1a_32("LUA_LOAD_DICTIONARY"), "oneshot",
    function(inst, params, event_type, event_id, event_data, raw_params)
        local dict_param = params[1]
        if not dict_param or dict_param.opcode ~= OPCODE.FIELD then
            print("[LUA_LOAD_DICTIONARY] ERROR: param[1] not FIELD")
            return
        end
        -- Store C address of raw_params[1] (the OPEN_DICT) into the PTR64 field
        inst:store_raw_param_ptr(raw_params, 1, dict_param.field_offset)
    end
)

-- ============================================================================
-- LOAD DICTIONARY HASH (hash keys)
-- Same structure, just the dict uses hash keys internally
-- ============================================================================

bridge.register(fnv1a_32("LUA_LOAD_DICTIONARY_HASH"), "oneshot",
    function(inst, params, event_type, event_id, event_data, raw_params)
        local dict_param = params[1]
        if not dict_param or dict_param.opcode ~= OPCODE.FIELD then
            print("[LUA_LOAD_DICTIONARY_HASH] ERROR: param[1] not FIELD")
            return
        end
        inst:store_raw_param_ptr(raw_params, 1, dict_param.field_offset)
    end
)

-- ============================================================================
-- STRING PATH EXTRACTION (5 types)
-- ============================================================================

bridge.register(fnv1a_32("LUA_DICT_EXTRACT_INT"), "oneshot",
    function(inst, params, event_type, event_id, event_data)
        local d, p, dest = parse_str_extract(params, "LUA_DICT_EXTRACT_INT", inst)
        if d then inst:dict_extract_str(d, p, dest, "int") end
    end
)

bridge.register(fnv1a_32("LUA_DICT_EXTRACT_FLOAT"), "oneshot",
    function(inst, params, event_type, event_id, event_data)
        local d, p, dest = parse_str_extract(params, "LUA_DICT_EXTRACT_FLOAT", inst)
        if d then inst:dict_extract_str(d, p, dest, "float") end
    end
)

bridge.register(fnv1a_32("LUA_DICT_EXTRACT_UINT"), "oneshot",
    function(inst, params, event_type, event_id, event_data)
        local d, p, dest = parse_str_extract(params, "LUA_DICT_EXTRACT_UINT", inst)
        if d then inst:dict_extract_str(d, p, dest, "uint") end
    end
)

bridge.register(fnv1a_32("LUA_DICT_EXTRACT_BOOL"), "oneshot",
    function(inst, params, event_type, event_id, event_data)
        local d, p, dest = parse_str_extract(params, "LUA_DICT_EXTRACT_BOOL", inst)
        if d then inst:dict_extract_str(d, p, dest, "bool") end
    end
)

bridge.register(fnv1a_32("LUA_DICT_EXTRACT_HASH"), "oneshot",
    function(inst, params, event_type, event_id, event_data)
        local d, p, dest = parse_str_extract(params, "LUA_DICT_EXTRACT_HASH", inst)
        if d then inst:dict_extract_str(d, p, dest, "hash") end
    end
)

-- ============================================================================
-- HASH PATH EXTRACTION (5 types)
-- ============================================================================

bridge.register(fnv1a_32("LUA_DICT_EXTRACT_INT_H"), "oneshot",
    function(inst, params, event_type, event_id, event_data)
        local d, keys, dest = parse_hash_extract(params, "LUA_DICT_EXTRACT_INT_H")
        if d then inst:dict_extract_keys(d, keys, dest, "int") end
    end
)

bridge.register(fnv1a_32("LUA_DICT_EXTRACT_FLOAT_H"), "oneshot",
    function(inst, params, event_type, event_id, event_data)
        local d, keys, dest = parse_hash_extract(params, "LUA_DICT_EXTRACT_FLOAT_H")
        if d then inst:dict_extract_keys(d, keys, dest, "float") end
    end
)

bridge.register(fnv1a_32("LUA_DICT_EXTRACT_UINT_H"), "oneshot",
    function(inst, params, event_type, event_id, event_data)
        local d, keys, dest = parse_hash_extract(params, "LUA_DICT_EXTRACT_UINT_H")
        if d then inst:dict_extract_keys(d, keys, dest, "uint") end
    end
)

bridge.register(fnv1a_32("LUA_DICT_EXTRACT_BOOL_H"), "oneshot",
    function(inst, params, event_type, event_id, event_data)
        local d, keys, dest = parse_hash_extract(params, "LUA_DICT_EXTRACT_BOOL_H")
        if d then inst:dict_extract_keys(d, keys, dest, "bool") end
    end
)

bridge.register(fnv1a_32("LUA_DICT_EXTRACT_HASH_H"), "oneshot",
    function(inst, params, event_type, event_id, event_data)
        local d, keys, dest = parse_hash_extract(params, "LUA_DICT_EXTRACT_HASH_H")
        if d then inst:dict_extract_keys(d, keys, dest, "hash") end
    end
)

-- ============================================================================
-- POINTER STORAGE
-- ============================================================================

bridge.register(fnv1a_32("LUA_DICT_STORE_PTR"), "oneshot",
    function(inst, params, event_type, event_id, event_data)
        local d, p, dest = parse_str_extract(params, "LUA_DICT_STORE_PTR", inst)
        if d then inst:dict_store_ptr_str(d, p, dest) end
    end
)

bridge.register(fnv1a_32("LUA_DICT_STORE_PTR_H"), "oneshot",
    function(inst, params, event_type, event_id, event_data)
        local d, keys, dest = parse_hash_extract(params, "LUA_DICT_STORE_PTR_H")
        if d then inst:dict_store_ptr_keys(d, keys, dest) end
    end
)

-- ============================================================================
-- Registration summary
-- ============================================================================

local names = {
    "LUA_LOAD_DICTIONARY", "LUA_LOAD_DICTIONARY_HASH",
    "LUA_DICT_EXTRACT_INT", "LUA_DICT_EXTRACT_FLOAT",
    "LUA_DICT_EXTRACT_UINT", "LUA_DICT_EXTRACT_BOOL",
    "LUA_DICT_EXTRACT_HASH",
    "LUA_DICT_EXTRACT_INT_H", "LUA_DICT_EXTRACT_FLOAT_H",
    "LUA_DICT_EXTRACT_UINT_H", "LUA_DICT_EXTRACT_BOOL_H",
    "LUA_DICT_EXTRACT_HASH_H",
    "LUA_DICT_STORE_PTR", "LUA_DICT_STORE_PTR_H",
}

for _, name in ipairs(names) do
    print(string.format("  Registered: %s (hash=0x%08X) as oneshot", name, fnv1a_32(name)))
end
print(string.format("  Lua dict functions: %d total", #names))