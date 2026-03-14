-- ============================================================================
-- json_lua_functions.lua
-- Lua 5.3 implementations of dictionary hash extraction
--
-- Replaces SE_DICT_EXTRACT_HASH and SE_DICT_EXTRACT_HASH_H builtins
-- with Lua implementations that call back into C via inst methods.
-- ============================================================================

local bridge = se_bridge

-- ============================================================================
-- FNV-1a 32-bit hash (Lua 5.3 native integers)
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
-- Param opcode constants (match s_engine_types.h)
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
-- ONESHOT: LUA_DICT_EXTRACT_HASH (string path version)
--
-- Params from DSL:
--   params[1] = FIELD  (dict_field: offset/size of PTR64 holding dict pointer)
--   params[2] = STR_IDX (path: dot-separated string like "hashes.state_idle")
--   params[3] = FIELD  (dest_field: offset/size where to write the hash result)
--
-- Calls inst:dict_extract_hash_str(dict_offset, path_string, dest_offset)
-- which wraps the C se_dict_string_lookup internally.
-- ============================================================================

bridge.register(fnv1a_32("LUA_DICT_EXTRACT_HASH"), "oneshot",
    function(inst, params, event_type, event_id, event_data)
        -- Parse params
        local dict_param = params[1]
        local path_param = params[2]
        local dest_param = params[3]
        
        if not dict_param or dict_param.opcode ~= OPCODE.FIELD then
            print("[LUA_DICT_EXTRACT_HASH] ERROR: param[1] not FIELD")
            return
        end
        if not path_param or path_param.opcode ~= OPCODE.STR_IDX then
            print("[LUA_DICT_EXTRACT_HASH] ERROR: param[2] not STR_IDX")
            return
        end
        if not dest_param or dest_param.opcode ~= OPCODE.FIELD then
            print("[LUA_DICT_EXTRACT_HASH] ERROR: param[3] not FIELD")
            return
        end
        
        -- Get the path string from the string table
        local path_str = inst:get_string(path_param.str_index)
        if not path_str then
            print("[LUA_DICT_EXTRACT_HASH] ERROR: string index invalid")
            return
        end
        
        -- Call the C dict lookup via inst method
        inst:dict_extract_hash_str(
            dict_param.field_offset,
            path_str,
            dest_param.field_offset
        )
    end
)

-- ============================================================================
-- ONESHOT: LUA_DICT_EXTRACT_HASH_H (hash path version)
--
-- Params from DSL:
--   params[1]     = FIELD    (dict_field: offset/size of PTR64 holding dict)
--   params[2..N-1] = STR_HASH (hash keys for path navigation)
--   params[N]     = FIELD    (dest_field: offset/size where to write result)
--
-- Calls inst:dict_extract_hash_keys(dict_offset, {h1, h2, ...}, dest_offset)
-- which wraps the C se_dict_hash_lookup internally.
-- ============================================================================

bridge.register(fnv1a_32("LUA_DICT_EXTRACT_HASH_H"), "oneshot",
    function(inst, params, event_type, event_id, event_data)
        local n = #params
        if n < 3 then
            print("[LUA_DICT_EXTRACT_HASH_H] ERROR: need at least 3 params")
            return
        end
        
        -- First param: dict field
        local dict_param = params[1]
        if not dict_param or dict_param.opcode ~= OPCODE.FIELD then
            print("[LUA_DICT_EXTRACT_HASH_H] ERROR: param[1] not FIELD")
            return
        end
        
        -- Last param: dest field
        local dest_param = params[n]
        if not dest_param or dest_param.opcode ~= OPCODE.FIELD then
            print("[LUA_DICT_EXTRACT_HASH_H] ERROR: param[N] not FIELD")
            return
        end
        
        -- Middle params: hash keys
        local hash_keys = {}
        for i = 2, n - 1 do
            local p = params[i]
            if not p or p.opcode ~= OPCODE.STR_HASH then
                print(string.format(
                    "[LUA_DICT_EXTRACT_HASH_H] ERROR: param[%d] not STR_HASH", i))
                return
            end
            hash_keys[#hash_keys + 1] = p.str_hash
        end
        
        -- Call the C dict lookup via inst method
        inst:dict_extract_hash_keys(
            dict_param.field_offset,
            hash_keys,
            dest_param.field_offset
        )
    end
)

-- ============================================================================
-- Registration summary
-- ============================================================================

print(string.format("  Registered: LUA_DICT_EXTRACT_HASH (hash=0x%08X) as oneshot",
    fnv1a_32("LUA_DICT_EXTRACT_HASH")))
print(string.format("  Registered: LUA_DICT_EXTRACT_HASH_H (hash=0x%08X) as oneshot",
    fnv1a_32("LUA_DICT_EXTRACT_HASH_H")))
print("  Lua dict hash extraction registration complete")
