-- json_extract_test.lua
-- Comprehensive test for all dictionary extraction functions
-- ALL dict operations implemented in Lua via bridge methods
--
-- Five passes:
--   Pass 1: String path extraction (all via Lua)
--   Pass 2: Hash path extraction (all via Lua)
--   Pass 3: Array element access via index paths
--   Pass 4: Sub-dictionary pointer storage and extraction
--   Pass 5: Hash-path pointer storage and extraction

local mod = start_module("json_test")

-- ============================================================================
-- LUA FUNCTION DECLARATIONS
-- ============================================================================

LUA_FUNCTIONS {
    oneshot = {
        "LUA_LOAD_DICTIONARY",
        "LUA_LOAD_DICTIONARY_HASH",
        "LUA_DICT_EXTRACT_INT",
        "LUA_DICT_EXTRACT_FLOAT",
        "LUA_DICT_EXTRACT_UINT",
        "LUA_DICT_EXTRACT_BOOL",
        "LUA_DICT_EXTRACT_HASH",
        "LUA_DICT_EXTRACT_INT_H",
        "LUA_DICT_EXTRACT_FLOAT_H",
        "LUA_DICT_EXTRACT_UINT_H",
        "LUA_DICT_EXTRACT_BOOL_H",
        "LUA_DICT_EXTRACT_HASH_H",
        "LUA_DICT_STORE_PTR",
        "LUA_DICT_STORE_PTR_H",
    },
}

-- ============================================================================
-- RECORD DEFINITION
-- ============================================================================

RECORD("extract_state")
    PTR64_FIELD("dict_string", "void")
    PTR64_FIELD("dict_hash", "void")
    FIELD("pass_number", "uint32")
    FIELD("int_val_1", "int32")
    FIELD("int_val_2", "int32")
    FIELD("int_val_3", "int32")
    FIELD("uint_val_1", "uint32")
    FIELD("uint_val_2", "uint32")
    FIELD("uint_val_3", "uint32")
    FIELD("float_val_1", "float")
    FIELD("float_val_2", "float")
    FIELD("float_val_3", "float")
    FIELD("bool_val_1", "uint32")
    FIELD("bool_val_2", "uint32")
    FIELD("bool_val_3", "uint32")
    FIELD("hash_val_1", "uint32")
    FIELD("hash_val_2", "uint32")
    FIELD("hash_val_3", "uint32")
    FIELD("arr_int_0", "int32")
    FIELD("arr_int_1", "int32")
    FIELD("arr_int_2", "int32")
    FIELD("arr_int_3", "int32")
    FIELD("arr_float_0", "float")
    FIELD("arr_float_1", "float")
    FIELD("arr_float_2", "float")
    FIELD("arr_nested_0_id", "uint32")
    FIELD("arr_nested_0_val", "float")
    FIELD("arr_nested_1_id", "uint32")
    FIELD("arr_nested_1_val", "float")
    FIELD("arr_nested_2_id", "uint32")
    FIELD("arr_nested_2_val", "float")
    PTR64_FIELD("sub_integers", "void")
    PTR64_FIELD("sub_floats", "void")
    PTR64_FIELD("sub_nested_0", "void")
    PTR64_FIELD("sub_nested_1", "void")
    FIELD("ptr_int_pos", "int32")
    FIELD("ptr_int_neg", "int32")
    FIELD("ptr_float_pi", "float")
    FIELD("ptr_float_neg", "float")
    FIELD("ptr_n0_id", "uint32")
    FIELD("ptr_n0_val", "float")
    FIELD("ptr_n1_id", "uint32")
    FIELD("ptr_n1_val", "float")
END_RECORD()

-- ============================================================================
-- LUA DSL HELPERS
-- Mirror the se_* helpers but route to Lua function names
-- ============================================================================

local function lua_load_dictionary(blackboard_field, json_expression)
    validate_field_is_ptr64(blackboard_field, "lua_load_dictionary")
    local c = o_call("LUA_LOAD_DICTIONARY")
        field_ref(blackboard_field)
        json(json_expression)
    end_call(c)
    return c
end

local function lua_load_dictionary_hash(blackboard_field, json_expression)
    validate_field_is_ptr64(blackboard_field, "lua_load_dictionary_hash")
    local c = o_call("LUA_LOAD_DICTIONARY_HASH")
        field_ref(blackboard_field)
        json_hash(json_expression)
    end_call(c)
    return c
end

-- String path extraction helpers
local function lua_dict_extract_int(dict_field, path, dest_field)
    validate_field_is_ptr64(dict_field, "lua_dict_extract_int")
    local c = o_call("LUA_DICT_EXTRACT_INT")
        field_ref(dict_field)
        str(path)
        field_ref(dest_field)
    end_call(c)
    return c
end

local function lua_dict_extract_float(dict_field, path, dest_field)
    validate_field_is_ptr64(dict_field, "lua_dict_extract_float")
    local c = o_call("LUA_DICT_EXTRACT_FLOAT")
        field_ref(dict_field)
        str(path)
        field_ref(dest_field)
    end_call(c)
    return c
end

local function lua_dict_extract_uint(dict_field, path, dest_field)
    validate_field_is_ptr64(dict_field, "lua_dict_extract_uint")
    local c = o_call("LUA_DICT_EXTRACT_UINT")
        field_ref(dict_field)
        str(path)
        field_ref(dest_field)
    end_call(c)
    return c
end

local function lua_dict_extract_bool(dict_field, path, dest_field)
    validate_field_is_ptr64(dict_field, "lua_dict_extract_bool")
    local c = o_call("LUA_DICT_EXTRACT_BOOL")
        field_ref(dict_field)
        str(path)
        field_ref(dest_field)
    end_call(c)
    return c
end

local function lua_dict_extract_hash(dict_field, path, dest_field)
    validate_field_is_ptr64(dict_field, "lua_dict_extract_hash")
    local c = o_call("LUA_DICT_EXTRACT_HASH")
        field_ref(dict_field)
        str(path)
        field_ref(dest_field)
    end_call(c)
    return c
end

-- Hash path extraction helpers
local function lua_dict_extract_int_h(dict_field, path_keys, dest_field)
    validate_field_is_ptr64(dict_field, "lua_dict_extract_int_h")
    if type(path_keys) ~= "table" or #path_keys == 0 then
        dsl_error("lua_dict_extract_int_h: path_keys must be non-empty table")
    end
    local c = o_call("LUA_DICT_EXTRACT_INT_H")
        field_ref(dict_field)
        for _, key in ipairs(path_keys) do str_hash(key) end
        field_ref(dest_field)
    end_call(c)
    return c
end

local function lua_dict_extract_float_h(dict_field, path_keys, dest_field)
    validate_field_is_ptr64(dict_field, "lua_dict_extract_float_h")
    if type(path_keys) ~= "table" or #path_keys == 0 then
        dsl_error("lua_dict_extract_float_h: path_keys must be non-empty table")
    end
    local c = o_call("LUA_DICT_EXTRACT_FLOAT_H")
        field_ref(dict_field)
        for _, key in ipairs(path_keys) do str_hash(key) end
        field_ref(dest_field)
    end_call(c)
    return c
end

local function lua_dict_extract_uint_h(dict_field, path_keys, dest_field)
    validate_field_is_ptr64(dict_field, "lua_dict_extract_uint_h")
    if type(path_keys) ~= "table" or #path_keys == 0 then
        dsl_error("lua_dict_extract_uint_h: path_keys must be non-empty table")
    end
    local c = o_call("LUA_DICT_EXTRACT_UINT_H")
        field_ref(dict_field)
        for _, key in ipairs(path_keys) do str_hash(key) end
        field_ref(dest_field)
    end_call(c)
    return c
end

local function lua_dict_extract_bool_h(dict_field, path_keys, dest_field)
    validate_field_is_ptr64(dict_field, "lua_dict_extract_bool_h")
    if type(path_keys) ~= "table" or #path_keys == 0 then
        dsl_error("lua_dict_extract_bool_h: path_keys must be non-empty table")
    end
    local c = o_call("LUA_DICT_EXTRACT_BOOL_H")
        field_ref(dict_field)
        for _, key in ipairs(path_keys) do str_hash(key) end
        field_ref(dest_field)
    end_call(c)
    return c
end

local function lua_dict_extract_hash_h(dict_field, path_keys, dest_field)
    validate_field_is_ptr64(dict_field, "lua_dict_extract_hash_h")
    if type(path_keys) ~= "table" or #path_keys == 0 then
        dsl_error("lua_dict_extract_hash_h: path_keys must be non-empty table")
    end
    local c = o_call("LUA_DICT_EXTRACT_HASH_H")
        field_ref(dict_field)
        for _, key in ipairs(path_keys) do str_hash(key) end
        field_ref(dest_field)
    end_call(c)
    return c
end

-- Pointer storage helpers
local function lua_dict_store_ptr(dict_field, path, dest_ptr_field)
    validate_field_is_ptr64(dict_field, "lua_dict_store_ptr")
    validate_field_is_ptr64(dest_ptr_field, "lua_dict_store_ptr")
    local c = o_call("LUA_DICT_STORE_PTR")
        field_ref(dict_field)
        str(path)
        field_ref(dest_ptr_field)
    end_call(c)
    return c
end

local function lua_dict_store_ptr_h(dict_field, path_keys, dest_ptr_field)
    validate_field_is_ptr64(dict_field, "lua_dict_store_ptr_h")
    validate_field_is_ptr64(dest_ptr_field, "lua_dict_store_ptr_h")
    if type(path_keys) ~= "table" or #path_keys == 0 then
        dsl_error("lua_dict_store_ptr_h: path_keys must be non-empty table")
    end
    local c = o_call("LUA_DICT_STORE_PTR_H")
        field_ref(dict_field)
        for _, key in ipairs(path_keys) do str_hash(key) end
        field_ref(dest_ptr_field)
    end_call(c)
    return c
end

-- ============================================================================
-- TEST CONFIGURATION DATA
-- ============================================================================

local config = {
    integers = {
        positive = 12345,
        negative = -9876,
        zero = 0,
        nested = { deep = { value = 42 } }
    },
    unsigned = {
        small = 100,
        medium = 50000,
        large = 0xFFFF,
        nested = { deep = { value = 255 } }
    },
    floats = {
        pi = 3.14159,
        negative = -273.15,
        zero = 0.0,
        nested = { deep = { value = 2.71828 } }
    },
    bools = {
        true_val = 1,
        false_val = 0,
        nested = { deep = { value = 1 } }
    },
    hashes = {
        state_idle = "idle",
        state_running = "running",
        state_error = "error",
        nested = { deep = { value = "deep_hash" } }
    },
    int_array = {10, 20, 30, 40},
    float_array = {1.5, 2.5, 3.5},
    items = {
        {id = 100, value = 10.1},
        {id = 200, value = 20.2},
        {id = 300, value = 30.3},
    },
    level1 = {
        level2 = {
            level3 = {
                level4 = {
                    final_int = 999,
                    final_float = 1.5,
                    final_bool = 1
                }
            }
        }
    }
}

-- ============================================================================
-- TREE DEFINITION
-- ============================================================================

start_tree("json_test")
use_record("extract_state")

se_function_interface(function()

    -- ========================================================================
    -- Load both dictionary formats (via Lua)
    -- ========================================================================

    lua_load_dictionary("dict_string", config)
    lua_load_dictionary_hash("dict_hash", config)

    se_set_field("pass_number", 0)

    -- ========================================================================
    -- PASS 1: String path extraction (all via Lua)
    -- ========================================================================

    se_log("=== PASS 1: String Path Extraction (Lua) ===")
    se_increment_field("pass_number", 1)

    lua_dict_extract_int("dict_string", "integers.positive", "int_val_1")
    lua_dict_extract_int("dict_string", "integers.negative", "int_val_2")
    lua_dict_extract_int("dict_string", "integers.nested.deep.value", "int_val_3")

    lua_dict_extract_uint("dict_string", "unsigned.small", "uint_val_1")
    lua_dict_extract_uint("dict_string", "unsigned.medium", "uint_val_2")
    lua_dict_extract_uint("dict_string", "unsigned.nested.deep.value", "uint_val_3")

    lua_dict_extract_float("dict_string", "floats.pi", "float_val_1")
    lua_dict_extract_float("dict_string", "floats.negative", "float_val_2")
    lua_dict_extract_float("dict_string", "floats.nested.deep.value", "float_val_3")

    lua_dict_extract_bool("dict_string", "bools.true_val", "bool_val_1")
    lua_dict_extract_bool("dict_string", "bools.false_val", "bool_val_2")
    lua_dict_extract_bool("dict_string", "bools.nested.deep.value", "bool_val_3")

    lua_dict_extract_hash("dict_string", "hashes.state_idle", "hash_val_1")
    lua_dict_extract_hash("dict_string", "hashes.state_running", "hash_val_2")
    lua_dict_extract_hash("dict_string", "hashes.nested.deep.value", "hash_val_3")

    local print1 = o_call("USER_PRINT_EXTRACT_RESULTS")
        str("Pass 1 - String Paths (Lua)")
        field_ref("pass_number")
        field_ref("int_val_1")  field_ref("int_val_2")  field_ref("int_val_3")
        field_ref("uint_val_1") field_ref("uint_val_2") field_ref("uint_val_3")
        field_ref("float_val_1") field_ref("float_val_2") field_ref("float_val_3")
        field_ref("bool_val_1") field_ref("bool_val_2") field_ref("bool_val_3")
        field_ref("hash_val_1") field_ref("hash_val_2") field_ref("hash_val_3")
    end_call(print1)

    -- ========================================================================
    -- Clear fields for Pass 2
    -- ========================================================================

    se_set_field("int_val_1", 0)  se_set_field("int_val_2", 0)  se_set_field("int_val_3", 0)
    se_set_field("uint_val_1", 0) se_set_field("uint_val_2", 0) se_set_field("uint_val_3", 0)
    se_set_field("float_val_1", 0) se_set_field("float_val_2", 0) se_set_field("float_val_3", 0)
    se_set_field("bool_val_1", 0) se_set_field("bool_val_2", 0) se_set_field("bool_val_3", 0)
    se_set_field("hash_val_1", 0) se_set_field("hash_val_2", 0) se_set_field("hash_val_3", 0)

    -- ========================================================================
    -- PASS 2: Hash path extraction (all via Lua)
    -- ========================================================================

    se_log("=== PASS 2: Hash Path Extraction (Lua) ===")
    se_increment_field("pass_number", 1)

    lua_dict_extract_int_h("dict_hash", {"integers", "positive"}, "int_val_1")
    lua_dict_extract_int_h("dict_hash", {"integers", "negative"}, "int_val_2")
    lua_dict_extract_int_h("dict_hash", {"integers", "nested", "deep", "value"}, "int_val_3")

    lua_dict_extract_uint_h("dict_hash", {"unsigned", "small"}, "uint_val_1")
    lua_dict_extract_uint_h("dict_hash", {"unsigned", "medium"}, "uint_val_2")
    lua_dict_extract_uint_h("dict_hash", {"unsigned", "nested", "deep", "value"}, "uint_val_3")

    lua_dict_extract_float_h("dict_hash", {"floats", "pi"}, "float_val_1")
    lua_dict_extract_float_h("dict_hash", {"floats", "negative"}, "float_val_2")
    lua_dict_extract_float_h("dict_hash", {"floats", "nested", "deep", "value"}, "float_val_3")

    lua_dict_extract_bool_h("dict_hash", {"bools", "true_val"}, "bool_val_1")
    lua_dict_extract_bool_h("dict_hash", {"bools", "false_val"}, "bool_val_2")
    lua_dict_extract_bool_h("dict_hash", {"bools", "nested", "deep", "value"}, "bool_val_3")

    lua_dict_extract_hash_h("dict_hash", {"hashes", "state_idle"}, "hash_val_1")
    lua_dict_extract_hash_h("dict_hash", {"hashes", "state_running"}, "hash_val_2")
    lua_dict_extract_hash_h("dict_hash", {"hashes", "nested", "deep", "value"}, "hash_val_3")

    local print2 = o_call("USER_PRINT_EXTRACT_RESULTS")
        str("Pass 2 - Hash Paths (Lua)")
        field_ref("pass_number")
        field_ref("int_val_1")  field_ref("int_val_2")  field_ref("int_val_3")
        field_ref("uint_val_1") field_ref("uint_val_2") field_ref("uint_val_3")
        field_ref("float_val_1") field_ref("float_val_2") field_ref("float_val_3")
        field_ref("bool_val_1") field_ref("bool_val_2") field_ref("bool_val_3")
        field_ref("hash_val_1") field_ref("hash_val_2") field_ref("hash_val_3")
    end_call(print2)

    -- ========================================================================
    -- PASS 3: Array element access (via Lua)
    -- ========================================================================

    se_log("=== PASS 3: Array Element Access (Lua) ===")
    se_increment_field("pass_number", 1)

    lua_dict_extract_int("dict_string", "int_array.0", "arr_int_0")
    lua_dict_extract_int("dict_string", "int_array.1", "arr_int_1")
    lua_dict_extract_int("dict_string", "int_array.2", "arr_int_2")
    lua_dict_extract_int("dict_string", "int_array.3", "arr_int_3")

    lua_dict_extract_float("dict_string", "float_array.0", "arr_float_0")
    lua_dict_extract_float("dict_string", "float_array.1", "arr_float_1")
    lua_dict_extract_float("dict_string", "float_array.2", "arr_float_2")

    lua_dict_extract_uint("dict_string", "items.0.id", "arr_nested_0_id")
    lua_dict_extract_float("dict_string", "items.0.value", "arr_nested_0_val")
    lua_dict_extract_uint("dict_string", "items.1.id", "arr_nested_1_id")
    lua_dict_extract_float("dict_string", "items.1.value", "arr_nested_1_val")
    lua_dict_extract_uint("dict_string", "items.2.id", "arr_nested_2_id")
    lua_dict_extract_float("dict_string", "items.2.value", "arr_nested_2_val")

    local print3 = o_call("USER_PRINT_ARRAY_RESULTS")
        str("Pass 3 - Array Access (Lua)")
        field_ref("pass_number")
        field_ref("arr_int_0") field_ref("arr_int_1") field_ref("arr_int_2") field_ref("arr_int_3")
        field_ref("arr_float_0") field_ref("arr_float_1") field_ref("arr_float_2")
        field_ref("arr_nested_0_id") field_ref("arr_nested_0_val")
        field_ref("arr_nested_1_id") field_ref("arr_nested_1_val")
        field_ref("arr_nested_2_id") field_ref("arr_nested_2_val")
    end_call(print3)

    -- ========================================================================
    -- PASS 4: String-path pointer storage and extraction (via Lua)
    -- ========================================================================

    se_log("=== PASS 4: Pointer Storage and Extraction (Lua) ===")
    se_increment_field("pass_number", 1)

    lua_dict_store_ptr("dict_string", "integers", "sub_integers")
    lua_dict_store_ptr("dict_string", "floats", "sub_floats")
    lua_dict_store_ptr("dict_string", "items.0", "sub_nested_0")
    lua_dict_store_ptr("dict_string", "items.1", "sub_nested_1")

    lua_dict_extract_int("sub_integers", "positive", "ptr_int_pos")
    lua_dict_extract_int("sub_integers", "negative", "ptr_int_neg")
    lua_dict_extract_float("sub_floats", "pi", "ptr_float_pi")
    lua_dict_extract_float("sub_floats", "negative", "ptr_float_neg")
    lua_dict_extract_uint("sub_nested_0", "id", "ptr_n0_id")
    lua_dict_extract_float("sub_nested_0", "value", "ptr_n0_val")
    lua_dict_extract_uint("sub_nested_1", "id", "ptr_n1_id")
    lua_dict_extract_float("sub_nested_1", "value", "ptr_n1_val")

    local print4 = o_call("USER_PRINT_POINTER_RESULTS")
        str("Pass 4 - String Pointer Extraction (Lua)")
        field_ref("pass_number")
        field_ref("ptr_int_pos") field_ref("ptr_int_neg")
        field_ref("ptr_float_pi") field_ref("ptr_float_neg")
        field_ref("ptr_n0_id") field_ref("ptr_n0_val")
        field_ref("ptr_n1_id") field_ref("ptr_n1_val")
    end_call(print4)

    -- ========================================================================
    -- PASS 5: Hash-path pointer storage and extraction (via Lua)
    -- ========================================================================

    se_log("=== PASS 5: Hash Pointer Storage and Extraction (Lua) ===")
    se_increment_field("pass_number", 1)

    se_set_field("ptr_int_pos", 0) se_set_field("ptr_int_neg", 0)
    se_set_field("ptr_float_pi", 0) se_set_field("ptr_float_neg", 0)
    se_set_field("ptr_n0_id", 0) se_set_field("ptr_n0_val", 0)
    se_set_field("ptr_n1_id", 0) se_set_field("ptr_n1_val", 0)

    lua_dict_store_ptr_h("dict_hash", {"integers"}, "sub_integers")
    lua_dict_store_ptr_h("dict_hash", {"floats"}, "sub_floats")
    lua_dict_store_ptr_h("dict_hash", {"items", "0"}, "sub_nested_0")
    lua_dict_store_ptr_h("dict_hash", {"items", "1"}, "sub_nested_1")

    lua_dict_extract_int_h("sub_integers", {"positive"}, "ptr_int_pos")
    lua_dict_extract_int_h("sub_integers", {"negative"}, "ptr_int_neg")
    lua_dict_extract_float_h("sub_floats", {"pi"}, "ptr_float_pi")
    lua_dict_extract_float_h("sub_floats", {"negative"}, "ptr_float_neg")
    lua_dict_extract_uint_h("sub_nested_0", {"id"}, "ptr_n0_id")
    lua_dict_extract_float_h("sub_nested_0", {"value"}, "ptr_n0_val")
    lua_dict_extract_uint_h("sub_nested_1", {"id"}, "ptr_n1_id")
    lua_dict_extract_float_h("sub_nested_1", {"value"}, "ptr_n1_val")

    local print5 = o_call("USER_PRINT_POINTER_RESULTS")
        str("Pass 5 - Hash Pointer Extraction (Lua)")
        field_ref("pass_number")
        field_ref("ptr_int_pos") field_ref("ptr_int_neg")
        field_ref("ptr_float_pi") field_ref("ptr_float_neg")
        field_ref("ptr_n0_id") field_ref("ptr_n0_val")
        field_ref("ptr_n1_id") field_ref("ptr_n1_val")
    end_call(print5)

    -- ========================================================================
    -- Final verification
    -- ========================================================================

    local verify = o_call("USER_VERIFY_RESULTS")
    end_call(verify)

    se_return_terminate()
end)

end_tree("json_test")

return end_module(mod)