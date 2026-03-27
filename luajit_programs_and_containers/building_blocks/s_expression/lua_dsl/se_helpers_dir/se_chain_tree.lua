--============================================================================
-- se_chain_tree.lua
-- CFL (ChainTree) bridge DSL helpers for S-Expression engine
-- Maps to C cfl_se_oneshot_functions.c / cfl_se_pred_functions.c /
-- cfl_se_main_functions.c
--============================================================================

-- Register all CFL builtin names
register_builtin("CFL_LOG")
register_builtin("CFL_ENABLE_CHILDREN")
register_builtin("CFL_DISABLE_CHILDREN")
register_builtin("CFL_ENABLE_CHILD")
register_builtin("CFL_DISABLE_CHILD")
register_builtin("CFL_INTERNAL_EVENT")
register_builtin("CFL_EXCEPTION")
register_builtin("CFL_SET_BITS")
register_builtin("CFL_CLEAR_BITS")
register_builtin("CFL_JSON_READ_INT")
register_builtin("CFL_JSON_READ_UINT")
register_builtin("CFL_JSON_READ_FLOAT")
register_builtin("CFL_JSON_READ_BOOL")
register_builtin("CFL_JSON_READ_STRING_PTR")
register_builtin("CFL_JSON_READ_STRING_BUF")
register_builtin("CFL_COPY_CONST")
register_builtin("CFL_COPY_CONST_FULL")
register_builtin("CFL_READ_BIT")
register_builtin("CFL_S_BIT_OR")
register_builtin("CFL_S_BIT_AND")
register_builtin("CFL_S_BIT_NOR")
register_builtin("CFL_S_BIT_NAND")
register_builtin("CFL_S_BIT_XOR")
register_builtin("CFL_WAIT_CHILD_DISABLED")

-- ============================================================================
-- Oneshot helpers (io_call — init-only one-shots)
-- ============================================================================

-- Disable all ChainTree children of the current node (io_call: runs once at init)
function cfl_i_disable_children()
    local c = io_call("CFL_DISABLE_CHILDREN")
    end_call(c)
end

-- Enable all ChainTree children (used as callback in trigger_on_change etc.)
function cfl_enable_children()
    local c = o_call("CFL_ENABLE_CHILDREN")
    end_call(c)
end

-- Disable all ChainTree children (used as callback)
function cfl_disable_children()
    local c = o_call("CFL_DISABLE_CHILDREN")
    end_call(c)
end

-- Enable a specific ChainTree child by index
function cfl_enable_child(child_index)
    local c = o_call("CFL_ENABLE_CHILD")
        int(child_index)
    end_call(c)
end

-- Disable a specific ChainTree child by index
function cfl_disable_child(child_index)
    local c = o_call("CFL_DISABLE_CHILD")
        int(child_index)
    end_call(c)
end

-- Post event to ChainTree event queue
function cfl_internal_event(event_type, event_data)
    local c = o_call("CFL_INTERNAL_EVENT")
        int(event_type)
        int(event_data)
    end_call(c)
end

-- Set bits in ChainTree shadow bitmask
function cfl_set_bits(...)
    local c = o_call("CFL_SET_BITS")
    for _, bit_idx in ipairs({...}) do
        int(bit_idx)
    end
    end_call(c)
end

-- Clear bits in ChainTree shadow bitmask
function cfl_clear_bits(...)
    local c = o_call("CFL_CLEAR_BITS")
    for _, bit_idx in ipairs({...}) do
        int(bit_idx)
    end
    end_call(c)
end

-- JSON read operations
function cfl_json_read_uint(field_name, json_path)
    local c = o_call("CFL_JSON_READ_UINT")
        field_ref(field_name)
        str_ptr(json_path)
    end_call(c)
end

function cfl_json_read_float(field_name, json_path)
    local c = o_call("CFL_JSON_READ_FLOAT")
        field_ref(field_name)
        str_ptr(json_path)
    end_call(c)
end

function cfl_json_read_bool(field_name, json_path)
    local c = o_call("CFL_JSON_READ_BOOL")
        field_ref(field_name)
        str_ptr(json_path)
    end_call(c)
end

function cfl_json_read_string_buf(field_name, json_path)
    local c = o_call("CFL_JSON_READ_STRING_BUF")
        field_ref(field_name)
        str_ptr(json_path)
    end_call(c)
end

function cfl_json_read_string_ptr(field_name, json_path)
    local c = o_call("CFL_JSON_READ_STRING_PTR")
        field_ref(field_name)
        str_ptr(json_path)
    end_call(c)
end

-- Copy constant data
function cfl_copy_const(field_name, const_name)
    local c = o_call("CFL_COPY_CONST")
        field_ref(field_name)
        str_ptr(const_name)
    end_call(c)
end

function cfl_copy_const_full(const_name)
    local c = o_call("CFL_COPY_CONST_FULL")
        str_ptr(const_name)
    end_call(c)
end

-- ============================================================================
-- Predicate helpers
-- ============================================================================

-- Read a single bit from ChainTree bitmask
function cfl_read_bit(bit_index)
    local c = p_call("CFL_READ_BIT")
        int(bit_index)
    end_call(c)
end

-- Compound boolean predicates on bitmask bits
-- Usage: local p = cfl_s_bit_or_start()
--            cfl_bit_entry(0, 1)
--        end_call(p)
function cfl_s_bit_or_start()
    return p_call_composite("CFL_S_BIT_OR")
end

function cfl_s_bit_and_start()
    return p_call_composite("CFL_S_BIT_AND")
end

function cfl_s_bit_nor_start()
    return p_call_composite("CFL_S_BIT_NOR")
end

function cfl_s_bit_nand_start()
    return p_call_composite("CFL_S_BIT_NAND")
end

function cfl_s_bit_xor_start()
    return p_call_composite("CFL_S_BIT_XOR")
end

-- Add bit indices as parameters to a compound predicate
function cfl_bit_entry(...)
    for _, bit_idx in ipairs({...}) do
        int(bit_idx)
    end
end

-- ============================================================================
-- Main function helpers
-- ============================================================================

-- Wait until a ChainTree child node is disabled
function cfl_wait_child_disabled(child_node_index)
    local c = m_call("CFL_WAIT_CHILD_DISABLED")
        int(child_node_index)
    end_call(c)
end
