-- ============================================================================
-- app_lua_functions.lua
-- Registers Lua functions into the se_lua bridge by hash
--
-- The bridge pushes params as tables with type-specific fields:
--   .type      = raw type byte
--   .opcode    = opcode (lower 6 bits)
--   .int_val   = for S_EXPR_PARAM_INT  (opcode 0x00)
--   .uint_val  = for S_EXPR_PARAM_UINT (opcode 0x01)
--   .float_val = for S_EXPR_PARAM_FLOAT (opcode 0x02)
--   .str_hash  = for S_EXPR_PARAM_STR_HASH (opcode 0x03)
--   .field_offset, .field_size = for FIELD refs (opcode 0x0B)
--   .node_index, .func_index  = for function refs (0x08-0x0A)
--   .brace_idx, .parent_offset = for brace tokens
--   .str_index, .str_len       = for string table refs (0x0D)
--   .const_index, .const_size  = for const refs (0x0E)
--   .stack_offset              = for stack ops (0x18, 0x19)
-- ============================================================================

local bridge = se_bridge  -- global table set by se_lua_bridge_init

-- ============================================================================
-- Helper: print a single parameter
-- ============================================================================

local OPCODE_NAMES = {
    [0x00] = "INT",
    [0x01] = "UINT",
    [0x02] = "FLOAT",
    [0x03] = "STR_HASH",
    [0x04] = "SLOT",
    [0x05] = "OPEN",
    [0x06] = "CLOSE",
    [0x07] = "OPEN_CALL",
    [0x08] = "ONESHOT",
    [0x09] = "MAIN",
    [0x0A] = "PRED",
    [0x0B] = "FIELD",
    [0x0C] = "RESULT",
    [0x0D] = "STR_IDX",
    [0x0E] = "CONST_REF",
    [0x10] = "OPEN_DICT",
    [0x11] = "CLOSE_DICT",
    [0x12] = "OPEN_KEY",
    [0x13] = "CLOSE_KEY",
    [0x14] = "OPEN_ARRAY",
    [0x15] = "CLOSE_ARRAY",
    [0x16] = "OPEN_TUPLE",
    [0x17] = "CLOSE_TUPLE",
    [0x18] = "STACK_TOS",
    [0x19] = "STACK_LOCAL",
    [0x1A] = "NULL",
    [0x1B] = "STACK_PUSH",
    [0x1C] = "STACK_POP",
}

local function print_param(idx, p)
    local opcode = p.opcode
    local name = OPCODE_NAMES[opcode] or string.format("UNKNOWN(0x%02X)", opcode)

    if opcode == 0x00 then
        print(string.format("  param[%d]: %s  int_val=%d", idx, name, p.int_val or 0))
    elseif opcode == 0x01 then
        print(string.format("  param[%d]: %s  uint_val=%u", idx, name, p.uint_val or 0))
    elseif opcode == 0x02 then
        print(string.format("  param[%d]: %s  float_val=%g", idx, name, p.float_val or 0))
    elseif opcode == 0x03 then
        print(string.format("  param[%d]: %s  str_hash=0x%08X", idx, name, p.str_hash or 0))
    elseif opcode == 0x0B then
        print(string.format("  param[%d]: %s  field_offset=%d  field_size=%d",
              idx, name, p.field_offset or 0, p.field_size or 0))
    elseif opcode >= 0x08 and opcode <= 0x0A then
        print(string.format("  param[%d]: %s  node_index=%d  func_index=%d",
              idx, name, p.node_index or 0, p.func_index or 0))
    elseif opcode == 0x0D then
        print(string.format("  param[%d]: %s  str_index=%d  str_len=%d",
              idx, name, p.str_index or 0, p.str_len or 0))
    elseif opcode == 0x0E then
        print(string.format("  param[%d]: %s  const_index=%d  const_size=%d",
              idx, name, p.const_index or 0, p.const_size or 0))
    elseif opcode == 0x18 or opcode == 0x19 then
        print(string.format("  param[%d]: %s  stack_offset=%d",
              idx, name, p.stack_offset or 0))
    elseif opcode == 0x0C then
        print(string.format("  param[%d]: %s  result_code=%d",
              idx, name, p.result_code or 0))
    else
        print(string.format("  param[%d]: %s  (type=0x%02X)", idx, name, p.type))
    end
end

local function print_all_params(label, params)
    print(string.format("[%s] %d parameters:", label, #params))
    for i, p in ipairs(params) do
        print_param(i, p)
    end
end

-- ============================================================================
-- CFL_DISABLE_CHILDREN (oneshot)
-- ============================================================================

bridge.register(0x5839B05B, "oneshot", function(inst, params, event_type, event_id)
    print_all_params("CFL_DISABLE_CHILDREN", params)
    print(string.format("  event_type=0x%04X  event_id=%d", event_type, event_id))

    -- local node_count = inst:get_node_count()
    -- disable all child nodes...
end)

-- ============================================================================
-- CFL_ENABLE_CHILD (oneshot)
-- ============================================================================

bridge.register(0xD42E3453, "oneshot", function(inst, params, event_type, event_id)
    print_all_params("CFL_ENABLE_CHILD", params)
    print(string.format("  event_type=0x%04X  event_id=%d", event_type, event_id))

    -- Extract child index from first param (expected: INT with the child index)
    local child_idx = params[1] and params[1].int_val or nil
    if child_idx then
        print(string.format("  -> enabling child index: %d", child_idx))
        -- inst:enable_child(child_idx)
    else
        print("  -> WARNING: no child index in params[1]")
    end
end)

print("app_lua_functions.lua: registered 2 functions")

