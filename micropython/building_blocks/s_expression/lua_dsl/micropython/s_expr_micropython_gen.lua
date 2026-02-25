-- ============================================================================
-- s_expr_micropython_gen.lua
-- S-Expression Engine MicroPython Class Generator
--
-- Generates a self-contained .py file with:
--   - Param stream as bytes constant (flash-resident when frozen)
--   - Blackboard field offsets as class constants
--   - String table as tuple
--   - Default values as bytes constant
--   - Dispatch table: tuple of bound methods indexed by func_index
--   - Stub methods for user functions
--
-- Usage:
--   local mp_gen = require("s_expr_micropython_gen")
--   local gen = mp_gen.MicroPythonGenerator.new(module_data, binary_gen)
--   local py_source = gen:generate("door_controller")
--
-- The binary_gen (BinaryModuleGenerator) is needed to access the
-- func hash index tables that map function names to indices.
-- ============================================================================

local ffi = require("ffi")
local bit = require("bit")

local MPG = {}

-- ============================================================================
-- Builtin name -> base class method mapping
-- ============================================================================

local BUILTIN_METHOD = {
    SE_NOP                  = "_se_nop",
    SE_SEQUENCE             = "_se_sequence",
    SE_SEQUENCE_ONCE        = "_se_sequence",
    SE_FORK                 = "_se_fork",
    SE_FORK_JOIN            = "_se_fork",
    SE_IF_THEN_ELSE         = "_se_if_then_else",
    SE_CHAIN_FLOW           = "_se_chain_flow",
    SE_FUNCTION_INTERFACE   = "_se_function_interface",
    SE_STATE_MACHINE        = "_se_state_machine",
    SE_FIELD_DISPATCH       = "_se_field_dispatch",
    SE_WHILE                = "_se_while",
    SE_COND                 = "_se_cond",
    SE_SET_FIELD            = "_se_set_field",
    SE_INC_FIELD            = "_se_inc_field",
    SE_DEC_FIELD            = "_se_dec_field",
    SE_LOG                  = "_se_log",
    SE_LOG_INT              = "_se_log",
    SE_LOG_FLOAT            = "_se_log",
    SE_TRUE                 = "_se_true",
    SE_FALSE                = "_se_false",
    SE_CHECK_EVENT          = "_se_check_event",
    SE_FIELD_EQ             = "_se_field_eq",
    SE_FIELD_NE             = "_se_field_ne",
    SE_FIELD_GT             = "_se_field_gt",
    SE_FIELD_GE             = "_se_field_ge",
    SE_FIELD_LT             = "_se_field_lt",
    SE_FIELD_LE             = "_se_field_le",
    SE_PRED_OR              = "_se_pred_or",
    SE_PRED_AND             = "_se_pred_and",
    SE_PRED_NOT             = "_se_pred_not",
    SE_PRED_NOR             = "_se_pred_not",   -- TODO: proper impl
    SE_PRED_NAND            = "_se_pred_not",   -- TODO: proper impl
    SE_QUEUE_EVENT          = "_se_queue_event",
    SE_TRIGGER_ON_CHANGE    = "_se_nop",        -- TODO
    SE_TICK_DELAY           = "_se_nop",        -- platform-specific
    SE_TIME_DELAY           = "_se_nop",        -- platform-specific
    SE_WAIT                 = "_se_nop",        -- platform-specific
    SE_WAIT_EVENT           = "_se_nop",        -- platform-specific
    SE_WAIT_TIMEOUT         = "_se_nop",        -- platform-specific
    SE_VERIFY               = "_se_nop",        -- platform-specific
    SE_VERIFY_AND_CHECK_ELAPSED_TIME   = "_se_nop",  -- platform-specific
    SE_VERIFY_AND_CHECK_ELAPSED_EVENTS = "_se_nop",  -- platform-specific
    SE_EVENT_DISPATCH       = "_se_state_machine",
    SE_PUSH_STACK           = "_se_nop",        -- TODO
    SE_LOG_STACK            = "_se_nop",        -- TODO
}

-- ============================================================================
-- GENERATOR CLASS
-- ============================================================================

local MicroPythonGenerator = {}
MicroPythonGenerator.__index = MicroPythonGenerator

function MicroPythonGenerator.new(module_data, binary_gen)
    local self = setmetatable({}, MicroPythonGenerator)
    self.module = module_data
    self.bin_gen = binary_gen
    return self
end

-- ============================================================================
-- Build combined function list with indices
-- The binary generator assigns indices in this order:
--   oneshot_funcs[0..N-1], main_funcs[N..M-1], pred_funcs[M..P-1]
-- ============================================================================

function MicroPythonGenerator:build_func_table()
    local mod = self.module
    local funcs = {}  -- { index, name, category, is_builtin, method_name }

    local idx = 0

    for _, name in ipairs(mod.oneshot_funcs) do
        local builtin_method = BUILTIN_METHOD[name]
        table.insert(funcs, {
            index = idx,
            name = name,
            category = "oneshot",
            is_builtin = (builtin_method ~= nil),
            method_name = builtin_method or ("_user_" .. name:lower()),
        })
        idx = idx + 1
    end

    for _, name in ipairs(mod.main_funcs) do
        local builtin_method = BUILTIN_METHOD[name]
        table.insert(funcs, {
            index = idx,
            name = name,
            category = "main",
            is_builtin = (builtin_method ~= nil),
            method_name = builtin_method or ("_user_" .. name:lower()),
        })
        idx = idx + 1
    end

    for _, name in ipairs(mod.pred_funcs) do
        local builtin_method = BUILTIN_METHOD[name]
        table.insert(funcs, {
            index = idx,
            name = name,
            category = "pred",
            is_builtin = (builtin_method ~= nil),
            method_name = builtin_method or ("_user_" .. name:lower()),
        })
        idx = idx + 1
    end

    return funcs
end

-- ============================================================================
-- GENERATE PYTHON SOURCE
-- ============================================================================

function MicroPythonGenerator:generate(class_name)
    local mod = self.module
    local lines = {}
    local funcs = self:build_func_table()

    local function ln(s) table.insert(lines, s) end
    local function blank() table.insert(lines, "") end

    -- Header
    ln("# ==========================================================================")
    ln("# " .. class_name .. ".py")
    ln("# Generated S-Expression tree class for: " .. mod.name)
    ln("# DO NOT EDIT - Generated by s_expr_micropython_gen.lua")
    ln("#")
    ln("# Freeze this module into firmware to keep PARAMS in flash.")
    ln("# ==========================================================================")
    blank()
    ln("from se_engine_base import SeEngineBase, CONTINUE, HALT, RESET")
    blank()

    -- Class definition
    local py_class = class_name:sub(1,1):upper() .. class_name:sub(2)
    ln("class " .. py_class .. "(SeEngineBase):")
    blank()

    -- ---- Param stream as bytes constant ----
    local bytes_data, size = self.bin_gen:generate()
    ln("    # Param stream (" .. size .. " bytes) - lives in flash when frozen")
    ln("    PARAMS = (")

    local row = {}
    local row_start = true
    for i, b in ipairs(bytes_data) do
        table.insert(row, string.format("\\x%02x", b))
        if #row == 20 or i == size then
            local prefix = row_start and "        b'" or "        b'"
            ln(prefix .. table.concat(row, "") .. "'")
            row = {}
            row_start = false
        end
    end
    ln("    )")
    blank()

    -- ---- Blackboard constants ----
    if #mod.record_order > 0 then
        local rec_name = mod.record_order[1]
        local rec = mod.records[rec_name]

        ln("    # Blackboard: " .. rec_name .. " (size=" .. rec.size .. ", align=" .. rec.align .. ")")
        ln("    BB_SIZE = " .. rec.size)
        ln("    BB_ALIGN = " .. rec.align)
        blank()

        ln("    # Field offsets and sizes: (offset, size)")
        for _, field in ipairs(rec.fields) do
            local const_name = "F_" .. field.name:upper()
            ln(string.format("    %s = (%d, %d)  # %s",
                const_name, field.offset, field.size, field.type))
        end
        blank()
    else
        ln("    BB_SIZE = 0")
        ln("    BB_ALIGN = 4")
        blank()
    end

    -- ---- Pointer count ----
    local total_ptrs = 0
    for _, name in ipairs(mod.tree_order) do
        total_ptrs = total_ptrs + (mod.trees[name].pointer_count or 0)
    end
    ln("    PTR_COUNT = " .. total_ptrs)
    blank()

    -- ---- String table ----
    if #mod.string_table > 0 then
        ln("    # String table (" .. #mod.string_table .. " entries)")
        ln("    STRINGS = (")
        for i, s in ipairs(mod.string_table) do
            local escaped = s:gsub("\\", "\\\\"):gsub("'", "\\'"):gsub("\n", "\\n")
            local comma = (i < #mod.string_table) and "," or ","
            ln("        '" .. escaped .. "'" .. comma)
        end
        ln("    )")
    else
        ln("    STRINGS = ()")
    end
    blank()

    -- ---- Defaults ----
    if #mod.const_order > 0 then
        local const_name = mod.const_order[1]
        local cnst = mod.constants[const_name]
        if cnst and cnst.data_bytes then
            ln("    # Default values: " .. const_name)
            ln("    DEFAULTS = (")
            local dbytes = cnst.data_bytes
            row = {}
            for i, b in ipairs(dbytes) do
                table.insert(row, string.format("\\x%02x", b))
                if #row == 20 or i == #dbytes then
                    ln("        b'" .. table.concat(row, "") .. "'")
                    row = {}
                end
            end
            ln("    )")
        else
            ln("    DEFAULTS = None")
        end
    else
        ln("    DEFAULTS = None")
    end
    blank()

    -- ---- Event ID constants ----
    if mod.events and #mod.events > 0 then
        ln("    # Event IDs")
        for _, evt in ipairs(mod.events) do
            ln(string.format("    %s = 0x%04X", evt.name, evt.id))
        end
        blank()
    end

    -- ---- Function index table (comment block for reference) ----
    ln("    # Function dispatch table")
    ln("    # Index | Category | Name -> Method")
    for _, f in ipairs(funcs) do
        local tag = f.is_builtin and "builtin" or "USER"
        ln(string.format("    # [%3d] %-8s %-8s %s -> %s",
            f.index, tag, f.category, f.name, f.method_name))
    end
    blank()

    -- ---- _build_dispatch ----
    ln("    def _build_dispatch(self):")
    ln("        return (")
    for i, f in ipairs(funcs) do
        local comma = ","
        ln(string.format("            self.%s%s  # [%d] %s",
            f.method_name, comma, f.index, f.name))
    end
    ln("        )")
    blank()

    -- ---- User function stubs ----
    local user_funcs = {}
    for _, f in ipairs(funcs) do
        if not f.is_builtin then
            table.insert(user_funcs, f)
        end
    end

    if #user_funcs > 0 then
        ln("    # ==================================================================")
        ln("    # USER FUNCTIONS - implement these")
        ln("    # ==================================================================")
        blank()

        for _, f in ipairs(user_funcs) do
            ln("    def " .. f.method_name .. "(self, pos, content_count, ev_type, ev_id, ev_data):")
            ln('        """' .. f.category:upper() .. ": " .. f.name .. '"""')
            if f.category == "pred" then
                ln("        # Return 1 (true) or 0 (false)")
                ln("        raise NotImplementedError('" .. f.name .. "')")
            elseif f.category == "oneshot" then
                ln("        # No return value needed (returns CONTINUE)")
                ln("        raise NotImplementedError('" .. f.name .. "')")
                ln("        return CONTINUE")
            else
                ln("        # Return CONTINUE, HALT, RESET, etc.")
                ln("        raise NotImplementedError('" .. f.name .. "')")
            end
            blank()
        end
    end

    -- ---- Convenience field accessors ----
    if #mod.record_order > 0 then
        local rec = mod.records[mod.record_order[1]]

        ln("    # ==================================================================")
        ln("    # FIELD ACCESSORS (convenience, not required)")
        ln("    # ==================================================================")
        blank()

        for _, field in ipairs(rec.fields) do
            if not field.is_ptr64 then
                local getter, setter
                if field.type == "float" then
                    getter = "bb_f32"
                    setter = "bb_set_f32"
                elseif field.type == "double" then
                    getter = "bb_f64"
                    setter = "bb_set_f64"
                else
                    getter = "bb_i32"
                    setter = "bb_set_i32"
                end

                ln(string.format("    @property"))
                ln(string.format("    def %s(self):", field.name))
                ln(string.format("        return self.%s(%d)", getter, field.offset))
                blank()
                ln(string.format("    @%s.setter", field.name))
                ln(string.format("    def %s(self, value):", field.name))
                ln(string.format("        self.%s(%d, value)", setter, field.offset))
                blank()
            end
        end
    end

    return table.concat(lines, "\n")
end

MPG.MicroPythonGenerator = MicroPythonGenerator
MPG.BUILTIN_METHOD = BUILTIN_METHOD

return MPG

