-- ============================================================================
-- sg_expr.lua
-- scan_graph Expression Module v0.2
--
-- Bridges scan_graph buffer/template/level metadata to the existing
-- s_expr_compiler. Builds frame_vars from scan_graph type information,
-- assigns stack frame slots, and delegates to quad_expr/quad_pred/quad_multi
-- for all parsing, constant folding, type inference, and code generation.
--
-- Does NOT duplicate any compiler logic. The full chain is:
--
--   sg_expr.lua            -> builds frame_vars, allocates stack frame
--   s_expr_compiler.lua    -> tokenize, parse, fold, infer types, emit ops
--   se_quad_ops.lua        -> quad_fgt() -> se_quad(FCMP_GT, ...)
--   se_p_quad_ops.lua      -> p_fcmp_gt() -> se_p_quad(FCMP_GT, ...)
--
-- Three existing modules are unchanged:
--   se_quad_ops.lua, se_p_quad_ops.lua, s_expr_compiler.lua
--
-- Stack frame layout for a compiled template:
--
--   locals[0..P-1]              = inputs (pushed by caller)
--   locals[P..P+Q-1]           = params (init from defaults)
--   locals[P+Q..P+Q+R-1]       = outputs (returned to caller)
--   locals[P+Q+R..P+Q+R+I-1]   = intermediates (expression-generated)
--   tos[0..S-1]                 = scratch (expression temporaries)
--
--   num_params    = P
--   num_locals    = Q + R + I
--   scratch_depth = S
--   return_vars   = output slot indices
--
-- Loaded by scan_graph DSL runtime via require("sg_expr")
-- ============================================================================

local sg_expr = {}

-- ============================================================================
-- TYPE MAPPING
-- scan_graph type -> s_expr_compiler type annotation
--
-- The expression compiler uses ":float" or ":int" annotations in frame_vars
-- to drive type inference and quad function selection. This table converts
-- scan_graph's richer type set to the compiler's two categories.
--
-- bool maps to int because the compiler represents booleans as int (0/1).
-- The compiler's infer_float() returns false for :int vars, causing it to
-- select int_cmp_ops / int_arith_ops / logic_ops. For :float vars it
-- selects float_cmp_ops / float_arith_ops.
-- ============================================================================

local sg_type_map = {
    bool    = "int",
    uint8   = "int",
    int8    = "int",
    uint16  = "int",
    int16   = "int",
    uint32  = "int",
    int32   = "int",
    float32 = "float",
    float64 = "float",
}

-- ============================================================================
-- TEMPLATE CONTEXT
--
-- Collects all names visible to expressions within an operator template
-- and assigns stack frame slot indices.
--
-- Slot ordering:
--   [inputs] [params] [outputs] [intermediates]
--
-- Inputs occupy the first P slots (num_params in se_frame_allocate terms)
-- because they are pushed onto the stack by the caller before the frame
-- is entered. Everything else is a local.
-- ============================================================================

function sg_expr.make_template_context(template_def)
    local ctx = {
        slots = {},           -- ordered list of {name, sg_type, role}
        name_to_slot = {},    -- name -> slot index (0-based)
        num_inputs = 0,
        num_params = 0,
        num_outputs = 0,
        num_intermediates = 0,
    }

    local function add_slot(name, sg_type, role)
        if ctx.name_to_slot[name] then
            dsl_error("sg_expr: duplicate name '" .. name .. "'")
        end
        local idx = #ctx.slots
        table.insert(ctx.slots, {name = name, sg_type = sg_type, role = role})
        ctx.name_to_slot[name] = idx
        return idx
    end

    -- Inputs first: these are the stack "params" pushed by caller
    for _, input in ipairs(template_def.inputs or {}) do
        add_slot(input.name, input.sg_type, "input")
        ctx.num_inputs = ctx.num_inputs + 1
    end

    -- Template parameters as locals (initialized from default constants)
    for _, param in ipairs(template_def.params or {}) do
        add_slot(param.name, param.sg_type, "param")
        ctx.num_params = ctx.num_params + 1
    end

    -- Outputs as locals (written by expressions, returned to caller)
    for _, output in ipairs(template_def.outputs or {}) do
        add_slot(output.name, output.sg_type, "output")
        ctx.num_outputs = ctx.num_outputs + 1
    end

    -- Stack frame dimensions (intermediates added later)
    ctx.stack_num_params = ctx.num_inputs
    ctx.stack_num_locals = ctx.num_params + ctx.num_outputs

    return ctx
end

-- ============================================================================
-- Add an intermediate variable to the context.
-- Called during the first pass over expressions when a destination name
-- is encountered that is not already in the context.
-- ============================================================================

function sg_expr.add_intermediate(ctx, name, sg_type)
    if ctx.name_to_slot[name] then return end

    local idx = #ctx.slots
    table.insert(ctx.slots, {name = name, sg_type = sg_type, role = "intermediate"})
    ctx.name_to_slot[name] = idx
    ctx.num_intermediates = ctx.num_intermediates + 1
    ctx.stack_num_locals = ctx.num_params + ctx.num_outputs + ctx.num_intermediates
end

-- ============================================================================
-- Build frame_vars table from context.
-- Maps each slot to stack_local(idx), sets _is_float and _type metadata.
-- Scratch variables map to stack_tos(offset).
-- ============================================================================

function sg_expr.build_frame_vars(ctx, scratch_count)
    local local_decls = {}
    local scratch_decls = {}
    local scratch_names = {}

    -- All slots become locals in declaration order
    for _, slot in ipairs(ctx.slots) do
        local expr_type = sg_type_map[slot.sg_type] or "int"
        table.insert(local_decls, slot.name .. ":" .. expr_type)
    end

    -- Scratch on TOS (typed as float to handle any promotion)
    for i = 1, scratch_count do
        local name = "_sg_t" .. (i - 1)
        table.insert(scratch_decls, name .. ":float")
        table.insert(scratch_names, name)
    end

    local vars = frame_vars(local_decls, scratch_decls)
    return vars, scratch_names, scratch_count
end

-- ============================================================================
-- Compute return_vars: the 0-based indices of output slots.
-- These are passed to se_stack_frame_instance / se_call so the caller
-- can retrieve the template's output values after execution.
-- ============================================================================

function sg_expr.get_return_vars(ctx)
    local rv = {}
    for i, slot in ipairs(ctx.slots) do
        if slot.role == "output" then
            table.insert(rv, i - 1)  -- 0-indexed
        end
    end
    return rv
end

-- ============================================================================
-- Estimate scratch depth from a list of expressions.
-- Heuristic: max parenthesis nesting depth + 1, minimum 2.
-- This matches the scratch consumption pattern of the Pratt parser's
-- binary tree emission in s_expr_compiler.
-- ============================================================================

local function estimate_scratch(expressions)
    local max_depth = 0
    for _, expr_str in ipairs(expressions) do
        local depth = 0
        for i = 1, #expr_str do
            local ch = expr_str:sub(i, i)
            if ch == "(" then
                depth = depth + 1
                if depth > max_depth then max_depth = depth end
            elseif ch == ")" then
                depth = depth - 1
            end
        end
    end
    return math.max(max_depth + 1, 2)
end

-- ============================================================================
-- Infer the scan_graph type of an intermediate variable from its expression.
--
-- Rules:
--   - Comparisons (>, <, ==, etc.) and logical (&&, ||) produce bool
--   - If any referenced float variable appears in the expression, float64
--   - Otherwise int32
-- ============================================================================

local function infer_intermediate_type(expr_str, ctx)
    -- Check for comparison operators
    if expr_str:match("&&") or expr_str:match("||") or expr_str:match("!")
       or expr_str:match("[><=!]=") or expr_str:match("[><]") then
        return "bool"
    end

    -- Check if any referenced name is float
    for _, slot in ipairs(ctx.slots) do
        if sg_type_map[slot.sg_type] == "float" then
            -- Word boundary match via Lua frontier patterns
            if expr_str:match("%f[%w_]" .. slot.name .. "%f[^%w_]") then
                return "float64"
            end
        end
    end

    return "int32"
end

-- ============================================================================
-- sg_expr.compile_template
--
-- Main entry point. Compiles a complete operator template definition
-- into a stack-frame-wrapped closure.
--
-- Parameters:
--   template_def: table with:
--     .inputs  = {{name=, sg_type=}, ...}
--     .params  = {{name=, sg_type=, default=}, ...}  (optional)
--     .outputs = {{name=, sg_type=}, ...}
--
--   expressions: ordered list of expression strings, e.g.:
--     {"over_current = current > current_limit",
--      "is_healthy = !electrical_fault && !thermal_fault"}
--
-- Returns:
--   emitter_fn   - closure that emits se_frame_allocate + all quad ops
--   return_vars  - list of 0-based output slot indices
--
-- The emitter closure, when called, produces:
--   se_frame_allocate(num_params, num_locals, scratch_depth,
--       param_init_fn,   -- quad_mov to load param defaults
--       expr_body_fn     -- compiled quad_expr closures
--   )
-- ============================================================================

function sg_expr.compile_template(template_def, expressions)
    local ctx = sg_expr.make_template_context(template_def)

    -- First pass: scan expressions for intermediate destination names
    for _, expr_str in ipairs(expressions) do
        -- Match simple assignment destination (not @field)
        local dest = expr_str:match("^%s*([%w_]+)%s*=")
            or expr_str:match("^%s*([%w_]+)%s*[+%-*/%%&|^]=")
        if dest and not ctx.name_to_slot[dest] then
            local sg_type = infer_intermediate_type(expr_str, ctx)
            sg_expr.add_intermediate(ctx, dest, sg_type)
        end
    end

    -- Compute scratch depth
    local scratch_count = estimate_scratch(expressions)

    -- Build frame_vars for the expression compiler
    local vars, scratch_names = sg_expr.build_frame_vars(ctx, scratch_count)

    -- Compile each expression via s_expr_compiler
    local expr_closures = {}
    for _, expr_str in ipairs(expressions) do
        table.insert(expr_closures, quad_expr(expr_str, vars, scratch_names))
    end

    -- Build param initialization closures
    -- Params are locals that must be loaded from constant defaults
    local param_inits = {}
    for _, slot in ipairs(ctx.slots) do
        if slot.role == "param" then
            -- Find the param definition with its default value
            local param_def = nil
            for _, p in ipairs(template_def.params or {}) do
                if p.name == slot.name then param_def = p; break end
            end
            if param_def and param_def.default ~= nil then
                local val_fn
                if sg_type_map[param_def.sg_type] == "float" then
                    val_fn = float_val(param_def.default)
                else
                    val_fn = uint_val(param_def.default)
                end
                local dest_fn = vars[slot.name]
                table.insert(param_inits, quad_mov(val_fn, dest_fn))
            end
        end
    end

    -- Output slot indices
    local return_vars = sg_expr.get_return_vars(ctx)

    -- Stack frame dimensions
    local num_params = ctx.stack_num_params
    local num_locals = ctx.stack_num_locals
    local total_scratch = scratch_count

    -- Return the emitter closure and return_vars
    local function emitter()
        se_frame_allocate(num_params, num_locals, total_scratch,
            -- Param initialization
            function()
                for _, init_fn in ipairs(param_inits) do
                    init_fn()
                end
            end,
            -- Expression body
            function()
                for _, expr_fn in ipairs(expr_closures) do
                    expr_fn()
                end
            end
        )
    end

    return emitter, return_vars
end

-- ============================================================================
-- sg_expr.compile_level_operator
--
-- Lighter weight compilation for a single operator within a level.
-- No frame allocation wrapper (the level manages its own frame).
--
-- Parameters:
--   bindings: list of {name=, sg_type=} from level context
--   expr_str: single expression string
--
-- Returns:
--   closure that emits quad ops (no frame wrapper)
-- ============================================================================

function sg_expr.compile_level_operator(bindings, expr_str)
    local local_decls = {}
    for _, b in ipairs(bindings) do
        local expr_type = sg_type_map[b.sg_type] or "int"
        table.insert(local_decls, b.name .. ":" .. expr_type)
    end

    local scratch_count = estimate_scratch({expr_str})
    local scratch_decls = {}
    local scratch_names = {}
    for i = 1, scratch_count do
        local name = "_sg_t" .. (i - 1)
        table.insert(scratch_decls, name .. ":float")
        table.insert(scratch_names, name)
    end

    local vars = frame_vars(local_decls, scratch_decls)
    return quad_expr(expr_str, vars, scratch_names)
end

-- ============================================================================
-- sg_expr.expr / sg_expr.pred / sg_expr.multi
--
-- Convenience wrappers for use when bindings are provided as a flat list
-- rather than a full template definition.
--
-- Parameters:
--   expr_str: expression string
--   bindings: list of {name=, sg_type=}
--
-- Returns:
--   closure that emits quad/p_quad ops
-- ============================================================================

function sg_expr.expr(expr_str, bindings)
    local local_decls = {}
    for _, b in ipairs(bindings) do
        local expr_type = sg_type_map[b.sg_type] or "int"
        table.insert(local_decls, b.name .. ":" .. expr_type)
    end

    local scratch_count = estimate_scratch({expr_str})
    local scratch_decls, scratch_names = {}, {}
    for i = 1, scratch_count do
        local name = "_sg_t" .. (i - 1)
        table.insert(scratch_decls, name .. ":float")
        table.insert(scratch_names, name)
    end

    local vars = frame_vars(local_decls, scratch_decls)
    return quad_expr(expr_str, vars, scratch_names)
end

function sg_expr.pred(expr_str, bindings)
    local local_decls = {}
    for _, b in ipairs(bindings) do
        local expr_type = sg_type_map[b.sg_type] or "int"
        table.insert(local_decls, b.name .. ":" .. expr_type)
    end

    local scratch_count = estimate_scratch({expr_str})
    local scratch_decls, scratch_names = {}, {}
    for i = 1, scratch_count do
        local name = "_sg_t" .. (i - 1)
        table.insert(scratch_decls, name .. ":float")
        table.insert(scratch_names, name)
    end

    local vars = frame_vars(local_decls, scratch_decls)
    return quad_pred(expr_str, vars, scratch_names)
end

function sg_expr.multi(expr_str, bindings)
    local local_decls = {}
    for _, b in ipairs(bindings) do
        local expr_type = sg_type_map[b.sg_type] or "int"
        table.insert(local_decls, b.name .. ":" .. expr_type)
    end

    local scratch_count = estimate_scratch({expr_str})
    local scratch_decls, scratch_names = {}, {}
    for i = 1, scratch_count do
        local name = "_sg_t" .. (i - 1)
        table.insert(scratch_decls, name .. ":float")
        table.insert(scratch_names, name)
    end

    local vars = frame_vars(local_decls, scratch_decls)
    return quad_multi(expr_str, vars, scratch_names)
end

-- ============================================================================
-- sg_expr.debug_template
--
-- Same as compile_template but prints the stack layout and compiled
-- operations for each expression.
-- ============================================================================

function sg_expr.debug_template(template_def, expressions)
    local ctx = sg_expr.make_template_context(template_def)

    -- First pass: intermediates
    for _, expr_str in ipairs(expressions) do
        local dest = expr_str:match("^%s*([%w_]+)%s*=")
            or expr_str:match("^%s*([%w_]+)%s*[+%-*/%%&|^]=")
        if dest and not ctx.name_to_slot[dest] then
            local sg_type = infer_intermediate_type(expr_str, ctx)
            sg_expr.add_intermediate(ctx, dest, sg_type)
        end
    end

    local scratch_count = estimate_scratch(expressions)
    local vars, scratch_names = sg_expr.build_frame_vars(ctx, scratch_count)
    local return_vars = sg_expr.get_return_vars(ctx)

    print("=== sg_expr.debug_template ===")
    print("Stack frame layout:")
    print(string.format("  num_params=%d  num_locals=%d  scratch=%d",
        ctx.stack_num_params, ctx.stack_num_locals, scratch_count))
    for i, slot in ipairs(ctx.slots) do
        local expr_type = sg_type_map[slot.sg_type] or "int"
        print(string.format("  slot %2d: %-20s %-12s :%s",
            i - 1, slot.name, slot.role, expr_type))
    end
    for i = 1, scratch_count do
        print(string.format("  tos  %2d: _sg_t%-15d scratch     :float", i - 1, i - 1))
    end
    print("  return_vars = {" .. table.concat(return_vars, ", ") .. "}")
    print("")

    -- Compile each expression with debug output
    for _, expr_str in ipairs(expressions) do
        quad_expr_debug(expr_str, vars, scratch_names)
    end
    print("=== end debug ===")

    -- Return the real compiled version
    return sg_expr.compile_template(template_def, expressions)
end

print("sg_expr module loaded (v0.2)")

return sg_expr


