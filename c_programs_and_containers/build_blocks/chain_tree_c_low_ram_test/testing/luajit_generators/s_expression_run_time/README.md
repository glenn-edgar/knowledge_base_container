# ChainTree S-Expression DSL

**Version 2.0** — Flat `node_t` Array Output

A Lua-based domain-specific language for defining ChainTree control structures that compile to compact C headers or binary blobs for embedded systems.

## Index

| Section | Description |
|---------|-------------|
| [Overview](#overview) | What the DSL does and target platforms |
| [Requirements](#requirements) | LuaJIT installation |
| [Quick Start](#quick-start) | Create, compile, and run your first module |
| [Compiler Usage](#compiler-usage) | Command-line options and examples |
| [DSL Reference](#dsl-reference) | Complete language reference |
| ↳ [Module Structure](#module-structure) | `start_module`, `start_tree`, etc. |
| ↳ [Function Types](#function-types) | Oneshot, Boolean, Main |
| ↳ [Leaf Nodes](#leaf-nodes) | `oneshot`, `main`, `bool_fn`, `quote` |
| ↳ [Parameter Types](#parameter-types) | int32, uint32, float32, string |
| ↳ [Control Structures](#control-structures) | `pipeline`, `if_then`, `if_then_else`, `cond`, `dispatch` |
| ↳ [Boolean Combinators](#boolean-combinators) | `bool_and`, `bool_or`, `bool_not` |
| ↳ [Debug Wrapper](#debug-wrapper) | `dbg` for tracing |
| [Structural Wrappers](#structural-wrappers-condition-action-clause-case) | `condition`, `action`, `clause`, `case` |
| [DSL Limitations](#dsl-limitations) | What the DSL cannot do |
| [Lua Macros](#lua-macros) | Metaprogramming and code generation |
| [Adding Custom Combinators](#adding-custom-combinators) | Extending the DSL with new control structures |
| [Design Patterns](#design-patterns) | Dataflow vs control flow, state machines, guards, column control, behavior trees |
| [Dynamic Plans (TBD)](#dynamic-plans-via-s-expression-node-control-tbd) | Runtime-generated control structures |
| [Output Formats](#output-formats) | C header and binary blob formats |
| [Node Type Encoding](#node-type-encoding) | Type field bit layout |
| [Control Flow Codes](#control-flow-codes) | CFL_CONTINUE, CFL_HALT, etc. |
| [Memory Layout](#memory-layout) | `node_t` and `param_t` structures |
| [Complete Example](#complete-example) | Full irrigation controller |
| [Integration with C Runtime](#integration-with-c-runtime) | Using generated code |

## Overview

The ChainTree S-Expression DSL provides a declarative way to define behavior trees, state machines, and sequential control flows. The DSL compiles to flat, position-independent node arrays suitable for:

- **ARM Cortex-M microcontrollers** (32KB+ RAM)
- **Deterministic real-time systems**
- **Cross-compilation environments**
- **Server-class deployments** (with dynamic scripting)

## Requirements

- **LuaJIT** (required for FFI and bit operations)
- POSIX-compatible shell (for running compiler)

```bash
# Ubuntu/Debian
sudo apt-get install luajit

# macOS
brew install luajit
```

## Quick Start

### 1. Create a module definition (`motor.lua`)

```lua
dofile("s_expr_dsl.lua")

start_module("motor_control")

start_tree("init")
    pipeline("setup")
        oneshot("init_gpio", 0x40020000)
        oneshot("init_pwm", 1000)
        oneshot("set_mode", str("idle"))
    end_pipeline("setup")
end_tree("init")

start_tree("run")
    if_then("check_fault")
        condition("fault_cond")
            bool_fn("is_fault_active")
        end_condition("fault_cond")
        action("handle_fault")
            oneshot("emergency_stop")
        end_action("handle_fault")
    end_if_then("check_fault")
end_tree("run")

end_module("motor_control")

return gen
```

### 2. Compile to C header

```bash
luajit compile.lua motor.lua --header=motor_module.h
```

### 3. Compile to binary blob

```bash
luajit compile.lua motor.lua --bin=motor.bin
```

## Compiler Usage

```
luajit compile.lua <input.lua> [options]

Options:
  --bin=<file>      Generate binary file (.bin)
  --header=<file>   Generate C header file (.h)
  --name=<name>     Base name for generated symbols (default: from input)
  --dump            Show tree structure
  --nodes           Show flat node array
  --stats           Show module statistics
  --all             Enable --dump, --nodes, --stats
  --help            Show this help
```

### Examples

```bash
# Generate both outputs
luajit compile.lua motor.lua --bin=motor.bin --header=motor.h

# Debug: show structure and stats
luajit compile.lua motor.lua --all

# Print header to stdout
luajit compile.lua motor.lua
```

## DSL Reference

### Module Structure

```lua
start_module("module_name")
    start_tree("tree_name")
        -- tree content
    end_tree("tree_name")
end_module("module_name")

return gen  -- Return the generator
```

### Function Types

| Type | Prefix | Description | Return |
|------|--------|-------------|--------|
| **Oneshot** | `@` | Execute once per tick | Control flow code |
| **Boolean** | `?` | Condition check | `true` / `false` |
| **Main** | `!` | Persistent execution | Control flow code |

### Leaf Nodes

```lua
-- Oneshot function (executes once)
oneshot("function_name", arg1, arg2, ...)

-- Main function (persistent state)
main("function_name", arg1, arg2, ...)

-- Boolean function (condition)
bool_fn("function_name", arg1, arg2, ...)

-- Control flow literal
quote("CFL_CONTINUE")   -- Continue execution
quote("CFL_HALT")       -- Halt this tick
quote("CFL_TERMINATE")  -- Terminate tree
quote("CFL_RESET")      -- Reset tree
quote("CFL_DISABLE")    -- Disable tree
```

### Parameter Types

```lua
oneshot("fn", 42)              -- Auto-detected as int32/uint32
oneshot("fn", 3.14)            -- Auto-detected as float32
oneshot("fn", "text")          -- Auto-detected as string

-- Explicit types
oneshot("fn", int32(-100))     -- Signed 32-bit
oneshot("fn", uint32(0xFF))    -- Unsigned 32-bit
oneshot("fn", float32(1.5))    -- 32-bit float
oneshot("fn", str("label"))    -- String reference
```

### Control Structures

#### Pipeline (Sequential Execution)

```lua
pipeline("name")
    oneshot("step1")
    oneshot("step2")
    oneshot("step3")
end_pipeline("name")
```

#### If-Then

```lua
if_then("name")
    condition("cond")
        bool_fn("check_something")
    end_condition("cond")
    action("act")
        oneshot("do_something")
    end_action("act")
end_if_then("name")
```

#### If-Then-Else

```lua
if_then_else("name")
    condition("cond")
        bool_fn("is_ready")
    end_condition("cond")
    action("then_act")
        oneshot("proceed")
    end_action("then_act")
    action("else_act")
        oneshot("wait")
    end_action("else_act")
end_if_then_else("name")
```

#### Cond (Multi-way Branch)

```lua
cond("state_machine")
    clause("check_idle")
        condition("c1")
            bool_fn("is_idle")
        end_condition("c1")
        action("a1")
            oneshot("start_work")
        end_action("a1")
    end_clause("check_idle")
    
    clause("check_running")
        condition("c2")
            bool_fn("is_running")
        end_condition("c2")
        action("a2")
            main("continue_work")
        end_action("a2")
    end_clause("check_running")
    
    default_clause("fallback")
        action("default_act")
            oneshot("handle_unknown")
        end_action("default_act")
    end_default_clause("fallback")
end_cond("state_machine")
```

#### Dispatch (Pattern Matching)

```lua
dispatch("command_handler", "cmd_type")
    case("start_case", "START")
        action("start_act")
            oneshot("handle_start")
        end_action("start_act")
    end_case("start_case")
    
    case("stop_case", "STOP")
        action("stop_act")
            oneshot("handle_stop")
        end_action("stop_act")
    end_case("stop_case")
    
    -- Multiple patterns
    case("pause_case", {"PAUSE", "SUSPEND"})
        action("pause_act")
            oneshot("handle_pause")
        end_action("pause_act")
    end_case("pause_case")
    
    default_case("unknown")
        action("unknown_act")
            quote("CFL_CONTINUE")
        end_action("unknown_act")
    end_default_case("unknown")
end_dispatch("command_handler")
```

### Boolean Combinators

```lua
-- AND: all must be true
bool_and("all_ready")
    bool_fn("sensor_ok")
    bool_fn("motor_ok")
    bool_fn("power_ok")
end_bool_and("all_ready")

-- OR: any must be true
bool_or("any_fault")
    bool_fn("over_temp")
    bool_fn("over_current")
    bool_fn("stall_detected")
end_bool_or("any_fault")

-- NOT: invert result
bool_not("not_busy")
    bool_fn("is_busy")
end_bool_not("not_busy")

-- Nested example
bool_and("complex_check")
    bool_fn("enabled")
    bool_or("trigger")
        bool_fn("manual_trigger")
        bool_fn("auto_trigger")
    end_bool_or("trigger")
    bool_not("inhibit")
        bool_fn("is_inhibited")
    end_bool_not("inhibit")
end_bool_and("complex_check")
```

### Debug Wrapper

```lua
dbg("trace_point", "Entering main loop")
    pipeline("main_work")
        oneshot("step1")
        oneshot("step2")
    end_pipeline("main_work")
end_dbg("trace_point")
```

## Structural Wrappers: condition, action, clause, case

These are **structural wrappers** that enforce the "test then do" pattern. They don't generate opcodes themselves — they organize children within control structures and switch parsing contexts.

### Why Wrappers Exist

```lua
-- Without wrappers (ambiguous - which is condition, which is action?)
if_then("check")
    bool_fn("is_ready")      -- condition? action?
    oneshot("do_work")       -- condition? action?
end_if_then("check")

-- With wrappers (explicit structure)
if_then("check")
    condition("c")           -- THIS is the test
        bool_fn("is_ready")
    end_condition("c")
    action("a")              -- THIS is what to do
        oneshot("do_work")
    end_action("a")
end_if_then("check")
```

### Where Wrappers Are Used

| Construct | Structure |
|-----------|-----------|
| `if_then` | `condition` → `action` |
| `if_then_else` | `condition` → `action` (then) → `action` (else) |
| `cond` | multiple `clause`, each with `condition` → `action` |
| `dispatch` | multiple `case`, each with `action` only (pattern is on `case` itself) |

### Context Switching

Wrappers switch the **parsing context**, controlling what's allowed inside:

```lua
if_then("example")
    -- Context: CONDITION (only boolean expressions allowed)
    condition("c")
        bool_fn("test")        -- ✅ allowed
        bool_and("combo")      -- ✅ allowed
            bool_fn("a")
            bool_fn("b")
        end_bool_and("combo")
        oneshot("work")        -- ❌ ERROR: wrong context
    end_condition("c")
    
    -- Context: ACTION (only control flow allowed)
    action("a")
        oneshot("work")        -- ✅ allowed
        pipeline("seq")        -- ✅ allowed
            oneshot("step1")
            oneshot("step2")
        end_pipeline("seq")
        bool_fn("test")        -- ❌ ERROR: wrong context
    end_action("a")
end_if_then("example")
```

### Clause vs Case

```lua
-- CLAUSE: has both condition and action (used in cond)
cond("state_machine")
    clause("check_idle")
        condition("c")              -- test expression
            bool_fn("is_idle")
        end_condition("c")
        action("a")                 -- what to do if true
            oneshot("start")
        end_action("a")
    end_clause("check_idle")
    
    default_clause("fallback")      -- no condition needed
        action("a")
            quote("CFL_CONTINUE")
        end_action("a")
    end_default_clause("fallback")
end_cond("state_machine")

-- CASE: pattern is on the case itself, only action inside (used in dispatch)
dispatch("handler", "cmd")
    case("start_cmd", "START")      -- pattern here
        action("a")                 -- just action, no condition
            oneshot("handle_start")
        end_action("a")
    end_case("start_cmd")
    
    default_case("unknown")
        action("a")
            quote("CFL_CONTINUE")
        end_action("a")
    end_default_case("unknown")
end_dispatch("handler")
```

### Generated Output

Wrappers don't create separate nodes — they assign children to the correct slots in the parent node:

```lua
-- This structure...
if_then_else("x")
    condition("c")
        bool_fn("test")
    end_condition("c")
    action("then")
        oneshot("yes")
    end_action("then")
    action("else")
        oneshot("no")
    end_action("else")
end_if_then_else("x")

-- ...produces a single if_else node with:
--   node.condition   = bool_fn("test") node
--   node.then_action = oneshot("yes") node  
--   node.else_action = oneshot("no") node
```

## DSL Limitations

### Parameters Are Data Only

Functions accept only **data parameters**, not other functions:

| Supported | Not Supported |
|-----------|---------------|
| `int32`, `uint32` | Function references |
| `float32` | Lambdas/closures |
| `string` (data) | Tree fragments |
| | Callbacks |

```lua
-- NOT supported - function as parameter
oneshot("map", bool_fn("predicate"))     -- ❌
oneshot("retry", oneshot("do_work"))     -- ❌
main("with_timeout", main("task"), 5000) -- ❌

-- Composition is structural, not parametric
pipeline("retry_pattern")                -- ✅
    oneshot("attempt_work")
    if_then("check")
        condition("c")
            bool_fn("succeeded")
        end_condition("c")
        action("a")
            quote("CFL_CONTINUE")
        end_action("a")
    end_if_then("check")
end_pipeline("retry_pattern")
```

### No User-Defined Combinators

The control structures (`pipeline`, `if`, `cond`, `dispatch`) and boolean combinators (`and`, `or`, `not`) are built-in with fixed opcodes. You cannot define custom combinators.

User-defined functions (`oneshot`, `main`, `bool_fn`) are the extension point — they're looked up by name in function tables at runtime.

### No 64-bit or Complex Types

- No `int64`, `uint64`, `double`
- No arrays as parameters
- No nested structures or binary blobs
- For booleans, use `uint32(0)` / `uint32(1)`

## Lua Macros

Since the DSL is embedded in Lua, you can use Lua functions as macros to generate repetitive patterns:

### Basic Macro

```lua
-- Define a macro
local function guarded(name, condition_fn, action_fn)
    if_then(name)
        condition(name .. "_cond")
            bool_fn(condition_fn)
        end_condition(name .. "_cond")
        action(name .. "_act")
            oneshot(action_fn)
        end_action(name .. "_act")
    end_if_then(name)
end

-- Use the macro
start_tree("main")
    pipeline("checks")
        guarded("temp_check", "is_over_temp", "shutdown_motor")
        guarded("pressure_check", "is_over_pressure", "open_relief")
    end_pipeline("checks")
end_tree("main")
```

### Table-Driven Macro

```lua
local function state_machine(name, states)
    cond(name)
        for _, s in ipairs(states) do
            clause(s.name)
                condition("c")
                    bool_fn(s.check)
                end_condition("c")
                action("a")
                    oneshot(s.action)
                end_action("a")
            end_clause(s.name)
        end
        default_clause("default")
            action("a")
                quote("CFL_CONTINUE")
            end_action("a")
        end_default_clause("default")
    end_cond(name)
end

-- Usage
start_tree("controller")
    state_machine("mode_select", {
        { name = "idle",    check = "is_idle",    action = "enter_idle" },
        { name = "running", check = "is_running", action = "enter_run" },
        { name = "fault",   check = "is_fault",   action = "enter_fault" },
    })
end_tree("controller")
```

### Parameterized Generation

```lua
-- Generate N identical zones
local function irrigation_zones(count)
    for i = 1, count do
        local z = "zone_" .. i
        if_then(z)
            condition("c")
                bool_fn("zone_scheduled", uint32(i))
            end_condition("c")
            action("a")
                pipeline(z .. "_run")
                    oneshot("open_valve", uint32(i))
                    main("run_timer", uint32(i))
                    oneshot("close_valve", uint32(i))
                end_pipeline(z .. "_run")
            end_action("a")
        end_if_then(z)
    end
end

start_tree("irrigation")
    pipeline("all_zones")
        irrigation_zones(8)  -- Generate 8 zones
    end_pipeline("all_zones")
end_tree("irrigation")
```

This is a key advantage of embedding the DSL in Lua — you get full metaprogramming capabilities for free.

## Adding Custom Combinators

The DSL is designed to be extensible. Adding a new combinator requires changes in two places: the Lua DSL and the C runtime.

### Effort Estimate

| Task | Difficulty | Lines |
|------|------------|-------|
| Add opcode + node type | Trivial | 2 |
| DSL functions | Easy | ~30 |
| get_node_children | Trivial | 3 |
| emit_nodes | Easy | 5-10 |
| C runtime handler | Varies | 10-50 |

**Total: ~50-100 lines** depending on combinator complexity.

### Lua DSL Side (s_expr_dsl.lua)

```lua
-- 1. Add opcode (pick next available number)
local OPCODES = {
    ...
    my_combinator = 0x0D,  -- NEW
}

-- 2. Add node type constant
local NODE_TYPES = {
    ...
    MY_COMBINATOR = "my_combinator",  -- NEW
}

-- 3. Add DSL functions (follow existing pattern)
function my_combinator(name, ...)
    if type(name) ~= "string" then
        dsl_error("my_combinator() requires name")
    end
    check_context({CONTEXTS.CONTROL_FLOW, CONTEXTS.ACTION}, "my_combinator")
    start_composite(NODE_TYPES.MY_COMBINATOR, name, CONTEXTS.CONTROL_FLOW, function(n)
        -- Initialize any extra fields
        n.custom_field = ...
    end)
end

function end_my_combinator(name)
    if type(name) ~= "string" then
        dsl_error("end_my_combinator() requires name")
    end
    local node = stack_pop(NODE_TYPES.MY_COMBINATOR, name)
    -- Validate children count, structure, etc.
    if #node.children < 1 then
        dsl_error(string.format("my_combinator('%s') requires at least 1 child", name))
    end
end

-- 4. Handle in get_node_children()
function TreeGenerator:get_node_children(node)
    ...
    elseif t == NODE_TYPES.MY_COMBINATOR then
        children = node.children  -- or custom logic
    end
    ...
end

-- 5. Handle in emit_nodes()
function TreeGenerator:emit_nodes(node, next_sibling_index)
    ...
    elseif t == NODE_TYPES.MY_COMBINATOR then
        n.type = TABLE_OPCODE + OPCODES.my_combinator
        -- Emit any custom params
    end
    ...
end
```

### C Runtime Side

```c
// In your interpreter switch statement
case OP_MY_COMBINATOR: {
    // Implement the combinator's semantics
    // Walk children using first_child/next_sibling
    // Return appropriate control flow code
    break;
}
```

### Complexity Examples

| Combinator | Complexity | Why |
|------------|------------|-----|
| `repeat_n` | Simple | Just loop N times over children |
| `timeout` | Medium | Needs timer state, child execution |
| `parallel` | Complex | Multiple simultaneous children, merge results |
| `priority` | Medium | Ordered fallback through children |

The hard part isn't adding it to the DSL — it's **designing the semantics** and **implementing them correctly in C** with proper state management.

## Design Patterns

### Dataflow vs Control Flow

The DSL enforces a clean separation between two flow patterns:

| Aspect | Condition Trees | Action Trees |
|--------|-----------------|--------------|
| **Flow direction** | Data flows UP | Control flows DOWN |
| **Purpose** | Compute boolean result | Execute effects |
| **Side effects** | None (pure) | Yes |
| **State** | Stateless | May have state |
| **Analogy** | Combinational logic | Sequential logic |

```
CONDITION (data flows UP)          ACTION (control flows DOWN)
                                   
        ┌─────────┐                      ┌──────────┐
        │   AND   │ ← result             │ pipeline │
        └────┬────┘                      └────┬─────┘
    ┌────────┼────────┐                       │
    ▼        ▼        ▼                       ▼
┌──────┐ ┌──────┐ ┌──────┐              ┌──────────┐
│bool_fn│ │  OR  │ │ NOT  │              │ oneshot  │
│ "a"  │ └───┬──┘ └───┬──┘              │ "step1"  │
└──────┘     │        │                 └────┬─────┘
        ┌────┴───┐    ▼                      │
        ▼        ▼ ┌──────┐                  ▼
    ┌──────┐ ┌──────┐│bool_fn│           ┌──────────┐
    │bool_fn│ │bool_fn││ "d"  │           │ oneshot  │
    │ "b"  │ │ "c"  │└──────┘            │ "step2"  │
    └──────┘ └──────┘                    └────┬─────┘
                                              │
    Leaves evaluate first,                    ▼
    results bubble up                   ┌──────────┐
                                        │ oneshot  │
                                        │ "step3"  │
                                        └──────────┘
                                        
                                        Executes top-down
```

### The "Test Then Do" Pattern

Every conditional structure follows the same pattern:

```lua
if_then("name")
    condition("c")      -- PURE: compute boolean
        -- dataflow tree
    end_condition("c")
    action("a")         -- EFFECTS: execute if true
        -- control flow tree
    end_action("a")
end_if_then("name")
```

This mirrors hardware design:
- **Condition network** = combinational logic (gates, no clock)
- **Action network** = sequential logic (registers, clocked)

### Boolean Tree Properties

| Property | Benefit |
|----------|---------|
| **Pure dataflow** | No side effects during evaluation |
| **Deterministic** | Same inputs → same output |
| **Short-circuit capable** | Runtime can skip branches early |
| **Stateless** | No hidden state between ticks |
| **Composable** | Subtrees reusable via Lua macros |

### State Machine Pattern

Use `cond` for explicit state machines:

```lua
cond("state_machine")
    clause("idle_to_running")
        condition("c")
            bool_and("start_conditions")
                bool_fn("is_idle")
                bool_fn("start_requested")
                bool_not("fault")
                    bool_fn("has_fault")
                end_bool_not("fault")
            end_bool_and("start_conditions")
        end_condition("c")
        action("a")
            oneshot("enter_running")
        end_action("a")
    end_clause("idle_to_running")
    
    clause("running_to_idle")
        condition("c")
            bool_and("stop_conditions")
                bool_fn("is_running")
                bool_fn("stop_requested")
            end_bool_and("stop_conditions")
        end_condition("c")
        action("a")
            oneshot("enter_idle")
        end_action("a")
    end_clause("running_to_idle")
    
    default_clause("no_transition")
        action("a")
            quote("CFL_CONTINUE")
        end_action("a")
    end_default_clause("no_transition")
end_cond("state_machine")
```

### Guard Pattern

Protect actions with complex preconditions:

```lua
-- Macro for reuse
local function guarded_action(name, guards, action_fn)
    if_then(name)
        condition("c")
            bool_and(name .. "_guards")
                for _, g in ipairs(guards) do
                    bool_fn(g)
                end
            end_bool_and(name .. "_guards")
        end_condition("c")
        action("a")
            oneshot(action_fn)
        end_action("a")
    end_if_then(name)
end

-- Usage
guarded_action("safe_start", {
    "motor_ready",
    "no_faults", 
    "safety_interlock_closed",
    "operator_present"
}, "start_motor")
```

### Reactive Sensor Pattern

Combine multiple sensor inputs into decisions:

```lua
condition("environment_safe")
    bool_and("all_sensors")
        bool_fn("temp_in_range", int32(0), int32(50))
        bool_fn("pressure_in_range", int32(90), int32(110))
        bool_not("vibration")
            bool_fn("excessive_vibration")
        end_bool_not("vibration")
        bool_or("power_ok")
            bool_fn("main_power_ok")
            bool_fn("backup_power_ok")
        end_bool_or("power_ok")
    end_bool_and("all_sensors")
end_condition("environment_safe")
```

This produces: `temp_ok AND pressure_ok AND (NOT vibration) AND (main_power OR backup_power)`

### Conditional Column Control Pattern

For concurrent/parallel control flow, use column primitives:

| Primitive | Parameters | Description |
|-----------|------------|-------------|
| `lc` | child index | **Launch column** — start child executing concurrently |
| `jc` | child index | **Join column** — wait for child to complete |
| `tc` | child index | **Terminate column** — abort/cancel child |

```
        ┌─────────────────────────────────────┐
        │            pipeline                 │
        └─────────────────────────────────────┘
                         │
         ┌───────────────┼───────────────┐
         ▼               ▼               ▼
    ┌─────────┐     ┌─────────┐     ┌─────────┐
    │ child 0 │     │ child 1 │     │ child 2 │
    │ (col A) │     │ (col B) │     │ (col C) │
    └─────────┘     └─────────┘     └─────────┘
         │               │               │
    lc(0) starts    lc(1) starts    lc(2) starts
         │               │               │
         └───────────────┴───────────────┘
                         │
                    jc(0), jc(1), jc(2)
                    wait for all
```

#### Fork-Join Parallelism

```lua
pipeline("parallel_work")
    -- Define the columns as children
    pipeline("column_a")
        oneshot("init_sensor_a")
        main("read_sensor_a")
    end_pipeline("column_a")
    
    pipeline("column_b")
        oneshot("init_sensor_b")
        main("read_sensor_b")
    end_pipeline("column_b")
    
    pipeline("column_c")
        oneshot("init_motor")
        main("run_motor")
    end_pipeline("column_c")
    
    -- Control sequence
    pipeline("orchestrator")
        lc(0)           -- launch column_a
        lc(1)           -- launch column_b
        lc(2)           -- launch column_c
        -- all three running concurrently
        jc(0)           -- wait for column_a
        jc(1)           -- wait for column_b
        jc(2)           -- wait for column_c
        oneshot("all_complete")
    end_pipeline("orchestrator")
end_pipeline("parallel_work")
```

#### Conditional Launch

```lua
pipeline("adaptive_control")
    -- Always run primary
    lc(0)
    
    -- Conditionally launch backup
    if_then("need_backup")
        condition("c")
            bool_fn("primary_degraded")
        end_condition("c")
        action("a")
            lc(1)       -- launch backup column
        end_action("a")
    end_if_then("need_backup")
    
    -- Wait for primary
    jc(0)
end_pipeline("adaptive_control")
```

#### Timeout with Termination

```lua
pipeline("timeout_pattern")
    lc(0)               -- launch worker column
    
    -- Monitor loop
    cond("monitor")
        clause("completed")
            condition("c")
                bool_fn("column_done", uint32(0))
            end_condition("c")
            action("a")
                jc(0)   -- clean join
            end_action("a")
        end_clause("completed")
        
        clause("timeout")
            condition("c")
                bool_fn("timeout_expired")
            end_condition("c")
            action("a")
                tc(0)   -- kill the slow column
                oneshot("log_timeout")
            end_action("a")
        end_clause("timeout")
        
        default_clause("wait")
            action("a")
                quote("CFL_HALT")  -- yield, check next tick
            end_action("a")
        end_default_clause("wait")
    end_cond("monitor")
end_pipeline("timeout_pattern")
```

#### Multi-Zone Irrigation Example

```lua
pipeline("irrigation_controller")
    -- Zone columns (children 0-7)
    irrigation_zones(8)  -- macro generates 8 zone pipelines
    
    -- Scheduler column
    pipeline("scheduler")
        cond("zone_dispatch")
            -- Launch zones based on schedule
            clause("zone1_time")
                condition("c")
                    bool_fn("zone_scheduled", uint32(1))
                end_condition("c")
                action("a")
                    lc(0)   -- launch zone 1 column
                end_action("a")
            end_clause("zone1_time")
            
            clause("zone2_time")
                condition("c")
                    bool_fn("zone_scheduled", uint32(2))
                end_condition("c")
                action("a")
                    lc(1)   -- launch zone 2 column
                end_action("a")
            end_clause("zone2_time")
            
            -- ... more zones
            
            default_clause("idle")
                action("a")
                    quote("CFL_HALT")
                end_action("a")
            end_default_clause("idle")
        end_cond("zone_dispatch")
    end_pipeline("scheduler")
    
    -- Emergency stop terminates all
    if_then("emergency")
        condition("c")
            bool_fn("emergency_stop_pressed")
        end_condition("c")
        action("a")
            pipeline("kill_all")
                tc(0) tc(1) tc(2) tc(3)
                tc(4) tc(5) tc(6) tc(7)
                oneshot("close_master_valve")
            end_pipeline("kill_all")
        end_action("a")
    end_if_then("emergency")
end_pipeline("irrigation_controller")
```

#### Column State

The runtime must track per-column state:

| State | Description |
|-------|-------------|
| `IDLE` | Not launched |
| `RUNNING` | Executing |
| `HALTED` | Yielded, resume next tick |
| `DONE` | Completed normally |
| `TERMINATED` | Killed by `tc` |

This enables Erlang-style supervisor patterns where parent columns monitor and restart child columns on failure.

### Behavior Tree Patterns with Boolean Combinators

The boolean combinators map directly to classic behavior tree nodes with short-circuit evaluation:

| Boolean Combinator | Behavior Tree | Semantics |
|--------------------|---------------|-----------|
| `bool_or` | **Selector** (try-pass) | Succeeds on first success, skips rest |
| `bool_and` | **Sequence** (try-fail) | Fails on first failure, skips rest |
| `bool_not` | **Inverter** | Flips child result |

#### Try-Pass (Selector / Fallback)

`bool_or` implements "try until one succeeds":

```lua
condition("find_target")
    bool_or("selector")
        bool_fn("target_in_front")      -- try first
        bool_fn("target_on_left")       -- try if first fails
        bool_fn("target_on_right")      -- try if both fail
        bool_fn("target_behind")        -- last resort
    end_bool_or("selector")
end_condition("find_target")
```

```
Evaluation order (short-circuit):

target_in_front? ──YES──► return TRUE (done)
        │
        NO
        ▼
target_on_left? ──YES──► return TRUE (done)
        │
        NO
        ▼
target_on_right? ──YES──► return TRUE (done)
        │
        NO
        ▼
target_behind? ──YES──► return TRUE (done)
        │
        NO
        ▼
    return FALSE (all failed)
```

#### Try-Fail (Sequence)

`bool_and` implements "all must succeed":

```lua
condition("can_fire")
    bool_and("sequence")
        bool_fn("weapon_loaded")        -- check first
        bool_fn("target_acquired")      -- check if first passes
        bool_fn("safety_off")           -- check if both pass
        bool_fn("in_range")             -- check if all pass
    end_bool_and("sequence")
end_condition("can_fire")
```

```
Evaluation order (short-circuit):

weapon_loaded? ──NO──► return FALSE (done)
        │
       YES
        ▼
target_acquired? ──NO──► return FALSE (done)
        │
       YES
        ▼
safety_off? ──NO──► return FALSE (done)
        │
       YES
        ▼
in_range? ──NO──► return FALSE (done)
        │
       YES
        ▼
    return TRUE (all passed)
```

#### Nested Behavior Trees

Combine for complex decision trees:

```lua
condition("ai_decision")
    bool_or("root_selector")
        -- Priority 1: Emergency behavior
        bool_and("flee_sequence")
            bool_fn("health_critical")
            bool_fn("escape_route_exists")
        end_bool_and("flee_sequence")
        
        -- Priority 2: Attack behavior
        bool_and("attack_sequence")
            bool_fn("has_weapon")
            bool_fn("enemy_visible")
            bool_fn("ammo_available")
        end_bool_and("attack_sequence")
        
        -- Priority 3: Patrol behavior
        bool_and("patrol_sequence")
            bool_fn("patrol_point_set")
            bool_not("at_destination")
                bool_fn("at_patrol_point")
            end_bool_not("at_destination")
        end_bool_and("patrol_sequence")
        
        -- Priority 4: Idle (always succeeds)
        bool_fn("true")
    end_bool_or("root_selector")
end_condition("ai_decision")
```

This produces classic behavior tree logic:

```
                    ┌─────────────┐
                    │  SELECTOR   │ (try-pass)
                    └──────┬──────┘
        ┌──────────────────┼──────────────────┬─────────────┐
        ▼                  ▼                  ▼             ▼
   ┌─────────┐        ┌─────────┐        ┌─────────┐   ┌─────────┐
   │SEQUENCE │        │SEQUENCE │        │SEQUENCE │   │  IDLE   │
   │ (flee)  │        │(attack) │        │(patrol) │   │ (true)  │
   └────┬────┘        └────┬────┘        └────┬────┘   └─────────┘
        │                  │                  │
   ┌────┴────┐        ┌────┼────┐        ┌────┴────┐
   ▼         ▼        ▼    ▼    ▼        ▼         ▼
health?  escape?   wpn? vis? ammo?    point?   NOT(at?)
```

#### Action Selection Pattern

Use the boolean result to select actions:

```lua
if_then_else("behavior")
    condition("try_attack")
        bool_and("attack_preconditions")
            bool_fn("enemy_in_range")
            bool_fn("weapon_ready")
        end_bool_and("attack_preconditions")
    end_condition("try_attack")
    action("do_attack")
        oneshot("fire_weapon")
    end_action("do_attack")
    action("do_fallback")
        oneshot("find_cover")
    end_action("do_fallback")
end_if_then_else("behavior")
```

#### Benefits Over Traditional Behavior Trees

| Aspect | ChainTree Approach | Traditional BT |
|--------|-------------------|----------------|
| **Separation** | Pure conditions vs effectful actions | Mixed in nodes |
| **Composition** | Lua macros for reuse | Usually hardcoded |
| **Memory** | Flat arrays, no pointers | Pointer-heavy trees |
| **Debugging** | Clear condition/action split | Interleaved logic |

The key insight: **boolean combinators ARE behavior tree selectors/sequences** when short-circuit evaluated. The DSL makes this explicit by separating the decision logic (boolean tree) from the execution logic (action tree).

### Dynamic Plans via S-Expression Node Control (TBD)

> **Status: TBD** — This section describes a planned extension for runtime-generated control structures.

For systems requiring runtime plan generation (planners, goal-oriented action planning, dynamic task allocation), a dynamic S-expression interface allows constructing and modifying trees at runtime:

```lua
-- Conceptual API (TBD)

-- Runtime node creation
local plan = s_expr.new_pipeline("dynamic_plan")

-- Add nodes dynamically
s_expr.append(plan, s_expr.oneshot("acquire_resource", resource_id))
s_expr.append(plan, s_expr.oneshot("move_to", target_x, target_y))

-- Conditional insertion
if needs_tool then
    s_expr.append(plan, s_expr.oneshot("equip_tool", tool_id))
end

-- Nested structures
local check = s_expr.new_if_then("validate")
s_expr.set_condition(check, s_expr.bool_fn("resource_available", resource_id))
s_expr.set_action(check, s_expr.oneshot("claim_resource", resource_id))
s_expr.append(plan, check)

-- Compile to executable form
local executable = s_expr.compile(plan)

-- Hot-swap into running tree
s_expr.replace_subtree(main_tree, "plan_slot", executable)
```

#### Potential Use Cases

| Use Case | Description |
|----------|-------------|
| **Goal planners** | GOAP/HTN planners emit ChainTree plans |
| **Dynamic scheduling** | Build execution order at runtime |
| **Self-modifying systems** | Adapt behavior based on learned patterns |
| **Remote updates** | Receive new plans over network |
| **Scripted behaviors** | Lua/Python scripts generate trees |

#### Design Considerations (TBD)

- Memory allocation strategy (arena per plan?)
- Plan validation before execution
- Atomic tree replacement (no partial swaps)
- Garbage collection of replaced plans
- Sandboxing for untrusted plan sources
- Binary format for network transmission
- Difference between server-class (dynamic) and embedded (static-only) targets

#### Relationship to Static Trees

```
┌─────────────────────────────────────────────────────────────┐
│                     Static Trees                            │
│  (Compiled at build time, stored in ROM/flash)              │
│                                                             │
│  ┌─────────┐  ┌─────────┐  ┌─────────┐                     │
│  │  init   │  │  main   │  │  fault  │                     │
│  └─────────┘  └────┬────┘  └─────────┘                     │
│                    │                                        │
│               ┌────┴────┐                                   │
│               ▼         ▼                                   │
│          ┌─────────┐ ┌─────────┐                           │
│          │ static  │ │ PLAN    │ ◄── Dynamic slot          │
│          │ logic   │ │ SLOT    │                           │
│          └─────────┘ └────┬────┘                           │
│                           │                                 │
└───────────────────────────┼─────────────────────────────────┘
                            │
                            ▼
┌─────────────────────────────────────────────────────────────┐
│                    Dynamic Plans                            │
│  (Generated at runtime, stored in RAM)                      │
│                                                             │
│  s_expr.compile() ──► ┌─────────────────┐                  │
│                       │ runtime-built   │                  │
│  Planner output ────► │ node_t array    │                  │
│                       └─────────────────┘                  │
│  Network receive ──►                                        │
│                                                             │
└─────────────────────────────────────────────────────────────┘
```

This extension would enable hybrid systems where the overall structure is static and reliable, but specific subtrees can be dynamically generated and hot-swapped.

## Output Formats

### C Header Format

The generated header includes:

```c
// Function name tables (shared)
static const char* const module_oneshot_names[] = {...};
static const char* const module_boolean_names[] = {...};
static const char* const module_main_names[] = {...};
static const char* const module_strings[] = {...};

// Per-tree node arrays
static const node_t module_tree1_nodes[] = {...};
static const param_t module_tree1_params[] = {...};

// Tree definitions
static const tree_def_t module_trees[] = {...};

// Module definition
static const module_def_t module_module = {...};
```

### Binary Format

Compact binary blob with:

| Section | Description |
|---------|-------------|
| Header (32 bytes) | Magic, version, counts |
| String index | Offsets into string blob |
| String blob | Length-prefixed strings |
| Tree directory | Tree metadata |
| Tree data | Nodes and parameters |

## Node Type Encoding

The `type` field uses the upper 2 bits for table selection:

| Bits 7-6 | Table | Description |
|----------|-------|-------------|
| `00` | Opcode | Built-in control structures |
| `01` | Oneshot | `@` function reference |
| `10` | Boolean | `?` function reference |
| `11` | Main | `!` function reference |

Built-in opcodes (when table = `00`):

| Code | Opcode |
|------|--------|
| `0x01` | pipeline |
| `0x02` | if |
| `0x03` | if_else |
| `0x04` | cond |
| `0x05` | dispatch |
| `0x06` | and |
| `0x07` | or |
| `0x08` | not |
| `0x09` | quote |
| `0x0A` | dbg |
| `0x0B` | clause |
| `0x0C` | case |

## Control Flow Codes

| Code | Value | Description |
|------|-------|-------------|
| `CFL_CONTINUE` | 0 | Continue to next node |
| `CFL_HALT` | 1 | Halt this tick, resume next |
| `CFL_TERMINATE` | 2 | Terminate tree execution |
| `CFL_RESET` | 3 | Reset tree to initial state |
| `CFL_DISABLE` | 4 | Disable tree |
| `CFL_FUNCTION_TERMINATE` | 5 | Function-level termination |

## Memory Layout

### node_t Structure (14 bytes)

```c
typedef struct {
    uint8_t  type;          // Table selector + opcode/index
    uint8_t  child_count;   // Number of children
    uint16_t node_index;    // This node's index
    uint16_t first_child;   // Index of first child (0xFFFF = none)
    uint16_t next_sibling;  // Index of next sibling (0xFFFF = none)
    uint16_t fn_index;      // Function table index
    uint16_t param_offset;  // Offset into param array
    uint8_t  param_count;   // Number of parameters
    uint8_t  reserved;      // Flags (is_default for clause/case)
} node_t;
```

### param_t Structure (8 bytes)

```c
typedef struct {
    uint8_t type;           // PARAM_INT32/UINT32/FLOAT32/STRING
    uint8_t reserved[3];
    union {
        int32_t  i32;
        uint32_t u32;
        float    f32;
        uint16_t str_index;
    };
} param_t;
```

## Complete Example

```lua
-- irrigation_zone.lua
dofile("s_expr_dsl.lua")

start_module("irrigation")

-- Initialization tree
start_tree("init")
    pipeline("zone_init")
        oneshot("init_valves", 8)
        oneshot("init_sensors")
        oneshot("load_schedule")
        oneshot("set_state", str("ready"))
    end_pipeline("zone_init")
end_tree("init")

-- Main control tree
start_tree("control")
    cond("zone_state")
        clause("check_schedule")
            condition("sched_cond")
                bool_and("time_check")
                    bool_fn("is_scheduled_time")
                    bool_fn("is_enabled")
                    bool_not("fault_check")
                        bool_fn("has_fault")
                    end_bool_not("fault_check")
                end_bool_and("time_check")
            end_condition("sched_cond")
            action("start_watering")
                pipeline("water_seq")
                    oneshot("open_valve", uint32(1))
                    oneshot("start_timer", 1800)
                    oneshot("set_state", str("watering"))
                end_pipeline("water_seq")
            end_action("start_watering")
        end_clause("check_schedule")
        
        clause("check_complete")
            condition("done_cond")
                bool_fn("timer_expired")
            end_condition("done_cond")
            action("stop_watering")
                pipeline("stop_seq")
                    oneshot("close_valve", uint32(1))
                    oneshot("log_usage")
                    oneshot("set_state", str("ready"))
                end_pipeline("stop_seq")
            end_action("stop_watering")
        end_clause("check_complete")
        
        default_clause("idle")
            action("continue")
                quote("CFL_CONTINUE")
            end_action("continue")
        end_default_clause("idle")
    end_cond("zone_state")
end_tree("control")

-- Fault handler tree
start_tree("fault")
    if_then("fault_handler")
        condition("fault_cond")
            bool_or("any_fault")
                bool_fn("over_pressure")
                bool_fn("no_flow")
                bool_fn("leak_detected")
            end_bool_or("any_fault")
        end_condition("fault_cond")
        action("emergency")
            pipeline("fault_seq")
                oneshot("close_all_valves")
                oneshot("send_alert")
                oneshot("set_state", str("fault"))
            end_pipeline("fault_seq")
        end_action("emergency")
    end_if_then("fault_handler")
end_tree("fault")

end_module("irrigation")

return gen
```

Compile:

```bash
luajit compile.lua irrigation_zone.lua --header=irrigation.h --stats
```

## Integration with C Runtime

The generated header requires `s_engine_types.h` with the type definitions. Your runtime engine walks the flat node array using `first_child` and `next_sibling` indices, looking up functions by name from the string tables.

```c
#include "irrigation.h"

// Function dispatch tables (you implement these)
extern oneshot_fn_t oneshot_dispatch[];
extern boolean_fn_t boolean_dispatch[];
extern main_fn_t main_dispatch[];

void run_tree(const tree_def_t* tree) {
    // Walk nodes starting from root_index
    // Use first_child/next_sibling for traversal
    // Dispatch functions using fn_index into appropriate table
}
```

## License

MIT License

## Author

Glenn Edgar — Onyx Engineering