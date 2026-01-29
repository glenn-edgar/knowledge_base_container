# State Machine Test Suite

This test suite validates the S-Expression engine's state machine functionality using `se_field_dispatch` for state-driven control flow.

## Overview

The `state_machine_test` module demonstrates a three-state machine that transitions through states 0 → 1 → 2, with each state executing for 100 ticks before transitioning.

## Module Structure

```
state_machine_test
├── Records
│   └── state_machine_blackboard
│       └── state (int32)
└── Trees
    ├── event_dispatch_test (placeholder)
    └── state_machine_test
```

## State Machine Flow

```
┌─────────────────────────────────────────────────────────────┐
│                    state_machine_test                        │
├─────────────────────────────────────────────────────────────┤
│  se_sequence                                                │
│    ├── se_i_set_field("state", 0)                          │
│    ├── se_log("State machine test started")                │
│    └── se_field_dispatch("state", case_fn)                 │
│          │                                                  │
│          ├── case 0: State 0 → 100 ticks → state=1 → HALT  │
│          ├── case 1: State 1 → 100 ticks → state=2 → HALT  │
│          └── default: State 2 → 100 ticks → TERMINATE      │
└─────────────────────────────────────────────────────────────┘
```

## State Transitions

| State | Ticks | Next State | Return Code |
|-------|-------|------------|-------------|
| 0 | 1-100 | 1 | SE_HALT |
| 1 | 101-200 | 2 | SE_HALT |
| 2 (default) | 201-300 | - | SE_TERMINATE |

**Total execution: ~300 ticks**

## Files

| File | Description |
|------|-------------|
| `state_machine_test.lua` | DSL source |
| `state_machine_test.h` | Tree hash definitions |
| `state_machine_test_bin_32.h` | Binary module (ROM) |
| `state_machine_test_records.h` | C struct definitions |
| `state_machine_test_user_functions.c` | User function implementations |
| `state_machine_test_user_functions.h` | User function prototypes |
| `main.c` | Test harness |

## User Functions

### CFL_DISABLE_CHILDREN

Disables all child nodes in the current context.

```c
void cfl_disable_children(
    s_expr_tree_instance_t* inst,
    const s_expr_param_t* params,
    uint16_t param_count,
    s_expr_event_type_t event_type,
    uint16_t event_id,
    void* event_data
);
```

**Parameters**: None

**Behavior**:
- Handles `SE_EVENT_INIT` and `SE_EVENT_TERMINATE` silently
- On `SE_EVENT_TICK`, disables all children
- Prints debug message: `"cfl_disable_children"`

**DSL Usage**:
```lua
local o0 = o_call("CFL_DISABLE_CHILDREN")
end_call(o0)
```

### CFL_ENABLE_CHILD

Enables a specific child node by index.

```c
void cfl_enable_child(
    s_expr_tree_instance_t* inst,
    const s_expr_param_t* params,
    uint16_t param_count,
    s_expr_event_type_t event_type,
    uint16_t event_id,
    void* event_data
);
```

**Parameters**:
| Index | Type | Description |
|-------|------|-------------|
| 0 | int/uint | Child index to enable |

**Behavior**:
- Returns early on `SE_EVENT_INIT` and `SE_EVENT_TERMINATE`
- Validates parameter count (requires ≥ 1)
- Validates parameter type (must be INT or UINT)
- Throws exception on validation failure
- Prints debug message: `"cfl_enable_child: enabling child N"`

**DSL Usage**:
```lua
local o1 = o_call("CFL_ENABLE_CHILD")
    int(0)  -- child index
end_call(o1)
```

## Exception Handling

The user functions use the `EXCEPTION` macro from `s_engine_exception.h`:

```c
if (param_count < 1) {
    EXCEPTION("cfl_enable_child: need at least one parameter");
}

if ((params[0].type != S_EXPR_PARAM_INT) && (params[0].type != S_EXPR_PARAM_UINT)) {
    EXCEPTION("cfl_enable_child: first parameter must be an integer or unsigned integer");
}
```

## Test Harness (main.c)

### Structure

```
main()
├── Load from ROM
│   └── run_state_machine_tests()
│       └── test_state_machine()
└── Load from file
    └── run_state_machine_tests()
        └── test_state_machine()
```

### test_state_machine()

Runs the state machine through a tick loop:

1. Creates tree by hash
2. Gets blackboard pointer to observe state
3. Ticks until `SE_TERMINATE` or max ticks (500)
4. Prints state at tick 1, every 100 ticks, and on termination
5. Reports pass/fail

### Expected Output

```
╔════════════════════════════════════════╗
║    STATE MACHINE TEST                  ║
╚════════════════════════════════════════╝

Testing state machine with tick loop...

  Initial state: 0

  Running tick loop...
  [DEBUG] State machine test started
  [DEBUG] State 0
cfl_disable_children
cfl_enable_child: enabling child 0
    Tick   1: state=0, result=HALT
    Tick 100: state=0, result=HALT
  [DEBUG] State 1
cfl_disable_children
cfl_enable_child: enabling child 1
    Tick 200: state=1, result=HALT
  [DEBUG] State 2
cfl_disable_children
cfl_enable_child: enabling child 2
    Tick 300: state=2, result=HALT
  [DEBUG] State 2 terminated
    Tick 301: state=2, result=TERMINATE

  Final state: 2
  Total ticks: 301
  Final result: TERMINATE

  ✅ PASSED - State machine terminated normally
```

## Building

```bash
gcc -o state_machine_test \
    main.c \
    state_machine_test_user_functions.c \
    -I./include \
    -L./lib -ls_engine
```

## Running

```bash
./state_machine_test
```

## DSL State Case Pattern

Each state follows this pattern:

```lua
case_fn[N] = function() 
    se_case(state_value, function()
        se_sequence(function()
            se_log("State N")
            
            -- Disable all children, enable specific one
            local o0 = o_call("CFL_DISABLE_CHILDREN")
            end_call(o0)
            local o1 = o_call("CFL_ENABLE_CHILD")
                int(N)
            end_call(o1)
            
            -- Wait 100 ticks
            se_tick_delay(100)
            
            -- Transition
            se_set_field("state", next_state)
            se_return_halt()  -- or se_return_terminate() for final
        end)
    end) 
end
```

## Blackboard Access

The test harness accesses the blackboard to observe state:

```c
// Get blackboard pointer
state_machine_blackboard_t* bb = (state_machine_blackboard_t*)s_expr_tree_get_blackboard(tree);

// Read state during tick loop
printf("state=%d\n", bb->state);
```

## Record Definition

```lua
RECORD("state_machine_blackboard")
    FIELD("state", "int32")
END_RECORD()
```

Generated C struct:

```c
typedef struct {
    int32_t state;
} state_machine_blackboard_t;
```

## Key Concepts Demonstrated

1. **Field dispatch** - `se_field_dispatch` switches on blackboard field value
2. **State transitions** - `se_set_field` modifies state between ticks
3. **Tick delays** - `se_tick_delay(100)` waits for N ticks
4. **Child control** - `CFL_DISABLE_CHILDREN` / `CFL_ENABLE_CHILD` pattern
5. **Return codes** - `SE_HALT` continues, `SE_TERMINATE` ends
6. **Blackboard observation** - External code reads state via pointer

## DSL Helper Functions

The following DSL helper functions are **built into the runtime system**. They wrap engine-provided S-functions (`SE_STATE_MACHINE`, `SE_SET_FIELD`, `SE_LOG`) and provide compile-time validation.

### se_state_machine

Dispatches execution based on a blackboard field value. This is the core state machine construct.

```lua
function se_state_machine(state_field, cases_fn)
    -- Reset case tracking for this dispatch
    dispatch_case_values = {}
    in_dispatch = true
    
    local success, err = pcall(function()
        local c = m_call("SE_STATE_MACHINE")
            field_ref(state_field)
            if type(cases_fn) == "function" then
                cases_fn()
            elseif type(cases_fn) == "table" then
                for _, case_fn in ipairs(cases_fn) do
                    case_fn()
                end
            else
                error("se_field_dispatch: cases must be function or table")
            end
        end_call(c)
    end)
    
    -- Clean up tracking state
    in_dispatch = false
    dispatch_case_values = {}
    
    if not success then
        error(err)
    end
end
```

**Parameters**:
| Parameter | Type | Description |
|-----------|------|-------------|
| `state_field` | string | Blackboard field name to dispatch on |
| `cases_fn` | function or table | Case definitions |

**Usage**:
```lua
se_state_machine("state", case_fn)
-- or
se_state_machine("state", function()
    se_case(0, function() ... end)
    se_case(1, function() ... end)
    se_case("default", function() ... end)
end)
```

**Engine Function**: `SE_STATE_MACHINE` (main function)

---

### se_case

Defines a case within a state machine dispatch. Includes compile-time duplicate detection.

```lua
local dispatch_case_values = {}
local in_dispatch = false

function se_case(case_val, action_fn)
    local int_val
    
    if case_val == "default" then
        int_val = -1
    elseif type(case_val) == "number" and math.floor(case_val) == case_val then
        int_val = case_val
    else
        error("se_case: first parameter must be integer or 'default', got: " .. tostring(case_val))
    end
    
    -- Check for duplicates if inside a dispatch
    if in_dispatch then
        if dispatch_case_values[int_val] then
            local label = (int_val == -1) and "default" or tostring(int_val)
            error("se_case: duplicate case value: " .. label)
        end
        dispatch_case_values[int_val] = true
    end
    
    int(int_val)
    action_fn()
end
```

**Parameters**:
| Parameter | Type | Description |
|-----------|------|-------------|
| `case_val` | integer or `"default"` | State value to match |
| `action_fn` | function | Action to execute when matched |

**Special Values**:
- `"default"` → `-1` (matches when no other case matches)

**Compile-Time Validation**:
- Rejects non-integer values
- Detects duplicate case values within same dispatch
- Error example: `se_case: duplicate case value: 0`

**Usage**:
```lua
se_case(0, function()
    se_log("State 0")
    se_set_field("state", 1)
end)

se_case("default", function()
    se_log("Unknown state")
end)
```

---

### se_set_field / se_i_set_field

Sets a blackboard field to an integer value.

```lua
function se_set_field(target_field, value)
    local c = o_call("SE_SET_FIELD")
        field_ref(target_field)
        int(value)
    end_call(c)
end

function se_i_set_field(target_field, value)
    local c = io_call("SE_SET_FIELD")
        field_ref(target_field)
        int(value)
    end_call(c)
end
```

**Parameters**:
| Parameter | Type | Description |
|-----------|------|-------------|
| `target_field` | string | Blackboard field name |
| `value` | integer | Value to set |

**Variants**:
| Function | Call Type | Behavior |
|----------|-----------|----------|
| `se_set_field` | `o_call` | Oneshot - executes every tick |
| `se_i_set_field` | `io_call` | Init-oneshot - executes only on first tick |

**Engine Function**: `SE_SET_FIELD` (oneshot)

**Usage**:
```lua
-- Initialize state to 0 (runs once)
se_i_set_field("state", 0)

-- Set next state (runs every tick when reached)
se_set_field("state", 1)
```

---

### se_log

Outputs a debug message via the engine's debug callback.

```lua
function se_log(message)
    local c = o_call("SE_LOG")
        str_ptr(message)
    end_call(c)
end
```

**Parameters**:
| Parameter | Type | Description |
|-----------|------|-------------|
| `message` | string | Message to output |

**Engine Function**: `SE_LOG` (oneshot)

**Output**: Calls `debug_callback(inst, message)` registered via `s_expr_module_set_debug()`

**Usage**:
```lua
se_log("State machine started")
se_log("Transitioning to state 1")
```

**Test Harness Output**:
```
  [DEBUG] State machine started
  [DEBUG] Transitioning to state 1
```

---

## Engine Built-in Functions Summary

These DSL helpers wrap the following engine-provided S-functions:

| DSL Helper | Engine Function | Type | Description |
|------------|-----------------|------|-------------|
| `se_state_machine` | `SE_STATE_MACHINE` | main | Field-based state dispatch |
| `se_set_field` | `SE_SET_FIELD` | oneshot | Set blackboard field |
| `se_i_set_field` | `SE_SET_FIELD` | io-oneshot | Set field on init only |
| `se_log` | `SE_LOG` | oneshot | Debug output |
| `se_tick_delay` | `SE_TICK_DELAY` | main | Wait N ticks |
| `se_return_halt` | `SE_RETURN` | oneshot | Return SE_HALT |
| `se_return_terminate` | `SE_RETURN` | oneshot | Return SE_TERMINATE |
| `se_sequence` | `SE_SEQUENCE` | main | Execute children in order |

These are registered automatically via `s_engine_register_builtins(engine)` and require no user implementation.