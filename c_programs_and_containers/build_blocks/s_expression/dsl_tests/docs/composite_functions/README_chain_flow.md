# SE_CHAIN_FLOW - ChainTree Walker Emulation Composite

## Overview

`se_chain_flow` emulates the default sequencing behavior of ChainTree's tree walker. Unlike other composites, it processes children in a **chain** where each child's result determines whether to continue, halt, reset, or terminate the entire chain.

This is the fundamental control flow primitive that ChainTree is built around.

## Key Differences from Other Composites

| Aspect | `se_chain_flow` | `se_fork` | `se_sequence` |
|--------|-----------------|-----------|---------------|
| **PIPELINE_HALT** | Stops chain, returns CONTINUE | Counts as active | Pauses sequence |
| **PIPELINE_CONTINUE** | Advances to next child | Counts as active | N/A (uses HALT) |
| **PIPELINE_RESET** | Resets ALL children | Resets that child | Advances |
| **PIPELINE_TERMINATE** | Terminates ALL children | Terminates that child | Propagates |
| **Execution model** | Sequential with control | Parallel | Sequential |

## Behavior

### Result Code Handling

| Child Returns | Chain Flow Action |
|---------------|-------------------|
| `SE_FUNCTION_HALT` (7) | Convert to `SE_PIPELINE_CONTINUE` - continue chain |
| **APPLICATION (0-5)** | Propagate immediately to caller |
| **FUNCTION (6,8-11)** | Propagate immediately to caller |
| `SE_PIPELINE_CONTINUE` (12) | Continue to next child |
| `SE_PIPELINE_HALT` (13) | **Stop chain this tick**, return CONTINUE |
| `SE_PIPELINE_DISABLE` (16) | Terminate child, continue to next |
| `SE_PIPELINE_TERMINATE` (14) | **Terminate ALL children**, return TERMINATE |
| `SE_PIPELINE_RESET` (15) | **Reset ALL children**, return CONTINUE |
| `SE_PIPELINE_SKIP_CONTINUE` (17) | Skip remaining children this tick |

### Critical Behaviors

#### PIPELINE_HALT Stops the Chain

When a child returns `SE_PIPELINE_HALT`, the chain **stops processing** for this tick but returns `SE_PIPELINE_CONTINUE` to its parent. The chain resumes from that child on the next tick.

```lua
se_chain_flow(function()
    se_log("A")           -- Tick 1: executes
    se_tick_delay(10)     -- Tick 1: returns HALT, chain stops
    se_log("B")           -- Tick 11: executes (after delay completes)
end)
```

#### PIPELINE_RESET Resets the Entire Chain

When a child returns `SE_PIPELINE_RESET`, ALL children are terminated and reset. This allows cyclic/looping behavior.

```lua
se_chain_flow(function()
    se_log("Loop iteration")
    se_tick_delay(10)
    se_queue_event(...)
    se_return_pipeline_reset()  -- Resets entire chain, loops forever
end)
```

#### PIPELINE_TERMINATE Stops Everything

When a child returns `SE_PIPELINE_TERMINATE`, ALL children are terminated and the chain returns `SE_PIPELINE_TERMINATE` to its parent.

## Lifecycle Events

### INIT
- Returns `SE_PIPELINE_CONTINUE`
- Children start active by default

### TERMINATE
- Terminates all children
- Returns `SE_PIPELINE_CONTINUE`

### TICK
- Iterates through active children
- Processes each child's result
- Returns based on chain state

## Usage

### Lua DSL

```lua
se_chain_flow(function()
    se_log("Step 1")
    se_tick_delay(10)
    se_log("Step 2")
    se_set_field("value", 42)
    se_return_pipeline_disable()  -- Chain completes
end)
```

### Cyclic Event Generator

The primary use case - a looping chain that periodically generates events:

```lua
se_case(1, function()
    se_chain_flow(function()
        se_log("State 1")
        se_tick_delay(20)
        se_set_field("event_data_3", 3.3)
        se_set_field("event_data_4", 4.4)
        se_queue_event(USER_EVENT_TYPE, USER_EVENT_3, "event_data_3")
        se_queue_event(USER_EVENT_TYPE, USER_EVENT_4, "event_data_4")
        se_return_pipeline_reset()  -- Loop back to start
    end)
end)
```

**Execution:**
```
Tick 1:  "State 1" logs, delay starts, chain halts
Tick 2-20: Delay running, chain halted
Tick 21: Delay completes, fields set, events queued
         RESET: all children terminated and reset
Tick 22: Chain starts over - "State 1" logs again
...
```

### Conditional Termination

```lua
se_chain_flow(function()
    se_log("Processing...")
    se_tick_delay(10)
    
    -- Predicate check could go here
    se_if(some_condition, function()
        se_return_pipeline_terminate()  -- Stop everything
    end)
    
    se_log("Continuing...")
    se_return_pipeline_reset()  -- Loop if not terminated
end)
```

## Comparison: Chain Flow vs Sequence

### se_sequence

Sequential execution, advances on child completion:

```lua
se_sequence(function()
    se_log("A")           -- Tick 1
    se_tick_delay(10)     -- Ticks 1-10
    se_log("B")           -- Tick 11
end)
-- Returns DISABLE when all children complete
```

- Child returns `PIPELINE_HALT` → sequence pauses
- Child returns `PIPELINE_DISABLE` → sequence advances
- No looping capability

### se_chain_flow

Chain execution, explicit control flow:

```lua
se_chain_flow(function()
    se_log("A")           -- Tick 1
    se_tick_delay(10)     -- Tick 1: HALT stops chain
    se_log("B")           -- Tick 11: continues
    se_return_pipeline_reset()  -- Loops back
end)
-- Returns CONTINUE while running, can loop forever
```

- Child returns `PIPELINE_HALT` → chain stops, resumes next tick
- Child returns `PIPELINE_RESET` → entire chain resets
- Built for looping/cyclic behavior

## ChainTree Walker Emulation

`se_chain_flow` emulates how ChainTree's default tree walker processes nodes:

1. **Walk children in order**
2. **CONTINUE** → move to next sibling
3. **HALT** → stop walking, resume next tick
4. **RESET** → restart from first child
5. **TERMINATE** → stop everything

This allows porting ChainTree behavior patterns directly to the S-Expression engine.

## Important Notes

### Return Codes Work Here

Unlike `se_fork` and `se_fork_join`, return code functions work correctly in `se_chain_flow`:

```lua
se_chain_flow(function()
    se_tick_delay(10)
    se_return_pipeline_reset()   -- Loops the chain
    -- or
    se_return_pipeline_disable() -- Completes the chain
end)
```

### FUNCTION_HALT Conversion

`SE_FUNCTION_HALT` from children (like `se_tick_delay`) is converted to `SE_PIPELINE_CONTINUE`, which then **continues to the next child**. This is different from `se_sequence` which would pause.

However, `SE_PIPELINE_HALT` (returned by composites) **stops the chain** for this tick.

### Active Count Tracking

The chain tracks active children. When `active_count == 0`, the chain returns `SE_PIPELINE_DISABLE` (complete).

## State Storage

- No state variable used
- No user_flags used
- Active status tracked per-child via node flags

## Error Handling

- Unknown result codes increment active count and continue
- Invalid child indices are skipped
- Non-callable parameters are skipped
