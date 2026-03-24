# Slim Runtime for SPIKE Prime

## Overview

`se_runtime_spike.py` is a single-file runtime containing only the builtins needed for SPIKE Prime FLL missions. It reduces flash from ~50K bytecode (full runtime) to ~10K bytecode while providing all the control flow needed for mission trees.

## Included Builtins

| Category | Functions |
|----------|-----------|
| Flow control | `se_sequence`, `se_function_interface`, `se_chain_flow`, `se_trigger_on_change` |
| Dispatch | `se_state_machine` |
| Predicates | `se_pred_and`, `se_pred_or`, `se_pred_not`, `se_true`, `se_false` |
| Oneshot | `se_log`, `se_set_field` |
| Delays | `se_tick_delay`, `se_nop` |
| Return codes | `se_return_continue`, `se_return_disable`, `se_return_pipeline_continue/disable/reset/terminate` |

## Not Included (saves ~38K source)

- `se_builtins_quads.py` -- 3-address arithmetic (12K)
- `se_builtins_dict.py` -- dictionary load/extract (8K)
- `se_builtins_stack.py` -- stack frames (6K)
- `se_builtins_spawn.py` -- tree spawning (9K)
- `se_builtins_verify.py` -- verify/timeout (3K)
- Fork, fork_join, while, if_then_else, cond, sequence_once
- Event queue, pointer arrays, u64/f64 accessors

## Adding Functions

To add a function (e.g., `se_while`), copy it from the full runtime file into `se_runtime_spike.py` and add it to the `builtins` dict:

```python
# Copy from se_builtins_flow_control.py:
def se_while(inst, node, event_id, event_data):
    ...

# Add to builtins dict at bottom:
builtins = {
    ...
    "se_while": se_while,
}
```

No imports to manage -- everything is in one file.

## Usage

```python
import se_runtime_spike as se

mod = se.new_module(module_data, se.builtins)
se.register_fns(mod, spike_user_functions)
inst = se.new_instance(mod, "match_dispatcher")

while True:
    result = se.tick_once(inst, se.SE_EVENT_TICK, None)
    if result in (se.SE_FUNCTION_TERMINATE, se.SE_FUNCTION_DISABLE):
        break
```

## Size Comparison

| | Full Runtime | Slim Runtime |
|---|---|---|
| Source | 101,706 bytes (15 files) | 24,454 bytes (1 file) |
| Bytecode (est) | ~50K | ~10K |
| Import heap (unfrozen) | 63,648 bytes | 13,920 bytes |
| Import heap (frozen) | 0 | 0 |
