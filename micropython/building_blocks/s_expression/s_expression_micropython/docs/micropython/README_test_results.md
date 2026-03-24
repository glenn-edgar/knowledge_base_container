# MicroPython Test Results

## Test Runner

All tests are run via `dsl_tests/run_all_mpy.py` under the `micropython` binary:

```bash
cd s_expression_micropython/dsl_tests
micropython run_all_mpy.py
```

Each test:
1. Compiles DSL source to `_module_mpy.py` via `s_build.sh`
2. Imports the module under MicroPython
3. Registers builtins + user functions
4. Runs a tick loop until completion or max ticks
5. Reports PASS/FAIL/ERROR

## Results

| Test | Status | Ticks | Features Exercised |
|------|--------|-------|--------------------|
| return_test | PASS | 1 each | All 18 return codes (continue through pipeline_skip_continue) |
| loop_test | PASS | 221 | se_while, field_increment_and_test, state_increment_and_test, nested loops |
| state_machine | PASS | 363 | se_state_machine, state transitions, user oneshot functions |
| dispatch | PASS | 42 | se_event_dispatch, se_queue_event, event queue drain |
| advanced_primitive | PASS | 42 | se_cond, se_if_then_else, se_check_event, event dispatch |
| complex_sequence | PASS | 438 | fork_join, verify_elapsed_time/events, wait_event, time_delay, wait_timeout |
| callback_function | PASS | 1 | se_load_function, se_exec_fn, function pointers in blackboard |
| stack_test | PASS | 841 | se_frame_allocate, quad_iadd/isub/fadd/fsub, stack locals, while loops |
| stack_equations | PASS | 841 | quad_expr (equation strings), quad_multi, frame_vars, float operations |
| json | PASS | 1 | se_load_dictionary, dict_extract_int/float/bool/hash, string + hash paths |
| function_dictionary | ERROR | -- | se_load_function_dict works; test needs blackboard pre-init for peripheral config |

## Coverage

The 10 passing tests cover all major runtime subsystems:

- **Core engine**: invoke_main, invoke_oneshot, invoke_pred, invoke_any, tick_once
- **Flow control**: sequence, sequence_once, function_interface, fork, fork_join, chain_flow, while, if_then_else, cond, trigger_on_change
- **Dispatch**: state_machine, event_dispatch
- **Predicates**: and, or, not, field comparisons, increment_and_test, check_event
- **Oneshot**: log, set_field, queue_event
- **Delays**: tick_delay, time_delay, wait, wait_event, wait_timeout, nop
- **Verify**: verify, verify_elapsed_time, verify_elapsed_events
- **Stack**: frame_allocate, frame_free, push/pop, stack_frame_instance
- **Quads**: integer and float arithmetic, bitwise ops, comparisons, move
- **Dictionary**: load_dictionary, extract (string and hash paths), store_ptr
- **Return codes**: all 18 variants
- **Callback**: load_function, exec_fn

## Bugs Found and Fixed During Testing

1. **se_trigger_on_change children layout** -- initial state was expected as a child node but the generator puts it in `node[N_PARAMS][0]`. Fixed children to `[0]=pred, [1]=rise, [2]=fall`.

2. **se_stack_frame_instance return value list** -- used `.get("items")` on a param tuple. Fixed to scan between `list_start`/`list_end` param markers.

3. **se_load_function_dict missing** -- not implemented in MicroPython builtins. Added to `se_builtins_dict.py`.
