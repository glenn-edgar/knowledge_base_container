# Continue: ChainTree MicroPython incremental_binary

## Status (2026-03-25)

17 of 26 tests PASS. Test results match C at correct tick counts.

### CRITICAL OPEN ISSUE: SM state re-entry cycling

**Problem:** SM2 in fifth_test cycles through states repeatedly. C runtime does ONE cycle then stops. MicroPython output is correct at termination (301 ticks) but produces extra state transition log messages.

**Root cause unknown.** Investigated:
- Init one-shot re-firing on state re-entry (C's cfl_disable_node clears all user flags including INITIALIZED, so init SHOULD re-fire — but C output shows no re-firing)
- Deferred state transitions (implemented — ns["state"] vs ns["user_data"])
- Sync event mechanism (implemented — blocks SM children while sync pending)
- Enable-without-reinit (tried — doesn't match C's cfl_disable_node which clears all flags)

**Hypothesis:** C's `cfl_terminate_node_tree` walker uses `cfl_get_forward_enabled_links` which only visits ENABLED children. When SM terminates a state, only the ENABLED nodes in that state's subtree get terminated. Previously-disabled (already-fired) one-shot nodes like CFL_DISABLE are skipped by the terminate walker. When the state is re-enabled, `cfl_column_init` calls `cfl_enable_all_nodes` which enables all children — but `cfl_smart_arena_alloc` may skip re-allocation for nodes that already have arena data, effectively making the init one-shot a no-op for previously-fired nodes.

**Next step:** Need to verify whether `cfl_smart_arena_alloc` returning `already_allocated=true` causes the one-shot to skip work. If so, the fix is: track which nodes have had their init one-shot fire and don't re-fire on subsequent enables. This is an arena-level optimization in C that our MicroPython runtime doesn't have.

### ARCHITECTURAL DECISION NEEDED: Walker model

The current recursive walker model diverges from C's flat iterative walker in several ways:

1. **Event delivery:** C walks the subtree from target node for every event. Our recursive model invokes the target node which recursively invokes children. Functionally equivalent but ordering differs.

2. **CT_TERMINATE:** Now correctly terminates only the parent column (matching C). But the recursive model means TERMINATE fires inside a column walker that's already mid-iteration.

3. **HALT from composites:** Leaf HALT stops siblings; composite HALT continues. This matches C's CT_STOP_SIBLINGS which pops parent but continues grandparent.

4. **Init one-shot dispatch:** Centralized in invoke_main via _dispatch_init_fn. Fires during first initialization. In C, the walker calls the one-shot separately from the main function.

**The user wants to rebuild the walker from scratch to be solid.** The current approach of patching the recursive model to match C behavior is accumulating complexity. A flat iterative walker matching C's `CT_Tree_Walker` would eliminate these behavioral differences.

## Passing Tests (17)

| # | Test | Ticks |
|---|------|-------|
| 0 | first_test | 121 |
| 1 | second_test | 201 |
| 2 | fourth_test | 201 |
| 3 | fifth_test | 301 |
| 4 | sixth_test | 201 |
| 5 | seventh_test | 201 |
| 6 | eighth_test | 101 |
| 7 | ninth_test | 101 |
| 9 | eleventh_test | 63 |
| 10 | twelfth_test | 105 |
| 11 | thirteenth_test | 331 |
| 12 | fourteenth_test | 201 |
| 13 | seventeenth_test | 394 |
| 14 | eighteenth_test | 373 |
| 18 | twenty_second_test | 101 |
| 22 | twenty_sixth_test | 301 |
| 25 | twenty_ninth_test | 1 |

## Failing Tests (9)

| # | Test | Reason |
|---|------|--------|
| 8 | tenth_test | Supervisor reset/failure window |
| 15 | ninteenth_test | SM_EVENT_FILTERING_MAIN |
| 16 | twentieth_test | JOIN on HALTed column |
| 17 | twenty_first_test | Start/stop test control |
| 19 | twenty_third_test | AVRO_VERIFY_PACKET |
| 20 | twenty_fourth_test | CFL_STREAMING_TRANSFORM_PACKET |
| 21 | twenty_fifth_test | CFL_STREAMING_COLLECT_PACKETS |
| 23 | twenty_seventh_test | CFL_CONTROLLED_NODE_CONTAINER_MAIN |
| 24 | twenty_eighth_test | CFL_CONTROLLED_NODE_CONTAINER_MAIN |

## Implementation Summary

### Files modified (ct_runtime/)
- **ct_runtime.py** — Core engine: invoke_main, walk_column_dfs, tick_once, tick_targeted, _dispatch_init_fn, event queue, bitmask
- **ct_builtins.py** — Aggregated import table
- **ct_builtins_leaf.py** — CFL_DISABLE (simplified: all behavior in init one-shots)
- **ct_builtins_flow.py** — CFL_COLUMN_MAIN, CFL_SEQUENCE, CFL_FOR_MAIN, CFL_PARALLEL, CFL_LOCAL_ARENA_MAIN
- **ct_builtins_gate.py** — CFL_GATE_NODE_MAIN
- **ct_builtins_composite.py** — CFL_WHILE_MAIN, CFL_EVENT_LOGGER (event filtering), CFL_DF_MASK_MAIN, CFL_JOIN_MAIN, bitmask predicates
- **ct_builtins_state.py** — CFL_STATE_MACHINE_MAIN (sync events, deferred transitions), _dispatch_change_state
- **ct_builtins_wait.py** — CFL_WAIT (internal guard, timeout), CFL_VERIFY, CFL_WAIT_FOR_EVENT
- **ct_builtins_exception.py** — NEW: CFL_EXCEPTION_CATCH_ALL_MAIN, CFL_EXCEPTION_CATCH_MAIN (3-phase), CFL_RECOVERY_MAIN, heartbeat

### Files modified (lua_dsl/)
- **luajit_pipeline/stage6_micropython.lua** — Node tuple: optional init_fn at index 6, ltree path resolution to node indices

### Key architectural changes
- CT_TERMINATE only terminates parent column (not whole tree)
- Init function dispatch centralized in invoke_main
- CFL_DISABLE is now a pure CFL_DISABLE (all behavior in one-shots)
- Column walker: leaf HALT breaks siblings, composite HALT continues
- Sequence returns CT_DISABLE when all children complete
- Event logger filters by configured event ID list
- Named events delivered via tick_targeted to specific subtrees
- SM sync events block children via sync_event_id_valid flag
