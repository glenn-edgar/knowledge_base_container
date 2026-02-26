# ChainTree DSL Reference

A Python DSL for defining hierarchical control trees targeting embedded C runtimes (8KB+ RAM on ARM Cortex-M0+).

## Table of Contents

1. [Design Philosophy](#design-philosophy)
2. [Execution Model](#execution-model)
3. [Quick Start](#quick-start)
4. [API Reference](#api-reference)
5. [Structural Rules](#structural-rules)
6. [Pattern Catalog](#pattern-catalog)
7. [Built-in Functions](#built-in-functions)
8. [Target Platform Notes](#target-platform-notes)
9. [Formal Specification](#formal-specification)

---

## Design Philosophy

### Historical Context

ChainTree is the hierarchical generalization of **Chain Flow** (2005-2024), a linear event-driven control system inspired by PLC ladder logic scan-cycle execution. Chain Flow successfully shipped in gasoline-station video controllers, Modbus RTU controllers, and a 120-zone agricultural irrigation system running continuously since 2013.

**Chain Flow limitations that motivated ChainTree:**
- Chains were strictly linear - deep nesting became an organizational burden
- Dynamic structural changes were difficult on embedded targets
- Behavior Tree patterns (selector, fallback) and Plexil patterns (parallel execution with invariant monitoring) were awkward to express across multiple chains

**ChainTree's solution:** Promote the linear chain to a first-class composable entity that can contain other chains in a true hierarchical tree topology, without sacrificing deterministic, event-driven scan semantics.

### Design Goals

1. **Unified model**: State machines, sequences, and parallel behaviors in one framework
2. **Deterministic execution**: Scan-based traversal with predictable timing
3. **Zero-allocation runtime**: Suitable for safety-critical embedded systems
4. **Resource efficiency**: < 8KB RAM, < 60KB flash on Cortex-M0+
5. **Hierarchical composition**: Deep nesting without organizational burden

### Influences

- **PLC Ladder Logic**: Scan-cycle execution, deterministic timing
- **Behavior Trees**: Hierarchical composition, selector/sequence patterns
- **NASA Plexil**: Invariant monitoring, parallel execution, skip/rollback semantics
- **Erlang/OTP**: Supervisor trees, restart strategies, fault isolation

---

## Execution Model

### Scan-Cycle Semantics (PLC Heritage)

ChainTree inherits scan-cycle execution from PLC ladder logic:

```
┌─────────────────────────────────────────────────────────────┐
│                        TICK CYCLE                           │
│                                                             │
│  1. Event arrives (tick, timer, user-defined)               │
│  2. Tree walker traverses all active nodes (depth-first)    │
│  3. Each node's main_function called once                   │
│  4. Opcodes control propagation to siblings/children        │
│  5. Cycle completes, wait for next event                    │
└─────────────────────────────────────────────────────────────┘
```

**Event types:**
- Periodic tick event (typically 1-100ms)
- Time-scaled events (second, minute, hour, day)
- User-defined async events (button_pressed, packet_received, sensor_tripped)

### Core Concepts

ChainTree organizes control logic as a tree of **columns** (composite nodes) and **leaf nodes**. A deterministic tree walker visits all active nodes once per tick.

```
Tree Walker Semantics:
- Traversal: depth-first, left-to-right (sibling order = priority)
- Tick cycle: all active nodes visited once per tick
- Ordering guarantee: earlier-defined siblings execute before later siblings
```

### Node Lifecycle

Every node has four function slots:

| Slot | When Called | Purpose |
|------|-------------|---------|
| `initialization_function` | On node start | Setup |
| `main_function` | Each tick while RUNNING | Core logic |
| `aux_function` | Varies by node type | Boolean condition |
| `termination_function` | On node cleanup | Teardown |

### Node States

```
IDLE → [start/enable] → RUNNING → [main_function called each tick]
                            │
                            ├── asm_halt()      → suspends, resumes next tick
                            ├── asm_reset()     → restarts column from beginning
                            └── asm_terminate() → exits column, signals parent
```

### Return Code Semantics

| Method | Effect |
|--------|--------|
| `asm_halt()` | Suspend execution, resume at this node next tick |
| `asm_reset()` | Restart the containing column from its first child |
| `asm_terminate()` | Exit column, trigger parent's join/continuation logic |
| `asm_disable()` | Disable node, skip during traversal |
| `asm_terminate_system()` | Halt entire knowledge base execution |

---

## Quick Start

```python
from chain_tree_c_low_ram.chain_tree_build.ct_build.chain_tree_master import ChainTreeMaster
from pathlib import Path

ct = ChainTreeMaster(yaml_file=Path("output.yaml"))

ct.start_test(test_name="my_controller", kb_memory_factor=10)

# Define tree structure
launch = ct.define_column(column_name="launch", auto_start=True)
ct.asm_one_shot_handler("INIT_HARDWARE", {})
ct.asm_log_message("System initialized")
ct.asm_wait_time(time_delay=1.0)
ct.asm_halt()
ct.end_column(column_name=launch)

ct.end_test()
ct.check_and_generate_yaml()
```

---

## API Reference

### Test/Knowledge Base Lifecycle

```python
ct.start_test(test_name: str, kb_memory_factor: int = 10)
```
Begin defining a knowledge base (control tree).

```python
ct.end_test()
```
Finalize the knowledge base, validate structure.

```python
ct.check_and_generate_yaml()
```
Validate all KBs and write YAML output file.

---

### Column Types (Composite Nodes)

All `define_*` methods return a column identifier (ltree path string) used with `end_column()`.

#### Basic Column

```python
ct.define_column(
    column_name: str,
    column_data: dict = None,
    auto_start: bool = False
) -> str
```

Sequential execution of child nodes.

#### Fork Column

```python
ct.define_fork_column(
    column_name: str,
    column_data: dict = {}
) -> str
```

Parallel execution of all children. Children run concurrently each tick.

#### For Column

```python
ct.define_for_column(
    column_name: str,
    number_of_iterations: int,
    auto_start: bool = False
) -> str
```

Execute single child N times. **Only one child allowed.**

#### While Column

```python
ct.define_while_column(
    column_name: str,
    aux_function: str = "CFL_NULL",
    user_data: dict = {},
    auto_start: bool = False
) -> str
```

Repeat child while `aux_function` returns true. **Only one child allowed.**

#### Data Flow Event Mask

```python
ct.define_data_flow_event_mask(
    column_name: str,
    event_list: list[str],
    aux_function: str = "CFL_NULL",
    auto_start: bool = False
) -> str
```

Column activates only when ALL specified bitmask bits are set.

#### Local Arena

```python
ct.define_local_arena(
    column_name: str,
    arena_size: int
) -> str
```

Column with dedicated memory arena (for dynamic allocation within subtree).

#### End Column

```python
ct.end_column(column_name: str)
```

Close a column. Must match the most recent unclosed `define_*` call.

---

### Leaf Nodes (asm_* Methods)

Leaf nodes are the atomic operations in the tree.

#### One-Shot Execution

```python
ct.asm_one_shot_handler(
    one_shot_fn: str,
    one_shot_data: dict
)
```

Execute function once, then advance to next node.

```python
ct.asm_log_message(message: str)
```

Log a debug message (calls `CFL_LOG_MESSAGE`).

#### Flow Control

```python
ct.asm_halt()
```
Suspend column, resume at this node next tick.

```python
ct.asm_reset()
```
Restart column from its first child.

```python
ct.asm_terminate()
```
Exit column, signal parent for join/continuation.

```python
ct.asm_disable()
```
Disable current node.

```python
ct.asm_terminate_system()
```
Halt entire system execution.

#### Timing

```python
ct.asm_wait_time(time_delay: float)
```

Block for `time_delay` seconds.

```python
ct.asm_wait_for_event(
    event_id: str,
    event_count: int = 1,
    reset_flag: bool = False,
    timeout: int = 0,
    error_fn: str = "CFL_NULL",
    error_data: dict = {}
) -> str
```

Block until event received `event_count` times. Optional timeout triggers `error_fn`.

```python
ct.asm_wait_for_bitmask(
    bitmask_event_list: list[str],
    reset_flag: bool = False,
    timeout: int = None,
    error_fn: str = "CFL_NULL",
    error_data: dict = None
)
```

Block until all specified bitmask bits are set.

#### Verification (Polling Conditions)

```python
ct.asm_verify(
    verify_fn: str,
    fn_data: dict = None,
    reset_flag: bool = False,
    error_fn: str = "CFL_NULL",
    error_data: dict = None
)
```

Each tick, call `verify_fn`. When it returns true, advance. If `reset_flag=True`, reset column instead.

```python
ct.asm_verify_timeout(
    time_out: float,
    reset_flag: bool = False,
    error_fn: str = "CFL_NULL",
    error_data: dict = None
)
```

Verify with timeout - advances after timeout expires.

```python
ct.asm_verify_bitmask(
    bitmask_event_list: list[str],
    reset_flag: bool = False,
    error_fn: str = "CFL_NULL",
    error_data: dict = None
)
```

Verify bitmask bits are set.

#### Events and Bitmasks

```python
ct.asm_send_named_event(
    node_id: str,
    event_id: str,
    event_data: dict
)
```

Send event to specific node.

```python
ct.asm_send_system_event(
    event_id: str,
    event_data: dict
)
```

Broadcast system-wide event.

```python
ct.asm_set_bitmask(event_list: list[str])
```

Set specified bitmask bits.

```python
ct.asm_clear_bitmask(event_list: list[str])
```

Clear specified bitmask bits.

#### Node Control

```python
ct.asm_enable_nodes(nodes: list[str])
```

Enable specified nodes (by ltree path).

```python
ct.asm_disable_nodes(nodes: list[str])
```

Disable specified nodes.

#### Join/Synchronization

```python
ct.define_join_link(parent_node_name: str)
```

Wait for specified column to terminate before continuing.

---

### State Machines

```python
ct.define_state_machine(
    column_name: str,
    sm_name: str,
    state_names: list[str],
    initial_state: str,
    auto_start: bool,
    aux_function_name: str = "CFL_STATE_MACHINE_NULL"
) -> str
```

Define a state machine container. Use `aux_function_name="CFL_SM_EVENT_SYNC"` for event synchronization on state transitions.

```python
ct.define_state(
    state_name: str,
    column_data: dict = None
) -> str
```

Define a state within the current state machine. Each state is a column.

```python
ct.change_state(
    sm_node_id: str,
    new_state: str,
    sync_event_id: str = None
)
```

Transition to new state. Optional `sync_event_id` synchronizes the transition.

```python
ct.end_state_machine(
    state_node: str,
    sm_name: str
)
```

Close state machine. **All declared states must be defined.**

```python
ct.terminate_state_machine(sm_node_id: str)
```

Stop state machine execution.

```python
ct.reset_state_machine(sm_node_id: str)
```

Reset state machine to initial state.

---

### Supervisors (Erlang-style)

```python
ct.define_supervisor_one_for_one_node(
    column_name: str,
    aux_function: str = "CFL_NULL",
    user_data: dict = {},
    restart_enabled: bool = True,
    reset_limited_enabled: bool = False,
    max_reset_number: int = 1,
    reset_window: int = 10,
    auto_start: bool = False,
    finalize_function: str = "CFL_NULL",
    finalize_function_data: dict = {}
) -> str
```

When a child fails, only that child restarts.

```python
ct.define_supervisor_one_for_all_node(...)  # Same parameters
```

When any child fails, all children restart.

```python
ct.define_supervisor_rest_for_all_node(...)  # Same parameters
```

When a child fails, all children defined after it restart.

```python
ct.define_mark_supervisor_node_failure(data: dict)
```

Mark current node as failed (triggers supervisor restart logic).

**Restart limiting:** When `reset_limited_enabled=True`, tracks failures within `reset_window` seconds. If `max_reset_number` exceeded, supervisor terminates.

---

### Sequences (Try-Until Patterns)

```python
ct.define_sequence_start_node(
    column_name: str,
    initialize_function: str = "CFL_NULL",
    finalize_function: str = "CFL_NULL",
    user_data: dict = {},
    auto_start: bool = False
) -> str
```

Container for sequence-til patterns.

```python
ct.define_sequence_til_pass_node(
    column_name: str,
    finalize_function: str = "CFL_NULL",
    user_data: dict = {}
) -> str
```

Try children in order until one passes.

```python
ct.define_sequence_til_fail_node(
    column_name: str,
    finalize_function: str = "CFL_NULL",
    user_data: dict = {}
) -> str
```

Try children in order until one fails.

```python
ct.mark_sequence_true_link(parent_node_name: str, data: dict = {})
ct.mark_sequence_false_link(parent_node_name: str, data: dict = {})
```

Mark sequence result from within a child.

```python
ct.end_sequence_node(column_name: str)
```

Close sequence node.

---

### Exception Handling

```python
ct.catch_all_exception(
    column_name: str,
    aux_function: str,
    aux_data: dict = {},
    auto_start: bool = True
) -> str
```

Top-level exception catcher.

```python
ct.define_exception_catch(
    column_name: str,
    aux_function_name: str,
    aux_function_data: dict,
    logging_function_name: str,
    logging_function_data: dict = {},
    auto_start: bool = True
) -> str
```

Exception handler with main/recovery/finalize structure.

```python
ct.define_main_exception_column(name: str, ...) -> str
ct.end_main_exception_column(name: str)
```

Main execution column within exception catch.

```python
ct.define_recovery_column(
    name: str,
    max_steps: int,
    skip_condition_function: str,
    skip_condition_data: dict = {}
) -> str
ct.end_recovery_column(name: str)
```

Recovery column - contains step columns for unwinding. `max_steps` defines recovery depth.

```python
ct.define_finalize_column(name: str, ...) -> str
ct.end_finalize_column(name: str)
```

Finalize column - always runs after main or recovery.

```python
ct.exception_catch_end(exception_catch_name: str)
```

Close exception catch. **Requires main + recovery + finalize to be defined.**

```python
ct.asm_raise_exception(exception_id: int, exception_data: dict = {})
ct.asm_set_exception_step(step: int)
```

Raise exception and set recovery step.

#### Heartbeat Monitoring

```python
ct.asm_turn_heartbeat_on(time_out: int)
ct.asm_turn_heartbeat_off()
ct.asm_heartbeat_event()
```

Heartbeat monitoring within exception columns. Missing heartbeats trigger exception.

---

### Watchdog

```python
ct.asm_watch_dog_node(
    wd_time_count: int,
    wd_reset: bool,
    wd_fn: str,
    wd_fn_data: dict
) -> str
```

Create watchdog node. Returns node ID.

```python
ct.asm_enable_watch_dog(node_id: str)
ct.asm_disable_watch_dog(node_id: str)
ct.asm_pat_watch_dog(node_id: str)
```

Control watchdog. Must pat within `wd_time_count` seconds or `wd_fn` triggers.

---

### Multi-KB Coordination

```python
ct.asm_start_stop_tests(
    stop_tests: list[int],
    start_tests: list[int]
)
```

Start/stop other knowledge bases by index.

```python
ct.asm_wait_for_tests_complete(
    test_ids: list[int],
    reset_flag: bool = False,
    timeout: int = 30,
    error_fn: str = "CFL_NULL",
    error_data: dict = None
)
```

Wait for specified KBs to complete.

```python
ct.asm_verify_tests_active(
    test_ids: list[int],
    reset_flag: bool = False,
    error_fn: str = "CFL_NULL",
    error_data: dict = None
)
```

Verify specified KBs are running.

---

## Structural Rules

### Balancing Rules

1. Every `define_column` must have matching `end_column`
2. Every `define_state_machine` must have matching `end_state_machine`
3. Every `define_exception_catch` must have matching `exception_catch_end`
4. Every `define_sequence_til_*` must have matching `end_sequence_node`

### Column Termination

Every column's sequence must end with one of:
- `asm_halt()` - suspend and resume
- `asm_reset()` - restart column
- `asm_terminate()` - exit column

### For/While Constraints

`define_for_column` and `define_while_column` can only have **one child link**.

### State Machine Constraints

All states declared in `state_names` must be defined with `define_state` before `end_state_machine`.

### Exception Catch Constraints

Before calling `exception_catch_end`, you must define:
1. Main column (`define_main_exception_column` / `end_main_exception_column`)
2. Recovery column (`define_recovery_column` / `end_recovery_column`)
3. Finalize column (`define_finalize_column` / `end_finalize_column`)

Recovery column must have `max_steps + 2` links (step columns + overhead).

### Traversal Order

Sibling columns execute in definition order. Use this for priority:

```python
# High priority - executes first
emergency = ct.define_data_flow_event_mask("emergency", ["ESTOP"])
# ... 
ct.end_column(emergency)

# Lower priority - executes after emergency
normal = ct.define_data_flow_event_mask("normal", ["RUN"])
# ...
ct.end_column(normal)
```

---

## Pattern Catalog

### Pattern 1: Basic Sequential Control

```python
def basic_sequence(ct):
    """
    Execute steps in order, then loop.
    Use for: Simple periodic tasks, initialization sequences.
    """
    ct.start_test("basic_sequence")
    
    main = ct.define_column("main", auto_start=True)
    ct.asm_one_shot_handler("STEP_1", {"param": 1})
    ct.asm_one_shot_handler("STEP_2", {"param": 2})
    ct.asm_wait_time(time_delay=1.0)
    ct.asm_reset()  # Loop back to STEP_1
    ct.end_column(main)
    
    ct.end_test()
```

### Pattern 2: Event-Gated Action

```python
def event_gated(ct):
    """
    Respond to bitmask event with action.
    Use for: Interrupt-style handlers, mode switches.
    """
    ct.start_test("event_gated")
    
    handler = ct.define_data_flow_event_mask(
        "handler", 
        event_list=["TRIGGER_BIT"], 
        auto_start=True
    )
    ct.asm_one_shot_handler("HANDLE_TRIGGER", {})
    ct.asm_clear_bitmask(["TRIGGER_BIT"])
    ct.asm_halt()
    ct.end_column(handler)
    
    ct.end_test()
```

### Pattern 3: Centralized State Arbiter

```python
def state_arbiter(ct):
    """
    Centralized state decoder at head of control tree.
    Use for: Multi-mode actuators, priority arbitration.
    
    Key insight: Tree traversal order guarantees arbiter
    runs before mode handlers each tick.
    """
    ct.start_test("state_arbiter")
    
    motor = ct.define_column("motor", auto_start=True)
    
    # Arbiter runs first - computes derived state
    arbiter = ct.define_column("arbiter", auto_start=True)
    ct.asm_one_shot_handler("COMPUTE_STATE", {})
    ct.asm_reset()
    ct.end_column(arbiter)
    
    # Mode handlers see fresh state
    emergency = ct.define_data_flow_event_mask("emergency", ["ESTOP"])
    ct.asm_one_shot_handler("EMERGENCY_STOP", {})
    ct.asm_halt()
    ct.end_column(emergency)
    
    normal = ct.define_data_flow_event_mask("normal", ["RUN"])
    ct.asm_one_shot_handler("NORMAL_OPERATION", {})
    ct.asm_halt()
    ct.end_column(normal)
    
    ct.asm_halt()
    ct.end_column(motor)
    
    ct.end_test()
```

### Pattern 4: State Machine

```python
def state_machine_example(ct):
    """
    Explicit state machine with transitions.
    Use for: Protocol handlers, mode sequencing.
    """
    ct.start_test("state_machine")
    
    launch = ct.define_column("launch", auto_start=True)
    
    sm = ct.define_state_machine(
        column_name="protocol_sm",
        sm_name="protocol",
        state_names=["IDLE", "CONNECTING", "CONNECTED"],
        initial_state="IDLE",
        auto_start=True
    )
    
    idle = ct.define_state("IDLE")
    ct.asm_log_message("Waiting for connect command")
    ct.asm_wait_for_bitmask(["CONNECT_CMD"])
    ct.change_state(sm, "CONNECTING")
    ct.asm_halt()
    ct.end_column(idle)
    
    connecting = ct.define_state("CONNECTING")
    ct.asm_one_shot_handler("START_CONNECTION", {})
    ct.asm_verify("CHECK_CONNECTED", error_fn="CONNECTION_FAILED")
    ct.change_state(sm, "CONNECTED")
    ct.asm_halt()
    ct.end_column(connecting)
    
    connected = ct.define_state("CONNECTED")
    ct.asm_one_shot_handler("HANDLE_DATA", {})
    ct.asm_verify_bitmask(["DISCONNECT_CMD"])
    ct.change_state(sm, "IDLE")
    ct.asm_halt()
    ct.end_column(connected)
    
    ct.end_state_machine(sm, "protocol")
    
    ct.asm_halt()
    ct.end_column(launch)
    
    ct.end_test()
```

### Pattern 5: Fork-Join Parallelism

```python
def fork_join(ct):
    """
    Parallel execution with synchronization.
    Use for: Concurrent initialization, parallel sensors.
    """
    ct.start_test("fork_join")
    
    main = ct.define_column("main", auto_start=True)
    
    fork = ct.define_fork_column("parallel_init")
    
    init_a = ct.define_column("init_a")
    ct.asm_one_shot_handler("INIT_SENSOR_A", {})
    ct.asm_terminate()
    ct.end_column(init_a)
    
    init_b = ct.define_column("init_b")
    ct.asm_one_shot_handler("INIT_SENSOR_B", {})
    ct.asm_terminate()
    ct.end_column(init_b)
    
    ct.end_column(fork)
    
    # Wait for both to complete
    ct.define_join_link(fork)
    
    ct.asm_log_message("All sensors initialized")
    ct.asm_halt()
    ct.end_column(main)
    
    ct.end_test()
```

### Pattern 6: Supervisor with Restart

```python
def supervisor_example(ct):
    """
    Erlang-style supervision with restart policies.
    Use for: Fault tolerance, self-healing systems.
    """
    ct.start_test("supervisor")
    
    top = ct.define_column("top", auto_start=True)
    
    sup = ct.define_supervisor_one_for_one_node(
        column_name="worker_supervisor",
        reset_limited_enabled=True,
        max_reset_number=3,
        reset_window=60,  # 3 failures in 60 seconds = give up
        finalize_function="SUPERVISOR_FAILED",
        auto_start=True
    )
    
    worker1 = ct.define_column("worker1", auto_start=True)
    ct.asm_one_shot_handler("DO_WORK", {})
    ct.asm_verify("CHECK_HEALTH", error_fn="WORKER_UNHEALTHY")
    ct.define_mark_supervisor_node_failure({"worker": "worker1"})
    ct.asm_terminate()
    ct.end_column(worker1)
    
    worker2 = ct.define_column("worker2", auto_start=True)
    ct.asm_one_shot_handler("DO_OTHER_WORK", {})
    ct.asm_wait_time(5.0)
    ct.asm_reset()
    ct.end_column(worker2)
    
    ct.end_column(sup)
    
    ct.define_join_link(sup)
    ct.asm_log_message("Supervisor stopped")
    ct.asm_terminate()
    ct.end_column(top)
    
    ct.end_test()
```

### Pattern 7: Exception Handling with Recovery

```python
def exception_handling(ct):
    """
    Structured exception handling with step-based recovery.
    Use for: Complex operations needing rollback.
    """
    ct.start_test("exception_handling")
    
    launch = ct.define_column("launch", auto_start=True)
    
    catch_all = ct.catch_all_exception("catch_all", "LOG_UNHANDLED")
    
    exc = ct.define_exception_catch(
        "operation",
        aux_function_name="EXCEPTION_FILTER",
        aux_function_data={},
        logging_function_name="LOG_EXCEPTION"
    )
    
    # Main execution
    main = ct.define_main_exception_column("main")
    ct.asm_set_exception_step(step=0)
    ct.asm_one_shot_handler("STEP_0_ACQUIRE", {})
    ct.asm_set_exception_step(step=1)
    ct.asm_one_shot_handler("STEP_1_PROCESS", {})
    ct.asm_set_exception_step(step=2)
    ct.asm_one_shot_handler("STEP_2_COMMIT", {})
    ct.asm_terminate()
    ct.end_main_exception_column(main)
    
    # Recovery (reverse order)
    recovery = ct.define_recovery_column("recovery", max_steps=3, 
                                         skip_condition_function="SKIP_RECOVERY")
    
    step2_recovery = ct.define_column("step2_recovery", auto_start=True)
    ct.asm_one_shot_handler("UNDO_COMMIT", {})
    ct.asm_terminate()
    ct.end_column(step2_recovery)
    
    step1_recovery = ct.define_column("step1_recovery", auto_start=True)
    ct.asm_one_shot_handler("UNDO_PROCESS", {})
    ct.asm_terminate()
    ct.end_column(step1_recovery)
    
    step0_recovery = ct.define_column("step0_recovery", auto_start=True)
    ct.asm_one_shot_handler("UNDO_ACQUIRE", {})
    ct.asm_terminate()
    ct.end_column(step0_recovery)
    
    ct.asm_terminate()
    ct.end_recovery_column(recovery)
    
    # Finalize (always runs)
    finalize = ct.define_finalize_column("finalize")
    ct.asm_one_shot_handler("CLEANUP", {})
    ct.asm_terminate()
    ct.end_finalize_column(finalize)
    
    ct.exception_catch_end(exc)
    ct.define_join_link(exc)
    
    ct.end_column(catch_all)
    ct.define_join_link(catch_all)
    
    ct.asm_terminate()
    ct.end_column(launch)
    
    ct.end_test()
```

### Pattern 8: Motor Controller (Complete Example)

```python
def motor_controller(ct):
    """
    Complete motor controller with:
    - Centralized state arbiter
    - Event-masked mode handlers
    - Current-based stall detection
    - Emergency stop
    - Calibration sequence
    """
    ct.start_test("motor_controller")
    
    launch = ct.define_column("launch", auto_start=True)
    
    # Initialization
    init = ct.define_column("init", auto_start=True)
    ct.asm_one_shot_handler("CHIP_INIT", {})
    ct.asm_terminate()
    ct.end_column(init)
    ct.define_join_link(init)
    
    # Motor control subtree
    motor = ct.define_column("motor", auto_start=True)
    
    # State arbiter - runs first each tick
    arbiter = ct.define_column("arbiter", auto_start=True)
    ct.asm_one_shot_handler("COMPUTE_MOTOR_STATE", {})
    ct.asm_reset()
    ct.end_column(arbiter)
    
    # Emergency stop - highest priority
    estop = ct.define_data_flow_event_mask("estop", ["EMERGENCY_STOP_BIT"])
    ct.asm_one_shot_handler("SET_PWM", {"pwm": 0})
    ct.asm_one_shot_handler("SET_DIRECTION", {"direction": "STOP"})
    ct.asm_one_shot_handler("SEND_ESTOP_MSG", {})
    ct.asm_wait_for_bitmask(["EMERGENCY_STOP_BIT"], reset_flag=False, timeout=5000)
    ct.asm_halt()
    ct.end_column(estop)
    
    # Calibration
    calibrate = ct.define_data_flow_event_mask("calibrate", ["CALIBRATION_BIT"])
    ct.asm_one_shot_handler("SEND_CAL_MSG", {})
    ct.asm_one_shot_handler("SET_PWM", {"pwm": 200})
    ct.asm_one_shot_handler("SET_DIRECTION", {"direction": "REVERSE"})
    ct.asm_verify("STALL_DETECTED", error_fn="CAL_TIMEOUT")
    ct.asm_one_shot_handler("SET_PWM", {"pwm": 0})
    ct.asm_one_shot_handler("LOG_POSITION", {})
    ct.asm_one_shot_handler("SET_DIRECTION", {"direction": "FORWARD"})
    ct.asm_one_shot_handler("SET_PWM", {"pwm": 200})
    ct.asm_verify("STALL_DETECTED", error_fn="CAL_TIMEOUT")
    ct.asm_one_shot_handler("SET_PWM", {"pwm": 0})
    ct.asm_one_shot_handler("SET_CAL_SUCCESS", {"success": True})
    ct.asm_clear_bitmask(["CALIBRATION_BIT"])
    ct.asm_halt()
    ct.end_column(calibrate)
    
    # Open door
    open_door = ct.define_data_flow_event_mask("open", ["OPEN_DOOR_BIT"])
    ct.asm_one_shot_handler("SET_DIRECTION", {"direction": "REVERSE"})
    ct.asm_one_shot_handler("SET_PWM", {"pwm": 200})
    ct.asm_verify("STALL_DETECTED", error_fn="OPEN_TIMEOUT")
    ct.asm_one_shot_handler("SET_PWM", {"pwm": 0})
    ct.asm_clear_bitmask(["OPEN_DOOR_BIT"])
    ct.asm_halt()
    ct.end_column(open_door)
    
    # Close door
    close_door = ct.define_data_flow_event_mask("close", ["CLOSE_DOOR_BIT"])
    ct.asm_one_shot_handler("SET_DIRECTION", {"direction": "FORWARD"})
    ct.asm_one_shot_handler("SET_PWM", {"pwm": 200})
    ct.asm_verify("STALL_DETECTED", error_fn="CLOSE_TIMEOUT")
    ct.asm_one_shot_handler("SET_PWM", {"pwm": 0})
    ct.asm_clear_bitmask(["CLOSE_DOOR_BIT"])
    ct.asm_halt()
    ct.end_column(close_door)
    
    ct.asm_halt()
    ct.end_column(motor)
    
    # Parallel monitors
    current_monitor = ct.define_column("current_monitor", auto_start=True)
    ct.asm_wait_time(0.01)
    ct.define_column_link(
        main_function_name="CURRENT_MONITOR_MAIN",
        initialization_function_name="CURRENT_MONITOR_INIT",
        aux_function_name="CFL_NULL",
        termination_function_name="CFL_NULL",
        node_data={"moving_average": 4, "threshold": 3.0}
    )
    ct.asm_halt()
    ct.end_column(current_monitor)
    
    watchdog = ct.define_column("watchdog", auto_start=True)
    ct.asm_one_shot_handler("CHECK_TEMP", {"max": 150})
    ct.asm_one_shot_handler("CHECK_VOLTAGE", {"min": 6.0, "max": 18.0})
    ct.asm_wait_time(0.1)
    ct.asm_reset()
    ct.end_column(watchdog)
    
    ct.asm_halt()
    ct.end_column(launch)
    
    ct.end_test()
```

---

## Built-in Functions

### Main Functions (CFL_*)

| Function | Purpose |
|----------|---------|
| `CFL_COLUMN_MAIN` | Default column main |
| `CFL_HALT` | Halt execution |
| `CFL_RESET` | Reset column |
| `CFL_TERMINATE` | Terminate column |
| `CFL_DISABLE` | Disable node |
| `CFL_NULL` | No-op |
| `CFL_WAIT` | Wait with condition |
| `CFL_WAIT_TIME` | Wait for duration |
| `CFL_VERIFY` | Verify condition |
| `CFL_STATE_MACHINE_MAIN` | State machine execution |
| `CFL_SUPERVISOR_MAIN` | Supervisor execution |
| `CFL_FORK_MAIN` | Fork execution |
| `CFL_FOR_MAIN` | For loop |
| `CFL_WHILE_MAIN` | While loop |
| `CFL_DF_MASK_MAIN` | Data flow mask |
| `CFL_JOIN_MAIN` | Join synchronization |

### System Events

| Event | Purpose |
|-------|---------|
| `CFL_INIT_EVENT` | System initialization |
| `CFL_TERMINATE_EVENT` | System termination |
| `CFL_TIMER_EVENT` | Tick timer |
| `CFL_SECOND_EVENT` | 1-second tick |
| `CFL_MINUTE_EVENT` | 1-minute tick |
| `CFL_RAISE_EXCEPTION_EVENT` | Exception raised |
| `CFL_HEARTBEAT_EVENT` | Heartbeat pulse |
| `CFL_CHANGE_STATE_EVENT` | State transition |

---

## Target Platform Notes

- **Memory**: Designed for 8KB+ RAM (ARM Cortex-M0+)
- **Flash**: < 60KB for runtime engine
- **Bitmask limit**: 32 events maximum (uint32_t)
- **String table**: Events stored once, referenced by index
- **Arena allocator**: Optional local arenas for dynamic allocation
- **YAML output**: Generates configuration for C runtime linker

---

## Formal Specification

### Node Naming: ltree Discipline

Every node has a globally unique, human-readable path in PostgreSQL ltree notation:

```
kb.motor_controller.launch.motor.emergency_stop
kb.motor_controller.launch.motor.calibration.find_limit
kb.irrigation.pump_control.sequence.verify_running
```

**Advantages:**
- Dot-separated labels are inherently hierarchical and readable
- Full path is the primary key - no separate ID lookup required
- Multiple independent programs coexist under different root labels
- Standard ltree operators can be used for matching and queries

**Internal representation:**
| Language | Tree representation |
|----------|---------------------|
| Python | Flat dict with ltree keys |
| C | `const Node nodes[]` + parent/child index tables |
| Go | `map[string]*Node` |

### Node Structure

Each node contains:

```yaml
ltree_name:        kb.motor.launch.calibration
label:             calibration                    # last segment
parent_ltree:      kb.motor.launch
children:          []                             # list of child ltree names (links)
label_dict:
  main_function_name:           "CFL_COLUMN_MAIN"
  initialization_function_name: "CFL_COLUMN_INIT"
  aux_function_name:            "CFL_NULL"
  termination_function_name:    "CFL_COLUMN_TERM"
  links:                        []                # child node references
  array_index:                  42                # position in flat array (for C)
node_dict:
  auto_start:      true
  column_data:     { sensor_id: 7, threshold: 2.0 }
```

### The Four Virtual Functions

Every node implements up to four virtual methods:

| Virtual | When Called | Typical Use |
|---------|-------------|-------------|
| `initialization_function` | First event after Disabled → Enabled | Allocate resources, arm one-shot |
| `main_function` | Every tick while Enabled and Initialized | Core logic; returns opcode |
| `aux_function` | Varies by node type | Boolean condition (verify, while, etc.) |
| `termination_function` | When node or ancestor terminates | Release resources, cleanup |

### Opcodes (Return Values)

| Opcode | Symbol | Effect |
|--------|--------|--------|
| `CFL_CONTINUE` | 1 | Normal propagation to next sibling/children |
| `CFL_HALT` | 2 | Stop propagation for this tick only |
| `CFL_DISABLE` | 3 | Disable this node permanently |
| `CFL_RESET` | 4 | Halt and restart entire column |
| `CFL_TERMINATE` | 5 | Terminate this node and signal parent |
| `CFL_TERMINATE_SYSTEM` | 6 | Immediate global shutdown |

### Node Lifecycle States

Every node is in exactly one of three states:

```
┌──────────┐    enable     ┌─────────────────────┐   first tick   ┌─────────────────────────┐
│ Disabled │ ────────────► │ Enabled+Uninitialized│ ─────────────► │ Enabled+Initialized     │
└──────────┘               └─────────────────────┘                 └─────────────────────────┘
     ▲                              │                                        │
     │                              │ terminate                              │ terminate
     │                              ▼                                        ▼
     └───────────────────────────────────────────────────────────────────────┘
                                (call termination_function)
```

**Termination semantics:**
1. If composite: recursively mark entire subtree for termination
2. Topologically sort marked nodes (leaf-first)
3. Execute `termination_function` on each in reverse order
4. Reset Initialized = false (node may be re-entered later)

### Chain Flow Lineage: Decorator Patterns

ChainTree inherits proven patterns from Chain Flow (2005-2024):

| Pattern | Chain Flow Origin | ChainTree Implementation |
|---------|-------------------|--------------------------|
| `wait_node(condition)` | Block until condition true, then detach | `asm_wait_*` methods |
| `verify_node(condition, on_fail)` | Monitor condition, TERMINATE on failure | `asm_verify_*` methods |
| `action_node()` | Execute irreversible step | `asm_one_shot_handler` |
| `one_shot(node)` | Execute only on first event | Built into one_shot semantics |
| `retry(max, node)` | Re-enable on failure up to N times | Supervisor patterns |

**Classic Chain Flow sequence (firmware upgrade):**
```
wait_node(image_authenticated) →
verify_node(auth_still_valid, TERMINATE) →
action_node(erase_flash) →
wait_node(erase_complete) →
verify_node(erase_successful, RESET) →
action_node(program_sector) →
verify_node(crc_ok, TERMINATE)
```

**ChainTree equivalent:**
```python
ct.asm_wait_for_bitmask(["IMAGE_AUTHENTICATED"])
ct.asm_verify("AUTH_VALID", error_fn="AUTH_FAILED")
ct.asm_one_shot_handler("ERASE_FLASH", {})
ct.asm_wait_for_bitmask(["ERASE_COMPLETE"])
ct.asm_verify("ERASE_OK", reset_flag=True, error_fn="ERASE_FAILED")
ct.asm_one_shot_handler("PROGRAM_SECTOR", {})
ct.asm_verify("CRC_OK", error_fn="CRC_FAILED")
```

### Built-in Higher-Level Nodes

The engine provides composite node types:

| Node Type | Semantics |
|-----------|-----------|
| `column` | Sequential execution of children |
| `fork` | Parallel execution of all children |
| `for` | Repeat single child N times |
| `while` | Repeat single child while condition true |
| `state_machine` | Exclusive state activation based on transitions |
| `supervisor` | Erlang-style restart on child failure |
| `data_flow_event_mask` | Activate only when bitmask matches |
| `sequence_til_pass` | Try children until one succeeds |
| `sequence_til_fail` | Try children until one fails |
| `exception_catch` | Main/recovery/finalize structure |
| `local_arena` | Subtree with dedicated memory arena |

### Why the Complexity Is Encapsulated

The data structures and lifecycle rules are fully encapsulated inside the ChainTree engine. Application developers interact only through:

1. **The DSL** - high-level, readable Python
2. **Leaf action functions** - user-provided C functions for hardware I/O
3. **Occasional custom decorators** - extend with domain-specific patterns

The result delivers the full expressive power of Plexil and Behavior Trees (deep hierarchy, invariant checking, dynamic fallback, parallel execution) while remaining implementable on Cortex-M0+ class devices.

---

## Version

ChainTree DSL v1.0 - Low RAM Edition

**Lineage:** Chain Flow (2005-2024) → ChainTree (2024+)