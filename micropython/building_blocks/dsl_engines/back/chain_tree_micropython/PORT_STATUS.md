# ChainTree MicroPython Port Status

## Completed

### Core Runtime (ct_runtime.py)
- Result codes, event types, node state flags
- Node tuple format (ROM-resident, inline children)
- Module/instance lifecycle (new_module, new_instance, validate)
- Core invocation (invoke_main with init/term lifecycle, invoke_pred)
- Guard predicates (per-node boolean check)
- Child helpers (invoke, terminate, reset, recursive reset)
- Pre-allocated event queue (zero allocation per tick)
- Blackboard (dict-based, record init from module data)
- tick_once entry point

### Composite Builtins
- ct_sequence (with TERMINATE propagation)
- ct_selector
- ct_parallel (AND completion policy)
- ct_state_machine (persistent, field-driven dispatch)
- ct_for_loop
- ct_supervisor (stub — runs as parallel)

### Leaf Builtins
- ct_log, ct_set_field, ct_tick_delay, ct_nop
- ct_return_terminate, ct_terminate_system

### S-Engine Bridge
- ct_se_tick (loads S-Engine module, creates instance, ticks per CT tick)
- Result code mapping (SE -> CT)
- Event queue drain on S-Engine side
- Module config via user_ctx

### Tests Passing
1. basic_test — state machine 0->1->2->3->TERMINATE (14 ticks)
2. s_engine_bridge_test — CT sequence drives SE state_machine_test (363 ticks)

## Not Yet Ported (s_engine_test_2)

### twenty_ninth_test (bitmask data flow)
- NEEDS: Bitmask get/set/clear operations on ChainTree side
- NEEDS: se_engine composite (wraps module load + tree load + tick with CT children)
- NEEDS: Event logger function
- STATUS: Requires bitmask subsystem and se_engine composite

### thirty_test (state machine controlling child columns)
- NEEDS: se_engine composite with child enable/disable
- NEEDS: cfl_disable_children / cfl_enable_child bridge from S-Engine to CT
- STATUS: Requires se_engine composite (not just se_tick leaf)

### thirty_one_test (command + event dispatch)
- NEEDS: se_engine_link (simpler than se_engine — no children)
- NEEDS: S-Engine field_dispatch and event_dispatch trees
- NEEDS: User functions (test_31_set_motor, test_32_*)
- STATUS: Partially portable — needs se_engine_link and user function ports

### thirty_two_test (sequential link tests 10-16)
- test_10 (nested fields): NEEDS nested blackboard field access (motor.position.x)
- test_11 (pointer fields): NEEDS malloc/free -> Python object references
- test_12 (linked lists): NEEDS linked list via Python objects
- test_13 (pointer sharing): NEEDS Python object reference semantics
- test_14 (static buffer + JSON reads): NEEDS JSON node data decoder or equivalent
- test_15 (const record copy): NEEDS const record system
- test_16 (external pointer init): NEEDS external init callback
- STATUS: Tests 11-13 work naturally in Python (objects ARE references). Tests 10,14-16 need infrastructure.

## Not Yet Ported (incremental_binary)

### Core behavior (portable)
- Sequence, state machine, blackboard field access
- Timer/delay patterns
- Exception logging (basic)

### Streaming pipeline (NOT portable without new subsystem)
- Avro packet emit/sink/tap/filter/transform/collect
- Port matching (schema_hash + event_id)
- Packet container collection
- Wire header format

### Exception system (partially portable)
- Exception raise/catch/filter
- Recovery state tracking
- Supervisor with failure window

### Node control (NOT portable without new subsystem)
- Client/server controlled nodes
- Request/response port pairs
- Drone control patterns (fly_arc, fly_straight, etc.)

### Blackboard advanced features (partially portable)
- Nested field access (nav.heading, nav.altitude)
- Constant records with typed defaults
- 64-bit pointer field storage
- Dynamic hash lookup

## Feature Gap Summary

| Feature | C | MicroPython | Status |
|---------|---|-------------|--------|
| Sequence/selector/parallel | Yes | Yes | Done |
| State machine | Yes | Yes | Done |
| For loop | Yes | Yes | Done |
| S-Engine leaf (se_tick) | Yes | Yes | Done |
| S-Engine composite (se_engine) | Yes | No | Needs child enable/disable bridge |
| S-Engine link (se_engine_link) | Yes | No | Simpler variant of se_engine |
| Bitmask ops | Yes | No | Needs bitmask subsystem |
| Event logger | Yes | No | Simple to add |
| Streaming pipeline | Yes | No | Major new subsystem |
| Exception system | Yes | No | Moderate new subsystem |
| Node control (client/server) | Yes | No | Complex, needs avro |
| Nested blackboard fields | Yes | No | Needs nested dict access |
| Const records | Yes | No | Needs const record lookup |
| JSON node data decoder | Yes | No | Use Python tuples instead |
| Arena allocator | Yes | N/A | Python GC handles this |
| Binary image loader | Yes | N/A | Use tuple modules instead |
| Avro wire format | Yes | No | Use Python dicts instead |
