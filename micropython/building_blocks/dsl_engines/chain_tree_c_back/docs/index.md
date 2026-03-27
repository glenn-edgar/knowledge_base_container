# ChainTree Documentation

ChainTree is a control flow framework that unifies behavior trees, state machines, and sequential control flows into a single C execution engine. It targets platforms from 32KB ARM Cortex-M microcontrollers to 8GB+ servers.

The framework consists of two engines:

- **ChainTree** — the outer control flow engine. Nodes are functions that manage their children through enable/disable patterns. A tree walker visits enabled nodes in DFS order, calling each node's main function. The main function decides how children execute — sequential, parallel, state-driven, or any custom pattern.

- **S-Expression Engine** — an inner tick-driven interpreter. Compiles Lua DSL definitions into flat parameter arrays (ROM) and evaluates them via a tick loop. Runs standalone or embedded inside ChainTree via bridge nodes.

Both engines share a two-stage code generation pipeline: Lua DSL → JSON IR → binary image (or C headers).

---

## Documentation Map

### 1. Architecture

Understand the design before reading anything else.

| Document | Description |
|----------|-------------|
| [ChainTree Design](README_chaintree_design.md) | How ChainTree evolves from behavior trees. Parent-controlled sequencing, the column pattern, node lifecycle, tree walker, memory model. **Start here.** |
| [S-Expression Top Design](s_expression/README_top_design.md) | S-Expression engine architecture overview |
| [S-Expression Outer Engine](s_expression/README_outer_engine_design.md) | Module/tree/instance lifecycle |
| [S-Expression Inner Engine](s_expression/README_inner_engine_design.md) | Tick evaluation, parameter stream, function dispatch |
| [CFL Bridge](README_cfl_bridge.md) | How ChainTree and S-Expression engine connect — node types, return code mapping, event flow, bridge function tables |

### 2. Getting Started

Build the system and run tests.

| Document | Description |
|----------|-------------|
| [Build Pipeline](README_build_pipeline.md) | All build commands — Lua DSL to JSON, JSON to binary image, S-Engine compilation, complete examples |
| [Makefile Patterns](README_makefile_patterns.md) | Three Makefile variants, library build order, source discovery |
| [Cross-Compilation](README_cross_compilation.md) | ARM Cortex-M setup, 32/64-bit modes, memory sizing, minimal configurations |

### 3. ChainTree DSL

Write ChainTree control flow programs.

| Document | Description |
|----------|-------------|
| [DSL Reference](README_chaintree_dsl.md) | Complete API — composites (column, fork, state machine, supervisor), leaf nodes (log, wait, verify, events), streaming, s-engine integration |
| [File Structure & Tutorial](README_chaintree_file_structure.md) | Module architecture, DSL file template, creating a new test step-by-step, function naming conventions, nesting rules |
| [JSON IR Schema](chaintree/README_dsl_schema.md) | JSON intermediate representation consumed by all backends |
| [Binary Image Design](chaintree/README_binary_image_design.md) | CTB1 binary format — sections, CRC32, FNV-1a function hashing |

### 4. S-Expression Engine DSL

Write tick-driven behavior trees and state machines.

| Document | Description |
|----------|-------------|
| [DSL Quick Reference](README_s_expression_dsl.md) | Concise reference — module structure, composites, dispatchers, timing, predicates, return codes, CFL bridge helpers |
| [File Structure & Tutorial](README_s_expression_file_structure.md) | Compiler architecture, module file template, creating a new module, standalone vs ChainTree usage |
| [DSL Full Specification](s_expression/README_DSL.md) | Complete 5.3 specification — type system, hash functions, all APIs |
| [Return Codes](s_expression/README_return_codes.md) | Three-tier result system — application, function, pipeline scopes |
| [Predicate System](s_expression/README_predicate_system.md) | Composable boolean predicates — OR/AND/NOT/XOR, field comparisons, event checks |
| [DSL Closure Guide](s_expression/README_DSL_closure_guide.md) | Lua closure patterns for DSL definitions |

#### 4a. S-Expression Composite Functions

| Document | Description |
|----------|-------------|
| [Overview](s_expression/composite_functions/README_composite_functions.md) | All composite types |
| [Sequence](s_expression/composite_functions/README_sequence.md) | Sequential execution |
| [Chain Flow](s_expression/composite_functions/README_chain_flow.md) | Pipeline processing |
| [Fork / Fork Join](s_expression/composite_functions/README_fork.md) | Parallel execution |
| [State Machine](s_expression/composite_functions/README_state_machine.md) | Field-based state dispatch |
| [Field Dispatch](s_expression/composite_functions/README_field_dispatch.md) | Field-based case dispatch |
| [Event Dispatch](s_expression/composite_functions/README_event_dispatch.md) | Event-based dispatch |
| [Trigger on Change](s_expression/composite_functions/README_trigger_on_change.md) | Edge-triggered actions |
| [While](s_expression/composite_functions/README_while.md) | Loop with predicate |
| [Cond](s_expression/composite_functions/README_cond.md) | Conditional branching |
| [Engine Interface](s_expression/composite_functions/README_engine_interface.md) | Tree entry point |
| [External Tree](s_expression/composite_functions/README_external_tree.md) | Cross-tree references |
| [Function Dictionary](s_expression/composite_functions/README_function_dictionary_handling.md) | Dynamic function lookup |

#### 4b. S-Expression Simple Functions

| Document | Description |
|----------|-------------|
| [Tick/Time Delay](s_expression/simple_functions/main_functions/README_tick_time_delay.md) | Hold for N ticks or seconds |
| [Wait](s_expression/simple_functions/main_functions/README_wait.md) | Wait with boolean condition |
| [Wait Event](s_expression/simple_functions/main_functions/README_wait_event.md) | Wait for specific event |
| [Verify](s_expression/simple_functions/main_functions/README_verify.md) | Assert with timeout |
| [Log](s_expression/simple_functions/one_shot_functions/README_se_log.md) | Timestamped logging |
| [Set Field](s_expression/simple_functions/one_shot_functions/README_set_field.md) | Blackboard field operations |
| [Queue Event](s_expression/simple_functions/one_shot_functions/README_queue_event.md) | Internal event posting |
| [Predicates](s_expression/simple_functions/predicate_functions/README.md) | Boolean predicate reference |

### 5. Runtime & Integration

C API for embedding ChainTree in applications.

| Document | Description |
|----------|-------------|
| [Runtime API](README_runtime_api.md) | Complete lifecycle — load, register, create, run, cleanup. Parameter sizing. Heap, arena, event, timer, JSON APIs |
| [User Functions](README_user_functions.md) | C function signatures for both engines — main/oneshot/boolean/predicate. Registration patterns. CFL bridge function table |
| [Blackboard](README_blackboard.md) | Shared mutable state — ChainTree blackboard, s-engine per-tree blackboard, constant records, cross-engine interaction |
| [CFL Bridge](README_cfl_bridge.md) | Bridge architecture, node types (se_engine, se_engine_link, se_tick), return code mapping, module registry, event flow |

### 6. Patterns & Tutorials

Common design patterns with working examples.

| Document | Description |
|----------|-------------|
| [Exception Handler](README_exception_handler.md) | Try/catch/recovery/finalize, heartbeat monitoring, nested handlers |
| [Controlled Nodes](README_controlled_nodes.md) | Client-controlled enable/disable with exception handling |
| [Streaming Pipeline](README_streaming_tutorial.md) | Tap → filter → transform → sink packet pipeline |
| [Car Window Controller](s_expression/car_window_controller/README_cardoor_requirements.md) | Real-world s-engine example — requirements and implementation |

### 7. Test Suites

Working integration tests that demonstrate all features.

| Document | Description |
|----------|-------------|
| [ChainTree Binary Tests](README_incremental_binary.md) | 29 tests — columns, forks, state machines, supervisors, exceptions, streaming, bitmask, controlled nodes, blackboard |
| [S-Engine Test 1](README_s_test_binary.md) | se_tick leaf node pipeline with state machine tree |
| [S-Engine Test 2](README_s_engine_test_2.md) | se_engine composite + se_engine_link — bitmask triggers, state machines, field/event dispatch, nested fields, pointers, JSON reads, constants |

#### 7a. S-Expression Standalone Tests

| Document | Description |
|----------|-------------|
| [Test Overview](s_expression/tests/README_test_overview.md) | All standalone s-engine tests |
| [Basic Primitives](s_expression/tests/README_basic_primitive_test.md) | Core function types |
| [Advanced Primitives](s_expression/tests/README_advanced_primitive_test.md) | Complex patterns |
| [Dispatch](s_expression/tests/README_dispatch.md) | Field and event dispatch |
| [State Machine](s_expression/tests/README_state_machine.md) | State machine patterns |
| [Blackboard](s_expression/tests/black_board/README_test_overview.md) | Blackboard operations |
| [Stack](s_expression/tests/README_stack_test.md) | Stack frame management |

### 8. Reference

Internal specifications and format documentation.

| Document | Description |
|----------|-------------|
| [Binary Runtime](runtime/README_runtime_binary.md) | `libcfl_binarycore.a` — binary image runtime |
| [Header Runtime](runtime/README_runtime_h.md) | `libcfl_core.a` — header-based runtime |
| [Avro Commands](avro/README_avro_commands.md) | Avro packet DSL commands |
| [Avro Packet Format](avro/README_c_avro_packtes.md) | Fixed-layout C message structs |
| [Debug Map](s_expression/README_reading_debug_map.md) | Reading s-engine debug output |
| [JSON Support](s_expression/README_json_support.md) | S-engine JSON loading |
| [Dictionary Handling](s_expression/README_runtime_dictionary_handling.md) | Runtime dictionary operations |
| [Stack Operations](s_expression/README_stack_operations.md) | Stack frame internals |
| [Stack Equations](s_expression/README_stack_equation_handler.md) | Expression compiler |

---

## Reading Order

**New to ChainTree:**
1. [ChainTree Design](README_chaintree_design.md) — understand the architecture
2. [Build Pipeline](README_build_pipeline.md) — build and run a test
3. [ChainTree DSL Reference](README_chaintree_dsl.md) — write your first test
4. [File Structure & Tutorial](README_chaintree_file_structure.md) — create a new test from scratch

**Adding S-Expression Engine:**
1. [S-Expression DSL Quick Reference](README_s_expression_dsl.md) — understand the inner engine
2. [CFL Bridge](README_cfl_bridge.md) — how the engines connect
3. [S-Engine Test 2](README_s_engine_test_2.md) — working integration examples

**Embedding in firmware:**
1. [Cross-Compilation](README_cross_compilation.md) — ARM setup and sizing
2. [Runtime API](README_runtime_api.md) — C lifecycle and memory configuration
3. [User Functions](README_user_functions.md) — implementing application logic
