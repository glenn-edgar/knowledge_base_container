# scan_graph

## Stateful Boolean Dependency Graph Engine

**Design Document v0.1 — DRAFT**

Glenn Edgar — Onyx Engineering  
February 2026  
MIT License

---

## Table of Contents

- [1. Overview](#1-overview)
- [2. Core Concepts](#2-core-concepts)
- [3. DSL Specification](#3-dsl-specification)
- [4. SCADA Example: Water Treatment Plant](#4-scada-example-water-treatment-plant)
- [5. Evaluation Model](#5-evaluation-model)
- [6. Target Compilation](#6-target-compilation)
- [7. Industrial Context](#7-industrial-context)
- [8. Open Design Questions](#8-open-design-questions)

---

## 1. Overview

scan_graph is a stateful Boolean dependency graph engine designed for industrial control and SCADA applications. It provides a LuaJIT-based domain-specific language (DSL) for defining hierarchical evaluation graphs that compile to efficient native code across multiple hardware targets, from 32KB ARM Cortex-M microcontrollers to multi-gigabyte server environments.

The engine introduces a universal primitive called the quad — a stateful operator that evaluates logic, compares results against previous state, and only propagates when values change. This change-driven evaluation model minimizes computational work in large-scale systems with thousands of I/O points.

### 1.1 Design Goals

- Unified DSL that compiles to multiple targets (ARM Cortex-M, ESP32, Raspberry Pi, x86 server)
- Change-driven evaluation — operators only propagate when output differs from previous state
- Tri-state logic (true / false / unknown) with hierarchical dependency masking
- Clean separation of I/O binding, operator logic, and evaluation scheduling
- Composable operator templates built entirely from quad primitives
- Stack-based DSL in LuaJIT with structural validation at definition time
- Version-controllable, diffable, testable configuration artifacts

### 1.2 Architecture Overview

scan_graph is organized into four layers, each defined independently in the DSL:

| Layer | DSL Construct | Purpose |
|-------|---------------|---------|
| **1. Buffers** | `sg.define_buffer_container()` | External and internal I/O binding, transport configuration |
| **2. Operators** | `sg.define_operator_library()` | Reusable logic templates built from quad primitives |
| **3. Levels** | `sg.define_level_container()` | Evaluation stages with tri-state output buffers and dependency masking |
| **4. Graph** | `sg.define_scan_graph()` | Top-level container binding buffers, operators, and levels into an executable scan cycle |

### 1.3 Relationship to ChainTree

scan_graph is a subsystem within the ChainTree architecture. ChainTree provides behavior tree and sequential control flow; scan_graph provides the Boolean dependency graph evaluation engine. In a ChainTree application, a scan_graph level becomes a subtree node that executes during the behavior tree tick cycle. Internal buffers between levels map to the blackboard data passed between ChainTree sibling nodes.

---

## 2. Core Concepts

### 2.1 The Quad Primitive

The quad is the universal operator primitive in scan_graph. Every computation — Boolean logic, mathematical operations, comparisons, type conversions, timers — is expressed as a quad. A quad takes 0–N inputs, produces exactly one output, and handles type conversion at its boundaries.

For Boolean two-input operations, the quad is literally a 4-bit truth table indexed by `(a << 1 | b)`. For other operations (math, comparison, timer), the quad generalizes to a typed operator with the same structural interface.

Every quad is stateful: it stores its previous output value and only propagates (writes to destination) when the result changes. This is the fundamental mechanism that drives change-based evaluation through the graph.

| Quad Type | Inputs | Output | Description |
|-----------|--------|--------|-------------|
| `bool_quad` | 2 × bool | bool | 4-bit truth table lookup. AND=0x8, OR=0xE, XOR=0x6, NAND=0x7, NOR=0x1 |
| `unary_bool` | 1 × bool | bool | NOT=0x1, PASS=0x2 |
| `compare` | 1–3 × numeric | bool | gt, lt, gte, lte, range. Optional deadband parameter |
| `math_op` | 2 × numeric | numeric | add, sub, mul, div. Type conversion on input boundaries |
| `math_func` | 1 × numeric | numeric | sin, cos, sqrt, abs, log. Input auto-converted to float64 |
| `convert` | 1 × any | any | Explicit type conversion: bool_to_float64, float_to_bool, etc. |
| `timer` | 1 × bool | bool | on_delay, off_delay, debounce. Delay parameter in milliseconds |

#### 2.1.1 Quad Input Sources

A quad can receive inputs from three sources:

- **Buffer field reference** — reads from an external or internal buffer at a named field position
- **Another quad's output** — within the same expression or template only (expression-scoped sharing)
- **Constant value** — either a template parameter (configurable at instantiation) or an inline literal (fixed at definition)

#### 2.1.2 Quad Output Rule

A quad produces exactly one output. Within a template, that output can feed another quad in the same template. At the expression boundary, every terminal output must be wired to a buffer. Dangling outputs are caught at definition time.

#### 2.1.3 Quads as Quantum Quarks

Quads only exist inside operator template definitions. They cannot be instantiated directly at the level or graph layer. Like quarks in physics, quads are the fundamental constituents but are never observed in isolation — they are always bound within an operator template.

---

### 2.2 Tri-State Logic

Each level in scan_graph maintains an output buffer with three-state values: true, false, and unknown. The tri-state representation uses two bitmasks per buffer:

```
value_mask:   the actual true/false value
known_mask:   whether the value has been resolved

known=0           → UNKNOWN  (not yet evaluated or blocked)
known=1, value=0  → FALSE
known=1, value=1  → TRUE
```

Before each level evaluates, its output buffer is cleared to all UNKNOWN. As operators execute and produce results, they set their output bits to known true or known false. If an operator's inputs include any UNKNOWN values, the operator is blocked and its output remains UNKNOWN. This UNKNOWN state propagates upward through the dependency hierarchy.

#### 2.2.1 Hierarchical Bit Masks

For large buffers (hundreds of bits), checking individual dependency bits is expensive. scan_graph organizes output buffers into a hierarchy of groups with summary bits:

```
Layer 0:  Individual operator outputs (leaf bits)
Layer 1:  Group summaries — ANY_UNKNOWN in group of N bits
Layer 2:  Section summaries — ANY_UNKNOWN in group of groups
```

Dependency checking proceeds top-down through the hierarchy. If a group summary shows all-known, no individual bit checks are needed for that group. This provides O(1) average-case dependency checking through bitmask AND operations.

---

### 2.3 Change-Driven Evaluation

Every quad maintains its previous output value. After computing a new result, the quad compares it to the previous state:

- **If the result is the same** — the quad does not write to its destination and returns false (no change)
- **If the result is different** — the quad updates its previous state, writes the new value to its destination, and returns true (changed)

At the level evaluation layer, change detection determines which operators in subsequent levels need re-evaluation. Only operators whose input dependencies include a changed value are marked dirty and evaluated in the next scan.

---

## 3. DSL Specification

The scan_graph DSL is written in LuaJIT using a stack-based calling convention. Each construct uses matched begin/end calls that return and validate identifiers, providing structural validation at definition time without requiring a separate parser.

### 3.1 Buffer Definitions

Buffers define the I/O boundary of the scan graph. Each buffer is bound to a hardware transport type and contains named field definitions that provide symbolic access to indexed positions.

```lua
buffer_container = sg.define_buffer_container("pump_station_io")

  buffer_di = sg.buffer("digital_inputs", "mqtt",
    '{"topic":"plant/ps1/di_bank"}', 32, "bool")
    sg.field_def("pump_running",   0, 1)
    sg.field_def("valve_open",     1, 1)
    sg.field_def("emergency_stop", 7, 1)
    sg.field_def("overload_trip",  8, 1)
  sg.end_buffer(buffer_di)

  buffer_ai = sg.buffer("analog_inputs", "nats-jetstream",
    '{"subject":"plant.ps1.ai_bank"}', 16, "float32")
    sg.field_def("discharge_pressure", 0, 1)
    sg.field_def("motor_current",      1, 1)
    sg.field_def("motor_temp",         2, 1)
  sg.end_buffer(buffer_ai)

  buffer_l0 = sg.buffer("level_0", "binary", "", 8, "bool")
    sg.field_def("pressure_high", 0, 1)
    sg.field_def("flow_ok",       1, 1)
    sg.field_def("motor_healthy", 2, 1)
  sg.end_buffer(buffer_l0)

sg.end_buffer_container(buffer_container)
```

#### 3.1.1 Hardware Types

| Type | Key/Topic Format | Description |
|------|-----------------|-------------|
| `mqtt` | `{"topic": "plant/bank"}` | MQTT pub/sub topic binding |
| `nats-jetstream` | `{"subject": "plant.bank"}` | NATS JetStream subject binding |
| `modbus` | `{"host":"...","port":502,"unit":1,"register":100}` | Modbus TCP register mapping |
| `postgres` | `{"table":"...","record_id":"...","ltree":"..."}` | PostgreSQL with ltree extension |
| `binary` | `""` (empty) | Internal buffer, resident on system |

#### 3.1.2 Data Types

| Type | Size | Range | Notes |
|------|------|-------|-------|
| `bool` | 1 bit | true / false | Packed into uint32 words |
| `uint8` | 1 byte | 0 – 255 | |
| `int16` | 2 bytes | -32768 – 32767 | Modbus register size |
| `uint16` | 2 bytes | 0 – 65535 | Modbus register size |
| `int32` | 4 bytes | ±2.1 billion | |
| `uint32` | 4 bytes | 0 – 4.2 billion | |
| `float32` | 4 bytes | IEEE 754 single | Sensor values |
| `float64` | 8 bytes | IEEE 754 double | Computation type |

#### 3.1.3 Validation Rules

- Buffer names must be unique within a container
- Field indices must not overlap within a buffer
- Field index + index_size must not exceed buffer size
- Every `sg.buffer()` must have a matching `sg.end_buffer()` with the same return code
- No buffer may be open when `sg.end_buffer_container()` is called

---

### 3.2 Operator Templates

Operator templates define reusable logic patterns built entirely from quad primitives. Templates declare typed inputs, outputs, and optional parameters, then compose quads to implement the logic. Templates are pure definitions with no buffer references or state — they become live nodes only when instantiated within a level.

```lua
op_lib = sg.define_operator_library("station_ops")

  -- Primitive: greater-than with deadband
  t_gt = sg.operator_template("greater_than")
    sg.template_input("value", "any_numeric")
    sg.template_output("result", "bool")
    sg.template_param("threshold", "float64")
    sg.template_param("deadband", "float64", 0.0)

    q1 = sg.quad("compare", "gt")
      sg.quad_in("value", "float64")     -- auto-convert input
      sg.quad_const("threshold")
      sg.quad_const("deadband")
      sg.quad_out("result", "bool")
    sg.end_quad(q1)

  sg.end_operator_template(t_gt)

  -- Composed: pressure alarm = threshold + delay
  t_palarm = sg.operator_template("pressure_alarm")
    sg.template_input("pressure", "float32")
    sg.template_output("alarm", "bool")
    sg.template_param("high_limit", "float64")
    sg.template_param("deadband", "float64", 2.0)
    sg.template_param("delay_ms", "uint32", 3000)

    q1 = sg.quad("compare", "gt")
      sg.quad_in("pressure", "float64")
      sg.quad_const("high_limit")
      sg.quad_const("deadband")
      sg.quad_out("raw_alarm", "bool")   -- internal
    sg.end_quad(q1)

    q2 = sg.quad("timer", "on_delay")
      sg.quad_in_from_quad(q1, "raw_alarm")
      sg.quad_const("delay_ms")
      sg.quad_out("alarm")                -- drives output
    sg.end_quad(q2)

  sg.end_operator_template(t_palarm)

sg.end_operator_library(op_lib)
```

#### 3.2.1 Quad Wiring Rules

- `sg.quad_in(name)` — pulls from a template input by name; optional second argument specifies conversion type
- `sg.quad_in_from_quad(quad_id, output_name)` — pulls from another quad's output within the same template
- `sg.quad_const(param_name)` — pulls from a template parameter (configurable at instantiation time)
- `sg.quad_const_value(literal)` — inline constant fixed at definition time
- `sg.quad_out(name)` — internal output consumable by other quads in the template, or drives a template output if name matches
- `sg.quad_out(name, type)` — same as above with explicit output type declaration

#### 3.2.2 Template Validation Rules

- Every template input must be consumed by at least one quad
- Every template output must be driven by exactly one quad
- Quad-to-quad wiring is scoped to the template — no cross-template quad references
- No circular dependencies between quads within a template
- Type mismatches that cannot be auto-converted are caught at definition time

---

### 3.3 Level Definitions

Levels are the evaluation stages of the scan graph. Each level maintains a tri-state output buffer and contains a set of instantiated operator templates. Levels execute in strict order: level 0 first, then level 1, and so on.

```lua
level_container = sg.define_level_container("pump_station")

  lv0 = sg.level(0, "input_conditioning")

    sg.level_output_buffer(16)
    sg.level_hierarchy({ group_size = 4 })

    sg.level_field("pump_running",   0)
    sg.level_field("valve_open",     1)
    sg.level_field("pressure_high",  2)
    sg.level_field("flow_ok",        3)

    op1 = sg.instantiate_operator("passthrough")
      sg.op_input("a", "digital_inputs", "pump_running")
      sg.op_output("result", 0)
    sg.end_instantiate_operator(op1)

    op2 = sg.instantiate_operator("greater_than")
      sg.op_input("value", "analog_inputs", "discharge_pressure")
      sg.op_param("threshold", 150.0)
      sg.op_param("deadband", 2.0)
      sg.op_output("result", 2)
    sg.end_instantiate_operator(op2)

    op3 = sg.instantiate_operator("and2")
      sg.op_input_from_level("a", 0)  -- pump_running
      sg.op_input_from_level("b", 1)  -- valve_open
      sg.op_output("result", 3)       -- flow_ok
      sg.op_depends({ 0, 1 })          -- blocked if unknown
    sg.end_instantiate_operator(op3)

  sg.end_level(lv0)

sg.end_level_container(level_container)
```

#### 3.3.1 Level Evaluation Sequence

1. Clear the level's output buffer to all UNKNOWN (known_mask = 0, value_mask = 0)
2. Iterate through operators in dependency order
3. For each operator, check its dependency mask against the known_mask hierarchy
4. If any dependency is UNKNOWN — skip the operator (output remains UNKNOWN)
5. If all dependencies are known — evaluate the operator and set the output bit to known true or known false
6. Update hierarchical group summaries after each write

#### 3.3.2 Operator Input Sources at Level Scope

- `sg.op_input(name, buffer, field)` — reads from an external or internal buffer
- `sg.op_input_from_level(name, bit)` — reads from this level's output buffer (requires dependency declaration)
- `sg.op_input_from_prev_level(level_num, name, bit)` — reads from a previous level's output buffer
- `sg.op_param(name, value)` — sets a template parameter value for this instance

---

## 4. SCADA Example: Water Treatment Plant

This example demonstrates a complete scan_graph configuration for a water treatment pump station with two pump/motor assemblies, discharge valves, and a common header. The system monitors motor health, valve status, flow, and pressure to determine station readiness and generate alarms.

### 4.1 System Description

The pump station has the following instrumentation:

- Two pump motors (M-101, M-102) each with: running status, overload trip, current transmitter, temperature transmitter
- Two discharge valves (XV-101, XV-102) each with: open limit switch, closed limit switch, fault indication
- Common header instrumentation: discharge pressure transmitter (PT-100), flow transmitter (FT-100)
- Alarm outputs via NATS JetStream to the SCADA historian and alarm management system

### 4.2 Buffer Definitions

```lua
local sg = require("scan_graph")

----------------------------------------------
-- Water Treatment Pump Station
-- Pump Station PS-100
----------------------------------------------

bc = sg.define_buffer_container("ps100_io")

  -- Motor 1 digital inputs (Modbus RTU via gateway)
  m101_di = sg.buffer("m101_digital", "modbus",
    '{"host":"10.1.1.10","port":502,"unit":1,"register":0}',
    8, "bool")
    sg.field_def("running",      0, 1)
    sg.field_def("overload",     1, 1)
    sg.field_def("local_mode",   2, 1)
    sg.field_def("e_stop",       3, 1)
  sg.end_buffer(m101_di)

  -- Motor 1 analog inputs (NATS from PLC gateway)
  m101_ai = sg.buffer("m101_analog", "nats-jetstream",
    '{"subject":"plant.ps100.m101.analog"}', 4, "float32")
    sg.field_def("current",      0, 1)
    sg.field_def("temperature",  1, 1)
    sg.field_def("vibration",    2, 1)
    sg.field_def("hours",        3, 1)
  sg.end_buffer(m101_ai)

  -- Motor 2 digital inputs
  m102_di = sg.buffer("m102_digital", "modbus",
    '{"host":"10.1.1.10","port":502,"unit":2,"register":0}',
    8, "bool")
    sg.field_def("running",      0, 1)
    sg.field_def("overload",     1, 1)
    sg.field_def("local_mode",   2, 1)
    sg.field_def("e_stop",       3, 1)
  sg.end_buffer(m102_di)

  -- Motor 2 analog inputs
  m102_ai = sg.buffer("m102_analog", "nats-jetstream",
    '{"subject":"plant.ps100.m102.analog"}', 4, "float32")
    sg.field_def("current",      0, 1)
    sg.field_def("temperature",  1, 1)
    sg.field_def("vibration",    2, 1)
    sg.field_def("hours",        3, 1)
  sg.end_buffer(m102_ai)

  -- Valve 1 digital inputs
  xv101_di = sg.buffer("xv101_digital", "modbus",
    '{"host":"10.1.1.10","port":502,"unit":3,"register":0}',
    4, "bool")
    sg.field_def("open_limit",   0, 1)
    sg.field_def("closed_limit", 1, 1)
    sg.field_def("fault",        2, 1)
  sg.end_buffer(xv101_di)

  -- Valve 2 digital inputs
  xv102_di = sg.buffer("xv102_digital", "modbus",
    '{"host":"10.1.1.10","port":502,"unit":4,"register":0}',
    4, "bool")
    sg.field_def("open_limit",   0, 1)
    sg.field_def("closed_limit", 1, 1)
    sg.field_def("fault",        2, 1)
  sg.end_buffer(xv102_di)

  -- Common header analog inputs
  header_ai = sg.buffer("header_analog", "nats-jetstream",
    '{"subject":"plant.ps100.header.analog"}', 4, "float32")
    sg.field_def("pressure",     0, 1)  -- PT-100
    sg.field_def("flow",         1, 1)  -- FT-100
    sg.field_def("ph",           2, 1)
    sg.field_def("turbidity",    3, 1)
  sg.end_buffer(header_ai)

  -- Alarm outputs
  alarm_out = sg.buffer("alarms", "nats-jetstream",
    '{"subject":"plant.ps100.alarms"}', 16, "bool")
    sg.field_def("m101_elec_fault",   0, 1)
    sg.field_def("m101_therm_fault",  1, 1)
    sg.field_def("m102_elec_fault",   2, 1)
    sg.field_def("m102_therm_fault",  3, 1)
    sg.field_def("high_pressure",     4, 1)
    sg.field_def("low_flow",          5, 1)
    sg.field_def("station_fault",     6, 1)
    sg.field_def("station_degraded",  7, 1)
  sg.end_buffer(alarm_out)

  -- Status output to SCADA historian
  status_out = sg.buffer("status", "postgres",
    '{"table":"station_status","record_id":"ps100","ltree":"plant.water.ps100"}',
    8, "bool")
    sg.field_def("station_running",   0, 1)
    sg.field_def("station_healthy",   1, 1)
    sg.field_def("station_degraded",  2, 1)
    sg.field_def("station_ready",     3, 1)
    sg.field_def("redundancy_ok",     4, 1)
  sg.end_buffer(status_out)

sg.end_buffer_container(bc)
```

### 4.3 Operator Templates

The following templates are defined for the pump station. Note how the motor_protection template composes multiple quads into a reusable block, and the pump_train template composes motor_protection with valve checking.

```lua
op_lib = sg.define_operator_library("water_treatment_ops")

  ------------------------------------------------
  -- Motor protection template
  -- Inputs: overload (bool), current (float32),
  --         temperature (float32)
  -- Outputs: is_healthy, electrical_fault,
  --          thermal_fault (all bool)
  ------------------------------------------------
  t_mp = sg.operator_template("motor_protection")
    sg.template_input("overload",    "bool")
    sg.template_input("current",     "float32")
    sg.template_input("temperature", "float32")
    sg.template_output("is_healthy",       "bool")
    sg.template_output("electrical_fault", "bool")
    sg.template_output("thermal_fault",    "bool")
    sg.template_param("current_limit",  "float64", 15.0)
    sg.template_param("current_db",     "float64", 0.5)
    sg.template_param("temp_limit",     "float64", 85.0)
    sg.template_param("temp_db",        "float64", 2.0)
    sg.template_param("elec_delay_ms",  "uint32", 2000)
    sg.template_param("therm_delay_ms", "uint32", 5000)

    -- Q1: current > limit
    q1 = sg.quad("compare", "gt")
      sg.quad_in("current", "float64")
      sg.quad_const("current_limit")
      sg.quad_const("current_db")
      sg.quad_out("over_current", "bool")
    sg.end_quad(q1)

    -- Q2: temp > limit
    q2 = sg.quad("compare", "gt")
      sg.quad_in("temperature", "float64")
      sg.quad_const("temp_limit")
      sg.quad_const("temp_db")
      sg.quad_out("over_temp", "bool")
    sg.end_quad(q2)

    -- Q3: elec_fault = overload OR over_current
    q3 = sg.quad("bool_quad", 0xE)
      sg.quad_in("overload")
      sg.quad_in_from_quad(q1, "over_current")
      sg.quad_out("elec_raw", "bool")
    sg.end_quad(q3)

    -- Q4: delay electrical fault
    q4 = sg.quad("timer", "on_delay")
      sg.quad_in_from_quad(q3, "elec_raw")
      sg.quad_const("elec_delay_ms")
      sg.quad_out("electrical_fault")
    sg.end_quad(q4)

    -- Q5: delay thermal fault
    q5 = sg.quad("timer", "on_delay")
      sg.quad_in_from_quad(q2, "over_temp")
      sg.quad_const("therm_delay_ms")
      sg.quad_out("thermal_fault")
    sg.end_quad(q5)

    -- Q6: NOT elec_fault
    q6 = sg.quad("unary_bool", 0x1)
      sg.quad_in_from_quad(q4, "electrical_fault")
      sg.quad_out("not_elec", "bool")
    sg.end_quad(q6)

    -- Q7: NOT therm_fault
    q7 = sg.quad("unary_bool", 0x1)
      sg.quad_in_from_quad(q5, "thermal_fault")
      sg.quad_out("not_therm", "bool")
    sg.end_quad(q7)

    -- Q8: healthy = NOT elec AND NOT therm
    q8 = sg.quad("bool_quad", 0x8)
      sg.quad_in_from_quad(q6, "not_elec")
      sg.quad_in_from_quad(q7, "not_therm")
      sg.quad_out("is_healthy")
    sg.end_quad(q8)

  sg.end_operator_template(t_mp)

  ------------------------------------------------
  -- Valve status template
  ------------------------------------------------
  t_vs = sg.operator_template("valve_status")
    sg.template_input("open_limit", "bool")
    sg.template_input("fault",      "bool")
    sg.template_output("is_open",    "bool")
    sg.template_output("is_healthy", "bool")

    q1 = sg.quad("unary_bool", 0x2)  -- passthrough
      sg.quad_in("open_limit")
      sg.quad_out("is_open")
    sg.end_quad(q1)

    q2 = sg.quad("unary_bool", 0x1)  -- NOT fault
      sg.quad_in("fault")
      sg.quad_out("is_healthy")
    sg.end_quad(q2)

  sg.end_operator_template(t_vs)

sg.end_operator_library(op_lib)
```

### 4.4 Level Definitions

The pump station uses three evaluation levels. Level 0 conditions raw inputs and evaluates subsystem health. Level 1 derives station-wide status from level 0 results. Level 2 assigns outputs to external alarm and status buffers.

```lua
lc = sg.define_level_container("ps100_levels")

  --============================================
  -- Level 0: Input conditioning & subsystem health
  -- 16 output bits, 4 groups of 4
  --============================================
  lv0 = sg.level(0, "subsystem_health")
    sg.level_output_buffer(16)
    sg.level_hierarchy({ group_size = 4 })

    -- Group 0: Motor 1 status (bits 0-3)
    sg.level_field("m101_running",     0)
    sg.level_field("m101_healthy",     1)
    sg.level_field("m101_elec_fault",  2)
    sg.level_field("m101_therm_fault", 3)

    -- Group 1: Motor 2 status (bits 4-7)
    sg.level_field("m102_running",     4)
    sg.level_field("m102_healthy",     5)
    sg.level_field("m102_elec_fault",  6)
    sg.level_field("m102_therm_fault", 7)

    -- Group 2: Valve status (bits 8-11)
    sg.level_field("xv101_open",       8)
    sg.level_field("xv101_healthy",    9)
    sg.level_field("xv102_open",      10)
    sg.level_field("xv102_healthy",   11)

    -- Group 3: Header conditions (bits 12-15)
    sg.level_field("pressure_high",   12)
    sg.level_field("pressure_low",    13)
    sg.level_field("flow_ok",         14)
    sg.level_field("e_stop_clear",    15)

    -- Motor 1 protection
    op1 = sg.instantiate_operator("passthrough")
      sg.op_input("a", "m101_digital", "running")
      sg.op_output("result", 0)
    sg.end_instantiate_operator(op1)

    op2 = sg.instantiate_operator("motor_protection")
      sg.op_input("overload",    "m101_digital", "overload")
      sg.op_input("current",     "m101_analog",  "current")
      sg.op_input("temperature", "m101_analog",  "temperature")
      sg.op_output("is_healthy",       1)
      sg.op_output("electrical_fault", 2)
      sg.op_output("thermal_fault",    3)
    sg.end_instantiate_operator(op2)

    -- Motor 2 protection
    op3 = sg.instantiate_operator("passthrough")
      sg.op_input("a", "m102_digital", "running")
      sg.op_output("result", 4)
    sg.end_instantiate_operator(op3)

    op4 = sg.instantiate_operator("motor_protection")
      sg.op_input("overload",    "m102_digital", "overload")
      sg.op_input("current",     "m102_analog",  "current")
      sg.op_input("temperature", "m102_analog",  "temperature")
      sg.op_output("is_healthy",       5)
      sg.op_output("electrical_fault", 6)
      sg.op_output("thermal_fault",    7)
    sg.end_instantiate_operator(op4)

    -- Valve 1 status
    op5 = sg.instantiate_operator("valve_status")
      sg.op_input("open_limit", "xv101_digital", "open_limit")
      sg.op_input("fault",      "xv101_digital", "fault")
      sg.op_output("is_open",    8)
      sg.op_output("is_healthy", 9)
    sg.end_instantiate_operator(op5)

    -- Valve 2 status
    op6 = sg.instantiate_operator("valve_status")
      sg.op_input("open_limit", "xv102_digital", "open_limit")
      sg.op_input("fault",      "xv102_digital", "fault")
      sg.op_output("is_open",   10)
      sg.op_output("is_healthy", 11)
    sg.end_instantiate_operator(op6)

    -- Header conditions
    op7 = sg.instantiate_operator("greater_than")
      sg.op_input("value", "header_analog", "pressure")
      sg.op_param("threshold", 80.0)  -- PSI
      sg.op_param("deadband", 2.0)
      sg.op_output("result", 12)      -- pressure_high
    sg.end_instantiate_operator(op7)

    op8 = sg.instantiate_operator("less_than")
      sg.op_input("value", "header_analog", "pressure")
      sg.op_param("threshold", 20.0)
      sg.op_param("deadband", 2.0)
      sg.op_output("result", 13)      -- pressure_low
    sg.end_instantiate_operator(op8)

    op9 = sg.instantiate_operator("greater_than")
      sg.op_input("value", "header_analog", "flow")
      sg.op_param("threshold", 50.0)  -- GPM
      sg.op_param("deadband", 5.0)
      sg.op_output("result", 14)      -- flow_ok
    sg.end_instantiate_operator(op9)

    op10 = sg.instantiate_operator("not")
      sg.op_input("a", "m101_digital", "e_stop")
      sg.op_output("result", 15)      -- e_stop_clear
    sg.end_instantiate_operator(op10)

  sg.end_level(lv0)

  --============================================
  -- Level 1: Station-wide logic
  -- Reads from level 0 tri-state output buffer
  -- 8 output bits, 2 groups of 4
  --============================================
  lv1 = sg.level(1, "station_logic")
    sg.level_output_buffer(8)
    sg.level_hierarchy({ group_size = 4 })

    -- Group 0: Running and health
    sg.level_field("any_pump_running", 0)
    sg.level_field("both_healthy",     1)
    sg.level_field("any_valve_open",   2)
    sg.level_field("station_running",  3)

    -- Group 1: Derived conditions
    sg.level_field("station_healthy",  4)
    sg.level_field("station_degraded", 5)
    sg.level_field("station_fault",    6)
    sg.level_field("station_ready",    7)

    -- any_pump_running = m101_running OR m102_running
    op1 = sg.instantiate_operator("or2")
      sg.op_input_from_prev_level(0, "a", 0)  -- m101_running
      sg.op_input_from_prev_level(0, "b", 4)  -- m102_running
      sg.op_output("result", 0)
    sg.end_instantiate_operator(op1)

    -- both_healthy = m101_healthy AND m102_healthy
    op2 = sg.instantiate_operator("and2")
      sg.op_input_from_prev_level(0, "a", 1)  -- m101_healthy
      sg.op_input_from_prev_level(0, "b", 5)  -- m102_healthy
      sg.op_output("result", 1)
    sg.end_instantiate_operator(op2)

    -- any_valve_open = xv101_open OR xv102_open
    op3 = sg.instantiate_operator("or2")
      sg.op_input_from_prev_level(0, "a", 8)   -- xv101_open
      sg.op_input_from_prev_level(0, "b", 10)  -- xv102_open
      sg.op_output("result", 2)
    sg.end_instantiate_operator(op3)

    -- station_running = any_pump AND any_valve AND flow_ok
    op4 = sg.instantiate_operator("and3")
      sg.op_input_from_level("a", 0)            -- any_pump_running
      sg.op_input_from_level("b", 2)            -- any_valve_open
      sg.op_input_from_prev_level(0, "c", 14)   -- flow_ok
      sg.op_output("result", 3)
      sg.op_depends({ 0, 2 })                   -- local deps
    sg.end_instantiate_operator(op4)

    -- station_healthy = station_running AND both_healthy
    --                    AND NOT pressure_high AND e_stop_clear
    op5 = sg.instantiate_operator("and4")
      sg.op_input_from_level("a", 3)            -- station_running
      sg.op_input_from_level("b", 1)            -- both_healthy
      sg.op_input_from_prev_level(0, "c", 12)   -- pressure_high (inverted)
      sg.op_input_from_prev_level(0, "d", 15)   -- e_stop_clear
      sg.op_output("result", 4)
      sg.op_depends({ 1, 3 })
    sg.end_instantiate_operator(op5)

    -- station_degraded = station_running AND NOT both_healthy
    op6 = sg.instantiate_operator("and_not")
      sg.op_input_from_level("a", 3)  -- station_running
      sg.op_input_from_level("b", 1)  -- both_healthy (inverted)
      sg.op_output("result", 5)
      sg.op_depends({ 1, 3 })
    sg.end_instantiate_operator(op6)

    -- station_fault = NOT station_running AND any_pump_running
    -- (pumps commanded but no flow confirmed)
    op7 = sg.instantiate_operator("and_not")
      sg.op_input_from_level("a", 0)  -- any_pump_running
      sg.op_input_from_level("b", 3)  -- station_running (inverted)
      sg.op_output("result", 6)
      sg.op_depends({ 0, 3 })
    sg.end_instantiate_operator(op7)

    -- station_ready = e_stop_clear AND (m101_healthy OR m102_healthy)
    --   AND (xv101_healthy OR xv102_healthy)
    op8 = sg.instantiate_operator("ready_check")
      sg.op_input_from_prev_level(0, "e_stop",    15)
      sg.op_input_from_prev_level(0, "m101_hlth",  1)
      sg.op_input_from_prev_level(0, "m102_hlth",  5)
      sg.op_input_from_prev_level(0, "xv101_hlth", 9)
      sg.op_input_from_prev_level(0, "xv102_hlth",11)
      sg.op_output("result", 7)
    sg.end_instantiate_operator(op8)

  sg.end_level(lv1)

  --============================================
  -- Level 2: Output assignment
  -- Writes to external alarm and status buffers
  --============================================
  lv2 = sg.level(2, "output_assignment")
    sg.level_output_buffer(16)
    sg.level_hierarchy({ group_size = 4 })

    sg.level_field("alarm_0_written",  0)
    sg.level_field("alarm_1_written",  1)
    sg.level_field("alarm_2_written",  2)
    sg.level_field("alarm_3_written",  3)
    sg.level_field("alarm_4_written",  4)
    sg.level_field("alarm_5_written",  5)
    sg.level_field("alarm_6_written",  6)
    sg.level_field("alarm_7_written",  7)
    sg.level_field("status_0_written", 8)
    sg.level_field("status_1_written", 9)
    sg.level_field("status_2_written", 10)
    sg.level_field("status_3_written", 11)
    sg.level_field("status_4_written", 12)

    -- Alarm outputs from level 0 motor faults
    op1 = sg.instantiate_operator("buffer_write")
      sg.op_input_from_prev_level(0, "value", 2)
      sg.op_target_buffer("alarms", "m101_elec_fault")
      sg.op_output("result", 0)
    sg.end_instantiate_operator(op1)

    op2 = sg.instantiate_operator("buffer_write")
      sg.op_input_from_prev_level(0, "value", 3)
      sg.op_target_buffer("alarms", "m101_therm_fault")
      sg.op_output("result", 1)
    sg.end_instantiate_operator(op2)

    op3 = sg.instantiate_operator("buffer_write")
      sg.op_input_from_prev_level(0, "value", 6)
      sg.op_target_buffer("alarms", "m102_elec_fault")
      sg.op_output("result", 2)
    sg.end_instantiate_operator(op3)

    op4 = sg.instantiate_operator("buffer_write")
      sg.op_input_from_prev_level(0, "value", 7)
      sg.op_target_buffer("alarms", "m102_therm_fault")
      sg.op_output("result", 3)
    sg.end_instantiate_operator(op4)

    -- Alarm outputs from level 0/1 conditions
    op5 = sg.instantiate_operator("buffer_write")
      sg.op_input_from_prev_level(0, "value", 12)
      sg.op_target_buffer("alarms", "high_pressure")
      sg.op_output("result", 4)
    sg.end_instantiate_operator(op5)

    op6 = sg.instantiate_operator("buffer_write_inverted")
      sg.op_input_from_prev_level(0, "value", 14)  -- flow_ok inverted
      sg.op_target_buffer("alarms", "low_flow")
      sg.op_output("result", 5)
    sg.end_instantiate_operator(op6)

    op7 = sg.instantiate_operator("buffer_write")
      sg.op_input_from_prev_level(1, "value", 6)
      sg.op_target_buffer("alarms", "station_fault")
      sg.op_output("result", 6)
    sg.end_instantiate_operator(op7)

    op8 = sg.instantiate_operator("buffer_write")
      sg.op_input_from_prev_level(1, "value", 5)
      sg.op_target_buffer("alarms", "station_degraded")
      sg.op_output("result", 7)
    sg.end_instantiate_operator(op8)

    -- Status outputs to historian
    op9 = sg.instantiate_operator("buffer_write")
      sg.op_input_from_prev_level(1, "value", 3)
      sg.op_target_buffer("status", "station_running")
      sg.op_output("result", 8)
    sg.end_instantiate_operator(op9)

    op10 = sg.instantiate_operator("buffer_write")
      sg.op_input_from_prev_level(1, "value", 4)
      sg.op_target_buffer("status", "station_healthy")
      sg.op_output("result", 9)
    sg.end_instantiate_operator(op10)

    op11 = sg.instantiate_operator("buffer_write")
      sg.op_input_from_prev_level(1, "value", 5)
      sg.op_target_buffer("status", "station_degraded")
      sg.op_output("result", 10)
    sg.end_instantiate_operator(op11)

    op12 = sg.instantiate_operator("buffer_write")
      sg.op_input_from_prev_level(1, "value", 7)
      sg.op_target_buffer("status", "station_ready")
      sg.op_output("result", 11)
    sg.end_instantiate_operator(op12)

    op13 = sg.instantiate_operator("buffer_write")
      sg.op_input_from_prev_level(1, "value", 1)
      sg.op_target_buffer("status", "redundancy_ok")
      sg.op_output("result", 12)
    sg.end_instantiate_operator(op13)

  sg.end_level(lv2)

sg.end_level_container(lc)
```

---

## 5. Evaluation Model

### 5.1 Scan Cycle

The scan_graph evaluation proceeds as a deterministic scan cycle, analogous to PLC scan execution:

1. Read external input buffers (MQTT, NATS, Modbus, etc.)
2. Evaluate level 0: clear output to UNKNOWN, run operators, resolve outputs
3. Evaluate level 1: clear output to UNKNOWN, check dependencies on level 0, run operators
4. Continue through all levels in order
5. Level N (output assignment): write results to external output buffers
6. Transmit changed output buffers to external systems

### 5.2 Tri-State Propagation

The three-state model provides natural fault propagation. If a communication failure prevents an input buffer from updating, the stale detection marks the buffer as invalid. The level 0 operators that read from that buffer produce UNKNOWN outputs. Level 1 operators that depend on those UNKNOWN outputs are blocked and remain UNKNOWN. This propagates all the way to the output assignment level, where UNKNOWN values prevent writing to external systems.

This is functionally equivalent to the quality propagation in Honeywell Experion and ABB 800xA, but implemented as a lightweight bitmask operation rather than a per-point metadata structure.

### 5.3 Memory Layout for ARM Cortex-M

On a 32KB ARM Cortex-M target, the tri-state buffer for a 32-field level requires only 12 bytes:

```c
typedef struct {
    uint32_t value_mask;      // actual true/false
    uint32_t known_mask;      // resolved flags
    uint8_t  group_known;     // hierarchy layer 1
    uint8_t  group_any_true;  // hierarchy layer 1
    uint8_t  group_value;     // hierarchy layer 1
    uint8_t  reserved;
} tristate_buffer_t;          // 15 bytes, padded to 16
```

A complete pump station with three levels of 16 fields each uses 48 bytes of tri-state buffer memory, plus operator state. The entire scan graph for the SCADA example in Section 4 fits comfortably within a 4KB memory footprint.

### 5.4 Change Optimization

In a steady-state plant, the vast majority of I/O points do not change between scans. The change-driven evaluation model means that a scan cycle through a 1,000-point system typically evaluates fewer than 50 operators — only those whose inputs actually changed. This is the same optimization principle used in OSIsoft PI's exception-based data collection, applied to the evaluation engine itself.

---

## 6. Target Compilation

The same scan_graph DSL definition compiles to different runtime representations depending on the target platform:

| Target | Output | Buffer Transport | Notes |
|--------|--------|-----------------|-------|
| ARM Cortex-M | C headers + binary blob | Direct register / Modbus | 32KB RAM, arena allocated |
| ESP32 | C headers + binary blob | MQTT / Modbus | WiFi/BLE connected |
| Raspberry Pi | C library + LuaJIT FFI | MQTT / NATS / Modbus | Gateway tier |
| x86 Server | Python / LuaJIT | NATS / PostgreSQL | SCADA tier with historian |

The compiler performs the following transformations on the DSL definition:

1. **Flatten operator templates** — expand composed templates into flat quad node lists
2. **Resolve wiring** — convert symbolic buffer/field references to index offsets
3. **Topological sort** — determine evaluation order within each level
4. **Generate dependency masks** — compute bitmasks for tri-state checking
5. **Generate target code** — emit C structures, Python classes, or binary blobs depending on target
6. **Generate buffer transport code** — emit MQTT/NATS/Modbus read/write stubs for the target platform

---

## 7. Industrial Context

scan_graph addresses the same problems solved by major DCS/SCADA vendors but with a fundamentally different approach:

| Capability | Traditional DCS | scan_graph | Advantage |
|------------|----------------|------------|-----------|
| **Source of truth** | GUI tool (proprietary binary) | LuaJIT DSL (text files) | Version control, diff, review |
| **Quality propagation** | Per-point metadata struct | Tri-state bitmask hierarchy | O(1) checking, minimal RAM |
| **Change detection** | Exception/compression (historian only) | Built into every operator | Evaluation engine optimization |
| **Multi-target** | Fixed platform per tier | Single DSL, multiple backends | Consistent logic across tiers |
| **Composition model** | CM/SCM/CAB (fragmented) | Quad templates (unified) | Single abstraction at all scales |
| **Testing** | Requires full runtime/sim | Unit testable per level | Fast CI/CD integration |

---

## 8. Open Design Questions

The following items require further discussion and resolution before implementation:

### 8.1 Historian Integration

How should the DSL express historian logging requirements? Options include per-operator annotations (periodic, on-change, exception-based) or a separate historian configuration layer that subscribes to level output buffers.

### 8.2 Alarm Management

ISA-18.2 alarm management (shelving, suppression, first-out, alarm flood control) is currently outside the scope of the quad evaluation model. Should alarm semantics be expressed as specialized operator templates, or as a separate post-processing layer that reads from output buffers?

### 8.3 Multi-Rate Evaluation

Some operators may need to evaluate at different rates (fast scan for safety interlocks, slow scan for trending). The current model assumes a single scan rate per graph. Multi-rate evaluation could be supported by defining separate scan_graph instances per rate, connected through shared buffers.

### 8.4 Online Modification

Can operator parameters (thresholds, delays) be modified at runtime without restarting the scan cycle? This is standard in DCS systems but requires careful design to avoid inconsistent state during a parameter change.

### 8.5 ChainTree Integration Points

The exact mechanism for embedding a scan_graph evaluation within a ChainTree behavior tree tick needs specification. Key questions include: how level outputs map to behavior tree node return values (success/failure/running), how the scan_graph scan rate relates to the behavior tree tick rate, and how shared buffers are managed across ChainTree and scan_graph boundaries.

### 8.6 SCADA Tier Qualifiers

The event-driven qualifiers discussed in earlier design exploration (debounce, delay, quality rules, historian annotations) need formal DSL syntax. These qualifiers may be optional annotations on operator instantiations or on level field definitions.

---

*End of Document — Draft v0.1*

