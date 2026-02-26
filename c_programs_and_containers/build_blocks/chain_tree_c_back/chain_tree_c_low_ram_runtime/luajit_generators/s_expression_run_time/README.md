````markdown
# ChainTree S-Expression DSL v3.1

**A LuaJIT-based domain-specific language for defining behavior trees, state machines, and sequential control flows that compile to embedded C code.**

Designed for resource-constrained systems ranging from **32KB ARM Cortex-M microcontrollers** to **8GB+ servers**.

---

## 📋 Overview

The **ChainTree S-Expression Engine (S-Engine)** is a lightweight execution core that acts as the bridge between high-level logic design and low-level embedded constraints. While fully integrated into the ChainTree ecosystem, the S-Engine is designed as a standalone runtime capable of driving logic for any embedded application.

### Why S-Expressions?

The S-Engine was engineered to solve the **“component explosion”** problem common in embedded state machines. Traditional approaches require a new C function for every small logic variation, quickly leading to brittle, unmaintainable “spaghetti code.”

#### The S-Engine adopts a *Microcode Philosophy*

- **Composition vs. Implementation**  
  Instead of monolithic nodes, behaviors are composed from a small, orthogonal set of primitives:
  * Sequences
  * Delays
  * State Machines
  * Event Dispatch

- **DSL Abstraction**  
  The Lua-based DSL manages composition and nesting, eliminating “brace hell” in raw C.

- **Event Director (Not a Calculator)**  
  Unlike classic Lisps that compute symbolic results (e.g. `(+ 1 2)`), the S-Engine **directs event flow**.  
  Its job is to evaluate the tree and route system events (ticks, messages, interrupts) to user-defined C functions.

---

## 🌟 Key Features

- **Declarative Definitions**  
  Write behavior trees using intuitive S-expression-style syntax in Lua.

- **Zero-Overhead Abstraction**  
  Logic is authored in Lua but executed as compiled C.  
  **No Lua interpreter exists on the target device.**

- **Ultra-Low Footprint**
  - Standard function nodes: **4 bytes** overhead (32-bit)
  - Stateful nodes (pointer capability): **8 bytes** overhead

- **Hash-Based Dispatch**  
  O(1) runtime lookup using compile-time-generated hashes.

- **Hybrid Blackboard**
  - Embedded structs for fast local data
  - `PTR_FIELD` for external data references

- **Persistent State**
  - Dedicated `pt_m_call` slots for functions that must survive across ticks
  - Ideal for timers, async waits, and edge-triggered logic

- **Compile-Time Safety**
  - Detects type mismatches
  - Detects unclosed tags
  - Detects hash collisions
  - Fails *before* C code is generated

---

## 📂 File Structure

The system cleanly separates **definition**, **compilation**, and **implementation**.

```text
project/
├── s_expr_dsl.lua                  # Core DSL Library (DO NOT EDIT)
├── s_compile.lua                   # Compiler Driver
│
├── my_module.lua                   # [INPUT] Your Behavior Definition
│
├── my_module_module.h              # [OUTPUT] Generated: Trees, params, module def
├── my_module_user_functions.h      # [OUTPUT] Generated: Function prototypes
├── my_module_user_registration.c   # [OUTPUT] Generated: Registration tables
│
└── my_module_impl.c                # [USER] Your C Implementation
````

---

## 🚀 Quick Start

### 1. Create Module Definition (`my_module.lua`)

Define your data structures (blackboard) and behavior tree logic.

```lua
start_module("my_module")

-- 1. Define Data Structure (Blackboard)
RECORD("robot_state")
    FIELD("position_x", "float")
    FIELD("position_y", "float")
    FIELD("state", "uint8")
END_RECORD()

-- 2. Define Behavior Tree
start_tree("main_control")
    use_record("robot_state")

    -- Execute sequence: Read Sensors -> Process Input
    local c = m_call("CFL_SEQUENCE")

        local a = o_call("READ_SENSORS")
        end_call(a)

        local b = m_call("PROCESS_INPUT")
            field_ref("position_x")
            field_ref("position_y")
        end_call(b)

    end_call(c)
end_tree("main_control")

return end_module("my_module")
```

---

### 2. Compile

Run the LuaJIT compiler to generate C headers and source files.

```bash
luajit s_compile.lua my_module.lua --header=my_module_module.h
```

Automatically generates:

* `_user_functions.h`
* `_user_registration.c`

if not explicitly specified.

---

### 3. Implement User Functions (`my_module_impl.c`)

Provide implementations for the functions declared by the DSL.

```c
#include "my_module_user_functions.h"

// DSL: READ_SENSORS | Hash: 0x12345678
void read_sensors_oneshot(
    s_expr_tree_instance_t* inst,
    const s_expr_param_t* params,
    ...
) {
    robot_state_t* bb = (robot_state_t*)inst->blackboard;
    bb->position_x = hardware_read_x();
    bb->position_y = hardware_read_y();
}

// DSL: PROCESS_INPUT | Hash: 0x87654321
s_expr_result_t process_input_main(
    s_expr_tree_instance_t* inst,
    const s_expr_param_t* params,
    ...
) {
    return SE_CONTINUE;
}
```

---

### 4. Runtime Initialization

Load the module in your embedded `main.c`.

```c
#include "my_module_module.h"

void init_system(cfl_runtime_handle_t* handle) {
    // 1. Create Module
    s_expr_module_t* mod =
        s_expr_module_create(&my_module_module, allocator);

    // 2. Register Functions
    load_cfl_s_functions(handle);   // System primitives
    load_user_s_functions(handle);  // User logic
}
```

---

## 📘 DSL Reference

### Records (Blackboard)

Records define the memory layout of the tree’s local storage.

#### Basic Types

| Type           | C Type             | Size |
| -------------- | ------------------ | ---- |
| int8 / uint8   | int8_t / uint8_t   | 1 B  |
| int16 / uint16 | int16_t / uint16_t | 2 B  |
| int32 / uint32 | int32_t / uint32_t | 4 B  |
| float          | float              | 4 B  |
| double         | double             | 8 B  |

---

### Embedded vs Pointer Fields

```lua
RECORD("pid_config")
    FIELD("kP", "float")
END_RECORD()

RECORD("controller")
    -- Embedded: bytes exist inside controller
    FIELD("internal_pid", "pid_config")

    -- Pointer: controller holds an address
    PTR_FIELD("external_pid_ptr", "pid_config")
END_RECORD()
```

---

### Tree Slot Pointers (`pt_m_call`) vs Blackboard Pointers

| Feature     | PTR_FIELD (Blackboard)     | pt_m_call (Tree Slots)        |
| ----------- | -------------------------- | ----------------------------- |
| Location    | Inside blackboard struct   | Tree pointer array            |
| Syntax      | `PTR_FIELD("name","type")` | `pt_m_call("FUNC")`           |
| Use Case    | Share data with C          | Save per-node execution state |
| Persistence | User managed               | Engine managed                |

Example:

```lua
local c = pt_m_call("TIMER_WAIT")
    flt(5.0)
end_call(c)
```

---

### Function Call Types

| Call Type  | DSL Syntax  | C Signature                 | Usage            |
| ---------- | ----------- | --------------------------- | ---------------- |
| OneShot    | `o_call`    | `void func(...)`            | Fire once        |
| Main       | `m_call`    | `s_expr_result_t func(...)` | Stateful logic   |
| Predicate  | `p_call`    | `bool func(...)`            | Condition checks |
| Persistent | `pt_m_call` | `s_expr_result_t func(...)` | Timers / async   |
| Init       | `i_call`    | `void func(...)`            | Initialization   |

---

## 🛠 C Integration API

### Blackboard Access

#### 1. By String (Flexible, slower)

```c
s_expr_blackboard_set_int_by_string(inst, "speed", 100);
float v = s_expr_blackboard_get_float_by_string(inst, "voltage", 0.0f);
```

#### 2. Direct Cast (Fastest, zero overhead)

```c
my_record_t* bb = (my_record_t*)inst->blackboard;
bb->speed = 100;
bb->voltage = 12.5f;
```

---

## 🔁 Tree Lifecycle & Tick

```c
void my_tree_init(s_expr_tree_instance_t* inst) {
    my_bb_t* bb = (my_bb_t*)inst->blackboard;
    bb->hardware_ptr = &global_driver;
}

while (system_running) {
    s_expr_result_t res =
        s_expr_tree_tick(inst, event_id, event_data);

    if (res == SE_FUNCTION_TERMINATE) {
        // Tree completed
    }
}
```

---

## ⚙️ Compiler Options

```text
Usage: luajit s_compile.lua <input.lua> [options]

  --header=<file>       Generate C module header
  --user-header=<file>  Generate user function prototypes
  --user-reg=<file>     Generate user registration C file
  --dump                Dump tree structure
  --stats               Show memory usage
  --debug               Enable debug logging
```

---

## 💡 Best Practices

* **Initialization**
  Always initialize `PTR_FIELD` values in C immediately after tree creation.

* **Naming**

  * DSL function names: `UPPER_CASE`
  * Lua variables: `snake_case`

* **Embed vs Point**

  * Embed for hot paths
  * Pointer for large configs or drivers

* **Debugging**
  Guard logging with `is_debug()` in Lua to avoid string overhead.

* **Validation**
  Use `--dump` to visually inspect tree structure before code generation.

---

## 📄 License

**MIT License**

```
```
