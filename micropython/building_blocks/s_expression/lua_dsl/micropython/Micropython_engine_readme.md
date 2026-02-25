# S-Expression Engine: MicroPython Class Generator

## Architecture

```
  Lua DSL                    Lua Generator              MicroPython
 ┌──────────┐              ┌───────────────┐          ┌──────────────┐
 │ .lua spec │──compile──▶ │ module_data   │──emit──▶ │ derived .py  │
 └──────────┘              └───────────────┘          └──────┬───────┘
                                                             │ inherits
                                                      ┌──────┴───────┐
                                                      │ se_engine_   │
                                                      │ base.py      │
                                                      └──────────────┘
```

**Base class** (`se_engine_base.py`): dispatch loop, param readers,
blackboard access, all builtin functions (SE_SEQUENCE, SE_IF_THEN_ELSE,
SE_FORK, predicates, etc.).

**Generated class** (e.g. `door_controller.py`): param stream as `bytes`
constant, field offsets, string table, defaults, dispatch tuple mapping
func_index to bound methods, stub methods for user functions.

No loader. No hash tables. No function registration. The generated class
IS the program.

## What Lives Where

| Data              | Location        | Mutable |
|-------------------|-----------------|---------|
| PARAMS (bytecode) | Flash (frozen)  | No      |
| STRINGS           | Flash (frozen)  | No      |
| DEFAULTS          | Flash (frozen)  | No      |
| Dispatch tuple    | RAM (once)      | No      |
| Blackboard (bb)   | RAM             | Yes     |
| Pointer slots     | RAM             | Yes     |
| Stack             | RAM             | Yes     |

## How Dispatch Works

There is no walker. There is no tree traversal engine. Each S-Expression
function evaluates its own parameters.

The root of every tree is a single function call. When `run()` is called:

1. `dispatch(0, ...)` reads OPEN_CALL at position 0
2. Reads the func_index from the opcode param
3. Indexes into the dispatch tuple: `self._dispatch[func_index]`
4. Calls that method, passing the position and content_count
5. The method reads its own params and calls `dispatch()` for any
   child OPEN_CALLs it finds

Each function owns its scope. SE_SEQUENCE iterates children.
SE_IF_THEN_ELSE reads three children (pred, then, else).
SE_STATE_MACHINE scans for matching case values. User functions
read whatever params the DSL emitted for them.

## How To Use

### 1. Compile the DSL (Lua side)

```lua
-- In s_compile.lua or your build script:
local mp_gen = require("s_expr_micropython_gen")

-- After building module_data and binary_gen as usual:
local gen = mp_gen.MicroPythonGenerator.new(module_data, binary_gen)
local py_source = gen:generate("door_controller")

local f = io.open("door_controller.py", "w")
f:write(py_source)
f:close()
```

### 2. Implement user functions (MicroPython side)

The generated class has stubs that raise `NotImplementedError`:

```python
from door_controller import DoorController

class MyDoorController(DoorController):

    def _user_disable_motor_tle7269g(self, pos, content_count,
                                      ev_type, ev_id, ev_data):
        # Your hardware code here
        self.motor_pin.value(0)
        return CONTINUE

    def _user_enable_motor_tle7269g(self, pos, content_count,
                                     ev_type, ev_id, ev_data):
        self.motor_pin.value(1)
        return CONTINUE

    def _user_set_bridge_mode(self, pos, content_count,
                               ev_type, ev_id, ev_data):
        cursor = self._first_param_pos(pos)
        mode = self._read_field_value(cursor)
        self.bridge.set_mode(mode)
        return CONTINUE
```

Or implement directly in the generated class — the stubs are there
to be filled in.

### 3. Run

```python
ctrl = MyDoorController()

# Main loop
while True:
    ev_type, ev_id, ev_data = get_next_event()
    ctrl.run(ev_type, ev_id, ev_data)
```

### 4. Freeze into firmware

Copy `se_engine_base.py` and the generated `.py` file into your
MicroPython frozen modules directory. The `PARAMS` bytes constant
will reside in flash, not RAM.

```
ports/esp32/modules/
    se_engine_base.py
    door_controller.py
```

Rebuild firmware. The param stream costs zero RAM.

## Field Accessors

The generated class includes Python properties for each blackboard field:

```python
ctrl = MyDoorController()

# Read
print(ctrl.motor_state)
print(ctrl.temperature)

# Write
ctrl.motor_state = 0
ctrl.temperature = 25.0
```

These use the field offsets baked in at generation time. No hash lookup,
no string comparison — just `struct.pack_into` at a constant offset.

## Adding Builtins

If you need SE_WAIT, SE_TICK_DELAY, or other platform-specific builtins,
implement them in the base class or override in your derived class:

```python
class MyDoorController(DoorController):

    def _se_tick_delay(self, pos, content_count, ev_type, ev_id, ev_data):
        cursor = self._first_param_pos(pos)
        ticks = self._pi32(cursor)
        # Save position, return HALT
        # On next run(), resume after delay
        ...
```

The timing builtins require cooperative scheduling — the base class
stubs them as _se_nop because the mechanism is platform-dependent
(asyncio, timer interrupts, simple tick counter, etc.).

## RAM Budget (ESP32, typical)

| Component              | Bytes    |
|------------------------|----------|
| MicroPython heap       | ~80,000  |
| se_engine_base (code)  | flash    |
| Generated class (code) | flash    |
| PARAMS stream          | flash    |
| Dispatch tuple (20 fn) | ~200     |
| Blackboard (100 fields)| ~400     |
| Pointer slots (4)      | ~64      |
| Stack (empty)          | ~64      |
| **Total RAM**          | **~730** |

The param stream for a complex tree (door controller) is a few KB —
all in flash. Runtime RAM is dominated by the blackboard.

