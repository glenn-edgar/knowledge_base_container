# Hierarchical Bit Mask Configuration DSL

A declarative Lua-based DSL and multi-target code generator for defining hierarchical bit-mask status, alarm, inhibit, permit, and command signaling in distributed control systems (e.g., robotics, automation, PLC networks).

The system supports sophisticated per-signal-type merge semantics (OR, AND, PRIORITY) and boundary behaviors (LATCH, COPY, RESET) as bits propagate up a plant hierarchy. Non-bit configuration parameters (speeds, timeouts, etc.) travel alongside in the same tree.

From a single source of truth, it emits radically different artifacts optimized for each target:

- **MCU / Embedded** – Ultra-compact tables + binary blob + parser-free hashed config records
- **Linux / Host** – Full names, pretty JSON sidecar, plus the same hashed records for fast access

## Features

- Declarative device classes with per-bitspace bank sizing
- Named and exported bits (e.g., `ALARM.OverTorque`, `STATE.Ready`)
- Different merge rules per bitspace (`OR` for alarms, `AND` for permits, `PRIORITY` for state)
- Boundary behaviors (`LATCH`, `COPY`, `RESET`)
- Hierarchical node instantiation (`Plant.Line1.Cell3.Robot2`)
- Co-located non-bit config (`MaxSpeed = 1200`, `TimeoutMs = 250`)
- Deterministic, pure Lua generator – no runtime dependencies
- Build-time FNV1a-32 hash collision detection (hard fail)
- Multiple independent output artifacts:
  - `schema_blob.bin` + embedded header (bit hierarchy only)
  - `schema_tables.c/h` (dense const tables for MCU)
  - `cfg_json_recs.c/h`, `cfg_index.c/h`, `cfg_hashes.h` (typed, hash-addressable config)
  - `config.json` sidecar (Linux profile only)

## Directory Structure

```
dsl/
├── gen.lua                  # Top-level generator entrypoint
├── example_schema.lua       # Sample schema (RobotArm + Conveyor)
├── schema_compiler.lua      # Compiles DSL → intermediate representation
├── emit_c.lua               # Emits schema_tables.h/c (MCU tables)
├── emit_bin.lua             # Emits schema_blob.bin
├── emit_bin2c.lua           # Emits schema_blob_embed.h (bin2c style)
├── emit_cfg_json.lua        # Emits pretty config.json (Linux)
├── emit_cfg_records.lua     # Emits hashed config records + index
└── dsl_runtime.lua          # DSL constructor functions (Schema, Node, etc.)
```

## Usage

```bash
# Generate for MCU (32k flash profile)
luajit dsl/gen.lua dsl/example_schema.lua mcu_32k out/mcu

# Generate for Linux/host (full names + JSON sidecar)
luajit dsl/gen.lua dsl/example_schema.lua linux out/linux
```

## Profiles (defined in schema)

```lua
profiles = {
  mcu_32k = {
    emit_json_sidecar = false,
    keep_names = false,
    enable_provenance = false,
    max_nodes = 64,
    max_banks = 256,
  },
  linux = {
    emit_json_sidecar = true,
    keep_names = true,
    enable_provenance = true,
  }
}
```

## Key Output Files

### MCU Target
- `schema_ids.h` – `#define SCHEMA_NODE_COUNT`, fingerprint, etc.
- `schema_tables.h/c` – Dense const tables (`g_schema_parents`, `g_schema_banks`, `g_schema_bits`)
- `schema_blob.bin` + `schema_blob_embed.h` – Packed binary blob for flash embedding
- `cfg_json_recs.c/h`, `cfg_index.c/h`, `cfg_hashes.h` – Parser-free config access

### Linux Target
- All of the above **plus**
- `config.json` – Human-readable hierarchical config

## Runtime Access Example (C – works on both MCU and Linux)

```c
#include "cfg_hashes.h"
#include "cfg_json_recs.h"
#include "cfg_index.h"

// Binary search helper (implement once, use everywhere)
static int cfg_find(json_hash32_t hash) {
  // ... binary search on g_cfg_index ...
}

uint32_t get_robot_max_speed(void) {
  int idx = cfg_find(CFG_HASH_PLANT_LINE1_CELL3_ROBOT2_CONFIG_MOTION_MAXSPEED);
  if (idx >= 0 && g_cfg_recs[idx].object_type == JSON_TYPE_UINT32)
    return g_cfg_recs[idx].value.u32_value;
  return 0;
}
```

## Why This Design Wins

- Single source of truth
- Config changes don’t shift bit offsets
- Bit schema changes don’t invalidate config hashes
- No JSON parser needed on MCU
- Instant hash-based lookups on both ends
- Hard fail on hash collisions
- Clean separation of signaling vs tuning

Built with stubborn attention to detail. 🐴

Enjoy!
