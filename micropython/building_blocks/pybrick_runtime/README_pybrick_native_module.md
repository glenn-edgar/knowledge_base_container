# Pybricks Native Modules (.mpy)

## Overview

Pybricks supports MicroPython Native Modules — the standard `.mpy` format that allows C code to be compiled into a module importable from Python. This provides near-C performance without rebuilding or reflashing the hub firmware.

## Performance Comparison

| Approach | Firmware Rebuild Required? | Speed |
|---|---|---|
| Pure Python (.py) | No | Slowest |
| Compiled bytecode (.mpy) | No | Moderate |
| Native module (.mpy with C) | No | Near C speed |
| Modifying pybricks/lib/pbio C code | Yes — full reflash | Full C speed |

## How It Works

1. Write performance-critical code in C
2. Compile it with `mpy-cross` using the `-march` flag for the target architecture (e.g., ARM Thumb for SPIKE Prime)
3. The resulting `.mpy` file contains actual native machine code instead of interpreted bytecode
4. `import` the `.mpy` file in your Python script like any normal module

The native code runs directly on the CPU, bypassing the MicroPython interpreter loop. This is useful for tight loops, math-heavy computations, or time-critical control logic.

## How Modules Get Onto the Hub

### BLE Transfer (normal path)

The primary method for loading user code and `.mpy` modules onto Pybricks hubs (SPIKE Prime, Technic, City, etc.):

1. Pybricks Code (web IDE at code.pybricks.com) connects to the hub over Bluetooth (BLE)
2. Your Python program and any `.mpy` modules are transferred over BLE to the hub's flash filesystem
3. The hub runs the program and imports the `.mpy` modules

The `pybricksdev` CLI tool also supports BLE transfer for command-line workflows.

### USB / DFU (firmware only)

USB on Pybricks hubs is used for DFU (Device Firmware Update) — flashing entire firmware images. It is not exposed as a mass storage device for dragging files onto.

### Frozen Modules (build-time)

Modules listed in `bricks/<target>/manifest.py` are compiled and baked into the firmware image at build time. This requires a full firmware rebuild and reflash, which is the heavier approach that native `.mpy` modules avoid.

## Flash Memory Constraints

LEGO hubs have very limited flash storage (SPIKE Prime has ~1MB usable). Native `.mpy` modules must be kept small. This constraint is also why the Pybricks team is selective about what gets included in the firmware itself.

## Building mpy-cross

The `mpy-cross` compiler is built from the `pybricks-micropython` directory:

```bash
make mpy-cross -j8
```

This produces the `micropython/mpy-cross/build/mpy-cross` binary used to compile `.py` to `.mpy` bytecode and C to native `.mpy` modules.
