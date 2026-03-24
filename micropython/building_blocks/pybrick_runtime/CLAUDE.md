# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Repository Overview

This directory contains two cloned Pybricks repositories used as reference/building-block material:

- **pybricks-micropython/** — Main firmware repo. Builds MicroPython-based firmware for LEGO hubs (SPIKE Prime, Technic, City, BOOST Move, Essential, EV3, etc.). Written primarily in C with Python tooling.
- **pybricks-api/** — Pure Python API stubs for documentation generation. No runnable code; used to generate docs at docs.pybricks.com.

## Build Commands (pybricks-micropython)

All commands run from the `pybricks-micropython/` directory.

```bash
# Set up Python environment (one-time)
poetry install
eval $(poetry env activate)

# Build mpy-cross (required before any brick build)
make mpy-cross -j8

# Build firmware for a specific hub
make primehub -j8          # SPIKE Prime / Robot Inventor
make cityhub -j8
make technichub -j8
make movehub -j8           # BOOST Move Hub
make essentialhub -j8      # SPIKE Essential
make virtualhub -j8        # Host-native build for testing
make ev3 -j8

# Clean
make clean-primehub        # or clean-<target>
make clean-all

# Deploy firmware to a hub over DFU
make -C bricks/primehub -j8 deploy
```

Build output lands in `bricks/<target>/build/`.

## Testing

```bash
# Automated virtualhub tests (MicroPython-level)
./test-virtualhub.sh
./test-virtualhub.sh --list-tests
./test-virtualhub.sh --include <regex>    # run single test

# pbio C library unit tests
./test-pbio.sh
./test-pbio.sh --list-tests

# pbio coverage report
./test-pbio-coverage.sh
```

## Code Formatting & Linting

```bash
# Auto-format C and Python (follows MicroPython coding conventions)
poetry run ./tools/codeformat.py

# Format C only
poetry run ./tools/codeformat.py -cf

# Format Python only
poetry run ./tools/codeformat.py -p

# Enable pre-commit hook (runs codeformat on commit)
poetry run pre-commit install

# Python linting
poetry run flake8        # max-line-length=99
poetry run ruff check    # excludes lib/btstack/ and micropython/
```

## Architecture (pybricks-micropython)

### Hub firmware structure

Each hub in `bricks/<target>/` has a short Makefile that sets platform-specific variables (MCU, oscillator, DFU IDs, etc.) and includes `bricks/_common/common.mk` which drives the actual build. The `_common/` directory contains shared MicroPython port code (`micropython.c`, `mphalport.c`, `mpconfigport.h`).

### Key directories

- **lib/pbio/** — Platform-independent C library ("Pybricks I/O"). Contains motor control, sensor drivers, color processing, drivebase kinematics, IMU, battery, BLE, and light animation. This is the core runtime. Has its own test suite under `lib/pbio/test/`.
- **pybricks/** — MicroPython C module bindings (the `pybricks` package). Subdirectories map to Python import paths: `pybricks.hubs`, `pybricks.pupdevices`, `pybricks.robotics`, `pybricks.tools`, `pybricks.parameters`, etc.
- **micropython/** — Git submodule pointing to `pybricks/micropython` fork (branch `pybricks-v3.x`). Do NOT clone with `--recursive`; submodules are fetched on-demand by make.
- **lib/btstack/, lib/lego/, lib/BlueNRG-MS/** — Vendor BLE and LEGO protocol libraries.

### Build flow

1. `mpy-cross` is built first (compiles `.py` → `.mpy` bytecode)
2. Each brick's Makefile sets `PBIO_PLATFORM` and includes `common.mk`
3. `common.mk` compiles pbio, MicroPython core, pybricks modules, and frozen Python modules
4. Output: firmware binary in `bricks/<target>/build/`

### Submodules

Only `micropython` is regularly needed. Other submodules (`btstack`, `STM32_USB_Device_Library`, `umm_malloc`) have `update = none` and are fetched manually when needed.

## Commit Message Convention

Prefix subject line with the area of code changed (relative file path). For pbio changes, omit `lib/` prefix. For pybricks package changes, use Python import path (e.g., `pybricks.tools: Add stopwatch feature`).

## Requirements

- Ubuntu 24.04 (recommended), Python 3.12+, Poetry v2.x
- ARM cross-compiler: `gcc-arm-none-eabi` v13.x (for hub firmware)
- GNU Make, GCC (for host/virtualhub builds)
