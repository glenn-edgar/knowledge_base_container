# Build

Two artefacts, two builders.

## `libphysics.so`

Built by `make`:

```Makefile
CC      = gcc
CFLAGS  = -O2 -fPIC -Wall -Wextra -Wno-unused-parameter -std=c99
LDFLAGS = -shared -lm
TARGET  = libphysics.so
SRC     = physics_core.c
```

Just:

```
make           # build
make clean     # remove libphysics.so
make -B        # force-rebuild (used by run_tests.sh)
```

The output (~72 KB) is loaded by `physics_ffi.lua` from the same
directory as the script.

## `remote.json` (ChainTree)

Built by `build.sh`:

```
bash build.sh
```

This compiles `remote_dsl.lua` (the worker DSL) and
`remote_mqtt_ct.lua` (per-worker virtual-node definitions) into
`remote.json` plus a `remote_debug.yaml`. Both are loaded at robot
startup by `ct_loader_pure`.

`build.sh` lives in this directory and shells out to the ChainTree
LuaJIT pipeline at `building_blocks/chain_tree_luajit/lua_dsl/`.

## Combined build

`run_tests.sh` runs both unless `--skip-build` is given:

```bash
./run_tests.sh                # build everything, then unit + e2e
./run_tests.sh --skip-build   # iterate on tests
./run_tests.sh --skip-e2e     # just unit
```

## Outputs (gitignored / generated)

| File | From | Source of truth? |
|---|---|---|
| `libphysics.so`     | `make`        | no — rebuild from `physics_core.c` |
| `remote.json`       | `bash build.sh` | no — rebuild from `remote_dsl.lua` |
| `remote_debug.yaml` | `bash build.sh` | no — debug only |

## External deps

- `gcc` (C99)
- `luajit` 2.1+
- `libmosquitto` (linked via shared `mqtt_pubsub`)
- `libpostgres` (only when running inside the DCS stack — irrelevant for
  this directory's standalone tests)
