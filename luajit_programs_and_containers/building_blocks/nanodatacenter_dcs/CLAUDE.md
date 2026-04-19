# CLAUDE.md — nanodatacenter_dcs

AI navigation. Where to put things; what never to touch.

## Three directories, three jobs

| Directory       | Holds                                         | Runs on |
|-----------------|-----------------------------------------------|---------|
| `construction/` | Laptop tools: KB builder, slicer, stager.     | laptop  |
| `runtime/`      | DCS host process + chain-tree behaviour.      | CPU     |
| `deployment/`   | **GENERATED** per-CPU artifacts. Git-ignored. | CPU     |

## Decision table

**"I'm adding something that ..."**

| ...builds the master KB or walks the topology      | `construction/`              |
| ...is a static input catalog (definitions, etc.)   | `construction/catalogs/`     |
| ...is a per-subsystem KB builder module            | `construction/subsystems/`   |
| ...is a standalone test of the builder             | `construction/tests/`        |
| ...is code the DCS host process imports at runtime | `runtime/dcs_host/`          |
| ...is DCS chain-tree DSL or its compiler           | `runtime/chain_tree/`        |
| ...is a per-CPU start script, env file, bootstrap  | (don't — `stage_deploy.sh` emits it into `deployment/<cpu>/`) |

## What NOT to put in each directory

- `construction/` — no runtime state (no `kb_stream.lua`, no `dcs.lua`),
  no per-CPU files, no compiled artifacts.
- `runtime/` — no laptop tools (no `build_kb.*`, no `slice_*`),
  no topology data, no per-CPU state. Runtime is the same on every CPU.
- `deployment/` — DO NOT hand-author files here. It is regenerated
  by `stage_deploy.sh`. The ONE exception is `env.sh`, which is
  seeded on first stage and then owned by the operator.

## Rules for AI edits

1. **Never edit anything under `deployment/<cpu>/` except `env.sh`.**
   Any other edit will be clobbered on the next `stage_deploy.sh`.
2. **Never create files at the top level** of `nanodatacenter_dcs/`.
   Pick a layer (`construction/` or `runtime/`) and a subdir.
3. **`runtime/` must stay CPU-identical.** If a change applies to
   only one CPU, it belongs in the KB (consumed at boot via
   `bootstrap.db`) or in `env.sh`, not in source.
4. **The shared driver library lives in `../knowledge_base/postgres/`,
   not here.** When adding a new KB driver (like the stream / doc
   drivers landed in Task 0), it goes under
   `knowledge_base/postgres/construct_kb/` or
   `knowledge_base/postgres/data_structures/`, not inside
   `nanodatacenter_dcs/`. DCS *uses* those drivers via the
   Construct_Data_Tables facade.

## Each subdirectory has its own README.md

Load it for local conventions before making file-placement choices
inside that subdirectory.
