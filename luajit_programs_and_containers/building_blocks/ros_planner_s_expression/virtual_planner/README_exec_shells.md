# Virtual Planner — Execution Guide

## Shell Commands

All commands run from the `virtual_planner/` directory.

### Run the global planner only

Generates `global_plan.json` and `global_plan.yaml` in the board directory.

```bash
./run_planner.sh                                          # default: workshop_floor, strategy_full
./run_planner.sh boards/workshop_floor                    # specific board, default strategy
./run_planner.sh boards/workshop_floor strategy_full.lua  # specific board and strategy
```

### Run a single test

```bash
./run_test.sh dsl_tests/full_simulation      # hub + remote + channels, all 15 virtual actions
./run_test.sh dsl_tests/obstacle_recovery    # single path with obstacle injection
./run_test.sh dsl_tests/single_path          # 4-segment path, no missions
```

### Run all tests

```bash
./run_all_tests.sh
```

Reports pass/fail for each test and overall count.

## Directory Structure

```
virtual_planner/
│
├── lib/                              Shared modules (LUA_PATH set by shell scripts)
│   ├── dijkstra.lua                    Bidirectional Dijkstra over board graph
│   ├── vn_dsl.lua                      DSL builder API — validates board, node, path, mission defs
│   ├── global_planner.lua              Routes through board graph, emits virtual actions
│   ├── plan_yaml.lua                   YAML serializer (native number types)
│   ├── tree_builder.lua                Builds ChainTree node/KB structures programmatically
│   ├── fn_registry.lua                 Function registration + validation (no cjson dependency)
│   ├── channels.lua                    RPC (hub→remote) and streaming (remote→hub) channel sim
│   └── sequencer.lua                   Walks global plan, activates ChainTree KBs per virtual action
│
├── hub_templates/                    Hub-side behavior tree templates (one per virtual node type)
│   ├── path_template.lua               Path following — segment progression, obstacle recovery
│   └── mission_template.lua            Mission execution — arm, RPC, sensor action handlers
│
├── remote_models/                    Remote (robot) behavior tree models — swappable per test
│   └── simulated/
│       └── remote_template.lua         Simulated robot — stub user functions, tick countdown
│
├── boards/                           Board definitions using vn_dsl builder
│   └── workshop_floor/
│       ├── board.lua                   Virtual node graph (12 board nodes, 17 edges, 5 missions)
│       ├── strategy_full.lua           Match strategy — all 5 missions, start/end at launch
│       ├── hub_dsl.lua                 Hub RPC service definitions (gate controller)
│       └── robot_dsl.lua               Robot equipment (ports, drivebase, guards)
│
├── dsl_tests/                        Test scenarios (one directory each)
│   ├── full_simulation/
│   │   ├── config.lua                  Board, strategy, and remote model selection
│   │   └── main.lua                    Hub + remote + channels — runs all 15 virtual actions
│   ├── obstacle_recovery/
│   │   ├── config.lua
│   │   └── main.lua                    Single path with obstacle injection at tick 6
│   └── single_path/
│       ├── config.lua
│       └── main.lua                    4-segment path following, no missions
│
├── run_planner.sh                    Global planner only — generates JSON + YAML output
├── run_test.sh                       Run a single test:  ./run_test.sh dsl_tests/<name>
├── run_all_tests.sh                  Run all tests, report pass/fail
└── README_exec_shells.md             This file
```

## How to add a new test

1. Create a directory under `dsl_tests/`, e.g. `dsl_tests/my_test/`
2. Add `config.lua` specifying board, strategy, and remote model:
   ```lua
   return {
     board        = "workshop_floor/board.lua",
     strategy     = "workshop_floor/strategy_full.lua",
     remote_model = "simulated/remote_template.lua",
   }
   ```
3. Add `main.lua` with the test logic. It can use any module from `lib/` and `hub_templates/`.
4. Run with `./run_test.sh dsl_tests/my_test`

## How to add a new remote model

1. Create a directory under `remote_models/`, e.g. `remote_models/spike_prime/`
2. Add `remote_template.lua` that exports:
   - `kb_name` — KB name string
   - `build_tree(tb)` — builds the ChainTree KB using tree_builder
   - `registry` — table with `main`, `one_shot`, `boolean` user function tables
3. Reference it in a test's `config.lua`:
   ```lua
   remote_model = "spike_prime/remote_template.lua",
   ```

## How to add a new board

1. Create a directory under `boards/`, e.g. `boards/competition_2026/`
2. Add `board.lua` using the `vn_dsl` builder:
   ```lua
   local vn = require("vn_dsl")
   vn.reset()
   vn.board("competition_2026", 2362, 1143)
   vn.node("launch", 0, 0, "base")
   -- ... nodes, paths, missions
   return vn.build()
   ```
3. Add one or more `strategy_*.lua` files
4. Run with `./run_planner.sh boards/competition_2026`

## Dependencies

- **LuaJIT** — must be in PATH
- **chain_tree_luajit/runtime_dict/** — ChainTree runtime (ct_runtime, ct_engine, ct_definitions, ct_common, ct_walker)
- **chain_tree_luajit/lua_dsl/luajit_pipeline/json_util.lua** — JSON encoder

The shell scripts set `LUA_PATH` to include all required module directories. No manual path setup needed.
