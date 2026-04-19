# runtime/chain_tree/

The DCS chain-tree DSL source + compiled IR.

## Files

| File                              | Kind      | Role                                  |
|-----------------------------------|-----------|---------------------------------------|
| `dcs_dsl.lua`                     | source    | The DCS state machine, DSL form.      |
| `dcs_dsl_multicpu_reference.lua`  | source    | Reference implementation for review.  |
| `build_dsl.sh`                    | tool      | Compile DSL → JSON IR + debug YAML.   |
| `dcs.json`                        | generated | Compiled IR (gitignored).             |
| `dcs_debug.yaml`                  | generated | Human-readable debug dump (gitignored). |

## Build step

The supervisor expects `dcs.json` to exist next to the DSL source.
Rebuild after editing `dcs_dsl.lua`:

```bash
./build_dsl.sh
```

`start.sh` (per-CPU) fails fast if `dcs.json` is missing.

## Why this lives under `runtime/` (not `construction/`)

The DSL + compiler + IR are all facets of the same thing: how the
DCS orchestrates itself. `construction/` builds operational *state*
(the KB); `runtime/` holds operational *behaviour*. Even though
`build_dsl.sh` is conceptually a build step, separating it from
the sources it compiles would obscure the relationship.
