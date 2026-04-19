# nanodatacenter_dcs

The Distributed Control System building block. Owns the master
knowledge-base construction, the per-CPU host process, and the
per-CPU deployment artifacts.

## Three-layer model

```
construction/   Laptop-side. Builds the master Postgres KB, slices
                per-CPU bootstrap.db, stages deployment/<cpu>/.
                You run scripts from here on your laptop.

runtime/        Ships to every CPU. The DCS host process + chain-tree
                behaviour it executes. Stable; changed only when DCS
                runtime itself changes.

deployment/     GENERATED per CPU. .gitignored. Contains bootstrap.db,
                start.sh, env.sh, and a copy-or-symlink of runtime/.
                Operator owns env.sh.
```

If you are asking "where does my change go?" — see `CLAUDE.md` at
the top of this directory.

## Typical dev cycle

```bash
cd construction
./build_kb.sh                   # build master Postgres KB
./slice_bootstrap.sh            # emit deployment/<cpu>/bootstrap.db + start.sh
./stage_deploy.sh --mode=dev    # symlink runtime/ into deployment/<cpu>/
# (operator edits deployment/<cpu>/env.sh once, on first stage)
cd ../deployment/cpu_01 && ./start.sh
```

## Top-level files

- `continue.md` — session plans and historical context; not code.
- `ops_ui/` — superseded design notes (admin UI shipped inside
  `dcs_console` instead). Flagged for deletion as a follow-up.

## Related building blocks

- `../knowledge_base/postgres/` — shared KB framework used by
  `construction/build_kb.lua` (both static-path KB tables and the
  new class/instance stream + doc drivers live here).
- `../chain_tree_luajit/` — chain-tree runtime library loaded by the
  DCS host process at boot.
- `../luajit_base/` — base container that vendors helpers from
  `runtime/dcs_host/` at image build time.
