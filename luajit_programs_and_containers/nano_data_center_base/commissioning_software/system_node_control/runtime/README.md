# runtime/

**Ships to every CPU unchanged.** The DCS host process (the thing
`start.sh` execs) and the chain-tree behaviour it loads.

Runtime is CPU-identical: the same files run on cpu_01, cpu_02,
etc. Per-CPU variation lives in `../deployment/<cpu>/bootstrap.db`
(identity, assignments) and `../deployment/<cpu>/env.sh` (secrets,
overrides), **not here**.

## Layout

```
runtime/
├── dcs_host/        The host process. Entry point: dcs.lua.
└── chain_tree/      DCS chain-tree DSL source + compiled IR (dcs.json).
```

## Entry point

`dcs_host/dcs.lua` — invoked by `../deployment/<cpu>/start.sh`
with one argument (the absolute path of bootstrap.db). Reads
bootstrap for identity + pg connect, then enters the chain-tree
supervisor loop.

## Do NOT put here

- Per-CPU files, paths, or hostnames.
- Laptop-side tools. Those live in `../construction/`.
- Generated artifacts (other than `chain_tree/dcs.json`, which
  is built in place by `chain_tree/build_dsl.sh`).
- New KB drivers. Those live in `../../knowledge_base/postgres/`
  and are imported by `dcs_host/` modules via LUA_PATH.
