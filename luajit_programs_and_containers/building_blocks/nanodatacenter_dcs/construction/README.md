# construction/

**Laptop-side tools that build a DCS from scratch.**

You run scripts from this directory on your laptop to construct the
master Postgres KB, slice per-CPU bootstraps, and stage deployment
artifacts into `../deployment/<cpu>/`.

## Workflow (typical dev cycle)

```bash
./build_kb.sh                   # 1. build master Postgres KB
./slice_bootstrap.sh            # 2. emit ../deployment/<cpu>/bootstrap.db + start.sh
./stage_deploy.sh --mode=dev    # 3. symlink runtime/ into ../deployment/<cpu>/
```

For a cold-start operator:

```bash
./setup_secrets.sh              # one-time: write ~/.config/nanodatacenter/secrets.env
./install_infra.sh              # one-time: start pg/nats/mosquitto/kv-bridge containers
```

## File map

| File                       | Purpose                                       |
|----------------------------|-----------------------------------------------|
| `build_kb.lua`             | Builds master Postgres KB from catalogs/     |
| `build_kb.sh`              | Shell wrapper: sets LUA_PATH, sources secrets |
| `slice_bootstrap.lua`      | Per-CPU bootstrap.db slicer                   |
| `slice_bootstrap.sh`       | Shell wrapper: sources secrets                |
| `stage_deploy.sh`          | Wires runtime/ into deployment/<cpu>/         |
| `start.sh.template`        | Watchdog template copied into each deployment/<cpu>/ |
| `install_infra.sh`         | One-time infra container bring-up             |
| `setup_secrets.sh`         | One-time secrets.env seeding                  |
| `catalogs/`                | Static inputs to build_kb (definitions, topology) |
| `subsystems/`              | Per-subsystem KB builder modules (Task 2)     |
| `tests/`                   | Standalone smoke tests                        |

## Do NOT put here

- Runtime code. That lives in `../runtime/dcs_host/` or `../runtime/chain_tree/`.
- Per-CPU state. That lives in `../deployment/<cpu>/` (generated).
- The KB drivers themselves. Those live in
  `../../knowledge_base/postgres/`; this directory *uses* them.
