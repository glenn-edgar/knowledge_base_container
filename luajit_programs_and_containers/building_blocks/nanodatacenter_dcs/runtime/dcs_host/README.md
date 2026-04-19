# runtime/dcs_host/

The DCS host process. Lua modules loaded by the supervisor at boot.

## Entry point

`dcs.lua` — called by `../../deployment/<cpu>/start.sh` as:

```
luajit dcs.lua <absolute-path-to-bootstrap.db>
```

## Module roles

| File                      | Role                                           |
|---------------------------|------------------------------------------------|
| `dcs.lua`                 | Supervisor entry point                         |
| `pg_connector.lua`        | Short-lived PG connect for VERIFY_PG           |
| `posix_time.lua`          | Monotonic clock helper                         |
| `docker.lua`              | docker-cli wrapper (START/STOP/etc.)           |
| `host_sampler.lua`        | Resource samples (host/process/container/disk/net) |
| `kb_assignments.lua`      | Reads container assignments from bootstrap.db  |
| `kb_container_registry.lua` | CONTAINER_REGISTRY REGISTER/DEREGISTER writes |
| `kb_exception.lua`        | SYS_EXCEPTION writer                           |
| `kb_status.lua`           | KB_STATUS_FIELD read/write                     |
| `kb_stream.lua`           | Legacy static-path KB_STREAM_FIELD writer      |
| `kb_log.lua`              | KB_LOG reader/writer (Task 4: push_sample, live_stats) |
| `kb_rule.lua`             | KB_RULE reader/writer (Task 4: config, state, cooldown) |
| `bit_mask_helpers.lua`    | bit_mask_table helpers (heartbeat, ready bits) |
| `user_functions.lua`      | Chain-tree user-function registry              |

## Do NOT add here

- Laptop-side tools.
- Per-CPU path-hardcoding.
- New class/instance KB drivers — those live in
  `../../../knowledge_base/postgres/data_structures/` (e.g.
  `kb_stream_store.lua`, `kb_doc_store.lua`) and are imported
  via LUA_PATH.
