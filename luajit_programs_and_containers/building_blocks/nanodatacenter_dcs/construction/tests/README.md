# construction/tests/

Standalone smoke tests that exercise the construction + driver
stack against a dev Postgres. User-run; no CI integration.

| File                                 | Covers                                      |
|--------------------------------------|---------------------------------------------|
| `test_new_drivers.lua`               | Stream + doc drivers end-to-end (20 cases). |
| `test_kb_container_registry.lua`     | `KB_CONTAINER_REGISTRY` REGISTER/DEREGISTER pattern. |
| `run_test_kb_container_registry.sh`  | Shell wrapper for the registry test.        |

## Running

```bash
source ~/.config/nanodatacenter/secrets.env
luajit test_new_drivers.lua
```

Each test creates its own throwaway tables and cleans up on exit,
so running against the real `knowledge_base` dev DB is safe.
