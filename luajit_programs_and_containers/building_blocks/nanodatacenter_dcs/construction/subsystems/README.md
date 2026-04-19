# construction/subsystems/

Per-subsystem KB builder modules. **Empty in Task 1**; populated
by Task 2 (KB streamlining).

## Intended shape (Task 2)

One file per subsystem. Each file exposes up to four hooks that
`build_kb.lua` iterates through:

```lua
return {
  install_site      = function(kb, site_cfg) ... end,
  install_cpu       = function(kb, cpu_cfg)  ... end,
  install_container = function(kb, container_cfg) ... end,
  install_own_kb    = function(kb)           ... end,   -- cross-site namespace
}
```

The build loop discovers subsystems by requiring every file in
this directory, so **adding a subsystem is one new file + one line
in a SUBSYSTEMS table**. This is the novice-onboarding goal from
the infrastructure roadmap.

## Do NOT put here

- Runtime state or satellite-table writers.
- Tests — those live in `../tests/`.
- Catalog data — that lives in `../catalogs/`.
