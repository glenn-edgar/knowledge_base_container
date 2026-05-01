# construction/catalogs/

Static data inputs consumed by `../build_kb.lua`.

- `definitions.lua` — the container catalog. One entry per
  kind of container DCS can run (image, port_spec, supervisor
  conventions).
- `topology.lua` — per-site placement. Which CPUs exist, which
  instances of which definitions land on each CPU, master CPU
  declaration, Postgres connect info.

Files here are **hand-edited inputs**. When you add or rename a
container, you edit one of these. No generated files land here.
