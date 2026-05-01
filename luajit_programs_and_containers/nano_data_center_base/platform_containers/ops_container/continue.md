# nanodatacenter_gateway — Continuation Plan

## Session 1 (2026-04-15) — design only; no code written

Long chat-mode session. Locked in the architecture for three related
containers + a DCS schema extension. Nothing committed to disk beyond
directory skeletons; all prior ideas evolved through back-and-forth.

This document is the single source for the full design; DCS's
continue.md cross-references it for the schema changes DCS needs.

## Model

**Microservice-per-container, web server per app.**

In the Go + jQuery era each app container embedded its own HTTP
handler; this model carries over. Each app that wants a UI ships
its own openresty web server — sometimes as a separate container
from the app core, sometimes bundled into the same container as
additional processes. Either way, the web server travels with the
app, not as a shared platform.

That means there are **two classes of web server**:

1. **The portal (gateway)** — one site-wide container,
   application-agnostic. Its job: discover registered app UIs,
   list them in a tree, reverse-proxy operator traffic to them.
   Operator's browser only ever talks to the gateway's origin.
2. **Per-app microservice UIs** — one per app that wants to expose
   a UI. They serve their own HTML + endpoints. They never face
   the operator's browser directly; all traffic flows through the
   gateway's reverse proxy.

DCS ops UI is one of these microservice UIs. Future ros_planner_ii
gateway (when ported) will be another. Each app's UI container is
owned by the app's team / codebase, not by the gateway's.

## Three deliverables

```
building_blocks/
├── openresty_base/                       shared base image for openresty-based app UIs
├── nanodatacenter_gateway/               the portal (this dir)
├── nanodatacenter_dcs/ops_ui/            DCS's microservice UI (one concrete consumer)
```

### openresty_base

- `FROM openresty/openresty:alpine-fat`
- `RUN luarocks install pgmoon lua-resty-openssl` (vendored early so layer
  is shared across every downstream app image).
- Stages a shared `resty_kb_client/` lua library into
  `/usr/local/openresty/lualib/resty_kb_client/`. Every app openresty
  image `FROM`s this and inherits the library without re-vendoring.
- Vendors htmx (single file) into `/usr/local/openresty/nginx/html/vendor/`
  for the same layer-sharing reason.

Layer sharing strategy — Docker reuses layers byte-for-byte when
identical commands produce them. Putting base + luarocks + resty_kb_client
in a shared parent image means every descendant gets those layers for
free.

### nanodatacenter_gateway

- `FROM nanodatacenter/openresty-base:latest`
- nginx.conf: reverse proxy `/ui/<container_name>/<...>` → upstream
  `<host>:<port>/<...>` with the prefix stripped.
- Polls pg for the container registry every 2s, caches in
  `lua_shared_dict ui_registry`.
- Portal page (htmx-driven): tree-grouped by cpu_id, one entry per
  UI-bearing registered container.

### nanodatacenter_dcs/ops_ui

- `FROM nanodatacenter/openresty-base:latest`
- DCS-specific endpoints (exception list, heartbeat grid, sample
  streams, ack/clear writes).
- Registers itself by NOT doing anything — node_control writes its
  registration row as part of starting the container.

## Gateway → microservice routing

**Reverse proxy**, not direct links or redirects. IPs/ports are
runtime-bound, often not reachable from the operator's browser in
production (firewalled fleets). Operator's browser talks only to the
gateway's URL; gateway forwards to upstream host:port resolved from
the registry at request time.

URL shape: `/ui/<container_name>/<path>` at the gateway →
`/<path>` at the upstream (prefix stripped).

Mechanism: `proxy_pass` with a Lua-resolved variable, backed by
`lua_shared_dict ui_registry` refreshed by a 2s timer. Shared-dict
lookup is microseconds per request; registry refresh is background.

## Registration contract

**node_control writes**, apps don't.

Rationale: node_control owns the container lifecycle. It knows when
a container is starting, when it dies, when it's being torn down —
with zero latency compared to the app self-publishing heartbeats.
Apps become simpler (no NATS/KV client, no self-identification, no
TTL refresh loop). Registration is removed at the moment teardown
starts, so stale portal entries are impossible.

### Backend: Postgres (`knowledge_base_status`)

Not NATS JetStream KV. Rationale:

- Supervisor-driven writes mean no TTL needed.
- node_control already has pg open for every other thing it does
  (heartbeats, exceptions, ready bits).
- Gateway already needs pg for its DCS-ops read path.
- One DB client, one credential set, one place to debug.
- NATS/KV would add a new client dependency for nobody's direct
  benefit.

Consequence: gateway polls pg every ~2s. That's fine; registration
events are rare (container deploy/tear-down), not per-tick.

### Path shape

```
system.site.<S>.cpu.<cpu_id>.container_registry.<container_name>
```

One row per container node_control is managing. Registry is
**supervisor-authoritative, not UI-specific**. All attached
containers register here. Gateway is one consumer; filters rows
whose `ports` include `purpose = "ui"`.

### Row data (JSON)

```json
{
  "cpu_id":         "cpu_01",
  "host":           "cpu_01",
  "container_name": "dcs_ops",
  "definition":     "dcs_ops_ui",
  "ports": [
    { "port": 8080, "protocol": "http", "purpose": "ui",
      "description": "operator dashboard" }
  ],
  "description":    "DCS system/node observability and control",
  "category":       "observability",
  "registered_at":  "2026-04-15T18:23:04Z"
}
```

- `ports[]` is always an array, even for single-port containers.
- Each port is an object carrying port number + role metadata.
  Consumers (gateway, metrics scrapers, RPC discoverers) filter
  by `purpose`.
- `cpu_id` is redundant with the ltree path but cheap in JSON;
  lets consumers skip path parsing.
- `definition` lets consumers group "all ros_planner instances"
  without parsing names.

### Port lifecycle: where each field comes from

- **External port number (per placement)**: `topology.lua` per-instance
  `ports = { <slot_name> = <port_number> }`.
- **Port role metadata (per image)**: `definitions.lua` per-def
  `port_spec = { <slot_name> = { protocol, purpose, description,
  internal } }`.
- **Join at construct time**: `construct_dcs_kb.lua` merges the
  per-instance port numbers with the per-def port_spec and writes
  the result into `system.site.<S>.cpu.<id>.container.<name>.build.spec`.
- **Join at runtime**: node_control reads the merged build.spec from
  bootstrap.db, translates it into `docker run -p <ext>:<int> ...`
  for each port, and writes the `ports[]` array in the registration
  row.

### Port convention

For openresty-based apps, internal port is **always 8080**. Baked
into the image's nginx.conf as a constant (`listen 8080;`). External
port varies per placement. `docker run -p <external>:8080`.

For non-openresty apps, internal port is whatever the image uses;
declared in `port_spec.<slot>.internal`.

### Port conflict detection

Two levels:

1. **Construct-time** in `construct_dcs_kb.lua`: after joining, check
   that all external ports on each CPU are unique. Raise an error
   at `build_kb.sh` time on collision.
2. **Runtime pre-flight** in node_control: before `docker run`,
   query the container_registry for any existing row on this CPU
   that already claims this port. If found, raise SYS_EXCEPTION
   and skip starting the container.

Kernel-level port-in-use (non-DCS process grabbed the port) surfaces
as `docker run` failure through the existing container-start error
path.

### Publish timing

**Optimistic**: write registration row immediately after
`docker run` returns. Portal briefly shows UIs that are still
booting; first visit may 502; operator refreshes. Self-correcting
within 2-3 seconds.

Health-checked publishing (probe `/_health` before writing row) is
deferred until apps with slow startup make it necessary.

### Boot-time reconciliation

On node_control's `sync` state, before starting any containers:

```sql
DELETE FROM knowledge_base_status
 WHERE path <@ 'system.site.<S>.cpu.<self>.container_registry'::ltree
```

Wipes any stale rows this supervisor wrote in its prior life. As
setup proceeds, fresh rows are written for actually-running
containers. Self-healing after any crash; no stale entries persist.

## DCS schema changes needed (see also DCS continue.md)

1. **`container_registry` anchor**: `construct_dcs_kb.lua` adds one
   per CPU at `system.site.<S>.cpu.<id>.container_registry`. Dynamic
   rows live in `knowledge_base_status` under that subtree; not
   declared at construct time.
2. **`port_spec` in container_definition**: `definitions.lua` grows
   a `port_spec = { <slot_name> = { protocol, purpose, description,
   internal } }` field per def. Construct serializes it into
   `system.container_definition.<def>.build.spec.port_spec`.
3. **`ports` in topology instances**: `topology.lua` per-instance
   grows `ports = { <slot_name> = <port_number> }`. Construct joins
   with port_spec and writes the merged form into the placement's
   `build.spec`.
4. **Port uniqueness sanity pass**: new pass in `construct_dcs_kb.lua`
   iterates all placements per CPU, unions their port lists, asserts
   uniqueness.
5. **node_control user functions**:
   - `RECONCILE_CONTAINER_REGISTRY` (oneshot at sync): runs the
     DELETE above.
   - `CHECK_PORT_CONFLICT(spec)` (per-container pre-flight): pre-
     flight query before docker run.
   - `REGISTER_CONTAINER(spec)` (per-container post-spawn): INSERT
     row into knowledge_base_status.
   - `DEREGISTER_CONTAINER(name)` (per-container teardown):
     path-targeted DELETE.

## Frontend (portal + ops UI)

- **No npm, no bundler.** Ever.
- **No jQuery.** The "no tree control" gap from prior experience
  rules it out.
- **htmx** as the client-side interaction library. Single ~16 KB
  JS file vendored once into `openresty_base` (shared across
  gateway + ops_ui). Server returns HTML fragments; htmx swaps
  them into the page. Matches the Go + jQuery mental model without
  the jQuery baggage.
- **CSS**: hand-rolled for v1. If scope grows, consider pico.css
  (classless, ~10 KB, no build). Never bootstrap.
- **Tree widget** (portal): nested `<ul>` with `hx-get` lazy-load
  per node. Each registered container = one `<li>` that opens as
  an anchor into `/ui/<container_name>/`.

Static content convention:
- gateway: `/usr/local/openresty/nginx/html/index.html` + `/tabs/*`
  + `/vendor/*` (vendored htmx).
- ops_ui: same layout, different content. Gateway proxies requests
  through, so ops_ui's HTML can use relative paths and it just works.

## Build order (REVISED 2026-04-15 evening)

**Chicken-and-egg insight**: to test the reverse-proxy gateway we
need a known-good upstream web server. To test DCS node_control's
container-instantiation + port-mapping path we need a real
(non-infra) container to run. **dcs_ops_ui serves both needs** —
so it goes first, and everything else slots in behind it.

1. **dcs_ops_ui v0 — standalone openresty container, no DCS
   plumbing yet.**
   - `FROM openresty/openresty:alpine-fat` directly (skip
     openresty_base for this session; inline pgmoon usage).
   - Frontend template: `building_blocks/system_api/shell/` — the
     federated shell built 2026-04-04 for ros_planner_ii's UI has
     nav strip + tree sidebar (already solves the tree-control
     problem) + iframe tabs + dark-mode monospace styling. Clone
     its shape into a new `dcs_ops_ui` dir; replace tabs and
     endpoints.
   - DCS endpoints: `/api/dcs/exceptions`, `/api/dcs/heartbeats`,
     `/api/dcs/system_ready`, `/api/dcs/samples?path=`, and POST
     `/api/dcs/ack`. Same inline-pgmoon pattern as
     `third_party_containers/openresty/nginx.conf`.
   - `run_ops_ui.sh` harness: mirrors
     `luajit_base/container/run_dummy.sh`. Sources secrets.env,
     `--network=host`, `docker run` directly.
   - Test: visit `http://127.0.0.1:8080/` in browser; see live
     DCS state rendered from pg. No DCS managing this yet.
2. **DCS schema extension.**
   - `construct_dcs_kb.lua`: add `container_registry` anchor per
     CPU; `port_spec` serialization under container_definition;
     topology-instance `ports` field; construct-time
     port-uniqueness sanity pass.
   - `definitions.lua`: grow `dcs_ops_ui` def with
     `port_spec = { ui = {protocol="http", purpose="ui", internal=8080} }`.
   - `topology.lua`: add instance
     `{ name="dcs_ops", definition="dcs_ops_ui", ports={ui=8080} }`
     on the master CPU.
   - Test: `build_kb.sh` produces bootstrap.db with new fields;
     inspect via sqlite3 CLI.
3. **DCS node_control user functions + wiring.**
   - `RECONCILE_CONTAINER_REGISTRY` (sync-state oneshot, before
     START_ASSIGNED_CONTAINERS).
   - `CHECK_PORT_CONFLICT(spec)` (pre-flight per container).
   - `REGISTER_CONTAINER(spec)` (post-spawn per container).
   - `DEREGISTER_CONTAINER(name)` (pre-teardown).
   - Replace `START_ASSIGNED_CONTAINERS` stub with real loop:
     per assignment → CHECK_PORT_CONFLICT → docker.run_from_spec →
     REGISTER_CONTAINER. Parallel for STOP.
   - Test: start DCS with dcs_ops_ui in topology. Watch
     node_control start it, write registry row; browser still
     reaches it directly. `docker stop` DCS → teardown removes
     row. First real test of app-container management.
4. **openresty_base (retrofit).**
   - By now dcs_ops_ui has shown us what's duplicated. Extract:
     `resty_kb_client/pg.lua` (pgmoon + keepalive), `ltree.lua`
     (path helpers), `registry.lua` (registry reader — gateway
     consumer). Vendor htmx here too.
   - Build `nanodatacenter/openresty-base:latest`.
   - Refactor dcs_ops_ui's Dockerfile to `FROM openresty-base`,
     delete inlined pgmoon calls. Rebuild, retest end-to-end.
   - Layer sharing now kicks in for the next openresty image.
5. **nanodatacenter_gateway.**
   - `FROM openresty-base`. Polls pg registry every 2s, caches
     in `lua_shared_dict`. Reverse-proxies `/ui/<name>/` with
     prefix stripped. Portal index with htmx-driven tree
     grouped by cpu_id.
   - Test: DCS topology grows gateway instance alongside
     dcs_ops_ui. Operator visits gateway URL. Portal shows
     dcs_ops in tree. Click → proxied → UI renders.
     Full end-to-end: registry → proxy → upstream → operator.

Each step is independently testable; partial progress is deployable.

### Why this ordering is strictly better

- **Concrete feedback in session 1.** dcs_ops_ui v0 renders real
  data. Visible proof the stack works. Compare to starting with
  openresty_base where test is "can we import the library".
- **DCS learns to run app containers** (step 3). First time
  node_control is exercised for anything beyond infra start/stop.
  Big milestone tested against a real target.
- **openresty_base emerges from real usage.** By step 4 we know
  what to extract because we've written it twice (in dcs_ops_ui
  inline). Abstraction is mechanical, not speculative.
- **Gateway built last, with a known-good upstream.** If the
  proxy is broken we know it's the proxy, not the backend.

### What gets thrown away

Step 1's inlined pgmoon boilerplate (~20 lines in the nginx.conf)
gets replaced in step 4 by `resty_kb_client.pg`. That's the
explicit cost of this ordering. Accepted — temporary dup is
cheaper than premature abstraction.

## Deferred / future

- **Pod pattern** (multi-app bundle, openresty as one of N
  supervised children in a luajit_base container). Supported by
  existing luajit_base architecture; not needed until a real
  app+web-server coupling case appears. See project_luajit_base_design.md.
- **Health-checked registration publish**.
- **NATS websocket live updates** from gateway to browser (instead
  of htmx polling every 2s).
- **Auth** on the gateway — none for v1, assume trusted LAN.
- **TLS** — none for v1.
- **Per-instance port name labeling** in the tree (e.g., "dcs_ops :ui"
  vs "dcs_ops :admin") — rendered from port.purpose + port.description.

## Non-goals

- **No app self-registration.** Apps never touch the registry.
- **No TTL.** Writes and deletes are authoritative.
- **No direct browser-to-app routing.** Everything through gateway
  reverse proxy.
- **No multi-site operator view** in v1. Each site's gateway shows
  only its own site. Cross-site aggregation is a later concern.

## Open items (minor)

- Exact htmx version to vendor.
- Gateway's 404 page when a registered container went away
  mid-session: friendly HTML or plain nginx 502.
- Whether `registered_at` in the row is RFC3339 text or epoch ns
  (DCS uses epoch ns everywhere else; align).

## Resume next session

Start with **openresty_base**. Contents:

1. `Dockerfile` — FROM openresty/openresty:alpine-fat, luarocks
   install pgmoon + lua-resty-openssl, COPY resty_kb_client +
   htmx.
2. `docker_build.sh` — stages resty_kb_client from (probably)
   building_blocks/resty_kb_client/ (new dir, sibling to
   openresty_base) and htmx from a vendored tarball.
3. `resty_kb_client/` — first pass: just the pgmoon+keepalive
   wrapper and a generic query helper. ltree-path builders as
   needed. Grow the module as ops_ui adds endpoints.

Don't try to build all three containers in one session — it's too
much code. Session 2 = openresty_base + resty_kb_client skeleton.
Session 3 = dcs_ops_ui. Session 4 = DCS schema + registration
user functions. Session 5 = gateway + end-to-end.
