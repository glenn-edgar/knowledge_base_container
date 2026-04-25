# docker_host_broker — continue plan

Created **2026-04-24** as Phase 1 of extracting blocking I/O from the
chain-tree supervisor (see `building_blocks/observability/continue.md`
for the architectural rationale).

## Status: Phase 1c COMPLETE — read-side fully migrated, system soaked clean for 7 min

### Phase 1c summary (2026-04-25)

`runtime/dcs_host/broker_client.lua` reads broker state from
`knowledge_base_status` via the existing pg connection. Migrations in
`runtime/dcs_host/user_functions.lua`:

* `is_running_verify` factory (VERIFY_PG/NATS/MQTT/KV_BRIDGE/KV_BRIDGE_HEALTHY)
* `VERIFY_SYSTEM_CONTAINERS_HEALTHY`
* `RECONCILE_ASSIGNED_CONTAINERS` (read side; respawn still shells out)
* `WATCHDOG_CHECK_ASSIGNED_CONTAINERS` (rewritten — broker state, not curl probes)
* `SAMPLE_CONTAINERS_VIA_DOCKER_STATS`

**Soak result:** 7 minutes, 14 ticks × 30s, `trips=0 hb_stale=0
watchdog_kicks=0`. All containers stayed up. The self-destructive
heartbeat-stale → infra-teardown loop is gone.

Mid-session lesson learned: broker `SITE` env must match DCS's
`bootstrap.config.site` exactly — for this deployment that's
`moonbase.alpha.dcs`, not `moonbase.alpha`. Bootstrap recipe at
the bottom of this file reflects the correct value.

### Phase 1b summary (earlier)

### Pg-mirror addendum (2026-04-24, after kv-bridge survey)

`kv-bridge` turned out to be the wrong primitive (one-way MQTT→NATS-KV
write helper, no Lua API). dcs_host has no native NATS or MQTT client
— only `pg_connector.lua`. So the broker now **double-publishes**:
NATS for fast pub/sub consumers, pg `knowledge_base_status` for
bare-LuaJIT dcs.lua via the existing `kb_status` helpers.

Lands as `internal/pgwriter/` (177 lines, pgx-backed). When `PG_DSN`
is set, every poll cycle UPSERTs the same JSON envelope into pg under
ltree paths matching the NATS subjects. Validated end-to-end: rows
materialize, dcs.lua read pattern (`SELECT data FROM
knowledge_base_status WHERE path = ...`) returns the expected JSON.

WIRE_PROTOCOL.md § "Pg mirror" documents the contract.



This session landed:
1. Wire protocol locked at design time (`WIRE_PROTOCOL.md`). NATS-only
   for reads (HTTP polling forbidden in supervisor hot path). HTTP for
   mutations + emergency fallback.
2. Directory + Dockerfile (Go 1.25 alpine multi-stage) + Go module.
3. **`internal/dockercli/` real** — Docker SDK wrapper: ListContainers,
   InspectContainer, Stats, Ping. Mutations (Phase 2) stubbed.
4. **`internal/state/` real** — in-memory cache + delta detection +
   per-container stats rate computation + Status atomic counters.
   9/9 unit tests pass.
5. **`internal/natspub/` real** — `nats.go`-backed publisher; connects
   with reconnect-forever, publishes 5 subjects (heartbeat, snapshot,
   delta, stats, host_metrics), drains gracefully on close.
6. **`internal/httpapi/` real (read side)** — `/v1/health`,
   `/v1/state/containers`, `/v1/state/containers/<name>`,
   `/v1/state/host_metrics`. Mutation handlers return 501 (Phase 2).
7. **`cmd/broker/main.go` real wiring** — four goroutine loops
   (container poll, stats poll, heartbeat, http server) with
   bounded shutdown.
8. **Container image builds clean** — `nanodatacenter/docker-host-broker:latest`
   at 28.7 MB.
9. **End-to-end smoke test passed against live cluster:**
   - HTTP `/v1/health` returns healthy with `docker_socket_ok=true`.
   - HTTP `/v1/state/containers` returns 10 containers (broker observing itself + all infra) with full wire-protocol fields.
   - NATS `heartbeat` firing 1Hz with seq increment and uptime.
   - NATS `containers.snapshot` carrying full per-container detail across both `bridge` and `planner-net` networks.
   - NATS `containers.stats` showing sane CPU/mem/disk/net rates.
   - `containers.delta` correctly fires `state_change` + `exit_code_change` on `docker stop kv-bridge` / `docker start kv-bridge`.

What's NOT done:
1. `internal/hoststats/` — `/proc` readers. Stubs only. Parallel work
   item; broker functional without it.
2. Phase 2: mutations (`/v1/cmd/start`, `/stop`, `/run`, `/rm`).
3. **Phase 1c**: chain-tree consumer code. The chain-tree still does its
   shell-outs and is currently OFF (per honest-broken state policy).

## Open security/hardening items (deferred)

* **Container runs as root** to access host's `/var/run/docker.sock`
  (mode 660 root:docker). Hardening: pass host docker GID via
  `--group-add` at runtime and restore the unprivileged broker user
  in the Dockerfile.

## Next-session priority order (Phase 2 — mutations through broker)

Goal: zero docker shell-outs from the chain-tree walker. The remaining
shell-outs are all mutation paths: starting/stopping/running/removing
containers. They go through new HTTP endpoints on the broker.

**Estimated: ~4 hours of focused work.**

### Decision required at session start: broker bootstrap model

* **Option A** — DCS-managed broker. New `START_DOCKER_HOST_BROKER`
  one-shot in sys_sm `launch_st` (before sync_st). dcs.lua does ONE
  direct shell-out at boot to `docker run docker-host-broker`, then
  every other docker mutation routes through HTTP. Symmetric: DCS
  owns the broker's lifecycle.
* **Option B** — broker as platform infra. Broker has
  `restart=unless-stopped`, you start it once after host boot, DCS
  treats it as a peer of the docker daemon. Cleaner; DCS has nothing
  to manage. Broker death has to be handled by some other watchdog
  (or host-level systemd).

Recommendation: **Option B for Phase 2** (simpler). Revisit later if
operational story needs it.

### Phase 2 implementation order

1. **Broker side — `internal/dockercli/`**: real implementations of
   `StartContainer`, `StopContainer`, `RunContainer`, `RemoveContainer`
   (currently `ErrNotImplemented` stubs). Use Docker SDK's container
   package. ~150 LoC.

2. **Broker side — `internal/httpapi/` mutation handlers**: replace
   the four 501 stubs (`handleCmdStart` / `handleCmdStop` / `handleCmdRun`
   / `handleCmdRm`) with real bodies. Parse JSON request, dispatch to
   dockercli, return `202 Accepted` on queued, `409 Conflict` on
   idempotent already-state, `4xx` on bad input. ~100 LoC. Wire
   protocol already specifies the request/response shapes
   (`WIRE_PROTOCOL.md` § HTTP endpoints).

3. **Broker side — idempotency**: `start` already-running → 409 with
   `error="already_running"` (chain-tree treats as success); `stop`
   already-stopped → 409 with `error="already_stopped"`; `run`
   name-already-exists → 409 with `error="name_taken"` and existing
   ID. Closed verb set keeps this small.

4. **Chain-tree side — HTTP client**: new dcs_host module
   (`http_client.lua`?) using `luasocket.http`. Synchronous is fine —
   mutations happen rarely (4 starts at sys_sm sync_st, 4 stops at
   teardown_st, plus per-assignment in node_sm). Block time during a
   POST is acceptable in those paths because they're already slow
   inherently. ~50 LoC.

5. **Chain-tree side — broker_client mutation methods**: extend
   `broker_client.lua` with `run(http, spec)`, `stop(http, name)`,
   `start(http, name)`, `rm(http, name)`. Each translates Lua spec →
   wire-protocol JSON, POSTs, parses response.

6. **Chain-tree side — spec adapter**: bootstrap.db has Lua container
   specs (image, env, ports, volumes, restart_policy, labels, network).
   Convert to the JSON shape the broker expects. Edge cases: `~`
   expansion in volume host paths; secret env resolution via
   `os.getenv`. ~50 LoC.

7. **Migrate handlers** in `runtime/dcs_host/user_functions.lua`:
   * `START_PG_CONTAINER` / `START_NATS_CONTAINER` /
     `START_MQTT_CONTAINER` / `START_KV_BRIDGE_CONTAINER`
     (sys_sm sync_st)
   * `STOP_*` counterparts (sys_sm teardown_st)
   * `START_ASSIGNED_CONTAINERS` (node_sm setup_st)
   * `STOP_ASSIGNED_CONTAINERS` (node_sm teardown_st)
   * `respawn_and_log` (used by RECONCILE + WATCHDOG)
   * `KILL_NON_INFRA_CONTAINERS` (launch oneshot — once at boot,
     migration optional)
   * `APPLY_MAINTENANCE_TRANSITIONS`

8. **Test plan**:
   * Cold start: bring up broker manually, then DCS. Confirm sys_sm
     sync_st starts pg/nats/mqtt/kv-bridge through broker HTTP.
   * Soak: 10+ minutes, 0 trips, 0 ERR_INFRA_FAIL on idempotent retries.
   * Deliberate destruction: `docker stop nats-js-ram` mid-soak.
     RECONCILE detects via broker; respawn via broker HTTP; container
     comes back up.
   * Clean shutdown: SIGTERM dcs.lua. Teardown_st stops infra in
     reverse order through broker.
   * Broker death: kill broker mid-operation. dcs.lua's verifies start
     failing closed (broker stale). Restart broker. Verifies recover.

### After Phase 2

Chain-tree shells out NOWHERE for docker. Only one shell-out remains
in the whole system: the host-level `docker run docker-host-broker`
at boot. Phase 3 (heartbeat coroutine) and Phase 4 (HTTP probes via
container internal IP) follow.

## Bootstrap recipe (validated this session)

```bash
docker run -d --name docker-host-broker \
  --restart unless-stopped \
  --network bridge \
  -v /var/run/docker.sock:/var/run/docker.sock:ro \
  -p 127.0.0.1:9100:9100 \
  -e SITE=moonbase.alpha \
  -e NATS_URL=nats://nats-js-ram:4222 \
  -e HTTP_ADDR=0.0.0.0:9100 \
  --link nats-js-ram:nats-js-ram \
  nanodatacenter/docker-host-broker:latest
```

(`--link` is legacy but works on the default bridge network. Once
nats-js-ram joins planner-net or a dedicated network, switch to
`--network planner-net` and drop `--link`.)

After Phase 1b, dcs.lua's chain-tree shells out only at boot (to
`docker run` the broker itself). Phase 2 (mutations) follows.

## Build environment notes

The user's host: WSL2 Ubuntu, Docker Desktop. Go 1.24+ IS installed
on the host (`/usr/local/go`); useful for fast iteration without going
through `docker build`. Dockerfile uses a multi-stage build with
`golang:1.22-alpine` so host Go is also not required at image-build
time. Final image is `alpine:3.19` minimum to keep size down (~25MB
target).

### Module pin: docker/go-connections@v0.5.0

The Docker SDK (v25 and v27) references `sockets.DialPipe` from
`go-connections`. v0.6+ removed the symbol. Fix: pin go-connections
to v0.5.0 (already done in go.sum). Don't run an unconstrained
`go get -u` — it'll re-bump go-connections and break the build.

## Testing approach

Per `feedback_user_driven_testing.md`: assistant analyzes pasted logs
when the user runs the integration tests. So next session, after
broker code is in place: assistant produces unit tests + clear smoke
test script; user runs them; assistant reads results.

Per `feedback_no_band_aid_over_architecture.md`: do not ship Phase 1
half-done. If broker's read path doesn't work end-to-end, do not put
it in the chain-tree's path. Better to leave dcs.lua off and infra
running unsupervised than to inject a half-baked dependency.

## File map at end of this session

```
docker_host_broker/
├── README.md                       written
├── WIRE_PROTOCOL.md                written
├── continue.md                     this file
├── docs/                           empty
└── container/
    ├── Dockerfile                  written (multi-stage, golang:1.22 → alpine)
    ├── docker_build.sh             written
    ├── go.mod                      written (deps: docker SDK, nats.go)
    ├── cmd/broker/main.go          written (skeleton; not functional)
    └── internal/
        ├── dockercli/              dir created, code TODO
        ├── hoststats/              dir created, code TODO
        ├── state/                  dir created, code TODO
        ├── natspub/                dir created, code TODO
        └── httpapi/                dir created, code TODO
```

## Open questions for next session

1. Should the broker also expose a unix socket beyond HTTP? Current
   draft says yes (mounted at `/run/docker_host_broker.sock`); might
   simplify to HTTP-only if no consumer needs it.
2. Per-CPU vs per-host: confirmed per-host (one broker total). But the
   chain-tree consumer code lives in dcs.lua which IS per-CPU. So
   broker_client.lua is per-CPU; both subscribe to the same NATS
   subjects and maintain independent caches. OK.
3. Hostname-derivation for SITE: should broker auto-detect from
   bootstrap.config or always require explicit `SITE` env? Currently
   spec says env var only. Simpler.

## Cold-start state at end of session

DCS off. All app containers removed. Infra (pg-vector, nats-js-ram,
mosquitto-ram-ws_main, kv-bridge) running unsupervised. Phase 0
band-aid was reverted; no DCS-side patches in place.

To resume:
* Don't relaunch DCS (it will loop again).
* Build the broker first, validate end-to-end, THEN migrate chain-tree.
