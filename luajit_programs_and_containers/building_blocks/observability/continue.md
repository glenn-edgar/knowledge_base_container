# observability + DCS — continue plan

Last updated **2026-04-25 (evening)** end of session. The session
brought Phase 4 (broker-active HTTP probes) end-to-end on the code
side: wire protocol locked, broker Go implementation + 19 unit tests
green, dcs.lua spec_adapter + WATCHDOG consumer landed. Cluster
unchanged; integration soak deferred to next session.

## Location of this file

`~/knowledge_base_assembly/luajit_programs_and_containers/building_blocks/observability/continue.md`

A second handoff with broker-internal detail lives at:
`~/knowledge_base_assembly/luajit_programs_and_containers/building_blocks/docker_host_broker/continue.md`
(read after this one if you'll be editing broker Go code.)

## Cluster status at session end

Unchanged from morning session — Phase 4 work was code-only, no
containers were rebuilt or redeployed.

* All 4 infra containers Up and healthy: `pg-vector`, `nats-js-ram`,
  `mosquitto-ram-ws_main`, `kv-bridge`.
* `docker-host-broker` Up running phase 2 image (`:latest` ==
  `:phase2`). The new Phase 4 broker code is committed but NOT YET
  built/deployed — that's step 1 of next session.
* `dcs.lua` running on cpu_01 (master) and cpu_02 (slave), both
  loading the post-Phase-3b WATCHDOG. The Phase 4 dcs.lua changes
  (spec_adapter probe labels + WATCHDOG probe branch) are committed
  but inert — no catalog entry has a probe block yet.
* Five app containers running: `observability_01`, `dcs_console_01`,
  `robot_manager_01`, `test_app_01`, `ros_mission_planner_ii_01`.

## What landed this session — 3 commits since `b2281839`

```
50269ea4  dcs_host: phase 4 -- consume broker probe state (dcs.lua side)
bc30cc9c  docker_host_broker: phase 4 -- broker-active HTTP probes (broker side)
fa52f8b9  docker_host_broker: phase 4 design -- wire protocol + catalog schema
```

## Phase 4 — broker-active HTTP probes (CODE DONE, SOAK PENDING)

**Why this matters.** Phase 1c traded HTTP probes for broker container-
state polling because vpnkit-flaky 127.0.0.1 probes were causing
coordinated cross-container false positives that detonated infra. That
trade lost the "running but stuck" detection capability. Phase 4
restores it by moving the probe inside the broker, hitting container
internal IPs (which don't go through vpnkit).

### Design locks (the four Q&A from morning)

| Q | Decision |
|---|---|
| Q1 | Probe spec lives in `port_spec.<slot>.probe = { path, expect_status, interval_s, timeout_ms }` in `definitions.lua`. Default-off. |
| Q2 | Broker resolves probe IP by intersecting its own networks with the container's; prefer non-`bridge`. `no_route` when no shared network. |
| Q3 | Probe state folded into `containers.snapshot` per container as a `probe` sub-object. Additive wire change, no v2 bump. |
| Q4 | Broker publishes raw `fail_streak`; WATCHDOG owns threshold (existing `WATCHDOG_FAIL_THRESHOLD = 3`). `no_route` is skipped, not failed. Gated by snapshot freshness. |

Probe configuration is carried as Docker labels on the target
container, written by DCS at `run` time (not via a separate broker
config file — keeps the broker stateless across restarts and avoids
drift).

### Broker Go (commit `bc30cc9c`)

New `internal/probes/` package, ~510 LoC + ~430 LoC tests:
* `router.go` — own-network resolution at startup, IP picker.
* `spec.go` — label parsing + validation.
* `runner.go` — per-(container × slot) goroutines, state cache,
  cross-slot aggregation.

Wired into `cmd/broker/main.go`: router built once at startup (fatal on
failure), `runner.Reconcile` per container poll, `runner.Annotate`
before each publish. `httpapi` gained an `Annotator` interface (kept
local to keep httpapi free of the probes import).

`ContainerInfo` gained `Probe *ProbeState`; nil serializes as JSON
`null` (additive wire change). natspub + pgwriter inherit the new
field for free since they emit `ContainerInfo` verbatim.

19/19 tests green. `broker_version` bumped to `0.2.0-phase2`.

### dcs.lua (commit `50269ea4`)

* `spec_adapter.lua`: walks `port_spec.<slot>.probe` blocks, emits
  `nanodatacenter.probe.<slot>.{path,internal_port,expect_status,
  interval_s,timeout_ms}` labels. Smoke-tested with three slots
  (full, none, minimal); output matches design.
* `user_functions.lua` WATCHDOG: new `elseif` branch on
  `ci.probe.fail_streak >= WATCHDOG_FAIL_THRESHOLD`. Skips when
  `probe.route == "no_route"`. Gated by `broker_client.is_fresh()`
  so stale fail_streak from a dead broker can't trigger trips.

`is_fresh()` gate intentionally asymmetric: applied only to the new
probe path, not retro-fitted to the existing health/state branches
that were soak-validated in Phase 3b. Tightening those is scope creep.

### What's INERT until next session

No catalog entry has a probe block yet. The broker also hasn't been
rebuilt + redeployed — it's still running `:phase2`. So today's commits
add the machinery without any behavior change. Turning probes on for
any container is a one-line catalog edit.

---

## Next session — Phase 4 integration test (~1-2 hours)

This is the part we explicitly deferred to the next day. Cluster-touching
work; user-driven per `feedback_user_driven_testing`.

### 1. Pick a guinea pig + add `/health` to its image

Cheapest: `test_app_01.exceptions_ui` slot. It's already a shell
process (one of test_app's four supervised workers). Adding `/health`
is ~3 lines of openresty / shell.

### 2. Catalog: add probe block to that one slot

```lua
-- definitions.lua
test_app = {
  port_spec = {
    exceptions_ui = {
      internal    = 8080,
      protocol    = "tcp",
      purpose     = "ui",
      description = "Exception aggregation viewer (shell)",
      probe = {
        path = "/health",   -- defaults: expect_status=200, interval_s=5, timeout_ms=2000
      },
    },
    ...
```

Rebuild bootstrap.db (the construct script).

### 3. Broker rebuild

```bash
cd ~/knowledge_base_assembly/luajit_programs_and_containers/building_blocks/docker_host_broker/container
# Tag the running phase2 image as a rollback before overwriting :latest
docker tag nanodatacenter/docker-host-broker:latest nanodatacenter/docker-host-broker:phase2-rollback
./docker_build.sh   # or whatever the local rebuild flow is
docker stop docker-host-broker && docker rm docker-host-broker
# Re-run via the bootstrap recipe at the bottom of this file.
```

### 4. Live tests (in order)

* **Health observation.** `SELECT data FROM knowledge_base_status WHERE
  path LIKE '%docker_broker.containers.KB_STATUS_FIELD.snapshot%';`
  should show `"probe": { "configured": true, "ok": true, ... }` for
  `test_app_01` and `"probe": null` for everyone else.
* **Soak baseline.** 10 minutes idle: 0 trips, 0 spurious WATCHDOG
  fires. Confirms inert default-off path stays inert.
* **Stuck-process test.** `docker exec test_app_01 sh -c 'kill -STOP $(pgrep -f exceptions_ui)'`.
  After ~15s broker probe times out, `fail_streak` hits 3, WATCHDOG
  fires `"broker probe stuck (streak=3, ...)"`, container respawns.
* **No-route test.** Briefly disconnect the broker from a network the
  container is on (or run a probe-configured container only on a
  network the broker isn't on). Confirm WATCHDOG does NOT trip and
  `route=no_route` is published in the snapshot.
* **Broker-outage gate test.** While probe is failing, kill the broker
  for >5s and bring it back. Confirm WATCHDOG holds during the outage
  (the `is_fresh()` gate works), then resumes once the broker recovers.

If all five pass: tag `:phase4`, expand probe blocks to the other
app slots in a follow-up commit.

---

## After Phase 4 — Phase 6 (luajit-base controller hardening)

This is the user's stated next priority after Phase 4. Carried over
from the morning's continue.md, lightly updated:

### Why

The chain-tree controller running INSIDE each app container (managed
by luajit-base supervisor) needs hardening against:

* **Infra container loss** — pg-vector / nats-js-ram dies during
  steady operation. Today: containers using libpq just see connection
  errors and either retry or crash. They should cooperatively pause
  workers and wait for re-sync.
* **Missing host-process heartbeat** — dcs.lua dies or stops
  publishing. Today: app containers keep running their workload.
  They should pause and wait, in line with
  `feedback_kill_non_infra_contract` (no destructive force-kill;
  controller-side graceful pause).

### Why now (after Phase 4)

Phase 4 gives us a way to *detect* per-container stuck state from the
host side. Phase 6 gives us the per-container *response* shape:
graceful pause when upstream infra is gone, resume when it returns.
Together they unlock promoting `KILL_NON_INFRA_CONTAINERS` from no-op
to graceful host-side coordination. That's the original
`feedback_kill_non_infra_contract` shape.

### Specific work likely

* Add a "sync lost" state to the luajit-base controller chain-tree.
* Add a worker-pause primitive that supervised app processes can
  poll (or be signaled with).
* Add a per-app heartbeat from app workers to controller, so the
  controller knows when a worker has acked the pause.
* Define recovery semantics: how does the controller decide sync
  is back? Probably: pg connection re-established + dcs heartbeat
  fresh + (optionally) broker probe ok.
* WATCHDOG strobing: needs design — the broker now provides probe
  signal; the question is what cadence the in-container controller
  should respond at, and whether the strobe is *to* dcs.lua (heartbeat
  beacon) or *from* dcs.lua (liveness ping).

### Phase 7 — KB_LOG construction DSL → tree (deferred again)

Same shape as morning: flat-list `add_log` calls in
`construct_kb/construct_log_store.lua` need to become a tree DSL for
hierarchical observability views. Big refactor, no urgency.

---

## Files to read at session start

1. This file.
2. `building_blocks/docker_host_broker/continue.md` — broker-side
   detail.
3. `building_blocks/docker_host_broker/WIRE_PROTOCOL.md` § "Broker-active
   HTTP probes (Phase 4)" — the contract you're testing.
4. `building_blocks/nanodatacenter_dcs/runtime/dcs_host/spec_adapter.lua`
   and the new WATCHDOG branch around `user_functions.lua:1297`.

## Recovery / rollback notes

If the Phase 4 broker rebuild goes sideways:

```bash
docker stop docker-host-broker && docker rm docker-host-broker
docker tag nanodatacenter/docker-host-broker:phase2-rollback \
           nanodatacenter/docker-host-broker:latest
docker run -d --name docker-host-broker --restart unless-stopped \
  --link pg-vector --link nats-js-ram \
  -v /var/run/docker.sock:/var/run/docker.sock:ro \
  -p 127.0.0.1:9100:9100 \
  -e SITE=moonbase.alpha.dcs \
  -e NATS_URL=nats://nats-js-ram:4222 \
  -e PG_DSN="host=pg-vector port=5432 user=gedgar dbname=knowledge_base password=ready2go sslmode=disable" \
  -e HTTP_ADDR=0.0.0.0:9100 \
  nanodatacenter/docker-host-broker:latest
```

(`:phase2-rollback` is what we're calling the currently-running
:latest image. Tag it before overwriting.)

DCS code rollback for Phase 4 (2 commits):
`git revert 50269ea4 bc30cc9c`
The design-only commit `fa52f8b9` is wire-protocol doc — fine to keep
even on rollback since no consumer depends on it.

The earlier-session phase 3b rollback recipe still applies if you need
to go back further.
