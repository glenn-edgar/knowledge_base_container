# observability + DCS — continue plan

Last updated **2026-04-25** end of session. The session ran the broker
Phase 2 cutover from design through validated soak, plus a corrective
Phase 3b after Phase 3a's first attempt revealed a destructive race
in the verify-trip cascade.

## Location of this file

`~/knowledge_base_assembly/luajit_programs_and_containers/building_blocks/observability/continue.md`

A second handoff with broker-internal detail lives at:
`~/knowledge_base_assembly/luajit_programs_and_containers/building_blocks/docker_host_broker/continue.md`
(read after this one if you'll be editing broker Go code.)

## Cluster status at session end

* All 4 infra containers Up and healthy: `pg-vector`, `nats-js-ram`,
  `mosquitto-ram-ws_main`, `kv-bridge`.
* `docker-host-broker` Up, running the Phase 2 image
  (`nanodatacenter/docker-host-broker:latest` = `:phase2`). Rollback
  tag preserved as `:phase1c-rollback` (sha `19b98ef5c035`).
* Two `dcs.lua` processes alive (cpu_01 master, cpu_02 slave),
  cpu_01 `sys_ready=true`, cpu_02 `sys_ready=false` (slave's steady
  state — see history: 66,398/66,398 lines false since 2026-04-18).
* Five app containers running: `observability_01`, `dcs_console_01`,
  `robot_manager_01`, `test_app_01`, `ros_mission_planner_ii_01`.

## What landed this session — 8 commits since `1e041067`

```
4f46039f  dcs_host: VERIFY_SYSTEM_CONTAINERS_HEALTHY reacts to confirmed bad state only (phase 3b)
0c0982a7  Revert "dcs_host: hysteresis on VERIFY_SYSTEM_CONTAINERS_HEALTHY (phase 3a)"
53907825  [reverted by 0c0982a7] phase 3a hysteresis -- N=3 was misleadingly small
9247f6df  dcs_host: log "broker run" not "docker run" in respawn_and_log
a4ca295f  dcs_host: route docker mutations through broker (phase 2)
fd7ccf12  docker_host_broker: phase 1b/1c baseline (Go scaffolding + read-side)
fe90ac67  docker_host_broker: phase 2 -- real start/stop/run/rm mutations
f98cf74b  construct_log_store: lift auto_health gap floor to 10s / 6 missed samples
```

## Phase 2 — broker mutations (DONE)

Full chain-tree-side cutover from `docker.run_from_spec` and friends
to `broker_client.run/stop/start/rm` over HTTP. Bootstrap model is
**Option B** (broker as platform infra, `--restart=unless-stopped`,
DCS treats it as a peer of dockerd; DCS does zero shell-outs in the
steady-state hot path).

New chain-tree-side modules in `runtime/dcs_host/`:

* `http_client.lua` (~55 LoC): synchronous JSON over `luasocket.http`,
  three-valued return `(status, body, err)` separating transport
  failure from HTTP error.
* `spec_adapter.lua` (~110 LoC): catalog-spec → wire-protocol RunSpec.
  Resolves `env_required` from `os.getenv`, expands `~` in volume
  host paths, normalizes the catalog's three port-record shapes,
  injects the `nanodatacenter=true` discovery label and the
  `host.docker.internal:host-gateway` extra_host.
* `broker_client.lua`: extended with `run/stop/start/rm` mutation
  methods. The `(ok, info_or_err)` shape collapses wire-protocol
  202/404/409 outcomes including the idempotent `already_running`,
  `already_stopped`, and `name_taken` collisions.

Wire-protocol additions: `extra_hosts[]` and `entrypoint[]` to
`/v1/cmd/run` (parity with the prior `docker.run_from_spec`). See
`docker_host_broker/WIRE_PROTOCOL.md`.

Migrated handlers in `user_functions.lua`:

* `start_container`/`stop_container` (sys infra)
* `launch_assignment` (used by START + RECONCILE + WATCHDOG respawn)
* `START_ASSIGNED_CONTAINERS` is_running gate
* `STOP_ASSIGNED_CONTAINERS`
* WATCHDOG hung-restart kill
* `APPLY_MAINTENANCE_TRANSITIONS` (both legs)

`KILL_NON_INFRA_CONTAINERS` intentionally still a no-op — see
`feedback_kill_non_infra_contract`. Promoting it requires
container-side cooperative pause first; that's Phase 6 below.

## Phase 3a — REVERTED hysteresis attempt (lesson captured)

Tried `N=3 consecutive failures before trip` on
`VERIFY_SYSTEM_CONTAINERS_HEALTHY`. Failed because:

1. The verify column ticks at **1 second**, not 5. So N=3 = 3s
   tolerance, far shorter than typical broker restart times (5-15s).
2. The cascade itself has a **destructive race**: after the trip,
   `teardown_st` runs and calls `broker_client.stop` on every infra
   container. If broker recovers DURING teardown, the queued stops
   succeed and infra goes down. Test on 2026-04-25 14:55 hit this:
   pg/nats/mqtt/kv-bridge all stopped, broker `--link pg-vector`
   broke, manual recovery needed.

Lesson saved: `feedback_broker_outage_threshold` (5s window =
hb_fresh_window_s, not 15s).

## Phase 3b — confirmed-bad-state-only verify (DONE, the fix)

Replaced 3a with the architectural fix: `VERIFY_SYSTEM_CONTAINERS_HEALTHY`
now returns false **only when the broker reports fresh data showing
a container down**. Stale broker data → return true, be quiet.
Refresh failure → return true, be quiet. The supervisor reacts only
to confirmed bad state, never to absence of state.

Validated:
* 12s broker outage → 0 trips, all infra Up, sys_ready=true throughout.
* 60s broker outage → 0 trips, all infra Up, sys_ready=true throughout.
  ~59 "staying quiet" log lines, transitioning from "heartbeat stale"
  (T+5s) to "snapshot stale" (T+15s).
* Confirmed bad state path still works: test 8c earlier (docker stop
  test_app_01) was correctly detected and respawned via RECONCILE.

Per-container verifies (`VERIFY_PG/NATS/MQTT/KV_BRIDGE` via
`is_running_verify`) intentionally NOT changed — those run during
sys_sm sync_st boot where fail-closed-on-broker-outage is correct.

## Trade-off accepted in 3b

If the broker is **permanently** down, sys_sm stays in `sys_ready=true`
indefinitely with no escalation. That's acceptable — operators see
broker death via its own `/v1/health` endpoint, and the alternative
(periodic teardown attempts that can't succeed because teardown also
goes through broker) is strictly worse. See commit message of
`4f46039f` for full reasoning.

## Outstanding small item

`broker_version` string in `internal/state/status.go` still reads
`0.1.0-phase1b` even though we're running phase 2 code. Cosmetic
only — surfaces in `/v1/health` response and broker startup log.
Bump to `0.2.0-phase2` whenever convenient.

---

## Next session — agreed roadmap

User agreed to this sequence at session end:

### 1. Phase 4 — broker-driven HTTP probes (~1+ hour)

Currently DCS WATCHDOG uses broker container state only
(state/health from `docker inspect`). That catches dead/exited
containers but misses "running but stuck" — a container whose
process is up but not responding on its port. Original WATCHDOG
used curl HTTP probes from dcs.lua side; that was traded away in
Phase 1c because vpnkit-flaky 127.0.0.1 probes caused false
positives (see `feedback_watchdog_vpnkit_false_positives`).

Phase 4 puts the HTTP probe **inside the broker**, hitting each
container's internal bridge IP (which doesn't go through vpnkit).
Broker publishes per-container health to NATS + pg-mirror. dcs.lua
reads it like any other broker state.

Implementation hints:
* Container internal IP is already exposed in `ContainerInfo.IPAddresses`
  (per-network map, see `internal/dockercli/dockercli.go`).
* Need a probe spec: which containers to probe, on what port + path,
  what cadence (default 5-10s). Probably extend the catalog
  `definitions.lua` entry shape.
* Goroutine per probed container; bounded http.Client; timeout 2-3s.
* Publish to `system.site.<S>.docker_broker.containers.health` (new
  subject) or fold into existing snapshot.

### 2. Bump broker_version (5 min)

`internal/state/status.go` — change `0.1.0-phase1b` to something
truthful. Could be folded into the same Phase 4 commit.

### 3. Fortify luajit-base container controller (Phase 6)

The chain-tree controller running INSIDE each app container (managed
by luajit-base supervisor) needs hardening against:

* **Infra container loss** — pg-vector / nats-js-ram dies during
  steady operation. Today: containers using libpq just see
  connection errors and either retry or crash. They should
  cooperatively pause workers and wait for re-sync.
* **Missing host-process heartbeat** — dcs.lua dies or stops
  publishing. Today: app containers keep running their workload.
  They should pause and wait, in line with
  `feedback_kill_non_infra_contract` (no destructive force-kill;
  controller-side graceful pause).

This is the prerequisite that, once landed, unlocks the
`KILL_NON_INFRA_CONTAINERS` host-side handler from "no-op" to
"graceful host-side coordination". See
`feedback_kill_non_infra_contract` for the contract shape.

Specific work likely:
* Add a "sync lost" state to the luajit-base controller chain-tree.
* Add a worker-pause primitive that supervised app processes can
  poll (or be signaled with).
* Add a per-app heartbeat from app workers to controller, so the
  controller knows when a worker has acked the pause.
* Define recovery semantics: how does the controller decide sync
  is back? Probably: pg connection re-established + dcs heartbeat
  fresh.

### 4. Restructure KB_LOG construction DSL — tree, not list (Phase 7)

The current KB_LOG construction in `construct_kb/construct_log_store.lua`
is essentially a flat list of `add_log(name, opts, body)` calls per
parent KB. The user wants log streams organized as a tree, so
hierarchical observability views (drill-down) become natural.

Specific work likely:
* Decide tree shape — by subsystem? by container? by log_kind?
* Migrate existing call sites in `construction/subsystems/*.lua`
  to the new shape.
* Keep the existing pg ltree storage; the change is at the DSL
  surface, not the storage.

This is a larger refactor — touches every `add_log` call site and
likely the construct script DSL itself.

---

## Files to read at session start

1. This file.
2. `building_blocks/docker_host_broker/continue.md` — broker-side
   detail, especially before Phase 4 work.
3. `building_blocks/docker_host_broker/WIRE_PROTOCOL.md` — the
   contract; Phase 4 probably extends this.
4. The post-Phase-3b `runtime/dcs_host/user_functions.lua` —
   current handler shapes.

## Recovery / rollback notes

If anything goes sideways with the running broker, the rollback
tag is preserved:

```bash
docker stop docker-host-broker && docker rm docker-host-broker
docker tag nanodatacenter/docker-host-broker:phase1c-rollback \
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

DCS code rollback (3 commits):
`git revert 4f46039f a4ca295f fe90ac67`
(skips `9247f6df` cosmetic + `f98cf74b` auto-health gap fix —
those are independent and fine to keep.)
