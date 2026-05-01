# docker_host_broker — continue plan

Last updated **2026-04-25 (evening)**. Phase 4 (broker-active HTTP
probes) Go implementation complete; soak deferred to next session.

## Status: Phase 4 CODE COMPLETE — broker not yet rebuilt/redeployed

The running broker container is still `:phase2` (commit `fe90ac67`).
The Phase 4 code is committed (`bc30cc9c`) but lives only on disk;
the running cluster has not been touched.

### Phase 4 summary (2026-04-25 evening)

New `internal/probes/` package implements broker-active HTTP health
probes against container internal IPs (NOT through host port forwarding
— bypasses vpnkit, which is the source of the false-positive cascade
documented in `feedback_watchdog_vpnkit_false_positives`).

Three modules, ~510 LoC + ~430 LoC tests, 19/19 green:

* `router.go` — at startup the broker inspects its own container,
  caches the set of networks it sits on. `PickIP` intersects with each
  container's `IPAddresses`, preferring non-`bridge` networks
  (e.g. `planner-net`) over `bridge`. Returns `"no_route"` when the
  broker shares no network with the target. Supervisors MUST treat
  `no_route` as "skip", never as "fail" (the design rule from Q2).
* `spec.go` — parses `nanodatacenter.probe.<slot>.{path,
  expect_status,interval_s,timeout_ms,internal_port}` labels off each
  container into a typed `SlotSpec` list. Defaults: 200 / 5s / 2000ms.
  Per-slot validation errors don't kill sibling slots.
* `runner.go` — keyed by container ID (not name) so `rm`+`run` cycles
  get fresh goroutines automatically. One goroutine per
  (container × slot) on the slot's interval, bounded `http.Client`.
  Aggregates cross-slot to the snapshot's single `Probe` object:
  `Ok = AND` over slots, `FailStreak = max`, `last_status` from the
  most-recent attempt, `last_err` from the worst slot only when
  `Ok=false`. Pre-probe state reports `Ok=false` so supervisors
  can't false-positive on a never-probed container.

Wired into `cmd/broker/main.go`: router built once at startup (fatal
on failure — silent absence of probes is worse than failing loud);
`runner.Reconcile` per container poll, `runner.Annotate` before each
publish. `httpapi` gained an `Annotator` interface (kept local to
keep httpapi free of the probes import) and wires the two GET state
endpoints through it.

`ContainerInfo` gained `Probe *ProbeState`; nil serializes as JSON
`null` (additive wire change per `WIRE_PROTOCOL.md` versioning rules
— no v2 bump). `natspub` and `pgwriter` inherit the new field for
free since they emit `ContainerInfo` verbatim.

`broker_version` bumped `0.1.0-phase1b` → `0.2.0-phase2` (commit
`fa52f8b9`). The bump cleared the cosmetic mismatch flagged in the
morning continue.md.

### Phase 2 summary (earlier today, morning)

Migrated all docker mutations from chain-tree shell-outs to broker
HTTP. New `internal/dockercli` mutation methods (`StartContainer`,
`StopContainer`, `RunContainer`, `RemoveContainer`); httpapi handlers
for `/v1/cmd/{start,stop,run,rm}` with idempotent 409 responses.
Wire protocol additions: `extra_hosts[]` and `entrypoint[]` to
`/v1/cmd/run`. Validated end-to-end before the Phase 3a/3b iterations.

### Phase 1c / 1b summary (yesterday)

Read-side migration. `kv-bridge` turned out to be the wrong primitive
for dcs_host (one-way MQTT→NATS-KV write helper, no Lua API), so the
broker double-publishes: NATS for fast pub/sub consumers, pg
`knowledge_base_status` for bare-LuaJIT dcs.lua via existing
`kb_status` helpers. Validated end-to-end.

## Open security/hardening items (deferred)

* **Container runs as root** to access host's `/var/run/docker.sock`
  (mode 660 root:docker). Hardening: pass host docker GID via
  `--group-add` at runtime and restore the unprivileged broker user
  in the Dockerfile.

## Next session — Phase 4 integration test

Goal: turn probes ON for one container, validate end-to-end against
the live cluster.

### Step order (also in observability/continue.md)

1. Pick the guinea pig — recommend `test_app_01.exceptions_ui`.
2. Add `/health` to test_app's shell server (~3 lines of code).
3. Add `probe = { path = "/health" }` to that slot in `definitions.lua`.
4. Rebuild bootstrap.db.
5. Tag running broker as `:phase2-rollback`, then build + redeploy
   the Phase 4 broker.
6. Run the five live tests in
   `observability/continue.md` § "Live tests (in order)":
   health observation → soak baseline → stuck-process →
   no-route → broker-outage gate.

### Expected wire output once probe is configured

`SELECT data->'containers' FROM knowledge_base_status WHERE path =
'system.site.moonbase.alpha.dcs.docker_broker.containers.KB_STATUS_FIELD.snapshot';`

For `test_app_01`:
```json
{
  "name": "test_app_01",
  "state": "running",
  "probe": {
    "configured": true,
    "ok": true,
    "fail_streak": 0,
    "last_ok_ts": 1714098765.123,
    "last_probe_ts": 1714098765.456,
    "last_status": 200,
    "last_err": null,
    "route": "planner-net"
  }
}
```

For every other container: `"probe": null`.

### Expected WATCHDOG log on stuck-process test

```
WATCHDOG test_app_01 broker probe stuck (streak=3, last=context deadline exceeded) fail=1/3
WATCHDOG test_app_01 broker probe stuck (streak=4, last=context deadline exceeded) fail=2/3
WATCHDOG test_app_01 broker probe stuck (streak=5, last=context deadline exceeded) fail=3/3
WATCHDOG test_app_01 hung -- restarting (broker probe stuck (streak=5, last=...))
```

(The double-counting on display reflects the existing
`st.fail_count` machinery wrapping the broker's `fail_streak` —
intentional belt-and-suspenders, see commit message of `50269ea4`.)

## Bootstrap recipe (Phase 2; rollback target)

```bash
docker run -d --name docker-host-broker \
  --restart unless-stopped \
  --network bridge \
  -v /var/run/docker.sock:/var/run/docker.sock:ro \
  -p 127.0.0.1:9100:9100 \
  -e SITE=moonbase.alpha.dcs \
  -e NATS_URL=nats://nats-js-ram:4222 \
  -e PG_DSN="host=pg-vector port=5432 user=gedgar dbname=knowledge_base password=ready2go sslmode=disable" \
  -e HTTP_ADDR=0.0.0.0:9100 \
  --link nats-js-ram:nats-js-ram --link pg-vector:pg-vector \
  nanodatacenter/docker-host-broker:latest
```

(SITE must match `bootstrap.config.site` exactly: `moonbase.alpha.dcs`,
not `moonbase.alpha`.)

## Build environment notes

The user's host: WSL2 Ubuntu, Docker Desktop. Go 1.24+ is installed at
`/usr/local/go/bin/go` — useful for fast iteration without going
through `docker build`. Used today for all 19 unit tests.

Dockerfile uses a multi-stage build (`golang:1.22-alpine` →
`alpine:3.19`). Final image ~25-30MB.

### Module pin: docker/go-connections@v0.5.0

The Docker SDK references `sockets.DialPipe` from go-connections;
v0.6+ removed the symbol. Pinned to v0.5.0 in `go.sum`. Don't run an
unconstrained `go get -u` — it'll re-bump go-connections and break
the build.

## Testing approach

Per `feedback_user_driven_testing.md`: assistant analyzes pasted logs
when the user runs the integration tests. Next session: assistant
produces clear smoke-test steps; user runs them on the live cluster;
assistant reads results.

## File map at end of this session

```
docker_host_broker/
├── README.md
├── WIRE_PROTOCOL.md                  v1, with "Broker-active HTTP probes (Phase 4)" section
├── continue.md                       this file
├── docs/
└── container/
    ├── Dockerfile                    multi-stage (golang:1.22 → alpine)
    ├── docker_build.sh
    ├── go.mod                        deps: docker SDK, nats.go, pgx
    ├── cmd/broker/main.go            wired with probes router/runner
    └── internal/
        ├── dockercli/                ContainerInfo + ProbeState; mutations Phase 2
        ├── hoststats/                stubs (parallel work item; broker functional w/o it)
        ├── state/                    cache + delta detection + per-container stats rates
        ├── natspub/                  NATS publisher (5 subjects)
        ├── pgwriter/                 knowledge_base_status mirror
        ├── httpapi/                  /v1/health, /v1/state/*, /v1/cmd/* + Annotator hook
        └── probes/                   NEW: router + spec + runner + tests
```
