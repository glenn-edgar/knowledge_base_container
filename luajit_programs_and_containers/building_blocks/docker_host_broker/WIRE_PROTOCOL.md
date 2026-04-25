# docker-host-broker — wire protocol v1 (DRAFT)

Source of truth for the contract between the broker and its consumers
(primarily `dcs.lua` chain-tree, secondarily `observability_01` and any
other surveyor that needs container/host state). Both NATS pub/sub
(normal path) and HTTP/unix-socket (mutations + fallback) are
specified here.

**Status:** Draft, locked at design time. Any change to this file
requires explicit review — both broker and consumer code depend on
this contract.

## Naming convention

NATS subjects mirror the KB ltree path convention
(`feedback_telemetry_routing.md`): `system.site.<site>.<...>`. The
broker's namespace is `system.site.<site>.docker_broker.*`. `<site>` is
read from `bootstrap.config.site` and from the `SITE` env var; they
must agree.

All timestamps are float seconds since Unix epoch (`time.time()`-like).
Sequence numbers are monotonically increasing uint64; consumers use them
to detect gaps and re-fetch.

## Pg mirror (parallel to NATS, for bare-LuaJIT consumers)

When `PG_DSN` is set, the broker mirrors every published payload into
`knowledge_base_status` (`UPSERT` semantics) at the following paths:

| Path | Mirrors NATS subject | Cadence |
|---|---|---|
| `system.site.<site>.docker_broker.containers.KB_STATUS_FIELD.snapshot`   | `containers.snapshot`        | poll cadence (5s default) |
| `system.site.<site>.docker_broker.containers.KB_STATUS_FIELD.stats`      | `containers.stats`           | stats cadence (5s default) |
| `system.site.<site>.docker_broker.heartbeat.KB_STATUS_FIELD.last`        | `heartbeat`                  | heartbeat cadence (1s default) |
| `system.site.<site>.docker_broker.host_metrics.KB_STATUS_FIELD.snapshot` | `host_metrics.snapshot`      | (when hoststats lands) |

Payload bytes are **identical** to the NATS envelope JSON — same Go
encoder, same field shape — so chain-tree consumers see consistent
data regardless of source.

Rationale: bare LuaJIT (the `dcs.lua` runtime) has no native NATS
client. Writing into `knowledge_base_status` lets the existing
`kb_status` helpers (already a per-tick pg read pattern) surface
broker state without adding any new transport to dcs_host. NATS is
still the primary publication channel for any consumer that has a
NATS client (browsers, observability UI, dashboards, future cloud
aggregator).

Pg mirroring degrades gracefully — if `PG_DSN` is empty or the pg
connection fails on startup, the broker logs and continues in
NATS-only mode.

Pg writes are `INSERT ... ON CONFLICT (path) DO UPDATE` — self-healing,
no construction-time row pre-allocation required.

## Read path is NATS, not HTTP polling (LOCKED)

The supervisor (and every other normal consumer) MUST read state via
NATS pub/sub. HTTP `/v1/state/*` endpoints exist *only* as an
emergency fallback when NATS is unreachable, and for one-shot
debugging via curl. Polling HTTP at chain-tree tick rate is forbidden:
it would re-introduce per-tick I/O cost into the supervisor's hot
path, defeating the purpose of the broker.

If a consumer cannot maintain a NATS subscription (no NATS client
available, exotic environment), the consumer is the wrong shape — fix
the consumer, do not work around it by polling.

HTTP for **mutations** (`POST /v1/cmd/*`) remains the canonical path.
That is request/reply by nature and is not subject to the no-polling
rule.

## NATS subjects (broker → consumers)

### `system.site.<site>.docker_broker.heartbeat`
Cadence: every 1.0s (configurable; default 1.0s).

```json
{
  "ts": 1714098765.123,
  "seq": 1234,
  "broker_version": "0.1.0",
  "uptime_s": 3600,
  "docker_socket_ok": true
}
```

If `docker_socket_ok` is false the broker is alive but its docker client
has no recent successful call. Consumers should distinguish "broker
dead" (no heartbeat for >3s) from "broker degraded" (heartbeats arriving
with `docker_socket_ok=false`).

### `system.site.<site>.docker_broker.containers.snapshot`
Cadence: every 5.0s (configurable). Also published once on subscriber
connect, gated by JetStream "last value" if available; for v1 we rely on
the regular cadence.

Full enumeration of all containers (running and stopped) seen by the
broker.

```json
{
  "ts": 1714098765.123,
  "seq": 567,
  "containers": [
    {
      "id": "abc123def456",
      "name": "pg-vector",
      "image": "pgvector/pgvector:pg17",
      "state": "running",
      "started_at": 1714098000.0,
      "finished_at": null,
      "exit_code": null,
      "health": "healthy",
      "ports": [
        {"host_ip": "0.0.0.0", "host_port": 5432, "container_port": 5432, "proto": "tcp"}
      ],
      "labels": {},
      "ip_addresses": {"bridge": "172.17.0.5"}
    }
  ]
}
```

`state`: one of `running`, `exited`, `restarting`, `paused`, `dead`,
`created`. Matches Docker's container State.Status verbatim.

`health`: one of `healthy`, `unhealthy`, `starting`, `none` (when no
HEALTHCHECK is defined). Matches Docker's health status.

### `system.site.<site>.docker_broker.containers.delta`
Cadence: on each detected change between two consecutive polls
(state transition, exit code change, health flip, ports change, IP
change, container appeared/disappeared).

```json
{
  "ts": 1714098765.456,
  "seq": 568,
  "events": [
    {
      "kind": "state_change",
      "name": "pg-vector",
      "id": "abc123",
      "before": "exited",
      "after": "running"
    },
    {
      "kind": "appeared",
      "name": "test_app_01",
      "id": "..."
    },
    {
      "kind": "disappeared",
      "name": "old-thing",
      "id": "..."
    },
    {
      "kind": "health_change",
      "name": "pg-vector",
      "id": "abc123",
      "before": "starting",
      "after": "healthy"
    }
  ]
}
```

Consumers may use deltas as event triggers but must not rely on them
for authoritative state — use the snapshot. Deltas are best-effort.

### `system.site.<site>.docker_broker.containers.stats`
Cadence: every 5.0s (configurable; default 5.0s).

Per-container resource samples derived from `docker stats`-equivalent
SDK calls.

```json
{
  "ts": 1714098765.123,
  "seq": 234,
  "stats": {
    "pg-vector": {
      "cpu_pct": 0.08,
      "mem_rss_mb": 124.5,
      "mem_limit_mb": 8192.0,
      "disk_read_kbps": 0.0,
      "disk_write_kbps": 12.3,
      "net_rx_kbps": 1.4,
      "net_tx_kbps": 0.6
    },
    "nats-js-ram": {...}
  }
}
```

`cpu_pct` is normalized: 100% = 1 core. (Per `feedback_docker_stats_cpu_semantics.md` — 8% CPU = 0.08 cores. Don't multiply by num_cpus.)

`disk_*_kbps` and `net_*_kbps` are deltas computed by the broker against
its previous sample. First sample after broker start has all-zero rates.

### `system.site.<site>.docker_broker.host_metrics.snapshot`
Cadence: every 1.0s (configurable; default 1.0s).

Host-level metrics derived from `/proc`, `/sys`, and `df`.

```json
{
  "ts": 1714098765.123,
  "seq": 4001,
  "host": {
    "cpu_pct": 12.3,
    "load_1m": 1.2,
    "load_5m": 0.8,
    "load_15m": 0.5,
    "mem_total_mb": 16384,
    "mem_used_mb": 8192,
    "mem_free_mb": 4096,
    "mem_buffers_mb": 1024,
    "mem_cached_mb": 3072,
    "swap_used_mb": 0,
    "swap_total_mb": 4096,
    "net_rx_kbps": 102.4,
    "net_tx_kbps": 51.2,
    "disk_used_pct": {"/": 0.45, "/var/lib/docker": 0.62}
  }
}
```

## HTTP endpoints (consumers → broker)

Listening on `127.0.0.1:9100` by default. Also reachable on the bridge
network at the broker's container IP. Consumers may also use a unix
socket at `/run/docker_host_broker.sock` if mounted.

All responses are JSON with `Content-Type: application/json`.

### `GET /v1/health`

Liveness probe.

* `200 OK`: broker healthy. Body: `{"status":"healthy","docker_socket_ok":true,"uptime_s":3600}`.
* `503 Service Unavailable`: broker degraded. Body identifies which subsystem.
* No body for either response is also acceptable; consumers must rely on
  status code primarily.

### `GET /v1/state/containers`

Full snapshot for warm-start / NATS-down fallback. Body identical to
the NATS `containers.snapshot` payload, with the current sequence
number.

### `GET /v1/state/containers/<name>`

Single-container subset. 404 if not found.

```json
{
  "ts": 1714098765.123,
  "seq": 567,
  "container": { ... }   // same shape as one element of snapshot.containers
}
```

### `GET /v1/state/host_metrics`

Body identical to the NATS `host_metrics.snapshot` payload.

### `POST /v1/cmd/start`

Start an existing (created or stopped) container.

Request:
```json
{ "name": "pg-vector" }
```

Response:
* `202 Accepted`: command queued. Body: `{"name":"pg-vector","cmd_id":"<uuid>","accepted_at":1714098765.0}`.
* `404 Not Found`: container does not exist.
* `409 Conflict`: container is already running. Body: `{"error":"already_running","name":"pg-vector"}`. Idempotent semantics — chain-tree treats this as success.

The actual start may take several seconds. Consumers verify success
via the next `containers.snapshot` showing `state="running"`.

### `POST /v1/cmd/stop`

Request:
```json
{ "name": "pg-vector", "timeout_s": 10 }
```

`timeout_s` is the SIGTERM grace before SIGKILL. Default 10.

Response:
* `202 Accepted`: command queued.
* `404 Not Found`: container does not exist.
* `409 Conflict`: container is already stopped. Idempotent — treat as success.

### `POST /v1/cmd/run`

Create + start a new container in one operation.

Request:
```json
{
  "name": "pg-vector",
  "image": "pgvector/pgvector:pg17",
  "env": {"POSTGRES_PASSWORD": "...", "POSTGRES_DB": "knowledge_base"},
  "ports": [{"host_port": 5432, "container_port": 5432, "proto": "tcp"}],
  "volumes": [{"host": "/var/lib/postgresql/data", "container": "/var/lib/postgresql/data"}],
  "restart_policy": "unless-stopped",
  "labels": {},
  "network": "bridge",
  "extra_hosts": ["host.docker.internal:host-gateway"],
  "entrypoint": ["/usr/local/bin/myproc", "--flag"]
}
```

Optional fields:

* `extra_hosts`: array of `"name:ip"` strings, mapped onto docker's `--add-host`.
  DCS uses `["host.docker.internal:host-gateway"]` so app containers can reach
  pg/nats on the docker host (required on Docker Desktop, harmless on Linux).
* `entrypoint`: argv override for the image's CMD. Mirrors `docker run image arg1 arg2`.
  When omitted/empty, the image's default CMD runs.

Response:
* `202 Accepted`: container created and starting. Body includes `id` and `cmd_id`.
* `409 Conflict`: a container with that name already exists. Body: `{"error":"name_taken","name":"...","existing_id":"..."}`.

Idempotency note: chain-tree retries are expected. If the existing
container has a *different* image or differing port spec, broker
returns `409 Conflict` without action — chain-tree must `rm` first.
This is intentional: the broker does not silently replace mismatched
state.

### `POST /v1/cmd/rm`

Request:
```json
{ "name": "pg-vector", "force": false }
```

`force=true` allows removing a running container (issues stop first).

Response:
* `202 Accepted`: removal queued.
* `404 Not Found`: container does not exist. Idempotent — treat as success.
* `409 Conflict`: container is running and `force=false`.

## Cache freshness contract

Every NATS payload carries `ts`. Consumers should compute
`now - ts` and refuse to use snapshots older than a consumer-specified
window. Recommended defaults:

* Container state: refuse beyond 15s
* Container stats: refuse beyond 30s
* Host metrics: refuse beyond 10s

When fresh data is required and the cache is stale, fall back to the
HTTP `/v1/state/*` endpoints.

## Bootstrap

The broker is itself a docker container. dcs.lua bootstraps it via ONE
direct shell-out at startup (not chain-tree-mediated):

```bash
docker run -d --name docker-host-broker \
    --restart unless-stopped \
    -v /var/run/docker.sock:/var/run/docker.sock:ro \
    -v /run/docker_host_broker.sock:/run/docker_host_broker.sock:rw \
    -v /proc:/host/proc:ro \
    -p 127.0.0.1:9100:9100 \
    --net host-or-bridge \
    -e SITE=moonbase.alpha \
    -e NATS_URL=nats://nats-js-ram:4222 \
    nanodatacenter/docker-host-broker:latest
```

Once running, all subsequent docker operations route through the
broker. The chain-tree's `KILL_NON_INFRA_CONTAINERS` and `START_*_CONTAINER`
handlers become broker calls.

## Failure modes and consumer guidance

### Broker missing (no heartbeat received within 3s)

Treat as `ERR_INFRA_FAIL`-equivalent. dcs.lua may attempt one direct
shell-out restart of the broker container before escalating. This is
the only place dcs.lua shells out post-boot.

### NATS down

Broker still publishes (failed publishes are buffered up to 1000
messages, then dropped oldest). Consumers fall back to HTTP
`/v1/state/*` for reads and HTTP `/v1/cmd/*` for mutations.

### Docker socket down (broker can't reach docker daemon)

Broker stays alive, marks `docker_socket_ok=false` in heartbeat,
serves stale-but-fresh-flagged data from cache. Consumers should
distinguish broker death from broker degradation.

### Two consumers issue conflicting mutations

Broker serializes mutation requests by container name in a per-name
goroutine. Last-arrived wins; previous in-flight is allowed to
complete. No transactional semantics; chain-tree is the authority and
should not have multiple supervisors fighting.

## Versioning

This is `wire_protocol_v1`. Future changes:
* Additive: new fields in payloads, new optional request fields. Allowed without version bump.
* Breaking: removing fields, changing field semantics, changing subjects. Bumps to `v2` and broker advertises both for a transition period via a `?version=v2` query parameter on HTTP and a parallel subject namespace `system.site.<site>.docker_broker_v2.*`.

`broker_version` in heartbeat reports the broker binary's semver, not
the wire protocol version. Wire protocol version is implicit until v2.

## Out of scope for v1

* Authentication on HTTP/NATS (we trust the host network in v1; tighten in v2).
* Multi-host (one broker per host; cluster aggregation is a different layer).
* Image pull (`docker pull`); chain-tree may want this later but for now images are pulled out-of-band.
* `docker exec` / interactive sessions; not a supervisor concern.
* Logs streaming (`docker logs -f`); observability has its own log path.
