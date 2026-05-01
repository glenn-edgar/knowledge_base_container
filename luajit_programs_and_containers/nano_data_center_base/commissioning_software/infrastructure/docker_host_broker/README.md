# docker_host_broker

Single-host adapter that owns ALL docker daemon access and host metric
collection on behalf of `dcs.lua`'s chain-tree supervisor and any other
consumer. Written in Go, talks to docker via the SDK over
`/var/run/docker.sock` (no shell-outs), publishes state to NATS, accepts
mutations via HTTP.

## Why

`feedback_chain_tree_no_blocking_io.md`. The chain-tree walker is
single-threaded; synchronous shell-outs to docker (and curl, ss, df,
etc.) from inside chain-tree handlers block sibling columns and have
caused infra-teardown loops on WSL2. The broker is the right side of
the supervisor / worker split: chain-tree decides, broker does I/O.

## Status

**Phase 1 — scaffolding only.** Wire protocol locked at design time
(`WIRE_PROTOCOL.md`). Go code skeleton is here for review; not yet
ready to run. The chain-tree migration is Phase 1b, after the broker
reads (containers/host_metrics) work end-to-end.

See `building_blocks/observability/continue.md` for the full 6-phase
plan and current status.

## Layout

```
docker_host_broker/
├── README.md           — this file
├── WIRE_PROTOCOL.md    — locked contract (NATS subjects + HTTP routes)
├── docs/               — design notes
└── container/
    ├── Dockerfile      — multi-stage Go build
    ├── docker_build.sh — host-side build helper
    ├── go.mod, go.sum
    ├── cmd/broker/main.go   — entry point
    └── internal/
        ├── dockercli/  — Docker SDK wrappers (containers, stats, network, IP)
        ├── hoststats/  — /proc, /sys, df readers
        ├── state/      — in-memory cache + delta detection
        ├── natspub/    — NATS publishing (heartbeat, snapshot, delta, stats)
        └── httpapi/    — HTTP handlers for /v1/health, /v1/state/*, /v1/cmd/*
```

## Build

```bash
cd container
./docker_build.sh
```

Image: `nanodatacenter/docker-host-broker:latest`

## Run

See `WIRE_PROTOCOL.md` § Bootstrap for the canonical `docker run`
incantation. Default ports: HTTP `127.0.0.1:9100`, no listener on
NATS (broker is a NATS *client*, not server).

## Environment variables

| var | default | meaning |
|---|---|---|
| `SITE` | `moonbase.alpha` | Site portion of NATS subject namespace |
| `NATS_URL` | `nats://nats-js-ram:4222` | NATS server URL |
| `HTTP_ADDR` | `127.0.0.1:9100` | HTTP listener address |
| `UNIX_SOCKET` | `/run/docker_host_broker.sock` | Unix socket path (creates if absent) |
| `DOCKER_HOST` | `unix:///var/run/docker.sock` | Docker daemon socket |
| `POLL_CONTAINERS_S` | `5` | Container snapshot cadence |
| `POLL_HOST_METRICS_S` | `1` | Host metrics cadence |
| `POLL_STATS_S` | `5` | Container stats cadence |
| `HEARTBEAT_S` | `1` | Broker heartbeat cadence |

## Out-of-scope

See `WIRE_PROTOCOL.md` § "Out of scope for v1".
