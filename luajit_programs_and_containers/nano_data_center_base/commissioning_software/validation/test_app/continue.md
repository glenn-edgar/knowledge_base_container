# test_app Container — Continuation Plan

## Session 1 (2026-04-17) — stage 2 code written, awaiting smoke test

Four-process container, all shells, built to exercise the
registration/deregistration path through node_control + the gateway.

## Processes

1. `exception_aggregator` (`start_order: 10`) — lua worker. Prints a
   heartbeat tick per second to stderr. No pg writes yet.
2. `log_aggregator` (`start_order: 20`) — lua worker, identical shape.
3. `exception_web` (`start_order: 30`) — openresty, internal port
   `8080`. Single static page at `/`.
4. `log_web` (`start_order: 40`) — openresty, internal port `8081`.

Web processes use `-p /opt/apps/<name>/ -c conf/nginx.conf -g "daemon off;"`
so the luajit-base supervisor can waitpid on them. All temp paths are
pinned under `/tmp` to decouple from the prefix dir layout.

## Build graph

```
luajit-base:latest  ←  openresty-base:latest  ←  test-app:latest
```

## Files

```
container/
├── Dockerfile                      # FROM openresty-base, COPY 4 apps, bundle
├── docker_build.sh                 # prereq check, docker build
├── run_test_app.sh                 # local dev wrapper (host networking)
├── exception_aggregator/
│   ├── main.lua
│   └── app.manifest.json
├── log_aggregator/
│   ├── main.lua
│   └── app.manifest.json
├── exception_web/
│   ├── conf/nginx.conf             # listen 8080
│   └── app.manifest.json
└── log_web/
    ├── conf/nginx.conf             # listen 8081
    └── app.manifest.json
```

## Smoke test (awaiting user run)

```bash
cd container/
./docker_build.sh       # builds nanodatacenter/test-app:latest
./run_test_app.sh       # runs with host networking; ctrl-C to stop
```

Expected in stderr (interleaved):
- Supervisor sync → setup → monitor transition.
- Four spawn lines for the four apps in start_order.
- Per-app tick lines from the two workers.
- Openresty worker-process-started lines for the two web processes.

From another shell:
```bash
curl http://localhost:8080   # exception_web shell page
curl http://localhost:8081   # log_web shell page
```

`docker stop test_app` should trigger the supervisor's
request_shutdown → teardown path; all four children SIGTERMed, then
SIGKILLed if they don't exit (openresty exits cleanly on SIGTERM; lua
workers ignore the signal since their nanosleep loops don't install
a handler, so they take the SIGKILL path — matches the dummy_app
behavior observed in luajit_base stage 1).

## What this validates

- openresty-base image works end-to-end (openresty binary resolvable,
  nginx.conf parsing OK, port bind OK).
- Supervisor happily manages heterogeneous processes (lua + openresty
  mixed in one controller.db).
- Two openresty instances in one container coexist on different
  internal ports.

## What this does NOT validate yet

- Registration / deregistration through node_control (stages 3-5).
- External port mapping (will be tested with node_control's
  `docker run -p ext:int` in stage 6).
- Gateway reverse proxy routing (stage 7+).

## Next session

Stage 3 — extend the DCS DSL (`definitions.lua`, `topology.lua`,
`construct_dcs_kb.lua`) with `port_spec` per image and `ports` per
placement, plus the construct-time per-CPU external-port conflict
check.
