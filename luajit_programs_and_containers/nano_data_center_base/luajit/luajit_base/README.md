# luajit_base

Platform image for hosting LuaJIT chain-tree apps in one docker container.
Container = pod, controller = pod manager, apps = workloads.

## Layout

```
luajit_base/
├── container/            stage 1: base image source + dummy app
│   ├── Dockerfile        nanodatacenter/luajit-base:latest
│   ├── docker_build.sh   stages libs + compiles controller DSL
│   ├── supervisor/       chain-tree controller (runs in-container)
│   │   ├── entrypoint.lua        CMD target
│   │   ├── dsl.lua               compiled -> controller.json at build
│   │   ├── user_functions.lua    spawn/reap/signal/heartbeat ops
│   │   └── process_primitives.lua  libc FFI (fork/execvp/waitpid/kill)
│   ├── bundler/          image-build-time tool
│   │   ├── bundler.lua   /opt/apps/*/app.manifest.json -> controller.db
│   │   └── bundle.sh     shell wrapper (symlinked to /usr/local/bin)
│   └── dummy_app/        smoke test
│       ├── main.lua
│       ├── app.manifest.json
│       └── Dockerfile    FROM luajit-base; bundles dummy; inherits CMD
├── tests/                stage 2: standalone function tests (TBD)
├── apps/                 stage 3: real apps (openresty_web, ...) (TBD)
└── continue.md           session notes
```

## Build

```
cd container/
./docker_build.sh            # builds nanodatacenter/luajit-base:latest
docker build -f dummy_app/Dockerfile -t nanodatacenter/luajit-dummy:latest .
```

## Run (smoke test)

```
docker run --rm \
    -e CONTAINER_NAME=dummy_test -e APP_SITE=dev -e APP_CPU_ID=cpu_01 \
    -e PG_HOST=host.docker.internal -e PG_PORT=5432 \
    -e PG_DB=knowledge_base -e PG_USER=postgres -e PG_PASSWORD=... \
    nanodatacenter/luajit-dummy:latest
```

The supervisor enters `sync -> setup -> monitor`, spawns the dummy app,
and ticks at 1 Hz. `docker stop` raises SIGTERM; supervisor advances to
`request_shutdown -> teardown`, SIGKILLs stragglers, exits.

## App author contract

Apps provide a directory under `/opt/apps/<name>/` in their container
image containing at minimum:

- `main.lua` (or whatever `argv` points at) — the app entrypoint.
- `app.manifest.json`:

  ```json
  {
    "name": "<unique name>",
    "argv": ["luajit", "/opt/apps/<name>/main.lua", "..."],
    "start_order": 10,
    "restart_policy": "always",
    "kb_path": "/opt/apps/<name>/kb.sqlite"
  }
  ```

App Dockerfile pattern:

```Dockerfile
FROM nanodatacenter/luajit-base:latest
COPY <name>/ /opt/apps/<name>/
RUN /usr/local/bin/bundle_controller
```

Apps read their context from env (set by the supervisor when spawning):

- `APP_NAME`, `CONTAINER_NAME`, `APP_SITE`, `APP_CPU_ID`
- `APP_NAMESPACE` — precomputed ltree path to the app in the KB
- `APP_KB_PATH` — the `kb_path` from the manifest
- `PG_HOST/PORT/DB/USER/PASSWORD`, `NATS_URL`, `MQTT_HOST`

## Status

Stage 1 code in place; not yet built or smoke-tested. See `continue.md`
for session log.
