# LuaJIT Base Container — Continuation Plan

## Session 2 (2026-04-15) — stage 1 code written, built, smoke-tested green

End-of-session state: `nanodatacenter/luajit-base:latest` builds,
`nanodatacenter/luajit-dummy:latest` builds on top, runs end-to-end:
pg connect, spawn, supervise, interleaved app stderr with supervisor
bursts, SIGTERM → request_shutdown → SIGKILL → clean exit. 9-step
plan from session 1 complete.

### Verified lifecycle (Ctrl-C on `./run_dummy.sh`)

```
[ctrl] pg connected (dbname=knowledge_base host=127.0.0.1 port=5432)
[ctrl] loaded 1 app(s) from command_map
[ctrl] ctrl state: sync -> setup
[ctrl] spawn dummy pid=7 argv=luajit /opt/apps/dummy/main.lua
[ctrl] ctrl state: setup -> monitor
...bursts with apps=1/1 and tick N from dummy...
[ctrl] SIGTERM/SIGINT caught -- requesting teardown
[ctrl] ERR_TEARDOWN_REQUESTED -> change_state(request_shutdown)
[ctrl] SIGTERM dummy pid=7
[ctrl] ctrl state: request_shutdown -> teardown
[ctrl] SIGKILL dummy pid=7
[ctrl] connections closed
[ctrl] exiting: no active tests
```

### Gotchas resolved along the way (save for next port)

1. **`ffi.load("sqlite3")` fails on Ubuntu 24.04** — apt ships only
   `libsqlite3.so.0` (versioned). Dockerfile now resolves and symlinks
   `libsqlite3.so -> libsqlite3.so.0`. Same pattern orchestrator uses.
2. **`cjson` missing.** `cfl_json_loader.lua` requires cjson; not in
   apt packages. Staged `/usr/local/lib/lua/5.1/cjson.so` from the
   host into `prebuilt_lua_libs/` (host-built luarocks package).
3. **DBD-PostgreSQL ignores positional `host`/`port` args.** The
   wrapper we consumed from DCS (`pg_connector.lua`) calls
   `DBI.Connect("PostgreSQL", db, user, pw, host, port)` — this works
   on the host because libpq falls back to the unix socket at
   `/var/run/postgresql/`. Inside a container that socket doesn't
   exist and libpq fails opaquely with "no connection to the server".
   Fix: build a DSN string `dbname=... host=... port=...` and pass
   that as the second arg. Used elsewhere in the KB codebase
   (`kb_search.lua`) — the right form.
4. **Chain-tree boolean verify fns MUST filter `event_id`.**
   `CFL_VERIFY` calls the boolean on every event: `CFL_INIT_EVENT`
   when the column enters, `CFL_TIMER_EVENT` periodically,
   `CFL_TERMINATE_EVENT` at teardown. An unfiltered verify that
   tries to do real work (e.g. pg connect) on INIT both fails
   spuriously AND fires the error handler on the very first tick.
   Convention (see built-in `CFL_VERIFY_TIME_OUT`): do real work
   only on `TIMER_EVENT`; return `true` for INIT/TERMINATE.
   All four of our verifies now filter explicitly. DCS's verifies
   don't filter either but survive because their gated checks
   happen to succeed on first INIT call (pg is already up, heartbeat
   is fresh enough). We should probably port the filter back to
   DCS's `user_functions.lua` the next time that file is touched.
5. **`--network=host` with Docker Desktop on WSL2 works** even
   though docs imply otherwise; TCP to `127.0.0.1:5432` reaches the
   pg-vector container's published port. Confirmed via
   `bash -c 'exec 3<>/dev/tcp/127.0.0.1/5432'` returning OK.
6. **pg_17_vector.sh creates postgres as user `gedgar`**, not
   `postgres`. `run_dummy.sh` default now reflects that. Only
   matters because our smoke test runs against the dev pg-vector
   container; real deployments set `PG_USER` explicitly.

### New bits of the stage-1 harness worth remembering

- `run_dummy.sh` — wraps build+run, sources
  `~/.config/nanodatacenter/secrets.env` for password (same file
  DCS uses). `--stop` to rm, `--rebuild` to force image rebuild,
  env overrides for all pg fields + network. Default network is
  `host` so the container reaches pg at `127.0.0.1` the same way
  DCS on the host does.
- Controller state machine runs `sync → setup → monitor →
  request_shutdown → teardown`. Monitor has three parallel
  reset-loop columns: `hb_col` (5s strobe), `liveness_col` (2s
  reap+respawn), `shutdown_watch` (1s SIGTERM/SIGINT poll).
- Sample observation: supervisor burst overhead during monitor is
  ~0.5ms per tick after the first setup pass (which included the
  pg connect). Chain-tree keeps up comfortably at 1 Hz with room
  for dozens of apps.
- `REAP_AND_RESPAWN` not yet stress-tested. The dummy never exits
  while the container is up, so no crash path exercised. First
  real validation will come from killing the child manually:
  `docker exec dummy_test bash -c "kill -9 <pid>"`.

### Observations on SIGTERM behavior

Dummy app's `nanosleep` blocks on a pending signal and returns -1
with `errno=EINTR`; our dummy's `while true` loop ignores the
return value and loops back into nanosleep. So dummy doesn't exit
on SIGTERM within the 20-tick grace window; supervisor correctly
escalates to SIGKILL. For real apps this is a useful reference:
apps that want clean shutdown must install a SIGTERM handler
(set a flag, check it each loop). The supervisor is robust to
non-cooperative apps.

### Next session

1. **Commit.** Stage 1 is a coherent unit worth persisting.
2. **Real-app container.** openresty_web per the session 1 plan:
   takes the place of dummy, runs an actual HTTP endpoint,
   exercises pg reads. Needs `lua-resty-*` libs staged.
3. **Heartbeat into KB.** `STROBE_HEARTBEAT` currently writes a
   local timestamp only. Grow to write a per-container bit in
   `bit_mask_table` (DCS schema already has the shape; need a
   schema add for container-scoped heartbeat).
4. **Per-app `health` status field** — declare in
   `construct_dcs_kb.lua` per app instance, write from
   `REAP_AND_RESPAWN` and setup/teardown.
5. **DCS integration.** node_control's
   `START_ASSIGNED_CONTAINERS` currently a stub; replace with
   `docker.run_from_spec` against a definition whose image is
   `nanodatacenter/luajit-<appname>:latest`. Wire one real app
   (openresty_web).

---

## Session 2 (2026-04-15) — original notes (pre-smoke-test)

Wrote the base-image code based on design from session 1, informed by a
historical Go supervisor the user built ~5 years ago (`process_control` +
`error_logger` + `system_control` package, redis-backed, goroutine per
child, shell-loop watchdog). Kept the shape and vocabulary
(`command_map`, `incident_log`, `strobe`, per-app status), swapped
everything else for DCS-era primitives: pg+sqlite instead of redis,
chain-tree states instead of goroutines, libc-FFI fork/exec instead of
`exec.Command`, chain-tree `request_shutdown`+`teardown` instead of a
separate `error_logger` binary.

### Files written (in `container/`)

- `Dockerfile` — Ubuntu 24.04 base; apt luajit/libpq5/libsqlite3/sqlite3;
  COPYs `prebuilt_libs`, `prebuilt_lua_libs`, `prebuilt_lua_share`;
  bundle.sh symlinked to `/usr/local/bin/bundle_controller`;
  CMD = `luajit supervisor/entrypoint.lua /opt/luajit_base/controller.db`.
- `docker_build.sh` — stages chain_tree runtime + DSL, DCS helpers
  (posix_time, pg_connector, kb_*, bit_mask_helpers), sqlite3_helpers,
  DBI/dkjson/ltree.so from host. Compiles `supervisor/dsl.lua ->
  controller.json` on host before `docker build`.
- `supervisor/process_primitives.lua` — libc FFI: `fork`, `execvp`,
  `waitpid_nohang` (decodes WEXITSTATUS/WTERMSIG), `kill`, `spawn`
  (fork+execvp+setenv), `sigaction_flag` (signal handler backed by a
  drain-once getter). ~180 lines.
- `supervisor/dsl.lua` — chain-tree DSL, one KB `controller`, one SM
  `ctrl_sm` with 5 states `{sync, setup, monitor, request_shutdown,
  teardown}`. Monitor has three parallel reset-loop columns:
  `ctrl_hb_col` (5s strobe), `ctrl_liveness_col` (2s reap+respawn),
  `ctrl_shutdown_watch` (1s SIGTERM/SIGINT poll). Error handler
  `ERR_TEARDOWN_REQUESTED` advances via `change_state`.
- `supervisor/user_functions.lua` — the registry. Spawn apps per
  `command_map`, `app_env` injection (CONTAINER_NAME/APP_SITE/
  APP_NAMESPACE/pg/nats/mqtt), reap via `waitpid_nohang`, policy-driven
  respawn (`always`|`on-failure`|`never`). Exceptions logged to
  `container.<name>.SYS_EXCEPTION.<exc>`. `VERIFY_PG` is a per-tick
  try-connect (same pattern as DCS). SIGKILL path reaps to avoid
  zombies on controller exit.
- `supervisor/entrypoint.lua` — mirrors `host_processes/dcs.lua`. Opens
  controller.db read-only, installs SIGTERM+SIGINT handlers,
  `init_chain_tree`, `add_test(controller)`, tick loop with
  `posix_time.sleep_until` pacing. Logs burst heartbeat with alive/total
  app count.
- `bundler/bundler.lua` — walks `/opt/apps/*/app.manifest.json`, sorts by
  `start_order`, writes `/opt/luajit_base/controller.db` with table
  `command_map(name PK, argv jsonarray, start_order, restart_policy,
  kb_path)`. Self-contained: sqlite via FFI so it doesn't pull in
  sqlite3_helpers' JSON fallback.
- `bundler/bundle.sh` — one-line wrapper (`luajit bundler.lua`), symlinked
  to `/usr/local/bin/bundle_controller` in the image.
- `dummy_app/{main.lua, app.manifest.json, Dockerfile}` — smoke-test app.
  `main.lua` prints a tick-per-second to stderr; Dockerfile
  `FROM luajit-base`, `COPY dummy_app/ /opt/apps/dummy/`,
  `RUN /usr/local/bin/bundle_controller`.
- `README.md` — layout + build + run + app-author contract.

### Deliberate deviations from the Go reference

- **No separate `error_logger` binary.** Chain-tree `teardown` state logs
  per-app `SYS_EXCEPTION` rows from inside the controller before exit;
  the outer docker-restart watchdog is enough for supervisor-level
  crashes (same pattern DCS uses). The Go design needed a second binary
  only because Go's `os.Exit` bypasses deferred handlers; chain-tree's
  terminate event is cooperative.
- **No kill-all-but-bash zombie sweep.** Supervisor tracks spawned pids
  explicitly (`ctx.apps[*].pid`). Reap list = exact child set. If the
  supervisor itself panics, docker's process group teardown kills
  children; restart brings everyone back clean.
- **Restart policy is per-app in the manifest**, not "infinite restart
  hardcoded". Manifest field `restart_policy ∈ {always, on-failure,
  never}`. Crash-loop throttling and max-retries deferred until a
  real app actually crash-loops.
- **Strategies (one-for-one/all/rest-for-one) not implemented.** The Go
  header comment described Erlang supervisor vocabulary; the
  implementation only did simple-one-for-one. We're at the same level.
  Adding strategies means adding a `restart_group` column to
  `command_map` and rewriting `REAP_AND_RESPAWN` to scan group members
  on failure — deferred.

### Assumptions to verify next session

- The docker build-time DSL compile expects `luajit` + chain_tree
  modules on the host. Orchestrator's `docker_build.sh` assumes the
  same, so OK.
- `ffi.load("sqlite3")` inside the bundler resolves to the apt
  libsqlite3-0. If not, `LD_LIBRARY_PATH` in Dockerfile covers it.
- `app.manifest.json` `argv[0]` must be executable via `execvp` (PATH
  lookup). For `luajit foo.lua`, PATH has `/usr/bin/luajit`. For a
  compiled binary, the manifest should use the absolute path.
- `CFL_CHANGE_STATE_EVENT` handling: user_functions.lua uses
  `sm_mod.change_state(handle, 0, sm.node_id, idx, nil)` — same call
  shape as DCS's `log_then_change_state`. Depends on
  `flash_handle.sm_by_name` being populated by the loader (session 7
  runtime fix).

### Next session — build + smoke test

1. Run `container/docker_build.sh` on a host that has luajit +
   chain_tree modules in LUA_PATH. Expect staging to complete and
   `controller.json` to be generated.
2. Build the image. Fix any missing-file errors by adjusting the stage
   script.
3. Build `dummy_app/Dockerfile`; confirm `bundle_controller` runs and
   `/opt/luajit_base/controller.db` exists with one row.
4. Run the dummy container against a live pg. Expect:
   - `sync` succeeds, loads command_map.
   - `setup` spawns dummy app.
   - `monitor` ticks; dummy_app stderr interleaves with supervisor log.
   - `docker stop` -> SIGTERM -> `VERIFY_NO_SHUTDOWN_SIGNAL` trips ->
     `ERR_TEARDOWN_REQUESTED` -> `request_shutdown` -> SIGTERM all ->
     clean exit (or timeout -> SIGKILL -> exit).
5. Crash test: SIGKILL the dummy app from outside the container.
   Expect `REAP_AND_RESPAWN` to catch the exit and relaunch within
   2 seconds. Confirm `SYS_EXCEPTION.app_died_dummy` appears in pg.

### Deferred to future sessions (unchanged from session 1)

- Container-level heartbeat bit into `bit_mask_table` (currently
  `STROBE_HEARTBEAT` only updates `ctx.last_heartbeat_ts` in-process;
  needs DCS-side schema to grow a per-container bit).
- Per-app health field writes under
  `container.<name>.app.<app>.health`. Schema needs to be declared in
  `construct_dcs_kb.lua` per app instance.
- Stdout/stderr pipe drain column (optional third monitor column). Not
  wired; apps write to the container's stderr directly and docker logs
  catches it.
- Probes beyond `kill(pid, 0)` liveness.
- Crash-loop escalation threshold.

---

## Session 1 (2026-04-14) — design decisions; no code written

First design session. Locked in the shape of the project; deferred all
actual file writing to next session. Directory skeleton created:

```
building_blocks/luajit_base/
├── container/    (stage 1 lives here: image source + dummy app)
├── tests/        (stage 2 lives here: standalone function tests)
├── apps/         (stage 3 lives here: real app containers)
├── continue.md   (this file)
└── README.md     (TBD)
```

## Architectural model (locked in this session)

**Container = pod metaphor, not a worker manifest.** Each luajit
container holds one controller (from the base image) plus N apps. The
controller is a chain-tree process that launches and supervises apps.

**Apps are standalone LuaJIT chain-tree procedures.** Each app has its
own compiled chain-tree JSON, its own sqlite KB (local state, built at
app-construction time), and its own `main.lua` entrypoint. Apps read
context from their CLI argv + environ vars, so the same app artifact
can move between containers without modification as long as the
container supplies the right envs.

**Controller has its own sqlite KB too.** Contents: CLI expressions,
one per app this container hosts (starting minimum — content will grow
as needs surface). Generated at bundle time. Each CLI includes the
path to the app's own sqlite KB as an argv flag — the app learns its
own KB location from argv.

**Convention: every app lives at `/opt/apps/<app_name>/`** inside the
container. Self-contained per app — copy a dir in to add an app, rm a
dir to remove. Predictable paths mean CLI expressions are simple.

**Two-step build.**
1. **Build each app** — produces a self-contained app artifact. Shape
   of the artifact is TBD; we'll settle it when we actually build one.
2. **Bundle** — assemble N app artifacts into a container image, copy
   each to `/opt/apps/<name>/`, populate the controller sqlite with
   CLI rows. The bundler tool ships *in the base image* so app
   Dockerfiles can invoke it from a multi-stage build.

**Three-stage development flow** (what the directories above host):
1. **Stage 1 — base image + dummy app together** (`container/`). Smoke
   test the whole harness end-to-end with something trivial so the
   contract + build pipeline get shaken out before real complexity.
2. **Stage 2 — unit-test individual functions outside the container**
   (`tests/`). Standalone luajit scripts that exercise supervisor
   primitives (spawn/verify/kill), the bundler, the controller-KB
   reader, etc. Fast feedback, no rebuild per tweak.
3. **Stage 3 — real app containers** (`apps/`). First inhabitant:
   `openresty_web/` — an OpenResty web server that interacts with
   system_control and node_control.

## Controller chain-tree shape (proposed, clone of DCS pattern)

Not yet written. Plan for next session:

- **sync** — open pg (retry), open controller sqlite, read CLI rows.
  Fail → terminate (docker restart policy re-runs us).
- **setup** — spawn each app (in `start_order` if specified), verify
  each pid alive. Write per-app heartbeat row to master pg.
- **monitor** — parallel reset-loop columns:
  - heartbeat (container + per-app)
  - liveness (kill(pid,0) per app; crash → log SYS_EXCEPTION +
    apply restart policy → respawn or escalate)
  - optional stdout/stderr drain (pipe read → pg events stream or
    docker logs)
- **request_shutdown** — on SIGTERM: ordered graceful-kill (reverse
  `start_order`), wait for exits, log exception if a worker hangs.
- **teardown** — SIGKILL stragglers, close pg/sqlite, asm_terminate_system.

Mirrors DCS: same state markers, same error-handler pattern, same
chain-tree primitives. Leverages everything fixed in Session 7 (chain
tree engine fixes) + Session 8 (node_monitor reference implementation).

## Base image contents (planned)

Match the orchestrator pattern where possible (`third_party_containers/orchestrator/` is a good reference):

Binaries (apt):
- luajit
- libpq5, libsqlite3, sqlite3
- (no docker.io — unlike orchestrator, base doesn't manage containers)

Prebuilt libs staged into build context:
- `prebuilt_libs/ltree.so`
- `prebuilt_lua_libs/dbd/postgresql.so`
- `prebuilt_lua_share/DBI.lua`, `dkjson.lua`
- `prebuilt_lua_share/chain_tree/` — full chain_tree_luajit/runtime + lua_dsl
- `prebuilt_lua_share/kb_status.lua`, `kb_stream.lua`, `kb_exception.lua`,
  `posix_time.lua`, `host_sampler.lua` (from nanodatacenter_dcs/host_processes/)

Process primitives: implement via FFI to libc
(`fork`, `execvp`, `kill`, `waitpid`, `pipe`). No luaposix dependency.
~80 lines of Lua — ship in `container/supervisor/process_primitives.lua`.

Entry point: `/usr/local/bin/controller` (or similar) that the
Dockerfile sets as CMD. Loads controller DSL, ticks.

## Environs contract (planned)

What node_control passes to the container on `docker run`:

- `CONTAINER_NAME` — matches topology + construct_dcs_kb registration.
- `APP_SITE` — site ltree (e.g. `moonbase.alpha.dcs`).
- `APP_CPU_ID` — CPU this container runs on.
- `PG_HOST`, `PG_PORT`, `PG_DB`, `PG_USER`, `PG_PASSWORD` — master KB.
- `NATS_URL`, `MQTT_HOST` — optional, passed through for apps.

Controller's ltree namespace: `system.site.<APP_SITE>.cpu.<APP_CPU_ID>.container.<CONTAINER_NAME>` — already declared by `construct_dcs_kb.lua` with `health`, `events`, `service` fields.

## Base image OS

Ubuntu 24.04 (matches orchestrator). Alpine deferred; libpq + musl has
arm64 build fragility not worth the size savings today. Revisit when
image size actually matters.

## Deferred questions (resolve when we hit them)

- Exact app artifact contents (what files in `/opt/apps/<name>/`).
  Will become clear when we build the dummy app. Guess: `main.lua`,
  `kb.sqlite`, maybe `chain_tree.json`, maybe `user_functions.lua` —
  but we'll know when we try one.
- CLI authorship: convention-driven (bundler generates from artifact
  layout) vs app-declared (each artifact includes a cli.txt). Pick
  after seeing what the dummy app needs.
- Controller KB schema beyond "CLI expressions, one per app". Add
  columns as restart policies, probes, start order become real.
- Per-container `bootstrap.db` (like DCS) vs environ-only context.
  Environ-only for openresty probably fine; richer config apps may
  push us toward bootstrap.db. Defer.
- Standalone base-image behavior when no apps are bundled (error?
  heartbeat-only? hello-world?). Pick after dummy-app works.
- Exception logging schema for apps — per-app `SYS_EXCEPTION` rows
  need schema support in `construct_dcs_kb.lua`. Figure out path
  when we wire the first real crash.
- Per-app probes beyond `kill(pid, 0)` liveness (http/exec/file/nats).
  Not needed until a real app needs one.
- Crash-loop escalation threshold (N restarts in M seconds). Default
  TBD.

## Resume here next session

1. Write `container/Dockerfile` + `container/docker_build.sh`.
2. Stage libs into `container/prebuilt_*/` using a stage script.
3. Write `container/supervisor/process_primitives.lua` (FFI wrappers).
4. Write `container/supervisor/dsl.lua` (controller chain-tree DSL,
   cloned from DCS shape).
5. Write `container/supervisor/user_functions.lua` (open-pg,
   open-sqlite, spawn/verify/kill, heartbeat, exception log).
6. Write `container/supervisor/entrypoint.lua` (mirrors
   `host_processes/dcs.lua`).
7. Write `container/bundler/bundler.lua` and
   `container/bundler/construct_controller_kb.lua`.
8. Write `container/dummy_app/` (trivial hello-world chain-tree).
9. Build, run, smoke-test. Iterate.

Don't work on `tests/` or `apps/` until `container/` is green.

## Where this fits in the larger DCS plan

Session 8's continue.md listed "application container development" as
the next task. This project is that work. Once the luajit base is
real:

- Update DCS's `topology.lua` to declare app container instances with
  `definition = "openresty_web"` etc.
- Update `construct_dcs_kb.lua` to emit per-app exception schema under
  each container instance.
- Update `user_functions.lua` in DCS's node_control to actually run
  `docker.run_from_spec` for app containers (today's stub).
- Wire the first real app (openresty_web) end-to-end: DCS starts it,
  controller inside it starts its workers, heartbeat flows, crash
  testing validates the exception path.
