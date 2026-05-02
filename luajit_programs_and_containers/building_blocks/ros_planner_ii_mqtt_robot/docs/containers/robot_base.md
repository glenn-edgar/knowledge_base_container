# `mycorp/robot_base:1.0`

The supervisor layer. Knows how to bring **a robot** up — independent
of robot class.

## Bakes

- Robot-supervisor chain-tree runtime (`/opt/robot_base/supervisor/`):
  - `entrypoint.lua` — main tick loop
  - `dsl.lua` → `robot_supervisor.json` (chain-tree IR, compiled in-image)
  - `user_functions.lua` — per-state handlers
  - `env_validate.lua` — `ROBOT_ID`/`ROBOT_CLASS`/`DONGLE_INSTANCE` gate
  - `config_render.lua` — `${VAR}` template renderer
  - `process_helpers.lua` — `spawn_with_stdout_pipe`, line-drain
  - `upward_peer.lua` — Phase-2 stub (no-op `register/tick/on_shutdown`)

## Supervisor state machine

```
sync           VALIDATE_ENV (gather + ROBOT_CLASS_BAKED check)
render_config  template -> /run/robot/config.json
spawn_sim      one-shot SPAWN_ROBOT_SIM with stdout pipe
wait_for_ready asm_wait VERIFY_SIM_READY (parse PTY=… + READY)
spawn_mqtt_robot                        with ROBOT_SIM_PTY env
register_peer  upward_peer:register (Phase 1 stub returns true)
monitor        3 parallel reset-loop columns:
                 robot_liveness_col   drain stdout, fail-stop on death
                 robot_shutdown_watch SIGTERM/SIGINT poll
                 robot_peer_col       upward_peer:tick
request_shutdown SIGTERM all, asm_wait VERIFY_ALL_CHILDREN_EXITED
teardown       SIGKILL stragglers, asm_terminate_system
```

71 chain-tree nodes total.

## Critical gotcha — `wait_for_ready` uses `asm_wait`, not `asm_verify`

`asm_verify` advances a column on `bool=true` regardless of event,
including INIT. `VERIFY_SIM_READY` can't usefully return true on INIT
(nothing has been read from sim's stdout yet) but can't return false
either (CFL_VERIFY trips the error handler on false). Solution:
`asm_wait` (CFL_WAIT semantics) halts on `bool=false` and counts timer
events for timeout. Any "wait for external state" gate must follow
this pattern.

See [`feedback_chaintree_verify_event_filter`](../architecture/index.md)
for the convention on event-pass-through verifies (which DO return
true on non-TIMER).

## Fail-stop discipline

Per [`feedback_no_soft_faults`](../architecture/index.md):

- Missing required env → `os.exit(1)` before chain-tree starts.
- `ROBOT_CLASS != ROBOT_CLASS_BAKED` → `os.exit(1)` (class mismatch).
- Any supervised child death → `ERR_CHILD_DIED` → `request_shutdown`.
- SIGTERM/SIGINT → `ERR_TEARDOWN_REQUESTED` → `request_shutdown`.

No retry, no soft-restart. Docker's restart policy decides whether to
re-launch the container after exit.

## Build

```bash
bash containers/robot_base/docker_build.sh
```

Self-contained: just `docker build` the supervisor tree. The DSL JSON
is compiled inside the image (no host-side `luajit`/chain_tree dep).
Pre-req: `nanodatacenter/luajit-base:latest` must exist locally.

## Layer size

≈ 165 MB (= luajit-base + supervisor source).
