# End-to-end MQTT tests

Full vertical: planner → MQTT → libcomm → pty → robot_sim →
drive_base → libphysics. Needs an MQTT broker on the host (mosquitto)
and a running rover (native or container).

## Two flow shapes

### Two-process (legacy)

```
mosquitto (host:1883)
    ↑
test_mock_planner.lua  ──── planner-side handshake + heartbeats
    ↑
[ rover process or container ]
    ↑
test_random_paths.lua  ──── publishes commands, reads kb_done
```

```bash
luajit test_mock_planner.lua --duration 600 &
luajit test_random_paths.lua --robot rover_1 --seed 11 \
                              --count 5 --mode paths_only --wait 60
```

`run_tests.sh` uses this flow because `mock_planner` outlives a single
test run, allowing back-to-back scenarios on the same live rover.

### Self-host (single process)

```
mosquitto (host:1883)
    ↑
test_random_paths.lua --self-host  ── peer brings rover live AND
                                       drives commands
    ↑
[ rover process or container ]
```

```bash
luajit test_random_paths.lua --robot rover_1 --self-host \
                              --seed 11 --count 5 --mode paths_only --wait 60
```

`--self-host` is the right choice for ad-hoc smoke tests and CI when
each invocation is independent.

Both flows share the same [`planner_test_peer`](planner_test_peer.md)
under the hood.

## Test modes

`test_random_paths.lua --mode <mode>`:

| Mode | Generates | Exercises |
|---|---|---|
| `paths_only` | `init_check` + N path commands (line/spline mix) | Pure motion: master push → slave drive_base → libphysics → pose deltas back |
| `mixed` | `init_check` + N paths interleaved with `idle`/`scan`/`paint`/`deliver` | Above + L6 tool commands (BEGIN_*/TOOL_MOVE/GET_TOOL_STATUS via arm-cycle workers) |
| `single_action` | One `idle` | Smoke check |

## Args

| Flag | Default | Notes |
|---|---|---|
| `--robot` | `rover_1` | Targets specific rover instance |
| `--site` | `moonbase.alpha.surface_ops` | MQTT topic prefix segment |
| `--host`, `--port` | `localhost`, `1883` | Broker |
| `--seed` | `1` | Random seed (deterministic generation) |
| `--count` | `20` | Number of generated commands (excluding init/idle bookends) |
| `--mode` | `mixed` | See above |
| `--workspace` | `3.0` | Random-position bound (meters) |
| `--wait` | `90` | Seconds to listen for kb_dones before timing out |
| `--self-host` | off | Drive entire e2e from this process |
| `--verbose` | off | Print every dispatched command + every received event |

## Exit codes

- `0` — all commands acked + all kb_dones received and ok
- `2` — wait window expired before all dones arrived
- `3` — at least one kb_done reported failure

## Reading the output

Per-command lines:

```
DONE test=2002 success=true dx=-0.71 dy=-1.08 dh=-3.59 energy=9995.7
```

`dx/dy/dh` are the pose delta accumulated during that command, NOT
absolute pose. The harness uses these to verify the robot moved in
the expected direction (test_random_paths doesn't assert specific
deltas; it just confirms `success=true`).

Trailer:

```
Summary: cmds=10  ack=10  hb=888  done=10 (10 ok / 0 fail)
```

| Field | Meaning |
|---|---|
| `cmds` | Sent |
| `ack` | RPC acks received from rover |
| `hb` | Telemetry heartbeats observed (depends on `BITMASK_PUBLISH_EVERY` × command duration) |
| `done` | `kb_done` events received |
| `ok / fail` | of the dones |

## What can go wrong

- **Broker connect refused** — start mosquitto on the host
  (`brew services start mosquitto`, `systemctl start mosquitto`, or
  `mosquitto -d`).
- **`done < cmds`** — rover slower than `--wait`. Bump `SPEED_FACTOR`
  on the rover or `--wait` on the harness.
- **`fail > 0`** — read `fault_reason` in the DONE line. Common
  causes: `path_fault` (cross-track explosion), `tool_fault` (gripper
  miss), `not_at_charger` (mission tried recharge away from a station).
- **Rover container not connecting** — confirm `MQTT_HOST` env (use
  `host.docker.internal` on Docker Desktop / WSL2).
