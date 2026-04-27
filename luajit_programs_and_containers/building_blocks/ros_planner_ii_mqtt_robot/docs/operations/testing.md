# Testing

Three test surfaces, all driven through `run_tests.sh`.

## `test_physics.lua` — standalone physics

26 assertions. No MQTT, no ChainTree. Drives the HAL directly:

| Test | Asserts |
|---|---|
| T1 | Single-segment line: arrives within 0.15 m of endpoint, queue empty, segment-id done |
| T2 | Continuous 3-segment path: never fully stops mid-path, all three segments complete |
| T3 | `request_stop` brings the robot to rest mid-path; `release_stop` resumes; final endpoint reached |
| T4 | Revolute arm tool: ramps to 90° target, then back to 0 |
| T5 | Charger dock + charge: drive 8 m, station_at_pose finds charger, battery integrates back to capacity |
| T6 | Payload pickup: drive to load_dock, grip, payload mass increases from 0 |

Run alone:

```
luajit test_physics.lua          # needs json_util on LUA_PATH
```

Run via harness:

```
./run_tests.sh --skip-e2e
```

## `test_mock_planner.lua` — link counterpart

Subscribes to `…/link` and `…/stream_bus`, sends `link_bridge_ack` on
`link_announce`, sends `link_bridge_heartbeat` once a second.
Intentionally minimal — does not issue commands. Used as one end of
`run_tests.sh`'s e2e stack.

## `test_random_paths.lua` — random command harness

Publishes `count` random commands to `…/rpc`, listens on `…/stream_bus`
for `ack` / `heartbeat` / `kb_done`. Three modes:

| `--mode` | Mix |
|---|---|
| `paths_only`   | 50% line, 50% spline |
| `mixed`        | 20% line, 60% spline, 10% idle, 6% scan, 4% paint |
| `single_action`| one idle |

The line/spline mix is biased toward splines because hard-cornered
lines can corner-cut and create cross-track spikes; G1-continuous
splines stitch better. See
[continuous-motion / cross-track](../architecture/continuous-motion.md#cross-track-abort).

Workspace defaults to ±3 m from current pose; the harness keeps a
running `pose` model and chains segments off it.

Exit codes:

| Code | Meaning |
|---|---|
| `0` | All commands acked + completed successfully |
| `2` | Some commands didn't return `kb_done` within `--wait` |
| `3` | Some commands returned `success=false` |

## `run_tests.sh` — orchestrator

```
./run_tests.sh                 # build + unit + 4 e2e scenarios
./run_tests.sh --skip-build
./run_tests.sh --skip-unit
./run_tests.sh --skip-e2e
./run_tests.sh --speed 10      # SPEED_FACTOR override
./run_tests.sh --count 20      # commands per scenario
./run_tests.sh --verbose       # tail logs on failure
```

Logs land in `/tmp/rover_test_<timestamp>/`. `make.log`, `build.log`,
`test_physics.log`, `mock_planner.log`, `rover_1.log`, plus one log per
scenario.

## E2E scenarios

| Scenario | Mode | Seed | Count | Wait |
|---|---|---|---|---|
| `paths_only_seed42` | paths_only | 42 | `--count` | 120 s |
| `paths_only_seed1`  | paths_only | 1  | `--count` | 120 s |
| `mixed_seed17`      | mixed      | 17 | `--count` | 180 s |
| `mixed_seed99`      | mixed      | 99 | `--count` | 180 s |

## Broker-flap handling

The Mosquitto container in DCS bounces occasionally; `run_tests.sh` has
two layers of recovery:

1. **`wait_for_broker`** — polls `mosquitto_pub` for up to 90 s.
2. **`relaunch_stack`** — kills the mock planner + robot and brings
   them back up against the (recovered) broker, rotating their log
   files so older logs aren't clobbered.

Each scenario will retry up to 2× if it sees `unexpected disconnect` in
its output. After all retries fail, the scenario is marked failed.
This is a workaround for Docker Desktop / WSL2 platform flakiness, not
a robot-side bug.
