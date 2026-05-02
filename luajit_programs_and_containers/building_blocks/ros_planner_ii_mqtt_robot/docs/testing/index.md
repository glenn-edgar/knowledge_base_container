# Testing

Three concentric layers of test, each runnable in isolation.

## Surfaces

| Surface | Driver | Needs broker? | Needs rover? | Purpose |
|---|---|---|---|---|
| **C unit tests** | `test_*` binaries via `run_tests.sh --skip-e2e` | no | no | Wire framing, bus_kernel, ext_bus, logical_robot, drive_base, dongle catalogue |
| **Lua unit tests** | `luajit test_*.lua` (also via `--skip-e2e`) | no | no | Physics, robot_controller contract |
| **MQTT e2e** | `test_random_paths.lua` (+ optional `test_mock_planner.lua`) | yes | yes | Full mission: planner → MQTT → libcomm → pty → robot_sim → drive_base → libphysics |

## `run_tests.sh`

Collected entry point. Two modes:

```bash
bash run_tests.sh --skip-e2e   # ~306 unit checks, all green; no broker
bash run_tests.sh --speed 10 --count 5   # +e2e (paths_only + mixed)
```

Logs go to `/tmp/rover_test_<timestamp>/` per run. `--verbose` keeps
the per-test output on stdout.

## Test inventory

### C unit tests (built by `make all`)

| Binary | Checks | Source |
|---|---|---|
| `test_frame_unit` | 28 | libcomm framing + CRC + escape |
| `test_link_router` | 11 | libcomm router |
| `test_transport_inproc` | 28 | in-process transport |
| `test_comm_loopback` | 24 | comm submit/poll loopback |
| `test_comm_slave_handler` | 13 | slave-side dispatch |
| `test_comm_pty_loopback` | 27 | pty multi-frame |
| `test_comm_pty_multi_dongle` | 18 | multi-dongle pty |
| `test_bus_kernel` | 29 | bus_kernel msgq smoke |
| `test_ext_bus_contract` | 14 | ext_bus 3-fn contract (Track A) |
| `test_bus_msg` | 23 | bus_msg envelope (Slice L1) |
| `test_logical_robot` | 16 | vtable lifecycle (Slice L2) |
| `test_drive_base` | 15 | drive_base logical_robot (Slice L3) |
| `test_dongle_catalogue` | 25 | full dongle-thread routing (Slice L4b) |

### Lua unit tests

| File | Checks | Coverage |
|---|---|---|
| `test_physics.lua` | 26 | Standalone physics: pose, segments, stop/resume, tools, charger, payload pickup |
| `test_robot_controller_contract.lua` | 37 | [robot_controller_test_peer](../controller/contract.md) verb surface — register, heartbeat, exception (idempotent), kb_read/write (scope+CAS), shutdown, controller-initiated verbs, unknown-verb |

### MQTT e2e

See [e2e](e2e.md) for the full flow.

## Continuous numbers (2026-05-02)

```
[PASS] frame_unit:                28 passed, 0 failed
[PASS] link_router:               11 passed, 0 failed
[PASS] transport_inproc:          28 passed, 0 failed
[PASS] comm_loopback:             24 passed, 0 failed
[PASS] comm_slave_handler:        13 passed, 0 failed
[PASS] comm_pty_loopback:         27 passed, 0 failed
[PASS] comm_pty_multi_dongle:     18 passed, 0 failed
[PASS] bus_kernel smoke:          29 passed, 0 failed
[PASS] ext_bus contract:          14 passed, 0 failed
[PASS] bus_msg:                   23 passed, 0 failed
[PASS] logical_robot:             16 passed, 0 failed
[PASS] drive_base:                15 passed, 0 failed
[PASS] dongle_catalogue:          25 passed, 0 failed
[PASS] rc_contract:               37 passed, 0 failed
e2e paths_only:                   cmds=6 ack=6 done=6 (6 ok / 0 fail)
e2e mixed:                        cmds=10 ack=10 done=10 (10 ok / 0 fail)
```
