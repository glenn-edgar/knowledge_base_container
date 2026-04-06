# ChainTree DSL (Robot Side)

The robot uses a ChainTree behavior tree compiled from `remote_dsl.lua` to `remote.json`.

## Structure

```
controller (always active)
    ├── CTRL_DISPATCH — receives RPC commands, activates workers
    ├── CTRL_WATCHDOG — timeout monitoring per action
    ├── CTRL_HEARTBEAT — periodic heartbeat to planner
    └── CTRL_COMPLETION — detects worker done, sends KB_DONE, applies pose

worker_init_check (activated per command)
worker_path_spline
worker_path_line
worker_path_wall
worker_path_rotate
worker_deliver_part
worker_paint_sample
worker_load_shipping
worker_pass_gate
worker_inspection_scan
worker_recharge
worker_idle
```

## Worker Pattern

Every worker follows the same single-column structure:

```lua
make_worker("worker_path_spline", "WKR_PATH_SPLINE_MAIN", "WKR_PATH_SPLINE_INIT")
```

- `WKR_*_INIT` — one-shot: set exec_start, clear state
- `WKR_*_MAIN` — main loop: execute action, set delta pose, return CFL_DISABLE when done
- `WORKER_TERM` — shared termination: set worker_done, worker_success

## Controller Flow

1. `CTRL_DISPATCH` receives `ROBOT_RPC_COMMAND` event with packet_type
2. Looks up worker by packet_type → `worker_by_packet_type` table
3. Sends ACK immediately
4. Activates worker KB (or queues as lookahead if another worker is active)
5. `CTRL_WATCHDOG` counts ticks, expires if exceeds max_time
6. `CTRL_HEARTBEAT` sends periodic heartbeat with delta pose
7. `CTRL_COMPLETION` detects worker_done, applies global pose, sends KB_DONE, deactivates worker

## Building

```bash
cd ros_planner_ii_mqtt_robot
./build.sh
# Output: remote.json (95KB, 137 nodes)
```
