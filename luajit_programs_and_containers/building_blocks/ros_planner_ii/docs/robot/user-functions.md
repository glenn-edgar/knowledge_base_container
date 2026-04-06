# User Functions

`remote_user_functions.lua` contains all ChainTree callback functions for the robot.

## Controller Functions (robot-independent)

| Function | Role |
|----------|------|
| CTRL_DISPATCH_MAIN | Receives RPC commands, sends ACK, activates workers |
| CTRL_DISPATCH_INIT | Sets controller_active flag |
| CTRL_WATCHDOG_MAIN | Counts ticks per action, triggers timeout |
| CTRL_HEARTBEAT_MAIN | Sends periodic heartbeat with delta pose |
| CTRL_COMPLETION_MAIN | Detects worker done, applies pose, sends KB_DONE |
| WORKER_TERM | Shared: marks worker_done, worker_success |

## Worker Functions (per VN type)

Each worker has INIT + MAIN:

| Worker | What it does (simulated) |
|--------|------------------------|
| WKR_INIT_CHECK | Countdown, no pose change |
| WKR_PATH_SPLINE | Interpolates delta_x/delta_y over ticks |
| WKR_PATH_LINE | Same as spline (different real behavior) |
| WKR_PATH_WALL | Same as spline (wall following) |
| WKR_PATH_ROTATE | Interpolates delta_heading |
| WKR_DELIVER_PART | Interpolates delta_arm_angle |
| WKR_PAINT_SAMPLE | Interpolates delta_arm_angle |
| WKR_LOAD_SHIPPING | Interpolates delta_arm_angle |
| WKR_PASS_GATE | Countdown, no pose change |
| WKR_INSPECTION_SCAN | Countdown, no pose change |
| WKR_RECHARGE | Countdown, energy restored by completion handler |
| WKR_IDLE | Short countdown |

## Energy Tracking

- `energy.remaining` decremented per action by `energy_costs[packet_type]`
- Recharge VN restores to `energy.max`
- `energy.infinite = true` skips deduction (stationary arm, plugged in)

## Pose Tracking

Workers set `bb.delta_x`, `bb.delta_y`, `bb.delta_heading`, `bb.delta_arm_angle` during execution. CTRL_COMPLETION applies these to `global_pos` and reports in KB_DONE.
