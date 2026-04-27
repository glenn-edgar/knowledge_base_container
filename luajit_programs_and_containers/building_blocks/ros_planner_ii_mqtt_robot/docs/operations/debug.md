# Debugging

A short list of common failure modes and where to look.

## "Robot never goes live"

Symptom: `run_tests.sh` times out at "waiting for robot link_state=live".

Check, in order:

1. **Broker reachable?**
   ```
   mosquitto_pub -h localhost -p 1883 -t test/ping -m hi -q 0
   ```
2. **Mock planner alive?**
   ```
   pgrep -fa test_mock_planner
   tail "$LOG_DIR/mock_planner.log"
   ```
   It should show `RECV link_announce` then `SENT link_bridge_ack`.
3. **Robot reading the broker?**
   ```
   tail "$LOG_DIR/rover_1.log"
   ```
   Look for `LINK_CLIENT … announcing` repeating without ack — that's
   the planner side not responding.

## "ChainTree validation failed: missing functions"

Means `remote.json` references a function name that's not in the
registry. Either the DSL has drifted from `remote_user_functions.lua`,
or the build wasn't rerun after editing one of them.

Fix: `bash build.sh` and try again.

## "libphysics: cannot load"

`physics_ffi`'s loader tries `<script_dir>/libphysics.so`,
`./libphysics.so`, `libphysics`. If none of these resolve, you get a
hard error.

Fix: `make` (or `./run_tests.sh`).

## "Cross-track fault" mid-path

The follower drifted more than `cross_track_abort_m` from the
projected segment point. Almost always a sign that:

- Two consecutive line segments meet at a sharp angle and the robot
  cuts the corner (this is benign; it's the harness's pose model
  that's wrong, not the physics).
- The robot has a payload and inertia is dragging it past the line.

If real, raise `cross_track_abort_m` in `physics_config.json` or
switch the segment to a spline.

## "Robot is moving but no kb_done"

Common cause: the worker is pinging `worker_alive` but nothing else,
so the watchdog never trips and the robot just runs forever. Check the
worker's `*_MAIN` for a missing `return defs.CFL_DISABLE` on the
completion path.

Slower path: the worker's completion condition is wrong. For path
workers, that's `st.last_done_seg_id == bb._seg_id`; if the seg_id is
zero (push failed) you'll never see it.

## "Energy goes negative"

Only possible if `energy_infinite = false` and the controller's
measurement of `energy_used_total` jumps by more than the remaining
budget. Check whether the worker spans a battery-state event (a
recharge in the middle of a stream); the snap-back logic is only run
for `TYPE_RECHARGE` packets.

## Debug tail filters

```
# Just worker activations + completions
grep -E "VN\[|DONE|FAULT" "$LOG_DIR/rover_1.log"

# Heartbeats only
grep " HB " "$LOG_DIR/rover_1.log"

# Just the link state machine
grep "LINK_CLIENT" "$LOG_DIR/rover_1.log"
```

## Replaying a scenario

The harness runs are deterministic for a given `--seed`. To reproduce
a failure that happened in `mixed_seed99`:

```
./run_tests.sh --skip-build --skip-unit
# … wait for the right scenario, OR run it directly:
luajit test_random_paths.lua --seed 99 --count 10 --mode mixed --wait 180 --verbose
```

(The mock planner + robot have to be running already — leave
`run_tests.sh` going up through `paths_only_seed42` then Ctrl-C and
launch your own random run.)
