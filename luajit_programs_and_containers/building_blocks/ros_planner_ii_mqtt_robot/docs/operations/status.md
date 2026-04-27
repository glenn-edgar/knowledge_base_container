# Status & open issues

Snapshot as of 2026-04-24.

## Build status

- **Standalone physics tests:** 26 / 26 green.
- **End-to-end MQTT tests:** 3 / 4 scenarios reliably green
  (`paths_only_seed42`, `paths_only_seed1`, `mixed_seed17`).
  `mixed_seed99` flakes when the broker container bounces mid-run; the
  `relaunch_stack` retry path is in place but hasn't been confirmed
  green since reboot.

Last clean run before reboot:

```
[PASS] paths_only_seed42   cmds=11 ack=11 done=11 (11 ok / 0 fail)
[PASS] paths_only_seed1    cmds=11 ack=11 done=11 (11 ok / 0 fail)
[PASS] mixed_seed17        cmds=12 ack=12 done=12 (12 ok / 0 fail)
[FAIL] mixed_seed99        broker flapped on retry, stack never recovered
```

## Known open issues

1. **Line corner-cutting at sharp turns.** `path_line` segments meeting
   at hard 90° angles cause the follower to cut the corner. Harmless in
   sim — real planners will derive the next segment from the robot's
   reported `kb_done.delta_*` rather than chaining off a stale planner
   pose.
2. **`pass_gate` is a stub.** No gate infrastructure in the sim; just
   a 1.5 s dwell.
3. **`inspection_scan` and `operation` are dwell-only.** No sensor
   model plumbed in yet.
4. **Arm tool faults aren't tested.** The 26 unit tests don't inject
   a `TOOL_F_FAULT`; the worker's surface for it is correct but
   unexercised.
5. **`phys_push_rotate` heading math is approximate.** The
   `active_progress` calculation uses linear interpolation that can
   look weird near wrap-around. Rotates work for small angle changes;
   exercise carefully before relying on it for >180° turns.

## Open TODOs (ordered)

1. Confirm `run_tests.sh` survives a broker flap (run once, verify
   retry path). If still flaky, fall back to adding auto-reconnect to
   the `mqtt_pubsub` client.
2. Commit the new files and rewrites currently in `git status`.
3. Containerization. Hold off until the sim runs unattended for a few
   hundred random commands.
4. Per-worker sensor models for `inspection_scan` (range + colour) and
   `operation` (site-dependent).
5. Real load_dock / assembly fixture tests — drive-to-dock + grip +
   drive-away-with-payload + drop, end-to-end.

## What's deliberately NOT in scope

- `path_wall` (no obstacle model).
- Multi-robot coordination at the planner level (single-robot tests
  only).
- CBOR wire format on the e2e path (the code path exists but JSON is
  what's exercised).
- Hardware HAL backend. The HAL is structured to accept one — see
  `robot_hal.lua` — but only `mode = "sim"` is implemented today.
