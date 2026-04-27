# Path workers

`path_line`, `path_spline`, `path_rotate` share an identical `*_MAIN`
because by the time the worker activates, C is already tracking the
segment. The worker's only job is to *poll for completion*.

## Lifecycle

1. **Controller pushes the segment** to the C queue eagerly when the RPC
   arrives — `seg_id` returned from `hal:push_line/push_spline/push_rotate`.
2. **Controller activates the worker** with `bb._seg_id = seg_id`.
3. **`path_init`** logs the seg_id and command; sets `exec_active = true`.
4. **`path_main`** each tick:
   ```lua
   local pose = hal:read_pose()
   local st   = hal:read_path_status()

   bb.delta_x       = pose.x - bb._seg_start.x
   bb.delta_y       = pose.y - bb._seg_start.y
   bb.delta_heading = pose.heading - bb._seg_start.heading

   if st.flags & PATH_F.FAULT ≠ 0 then
       bb.bitmask = 0x04 (motor_fault); worker_success = false; DISABLE
   end
   if st.last_done_seg_id == bb._seg_id then
       bb.bitmask = 0x01 (seg_complete); worker_success = true; DISABLE
   end
   ```
5. **Controller's completion path** snapshots truth pose, debits energy,
   sends `kb_done`.

## `path_wall` (unsupported)

There is no obstacle infrastructure in the sim today. `worker_path_wall`
is a stub that immediately fails with `fault_reason = "path_wall_unsupported"`
and sets `bitmask = 0x04`. The capability is omitted from
`rover_1_config.json`'s `capabilities` list, so the planner shouldn't try
to use it.

## Per-packet deltas use noisy pose

`bb.delta_*` fields are computed from `read_pose()` (with sensor noise) so
the planner gets the values its own observers would see. The controller's
completion-time snapshot uses `read_pose_truth()` for `global_pos`
accounting — that's the robot's own internal "truth" odometry.

## Speed handling

Path packets carry a `speed` field (m/s for line/spline, rad/s for
rotate). The C follower clamps to the curve's curvature (`v_limit_curve =
1.5 / |κ|`) so sharp Bezier sections won't drive the robot over their
follow limit even if the packet asked for higher speed. There's no Lua
override for this.
