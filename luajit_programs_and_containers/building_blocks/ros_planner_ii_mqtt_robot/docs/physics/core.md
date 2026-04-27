# `physics_core.c`

~1000 lines of C99, built into `libphysics.so`. No external deps beyond
libm.

## Public flags

```c
#define PATH_F_TRACKING     0x01u
#define PATH_F_STOPPED      0x02u
#define PATH_F_QUEUE_EMPTY  0x04u
#define PATH_F_FAULT        0x08u
#define PATH_F_SEG_DONE     0x10u   /* latched, cleared by phys_read_path_status */

#define TOOL_F_AT_TARGET    0x01u
#define TOOL_F_GRASPED      0x02u
#define TOOL_F_RELEASED     0x04u
#define TOOL_F_DOCKED       0x08u
#define TOOL_F_CHARGING     0x10u
#define TOOL_F_FAULT        0x80u
```

```c
#define SEG_LINE 0    SEG_SPLINE 1   SEG_ROTATE 2

#define TOOL_NONE 0   TOOL_REVOLUTE_1 1   TOOL_BINARY 2   TOOL_PASSIVE_DOCK 3

#define STATION_CHARGER 1   STATION_LOAD_DOCK 2
#define STATION_PAINT   3   STATION_ASSEMBLY  4
```

## Exported functions (FFI surface)

### Lifecycle / config

| Function | Purpose |
|---|---|
| `phys_t* phys_create()` | Allocate + zero a plant struct with conservative defaults. |
| `void phys_destroy(phys_t*)` | Free. `physics_ffi` attaches via `ffi.gc`. |
| `phys_set_chassis(p, wb, r, m, I, lf, af)` | Wheelbase, wheel radius, mass, inertia, lin/ang friction. |
| `phys_set_motors(p, max_tau, max_w, kp, ki, kd, ek)` | Torque + wheel-speed cap, PID gains, energy coefficient. |
| `phys_set_follower(p, la_min, la_kv, arr_tol, hd_tol, mla, maa, inner_dt, xt_abort)` | Pure-pursuit + ramp + abort. |
| `phys_set_sensors(p, gps_sxy, gps_sh, imu_s, batt_pct)` | Gaussian noise sigmas. |
| `phys_set_battery(p, cap_j, init_j)` | Capacity + initial charge. |
| `phys_set_initial_pose(p, x, y, h)` | Truth pose at boot. |
| `phys_set_seed(p, uint64_t)` | RNG seed (0 → keep default). |
| `phys_add_tool(p, slot, kind, lim_min, lim_max, max_speed, transition_s)` | Define a tool in slot 0..7. |
| `phys_add_station(p, kind, x, y, h, tol_m, tol_h, param1, param2)` | Add a passive fixture. |

### Step / queue

| Function | Purpose |
|---|---|
| `void phys_step(p, dt_sim)` | Advance sim by `dt_sim` in `ceil(dt_sim / inner_dt)` inner steps. |
| `double phys_sim_time(p)` | Current sim clock. |
| `uint32_t phys_push_line(p, fx, fy, tx, ty, h_from, h_to, speed)` | Push a degenerate spline (control points at thirds). Returns `seg_id`. |
| `uint32_t phys_push_spline(p, fx, fy, tx, ty, h_from, h_to, speed)` | Push a cubic Bezier with control points reflecting headings. |
| `uint32_t phys_push_rotate(p, from_h, to_h, rate)` | Push an in-place rotate. `arc_len = |wrap(to-from)|`. |
| `void phys_request_stop(p)` | Force `v_want = 0` while non-zero; doesn't flush queue. |
| `void phys_release_stop(p)` | Resume motion. |
| `int phys_is_stopped(p)` | 1 iff `PATH_F_STOPPED`. |
| `void phys_abort_path(p)` | Clear segment queue, zero `v_cmd`. |
| `int phys_queue_depth(p)` | Number of segments in queue. |
| `uint32_t phys_active_seg_id(p)` | Head segment's id, or 0. |
| `uint32_t phys_last_done_seg_id(p)` | Last popped seg id. |

### Reads

| Function | Out struct |
|---|---|
| `phys_read_pose(p, *out)` | `phys_pose_t` with sensor noise. |
| `phys_read_pose_truth(p, *out)` | `phys_pose_t` ground truth. |
| `phys_read_path_status(p, *out)` | `phys_path_status_t`. Reading clears `PATH_F_SEG_DONE`. |
| `phys_read_tool_status(p, slot, *out)` | `phys_tool_status_t`. `payload_mass`, `battery_j` are plant-global, repeated for convenience. |

### Tool ops

| Function | Effect |
|---|---|
| `phys_begin_tool_move(p, slot, target, speed)` | Revolute-only. Clamps to `lim_min..lim_max`. |
| `phys_begin_grip(p, slot)` | Binary tool → close. After `transition_s`, sets `GRASPED \| AT_TARGET`. If at a `STATION_LOAD_DOCK`, picks up the station's payload mass. |
| `phys_begin_release(p, slot)` | Binary tool → open. Drops payload if attached. |
| `phys_begin_dock(p, slot)` | Passive-dock tool. If at a `STATION_CHARGER`, sets `DOCKED \| AT_TARGET`; else `FAULT`. |
| `phys_begin_charge(p, slot, target_j)` | Passive-dock tool at charger station. Battery integrates `rate_w * dt` until `>= target_j`, then `AT_TARGET \| DOCKED`. |
| `phys_station_at_pose(p, kind)` | Index of in-tolerance station of given kind, or -1. |
| `phys_payload_mass(p)`, `phys_battery_j(p)` | Convenience scalars. |

## Internal structures

```c
typedef struct {
    int      type;          /* SEG_LINE | SEG_SPLINE | SEG_ROTATE */
    uint32_t seg_id;
    double   p0..p3 (Bezier), h_from, h_to, speed;
    double   arc_len;
    double   lut_s[ARC_LUT_N];   /* 33-sample arc-length LUT */
    double   rot_from_h, rot_to_h, rot_at_x, rot_at_y;
} segment_t;
```

```c
typedef struct {
    int      in_use, kind;
    double   value, target, speed;
    double   lim_min, lim_max, max_speed, transition_s;
    double   op_t;           /* sub-op timer */
    int      op;             /* 0 idle 1 move 2 grip 3 release 4 dock 5 charge_hold */
    uint32_t bits;           /* TOOL_F_* */
    /* passive dock layout (offsets, tolerances) */
} tool_t;
```

```c
typedef struct {
    int      kind;
    double   x, y, heading, tol_m, tol_h;
    double   param1, param2;     /* charger: rate_w; load_dock: payload_mass, arm_pickup_h */
    char     id[32], payload_id[32];
} station_t;
```

## RNG

xorshift64 seeded from `phys_set_seed` or default.
`rng_uniform` → `[0,1)`, `rng_gauss(sigma)` is one-tap Box-Muller (second
draw discarded). Used for GPS / IMU / battery noise.
