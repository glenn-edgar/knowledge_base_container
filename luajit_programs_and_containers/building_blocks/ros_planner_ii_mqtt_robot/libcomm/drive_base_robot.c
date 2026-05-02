// libcomm/drive_base_robot.c
// drive_base logical_robot — see drive_base_robot.h for the contract.
//
// Implementation is intentionally thin: it's a translator between two
// already-designed shapes (bus_msg_t and physics_core's direct C API)
// plus a thin scheduler that calls phys_step at the configured tick
// rate. All "drive_base behavior" — motion control, path following,
// trajectory queueing — already lives inside libphysics.so.

#include "drive_base_robot.h"

#include <stdint.h>
#include <string.h>

// ---- libphysics.so prototypes (don't pull physics_ffi.lua's cdef in) ----
// These mirror what's in libphysics.so. If the public surface of
// libphysics.so changes, this list is the authoritative C-side mirror.

extern phys_t* phys_create(void);
extern void    phys_destroy(phys_t *p);
extern void    phys_set_chassis(phys_t *p, double wb, double r, double m, double I,
                                double lf, double af);
extern void    phys_set_motors(phys_t *p, double max_tau, double max_w,
                               double kp, double ki, double kd, double ek);
extern void    phys_set_follower(phys_t *p, double la_min, double la_kv,
                                 double arr_tol, double hd_tol,
                                 double mla, double maa, double inner_dt,
                                 double xt_abort);
extern void    phys_set_sensors(phys_t *p, double sxy, double sh, double simu,
                                double batt_pct);
extern void    phys_set_battery(phys_t *p, double cap_j, double init_j);
extern void    phys_set_initial_pose(phys_t *p, double x, double y, double h);
extern void    phys_set_seed(phys_t *p, uint64_t seed);
extern void    phys_step(phys_t *p, double dt_sim);
extern uint32_t phys_push_line(phys_t *p, double fx, double fy,
                               double tx, double ty,
                               double h_from, double h_to, double speed);
extern uint32_t phys_push_spline(phys_t *p, double fx, double fy,
                                 double tx, double ty,
                                 double h_from, double h_to, double speed);
extern uint32_t phys_push_rotate(phys_t *p, double from_h, double to_h, double rate);
extern void    phys_request_stop(phys_t *p);
extern void    phys_release_stop(phys_t *p);
extern void    phys_abort_path(phys_t *p);
extern int     phys_queue_depth(phys_t *p);
extern uint32_t phys_active_seg_id(phys_t *p);
extern uint32_t phys_last_done_seg_id(phys_t *p);

typedef struct {
    double x, y, heading;
    double v, omega;
    double sim_t;
} phys_pose_t;

typedef struct {
    uint32_t flags;
    uint32_t active_seg_id;
    uint32_t last_done_seg_id;
    int32_t  queue_depth;
    double   active_progress;
    double   cross_track_err;
    double   heading_err;
    double   energy_used_total;
    double   v_cmd;
} phys_path_status_t;

extern void phys_read_pose       (phys_t *p, phys_pose_t *out);
extern void phys_read_path_status(phys_t *p, phys_path_status_t *out);

// Tool primitives (L6 catalogue).
typedef struct {
    uint32_t flags;
    int32_t  kind;
    double   value;
    double   target;
    double   payload_mass;
    double   battery_j;
    double   battery_capacity_j;
} phys_tool_status_t;

extern void phys_read_tool_status(phys_t *p, int slot, phys_tool_status_t *out);
extern int  phys_begin_tool_move (phys_t *p, int slot, double target, double speed);
extern int  phys_begin_grip      (phys_t *p, int slot);
extern int  phys_begin_release   (phys_t *p, int slot);
extern int  phys_begin_dock      (phys_t *p, int slot);
extern int  phys_begin_charge    (phys_t *p, int slot, double target_j);
extern int  phys_station_at_pose (phys_t *p, int kind);

// ============ HELPERS — bus_msg_t builders ============

static inline void put_le_u16(uint8_t *p, uint16_t v) {
    p[0] = (uint8_t)(v & 0xFF);
    p[1] = (uint8_t)(v >> 8);
}
static inline uint16_t get_le_u16(const uint8_t *p) {
    return (uint16_t)p[0] | ((uint16_t)p[1] << 8);
}
static inline void put_le_u32(uint8_t *p, uint32_t v) {
    p[0] = (uint8_t)(v       & 0xFF);
    p[1] = (uint8_t)(v >>  8 & 0xFF);
    p[2] = (uint8_t)(v >> 16 & 0xFF);
    p[3] = (uint8_t)(v >> 24 & 0xFF);
}
static inline uint32_t get_le_u32(const uint8_t *p) {
    return  (uint32_t)p[0]
         | ((uint32_t)p[1] <<  8)
         | ((uint32_t)p[2] << 16)
         | ((uint32_t)p[3] << 24);
}
static inline void put_f32(uint8_t *p, float f) {
    union { float f; uint32_t u; } x; x.f = f;
    put_le_u32(p, x.u);
}
static inline float get_f32(const uint8_t *p) {
    union { float f; uint32_t u; } x; x.u = get_le_u32(p);
    return x.f;
}

static void msg_clear(bus_msg_t *m, uint8_t dst, uint8_t seq, uint16_t cmd) {
    memset(m, 0, sizeof(*m));
    m->dst_robot = dst;
    m->cmd_lo    = (uint8_t)(cmd & 0xFF);
    m->cmd_hi    = (uint8_t)(cmd >> 8);
    m->seq       = seq;
}

int drive_base_build_push_line(bus_msg_t *m,
                               uint8_t    dst_robot,
                               uint8_t    seq,
                               float fx, float fy,
                               float tx, float ty,
                               float h_from, float h_to,
                               float speed)
{
    if (!m) return -1;
    msg_clear(m, dst_robot, seq, DRV_CMD_PUSH_LINE);
    m->payload_len = 28;
    put_f32(m->payload +  0, fx);
    put_f32(m->payload +  4, fy);
    put_f32(m->payload +  8, tx);
    put_f32(m->payload + 12, ty);
    put_f32(m->payload + 16, h_from);
    put_f32(m->payload + 20, h_to);
    put_f32(m->payload + 24, speed);
    return 0;
}

int drive_base_build_push_spline(bus_msg_t *m,
                                 uint8_t    dst_robot,
                                 uint8_t    seq,
                                 float fx, float fy,
                                 float tx, float ty,
                                 float h_from, float h_to,
                                 float speed)
{
    int rc = drive_base_build_push_line(m, dst_robot, seq,
                                        fx, fy, tx, ty,
                                        h_from, h_to, speed);
    if (rc != 0) return rc;
    m->cmd_lo = (uint8_t)(DRV_CMD_PUSH_SPLINE & 0xFF);
    m->cmd_hi = (uint8_t)(DRV_CMD_PUSH_SPLINE >> 8);
    return 0;
}

int drive_base_build_push_rotate(bus_msg_t *m,
                                 uint8_t    dst_robot,
                                 uint8_t    seq,
                                 float from_h, float to_h, float rate)
{
    if (!m) return -1;
    msg_clear(m, dst_robot, seq, DRV_CMD_PUSH_ROTATE);
    m->payload_len = 12;
    put_f32(m->payload + 0, from_h);
    put_f32(m->payload + 4, to_h);
    put_f32(m->payload + 8, rate);
    return 0;
}

int drive_base_build_simple(bus_msg_t *m,
                            uint8_t    dst_robot,
                            uint8_t    seq,
                            uint16_t   cmd)
{
    if (!m) return -1;
    if (cmd != DRV_CMD_STOP && cmd != DRV_CMD_RESUME && cmd != DRV_CMD_ABORT) {
        return -1;
    }
    msg_clear(m, dst_robot, seq, cmd);
    return 0;
}

// ============ EVENT DECODERS ============

int drive_base_decode_telemetry(const bus_msg_t        *m,
                                drive_base_telemetry_t *out)
{
    if (!m || !out) return -1;
    uint16_t cmd = (uint16_t)m->cmd_lo | ((uint16_t)m->cmd_hi << 8);
    if (cmd != DRV_EVT_TELEMETRY || m->payload_len != 32) return -1;
    out->x                 = get_f32(m->payload +  0);
    out->y                 = get_f32(m->payload +  4);
    out->heading           = get_f32(m->payload +  8);
    out->v                 = get_f32(m->payload + 12);
    out->omega             = get_f32(m->payload + 16);
    out->energy_used_total = get_f32(m->payload + 20);
    out->active_seg_id     = get_le_u32(m->payload + 24);
    out->queue_depth       = get_le_u16(m->payload + 28);
    out->flags             = get_le_u16(m->payload + 30);
    return 0;
}

int drive_base_decode_seg_done(const bus_msg_t       *m,
                               drive_base_seg_done_t *out)
{
    if (!m || !out) return -1;
    uint16_t cmd = (uint16_t)m->cmd_lo | ((uint16_t)m->cmd_hi << 8);
    if (cmd != DRV_EVT_SEG_DONE || m->payload_len != 8) return -1;
    out->seg_id             = get_le_u32(m->payload + 0);
    out->energy_at_complete = get_f32   (m->payload + 4);
    return 0;
}

int drive_base_decode_tool_status(const bus_msg_t          *m,
                                  drive_base_tool_status_t *out)
{
    if (!m || !out) return -1;
    uint16_t cmd = (uint16_t)m->cmd_lo | ((uint16_t)m->cmd_hi << 8);
    if (cmd != DRV_CMD_GET_TOOL_STATUS || m->payload_len != 28) return -1;
    out->flags              = get_le_u32(m->payload +  0);
    out->kind               = (int32_t)get_le_u32(m->payload + 4);
    out->value              = get_f32   (m->payload +  8);
    out->target             = get_f32   (m->payload + 12);
    out->payload_mass       = get_f32   (m->payload + 16);
    out->battery_j          = get_f32   (m->payload + 20);
    out->battery_capacity_j = get_f32   (m->payload + 24);
    return 0;
}

// ============ VTABLE FUNCTIONS ============

static void drive_base_init_fn(void *self_)
{
    drive_base_t *s = (drive_base_t *)self_;
    if (s->tun.schema_version != DRV_TUNABLES_SCHEMA_VERSION) return;

    s->phys = phys_create();
    if (!s->phys) return;

    phys_set_chassis (s->phys,
                      s->tun.wheelbase_m, s->tun.wheel_radius_m,
                      s->tun.mass_kg,     s->tun.inertia_kg_m2,
                      s->tun.lin_friction, s->tun.ang_friction);
    phys_set_motors  (s->phys,
                      s->tun.max_torque_nm, s->tun.max_wheel_rad_s,
                      s->tun.pid_kp, s->tun.pid_ki, s->tun.pid_kd,
                      s->tun.energy_k);
    phys_set_follower(s->phys,
                      s->tun.lookahead_min_m, s->tun.lookahead_k_v,
                      s->tun.arrival_tol_m,   s->tun.heading_tol_rad,
                      s->tun.max_linear_accel, s->tun.max_angular_accel,
                      s->tun.inner_dt_s,       s->tun.cross_track_abort_m);
    phys_set_sensors (s->phys,
                      s->tun.gps_sigma_xy, s->tun.gps_sigma_h,
                      s->tun.imu_sigma,    s->tun.battery_noise_pct);
    phys_set_battery (s->phys,
                      s->tun.battery_capacity_j, s->tun.battery_initial_j);
    phys_set_initial_pose(s->phys,
                          s->tun.init_x, s->tun.init_y, s->tun.init_heading);
    if (s->tun.seed) phys_set_seed(s->phys, s->tun.seed);

    s->started               = 1;
    s->last_tick_ms          = 0;
    s->last_done_phys_seg_id = 0;
    s->last_done_master_seq  = 0;
    s->seg_track_head        = 0;
    s->seg_track_tail        = 0;
    s->seg_track_count       = 0;
}

static void seg_track_push(drive_base_t *s, uint8_t master_seq, uint32_t phys_seg_id)
{
    if (s->seg_track_count >= DRV_SEG_TRACK_DEPTH) {
        // Full — pop oldest. This indicates more in-flight commands than
        // the ring sized for, which the master should not allow given
        // its own outstanding-count budget. If it happens, the oldest
        // tracked seg loses its master_seq attribution and would emit
        // SEG_DONE with seg_id=0 instead.
        s->seg_track_head = (uint8_t)((s->seg_track_head + 1) % DRV_SEG_TRACK_DEPTH);
        s->seg_track_count--;
    }
    s->seg_track[s->seg_track_tail].master_seq  = master_seq;
    s->seg_track[s->seg_track_tail].phys_seg_id = phys_seg_id;
    s->seg_track_tail = (uint8_t)((s->seg_track_tail + 1) % DRV_SEG_TRACK_DEPTH);
    s->seg_track_count++;
}

static int seg_track_pop_match(drive_base_t *s, uint32_t phys_seg_id, uint8_t *master_seq_out)
{
    while (s->seg_track_count > 0) {
        drv_seg_track_t *head = &s->seg_track[s->seg_track_head];
        if (head->phys_seg_id == phys_seg_id) {
            *master_seq_out = head->master_seq;
            s->seg_track_head = (uint8_t)((s->seg_track_head + 1) % DRV_SEG_TRACK_DEPTH);
            s->seg_track_count--;
            return 1;
        }
        // Out-of-order completion: discard heads with smaller seg_id.
        // libphysics reports completions in queue order (FIFO) so this
        // shouldn't happen in practice, but guard against drift.
        if (head->phys_seg_id < phys_seg_id) {
            s->seg_track_head = (uint8_t)((s->seg_track_head + 1) % DRV_SEG_TRACK_DEPTH);
            s->seg_track_count--;
            continue;
        }
        break;
    }
    return 0;
}

static void drive_base_on_msg_fn(void *self_, const bus_msg_t *m)
{
    drive_base_t *s = (drive_base_t *)self_;
    if (!s->phys) return;
    uint16_t cmd = (uint16_t)m->cmd_lo | ((uint16_t)m->cmd_hi << 8);

    switch (cmd) {
    case DRV_CMD_PUSH_LINE: {
        if (m->payload_len < 28) return;
        float fx = get_f32(m->payload +  0);
        float fy = get_f32(m->payload +  4);
        float tx = get_f32(m->payload +  8);
        float ty = get_f32(m->payload + 12);
        float hf = get_f32(m->payload + 16);
        float ht = get_f32(m->payload + 20);
        float sp = get_f32(m->payload + 24);
        uint32_t pid = phys_push_line(s->phys, fx, fy, tx, ty, hf, ht, sp);
        if (pid != 0) seg_track_push(s, m->seq, pid);
        break;
    }
    case DRV_CMD_PUSH_SPLINE: {
        if (m->payload_len < 28) return;
        float fx = get_f32(m->payload +  0);
        float fy = get_f32(m->payload +  4);
        float tx = get_f32(m->payload +  8);
        float ty = get_f32(m->payload + 12);
        float hf = get_f32(m->payload + 16);
        float ht = get_f32(m->payload + 20);
        float sp = get_f32(m->payload + 24);
        uint32_t pid = phys_push_spline(s->phys, fx, fy, tx, ty, hf, ht, sp);
        if (pid != 0) seg_track_push(s, m->seq, pid);
        break;
    }
    case DRV_CMD_PUSH_ROTATE: {
        if (m->payload_len < 12) return;
        float fh = get_f32(m->payload + 0);
        float th = get_f32(m->payload + 4);
        float r  = get_f32(m->payload + 8);
        uint32_t pid = phys_push_rotate(s->phys, fh, th, r);
        if (pid != 0) seg_track_push(s, m->seq, pid);
        break;
    }
    case DRV_CMD_STOP:           phys_request_stop (s->phys); break;
    case DRV_CMD_RESUME:         phys_release_stop (s->phys); break;
    case DRV_CMD_ABORT:          phys_abort_path   (s->phys);
                                 // Stale seg trackers belong to aborted
                                 // path — drop them so a stale completion
                                 // doesn't incorrectly emit SEG_DONE.
                                 s->seg_track_head  = 0;
                                 s->seg_track_tail  = 0;
                                 s->seg_track_count = 0;
                                 break;
    case DRV_CMD_TELEMETRY_ON:   s->telemetry_enabled = 1; break;
    case DRV_CMD_TELEMETRY_OFF:  s->telemetry_enabled = 0; break;
    case DRV_CMD_BEGIN_GRIP: {
        if (m->payload_len < 1) return;
        (void)phys_begin_grip(s->phys, (int)m->payload[0]);
        break;
    }
    case DRV_CMD_BEGIN_RELEASE: {
        if (m->payload_len < 1) return;
        (void)phys_begin_release(s->phys, (int)m->payload[0]);
        break;
    }
    case DRV_CMD_BEGIN_DOCK: {
        if (m->payload_len < 1) return;
        (void)phys_begin_dock(s->phys, (int)m->payload[0]);
        break;
    }
    case DRV_CMD_BEGIN_CHARGE: {
        if (m->payload_len < 5) return;
        int   slot     = (int)m->payload[0];
        float target_j = get_f32(m->payload + 1);
        (void)phys_begin_charge(s->phys, slot, (double)target_j);
        break;
    }
    case DRV_CMD_TOOL_MOVE: {
        if (m->payload_len < 9) return;
        int   slot   = (int)m->payload[0];
        float target = get_f32(m->payload + 1);
        float speed  = get_f32(m->payload + 5);
        (void)phys_begin_tool_move(s->phys, slot, (double)target, (double)speed);
        break;
    }
    case DRV_CMD_GET_TOOL_STATUS: {
        // Master polls tool state; response is 28 B mirroring
        // phys_tool_status_t (doubles → floats). seq echoes request.
        if (!s->outbound || m->payload_len < 1) break;
        int slot = (int)m->payload[0];
        phys_tool_status_t ts;
        phys_read_tool_status(s->phys, slot, &ts);

        bus_msg_t resp;
        msg_clear(&resp, 0, m->seq, DRV_CMD_GET_TOOL_STATUS);
        resp.src_addr   = s->bus_addr;
        resp.payload_len = 28;
        put_le_u32(resp.payload +  0, ts.flags);
        put_le_u32(resp.payload +  4, (uint32_t)ts.kind);
        put_f32   (resp.payload +  8, (float)ts.value);
        put_f32   (resp.payload + 12, (float)ts.target);
        put_f32   (resp.payload + 16, (float)ts.payload_mass);
        put_f32   (resp.payload + 20, (float)ts.battery_j);
        put_f32   (resp.payload + 24, (float)ts.battery_capacity_j);
        (void)bus_msgq_put(s->outbound, &resp);
        break;
    }
    case DRV_CMD_GET_STATION: {
        // Master queries "is the rover docked at a station of `kind`?".
        // Response: 4 B i32 (negative = none, else station index).
        if (!s->outbound || m->payload_len < 1) break;
        int kind = (int)m->payload[0];
        int idx  = phys_station_at_pose(s->phys, kind);

        bus_msg_t resp;
        msg_clear(&resp, 0, m->seq, DRV_CMD_GET_STATION);
        resp.src_addr   = s->bus_addr;
        resp.payload_len = 4;
        put_le_u32(resp.payload + 0, (uint32_t)(int32_t)idx);
        (void)bus_msgq_put(s->outbound, &resp);
        break;
    }
    case DRV_CMD_GET_TELEMETRY: {
        // Request-response: master polls; we reply with current state
        // as a 32-byte payload. Manager refrains from auto-ACK'ing
        // GET_* commands so this response IS the response. seq echoes
        // the request seq for libcomm's master correlation.
        if (!s->outbound) break;
        phys_pose_t        pose;
        phys_path_status_t status;
        phys_read_pose       (s->phys, &pose);
        phys_read_path_status(s->phys, &status);

        bus_msg_t resp;
        msg_clear(&resp, 0, m->seq, DRV_CMD_GET_TELEMETRY);
        resp.src_addr   = s->bus_addr;
        resp.payload_len = 32;
        put_f32   (resp.payload +  0, (float)pose.x);
        put_f32   (resp.payload +  4, (float)pose.y);
        put_f32   (resp.payload +  8, (float)pose.heading);
        put_f32   (resp.payload + 12, (float)pose.v);
        put_f32   (resp.payload + 16, (float)pose.omega);
        put_f32   (resp.payload + 20, (float)status.energy_used_total);
        // 4 bytes for last_done_master_seq (zero-extended), then
        // 2 + 2 for queue_depth + flags.
        put_le_u32(resp.payload + 24, (uint32_t)s->last_done_master_seq);
        put_le_u16(resp.payload + 28, (uint16_t)(status.queue_depth & 0xFFFF));
        put_le_u16(resp.payload + 30, (uint16_t)(status.flags       & 0xFFFF));
        (void)bus_msgq_put(s->outbound, &resp);
        break;
    }
    default:
        // Unknown command — fail-stop discipline says raise it as a
        // fault event rather than silently dropping. v1 just logs via
        // ignoring; manager-side will produce a NAK at the wire layer
        // before it ever gets here.
        break;
    }
}

static void drive_base_emit_telemetry(drive_base_t *s)
{
    if (!s->outbound || !s->phys) return;
    phys_pose_t        pose;
    phys_path_status_t status;
    phys_read_pose       (s->phys, &pose);
    phys_read_path_status(s->phys, &status);

    // Periodic telemetry is opt-in; without it, drive_base still
    // advances physics every tick but stays quiet on the wire.
    // SEG_DONE (below) is unconditional — completion edges always
    // flow because they're correlated to commands the master sent.
    if (s->telemetry_enabled) {
        bus_msg_t evt;
        msg_clear(&evt, /*dst*/0, /*seq*/0, DRV_EVT_TELEMETRY);
        evt.src_addr   = s->bus_addr;
        evt.payload_len = 32;
        put_f32   (evt.payload +  0, (float)pose.x);
        put_f32   (evt.payload +  4, (float)pose.y);
        put_f32   (evt.payload +  8, (float)pose.heading);
        put_f32   (evt.payload + 12, (float)pose.v);
        put_f32   (evt.payload + 16, (float)pose.omega);
        put_f32   (evt.payload + 20, (float)status.energy_used_total);
        put_le_u32(evt.payload + 24, status.active_seg_id);
        put_le_u16(evt.payload + 28, (uint16_t)(status.queue_depth & 0xFFFF));
        put_le_u16(evt.payload + 30, (uint16_t)(status.flags       & 0xFFFF));
        (void)bus_msgq_put(s->outbound, &evt);
    }

    // Edge-detect seg-done: emit DRV_EVT_SEG_DONE on each new completion.
    // The seg_id field on the wire carries the MASTER's bus_msg.seq for
    // the originating PUSH_*, popped from seg_track via FIFO match —
    // not libphysics's internal seg_id, which the master never saw.
    if (status.last_done_seg_id != 0
     && status.last_done_seg_id != s->last_done_phys_seg_id) {
        uint8_t  master_seq = 0;
        (void)seg_track_pop_match(s, status.last_done_seg_id, &master_seq);

        // Cache for GET_TELEMETRY responses (master HAL reads this
        // to track segment completion via libcomm's request/response
        // model — unsolicited DRV_EVT_SEG_DONE below is dropped on
        // the wire by libcomm's master_handle_frame).
        s->last_done_master_seq = master_seq;

        // Unsolicited SEG_DONE event — kept for in-process tests
        // (test_drive_base, test_dongle_catalogue) that observe
        // ext_tx_q directly. Master HAL doesn't see these over the
        // wire; it polls GET_TELEMETRY instead.
        bus_msg_t done;
        msg_clear(&done, 0, 0, DRV_EVT_SEG_DONE);
        done.src_addr   = s->bus_addr;
        done.payload_len = 8;
        put_le_u32(done.payload + 0, (uint32_t)master_seq);
        put_f32   (done.payload + 4, (float)status.energy_used_total);
        (void)bus_msgq_put(s->outbound, &done);
        s->last_done_phys_seg_id = status.last_done_seg_id;
    }
}

static void drive_base_tick_fn(void *self_, uint32_t now_ms)
{
    drive_base_t *s = (drive_base_t *)self_;
    if (!s->phys) return;

    // Compute dt from last tick. First tick uses tick_period_ms as dt
    // since last_tick_ms is 0 from init.
    uint32_t dt_ms;
    if (s->last_tick_ms == 0) {
        dt_ms = s->tick_period_ms ? s->tick_period_ms : 10;
    } else {
        dt_ms = now_ms - s->last_tick_ms;
        // Cap dt against scheduler stalls — phys_step handles this
        // internally via its inner loop, but a 10× slip would still
        // yield bad simulation. Cap at 5× nominal.
        uint32_t cap = (s->tick_period_ms ? s->tick_period_ms : 10) * 5u;
        if (dt_ms > cap) dt_ms = cap;
    }
    s->last_tick_ms = now_ms;

    phys_step(s->phys, (double)dt_ms / 1000.0);
    drive_base_emit_telemetry(s);
}

static void drive_base_shutdown_fn(void *self_)
{
    drive_base_t *s = (drive_base_t *)self_;
    if (s->phys) {
        phys_destroy(s->phys);
        s->phys = NULL;
    }
    s->started = 0;
}

const logical_robot_vtable_t drive_base_vtable = {
    .init           = drive_base_init_fn,
    .on_msg         = drive_base_on_msg_fn,
    .tick           = drive_base_tick_fn,
    .shutdown       = drive_base_shutdown_fn,
    .tick_period_ms = 10,           // 100 Hz; the inner physics loop
                                    // runs at 200 Hz (inner_dt_s default)
};
