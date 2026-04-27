/*
 * physics_core.c -- Differential-drive robot physics + path-tracking controller.
 *
 *   Layer owned by C (200 Hz inner loop, free-running via phys_step(dt_sim)):
 *     - diff-drive chassis dynamics
 *     - motor PID per wheel
 *     - pure-pursuit path follower over a segment queue
 *     - line + cubic-Bezier-spline + in-place rotate segments
 *     - stop-request for non-motion VNs
 *     - simple 1-DoF arm, binary gripper, passive dock (charger)
 *     - payload mass coupling, battery + charging, GPS/IMU noise
 *
 *   Lua sees only the FFI-exported API at the bottom of this file.
 */

#include <math.h>
#include <stdint.h>
#include <stdlib.h>
#include <string.h>

#ifndef M_PI
#define M_PI 3.14159265358979323846
#endif

/* ---------- flags (exposed verbatim to Lua via ffi enums) --------------- */

#define PATH_F_TRACKING     0x01u
#define PATH_F_STOPPED      0x02u
#define PATH_F_QUEUE_EMPTY  0x04u
#define PATH_F_FAULT        0x08u
#define PATH_F_SEG_DONE     0x10u   /* latched when last_done_seg_id advances */

#define TOOL_F_AT_TARGET    0x01u
#define TOOL_F_GRASPED      0x02u
#define TOOL_F_RELEASED     0x04u
#define TOOL_F_DOCKED       0x08u
#define TOOL_F_CHARGING     0x10u
#define TOOL_F_FAULT        0x80u

#define SEG_LINE    0
#define SEG_SPLINE  1
#define SEG_ROTATE  2

#define TOOL_NONE         0
#define TOOL_REVOLUTE_1   1
#define TOOL_BINARY       2
#define TOOL_PASSIVE_DOCK 3

#define STATION_CHARGER   1
#define STATION_LOAD_DOCK 2
#define STATION_PAINT     3
#define STATION_ASSEMBLY  4

#define ARC_LUT_N 33               /* 32 intervals, 33 samples */
#define QUEUE_CAP 32
#define MAX_TOOLS  8
#define MAX_STATIONS 16

/* ---------- POD structs exposed to Lua --------------------------------- */

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

typedef struct {
    uint32_t flags;
    int32_t  kind;
    double   value;
    double   target;
    double   payload_mass;
    double   battery_j;
    double   battery_capacity_j;
} phys_tool_status_t;

/* ---------- internal ---------------------------------------------------- */

typedef struct {
    int      type;
    uint32_t seg_id;
    double   p0x, p0y, p1x, p1y, p2x, p2y, p3x, p3y;    /* Bezier */
    double   h_from, h_to;
    double   speed;
    double   arc_len;
    double   lut_s[ARC_LUT_N];    /* arc-length at parameter t[i] = i/(N-1) */
    /* rotate only */
    double   rot_from_h, rot_to_h, rot_at_x, rot_at_y;
} segment_t;

typedef struct {
    int      in_use;
    int      kind;
    char     name[32];
    double   value;
    double   target;
    double   speed;
    double   lim_min, lim_max;
    double   max_speed;
    double   transition_s;
    double   op_t;             /* sim-time offset into op */
    int      op;               /* 0 idle, 1 move, 2 grip, 3 release, 4 dock, 5 charge_hold */
    uint32_t bits;
    /* passive_dock spec */
    double   offset_x, offset_y, offset_z;
    double   dock_tol_m, dock_tol_h;
} tool_t;

typedef struct {
    int      kind;
    double   x, y, heading;
    double   tol_m, tol_h;
    double   param1, param2;     /* charger: rate_w, -- ; load_dock: payload_mass, arm_pickup */
    char     id[32];
    char     payload_id[32];
} station_t;

struct phys_s {
    /* --- config --- */
    int    kinematics;        /* 0 diff */
    double wheelbase, wheel_radius, mass_kg, inertia;
    double lin_friction, ang_friction;
    double max_torque, max_wheel_rad_s;
    double kp, ki, kd, energy_k;
    double lookahead_min, lookahead_k_v;
    double arrival_tol, heading_tol;
    double max_lin_accel, max_ang_accel;
    double stopped_eps_v, stopped_eps_omega;
    double inner_dt;
    double cross_track_abort;
    int    queue_capacity;

    double gps_sigma_xy, gps_sigma_h;
    double imu_sigma;
    double battery_noise_pct;

    /* --- state --- */
    double   sim_t;
    double   x, y, heading;
    double   v_l, v_r;                /* current wheel linear speeds (m/s) */
    double   v_l_target, v_r_target;  /* setpoints from follower */
    double   int_l, int_r;            /* PID integrators */
    double   prev_l, prev_r;          /* prev error (for D) */
    double   torque_l, torque_r;

    /* --- path queue (ring buffer) --- */
    segment_t queue[QUEUE_CAP];
    int       q_head, q_count;
    double    active_s;
    double    active_progress;
    uint32_t  next_seg_id;
    uint32_t  last_done_seg_id;

    /* --- status --- */
    uint32_t flags;
    double   cross_track_err;
    double   heading_err;
    double   energy_used_total;
    double   v_cmd;                  /* ramped commanded linear speed */
    int      stop_requested;

    /* --- tools / stations --- */
    tool_t   tools[MAX_TOOLS];
    int      tool_count;
    station_t stations[MAX_STATIONS];
    int      station_count;

    /* --- battery + payload --- */
    double   battery_j, battery_capacity_j;
    double   payload_mass;
    int      payload_attached;
    int      charging;
    int      charger_idx;
    double   charge_target_j;

    /* --- rng --- */
    uint64_t rng_state;
};
typedef struct phys_s phys_t;

/* ====================================================================== */
/* RNG + noise                                                            */
/* ====================================================================== */

static uint64_t rng_next(phys_t *p) {
    uint64_t x = p->rng_state;
    if (x == 0) x = 0x9E3779B97F4A7C15ULL;
    x ^= x << 13; x ^= x >> 7; x ^= x << 17;
    p->rng_state = x;
    return x;
}

static double rng_uniform(phys_t *p) {
    return (rng_next(p) >> 11) * (1.0 / (double)(1ULL << 53));
}

/* Box-Muller once per call (we discard the second draw for simplicity) */
static double rng_gauss(phys_t *p, double sigma) {
    if (sigma <= 0.0) return 0.0;
    double u1 = rng_uniform(p);
    double u2 = rng_uniform(p);
    if (u1 < 1e-12) u1 = 1e-12;
    return sigma * sqrt(-2.0 * log(u1)) * cos(2.0 * M_PI * u2);
}

static double wrap_angle(double a) {
    while (a >  M_PI) a -= 2.0 * M_PI;
    while (a < -M_PI) a += 2.0 * M_PI;
    return a;
}

/* ====================================================================== */
/* Bezier helpers                                                         */
/* ====================================================================== */

static void bezier_point(const segment_t *s, double t, double *px, double *py) {
    double u  = 1.0 - t;
    double u2 = u * u, u3 = u2 * u;
    double t2 = t * t, t3 = t2 * t;
    *px = u3 * s->p0x + 3.0 * u2 * t * s->p1x + 3.0 * u * t2 * s->p2x + t3 * s->p3x;
    *py = u3 * s->p0y + 3.0 * u2 * t * s->p1y + 3.0 * u * t2 * s->p2y + t3 * s->p3y;
}

__attribute__((unused))
static void bezier_tangent(const segment_t *s, double t, double *tx, double *ty) {
    double u = 1.0 - t;
    *tx = 3.0 * (u * u * (s->p1x - s->p0x)
               + 2.0 * u * t * (s->p2x - s->p1x)
               + t * t * (s->p3x - s->p2x));
    *ty = 3.0 * (u * u * (s->p1y - s->p0y)
               + 2.0 * u * t * (s->p2y - s->p1y)
               + t * t * (s->p3y - s->p2y));
}

static void build_arc_lut(segment_t *s) {
    double px_prev, py_prev;
    bezier_point(s, 0.0, &px_prev, &py_prev);
    s->lut_s[0] = 0.0;
    for (int i = 1; i < ARC_LUT_N; i++) {
        double t = (double)i / (double)(ARC_LUT_N - 1);
        double px, py;
        bezier_point(s, t, &px, &py);
        double dx = px - px_prev, dy = py - py_prev;
        s->lut_s[i] = s->lut_s[i-1] + sqrt(dx * dx + dy * dy);
        px_prev = px; py_prev = py;
    }
    s->arc_len = s->lut_s[ARC_LUT_N - 1];
}

/* Convert arc-length s to Bezier param t by LUT lookup + linear interp */
static double arc_to_param(const segment_t *s, double target_s) {
    if (target_s <= 0.0)       return 0.0;
    if (target_s >= s->arc_len) return 1.0;
    int lo = 0, hi = ARC_LUT_N - 1;
    while (hi - lo > 1) {
        int mid = (lo + hi) / 2;
        if (s->lut_s[mid] < target_s) lo = mid; else hi = mid;
    }
    double sl = s->lut_s[lo], sh = s->lut_s[hi];
    double frac = (sh > sl) ? (target_s - sl) / (sh - sl) : 0.0;
    double tl = (double)lo / (double)(ARC_LUT_N - 1);
    double th = (double)hi / (double)(ARC_LUT_N - 1);
    return tl + frac * (th - tl);
}

/* Nearest point on segment to (x,y) -> returns arc-length s and distance */
static void project_onto_segment(const segment_t *s, double x, double y,
                                 double *out_s, double *out_dist) {
    double best_s = 0.0, best_d2 = 1e30;
    for (int i = 0; i < ARC_LUT_N; i++) {
        double t = (double)i / (double)(ARC_LUT_N - 1);
        double px, py;
        bezier_point(s, t, &px, &py);
        double dx = px - x, dy = py - y;
        double d2 = dx * dx + dy * dy;
        if (d2 < best_d2) { best_d2 = d2; best_s = s->lut_s[i]; }
    }
    *out_s    = best_s;
    *out_dist = sqrt(best_d2);
}

/* ====================================================================== */
/* Queue helpers                                                          */
/* ====================================================================== */

static segment_t* q_front(phys_t *p) {
    if (p->q_count == 0) return NULL;
    return &p->queue[p->q_head];
}

static segment_t* q_peek(phys_t *p, int offset) {
    if (offset >= p->q_count) return NULL;
    return &p->queue[(p->q_head + offset) % QUEUE_CAP];
}

static int q_push(phys_t *p, const segment_t *s) {
    if (p->q_count >= p->queue_capacity) return -1;
    int idx = (p->q_head + p->q_count) % QUEUE_CAP;
    p->queue[idx] = *s;
    p->q_count++;
    return 0;
}

static void q_pop(phys_t *p) {
    if (p->q_count == 0) return;
    p->q_head = (p->q_head + 1) % QUEUE_CAP;
    p->q_count--;
    p->active_s = 0.0;
    p->active_progress = 0.0;
}

static void q_clear(phys_t *p) {
    p->q_head = 0; p->q_count = 0;
    p->active_s = 0.0; p->active_progress = 0.0;
}

/* ====================================================================== */
/* Path follower (pure pursuit)                                           */
/* ====================================================================== */

/* Get path point at arc-length s, walking forward into next segments
 * if s exceeds the active segment's length. */
static void path_point_at(phys_t *p, double s_req, double *px, double *py) {
    int off = 0;
    double s = s_req;
    segment_t *cur = q_front(p);
    if (!cur) { *px = p->x; *py = p->y; return; }
    while (s > cur->arc_len && off + 1 < p->q_count) {
        s -= cur->arc_len;
        off++;
        cur = q_peek(p, off);
        if (!cur) break;
    }
    if (!cur) { *px = p->x; *py = p->y; return; }
    if (cur->type == SEG_ROTATE) {
        *px = cur->rot_at_x; *py = cur->rot_at_y;
        return;
    }
    if (s > cur->arc_len) s = cur->arc_len;
    double t = arc_to_param(cur, s);
    bezier_point(cur, t, px, py);
}

/* Returns 1 if an active segment exists and we set v*,w*. */
static int follower_step(phys_t *p, double dt, double *v_out, double *w_out) {
    segment_t *cur = q_front(p);
    if (!cur) { *v_out = 0.0; *w_out = 0.0; return 0; }

    /* ---- path-rotate: trivial sub-case --------------------------- */
    if (cur->type == SEG_ROTATE) {
        double err = wrap_angle(cur->rot_to_h - p->heading);
        p->heading_err = err;
        p->cross_track_err = 0.0;
        p->active_s = cur->arc_len * (1.0 - fabs(err) / fmax(fabs(cur->rot_to_h - cur->rot_from_h), 1e-6));
        p->active_progress = (cur->arc_len > 0.0) ? (p->active_s / cur->arc_len) : 1.0;
        if (fabs(err) < p->heading_tol) {
            p->last_done_seg_id = cur->seg_id;
            p->flags |= PATH_F_SEG_DONE;
            q_pop(p);
            return follower_step(p, dt, v_out, w_out);
        }
        double rate = (cur->speed > 0.0) ? cur->speed : 1.0;
        *v_out = 0.0;
        *w_out = (err >= 0.0 ? 1.0 : -1.0) * rate;
        return 1;
    }

    /* ---- line / spline ------------------------------------------ */
    double s_closest, dist_closest;
    project_onto_segment(cur, p->x, p->y, &s_closest, &dist_closest);
    p->cross_track_err = dist_closest;
    p->active_s = s_closest;
    p->active_progress = (cur->arc_len > 0.0) ? (s_closest / cur->arc_len) : 1.0;

    /* Arrival: close to end of segment AND close to end-point in world */
    double dx_end = cur->p3x - p->x, dy_end = cur->p3y - p->y;
    double dist_end = sqrt(dx_end * dx_end + dy_end * dy_end);
    if (s_closest >= cur->arc_len - p->arrival_tol || dist_end < p->arrival_tol) {
        p->last_done_seg_id = cur->seg_id;
        p->flags |= PATH_F_SEG_DONE;
        q_pop(p);
        return follower_step(p, dt, v_out, w_out);
    }

    /* Cross-track abort */
    if (dist_closest > p->cross_track_abort) {
        p->flags |= PATH_F_FAULT;
        *v_out = 0.0; *w_out = 0.0;
        return 1;
    }

    double speed_eff = cur->speed;
    if (p->stop_requested) speed_eff = 0.0;

    double L = p->lookahead_min + p->lookahead_k_v * fabs(p->v_cmd);
    if (L < p->lookahead_min) L = p->lookahead_min;

    double goal_px, goal_py;
    path_point_at(p, s_closest + L, &goal_px, &goal_py);

    double gdx = goal_px - p->x, gdy = goal_py - p->y;
    double ld = sqrt(gdx * gdx + gdy * gdy);
    if (ld < 1e-6) ld = 1e-6;

    double target_h = atan2(gdy, gdx);
    double alpha = wrap_angle(target_h - p->heading);
    p->heading_err = alpha;

    double kappa = 2.0 * sin(alpha) / ld;
    double v_want = speed_eff;
    /* Slow near sharp curvature */
    double v_limit_curve = (fabs(kappa) > 1e-3) ? (1.5 / fabs(kappa)) : v_want;
    if (v_want > v_limit_curve) v_want = v_limit_curve;

    *v_out = v_want;
    *w_out = v_want * kappa;
    return 1;
}

/* ====================================================================== */
/* Motor PID + chassis dynamics                                           */
/* ====================================================================== */

static double clamp(double x, double lo, double hi) {
    return x < lo ? lo : (x > hi ? hi : x);
}

static void motor_pid_step(phys_t *p, double dt) {
    double e_l = p->v_l_target - p->v_l;
    double e_r = p->v_r_target - p->v_r;

    p->int_l += e_l * dt;
    p->int_r += e_r * dt;
    /* Anti-windup: clamp integrator */
    double imax = p->max_torque / (p->ki > 0.0 ? p->ki : 1.0);
    p->int_l = clamp(p->int_l, -imax, imax);
    p->int_r = clamp(p->int_r, -imax, imax);

    double de_l = (e_l - p->prev_l) / (dt > 0.0 ? dt : 1e-6);
    double de_r = (e_r - p->prev_r) / (dt > 0.0 ? dt : 1e-6);
    p->prev_l = e_l; p->prev_r = e_r;

    p->torque_l = clamp(p->kp * e_l + p->ki * p->int_l + p->kd * de_l,
                        -p->max_torque, p->max_torque);
    p->torque_r = clamp(p->kp * e_r + p->ki * p->int_r + p->kd * de_r,
                        -p->max_torque, p->max_torque);

    double m_total = p->mass_kg + p->payload_mass;
    double m_eff_wheel = 0.5 * m_total;              /* lumped per wheel */
    double r = p->wheel_radius;
    /* Wheel force = torque / radius; subtract viscous loss */
    double F_l = p->torque_l / (r > 0.0 ? r : 1e-3) - p->lin_friction * p->v_l;
    double F_r = p->torque_r / (r > 0.0 ? r : 1e-3) - p->lin_friction * p->v_r;
    p->v_l += (F_l / m_eff_wheel) * dt;
    p->v_r += (F_r / m_eff_wheel) * dt;

    /* Clamp to physical max wheel speed */
    double v_max = p->max_wheel_rad_s * r;
    p->v_l = clamp(p->v_l, -v_max, v_max);
    p->v_r = clamp(p->v_r, -v_max, v_max);

    /* Energy: sum of |tau * omega_w| */
    double omega_l = p->v_l / (r > 0.0 ? r : 1e-3);
    double omega_r = p->v_r / (r > 0.0 ? r : 1e-3);
    double power = fabs(p->torque_l * omega_l) + fabs(p->torque_r * omega_r);
    p->energy_used_total += p->energy_k * power * dt;
    p->battery_j -= p->energy_k * power * dt;
    if (p->battery_j < 0.0) p->battery_j = 0.0;
}

static void chassis_step(phys_t *p, double dt) {
    double v = 0.5 * (p->v_l + p->v_r);
    double w = (p->v_r - p->v_l) / (p->wheelbase > 0.0 ? p->wheelbase : 0.3);

    /* Angular friction bleed */
    w -= p->ang_friction * w * dt;

    p->x       += v * cos(p->heading) * dt;
    p->y       += v * sin(p->heading) * dt;
    p->heading  = wrap_angle(p->heading + w * dt);
}

/* ====================================================================== */
/* Speed-ramp toward follower's v*, and translate to wheel setpoints      */
/* ====================================================================== */

static void compute_setpoints(phys_t *p, double v_want, double w_want, double dt) {
    /* Ramp v_cmd toward v_want */
    double v_diff = v_want - p->v_cmd;
    double amax = p->max_lin_accel * dt;
    if (v_diff >  amax) v_diff =  amax;
    if (v_diff < -amax) v_diff = -amax;
    p->v_cmd += v_diff;

    /* omega ramp */
    double w_max = p->max_ang_accel * dt;
    /* No separate state for omega cmd — the follower output becomes direct input;
       but we still need bounded angular accel, so clamp by adjusting w_want
       relative to current chassis omega. */
    double w_cur = (p->v_r - p->v_l) / (p->wheelbase > 0.0 ? p->wheelbase : 0.3);
    double w_diff = w_want - w_cur;
    if (w_diff >  w_max) w_diff =  w_max;
    if (w_diff < -w_max) w_diff = -w_max;
    double w_use = w_cur + w_diff;

    double v_use = p->v_cmd;
    p->v_l_target = v_use - 0.5 * w_use * p->wheelbase;
    p->v_r_target = v_use + 0.5 * w_use * p->wheelbase;
}

/* ====================================================================== */
/* Tools                                                                  */
/* ====================================================================== */

static int station_at_pose(phys_t *p, int kind_filter) {
    for (int i = 0; i < p->station_count; i++) {
        station_t *st = &p->stations[i];
        if (kind_filter && st->kind != kind_filter) continue;
        double dx = p->x - st->x, dy = p->y - st->y;
        double dh = fabs(wrap_angle(p->heading - st->heading));
        if (sqrt(dx * dx + dy * dy) <= st->tol_m && dh <= st->tol_h) return i;
    }
    return -1;
}

static void tool_step(phys_t *p, tool_t *t, double dt) {
    if (!t->in_use) return;
    t->op_t += dt;
    if (t->op == 0) return;

    if (t->kind == TOOL_REVOLUTE_1 && t->op == 1) {
        double step = t->speed * dt;
        double diff = t->target - t->value;
        if (fabs(diff) <= step) {
            t->value = t->target;
            t->bits = TOOL_F_AT_TARGET;
            t->op = 0;
        } else {
            t->value += (diff > 0.0 ? step : -step);
        }
        return;
    }

    if (t->kind == TOOL_BINARY) {
        /* transition over transition_s */
        double total = t->transition_s > 0.0 ? t->transition_s : 0.4;
        if (t->op_t >= total) {
            t->value = t->target;
            uint32_t b = TOOL_F_AT_TARGET;
            if (t->op == 2) b |= TOOL_F_GRASPED;
            if (t->op == 3) b |= TOOL_F_RELEASED;
            t->bits = b;
            t->op = 0;

            /* Grip/release payload interaction with nearby station */
            if (t->op_t >= total) {
                if ((b & TOOL_F_GRASPED) && !p->payload_attached) {
                    int si = station_at_pose(p, STATION_LOAD_DOCK);
                    if (si >= 0) {
                        p->payload_attached = 1;
                        p->payload_mass = p->stations[si].param1;
                    }
                } else if ((b & TOOL_F_RELEASED) && p->payload_attached) {
                    p->payload_attached = 0;
                    p->payload_mass = 0.0;
                }
            }
        }
        return;
    }

    if (t->kind == TOOL_PASSIVE_DOCK && t->op == 4) {
        int si = station_at_pose(p, STATION_CHARGER);
        if (si >= 0) {
            t->bits = TOOL_F_DOCKED | TOOL_F_AT_TARGET;
        } else {
            t->bits = TOOL_F_FAULT;
        }
        t->op = 0;
        return;
    }

    if (t->kind == TOOL_PASSIVE_DOCK && t->op == 5) {
        /* charging */
        int si = p->charger_idx;
        if (si < 0 || si >= p->station_count) {
            t->bits = TOOL_F_FAULT; t->op = 0; p->charging = 0; return;
        }
        double rate_w = p->stations[si].param1;
        p->battery_j += rate_w * dt;
        if (p->battery_j >= p->battery_capacity_j) {
            p->battery_j = p->battery_capacity_j;
        }
        if (p->battery_j >= p->charge_target_j) {
            t->bits = TOOL_F_AT_TARGET | TOOL_F_DOCKED;
            t->op = 0;
            p->charging = 0;
        } else {
            t->bits = TOOL_F_DOCKED | TOOL_F_CHARGING;
        }
        return;
    }
}

/* ====================================================================== */
/* Inner step                                                             */
/* ====================================================================== */

static void inner_step(phys_t *p, double dt) {
    double v_want = 0.0, w_want = 0.0;
    int has_active = follower_step(p, dt, &v_want, &w_want);

    if (!has_active) {
        v_want = 0.0; w_want = 0.0;
        p->flags |= PATH_F_QUEUE_EMPTY;
    } else {
        p->flags &= ~PATH_F_QUEUE_EMPTY;
    }
    if (p->stop_requested) v_want = 0.0;

    compute_setpoints(p, v_want, w_want, dt);
    motor_pid_step(p, dt);
    chassis_step(p, dt);

    /* Stopped flag */
    double v_mag = 0.5 * fabs(p->v_l + p->v_r);
    double w_mag = fabs((p->v_r - p->v_l) / (p->wheelbase > 0.0 ? p->wheelbase : 0.3));
    if (v_mag < p->stopped_eps_v && w_mag < p->stopped_eps_omega) {
        p->flags |= PATH_F_STOPPED;
    } else {
        p->flags &= ~PATH_F_STOPPED;
    }
    if (has_active && !(p->flags & PATH_F_FAULT)) {
        p->flags |= PATH_F_TRACKING;
    } else {
        p->flags &= ~PATH_F_TRACKING;
    }

    /* tool updates */
    for (int i = 0; i < p->tool_count; i++) {
        tool_step(p, &p->tools[i], dt);
    }

    p->sim_t += dt;
}

/* ====================================================================== */
/*                     EXPORTED FFI API                                   */
/* ====================================================================== */

#ifdef __GNUC__
#define EXPORT __attribute__((visibility("default")))
#else
#define EXPORT
#endif

EXPORT phys_t* phys_create(void) {
    phys_t *p = (phys_t*)calloc(1, sizeof(phys_t));
    if (!p) return NULL;
    /* Defaults */
    p->kinematics = 0;
    p->wheelbase = 0.30; p->wheel_radius = 0.04;
    p->mass_kg = 8.0; p->inertia = 0.12;
    p->lin_friction = 0.8; p->ang_friction = 0.4;
    p->max_torque = 1.5; p->max_wheel_rad_s = 30.0;
    p->kp = 8.0; p->ki = 4.0; p->kd = 0.05; p->energy_k = 1.0;
    p->lookahead_min = 0.20; p->lookahead_k_v = 0.40;
    p->arrival_tol = 0.05; p->heading_tol = 0.05;
    p->max_lin_accel = 1.0; p->max_ang_accel = 3.0;
    p->stopped_eps_v = 0.01; p->stopped_eps_omega = 0.02;
    p->inner_dt = 0.005;
    p->cross_track_abort = 5.0;
    p->queue_capacity = QUEUE_CAP;
    p->battery_capacity_j = 100000.0; p->battery_j = 100000.0;
    p->rng_state = 0x9E3779B97F4A7C15ULL;
    p->charger_idx = -1;
    p->flags = PATH_F_QUEUE_EMPTY | PATH_F_STOPPED;
    p->next_seg_id = 1;
    return p;
}

EXPORT void phys_destroy(phys_t *p) { if (p) free(p); }

EXPORT void phys_set_chassis(phys_t *p, double wb, double r, double m, double I,
                             double lf, double af) {
    if (!p) return;
    p->wheelbase = wb; p->wheel_radius = r; p->mass_kg = m; p->inertia = I;
    p->lin_friction = lf; p->ang_friction = af;
}

EXPORT void phys_set_motors(phys_t *p, double max_tau, double max_w,
                            double kp, double ki, double kd, double ek) {
    if (!p) return;
    p->max_torque = max_tau; p->max_wheel_rad_s = max_w;
    p->kp = kp; p->ki = ki; p->kd = kd; p->energy_k = ek;
}

EXPORT void phys_set_follower(phys_t *p, double la_min, double la_kv,
                              double arr_tol, double hd_tol,
                              double mla, double maa, double inner_dt,
                              double xt_abort) {
    if (!p) return;
    p->lookahead_min = la_min; p->lookahead_k_v = la_kv;
    p->arrival_tol = arr_tol; p->heading_tol = hd_tol;
    p->max_lin_accel = mla; p->max_ang_accel = maa;
    p->inner_dt = inner_dt > 0.0 ? inner_dt : 0.005;
    p->cross_track_abort = xt_abort;
}

EXPORT void phys_set_sensors(phys_t *p, double sxy, double sh, double simu,
                             double batt_pct) {
    if (!p) return;
    p->gps_sigma_xy = sxy; p->gps_sigma_h = sh;
    p->imu_sigma = simu; p->battery_noise_pct = batt_pct;
}

EXPORT void phys_set_battery(phys_t *p, double cap_j, double init_j) {
    if (!p) return;
    p->battery_capacity_j = cap_j; p->battery_j = init_j;
}

EXPORT void phys_set_initial_pose(phys_t *p, double x, double y, double h) {
    if (!p) return;
    p->x = x; p->y = y; p->heading = h;
    p->v_l = p->v_r = 0.0;
}

EXPORT void phys_set_seed(phys_t *p, uint64_t seed) {
    if (!p) return;
    p->rng_state = seed ? seed : 0x9E3779B97F4A7C15ULL;
}

EXPORT int phys_add_tool(phys_t *p, int slot, int kind,
                         double lim_min, double lim_max,
                         double max_speed, double transition_s) {
    if (!p || slot < 0 || slot >= MAX_TOOLS) return -1;
    tool_t *t = &p->tools[slot];
    memset(t, 0, sizeof(*t));
    t->in_use = 1; t->kind = kind;
    t->lim_min = lim_min; t->lim_max = lim_max;
    t->max_speed = max_speed; t->transition_s = transition_s;
    t->value = 0.0;
    if (slot + 1 > p->tool_count) p->tool_count = slot + 1;
    return 0;
}

EXPORT int phys_add_station(phys_t *p, int kind, double x, double y, double h,
                            double tol_m, double tol_h,
                            double param1, double param2) {
    if (!p || p->station_count >= MAX_STATIONS) return -1;
    station_t *st = &p->stations[p->station_count++];
    memset(st, 0, sizeof(*st));
    st->kind = kind;
    st->x = x; st->y = y; st->heading = h;
    st->tol_m = tol_m; st->tol_h = tol_h;
    st->param1 = param1; st->param2 = param2;
    return p->station_count - 1;
}

EXPORT double phys_sim_time(phys_t *p) { return p ? p->sim_t : 0.0; }

/* Step: advance sim by dt_sim, in N inner steps of inner_dt. */
EXPORT void phys_step(phys_t *p, double dt_sim) {
    if (!p || dt_sim <= 0.0) return;
    double inner = p->inner_dt;
    int n = (int)ceil(dt_sim / inner);
    if (n < 1) n = 1;
    double step = dt_sim / (double)n;
    for (int i = 0; i < n; i++) inner_step(p, step);
}

/* ---- Push primitives ------------------------------------------------- */

static uint32_t new_seg_id(phys_t *p) {
    uint32_t id = p->next_seg_id++;
    if (p->next_seg_id == 0) p->next_seg_id = 1;
    return id;
}

EXPORT uint32_t phys_push_line(phys_t *p, double fx, double fy,
                               double tx, double ty,
                               double h_from, double h_to, double speed) {
    if (!p) return 0;
    segment_t s; memset(&s, 0, sizeof(s));
    s.type = SEG_LINE;
    s.seg_id = new_seg_id(p);
    s.p0x = fx; s.p0y = fy; s.p3x = tx; s.p3y = ty;
    s.p1x = fx + (tx - fx) / 3.0; s.p1y = fy + (ty - fy) / 3.0;
    s.p2x = fx + 2.0 * (tx - fx) / 3.0; s.p2y = fy + 2.0 * (ty - fy) / 3.0;
    s.h_from = h_from; s.h_to = h_to;
    s.speed = speed;
    build_arc_lut(&s);
    if (q_push(p, &s) < 0) return 0;
    return s.seg_id;
}

EXPORT uint32_t phys_push_spline(phys_t *p, double fx, double fy,
                                 double tx, double ty,
                                 double h_from, double h_to, double speed) {
    if (!p) return 0;
    segment_t s; memset(&s, 0, sizeof(s));
    s.type = SEG_SPLINE;
    s.seg_id = new_seg_id(p);
    double chord = sqrt((tx - fx) * (tx - fx) + (ty - fy) * (ty - fy));
    double d = chord / 3.0;
    s.p0x = fx; s.p0y = fy;
    s.p3x = tx; s.p3y = ty;
    s.p1x = fx + d * cos(h_from); s.p1y = fy + d * sin(h_from);
    s.p2x = tx - d * cos(h_to);   s.p2y = ty - d * sin(h_to);
    s.h_from = h_from; s.h_to = h_to;
    s.speed = speed;
    build_arc_lut(&s);
    if (q_push(p, &s) < 0) return 0;
    return s.seg_id;
}

EXPORT uint32_t phys_push_rotate(phys_t *p, double from_h, double to_h, double rate) {
    if (!p) return 0;
    segment_t s; memset(&s, 0, sizeof(s));
    s.type = SEG_ROTATE;
    s.seg_id = new_seg_id(p);
    s.rot_from_h = from_h; s.rot_to_h = to_h;
    s.rot_at_x = p->x; s.rot_at_y = p->y;
    s.speed = rate;
    s.arc_len = fabs(wrap_angle(to_h - from_h));
    if (q_push(p, &s) < 0) return 0;
    return s.seg_id;
}

EXPORT void phys_request_stop(phys_t *p) {
    if (!p) return;
    p->stop_requested = 1;
}

EXPORT void phys_release_stop(phys_t *p) {
    if (!p) return;
    p->stop_requested = 0;
}

EXPORT int phys_is_stopped(phys_t *p) {
    if (!p) return 1;
    return (p->flags & PATH_F_STOPPED) ? 1 : 0;
}

EXPORT void phys_abort_path(phys_t *p) {
    if (!p) return;
    q_clear(p);
    p->v_cmd = 0.0;
}

EXPORT int phys_queue_depth(phys_t *p) { return p ? p->q_count : 0; }

EXPORT uint32_t phys_active_seg_id(phys_t *p) {
    if (!p) return 0;
    segment_t *s = q_front(p);
    return s ? s->seg_id : 0;
}

EXPORT uint32_t phys_last_done_seg_id(phys_t *p) {
    return p ? p->last_done_seg_id : 0;
}

/* ---- Reads ----------------------------------------------------------- */

EXPORT void phys_read_pose(phys_t *p, phys_pose_t *out) {
    if (!p || !out) return;
    out->x       = p->x       + rng_gauss(p, p->gps_sigma_xy);
    out->y       = p->y       + rng_gauss(p, p->gps_sigma_xy);
    out->heading = wrap_angle(p->heading + rng_gauss(p, p->gps_sigma_h));
    out->v       = 0.5 * (p->v_l + p->v_r);
    out->omega   = (p->v_r - p->v_l) / (p->wheelbase > 0.0 ? p->wheelbase : 0.3);
    out->sim_t   = p->sim_t;
}

EXPORT void phys_read_pose_truth(phys_t *p, phys_pose_t *out) {
    if (!p || !out) return;
    out->x = p->x; out->y = p->y; out->heading = p->heading;
    out->v = 0.5 * (p->v_l + p->v_r);
    out->omega = (p->v_r - p->v_l) / (p->wheelbase > 0.0 ? p->wheelbase : 0.3);
    out->sim_t = p->sim_t;
}

EXPORT void phys_read_path_status(phys_t *p, phys_path_status_t *out) {
    if (!p || !out) return;
    out->flags             = p->flags;
    out->active_seg_id     = phys_active_seg_id(p);
    out->last_done_seg_id  = p->last_done_seg_id;
    out->queue_depth       = p->q_count;
    out->active_progress   = p->active_progress;
    out->cross_track_err   = p->cross_track_err;
    out->heading_err       = p->heading_err;
    out->energy_used_total = p->energy_used_total;
    out->v_cmd             = p->v_cmd;
    /* Clear SEG_DONE latch after reading */
    p->flags &= ~PATH_F_SEG_DONE;
}

EXPORT void phys_read_tool_status(phys_t *p, int slot, phys_tool_status_t *out) {
    if (!p || !out || slot < 0 || slot >= MAX_TOOLS) return;
    tool_t *t = &p->tools[slot];
    out->flags              = t->bits;
    out->kind               = t->kind;
    out->value              = t->value;
    out->target             = t->target;
    out->payload_mass       = p->payload_mass;
    out->battery_j          = p->battery_j;
    out->battery_capacity_j = p->battery_capacity_j;
}

/* ---- Tool ops -------------------------------------------------------- */

EXPORT int phys_begin_tool_move(phys_t *p, int slot, double target, double speed) {
    if (!p || slot < 0 || slot >= MAX_TOOLS) return -1;
    tool_t *t = &p->tools[slot];
    if (!t->in_use || t->kind != TOOL_REVOLUTE_1) return -2;
    if (target < t->lim_min) target = t->lim_min;
    if (target > t->lim_max) target = t->lim_max;
    t->target = target;
    t->speed = speed > 0.0 ? speed : t->max_speed;
    t->op = 1; t->op_t = 0.0;
    t->bits = 0;
    return 0;
}

EXPORT int phys_begin_grip(phys_t *p, int slot) {
    if (!p || slot < 0 || slot >= MAX_TOOLS) return -1;
    tool_t *t = &p->tools[slot];
    if (!t->in_use || t->kind != TOOL_BINARY) return -2;
    t->target = 1.0; t->op = 2; t->op_t = 0.0; t->bits = 0;
    return 0;
}

EXPORT int phys_begin_release(phys_t *p, int slot) {
    if (!p || slot < 0 || slot >= MAX_TOOLS) return -1;
    tool_t *t = &p->tools[slot];
    if (!t->in_use || t->kind != TOOL_BINARY) return -2;
    t->target = 0.0; t->op = 3; t->op_t = 0.0; t->bits = 0;
    return 0;
}

EXPORT int phys_begin_dock(phys_t *p, int slot) {
    if (!p || slot < 0 || slot >= MAX_TOOLS) return -1;
    tool_t *t = &p->tools[slot];
    if (!t->in_use || t->kind != TOOL_PASSIVE_DOCK) return -2;
    t->op = 4; t->op_t = 0.0; t->bits = 0;
    return 0;
}

EXPORT int phys_begin_charge(phys_t *p, int slot, double target_j) {
    if (!p || slot < 0 || slot >= MAX_TOOLS) return -1;
    tool_t *t = &p->tools[slot];
    if (!t->in_use || t->kind != TOOL_PASSIVE_DOCK) return -2;
    int si = station_at_pose(p, STATION_CHARGER);
    if (si < 0) { t->bits = TOOL_F_FAULT; return -3; }
    p->charger_idx = si;
    p->charge_target_j = target_j > 0.0 ? target_j : p->battery_capacity_j;
    if (p->charge_target_j > p->battery_capacity_j) p->charge_target_j = p->battery_capacity_j;
    p->charging = 1;
    t->op = 5; t->op_t = 0.0;
    t->bits = TOOL_F_DOCKED | TOOL_F_CHARGING;
    return 0;
}

EXPORT int phys_station_at_pose(phys_t *p, int kind) {
    return p ? station_at_pose(p, kind) : -1;
}

EXPORT double phys_payload_mass(phys_t *p) { return p ? p->payload_mass : 0.0; }
EXPORT double phys_battery_j(phys_t *p) { return p ? p->battery_j : 0.0; }
