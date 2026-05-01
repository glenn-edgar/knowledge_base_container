// libcomm/drive_base_robot.h
// drive_base logical_robot — translates between bus_msg_t (40-byte
// envelope on the dongle's internal bus) and physics_core's direct
// C API (libphysics.so / phys_*). One file per logical_robot class
// per Track C Q2.
//
// Lifecycle (vtable-driven, see logical_robot.h):
//   init     — phys_create, apply tunables via phys_set_*.
//   on_msg   — DRV_CMD_* bus_msg_t → phys_push_line/spline/rotate/etc.
//   tick     — phys_step(dt), drain pose/path status, emit
//              DRV_EVT_TELEMETRY upward; on seg-done transitions emit
//              DRV_EVT_SEG_DONE.
//   shutdown — phys_destroy.
//
// Catalogue mapping (cmd_hi:cmd_lo → physics_core call):
//   0x10:01 DRV_CMD_PUSH_LINE    payload 7 floats   →  phys_push_line
//   0x10:02 DRV_CMD_PUSH_SPLINE  payload 7 floats   →  phys_push_spline
//   0x10:03 DRV_CMD_PUSH_ROTATE  payload 3 floats   →  phys_push_rotate
//   0x10:10 DRV_CMD_STOP         empty              →  phys_request_stop
//   0x10:11 DRV_CMD_RESUME       empty              →  phys_release_stop
//   0x10:12 DRV_CMD_ABORT        empty              →  phys_abort_path
//
//   0x10:80 DRV_EVT_TELEMETRY    32 B packed pose + path status
//   0x10:81 DRV_EVT_SEG_DONE      8 B (seg_id, energy_at_complete)

#pragma once

#include <stdint.h>

#include "logical_robot.h"
#include "bus_msg.h"
#include "bus_kernel.h"

#ifdef __cplusplus
extern "C" {
#endif

// ============ CMD/EVT CODES ============

#define DRV_CMD_PUSH_LINE     0x1001u
#define DRV_CMD_PUSH_SPLINE   0x1002u
#define DRV_CMD_PUSH_ROTATE   0x1003u
#define DRV_CMD_STOP          0x1010u
#define DRV_CMD_RESUME        0x1011u
#define DRV_CMD_ABORT         0x1012u
#define DRV_CMD_TELEMETRY_ON  0x1020u    // master enables periodic events
#define DRV_CMD_TELEMETRY_OFF 0x1021u    // and disables them

#define DRV_EVT_TELEMETRY     0x1080u
#define DRV_EVT_SEG_DONE      0x1081u    // seg_id = master_seq, not libphysics internal
#define DRV_EVT_FAULT         0x1082u

// ============ TUNABLES ============
// Mirrors the relevant subset of physics_config.json. Single packed
// struct so it can live in NVS on embedded (Q3 schema-versioned blob
// pattern). v1 = schema_version 1.

#define DRV_TUNABLES_SCHEMA_VERSION  1u

typedef struct {
    uint16_t schema_version;       // = DRV_TUNABLES_SCHEMA_VERSION
    uint16_t _pad0;

    // Chassis
    float wheelbase_m;
    float wheel_radius_m;
    float mass_kg;
    float inertia_kg_m2;
    float lin_friction;
    float ang_friction;

    // Motors
    float max_torque_nm;
    float max_wheel_rad_s;
    float pid_kp, pid_ki, pid_kd;
    float energy_k;

    // Path follower
    float lookahead_min_m;
    float lookahead_k_v;
    float arrival_tol_m;
    float heading_tol_rad;
    float max_linear_accel;
    float max_angular_accel;
    float cross_track_abort_m;
    float inner_dt_s;

    // Sensors
    float gps_sigma_xy;
    float gps_sigma_h;
    float imu_sigma;
    float battery_noise_pct;

    // Battery
    float battery_capacity_j;
    float battery_initial_j;

    // Initial pose
    float init_x, init_y, init_heading;

    uint64_t seed;                  // 0 = use a default
} drive_base_tunables_t;

// ============ INSTANCE ============
// One per drive_base logical_robot. Caller-allocated. The phys_t* is
// owned by this instance (created in init, destroyed on shutdown).

typedef struct phys_s phys_t;       // forward decl from libphysics

// Master-seq → libphysics seg_id FIFO. Whenever drive_base receives a
// DRV_CMD_PUSH_* it remembers (master_seq, libphysics_seg_id) here in
// arrival order. When libphysics reports a new last_done_seg_id, the
// matching head entry is popped and DRV_EVT_SEG_DONE fires with the
// MASTER's seq as the seg_id field. This means master-side HAL's
// last_done_seg_id matches whatever push_line returned, not an
// internal libphysics number that the master never saw.
//
// Ring sized to MSGQ_DEPTH_DEFAULT × 2 so a saturated inbox-load
// (depth 16) still has slack while in-flight commands drain.
#define DRV_SEG_TRACK_DEPTH   32

typedef struct {
    uint8_t   master_seq;
    uint8_t   _pad0;
    uint16_t  _pad1;
    uint32_t  phys_seg_id;
} drv_seg_track_t;

typedef struct {
    drive_base_tunables_t tun;
    phys_t               *phys;             // owned: phys_create at init
    bus_msgq_t           *outbound;         // caller-owned events sink (typically ext_tx_q)
    uint32_t              tick_period_ms;   // for dt computation
    uint32_t              last_tick_ms;
    uint32_t              last_done_phys_seg_id;
    uint8_t               bus_addr;         // src_addr stamped on emitted events
    uint8_t               started;          // 1 once init has run
    // Telemetry is opt-in. Default 0 — drive_base runs phys_step every
    // tick but does NOT emit DRV_EVT_TELEMETRY events. SEG_DONE
    // (correlated to commands) and FAULT (mandatory) still flow.
    // Master enables/disables via DRV_CMD_TELEMETRY_ON/OFF; tests may
    // also write the field directly. Avoids ext_tx_q saturation when
    // nobody is listening.
    uint8_t               telemetry_enabled;
    uint8_t               _pad[1];

    // master_seq -> phys_seg_id FIFO (see DRV_SEG_TRACK_DEPTH).
    drv_seg_track_t       seg_track[DRV_SEG_TRACK_DEPTH];
    uint8_t               seg_track_head;   // next pop slot
    uint8_t               seg_track_tail;   // next push slot
    uint8_t               seg_track_count;
    uint8_t               _pad2;
} drive_base_t;

// ============ VTABLE ============
// Static const, exported so the manager's registry can reference it
// (see logical_robot_init's `vtable` parameter).

extern const logical_robot_vtable_t drive_base_vtable;

// ============ HELPERS ============
// Build a DRV_CMD_PUSH_LINE bus_msg_t with packed payload. Returns 0
// on success, -1 on bad args. Used by the manager when translating
// inbound external-bus frames.

int drive_base_build_push_line  (bus_msg_t *m,
                                 uint8_t    dst_robot,
                                 uint8_t    seq,
                                 float fx, float fy,
                                 float tx, float ty,
                                 float h_from, float h_to,
                                 float speed);

int drive_base_build_push_spline(bus_msg_t *m,
                                 uint8_t    dst_robot,
                                 uint8_t    seq,
                                 float fx, float fy,
                                 float tx, float ty,
                                 float h_from, float h_to,
                                 float speed);

int drive_base_build_push_rotate(bus_msg_t *m,
                                 uint8_t    dst_robot,
                                 uint8_t    seq,
                                 float from_h, float to_h, float rate);

int drive_base_build_simple     (bus_msg_t *m,
                                 uint8_t    dst_robot,
                                 uint8_t    seq,
                                 uint16_t   cmd);     // STOP/RESUME/ABORT

// ============ EVENT DECODERS ============
// Counterpart helpers used by tests and (eventually) the manager when
// translating outbound bus_msg_t back to wire frames.

typedef struct {
    float    x, y, heading;
    float    v, omega;
    float    energy_used_total;
    uint32_t active_seg_id;
    uint16_t queue_depth;
    uint16_t flags;
} drive_base_telemetry_t;

int drive_base_decode_telemetry(const bus_msg_t        *m,
                                drive_base_telemetry_t *out);

typedef struct {
    uint32_t seg_id;
    float    energy_at_complete;
} drive_base_seg_done_t;

int drive_base_decode_seg_done(const bus_msg_t       *m,
                               drive_base_seg_done_t *out);

#ifdef __cplusplus
}
#endif
