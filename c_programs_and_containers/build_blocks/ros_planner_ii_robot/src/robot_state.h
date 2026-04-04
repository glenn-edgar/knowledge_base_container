/**
 * @file robot_state.h
 * @brief Robot state: energy, pose, active worker, bitmask.
 */

#ifndef ROBOT_STATE_H
#define ROBOT_STATE_H

#include <stdbool.h>
#include <stdint.h>

#define ROBOT_ID_MAX      64
#define SITE_MAX          256
#define TOPIC_MAX         512
#define JSON_BUF_MAX      4096
#define WIRE_FORMAT_MAX   8

/* ================================================================== */
/*  Robot configuration (loaded from JSON file)                        */
/* ================================================================== */

typedef struct {
    char     robot_id[ROBOT_ID_MAX];
    char     site[SITE_MAX];
    char     mqtt_host[256];
    int      mqtt_port;
    char     robot_class[64];
    int      energy_max;
    bool     energy_infinite;
    char     wire_format[WIRE_FORMAT_MAX];   /* "json" or "cbor" */
} robot_config_t;

/* ================================================================== */
/*  Robot pose (absolute position + arm)                               */
/* ================================================================== */

typedef struct {
    double x;
    double y;
    double z;
    double heading;
    double arm_angle;
} robot_pose_t;

/* ================================================================== */
/*  Active worker state                                                */
/* ================================================================== */

typedef struct {
    bool     active;
    int      packet_type;
    int      seq;
    int      test_id;
    int      duration;         /* total ticks for this action */
    int      elapsed;          /* ticks since start */
    /* start pose (for computing deltas) */
    robot_pose_t start_pose;
    /* target deltas (computed from command params) */
    double   delta_x;
    double   delta_y;
    double   delta_z;
    double   delta_heading;
    double   delta_arm_angle;
} worker_state_t;

/* ================================================================== */
/*  Robot state                                                        */
/* ================================================================== */

typedef struct {
    robot_config_t  config;
    robot_pose_t    pose;
    worker_state_t  worker;
    int             energy_remaining;
    uint32_t        bitmask;
    uint64_t        tick_count;
    bool            running;
    /* MQTT topics (built from config at init) */
    char            topic_rpc[TOPIC_MAX];
    char            topic_stream[TOPIC_MAX];
    char            topic_status_state[TOPIC_MAX];
    char            topic_status_energy[TOPIC_MAX];
    char            topic_status_bitmask[TOPIC_MAX];
} robot_state_t;

/* Initialize robot state from config */
void robot_state_init(robot_state_t *state, const robot_config_t *config);

/* Build MQTT topic paths from site + robot_id */
void robot_state_build_topics(robot_state_t *state);

/* Apply simulated movement for the current tick */
void robot_state_tick_worker(robot_state_t *state);

/* Deduct energy for completed action */
void robot_state_deduct_energy(robot_state_t *state, int packet_type);

/* Start a new worker from an RPC command */
void robot_state_start_worker(robot_state_t *state, int packet_type,
                               int seq, int test_id,
                               double dx, double dy, double dz,
                               double dheading, double darm);

/* Complete the active worker */
void robot_state_complete_worker(robot_state_t *state);

#endif /* ROBOT_STATE_H */
