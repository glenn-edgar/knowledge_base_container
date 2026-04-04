/**
 * @file robot_state.c
 * @brief Robot state management: energy, pose, worker lifecycle.
 */

#include "robot_state.h"
#include "robot_protocol.h"
#include <string.h>
#include <stdio.h>

/* ================================================================== */
/*  Initialization                                                     */
/* ================================================================== */

void robot_state_init(robot_state_t *state, const robot_config_t *config) {
    memset(state, 0, sizeof(robot_state_t));
    memcpy(&state->config, config, sizeof(robot_config_t));
    state->energy_remaining = config->energy_max;
    state->running = true;
    robot_state_build_topics(state);
}

void robot_state_build_topics(robot_state_t *state) {
    /* Convert site dots to slashes: moonbase.alpha.surface_ops ->
       moonbase/alpha/surface_ops */
    char site_path[SITE_MAX];
    strncpy(site_path, state->config.site, SITE_MAX - 1);
    site_path[SITE_MAX - 1] = '\0';
    for (char *p = site_path; *p; p++) {
        if (*p == '.') *p = '/';
    }

    const char *id = state->config.robot_id;

    snprintf(state->topic_rpc, TOPIC_MAX,
             "%s/robots/%s/rpc", site_path, id);
    snprintf(state->topic_stream, TOPIC_MAX,
             "%s/robots/%s/stream_bus", site_path, id);
    snprintf(state->topic_status_state, TOPIC_MAX,
             "%s/robots/%s/status/state", site_path, id);
    snprintf(state->topic_status_energy, TOPIC_MAX,
             "%s/robots/%s/status/energy", site_path, id);
    snprintf(state->topic_status_bitmask, TOPIC_MAX,
             "%s/robots/%s/status/bitmask", site_path, id);
}

/* ================================================================== */
/*  Worker lifecycle                                                   */
/* ================================================================== */

void robot_state_start_worker(robot_state_t *state, int packet_type,
                               int seq, int test_id,
                               double dx, double dy, double dz,
                               double dheading, double darm) {
    worker_state_t *w = &state->worker;
    w->active = true;
    w->packet_type = packet_type;
    w->seq = seq;
    w->test_id = test_id;
    w->duration = (packet_type >= 1 && packet_type <= 12)
                  ? action_durations[packet_type] : 10;
    w->elapsed = 0;
    w->start_pose = state->pose;
    w->delta_x = dx;
    w->delta_y = dy;
    w->delta_z = dz;
    w->delta_heading = dheading;
    w->delta_arm_angle = darm;
}

void robot_state_tick_worker(robot_state_t *state) {
    worker_state_t *w = &state->worker;
    if (!w->active) return;

    w->elapsed++;

    /* Interpolate pose toward target */
    if (w->duration > 0) {
        double frac = (double)w->elapsed / (double)w->duration;
        if (frac > 1.0) frac = 1.0;
        state->pose.x = w->start_pose.x + w->delta_x * frac;
        state->pose.y = w->start_pose.y + w->delta_y * frac;
        state->pose.z = w->start_pose.z + w->delta_z * frac;
        state->pose.heading = w->start_pose.heading + w->delta_heading * frac;
        state->pose.arm_angle = w->start_pose.arm_angle + w->delta_arm_angle * frac;
    }
}

void robot_state_complete_worker(robot_state_t *state) {
    worker_state_t *w = &state->worker;

    /* Snap to final pose */
    state->pose.x = w->start_pose.x + w->delta_x;
    state->pose.y = w->start_pose.y + w->delta_y;
    state->pose.z = w->start_pose.z + w->delta_z;
    state->pose.heading = w->start_pose.heading + w->delta_heading;
    state->pose.arm_angle = w->start_pose.arm_angle + w->delta_arm_angle;

    w->active = false;
}

void robot_state_deduct_energy(robot_state_t *state, int packet_type) {
    if (state->config.energy_infinite) return;

    if (packet_type == TYPE_RECHARGE) {
        state->energy_remaining = state->config.energy_max;
        return;
    }

    if (packet_type >= 1 && packet_type <= 12) {
        state->energy_remaining -= energy_costs[packet_type];
        if (state->energy_remaining < 0) state->energy_remaining = 0;
    }
}
