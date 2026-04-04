/**
 * @file robot_config.h
 * @brief Load robot configuration from JSON file.
 */

#ifndef ROBOT_CONFIG_H
#define ROBOT_CONFIG_H

#include "robot_state.h"

/**
 * Load configuration from a JSON file into a robot_config_t struct.
 * Returns 0 on success, -1 on error.
 */
int robot_config_load(const char *filename, robot_config_t *config);

/**
 * Print configuration to stdout for debugging.
 */
void robot_config_print(const robot_config_t *config);

#endif /* ROBOT_CONFIG_H */
