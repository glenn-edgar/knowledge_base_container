/* pump_station_user_vft.h - User VFT prototypes */
#ifndef PUMP_STATION_USER_VFT_H
#define PUMP_STATION_USER_VFT_H
#include "scan_tree.h"

/* VFT_user_motor_health_check
 * Inputs:
 *   current (float, count=1)
 *   threshold (float, count=1)
 * Access data: st_buf_data(h, inputs[i].buf_id) + inputs[i].start
 */
uint8_t user_vft_motor_health_check(uint8_t *state, const st_handle_t *h, const st_input_desc_t *inputs, uint32_t n_inputs);

#endif
