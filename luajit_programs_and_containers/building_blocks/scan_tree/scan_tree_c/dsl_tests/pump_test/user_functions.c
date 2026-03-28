#include "scan_tree.h"

uint8_t user_vft_motor_health_check(uint8_t *state, const st_handle_t *h, const st_input_desc_t *inputs, uint32_t n_inputs)
{
    (void)state; (void)n_inputs;
    float cur = ((const float*)st_buf_data(h, inputs[0].buf_id))[inputs[0].start];
    float thr = ((const float*)st_buf_data(h, inputs[1].buf_id))[inputs[1].start];
    return (cur < thr) ? 1 : 0;
}