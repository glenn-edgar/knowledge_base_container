#include "cfl_hbit_support.h"

void cfl_hbit_clear_controller_latches(cfl_hbit_controller_t* ctrl){
/* Clear leaf node latches */
    for (uint16_t i = 0; i < ctrl->leaf_count; i++) {
        uint16_t leaf_node = ctrl->leaf_nodes[i];
        cfl_hbit_clear_latch_all(ctrl->inst, ctrl->buffer_idx, leaf_node);
    }
}