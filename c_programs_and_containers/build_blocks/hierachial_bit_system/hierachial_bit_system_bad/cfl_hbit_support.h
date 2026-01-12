#ifndef CFL_HBIT_SUPPORT_H
#define CFL_HBIT_SUPPORT_H

#ifdef __cplusplus
extern "C" {
#endif
#include "cfl_hbit.h"

void cfl_hbit_print_tree(cfl_hbit2_tree_t* tree, const char* path, uint16_t bitspace_id);

cfl_hbit2_controller_t* cfl_hbit_create_controller(cfl_hbit2_tree_t* tree, const char* path, uint16_t bitspace_id);


void cfl_hbit_fill_all_leaf_nodes(cfl_hbit2_tree_t* tree,uint32_t top_node_id, uint16_t bitspace_id);

void cfl_hbit_clear_all_leaf_nodes(cfl_hbit2_tree_t* tree, uint32_t top_node_id, uint16_t bitspace_id);




#ifdef __cplusplus
}
#endif
#endif