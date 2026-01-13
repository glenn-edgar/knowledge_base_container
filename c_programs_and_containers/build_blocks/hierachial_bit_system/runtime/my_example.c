#include <stdio.h>
 #include <stdlib.h>
 #include <string.h>
 
 /* Runtime first */
 #include "cfl_hbit.h"
 #include "cfl_hbit_support.h"
 
 /* Generated headers */
 #define IRRIGATION_VALVES_INCLUDE_PATH_STRINGS
 #include "../test_out/generated_irrigation_valves.h"
 #include "../test_out/generated_irrigation_valves_data.h"
 


 

 void test_or_latch_test(cfl_hbit_instance_t* inst) {
    printf("\n\nTesting OR Latch Test\n\n");
    uint16_t bit_space_id = IRRIGATION_VALVES_BUF_ALARM_LATCHED;
    printf("Found ALARM_LATCHED bitspace at %d\n", bit_space_id);
    
    /* Look up nodes by hash */
    int16_t top_node = cfl_hbit_find_node_path(inst, "VALVE_STATUS");
    if (top_node < 0) {
        printf("ERROR: could not find VALVE_STATUS node\n");
        return;
    }

    
    /* Setup controller for flat access to leaves */
    cfl_hbit_controller_t* ctrl = cfl_hbit_controller_create(inst, (uint16_t)top_node, bit_space_id);
    if (!ctrl) {
        printf("ERROR: controller create failed\n");
        return;
    }
    cfl_hbit_controller_clear_all(ctrl);
    cfl_hbit_clear_controller_latches(ctrl);
    cfl_hbit_sync_and_propagate(inst);
    printf("\nInitial state:\n");
    cfl_hbit_print_node_state(inst,bit_space_id,top_node,NULL);
    

    printf("\nSetting child bits:\n");
    cfl_hbit_controller_set_child_bit(ctrl, 0, 0);
    cfl_hbit_controller_set_child_bit(ctrl, 1, 1);
    cfl_hbit_controller_set_child_bit(ctrl, 2, 2);
    cfl_hbit_controller_set_child_bit(ctrl, 3, 3);
    cfl_hbit_sync_and_propagate(inst);
    cfl_hbit_print_node_state(inst,bit_space_id,top_node,NULL);

    printf("\nClearing child bits:\n");
    cfl_hbit_controller_clear_child_bit(ctrl, 0, 0);
    cfl_hbit_controller_clear_child_bit(ctrl, 1, 1);
    cfl_hbit_controller_clear_child_bit(ctrl, 2, 2);
    cfl_hbit_controller_clear_child_bit(ctrl, 3, 3);
    cfl_hbit_sync_and_propagate(inst);
    
    printf("\nClearing latch child bits:\n");
    cfl_hbit_controller_clear_latch_child_bit(ctrl, 0, 0);
    cfl_hbit_controller_clear_latch_child_bit(ctrl, 1, 1);
    cfl_hbit_controller_clear_latch_child_bit(ctrl, 2, 2);
    cfl_hbit_controller_clear_latch_child_bit(ctrl, 3, 3);
    cfl_hbit_sync_and_propagate(inst);
    cfl_hbit_print_node_state(inst,bit_space_id,top_node,NULL);

    cfl_hbit_controller_set_child_bit(ctrl, 0, 0);
    cfl_hbit_sync_and_propagate(inst);
    printf("\n\nreading bit top node %d bit index %d value %d expected 1 \n", top_node, 0, cfl_hbit_controller_read_bit(ctrl, 0));
    cfl_hbit_print_node_state(inst,bit_space_id,top_node,NULL);
}

void test_or_mask_test(cfl_hbit_instance_t* inst) {
    printf("\n\nTesting OR MASK Test\n\n");
    uint16_t bit_space_id = IRRIGATION_VALVES_BUF_OR_MASK;
    printf("Found OR_MASK bitspace at %d\n", bit_space_id);
    
    /* Look up nodes by hash */
    int16_t top_node = cfl_hbit_find_node_path(inst, "VALVE_STATUS");
    if (top_node < 0) {
        printf("ERROR: could not find VALVE_STATUS node\n");
        return;
    }

    
    /* Setup controller for flat access to leaves */
    cfl_hbit_controller_t* ctrl = cfl_hbit_controller_create(inst, (uint16_t)top_node, bit_space_id);
    if (!ctrl) {
        printf("ERROR: controller create failed\n");
        return;
    }
    cfl_hbit_controller_clear_all(ctrl);
    cfl_hbit_clear_controller_masks(ctrl);
    cfl_hbit_sync_and_propagate(inst);
    printf("\nInitial state:\n");
    cfl_hbit_print_node_state(inst,bit_space_id,top_node,NULL);
    
    printf("\nSetting child bits:\n");
    cfl_hbit_controller_set_child_bit(ctrl, 0, 0);
    cfl_hbit_controller_set_child_bit(ctrl, 1, 1);
    cfl_hbit_controller_set_child_bit(ctrl, 2, 2);
    cfl_hbit_controller_set_child_bit(ctrl, 3, 3);
    cfl_hbit_sync_and_propagate(inst);
    cfl_hbit_print_node_state(inst,bit_space_id,top_node,NULL);

    printf("\nClearing mask child bits:\n");
    cfl_hbit_controller_set_mask_child_bit(ctrl, 0, 0,false);
    cfl_hbit_controller_set_mask_child_bit(ctrl, 1, 1,false);
    cfl_hbit_controller_set_mask_child_bit(ctrl, 2, 2,false);
    cfl_hbit_controller_set_mask_child_bit(ctrl, 3, 3,false);
    cfl_hbit_sync_and_propagate(inst);
    cfl_hbit_print_node_state(inst,bit_space_id,top_node,NULL);
  
      
    printf("\nSetting mask child bits:\n");
    cfl_hbit_controller_set_mask_child_bit(ctrl, 0, 0,true);
    cfl_hbit_controller_set_mask_child_bit(ctrl, 1, 1,true);
    cfl_hbit_controller_set_mask_child_bit(ctrl, 2, 2,true);
    cfl_hbit_controller_set_mask_child_bit(ctrl, 3, 3,true);
    cfl_hbit_sync_and_propagate(inst);
    cfl_hbit_print_node_state(inst,bit_space_id,top_node,NULL);

    printf("\nClearing child bits:\n");
    cfl_hbit_controller_clear_child_bit(ctrl, 0, 0);
    cfl_hbit_controller_clear_child_bit(ctrl, 1, 1);
    cfl_hbit_controller_clear_child_bit(ctrl, 2, 2);
    cfl_hbit_controller_clear_child_bit(ctrl, 3, 3);
    cfl_hbit_sync_and_propagate(inst);
    cfl_hbit_print_node_state(inst,bit_space_id,top_node,NULL);

    cfl_hbit_controller_set_child_bit(ctrl, 0, 0);
    cfl_hbit_sync_and_propagate(inst);
    
    printf("\n\nreading bit top node %d bit index %d value %d expected 1 \n", top_node, 0, cfl_hbit_controller_read_bit(ctrl, 0));
    cfl_hbit_print_node_state(inst,bit_space_id,top_node,NULL);
}

void test_and_test(cfl_hbit_instance_t* inst) {
    printf("\n\nTesting AND Mask Test\n\n");
    
    uint16_t bit_space_id = IRRIGATION_VALVES_BUF_AND_LATCHED;
    printf("Found AND_LATCHED bitspace at %d\n", bit_space_id);
    
    /* Look up nodes by hash */
    int16_t top_node = cfl_hbit_find_node_path(inst, "VALVE_STATE");
    if (top_node < 0) {
        printf("ERROR: could not find VALVE_STATUS node\n");
        return;
    }

    
    /* Setup controller for flat access to leaves */
    cfl_hbit_controller_t* ctrl = cfl_hbit_controller_create(inst, (uint16_t)top_node, bit_space_id);
    if (!ctrl) {
        printf("ERROR: controller create failed\n");
        return;
    }
    cfl_hbit_controller_fill_all(ctrl,0xff);
    
    cfl_hbit_sync_and_propagate(inst);
    printf("\nInitial state:\n");
    cfl_hbit_print_node_state(inst,bit_space_id,top_node,NULL);
    

    printf("\nSetting child bits:\n");
    cfl_hbit_controller_clear_child_bit(ctrl, 0, 0);
    cfl_hbit_controller_clear_child_bit(ctrl, 1, 1);
    cfl_hbit_controller_clear_child_bit(ctrl, 2, 2);
    cfl_hbit_controller_clear_child_bit(ctrl, 3, 3);
    cfl_hbit_sync_and_propagate(inst);
    cfl_hbit_print_node_state(inst,bit_space_id,top_node,NULL);

    printf("\nClearing child bits:\n");
    cfl_hbit_controller_set_child_bit(ctrl, 0, 0);
    cfl_hbit_controller_set_child_bit(ctrl, 1, 1);
    cfl_hbit_controller_set_child_bit(ctrl, 2, 2);
    cfl_hbit_controller_set_child_bit(ctrl, 3, 3);
    cfl_hbit_sync_and_propagate(inst);
    cfl_hbit_print_node_state(inst,bit_space_id,top_node,NULL);
    
    

    cfl_hbit_controller_clear_child_bit(ctrl, 0, 0);
    cfl_hbit_sync_and_propagate(inst);
    printf("\n\nreading bit top node %d bit index %d value %d expected 0 \n", top_node, 0, cfl_hbit_controller_read_bit(ctrl, 0));
    cfl_hbit_print_node_state(inst,bit_space_id,top_node,NULL);
}

void test_simple_walker_based_error_handling(cfl_hbit_instance_t* inst) {
    printf("\n\nTesting Simple Walker Based Error Handling\n\n");
    //uint16_t bit_space_id = IRRIGATION_VALVES_BUF_OR_LATCH;
    uint16_t bit_space_id = irrigation_valves_find_buffer("ALARM_LATCHED");
    printf("Found OR_LATCH bitspace at %d\n", bit_space_id);
    
    /* Look up nodes by hash */
    int16_t top_node = cfl_hbit_find_node_path(inst, "VALVE_STATUS");
    if (top_node < 0) {
        printf("ERROR: could not find VALVE_STATUS node\n");
        return;
    }

    /* Setup controller for flat access to leaves */
    cfl_hbit_controller_t* ctrl = cfl_hbit_controller_create(inst, (uint16_t)top_node, bit_space_id);
    if (!ctrl) {
        printf("ERROR: controller create failed\n");
        return;
    }
    cfl_hbit_controller_clear_all(ctrl);
    cfl_hbit_clear_controller_latches(ctrl);
    cfl_hbit_sync_and_propagate(inst);
    printf("\nInitial state:\n");
    cfl_hbit_print_node_state(inst,bit_space_id,top_node,NULL);

    printf("\nSetting child bits:\n");
    cfl_hbit_controller_set_child_bit(ctrl, 0, 0);
    cfl_hbit_controller_set_child_bit(ctrl, 1, 1);
    cfl_hbit_controller_set_child_bit(ctrl, 2, 2);
    cfl_hbit_controller_set_child_bit(ctrl, 3, 3);
    cfl_hbit_sync_and_propagate(inst);
    cfl_hbit_print_node_state(inst,bit_space_id,top_node,NULL);

    uint8_t temp_bit_index = 0;
    uint16_t monitoring_nodes[4];
    monitoring_nodes[0] = cfl_hbit_controller_get_node_bit(ctrl, 0, 0, &temp_bit_index);
    monitoring_nodes[1] = cfl_hbit_controller_get_node_bit(ctrl, 1, 1, &temp_bit_index);
    monitoring_nodes[2] = cfl_hbit_controller_get_node_bit(ctrl, 2, 2, &temp_bit_index);
    monitoring_nodes[3] = cfl_hbit_controller_get_node_bit(ctrl, 3, 3, &temp_bit_index);


    uint32_t error_bits = cfl_hbit_count_error_bits(inst, top_node, bit_space_id,false);
    printf("\n\nNumber of error bits: %d\n", error_bits);
    // Collect all error bits
    cfl_hbit_error_bits_t* errors = 
    cfl_hbit_count_error_bits_and_get_bits(inst, top_node, bit_space_id, sizeof(monitoring_nodes)/sizeof(monitoring_nodes[0]), monitoring_nodes, true);

    if (errors) {
        printf("Found %u error bits:\n", errors->count);

        for (uint32_t i = 0; i < errors->count; i++) {
            cfl_hbit_error_bit_t* err = &errors->error_bits[i];
            const cfl_hbit_node_t* node = &inst->config->nodes[err->node];
            
            printf("  Error at node %u (hash 0x%08X), bit %u monitoring node %u\n",
                err->node, node->path_hash, err->index, err->monitoring_node);
            
           
        }

        // Propagate the clears
        
        cfl_hbit_print_error_bits_by_node(inst, errors);
        // Free the error list
        cfl_hbit_error_bits_destroy(inst, errors);
    }
    
}



static void* my_alloc(size_t size, void* ctx) {
    (void)ctx;
    return malloc(size);
}

static void my_free(void* ptr, void* ctx) {
    (void)ctx;
    
    free(ptr);
}

static const cfl_hbit_allocator_t g_alloc = { my_alloc, my_free, NULL };

int main(void) {
    printf("========================================\n");
    printf("Hierarchical Bit Map My Example\n");
    printf("========================================\n");


    cfl_hbit_instance_t* inst = cfl_hbit_create(
        &g_alloc,
        (const cfl_hbit_config_t*)&irrigation_valves_config);

    if (!inst) {
        printf("\nFATAL: Failed to create instance\n");
        return 1;
    }
    test_or_latch_test(inst);
    test_or_mask_test(inst);
    test_and_test(inst);
    test_simple_walker_based_error_handling(inst);
    

    cfl_hbit_destroy(inst);
    return 0;
}