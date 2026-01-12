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
    exit(0);
#if 0
    printf("Controller: %d children, %d leaves, %d bits/leaf, %d total bits\n",
           ctrl->child_count, ctrl->leaf_count, ctrl->bits_per_leaf, ctrl->total_bits);
    
    /* Fill leaf banks with 0 (clear current values) */
    uint8_t zeros[1] = {0x00};
    for (uint16_t i = 0; i < ctrl->leaf_count; i++) {
        uint16_t leaf_node = ctrl->leaf_nodes[i];
        cfl_hbit_leaf_write(inst, buf_id, leaf_node, zeros, 1);
    }
    cfl_hbit_propagate(inst);
    printf("Cleared all leaf current values\n");
    
    /* Clear leaf node latches */
    for (uint16_t i = 0; i < ctrl->leaf_count; i++) {
        uint16_t leaf_node = ctrl->leaf_nodes[i];
        cfl_hbit_clear_latch_all(inst, buf_id, leaf_node);
    }
    cfl_hbit_propagate(inst);
    printf("Cleared all leaf latches\n");
    
    /* Verify everything is cleared */
    bool all_clear = true;
    for (uint16_t i = 0; i < ctrl->leaf_count; i++) {
        uint16_t leaf_node = ctrl->leaf_nodes[i];
        uint8_t data[1] = {0xFF};
        cfl_hbit_read_node(inst, buf_id, leaf_node, data, 1);
        if (data[0] != 0) all_clear = false;
        
        bool latched = cfl_hbit_read_latched_bit(inst, buf_id, leaf_node, bit_id);
        if (latched) all_clear = false;
    }
    printf("All leaves cleared: %s\n", all_clear ? "YES" : "NO");
    
    /* Now do your test - e.g., set a bit, verify latch behavior */
    printf("\nSetting LEAK alarm on leaf 0 (node %d)...\n", ctrl->leaf_nodes[0]);
    cfl_hbit_leaf_set_bit(inst, buf_id, ctrl->leaf_nodes[0], bit_id);
    cfl_hbit_propagate(inst);
    
    /* Check current and latched at leaf, parent, root */
    bool leaf_cur = cfl_hbit_read_bit(inst, buf_id, ctrl->leaf_nodes[0], bit_id);
    bool leaf_lat = cfl_hbit_read_latched_bit(inst, buf_id, ctrl->leaf_nodes[0], bit_id);
    bool root_cur = cfl_hbit_read_bit(inst, buf_id, (uint16_t)top_node, bit_id);
    bool root_lat = cfl_hbit_read_latched_bit(inst, buf_id, (uint16_t)top_node, bit_id);
    
    printf("  Leaf: current=%d, latched=%d\n", leaf_cur, leaf_lat);
    printf("  Root: current=%d, latched=%d\n", root_cur, root_lat);
    
    /* Clear current, verify latch remains */
    printf("\nClearing current (alarm goes away)...\n");
    cfl_hbit_leaf_clear_bit(inst, buf_id, ctrl->leaf_nodes[0], bit_id);
    cfl_hbit_propagate(inst);
    
    leaf_cur = cfl_hbit_read_bit(inst, buf_id, ctrl->leaf_nodes[0], bit_id);
    leaf_lat = cfl_hbit_read_latched_bit(inst, buf_id, ctrl->leaf_nodes[0], bit_id);
    root_cur = cfl_hbit_read_bit(inst, buf_id, (uint16_t)top_node, bit_id);
    root_lat = cfl_hbit_read_latched_bit(inst, buf_id, (uint16_t)top_node, bit_id);
    
    printf("  Leaf: current=%d, latched=%d\n", leaf_cur, leaf_lat);
    printf("  Root: current=%d, latched=%d\n", root_cur, root_lat);
    
    cfl_hbit_controller_destroy(ctrl);
    printf("\nOR Latch Test Complete\n");
#endif
}

void test_or_mask_test(cfl_hbit_instance_t* inst) {
    printf("\n\nTesting OR Mask Test\n\n");
}

void test_and_mask_test(cfl_hbit_instance_t* inst) {
    printf("\n\nTesting AND Mask Test\n\n");
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
    test_and_mask_test(inst);

    cfl_hbit_destroy(inst);
    return 0;
}