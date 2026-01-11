#include <stdio.h>
 #include <stdlib.h>
 #include <string.h>
 #include <assert.h>
 
 #include "cfl_hbit.h"
 #include "cfl_exception.h"
 #include "cfl_hbit_support.h"
 #include "./irrigation_output/generated_Irrigation_Example.bin.h"
 #include "./irrigation_output/generated_Irrigation_Example_hashes.h"

 void cfl_exception_handler(const char* file, const char* func, uint16_t line, const char* msg) {
     fprintf(stderr, "EXCEPTION at %s:%s:%u: %s\n", file, func, line, msg);
     exit(1);
 }

 void* my_malloc(void* handle, size_t size) {
    (void)handle;
    return malloc(size);
}

void my_free(void* handle, void* ptr) {
    (void)handle;
    free(ptr);
}


static void test_state(cfl_hbit2_tree_t* tree, uint16_t bit_space_id, uint16_t top_node_id,
            uint16_t leaf_node_id, uint16_t bit_position, bool state) {
   
    cfl_hbit2_sync(tree);
    bool bit_state = cfl_hbit2_bit_get(tree, leaf_node_id,bit_space_id, bit_position);
    printf("Bit state: %d\n", bit_state);
    assert(bit_state == state);
    bool bit_state_top = cfl_hbit2_bit_get(tree, top_node_id,bit_space_id, bit_position);
    printf("Top node Bit state: %d\n", bit_state_top);
    assert(bit_state_top == state);
    int edge = cfl_hbit2_bit_edge(tree, leaf_node_id,bit_space_id, bit_position);
    printf("Leaf node Edge: %d\n", edge);
    int edge_top = cfl_hbit2_bit_edge(tree, top_node_id,bit_space_id, bit_position);
    printf("Top node Edge: %d\n", edge_top);
    assert(edge == edge_top);
    printf("leaf latch: %d\n", cfl_hbit2_latch_get_bit(tree, leaf_node_id,bit_space_id, bit_position));
    cfl_hbit2_sync(tree);
#if 0
// Debug: check latch vs current directly
const uint8_t* latch = cfl_hbit2_latch_get(tree, leaf_node_id, bit_space_id);
const uint8_t* bank = cfl_hbit2_bank_get(tree, leaf_node_id, bit_space_id);
printf("DEBUG: latch[0]=0x%02x, current[0]=0x%02x\n", 
       latch ? latch[0] : 0xEE, 
       bank ? bank[0] : 0xEE);
#endif
}

static void test_and_bits(cfl_hbit2_tree_t* tree, uint16_t bit_space_id) {
    printf("\n\nTesting AND bits\n");
    printf("================\n\n");
    
    
    uint16_t bitmask_id;
    uint16_t controller_bit_id;
    int32_t node_id_1;
    int32_t node_id_2;
    int32_t top_node_id = cfl_hbit2_node(tree, "Overall_Valve_State");
    cfl_hbit_fill_all_leaf_nodes(tree, top_node_id, bit_space_id);
    cfl_hbit2_latch_clear_all(tree, bit_space_id);  // Reset latch to 0xFF for AND
    cfl_hbit2_sync(tree);
    printf("top node id: %d, bit space id: %d\n", top_node_id, bit_space_id);
    cfl_hbit2_controller_t* controller = cfl_hbit2_controller_create(tree,top_node_id, bit_space_id);
    if (!controller) {
        printf("Failed to create controller\n");
        EXCEPTION("Failed to create controller");
        
    }

    node_id_1 = cfl_hbit2_controller_get_node_bit(controller, 0, 0, &controller_bit_id);
    printf("Node ID: %d, Bitmask ID: %d\n", node_id_1, bitmask_id);
    node_id_2 = cfl_hbit2_controller_get_bitmap_node(controller, 0, &bitmask_id);
    printf("Node ID: %d, Controller Bit ID: %d\n", node_id_2, controller_bit_id);
    assert(node_id_1 == node_id_2);
    assert(bitmask_id == controller_bit_id);
    test_state(tree, bit_space_id, top_node_id, node_id_1, controller_bit_id, 1);
    
    cfl_hbit2_bit_set(tree, node_id_1,bit_space_id, controller_bit_id, 1);
    test_state(tree, bit_space_id, top_node_id, node_id_1, controller_bit_id, 1);

    cfl_hbit2_bit_set(tree, node_id_1,bit_space_id, controller_bit_id, 0);
    test_state(tree, bit_space_id, top_node_id, node_id_1, controller_bit_id, 0);
    
    cfl_hbit2_bit_set(tree, node_id_1,bit_space_id, controller_bit_id, 1);
    test_state(tree, bit_space_id, top_node_id, node_id_1, controller_bit_id, 0);
    cfl_hbit2_bit_set(tree, node_id_1,bit_space_id, controller_bit_id, 1);
    cfl_hbit2_latch_set_bit(tree, node_id_1, bit_space_id, controller_bit_id);
    test_state(tree, bit_space_id, top_node_id, node_id_1, controller_bit_id, 1);
    
    printf("setting mask to 0xfe\n");
    uint8_t mask = 0xfe;
    cfl_hbit2_mask_set(tree, node_id_1, bit_space_id, &mask, 1);
    cfl_hbit2_bit_set(tree, node_id_1,bit_space_id, controller_bit_id, 0);
    test_state(tree, bit_space_id, top_node_id, node_id_1, controller_bit_id, 1);
    
    mask = 0xff;
    cfl_hbit2_mask_set(tree, node_id_1, bit_space_id, &mask, 1);
    // mask wipes out the write
    test_state(tree, bit_space_id, top_node_id, node_id_1, controller_bit_id, 1);
    cfl_hbit2_bit_set(tree, node_id_1,bit_space_id, controller_bit_id, 0);
    test_state(tree, bit_space_id, top_node_id, node_id_1, controller_bit_id, 0);
    cfl_hbit2_controller_destroy(controller);
}


static void test_or_bits(cfl_hbit2_tree_t* tree, uint16_t bit_space_id) {
    printf("Testing OR bits\n");
    printf("================\n\n");
    uint16_t bitmask_id;
    uint16_t controller_bit_id;
    int32_t node_id_1;
    int32_t node_id_2;
    int32_t top_node_id = cfl_hbit2_node(tree, "Overall_Valve_Status");
    cfl_hbit2_controller_t* controller = cfl_hbit2_controller_create(tree,top_node_id, bit_space_id);
    if (!controller) {
        printf("Failed to create controller\n");
        EXCEPTION("Failed to create controller");
        
    }
    printf("top node id: %d, bit space id: %d\n", top_node_id, bit_space_id);
    cfl_hbit_clear_all_leaf_nodes(tree, top_node_id, bit_space_id);
    cfl_hbit2_sync(tree);
    node_id_1 = cfl_hbit2_controller_get_node_bit(controller, 0, 0, &controller_bit_id);
    printf("Node ID: %d, Bitmask ID: %d\n", node_id_1, controller_bit_id);
    node_id_2 = cfl_hbit2_controller_get_bitmap_node(controller, 0, &bitmask_id);
    printf("Node ID: %d, Controller Bit ID: %d\n", node_id_2, bitmask_id);
    assert(node_id_1 == node_id_2);
    assert(bitmask_id == controller_bit_id);
    // set node_id_1 to 1
    cfl_hbit2_bit_set(tree, node_id_1,bit_space_id, controller_bit_id, 1);
    test_state(tree, bit_space_id, top_node_id, node_id_1, controller_bit_id, 1);
    cfl_hbit2_bit_set(tree, node_id_1,bit_space_id, controller_bit_id, 0);
    test_state(tree, bit_space_id, top_node_id, node_id_1, controller_bit_id, 1);
    cfl_hbit2_bit_set(tree, node_id_1,bit_space_id, controller_bit_id, 0);
    cfl_hbit2_latch_clear_bit(tree, node_id_1, bit_space_id, controller_bit_id);
    test_state(tree, bit_space_id, top_node_id, node_id_1, controller_bit_id, 0);
    uint8_t mask = 0xfe;
    cfl_hbit2_mask_set(tree, node_id_1, bit_space_id, &mask, 1);
    cfl_hbit2_bit_set(tree, node_id_1,bit_space_id, controller_bit_id, 1);
    test_state(tree, bit_space_id, top_node_id, node_id_1, controller_bit_id, 0);
    
    mask = 0xff;
    // changing masks wipes out the old values
    cfl_hbit2_mask_set(tree, node_id_1, bit_space_id, &mask, 1);
    test_state(tree, bit_space_id, top_node_id, node_id_1, controller_bit_id, 0);
    cfl_hbit2_controller_destroy(controller);
    
}

 


 int main(void) {
    printf("Irrigation Example\n");
    printf("==================\n\n");
    
    cfl_hbit2_tree_t* tree = cfl_hbit2_create_with_allocator(
            Irrigation_Example_descriptor, sizeof(Irrigation_Example_descriptor),
             my_malloc, my_free, NULL);
    if (!tree) {
        printf("Failed to create tree\n");
        EXCEPTION("Failed to create tree");
        return 1;
    }
    int32_t bs_id_1 = cfl_hbit2_bitspace(tree, "ALARM_LATCHED");
    int32_t bs_id_2 = cfl_hbit2_bitspace(tree, "AND_LATCHED");
    if (bs_id_1 < 0 || bs_id_2 < 0) {
        printf("Failed to get bitspace\n");
        EXCEPTION("Failed to get bitspace");
        return 1;
    }
    cfl_hbit_print_tree(tree, "Overall_Valve_Status", bs_id_1);
    cfl_hbit_print_tree(tree, "Overall_Valve_State", bs_id_2);
    
    test_or_bits(tree, bs_id_1);    
    test_and_bits(tree, bs_id_2);
   
    cfl_hbit2_destroy(tree);
    
    return 0;
 }