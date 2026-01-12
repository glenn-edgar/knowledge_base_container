#include <stdio.h>
 #include <stdlib.h>
 #include <string.h>
 #include <assert.h>
 
 #include "cfl_hbit.h"
 #include "cfl_exception.h"
 #include "cfl_hbit_support.h"
 #include "cfl_hbit_error_walker.h"

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
static void test_or_error_tree(cfl_hbit2_tree_t* tree, uint16_t bit_space_id) {
    printf("\n\n");
    printf("Testing OR error tree\n");
    printf("================\n\n");
    
    uint16_t controller1_bit_id;
    uint16_t controller2_bit_id;
    int32_t node_id_1;
    int32_t node_id_2;
    
    cfl_hbit2_error_walker_t walker;

    int32_t top_node_id = cfl_hbit2_node(tree, "Overall_Valve_Status");
    cfl_hbit2_controller_t* controller = cfl_hbit2_controller_create(tree,top_node_id, bit_space_id);
    if (!controller) {
        printf("Failed to create controller\n");
        EXCEPTION("Failed to create controller");
        
    }
    printf("top node id: %d, bit space id: %d\n", top_node_id, bit_space_id);

    cfl_hbit_clear_all_leaf_nodes(tree, top_node_id, bit_space_id);
    cfl_hbit2_latch_clear_all(tree, bit_space_id);
    cfl_hbit2_sync(tree);
    node_id_1 = cfl_hbit2_controller_get_node_bit(controller, 0, 0, &controller1_bit_id);
    printf("Node ID: %d, Controller 1 Bit ID: %d\n", node_id_1, controller1_bit_id);
    node_id_2 = cfl_hbit2_controller_get_node_bit(controller, 1, 0, &controller2_bit_id);
    printf("Node ID: %d, Controller 2 Bit ID: %d\n", node_id_2, controller2_bit_id);
    printf("Setting node 3 bit 0...\n");
    cfl_hbit2_status_t s1 = cfl_hbit2_bit_set(tree, node_id_1, bit_space_id, controller1_bit_id, 1);
    printf("  result: %d\n", s1);
    
    printf("Setting node 26 bit 0...\n");
    cfl_hbit2_status_t s2 = cfl_hbit2_bit_set(tree, node_id_2, bit_space_id, controller2_bit_id, 1);
    printf("  result: %d\n", s2);
    printf("Setting node 3 bit 0...\n");
    s1 = cfl_hbit2_bit_set(tree, node_id_1, bit_space_id, controller1_bit_id, 1);
    printf("  result: %d\n", s1);
    
    printf("Setting node 26 bit 0...\n");
    s2 = cfl_hbit2_bit_set(tree, node_id_2, bit_space_id, controller2_bit_id, 1);
    printf("  result: %d\n", s2);
    
    // Check dirty flag and shadow BEFORE sync
    printf("Before sync:\n");
    printf("  dirty: %d\n", tree->impl.dirty);
    
    // Read shadow directly - need to find offset
    // Use internal test: set again and read current (which is still old)
    const uint8_t* cur3 = cfl_hbit2_bank_get(tree, 3, 0);
    const uint8_t* cur26 = cfl_hbit2_bank_get(tree, 26, 0);
    printf("  Node 3 current (before sync): 0x%02x\n", cur3 ? cur3[0] : 0xEE);
    printf("  Node 26 current (before sync): 0x%02x\n", cur26 ? cur26[0] : 0xEE);
    
printf("Debug offsets:\n");
extern uint32_t get_node_offset_debug(cfl_hbit2_tree_t* tree, uint16_t bs, int32_t node);
// We can't call internal function, so let's check another way

// Set bit again and check if it's writing to correct location
printf("Setting node 3 bit 0 again, checking shadow directly...\n");
cfl_hbit2_bit_set(tree, 3, 0, 0, 1);

// Print first 50 bytes of shadow buffer
printf("Shadow buffer bs0 first 50 bytes:\n");
for (int i = 0; i < 50; i++) {
    printf("%02x ", tree->impl.arenas[0].shadow[i]);
    if ((i + 1) % 16 == 0) printf("\n");
}
printf("\n");
    cfl_hbit2_sync(tree);
    
    printf("After sync:\n");
    // Check what offset get_node_offset returns for each node
// We need to expose this or compute it ourselves

// Print current buffer too
printf("Current buffer bs0 first 50 bytes:\n");
for (int i = 0; i < 50; i++) {
    printf("%02x ", tree->impl.arenas[0].current[i]);
    if ((i + 1) % 16 == 0) printf("\n");
}
printf("\n");
    cur3 = cfl_hbit2_bank_get(tree, 3, 0);
    cur26 = cfl_hbit2_bank_get(tree, 26, 0);
    printf("  Node 3 current (after sync): 0x%02x\n", cur3 ? cur3[0] : 0xEE);
    printf("  Node 26 current (after sync): 0x%02x\n", cur26 ? cur26[0] : 0xEE);
    printf("bit values: %d, %d\n", cfl_hbit2_bit_get(tree, node_id_1,bit_space_id, controller1_bit_id), cfl_hbit2_bit_get(tree, node_id_2,bit_space_id, controller2_bit_id));
    exit(0);
    cfl_hbit2_error_walker_init(&walker, tree, top_node_id, bit_space_id, true);
    int32_t and_top = cfl_hbit2_node(tree, "Overall_Valve_State");
printf("AND tree top: %d\n", and_top);

// Check node 3's bits for both bitspaces
printf("Node 3 bits for bs0 (OR): %d\n", cfl_hbit2_info_bits(tree, 3, 0));
printf("Node 3 bits for bs1 (AND): %d\n", cfl_hbit2_info_bits(tree, 3, 1));

const uint8_t* bank3 = cfl_hbit2_bank_get(tree, 3, 0);
const uint8_t* bank26 = cfl_hbit2_bank_get(tree, 26, 0);
printf("After sync:\n");
printf("  Node 3 bank[0]: 0x%02x\n", bank3 ? bank3[0] : 0xEE);
printf("  Node 26 bank[0]: 0x%02x\n", bank26 ? bank26[0] : 0xEE);
printf("Node 3 is_leaf: %d\n", cfl_hbit2_info_is_leaf(tree, 3));
printf("Node 26 is_leaf: %d\n", cfl_hbit2_info_is_leaf(tree, 26));
    int count = cfl_hbit2_error_walker_foreach(&walker, NULL, NULL);
    printf("  Top node has %d errors\n\n", count);
    printf("Controller created:\n");
printf("  child_count: %d\n", controller->child_count);
printf("  leaf_count: %d\n", controller->leaf_count);
printf("  bits_per_leaf: %d\n", controller->bits_per_leaf);

printf("Node 3 bits for bs0: %d\n", cfl_hbit2_info_bits(tree, 3, 0));
printf("Node 26 bits for bs0: %d\n", cfl_hbit2_info_bits(tree, 26, 0));
for (int i = 0; i < controller->child_count; i++) {
    printf("  child[%d]: node=%d, leaf_start=%d, leaf_count=%d\n", 
           i, 
           controller->children[i].node_id,
           controller->children[i].leaf_node_start_index,
           controller->children[i].leaf_count);
}

printf("  leaf_nodes: ");
for (int i = 0; i < controller->leaf_count; i++) {
    printf("%d ", controller->leaf_nodes[i]);
}
printf("\n");

printf("Node 3 parent chain: ");
int32_t n = 3;
while (n >= 0) {
    printf("%d -> ", n);
    n = cfl_hbit2_nav_parent(tree, n);
}
printf("root\n");

printf("Node 26 parent chain: ");
n = 26;
while (n >= 0) {
    printf("%d -> ", n);
    n = cfl_hbit2_nav_parent(tree, n);
}


printf("root\n");
    exit(0);
    
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
    cfl_hbit2_latch_clear_bit(tree, node_id_1, bit_space_id, controller_bit_id);
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
    cfl_hbit2_latch_clear_all(tree, bit_space_id);
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
    test_or_error_tree(tree, bs_id_1);
    cfl_hbit2_destroy(tree);
    
    return 0;
 }