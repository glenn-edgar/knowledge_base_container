#include <stdio.h>
#include <stdlib.h>
#include <stdarg.h>
#include "chain_tree.h"

// 1. Include Generated Header
#include "generated_ChainBitTreeDemo.h"

// 2. Mock Allocator
void* alloc(sys_handle_t h, size_t sz) { 
    return calloc(1, sz); // calloc guarantees zero-init
}
void free(sys_handle_t h) { 
    // OS handles exit cleanup 
}

// 3. Helper to print bit state
void print_bit(chain_tree_t* tree, int bank, int bit, const char* label, const char* path) {
    uint8_t* ptr = chain_get_bits(tree, bank, path);
    if (!ptr) { 
        printf("  [%-10s] NOT FOUND\n", label); 
        return; 
    }
    
    bool val = (*ptr >> bit) & 1;
    printf("  [%-10s] %s : Bit %d = %d\n", label, path, bit, val);
}

int main() {
    printf("--- Chain Tree Logic Test ---\n");

    // Initialize
    chain_tree_t tree;
    chain_tree_init(&tree, &ChainBitTreeDemo_desc, NULL);
    
    int ALARM_BANK = 1; // "ALARM" is the 2nd bitspace defined in schema
    int OVERTORQUE = 0; // Bit index 0

    printf("\n1. Initial State (Expect 0 everywhere)\n");
    print_bit(&tree, ALARM_BANK, OVERTORQUE, "Robot2", "Plant.Line1.Cell3.Robot2");
    print_bit(&tree, ALARM_BANK, OVERTORQUE, "Cell3",  "Plant.Line1.Cell3");
    print_bit(&tree, ALARM_BANK, OVERTORQUE, "Line1",  "Plant.Line1");

    // Action: Set Robot Alarm
    // "chain_set_bit" comes from chain_tree_logic.c
    printf("\n2. Setting Robot2 Alarm...\n");
    chain_set_bit(&tree, ALARM_BANK, OVERTORQUE, true, "Plant.Line1.Cell3.Robot2");

    // Verification
    printf("\n3. Verification (Expect 1 Bubbled Up)\n");
    print_bit(&tree, ALARM_BANK, OVERTORQUE, "Robot2", "Plant.Line1.Cell3.Robot2");
    
    // These checks prove the Logic Engine ran and propagated the bit up
    print_bit(&tree, ALARM_BANK, OVERTORQUE, "Cell3",  "Plant.Line1.Cell3");
    print_bit(&tree, ALARM_BANK, OVERTORQUE, "Line1",  "Plant.Line1");
    print_bit(&tree, ALARM_BANK, OVERTORQUE, "Plant",  "Plant");

    // Action: Clear Robot Alarm
    printf("\n4. Clearing Robot2 Alarm...\n");
    chain_set_bit(&tree, ALARM_BANK, OVERTORQUE, false, "Plant.Line1.Cell3.Robot2");
    
    // Verification
    printf("\n5. Verification (Expect 0)\n");
    print_bit(&tree, ALARM_BANK, OVERTORQUE, "Line1",  "Plant.Line1");

    chain_tree_destroy(&tree);
    return 0;
}