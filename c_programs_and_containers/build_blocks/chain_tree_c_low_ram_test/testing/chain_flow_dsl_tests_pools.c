// ============================================================================
// chain_flow_pools.c
// Pool Instance Definitions for chain_flow_dsl_tests module
// Version 2.6 - Slotted Blackboard Support
// ============================================================================

#include "chain_flow_dsl_tests_pools.h"
#include <string.h>

// ============================================================================
// POOL ARRAY INSTANCES
// ============================================================================

node_state_t node_state_pool[NODE_STATE_POOL_SIZE];
state_machine_state_t state_machine_state_pool[STATE_MACHINE_STATE_POOL_SIZE];
// ============================================================================
// POOL TABLE
// ============================================================================

void* chain_flow_pool_table[CHAIN_FLOW_POOL_COUNT] = {
    node_state_pool,  // POOL_NODE_STATE = 0
    state_machine_state_pool,  // POOL_STATE_MACHINE_STATE = 1
};

// ============================================================================
// POOL INITIALIZATION
// ============================================================================

void chain_flow_pools_init(void) {
    memset(node_state_pool, 0, sizeof(node_state_pool));
    memset(state_machine_state_pool, 0, sizeof(state_machine_state_pool));
}