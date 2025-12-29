// ============================================================================
// chain_flow_pools.h
// Generated Pool Definitions for chain_flow_dsl_tests module
// Version 2.6 - Slotted Blackboard Support
// ============================================================================

#ifndef CHAIN_FLOW_POOLS_H
#define CHAIN_FLOW_POOLS_H

#include <stdint.h>
#include <stdbool.h>

#ifdef __cplusplus
extern "C" {
#endif

// ============================================================================
// POOL TYPE DEFINITIONS
// ============================================================================

typedef struct {
    bool children_active;
} node_state_t;

typedef struct {
    uint32_t state;
} state_machine_state_t;

// ============================================================================
// POOL IDS
// ============================================================================

#define POOL_NODE_STATE  0

#define CHAIN_FLOW_POOL_COUNT 2

// ============================================================================
// POOL SIZES
// ============================================================================

#define NODE_STATE_POOL_SIZE 2
#define STATE_MACHINE_STATE_POOL_SIZE 1

// ============================================================================
// SLOT DEFINITIONS (pool_id, slot_index)
// ============================================================================

#define SLOT_BRANCH_1  POOL_NODE_STATE, 0
#define SLOT_STATE_MACHINE_STATE  POOL_STATE_MACHINE_STATE, 0
// ============================================================================
// POOL ARRAYS
// ============================================================================

extern node_state_t node_state_pool[NODE_STATE_POOL_SIZE];
extern state_machine_state_t state_machine_state_pool[STATE_MACHINE_STATE_POOL_SIZE];

// ============================================================================
// POOL TABLE
// ============================================================================

extern void* chain_flow_pool_table[CHAIN_FLOW_POOL_COUNT];

// ============================================================================
// CONVENIENCE ACCESSORS
// ============================================================================

#define GET_BRANCH_1()  (&node_state_pool[0])

// Pool initialization
void chain_flow_pools_init(void);

#ifdef __cplusplus
}
#endif

#endif // CHAIN_FLOW_POOLS_H