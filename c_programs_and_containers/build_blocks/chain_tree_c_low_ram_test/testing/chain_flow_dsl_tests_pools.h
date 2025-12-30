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

#define POOL_NODE_STATE           0
#define POOL_STATE_MACHINE_STATE  1
#define POOL_CMD                  2
#define POOL_COUNTER              3
#define POOL_SENSOR               4

#define CHAIN_FLOW_POOL_COUNT 6

// ============================================================================
// POOL SIZES
// ============================================================================

#define NODE_STATE_POOL_SIZE           2
#define STATE_MACHINE_STATE_POOL_SIZE  2
#define CMD_POOL_SIZE                  1
#define COUNTER_POOL_SIZE              1
#define SENSOR_POOL_SIZE               1
#define EVENT_POOL_SIZE                1

// ============================================================================
// SLOT DEFINITIONS (pool_id, slot_index)
// ============================================================================

// node_state slots
#define SLOT_BRANCH_1                      POOL_NODE_STATE, 0

// state_machine_state slots
#define SLOT_TEST_30_STATE_MACHINE_STATE   POOL_STATE_MACHINE_STATE, 0
#define SLOT_TEST_30_STATE_MACHINE_STATE_B POOL_STATE_MACHINE_STATE, 1

// cmd_pool slots
#define SLOT_ROBOT_COMMAND                 POOL_CMD, 0

// counter_pool slots
#define SLOT_TIMER_COUNT                   POOL_COUNTER, 0

// sensor_pool slots
#define SLOT_SENSOR_VALUE                  POOL_SENSOR, 0

// event_pool slots
#define SLOT_EVENT_ID                      POOL_EVENT, 0

// ============================================================================
// POOL ARRAYS
// ============================================================================

extern node_state_t node_state_pool[NODE_STATE_POOL_SIZE];
extern state_machine_state_t state_machine_state_pool[STATE_MACHINE_STATE_POOL_SIZE];
extern int32_t cmd_pool[CMD_POOL_SIZE];
extern int32_t counter_pool[COUNTER_POOL_SIZE];
extern int32_t sensor_pool[SENSOR_POOL_SIZE];
extern int32_t event_pool[EVENT_POOL_SIZE];

// ============================================================================
// POOL TABLE
// ============================================================================

extern void* chain_flow_pool_table[CHAIN_FLOW_POOL_COUNT];

// ============================================================================
// CONVENIENCE ACCESSORS
// ============================================================================

#define GET_BRANCH_1()                      (&node_state_pool[0])
#define GET_TEST_30_STATE_MACHINE_STATE()   (&state_machine_state_pool[0])
#define GET_TEST_30_STATE_MACHINE_STATE_B() (&state_machine_state_pool[1])
#define GET_ROBOT_COMMAND()                 (&cmd_pool[0])
#define GET_TIMER_COUNT()                   (&counter_pool[0])
#define GET_SENSOR_VALUE()                  (&sensor_pool[0])

// Pool initialization
void chain_flow_pools_init(void);

#ifdef __cplusplus
}
#endif

#endif // CHAIN_FLOW_POOLS_H