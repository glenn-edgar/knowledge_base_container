// ============================================================================
// test_comprehensive_pools.c
// Pool Instance Definitions for test_comprehensive module
// Version 2.6 - Slotted Blackboard Support
// ============================================================================

#include "test_comprehensive_pools.h"
#include <string.h>

// ============================================================================
// POOL ARRAY INSTANCES
// ============================================================================

motor_state_t   motor_state_pool[MOTOR_STATE_POOL_SIZE];
led_state_t     led_state_pool[LED_STATE_POOL_SIZE];
system_state_t  system_state_pool[SYSTEM_STATE_POOL_SIZE];
alarm_state_t   alarm_state_pool[ALARM_STATE_POOL_SIZE];
counter_state_t counter_state_pool[COUNTER_STATE_POOL_SIZE];

// ============================================================================
// POOL TABLE
// ============================================================================

void* test_comprehensive_pool_table[TEST_COMPREHENSIVE_POOL_COUNT] = {
    motor_state_pool,      // POOL_MOTOR_STATE   = 0
    led_state_pool,        // POOL_LED_STATE     = 1
    system_state_pool,     // POOL_SYSTEM_STATE  = 2
    alarm_state_pool,      // POOL_ALARM_STATE   = 3
    counter_state_pool,    // POOL_COUNTER_STATE = 4
};

// ============================================================================
// POOL INITIALIZATION
// ============================================================================

void test_comprehensive_pools_init(void) {
    memset(motor_state_pool, 0, sizeof(motor_state_pool));
    memset(led_state_pool, 0, sizeof(led_state_pool));
    memset(system_state_pool, 0, sizeof(system_state_pool));
    memset(alarm_state_pool, 0, sizeof(alarm_state_pool));
    memset(counter_state_pool, 0, sizeof(counter_state_pool));
}