// ============================================================================
// test_comprehensive_pools.h
// Generated Pool Definitions for test_comprehensive module
// Version 2.6 - Slotted Blackboard Support
// ============================================================================

#ifndef TEST_COMPREHENSIVE_POOLS_H
#define TEST_COMPREHENSIVE_POOLS_H

#include <stdint.h>
#include <stdbool.h>

#ifdef __cplusplus
extern "C" {
#endif

// ============================================================================
// POOL TYPE DEFINITIONS
// User must define these structs to match their application
// ============================================================================

typedef struct {
    bool     running;
    uint8_t  speed;
    uint8_t  direction;
    uint8_t  fault_code;
} motor_state_t;

typedef struct {
    bool     on;
    uint8_t  brightness;
    uint8_t  color;
    uint8_t  blink_rate;
} led_state_t;

typedef struct {
    bool     ready;
    bool     calibrated;
    bool     has_power;
    bool     has_fault;
    bool     has_warning;
    bool     has_timeout;
    bool     has_override;
    uint16_t error_code;
} system_state_t;

typedef struct {
    bool     active;
    uint8_t  level;
    uint16_t duration;
} alarm_state_t;

typedef struct {
    uint32_t count;
    uint32_t max_count;
    bool     overflow;
} counter_state_t;

// ============================================================================
// POOL IDS
// ============================================================================

#define POOL_MOTOR_STATE    0
#define POOL_LED_STATE      1
#define POOL_SYSTEM_STATE   2
#define POOL_ALARM_STATE    3
#define POOL_COUNTER_STATE  4

#define TEST_COMPREHENSIVE_POOL_COUNT 5

// ============================================================================
// POOL SIZES (number of slots per pool)
// ============================================================================

#define MOTOR_STATE_POOL_SIZE   2
#define LED_STATE_POOL_SIZE     2
#define SYSTEM_STATE_POOL_SIZE  1
#define ALARM_STATE_POOL_SIZE   1
#define COUNTER_STATE_POOL_SIZE 2

// ============================================================================
// SLOT DEFINITIONS (pool_id, slot_index pairs)
// Usage: S_EXPR_POOL_SLOT(pool_table, SLOT_MOTOR_MAIN, motor_state_t)
// ============================================================================

#define SLOT_MOTOR_MAIN   POOL_MOTOR_STATE, 0
#define SLOT_MOTOR_AUX    POOL_MOTOR_STATE, 1

#define SLOT_LED_STATUS   POOL_LED_STATE, 0
#define SLOT_LED_ALARM    POOL_LED_STATE, 1

#define SLOT_SYS_MAIN     POOL_SYSTEM_STATE, 0

#define SLOT_ALARM_MAIN   POOL_ALARM_STATE, 0

#define SLOT_COUNTER_A    POOL_COUNTER_STATE, 0
#define SLOT_COUNTER_B    POOL_COUNTER_STATE, 1

// ============================================================================
// POOL ARRAYS (extern declarations)
// ============================================================================

extern motor_state_t   motor_state_pool[MOTOR_STATE_POOL_SIZE];
extern led_state_t     led_state_pool[LED_STATE_POOL_SIZE];
extern system_state_t  system_state_pool[SYSTEM_STATE_POOL_SIZE];
extern alarm_state_t   alarm_state_pool[ALARM_STATE_POOL_SIZE];
extern counter_state_t counter_state_pool[COUNTER_STATE_POOL_SIZE];

// ============================================================================
// POOL TABLE (extern declaration)
// ============================================================================

extern void* test_comprehensive_pool_table[TEST_COMPREHENSIVE_POOL_COUNT];

// ============================================================================
// CONVENIENCE ACCESSORS
// ============================================================================

// Direct slot access macros
#define GET_MOTOR_MAIN()   (&motor_state_pool[0])
#define GET_MOTOR_AUX()    (&motor_state_pool[1])
#define GET_LED_STATUS()   (&led_state_pool[0])
#define GET_LED_ALARM()    (&led_state_pool[1])
#define GET_SYS_MAIN()     (&system_state_pool[0])
#define GET_ALARM_MAIN()   (&alarm_state_pool[0])
#define GET_COUNTER_A()    (&counter_state_pool[0])
#define GET_COUNTER_B()    (&counter_state_pool[1])

// Pool initialization
void test_comprehensive_pools_init(void);

#ifdef __cplusplus
}
#endif

#endif // TEST_COMPREHENSIVE_POOLS_H