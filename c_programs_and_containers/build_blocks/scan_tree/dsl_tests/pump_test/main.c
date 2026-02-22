/* pump_station_test.c */
#include "pump_station.h"
#include "st_display.h"
#include <stdio.h>
#include <stdlib.h>

static const char *sn(int8_t s){return s==1?"ACTIVE":s==0?"FAULT":"NOT_OP";}

static void print_states(const char *label, const int8_t *s, uint16_t n)
{
    printf("  %s:", label);
    for (uint16_t i = 0; i < n; i++)
        printf(" [%u]=%s", i, sn(s[i]));
    printf("\n");
}

int main(void)
{
    st_handle_t h;
    if (st_init(&h, &pump_station_desc) != 0) { fprintf(stderr, "init failed\n"); return 1; }

    /* Cache raw buffer pointers — type-checked */
    uint8_t *power_status     = ST_RAW_PTR(&h, PUMP_STATION_POWER_STATUS_ID, uint8_t);
    float   *motor_current    = ST_RAW_PTR(&h, PUMP_STATION_MOTOR_CURRENT_ID, float);
    float   *motor_thresholds = ST_RAW_PTR(&h, PUMP_STATION_MOTOR_THRESHOLDS_ID, float);

    if (!power_status || !motor_current || !motor_thresholds) {
        fprintf(stderr, "raw buffer type mismatch\n");
        return 1;
    }

    /* Cache layer state pointers — stable for lifetime of handle */
    uint16_t power_sz, act_sz, ga_sz, gb_sz;
    const int8_t *power_states   = st_layer_states(&h, PUMP_STATION_POWER_POWER_OUTPUT_ID, &power_sz);
    const int8_t *act_states     = st_layer_states(&h, PUMP_STATION_ACTUATION_ACTUATION_OUTPUT_ID, &act_sz);
    const int8_t *group_a_states = st_layer_states(&h, PUMP_STATION_ACTUATION_GROUP_A_GROUP_A_OUTPUT_ID, &ga_sz);
    const int8_t *group_b_states = st_layer_states(&h, PUMP_STATION_ACTUATION_GROUP_B_GROUP_B_OUTPUT_ID, &gb_sz);

    if (!power_states || !act_states || !group_a_states || !group_b_states) {
        fprintf(stderr, "layer state pointer failure\n");
        return 1;
    }

    /* Build display hierarchy once */
    st_display_t *disp = st_display_init(&h);
    if (!disp) { fprintf(stderr, "display init failed\n"); return 1; }

    st_print_info(&h);

    /* --- Step 0: Initial state --- */
    printf("\n=== Step 0: Initial ===\n");
    print_states("power",   power_states,   power_sz);
    print_states("actuation", act_states,   act_sz);
    print_states("group_a", group_a_states, ga_sz);
    print_states("group_b", group_b_states, gb_sz);
    st_display_tree(disp, &h);

    /* --- Step 1: Power ON, set thresholds --- */
    printf("\n=== Step 1: Power ON, thresholds=100A ===\n");
    power_status[0] = 1;
    for (int i = 0; i < 4; i++) motor_thresholds[i] = 100.0f;
    st_cycle(&h);
    print_states("power",   power_states,   power_sz);
    print_states("actuation", act_states,   act_sz);
    print_states("group_a", group_a_states, ga_sz);
    print_states("group_b", group_b_states, gb_sz);
    st_display_tree(disp, &h);

    /* --- Step 2: Pump 0 overcurrent --- */
    printf("\n=== Step 2: Pump 0 overcurrent (150A) ===\n");
    motor_current[0] = 150.0f;
    st_cycle(&h);
    print_states("power",   power_states,   power_sz);
    print_states("actuation", act_states,   act_sz);
    print_states("group_a", group_a_states, ga_sz);
    print_states("group_b", group_b_states, gb_sz);
    st_display_tree(disp, &h);

    /* --- Step 3: Pump 0 current returns to normal --- */
    printf("\n=== Step 3: Pump 0 current normal (50A) ===\n");
    motor_current[0] = 50.0f;
    st_cycle(&h);
    print_states("power",   power_states,   power_sz);
    print_states("actuation", act_states,   act_sz);
    print_states("group_a", group_a_states, ga_sz);
    print_states("group_b", group_b_states, gb_sz);
    st_display_tree(disp, &h);

    /* --- Step 4: Power OFF --- */
    printf("\n=== Step 4: Power OFF ===\n");
    power_status[0] = 0;
    st_cycle(&h);
    print_states("power",   power_states,   power_sz);
    print_states("actuation", act_states,   act_sz);
    print_states("group_a", group_a_states, ga_sz);
    print_states("group_b", group_b_states, gb_sz);
    st_display_tree(disp, &h);

    /* --- Step 5: Power back ON --- */
    printf("\n=== Step 5: Power back ON ===\n");
    power_status[0] = 1;
    st_cycle(&h);
    print_states("power",   power_states,   power_sz);
    print_states("actuation", act_states,   act_sz);
    print_states("group_a", group_a_states, ga_sz);
    print_states("group_b", group_b_states, gb_sz);
    st_display_tree(disp, &h);

    st_display_destroy(disp);
    st_destroy(&h);
    printf("\nDone.\n");
    return 0;
}