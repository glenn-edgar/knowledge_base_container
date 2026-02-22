/* pump_station_test.c
 * Auto-generated test for Scan Tree: pump_station
 */

#include "pump_station.h"
#include <stdio.h>
#include <string.h>

static const char *state_name(int8_t state)
{
    switch (state) {
        case  1: return "ACTIVE";
        case  0: return "FAULT";
        case -1: return "NOT_OPERATIONAL";
        default: return "UNKNOWN";
    }
}

static void print_layer(const char *name, const scan_tree_layer_buf_t *buf)
{
    printf("  %s: ", name);
    for (uint32_t i = 0; i < buf->size; i++) {
        int8_t s = buf->not_active[i] ? -1 : (buf->value[i] ? 1 : 0);
        printf("[%u]=%s ", i, state_name(s));
    }
    printf("\n");
}

int main(void)
{
    pump_station_ctx_t ctx;
    pump_station_init(&ctx);

    printf("=== Initial State (all Not Operational) ===\n");
    print_layer("power_output", &ctx.pump_station_power_power_output);
    print_layer("actuation_output", &ctx.pump_station_actuation_actuation_output);
    print_layer("group_a_output", &ctx.pump_station_actuation_group_a_group_a_output);
    print_layer("group_b_output", &ctx.pump_station_actuation_group_b_group_b_output);

    printf("\n=== Scenario 1: Grid power ON, no faults ===\n");
    ((uint8_t*)ctx.pump_station_power_status.current)[0] = 1;  /* grid power ON */
    pump_station_swap_raw_buffers(&ctx);
    pump_station_evaluate(&ctx);
    print_layer("power_output", &ctx.pump_station_power_power_output);
    print_layer("actuation_output", &ctx.pump_station_actuation_actuation_output);
    print_layer("group_a_output", &ctx.pump_station_actuation_group_a_group_a_output);
    print_layer("group_b_output", &ctx.pump_station_actuation_group_b_group_b_output);

    printf("\n=== Scenario 2: Pump 0 faults ===\n");
    ((uint8_t*)ctx.pump_station_pump_faults.current)[0] = 1;  /* pump 0 fault */
    pump_station_swap_raw_buffers(&ctx);
    pump_station_evaluate(&ctx);
    print_layer("power_output", &ctx.pump_station_power_power_output);
    print_layer("actuation_output", &ctx.pump_station_actuation_actuation_output);
    print_layer("group_a_output", &ctx.pump_station_actuation_group_a_group_a_output);
    print_layer("group_b_output", &ctx.pump_station_actuation_group_b_group_b_output);

    printf("\n=== Scenario 3: Operator clears pump 0 fault ===\n");
    ((uint8_t*)ctx.pump_station_alarm_clear.current)[0] = 1;  /* clear pump 0 */
    ((uint8_t*)ctx.pump_station_pump_faults.current)[0] = 0;  /* fault cleared */
    pump_station_swap_raw_buffers(&ctx);
    pump_station_evaluate(&ctx);
    print_layer("power_output", &ctx.pump_station_power_power_output);
    print_layer("actuation_output", &ctx.pump_station_actuation_actuation_output);
    print_layer("group_a_output", &ctx.pump_station_actuation_group_a_group_a_output);
    print_layer("group_b_output", &ctx.pump_station_actuation_group_b_group_b_output);

    printf("\n=== Scenario 4: Clear alarm_clear bit (normal operation) ===\n");
    ((uint8_t*)ctx.pump_station_alarm_clear.current)[0] = 0;  /* remove clear */
    pump_station_swap_raw_buffers(&ctx);
    pump_station_evaluate(&ctx);
    print_layer("power_output", &ctx.pump_station_power_power_output);
    print_layer("actuation_output", &ctx.pump_station_actuation_actuation_output);
    print_layer("group_a_output", &ctx.pump_station_actuation_group_a_group_a_output);
    print_layer("group_b_output", &ctx.pump_station_actuation_group_b_group_b_output);

    printf("\nDone.\n");
    return 0;
}
