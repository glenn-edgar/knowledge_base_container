/*
 * chain_flow_dsl_tests_user_registration.c
 * Manual — registers user functions only (TEST_31 through TEST_39).
 * CFL_* and SE_* bridge functions are registered via cfl_se_get_*_table().
 */

#include "chain_flow_dsl_tests.h"
#include "s_engine_module.h"

extern void test_31_set_motor_oneshot(void*, const void*, uint16_t, uint16_t, uint16_t, void*);
extern void test_32_toggle_led_oneshot(void*, const void*, uint16_t, uint16_t, uint16_t, void*);
extern void test_32_enable_buzzer_oneshot(void*, const void*, uint16_t, uint16_t, uint16_t, void*);
extern void test_32_set_led_oneshot(void*, const void*, uint16_t, uint16_t, uint16_t, void*);
extern void test_32_notify_system_oneshot(void*, const void*, uint16_t, uint16_t, uint16_t, void*);
extern void test_32_disable_all_outputs_oneshot(void*, const void*, uint16_t, uint16_t, uint16_t, void*);
extern void test_32_save_state_oneshot(void*, const void*, uint16_t, uint16_t, uint16_t, void*);
extern void test_33_read_vector_oneshot(void*, const void*, uint16_t, uint16_t, uint16_t, void*);
extern void test_33_read_pid_oneshot(void*, const void*, uint16_t, uint16_t, uint16_t, void*);
extern void test_33_read_system_oneshot(void*, const void*, uint16_t, uint16_t, uint16_t, void*);
extern void test_34_alloc_node_oneshot(void*, const void*, uint16_t, uint16_t, uint16_t, void*);
extern void test_34_alloc_sensor_oneshot(void*, const void*, uint16_t, uint16_t, uint16_t, void*);
extern void test_34_read_node_oneshot(void*, const void*, uint16_t, uint16_t, uint16_t, void*);
extern void test_34_read_sensor_oneshot(void*, const void*, uint16_t, uint16_t, uint16_t, void*);
extern void test_34_read_uint32_oneshot(void*, const void*, uint16_t, uint16_t, uint16_t, void*);
extern void test_34_read_uint16_oneshot(void*, const void*, uint16_t, uint16_t, uint16_t, void*);
extern void test_34_check_null_oneshot(void*, const void*, uint16_t, uint16_t, uint16_t, void*);
extern void test_34_free_ptr_oneshot(void*, const void*, uint16_t, uint16_t, uint16_t, void*);
extern void test_35_build_list_oneshot(void*, const void*, uint16_t, uint16_t, uint16_t, void*);
extern void test_35_traverse_list_oneshot(void*, const void*, uint16_t, uint16_t, uint16_t, void*);
extern void test_35_free_list_oneshot(void*, const void*, uint16_t, uint16_t, uint16_t, void*);
extern void test_36_copy_ptr_oneshot(void*, const void*, uint16_t, uint16_t, uint16_t, void*);
extern void test_36_verify_same_ptr_oneshot(void*, const void*, uint16_t, uint16_t, uint16_t, void*);
extern void test_36_modify_node_value_oneshot(void*, const void*, uint16_t, uint16_t, uint16_t, void*);
extern void test_36_clear_ptr_oneshot(void*, const void*, uint16_t, uint16_t, uint16_t, void*);
extern void test_37_copy_static_network_oneshot(void*, const void*, uint16_t, uint16_t, uint16_t, void*);
extern void test_37_verify_network_oneshot(void*, const void*, uint16_t, uint16_t, uint16_t, void*);
extern void test_37_verify_sensors_oneshot(void*, const void*, uint16_t, uint16_t, uint16_t, void*);
extern void test_37_verify_device_name_oneshot(void*, const void*, uint16_t, uint16_t, uint16_t, void*);
extern void test_37_verify_device_serial_oneshot(void*, const void*, uint16_t, uint16_t, uint16_t, void*);
extern void test_37_verify_device_info_oneshot(void*, const void*, uint16_t, uint16_t, uint16_t, void*);
extern void test_37_verify_top_level_oneshot(void*, const void*, uint16_t, uint16_t, uint16_t, void*);
extern void test_37_dump_state_oneshot(void*, const void*, uint16_t, uint16_t, uint16_t, void*);
extern void test_37_verify_string_ptr_oneshot(void*, const void*, uint16_t, uint16_t, uint16_t, void*);
extern void test_38_verify_defaults_oneshot(void*, const void*, uint16_t, uint16_t, uint16_t, void*);
extern void test_38_verify_test_pid_oneshot(void*, const void*, uint16_t, uint16_t, uint16_t, void*);
extern void test_39_verify_gains_oneshot(void*, const void*, uint16_t, uint16_t, uint16_t, void*);
extern void test_39_verify_pointer_oneshot(void*, const void*, uint16_t, uint16_t, uint16_t, void*);
extern s_expr_result_t test_32_process_scheduled_tasks_main(void*, const void*, uint16_t, uint16_t, uint16_t, void*);
extern s_expr_result_t test_32_run_background_tasks_main(void*, const void*, uint16_t, uint16_t, uint16_t, void*);
extern s_expr_result_t test_32_check_threshold_main(void*, const void*, uint16_t, uint16_t, uint16_t, void*);
extern s_expr_result_t test_32_generate_internal_events_main(void*, const void*, uint16_t, uint16_t, uint16_t, void*);

static s_expr_fn_entry_t user_oneshot_entries[] = {
    { 0xCA568877, (void*)test_31_set_motor_oneshot },
    { 0x8ACEDC6E, (void*)test_32_toggle_led_oneshot },
    { 0x81B642F2, (void*)test_32_enable_buzzer_oneshot },
    { 0x8B9E28B2, (void*)test_32_set_led_oneshot },
    { 0x1593B0BB, (void*)test_32_notify_system_oneshot },
    { 0x9BEEA78D, (void*)test_32_disable_all_outputs_oneshot },
    { 0x9B59F0CB, (void*)test_32_save_state_oneshot },
    { 0x88F549D9, (void*)test_33_read_vector_oneshot },
    { 0xC12BB41B, (void*)test_33_read_pid_oneshot },
    { 0xCC1D9E5B, (void*)test_33_read_system_oneshot },
    { 0xF88A0240, (void*)test_34_alloc_node_oneshot },
    { 0x5A022D1A, (void*)test_34_alloc_sensor_oneshot },
    { 0xA29AE36F, (void*)test_34_read_node_oneshot },
    { 0x5334E5D1, (void*)test_34_read_sensor_oneshot },
    { 0xA446E0E2, (void*)test_34_read_uint32_oneshot },
    { 0x304C3A74, (void*)test_34_read_uint16_oneshot },
    { 0x3311CA8A, (void*)test_34_check_null_oneshot },
    { 0xD354D8E9, (void*)test_34_free_ptr_oneshot },
    { 0xEE44D556, (void*)test_35_build_list_oneshot },
    { 0xABB0C6FE, (void*)test_35_traverse_list_oneshot },
    { 0x05F01006, (void*)test_35_free_list_oneshot },
    { 0x1E3246EC, (void*)test_36_copy_ptr_oneshot },
    { 0x285D86AD, (void*)test_36_verify_same_ptr_oneshot },
    { 0x09440419, (void*)test_36_modify_node_value_oneshot },
    { 0x689DAB3C, (void*)test_36_clear_ptr_oneshot },
    { 0x8DBF50B6, (void*)test_37_copy_static_network_oneshot },
    { 0x9D921679, (void*)test_37_verify_network_oneshot },
    { 0x116ACB0C, (void*)test_37_verify_sensors_oneshot },
    { 0x2F23BFFB, (void*)test_37_verify_device_name_oneshot },
    { 0x7293A0CC, (void*)test_37_verify_device_serial_oneshot },
    { 0xADB946C4, (void*)test_37_verify_device_info_oneshot },
    { 0x25ED98CD, (void*)test_37_verify_top_level_oneshot },
    { 0x85E37965, (void*)test_37_dump_state_oneshot },
    { 0x3A2C8535, (void*)test_37_verify_string_ptr_oneshot },
    { 0x3C18F4F2, (void*)test_38_verify_defaults_oneshot },
    { 0x262F60C6, (void*)test_38_verify_test_pid_oneshot },
    { 0x83BFA6A9, (void*)test_39_verify_gains_oneshot },
    { 0x61B4DC54, (void*)test_39_verify_pointer_oneshot },
};

static const s_expr_fn_table_t user_oneshot_table = {
    .entries = user_oneshot_entries,
    .count = 38
};

static s_expr_fn_entry_t user_main_entries[] = {
    { 0xE8D34100, (void*)test_32_process_scheduled_tasks_main },
    { 0x86573308, (void*)test_32_check_threshold_main },
    { 0x18DDD46F, (void*)test_32_generate_internal_events_main },
    { 0x7E0874DB, (void*)test_32_run_background_tasks_main },
};

static const s_expr_fn_table_t user_main_table = {
    .entries = user_main_entries,
    .count = 4
};

void chain_flow_dsl_tests_register_all(s_expr_module_t* module) {
    s_expr_module_register_oneshot(module, &user_oneshot_table);
    s_expr_module_register_main(module, &user_main_table);
}
