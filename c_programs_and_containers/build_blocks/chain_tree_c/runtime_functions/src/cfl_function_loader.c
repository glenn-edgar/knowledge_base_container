/*
 * cfl_function_loader.c - Register all ChainTree functions with binary image loader
 *
 * Registration names use typed names matching C symbols minus "_fn" suffix.
 * These match what stage6_binary.lua stores in the FSTR section.
 *
 * Convention: C symbol "cfl_column_main_main_fn" -> registration name "cfl_column_main_main"
 */

 #include "cfl_image_loader.h"
 #include <stdio.h>
 
 /* =====================================================================
  * Forward declarations
  * ===================================================================== */
 
 /* ---- Main functions ---- */
 extern unsigned cfl_null_main_fn(void*, unsigned, unsigned, unsigned, unsigned, void*);
 extern unsigned cfl_disable_main_fn(void*, unsigned, unsigned, unsigned, unsigned, void*);
 extern unsigned cfl_halt_main_fn(void*, unsigned, unsigned, unsigned, unsigned, void*);
 extern unsigned cfl_reset_main_fn(void*, unsigned, unsigned, unsigned, unsigned, void*);
 extern unsigned cfl_terminate_main_fn(void*, unsigned, unsigned, unsigned, unsigned, void*);
 extern unsigned cfl_terminate_system_main_fn(void*, unsigned, unsigned, unsigned, unsigned, void*);
 extern unsigned cfl_column_main_main_fn(void*, unsigned, unsigned, unsigned, unsigned, void*);
 extern unsigned cfl_local_arena_main_main_fn(void*, unsigned, unsigned, unsigned, unsigned, void*);
 extern unsigned cfl_gate_node_main_main_fn(void*, unsigned, unsigned, unsigned, unsigned, void*);
 extern unsigned cfl_verify_main_fn(void*, unsigned, unsigned, unsigned, unsigned, void*);
 extern unsigned cfl_wait_main_fn(void*, unsigned, unsigned, unsigned, unsigned, void*);
 extern unsigned cfl_wait_time_main_fn(void*, unsigned, unsigned, unsigned, unsigned, void*);
 extern unsigned cfl_event_logger_main_fn(void*, unsigned, unsigned, unsigned, unsigned, void*);
 extern unsigned cfl_fork_main_main_fn(void*, unsigned, unsigned, unsigned, unsigned, void*);
 extern unsigned cfl_join_main_main_fn(void*, unsigned, unsigned, unsigned, unsigned, void*);
 extern unsigned cfl_join_sequence_element_main_fn(void*, unsigned, unsigned, unsigned, unsigned, void*);
 extern unsigned cfl_sequence_pass_main_main_fn(void*, unsigned, unsigned, unsigned, unsigned, void*);
 extern unsigned cfl_sequence_fail_main_main_fn(void*, unsigned, unsigned, unsigned, unsigned, void*);
 extern unsigned cfl_sequence_start_main_main_fn(void*, unsigned, unsigned, unsigned, unsigned, void*);
 extern unsigned cfl_for_main_main_fn(void*, unsigned, unsigned, unsigned, unsigned, void*);
 extern unsigned cfl_watch_dog_main_main_fn(void*, unsigned, unsigned, unsigned, unsigned, void*);
 extern unsigned cfl_while_main_main_fn(void*, unsigned, unsigned, unsigned, unsigned, void*);
 extern unsigned cfl_df_mask_main_main_fn(void*, unsigned, unsigned, unsigned, unsigned, void*);
 extern unsigned cfl_supervisor_main_main_fn(void*, unsigned, unsigned, unsigned, unsigned, void*);
 extern unsigned cfl_state_machine_main_main_fn(void*, unsigned, unsigned, unsigned, unsigned, void*);
 extern unsigned cfl_recovery_main_main_fn(void*, unsigned, unsigned, unsigned, unsigned, void*);
 extern unsigned cfl_exception_catch_all_main_main_fn(void*, unsigned, unsigned, unsigned, unsigned, void*);
 extern unsigned cfl_exception_catch_main_main_fn(void*, unsigned, unsigned, unsigned, unsigned, void*);
 extern unsigned cfl_controlled_node_container_main_main_fn(void*, unsigned, unsigned, unsigned, unsigned, void*);
 extern unsigned cfl_controlled_node_main_main_fn(void*, unsigned, unsigned, unsigned, unsigned, void*);
 extern unsigned cfl_client_controlled_node_main_main_fn(void*, unsigned, unsigned, unsigned, unsigned, void*);
 extern unsigned cfl_streaming_sink_packet_main_fn(void*, unsigned, unsigned, unsigned, unsigned, void*);
 extern unsigned cfl_streaming_tap_packet_main_fn(void*, unsigned, unsigned, unsigned, unsigned, void*);
 extern unsigned cfl_streaming_filter_packet_main_fn(void*, unsigned, unsigned, unsigned, unsigned, void*);
 extern unsigned cfl_streaming_transform_packet_main_fn(void*, unsigned, unsigned, unsigned, unsigned, void*);
 extern unsigned cfl_streaming_collect_packets_main_fn(void*, unsigned, unsigned, unsigned, unsigned, void*);
 extern unsigned cfl_streaming_sink_collected_packets_main_fn(void*, unsigned, unsigned, unsigned, unsigned, void*);
 
 /* ---- One-shot functions ---- */
 extern void cfl_null_one_shot_fn(void*, unsigned);
 extern void cfl_column_init_one_shot_fn(void*, unsigned);
 extern void cfl_column_term_one_shot_fn(void*, unsigned);
 extern void cfl_gate_node_init_one_shot_fn(void*, unsigned);
 extern void cfl_gate_node_term_one_shot_fn(void*, unsigned);
 extern void cfl_log_message_one_shot_fn(void*, unsigned);
 extern void cfl_send_named_event_one_shot_fn(void*, unsigned);
 extern void cfl_verify_init_one_shot_fn(void*, unsigned);
 extern void cfl_verify_term_one_shot_fn(void*, unsigned);
 extern void cfl_wait_init_one_shot_fn(void*, unsigned);
 extern void cfl_wait_term_one_shot_fn(void*, unsigned);
 extern void cfl_wait_time_init_one_shot_fn(void*, unsigned);
 extern void cfl_disable_nodes_one_shot_fn(void*, unsigned);
 extern void cfl_enable_nodes_one_shot_fn(void*, unsigned);
 extern void cfl_event_logger_init_one_shot_fn(void*, unsigned);
 extern void cfl_event_logger_term_one_shot_fn(void*, unsigned);
 extern void cfl_fork_init_one_shot_fn(void*, unsigned);
 extern void cfl_fork_term_one_shot_fn(void*, unsigned);
 extern void cfl_sequence_pass_init_one_shot_fn(void*, unsigned);
 extern void cfl_sequence_pass_term_one_shot_fn(void*, unsigned);
 extern void cfl_sequence_fail_init_one_shot_fn(void*, unsigned);
 extern void cfl_sequence_fail_term_one_shot_fn(void*, unsigned);
 extern void cfl_sequence_start_init_one_shot_fn(void*, unsigned);
 extern void cfl_sequence_start_term_one_shot_fn(void*, unsigned);
 extern void cfl_join_init_one_shot_fn(void*, unsigned);
 extern void cfl_join_term_one_shot_fn(void*, unsigned);
 extern void cfl_join_sequence_element_init_one_shot_fn(void*, unsigned);
 extern void cfl_join_sequence_element_term_one_shot_fn(void*, unsigned);
 extern void cfl_mark_sequence_one_shot_fn(void*, unsigned);
 extern void cfl_watch_dog_init_one_shot_fn(void*, unsigned);
 extern void cfl_watch_dog_term_one_shot_fn(void*, unsigned);
 extern void cfl_while_init_one_shot_fn(void*, unsigned);
 extern void cfl_while_term_one_shot_fn(void*, unsigned);
 extern void cfl_for_init_one_shot_fn(void*, unsigned);
 extern void cfl_for_term_one_shot_fn(void*, unsigned);
 extern void cfl_pat_watch_dog_one_shot_fn(void*, unsigned);
 extern void cfl_enable_watch_dog_one_shot_fn(void*, unsigned);
 extern void cfl_disable_watch_dog_one_shot_fn(void*, unsigned);
 extern void cfl_clear_bitmask_one_shot_fn(void*, unsigned);
 extern void cfl_set_bitmask_one_shot_fn(void*, unsigned);
 extern void cfl_df_mask_init_one_shot_fn(void*, unsigned);
 extern void cfl_df_mask_term_one_shot_fn(void*, unsigned);
 extern void cfl_start_stop_tests_one_shot_fn(void*, unsigned);
 extern void cfl_local_arena_init_one_shot_fn(void*, unsigned);
 extern void cfl_local_arena_term_one_shot_fn(void*, unsigned);
 extern void cfl_supervisor_init_one_shot_fn(void*, unsigned);
 extern void cfl_supervisor_term_one_shot_fn(void*, unsigned);
 extern void cfl_mark_supervisor_node_failure_init_one_shot_fn(void*, unsigned);
 extern void cfl_state_machine_init_one_shot_fn(void*, unsigned);
 extern void cfl_state_machine_term_one_shot_fn(void*, unsigned);
 extern void cfl_change_state_one_shot_fn(void*, unsigned);
 extern void cfl_terminate_state_machine_one_shot_fn(void*, unsigned);
 extern void cfl_reset_state_machine_one_shot_fn(void*, unsigned);
 extern void cfl_set_exception_step_one_shot_fn(void*, unsigned);
 extern void cfl_raise_exception_one_shot_fn(void*, unsigned);
 extern void cfl_heartbeat_event_one_shot_fn(void*, unsigned);
 extern void cfl_turn_heartbeat_off_one_shot_fn(void*, unsigned);
 extern void cfl_turn_heartbeat_on_one_shot_fn(void*, unsigned);
 extern void cfl_recovery_init_one_shot_fn(void*, unsigned);
 extern void cfl_recovery_term_one_shot_fn(void*, unsigned);
 extern void cfl_catch_all_exception_init_one_shot_fn(void*, unsigned);
 extern void cfl_catch_all_exception_term_one_shot_fn(void*, unsigned);
 extern void cfl_exception_catch_init_one_shot_fn(void*, unsigned);
 extern void cfl_exception_catch_term_one_shot_fn(void*, unsigned);
 extern void cfl_controlled_node_container_init_one_shot_fn(void*, unsigned);
 extern void cfl_controlled_node_container_term_one_shot_fn(void*, unsigned);
 extern void cfl_controlled_node_init_one_shot_fn(void*, unsigned);
 extern void cfl_controlled_node_term_one_shot_fn(void*, unsigned);
 extern void cfl_client_controlled_node_init_one_shot_fn(void*, unsigned);
 extern void cfl_client_controlled_node_term_one_shot_fn(void*, unsigned);
 extern void cfl_streaming_sink_packet_init_one_shot_fn(void*, unsigned);
 extern void cfl_streaming_sink_packet_term_one_shot_fn(void*, unsigned);
 extern void cfl_streaming_tap_packet_init_one_shot_fn(void*, unsigned);
 extern void cfl_streaming_tap_packet_term_one_shot_fn(void*, unsigned);
 extern void cfl_streaming_filter_packet_init_one_shot_fn(void*, unsigned);
 extern void cfl_streaming_filter_packet_term_one_shot_fn(void*, unsigned);
 extern void cfl_streaming_transform_packet_init_one_shot_fn(void*, unsigned);
 extern void cfl_streaming_transform_packet_term_one_shot_fn(void*, unsigned);
 extern void cfl_streaming_collect_packets_init_one_shot_fn(void*, unsigned);
 extern void cfl_streaming_collect_packets_term_one_shot_fn(void*, unsigned);
 extern void cfl_streaming_sink_collected_packets_init_one_shot_fn(void*, unsigned);
 extern void cfl_streaming_sink_collected_packets_term_one_shot_fn(void*, unsigned);
 
 /* ---- Boolean functions ---- */
 extern bool cfl_null_boolean_fn(void*, unsigned, unsigned, unsigned, void*);
 extern bool cfl_bool_false_boolean_fn(void*, unsigned, unsigned, unsigned, void*);
 extern bool cfl_column_null_boolean_fn(void*, unsigned, unsigned, unsigned, void*);
 extern bool cfl_gate_node_null_boolean_fn(void*, unsigned, unsigned, unsigned, void*);
 extern bool cfl_verify_time_out_boolean_fn(void*, unsigned, unsigned, unsigned, void*);
 extern bool cfl_wait_for_event_boolean_fn(void*, unsigned, unsigned, unsigned, void*);
 extern bool cfl_state_machine_null_boolean_fn(void*, unsigned, unsigned, unsigned, void*);
 extern bool cfl_sm_event_sync_boolean_fn(void*, unsigned, unsigned, unsigned, void*);
 extern bool cfl_verify_bitmask_boolean_fn(void*, unsigned, unsigned, unsigned, void*);
 extern bool cfl_wait_for_bitmask_boolean_fn(void*, unsigned, unsigned, unsigned, void*);
 extern bool cfl_verify_tests_active_boolean_fn(void*, unsigned, unsigned, unsigned, void*);
 extern bool cfl_wait_for_tests_complete_boolean_fn(void*, unsigned, unsigned, unsigned, void*);
 extern bool cfl_streaming_verify_packet_boolean_fn(void*, unsigned, unsigned, unsigned, void*);
 
 /* =====================================================================
  * Registration helper macros
  *
  * name_str: typed name matching what stage6_binary.lua stores in FSTR
  *           = C symbol minus "_fn" suffix
  * ===================================================================== */
 
  #define REG_MAIN(img, name_str, sym) do { \
    (void)cfl_image_register_main((img), (name_str), (sym)); \
} while(0)

#define REG_ONE_SHOT(img, name_str, sym) do { \
    (void)cfl_image_register_one_shot((img), (name_str), (sym)); \
} while(0)

#define REG_BOOLEAN(img, name_str, sym) do { \
    (void)cfl_image_register_boolean((img), (name_str), (sym)); \
} while(0)

 /* =====================================================================
  * cfl_register_all_main_functions
  *
  * Names = C symbol minus "_fn": "cfl_null_main", "cfl_column_main_main", etc.
  * ===================================================================== */
 
 void cfl_register_all_main_functions(cfl_image_loader_t *img)
 {
     REG_MAIN(img, "cfl_null_main",                          cfl_null_main_fn);
     REG_MAIN(img, "cfl_disable_main",                       cfl_disable_main_fn);
     REG_MAIN(img, "cfl_halt_main",                          cfl_halt_main_fn);
     REG_MAIN(img, "cfl_reset_main",                         cfl_reset_main_fn);
     REG_MAIN(img, "cfl_terminate_main",                     cfl_terminate_main_fn);
     REG_MAIN(img, "cfl_terminate_system_main",              cfl_terminate_system_main_fn);
     REG_MAIN(img, "cfl_column_main_main",                   cfl_column_main_main_fn);
     REG_MAIN(img, "cfl_local_arena_main_main",              cfl_local_arena_main_main_fn);
     REG_MAIN(img, "cfl_gate_node_main_main",                cfl_gate_node_main_main_fn);
     REG_MAIN(img, "cfl_verify_main",                        cfl_verify_main_fn);
     REG_MAIN(img, "cfl_wait_main",                          cfl_wait_main_fn);
     REG_MAIN(img, "cfl_wait_time_main",                     cfl_wait_time_main_fn);
     REG_MAIN(img, "cfl_event_logger_main",                  cfl_event_logger_main_fn);
     REG_MAIN(img, "cfl_fork_main_main",                     cfl_fork_main_main_fn);
     REG_MAIN(img, "cfl_join_main_main",                     cfl_join_main_main_fn);
     REG_MAIN(img, "cfl_join_sequence_element_main",         cfl_join_sequence_element_main_fn);
     REG_MAIN(img, "cfl_sequence_pass_main_main",            cfl_sequence_pass_main_main_fn);
     REG_MAIN(img, "cfl_sequence_fail_main_main",            cfl_sequence_fail_main_main_fn);
     REG_MAIN(img, "cfl_sequence_start_main_main",           cfl_sequence_start_main_main_fn);
     REG_MAIN(img, "cfl_for_main_main",                      cfl_for_main_main_fn);
     REG_MAIN(img, "cfl_watch_dog_main_main",                cfl_watch_dog_main_main_fn);
     REG_MAIN(img, "cfl_while_main_main",                    cfl_while_main_main_fn);
     REG_MAIN(img, "cfl_df_mask_main_main",                  cfl_df_mask_main_main_fn);
     REG_MAIN(img, "cfl_supervisor_main_main",               cfl_supervisor_main_main_fn);
     REG_MAIN(img, "cfl_state_machine_main_main",            cfl_state_machine_main_main_fn);
     REG_MAIN(img, "cfl_recovery_main_main",                 cfl_recovery_main_main_fn);
     REG_MAIN(img, "cfl_exception_catch_all_main_main",      cfl_exception_catch_all_main_main_fn);
     REG_MAIN(img, "cfl_exception_catch_main_main",          cfl_exception_catch_main_main_fn);
     REG_MAIN(img, "cfl_controlled_node_container_main_main", cfl_controlled_node_container_main_main_fn);
     REG_MAIN(img, "cfl_controlled_node_main_main",          cfl_controlled_node_main_main_fn);
     REG_MAIN(img, "cfl_client_controlled_node_main_main",   cfl_client_controlled_node_main_main_fn);
     REG_MAIN(img, "cfl_streaming_sink_packet_main",         cfl_streaming_sink_packet_main_fn);
     REG_MAIN(img, "cfl_streaming_tap_packet_main",          cfl_streaming_tap_packet_main_fn);
     REG_MAIN(img, "cfl_streaming_filter_packet_main",       cfl_streaming_filter_packet_main_fn);
     REG_MAIN(img, "cfl_streaming_transform_packet_main",    cfl_streaming_transform_packet_main_fn);
     REG_MAIN(img, "cfl_streaming_collect_packets_main",     cfl_streaming_collect_packets_main_fn);
     REG_MAIN(img, "cfl_streaming_sink_collected_packets_main", cfl_streaming_sink_collected_packets_main_fn);
    
 }
 
 /* =====================================================================
  * cfl_register_all_one_shot_functions
  * ===================================================================== */
 
 void cfl_register_all_one_shot_functions(cfl_image_loader_t *img)
 {
     REG_ONE_SHOT(img, "cfl_null_one_shot",                              cfl_null_one_shot_fn);
     REG_ONE_SHOT(img, "cfl_column_init_one_shot",                      cfl_column_init_one_shot_fn);
     REG_ONE_SHOT(img, "cfl_column_term_one_shot",                      cfl_column_term_one_shot_fn);
     REG_ONE_SHOT(img, "cfl_gate_node_init_one_shot",                   cfl_gate_node_init_one_shot_fn);
     REG_ONE_SHOT(img, "cfl_gate_node_term_one_shot",                   cfl_gate_node_term_one_shot_fn);
     REG_ONE_SHOT(img, "cfl_log_message_one_shot",                      cfl_log_message_one_shot_fn);
     REG_ONE_SHOT(img, "cfl_send_named_event_one_shot",                 cfl_send_named_event_one_shot_fn);
     REG_ONE_SHOT(img, "cfl_verify_init_one_shot",                      cfl_verify_init_one_shot_fn);
     REG_ONE_SHOT(img, "cfl_verify_term_one_shot",                      cfl_verify_term_one_shot_fn);
     REG_ONE_SHOT(img, "cfl_wait_init_one_shot",                        cfl_wait_init_one_shot_fn);
     REG_ONE_SHOT(img, "cfl_wait_term_one_shot",                        cfl_wait_term_one_shot_fn);
     REG_ONE_SHOT(img, "cfl_wait_time_init_one_shot",                   cfl_wait_time_init_one_shot_fn);
     REG_ONE_SHOT(img, "cfl_disable_nodes_one_shot",                    cfl_disable_nodes_one_shot_fn);
     REG_ONE_SHOT(img, "cfl_enable_nodes_one_shot",                     cfl_enable_nodes_one_shot_fn);
     REG_ONE_SHOT(img, "cfl_event_logger_init_one_shot",                cfl_event_logger_init_one_shot_fn);
     REG_ONE_SHOT(img, "cfl_event_logger_term_one_shot",                cfl_event_logger_term_one_shot_fn);
     REG_ONE_SHOT(img, "cfl_fork_init_one_shot",                        cfl_fork_init_one_shot_fn);
     REG_ONE_SHOT(img, "cfl_fork_term_one_shot",                        cfl_fork_term_one_shot_fn);
     REG_ONE_SHOT(img, "cfl_sequence_pass_init_one_shot",               cfl_sequence_pass_init_one_shot_fn);
     REG_ONE_SHOT(img, "cfl_sequence_pass_term_one_shot",               cfl_sequence_pass_term_one_shot_fn);
     REG_ONE_SHOT(img, "cfl_sequence_fail_init_one_shot",               cfl_sequence_fail_init_one_shot_fn);
     REG_ONE_SHOT(img, "cfl_sequence_fail_term_one_shot",               cfl_sequence_fail_term_one_shot_fn);
     REG_ONE_SHOT(img, "cfl_sequence_start_init_one_shot",              cfl_sequence_start_init_one_shot_fn);
     REG_ONE_SHOT(img, "cfl_sequence_start_term_one_shot",              cfl_sequence_start_term_one_shot_fn);
     REG_ONE_SHOT(img, "cfl_join_init_one_shot",                        cfl_join_init_one_shot_fn);
     REG_ONE_SHOT(img, "cfl_join_term_one_shot",                        cfl_join_term_one_shot_fn);
     REG_ONE_SHOT(img, "cfl_join_sequence_element_init_one_shot",       cfl_join_sequence_element_init_one_shot_fn);
     REG_ONE_SHOT(img, "cfl_join_sequence_element_term_one_shot",       cfl_join_sequence_element_term_one_shot_fn);
     REG_ONE_SHOT(img, "cfl_mark_sequence_one_shot",                    cfl_mark_sequence_one_shot_fn);
     REG_ONE_SHOT(img, "cfl_watch_dog_init_one_shot",                   cfl_watch_dog_init_one_shot_fn);
     REG_ONE_SHOT(img, "cfl_watch_dog_term_one_shot",                   cfl_watch_dog_term_one_shot_fn);
     REG_ONE_SHOT(img, "cfl_while_init_one_shot",                       cfl_while_init_one_shot_fn);
     REG_ONE_SHOT(img, "cfl_while_term_one_shot",                       cfl_while_term_one_shot_fn);
     REG_ONE_SHOT(img, "cfl_for_init_one_shot",                         cfl_for_init_one_shot_fn);
     REG_ONE_SHOT(img, "cfl_for_term_one_shot",                         cfl_for_term_one_shot_fn);
     REG_ONE_SHOT(img, "cfl_pat_watch_dog_one_shot",                    cfl_pat_watch_dog_one_shot_fn);
     REG_ONE_SHOT(img, "cfl_enable_watch_dog_one_shot",                 cfl_enable_watch_dog_one_shot_fn);
     REG_ONE_SHOT(img, "cfl_disable_watch_dog_one_shot",                cfl_disable_watch_dog_one_shot_fn);
     REG_ONE_SHOT(img, "cfl_clear_bitmask_one_shot",                    cfl_clear_bitmask_one_shot_fn);
     REG_ONE_SHOT(img, "cfl_set_bitmask_one_shot",                      cfl_set_bitmask_one_shot_fn);
     REG_ONE_SHOT(img, "cfl_df_mask_init_one_shot",                     cfl_df_mask_init_one_shot_fn);
     REG_ONE_SHOT(img, "cfl_df_mask_term_one_shot",                     cfl_df_mask_term_one_shot_fn);
     REG_ONE_SHOT(img, "cfl_start_stop_tests_one_shot",                 cfl_start_stop_tests_one_shot_fn);
     REG_ONE_SHOT(img, "cfl_local_arena_init_one_shot",                 cfl_local_arena_init_one_shot_fn);
     REG_ONE_SHOT(img, "cfl_local_arena_term_one_shot",                 cfl_local_arena_term_one_shot_fn);
     REG_ONE_SHOT(img, "cfl_supervisor_init_one_shot",                  cfl_supervisor_init_one_shot_fn);
     REG_ONE_SHOT(img, "cfl_supervisor_term_one_shot",                  cfl_supervisor_term_one_shot_fn);
     REG_ONE_SHOT(img, "cfl_mark_supervisor_node_failure_init_one_shot", cfl_mark_supervisor_node_failure_init_one_shot_fn);
     REG_ONE_SHOT(img, "cfl_state_machine_init_one_shot",               cfl_state_machine_init_one_shot_fn);
     REG_ONE_SHOT(img, "cfl_state_machine_term_one_shot",               cfl_state_machine_term_one_shot_fn);
     REG_ONE_SHOT(img, "cfl_change_state_one_shot",                     cfl_change_state_one_shot_fn);
     REG_ONE_SHOT(img, "cfl_terminate_state_machine_one_shot",          cfl_terminate_state_machine_one_shot_fn);
     REG_ONE_SHOT(img, "cfl_reset_state_machine_one_shot",              cfl_reset_state_machine_one_shot_fn);
     REG_ONE_SHOT(img, "cfl_set_exception_step_one_shot",               cfl_set_exception_step_one_shot_fn);
     REG_ONE_SHOT(img, "cfl_raise_exception_one_shot",                  cfl_raise_exception_one_shot_fn);
     REG_ONE_SHOT(img, "cfl_heartbeat_event_one_shot",                  cfl_heartbeat_event_one_shot_fn);
     REG_ONE_SHOT(img, "cfl_turn_heartbeat_off_one_shot",               cfl_turn_heartbeat_off_one_shot_fn);
     REG_ONE_SHOT(img, "cfl_turn_heartbeat_on_one_shot",                cfl_turn_heartbeat_on_one_shot_fn);
     REG_ONE_SHOT(img, "cfl_recovery_init_one_shot",                    cfl_recovery_init_one_shot_fn);
     REG_ONE_SHOT(img, "cfl_recovery_term_one_shot",                    cfl_recovery_term_one_shot_fn);
     REG_ONE_SHOT(img, "cfl_catch_all_exception_init_one_shot",         cfl_catch_all_exception_init_one_shot_fn);
     REG_ONE_SHOT(img, "cfl_catch_all_exception_term_one_shot",         cfl_catch_all_exception_term_one_shot_fn);
     REG_ONE_SHOT(img, "cfl_exception_catch_init_one_shot",             cfl_exception_catch_init_one_shot_fn);
     REG_ONE_SHOT(img, "cfl_exception_catch_term_one_shot",             cfl_exception_catch_term_one_shot_fn);
     REG_ONE_SHOT(img, "cfl_controlled_node_container_init_one_shot",   cfl_controlled_node_container_init_one_shot_fn);
     REG_ONE_SHOT(img, "cfl_controlled_node_container_term_one_shot",   cfl_controlled_node_container_term_one_shot_fn);
     REG_ONE_SHOT(img, "cfl_controlled_node_init_one_shot",             cfl_controlled_node_init_one_shot_fn);
     REG_ONE_SHOT(img, "cfl_controlled_node_term_one_shot",             cfl_controlled_node_term_one_shot_fn);
     REG_ONE_SHOT(img, "cfl_client_controlled_node_init_one_shot",      cfl_client_controlled_node_init_one_shot_fn);
     REG_ONE_SHOT(img, "cfl_client_controlled_node_term_one_shot",      cfl_client_controlled_node_term_one_shot_fn);
     REG_ONE_SHOT(img, "cfl_streaming_sink_packet_init_one_shot",       cfl_streaming_sink_packet_init_one_shot_fn);
     REG_ONE_SHOT(img, "cfl_streaming_sink_packet_term_one_shot",       cfl_streaming_sink_packet_term_one_shot_fn);
     REG_ONE_SHOT(img, "cfl_streaming_tap_packet_init_one_shot",        cfl_streaming_tap_packet_init_one_shot_fn);
     REG_ONE_SHOT(img, "cfl_streaming_tap_packet_term_one_shot",        cfl_streaming_tap_packet_term_one_shot_fn);
     REG_ONE_SHOT(img, "cfl_streaming_filter_packet_init_one_shot",     cfl_streaming_filter_packet_init_one_shot_fn);
     REG_ONE_SHOT(img, "cfl_streaming_filter_packet_term_one_shot",     cfl_streaming_filter_packet_term_one_shot_fn);
     REG_ONE_SHOT(img, "cfl_streaming_transform_packet_init_one_shot",  cfl_streaming_transform_packet_init_one_shot_fn);
     REG_ONE_SHOT(img, "cfl_streaming_transform_packet_term_one_shot",  cfl_streaming_transform_packet_term_one_shot_fn);
     REG_ONE_SHOT(img, "cfl_streaming_collect_packets_init_one_shot",   cfl_streaming_collect_packets_init_one_shot_fn);
     REG_ONE_SHOT(img, "cfl_streaming_collect_packets_term_one_shot",   cfl_streaming_collect_packets_term_one_shot_fn);
     REG_ONE_SHOT(img, "cfl_streaming_sink_collected_packets_init_one_shot", cfl_streaming_sink_collected_packets_init_one_shot_fn);
     REG_ONE_SHOT(img, "cfl_streaming_sink_collected_packets_term_one_shot", cfl_streaming_sink_collected_packets_term_one_shot_fn);
 }
 
 /* =====================================================================
  * cfl_register_all_boolean_functions
  * ===================================================================== */
 
 void cfl_register_all_boolean_functions(cfl_image_loader_t *img)
 {
     REG_BOOLEAN(img, "cfl_null_boolean",                     cfl_null_boolean_fn);
     REG_BOOLEAN(img, "cfl_bool_false_boolean",               cfl_bool_false_boolean_fn);
     REG_BOOLEAN(img, "cfl_column_null_boolean",              cfl_column_null_boolean_fn);
     REG_BOOLEAN(img, "cfl_gate_node_null_boolean",           cfl_gate_node_null_boolean_fn);
     REG_BOOLEAN(img, "cfl_verify_time_out_boolean",          cfl_verify_time_out_boolean_fn);
     REG_BOOLEAN(img, "cfl_wait_for_event_boolean",           cfl_wait_for_event_boolean_fn);
     REG_BOOLEAN(img, "cfl_state_machine_null_boolean",       cfl_state_machine_null_boolean_fn);
     REG_BOOLEAN(img, "cfl_sm_event_sync_boolean",            cfl_sm_event_sync_boolean_fn);
     REG_BOOLEAN(img, "cfl_verify_bitmask_boolean",           cfl_verify_bitmask_boolean_fn);
     REG_BOOLEAN(img, "cfl_wait_for_bitmask_boolean",         cfl_wait_for_bitmask_boolean_fn);
     REG_BOOLEAN(img, "cfl_verify_tests_active_boolean",      cfl_verify_tests_active_boolean_fn);
     REG_BOOLEAN(img, "cfl_wait_for_tests_complete_boolean",  cfl_wait_for_tests_complete_boolean_fn);
     REG_BOOLEAN(img, "cfl_streaming_verify_packet_boolean",  cfl_streaming_verify_packet_boolean_fn);
 }
 
 /* =====================================================================
  * Convenience: register everything in one call
  * ===================================================================== */
 
 void cfl_register_all_functions(cfl_image_loader_t *img)
 {
     cfl_register_all_main_functions(img);
     cfl_register_all_one_shot_functions(img);
     cfl_register_all_boolean_functions(img);
    
 }