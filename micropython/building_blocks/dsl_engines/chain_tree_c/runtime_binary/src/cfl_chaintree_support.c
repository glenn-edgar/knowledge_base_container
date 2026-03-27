/*
 * cfl_chaintree_support.c - ChainTree Runtime Support Functions
 *
 * Generic lookup functions operating on cfl_chaintree_handle_t.
 * Works identically for both .h/.c and binary frontend paths.
 */

 #include "cfl_chaintree_support.h"
 #include <string.h>
 
 /* ===== Main Function Lookups ===== */
 
 const char *cfl_get_main_function_name(
     const cfl_chaintree_handle_t *handle, uint16_t func_index)
 {
     if (!handle || !handle->main_function_names) return NULL;
     if (func_index >= handle->main_function_count) return NULL;
     return handle->main_function_names[func_index];
 }
 
 int cfl_get_main_function_index(
     const cfl_chaintree_handle_t *handle, const char *func_name)
 {
     if (!handle || !func_name || !handle->main_function_names) return -1;
     for (uint16_t i = 0; i < handle->main_function_count; i++) {
         if (strcmp(func_name, handle->main_function_names[i]) == 0) return i;
     }
     return -1;
 }
 
 /* ===== One-Shot Function Lookups ===== */
 
 const char *cfl_get_one_shot_function_name(
     const cfl_chaintree_handle_t *handle, uint16_t func_index)
 {
     if (!handle || !handle->one_shot_function_names) return NULL;
     if (func_index >= handle->one_shot_function_count) return NULL;
     return handle->one_shot_function_names[func_index];
 }
 
 int cfl_get_one_shot_function_index(
     const cfl_chaintree_handle_t *handle, const char *func_name)
 {
     if (!handle || !func_name || !handle->one_shot_function_names) return -1;
     for (uint16_t i = 0; i < handle->one_shot_function_count; i++) {
         if (strcmp(func_name, handle->one_shot_function_names[i]) == 0) return i;
     }
     return -1;
 }
 
 /* ===== Boolean Function Lookups ===== */
 
 const char *cfl_get_boolean_function_name(
     const cfl_chaintree_handle_t *handle, uint16_t func_index)
 {
     if (!handle || !handle->boolean_function_names) return NULL;
     if (func_index >= handle->boolean_function_count) return NULL;
     return handle->boolean_function_names[func_index];
 }
 
 int cfl_get_boolean_function_index(
     const cfl_chaintree_handle_t *handle, const char *func_name)
 {
     if (!handle || !func_name || !handle->boolean_function_names) return -1;
     for (uint16_t i = 0; i < handle->boolean_function_count; i++) {
         if (strcmp(func_name, handle->boolean_function_names[i]) == 0) return i;
     }
     return -1;
 }
 
 /* ===== Event Lookups ===== */
 
 const char *cfl_get_event_name(
     const cfl_chaintree_handle_t *handle, uint16_t event_index)
 {
     if (!handle || !handle->event_strings) return NULL;
     if (event_index >= handle->event_count) return NULL;
     return handle->event_strings[event_index];
 }
 
 int cfl_get_event_index(
     const cfl_chaintree_handle_t *handle, const char *name)
 {
     if (!handle || !name || !handle->event_strings) return -1;
     for (uint16_t i = 0; i < handle->event_count; i++) {
         if (strcmp(name, handle->event_strings[i]) == 0) return i;
     }
     return -1;
 }
 
 /* ===== Bitmask Lookups ===== */
 
 const char *cfl_get_bitmask_name(
     const cfl_chaintree_handle_t *handle, uint8_t bit_index)
 {
     if (!handle || !handle->bitmask_names) return NULL;
     if (bit_index >= handle->bitmask_count) return NULL;
     return handle->bitmask_names[bit_index];
 }
 
 int cfl_get_bitmask_index(
     const cfl_chaintree_handle_t *handle, const char *name)
 {
     if (!handle || !name || !handle->bitmask_names) return -1;
     for (uint8_t i = 0; i < handle->bitmask_count; i++) {
         if (strcmp(name, handle->bitmask_names[i]) == 0) return i;
     }
     return -1;
 }
 
 /* ===== KB Info ===== */
 
 uint16_t cfl_get_kb_count(const cfl_chaintree_handle_t *handle)
 {
     if (!handle) return 0;
     return handle->kb_count;
 }