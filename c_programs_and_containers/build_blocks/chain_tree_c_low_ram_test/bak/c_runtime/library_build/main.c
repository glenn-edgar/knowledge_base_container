/**
 * @file main.c
 * @brief Unit tests for CFL Timer system
 * 
 * @author Glenn - Onyx Engineering
 * @date 2025
 */

 #include <stdio.h>
 #include <stdlib.h>
 #include <string.h>
 #include <stdint.h>
 #include <stdbool.h>
 #include <unistd.h>
 #include <time.h>
 #include "cfl_timer_system.h"
 #include "cfl_perm.h"
 #include "cfl_exception.h"
 
 /*==============================================================================
  * Test Framework Macros
  *============================================================================*/
 
 #define TEST_ASSERT(condition, message) \
     do { \
         if (!(condition)) { \
             printf("  ✗ FAIL: %s\n", message); \
             printf("    Line %d: %s\n", __LINE__, #condition); \
             test_failed = true; \
             return false; \
         } \
     } while(0)
 
 #define TEST_ASSERT_EQ(actual, expected, message) \
     do { \
         if ((actual) != (expected)) { \
             printf("  ✗ FAIL: %s\n", message); \
             printf("    Line %d: Expected %ld, got %ld\n", __LINE__, (long)(expected), (long)(actual)); \
             test_failed = true; \
             return false; \
         } \
     } while(0)
 
 #define TEST_ASSERT_NEQ(actual, not_expected, message) \
     do { \
         if ((actual) == (not_expected)) { \
             printf("  ✗ FAIL: %s\n", message); \
             printf("    Line %d: Should not equal %ld\n", __LINE__, (long)(not_expected)); \
             test_failed = true; \
             return false; \
         } \
     } while(0)
 
 #define TEST_ASSERT_NEAR(actual, expected, tolerance, message) \
     do { \
         double diff = (actual) - (expected); \
         if (diff < 0) diff = -diff; \
         if (diff > (tolerance)) { \
             printf("  ✗ FAIL: %s\n", message); \
             printf("    Line %d: Expected %.3f ± %.3f, got %.3f\n", __LINE__, \
                    (double)(expected), (double)(tolerance), (double)(actual)); \
             test_failed = true; \
             return false; \
         } \
     } while(0)
 
 #define RUN_TEST(test_func) \
     do { \
         printf("\n=== Running: %s ===\n", #test_func); \
         test_failed = false; \
         if (test_func()) { \
             printf("  ✓ PASS\n"); \
             tests_passed++; \
         } else { \
             tests_failed++; \
         } \
         tests_run++; \
     } while(0)
 
 /*==============================================================================
  * Global Test State
  *============================================================================*/
 
 static bool test_failed = false;
 static int tests_run = 0;
 static int tests_passed = 0;
 static int tests_failed = 0;
 
 // Permanent allocator for tests
 static CflPerm* g_perm = NULL;
 static uint8_t g_perm_buffer[16384];  // 16KB buffer for tests
 
 /*==============================================================================
  * Test Setup/Teardown
  *============================================================================*/
 
 static void setup_perm_allocator(void)
 {
     g_perm = cfl_perm_create();
     cfl_perm_init(g_perm, g_perm_buffer, sizeof(g_perm_buffer));
     printf("  Setup: Perm allocator initialized (%u bytes)\n", (unsigned)sizeof(g_perm_buffer));
 }
 
 static void teardown_perm_allocator(void)
 {
     if (g_perm) {
         cfl_perm_reset(g_perm);
         printf("  Teardown: Perm allocator reset\n");
     }
 }
 
 /*==============================================================================
  * Test Cases
  *============================================================================*/
 
 /**
  * Test 1: Basic timer creation
  */
 static bool test_timer_creation(void)
 {
     setup_perm_allocator();
     
     cfl_timer_handle_t timer = cfl_timer_create(1.0, g_perm);
     TEST_ASSERT(timer != NULL, "Timer creation should succeed");
     
     double wait = cfl_timer_get_wait(timer);
     TEST_ASSERT_NEAR(wait, 1.0, 0.001, "Wait time should match");
     
     // No destroy call - memory persists in perm allocator
     
     teardown_perm_allocator();
     return true;
 }
 
 /**
  * Test 2: Set and get wait time
  */
 static bool test_wait_time_configuration(void)
 {
     setup_perm_allocator();
     
     cfl_timer_handle_t timer = cfl_timer_create(1.0, g_perm);
     TEST_ASSERT(timer != NULL, "Timer creation should succeed");
     
     cfl_timer_error_t err = cfl_timer_set_wait(timer, 2.5);
     TEST_ASSERT_EQ(err, CFL_TIMER_SUCCESS, "Set wait should succeed");
     
     double wait = cfl_timer_get_wait(timer);
     TEST_ASSERT_NEAR(wait, 2.5, 0.001, "Wait time should be updated");
     
     // Test invalid parameters
     err = cfl_timer_set_wait(NULL, 1.0);
     TEST_ASSERT_EQ(err, CFL_TIMER_ERROR_INVALID_HANDLE, "NULL handle should return error");
     
     err = cfl_timer_set_wait(timer, -1.0);
     TEST_ASSERT_EQ(err, CFL_TIMER_ERROR_INVALID_PARAM, "Negative wait should return error");
     
     double invalid_wait = cfl_timer_get_wait(NULL);
     TEST_ASSERT_NEAR(invalid_wait, -1.0, 0.001, "NULL handle should return -1.0");
     
     teardown_perm_allocator();
     return true;
 }
 
 /**
  * Test 3: Get current time simple (no handle)
  */
 static bool test_get_time_simple(void)
 {
     cfl_time_info_t time_info;
     
     cfl_timer_error_t err = cfl_timer_get_time_simple(&time_info);
     TEST_ASSERT_EQ(err, CFL_TIMER_SUCCESS, "Get time simple should succeed");
     
     // Validate reasonable values
     TEST_ASSERT(time_info.year >= 2024 && time_info.year <= 2100, "Year should be reasonable");
     TEST_ASSERT(time_info.month >= 1 && time_info.month <= 12, "Month should be 1-12");
     TEST_ASSERT(time_info.day >= 1 && time_info.day <= 31, "Day should be 1-31");
     TEST_ASSERT(time_info.hour >= 0 && time_info.hour <= 23, "Hour should be 0-23");
     TEST_ASSERT(time_info.minute >= 0 && time_info.minute <= 59, "Minute should be 0-59");
     TEST_ASSERT(time_info.second >= 0 && time_info.second <= 59, "Second should be 0-59");
     TEST_ASSERT(time_info.dow >= 0 && time_info.dow <= 6, "DOW should be 0-6");
     TEST_ASSERT(time_info.doy >= 1 && time_info.doy <= 366, "DOY should be 1-366");
     TEST_ASSERT(time_info.timestamp > 0.0, "Timestamp should be positive");
     
     printf("  Timestamp with fractional seconds: %.6f\n", time_info.timestamp);
     
     // Test NULL parameter
     err = cfl_timer_get_time_simple(NULL);
     TEST_ASSERT_EQ(err, CFL_TIMER_ERROR_INVALID_PARAM, "NULL parameter should return error");
     
     return true;
 }
 
 /**
  * Test 4: Get timestamp with fractional seconds
  */
 static bool test_get_timestamp(void)
 {
     setup_perm_allocator();
     
     double ts1, ts2;
     
     // Test with NULL handle (stateless)
     ts1 = cfl_timer_get_timestamp(NULL);
     TEST_ASSERT(ts1 > 0.0, "Timestamp should be positive");
     printf("  Timestamp 1: %.6f\n", ts1);
     
     // Small delay to ensure fractional difference
     struct timespec tiny_delay = {0, 10000000}; // 10ms
     nanosleep(&tiny_delay, NULL);
     
     // Test with valid handle
     cfl_timer_handle_t timer = cfl_timer_create(1.0, g_perm);
     ts2 = cfl_timer_get_timestamp(timer);
     TEST_ASSERT(ts2 > 0.0, "Timestamp should be positive");
     TEST_ASSERT(ts2 > ts1, "Second timestamp should be > first");
     printf("  Timestamp 2: %.6f (diff: %.6f)\n", ts2, ts2 - ts1);
     
     teardown_perm_allocator();
     return true;
 }
 
 /**
  * Test 5: Wait timer with change detection
  */
 static bool test_wait_timer(void)
 {
     setup_perm_allocator();
     
     cfl_timer_handle_t timer = cfl_timer_create(1.0, g_perm);
     cfl_tick_result_t result;
     
     printf("  [Waiting 0.1 seconds...]\n");
     cfl_timer_error_t err = cfl_timer_wait(timer, 0.1, &result);
     TEST_ASSERT_EQ(err, CFL_TIMER_SUCCESS, "Wait should succeed");
     
     // First call should mark all fields as changed
     TEST_ASSERT(result.changed_mask & CFL_CHANGED_SECOND, "Second should be marked changed");
     TEST_ASSERT(result.changed_mask & CFL_CHANGED_MINUTE, "Minute should be marked changed");
     TEST_ASSERT(result.changed_mask & CFL_CHANGED_HOUR, "Hour should be marked changed");
     
     // Validate time info
     TEST_ASSERT(result.all_values.year >= 2024, "Year should be reasonable");
     TEST_ASSERT(result.all_values.timestamp > 0.0, "Timestamp should be positive");
     
     printf("  Timestamp: %.6f\n", result.all_values.timestamp);
     
     // Test NULL result parameter
     err = cfl_timer_wait(timer, 0.1, NULL);
     TEST_ASSERT_EQ(err, CFL_TIMER_ERROR_INVALID_PARAM, "NULL result should return error");
     
     teardown_perm_allocator();
     return true;
 }
 
 /**
  * Test 6: Get current time with change detection
  */
 static bool test_get_current_time(void)
 {
     setup_perm_allocator();
     
     cfl_timer_handle_t timer = cfl_timer_create(1.0, g_perm);
     cfl_tick_result_t result1, result2;
     
     cfl_timer_error_t err = cfl_timer_get_current_time(timer, &result1);
     TEST_ASSERT_EQ(err, CFL_TIMER_SUCCESS, "Get current time should succeed");
     
     // First call marks everything as changed
     TEST_ASSERT(result1.changed_mask != 0, "First call should have changes");
     
     // Immediate second call should show no changes (timestamp always updates but not in mask)
     err = cfl_timer_get_current_time(timer, &result2);
     TEST_ASSERT_EQ(err, CFL_TIMER_SUCCESS, "Second get current time should succeed");
     
     // Should show minimal or no changes in calendar fields
     TEST_ASSERT(
         result2.changed_mask == 0 || 
         result2.changed_mask == CFL_CHANGED_SECOND,
         "Immediate second call should show minimal or no changes"
     );
     
     // But timestamp should always be different (or at least not go backwards)
     TEST_ASSERT(result2.all_values.timestamp >= result1.all_values.timestamp,
                "Timestamp should not go backwards");
     
     printf("  Timestamp 1: %.6f\n", result1.all_values.timestamp);
     printf("  Timestamp 2: %.6f\n", result2.all_values.timestamp);
     
     teardown_perm_allocator();
     return true;
 }
 
 /**
  * Test 7: Timer tick (main function)
  */
 static bool test_timer_tick(void)
 {
     setup_perm_allocator();
     
     cfl_timer_handle_t timer = cfl_timer_create(0.1, g_perm);
     cfl_tick_result_t result;
     
     printf("  [Performing timer tick (0.1s wait)...]\n");
     cfl_timer_error_t err = cfl_timer_tick(timer, &result);
     TEST_ASSERT_EQ(err, CFL_TIMER_SUCCESS, "Timer tick should succeed");
     
     // First tick marks everything as changed
     TEST_ASSERT(result.changed_mask != 0, "First tick should have changes");
     
     printf("  Timestamp: %.6f\n", result.all_values.timestamp);
     
     // Test NULL handle
     err = cfl_timer_tick(NULL, &result);
     TEST_ASSERT_EQ(err, CFL_TIMER_ERROR_INVALID_HANDLE, "NULL handle should return error");
     
     teardown_perm_allocator();
     return true;
 }
 
 /**
  * Test 8: Change detection across calls
  */
 static bool test_change_detection(void)
 {
     setup_perm_allocator();
     
     cfl_timer_handle_t timer = cfl_timer_create(0.5, g_perm);
     cfl_tick_result_t result;
     
     // First call - everything changed
     printf("  [First tick...]\n");
     cfl_timer_get_current_time(timer, &result);
     uint32_t first_mask = result.changed_mask;
     double first_timestamp = result.all_values.timestamp;
     TEST_ASSERT(first_mask != 0, "First call should have changes");
     printf("  First timestamp: %.6f\n", first_timestamp);
     
     // Immediate second call - should be same or minimal change
     cfl_timer_get_current_time(timer, &result);
     printf("  [Change mask: 0x%08X]\n", result.changed_mask);
     printf("  Second timestamp: %.6f\n", result.all_values.timestamp);
     
     // Wait and check again - second should definitely change
     printf("  [Waiting 1.1 seconds...]\n");
     sleep(1);
     cfl_timer_get_current_time(timer, &result);
     TEST_ASSERT(result.changed_mask & CFL_CHANGED_SECOND, "After 1s wait, second should change");
     TEST_ASSERT(result.all_values.timestamp > first_timestamp + 1.0, 
                "After 1s wait, timestamp should have advanced");
     printf("  Third timestamp: %.6f (delta: %.6f)\n", 
            result.all_values.timestamp, 
            result.all_values.timestamp - first_timestamp);
     
     teardown_perm_allocator();
     return true;
 }
 
 /**
  * Test 9: Tick data (custom fields)
  */
 static bool test_tick_data(void)
 {
     setup_perm_allocator();
     
     cfl_timer_handle_t timer = cfl_timer_create(1.0, g_perm);
     int64_t value;
     
     // Check that time_tick was auto-created
     cfl_timer_error_t err = cfl_timer_get_tick_data(timer, "time_tick", &value);
     TEST_ASSERT_EQ(err, CFL_TIMER_SUCCESS, "time_tick should exist");
     TEST_ASSERT_EQ(value, 1000, "time_tick should be 1000ms");
     
     // Add custom field
     err = cfl_timer_add_tick_data(timer, "custom_counter", 42, g_perm);
     TEST_ASSERT_EQ(err, CFL_TIMER_SUCCESS, "Add tick data should succeed");
     
     // Retrieve custom field
     err = cfl_timer_get_tick_data(timer, "custom_counter", &value);
     TEST_ASSERT_EQ(err, CFL_TIMER_SUCCESS, "Get tick data should succeed");
     TEST_ASSERT_EQ(value, 42, "Custom value should match");
     
     // Update existing field
     err = cfl_timer_add_tick_data(timer, "custom_counter", 99, g_perm);
     TEST_ASSERT_EQ(err, CFL_TIMER_SUCCESS, "Update tick data should succeed");
     
     err = cfl_timer_get_tick_data(timer, "custom_counter", &value);
     TEST_ASSERT_EQ(err, CFL_TIMER_SUCCESS, "Get updated tick data should succeed");
     TEST_ASSERT_EQ(value, 99, "Updated value should match");
     
     // Try to get non-existent field
     err = cfl_timer_get_tick_data(timer, "nonexistent", &value);
     TEST_ASSERT_EQ(err, CFL_TIMER_ERROR_NOT_FOUND, "Non-existent field should return NOT_FOUND");
     
     // Test NULL parameters
     err = cfl_timer_add_tick_data(NULL, "test", 1, g_perm);
     TEST_ASSERT_EQ(err, CFL_TIMER_ERROR_INVALID_PARAM, "NULL handle should return error");
     
     err = cfl_timer_add_tick_data(timer, NULL, 1, g_perm);
     TEST_ASSERT_EQ(err, CFL_TIMER_ERROR_INVALID_PARAM, "NULL field name should return error");
     
     err = cfl_timer_get_tick_data(NULL, "test", &value);
     TEST_ASSERT_EQ(err, CFL_TIMER_ERROR_INVALID_PARAM, "NULL handle should return error");
     
     teardown_perm_allocator();
     return true;
 }
 
 /**
  * Test 10: Multiple timers (independence)
  */
 static bool test_multiple_timers(void)
 {
     setup_perm_allocator();
     
     cfl_timer_handle_t timer1 = cfl_timer_create(1.0, g_perm);
     cfl_timer_handle_t timer2 = cfl_timer_create(2.0, g_perm);
     
     TEST_ASSERT(timer1 != NULL, "Timer 1 creation should succeed");
     TEST_ASSERT(timer2 != NULL, "Timer 2 creation should succeed");
     TEST_ASSERT(timer1 != timer2, "Timers should be independent");
     
     // Set different wait times
     cfl_timer_set_wait(timer1, 0.5);
     cfl_timer_set_wait(timer2, 1.5);
     
     double wait1 = cfl_timer_get_wait(timer1);
     double wait2 = cfl_timer_get_wait(timer2);
     
     TEST_ASSERT_NEAR(wait1, 0.5, 0.001, "Timer 1 wait should be 0.5");
     TEST_ASSERT_NEAR(wait2, 1.5, 0.001, "Timer 2 wait should be 1.5");
     
     // Add different tick data
     cfl_timer_add_tick_data(timer1, "id", 1, g_perm);
     cfl_timer_add_tick_data(timer2, "id", 2, g_perm);
     
     int64_t value1, value2;
     cfl_timer_get_tick_data(timer1, "id", &value1);
     cfl_timer_get_tick_data(timer2, "id", &value2);
     
     TEST_ASSERT_EQ(value1, 1, "Timer 1 should have its own data");
     TEST_ASSERT_EQ(value2, 2, "Timer 2 should have its own data");
     
     // Each timer should track changes independently
     cfl_tick_result_t result1, result2;
     cfl_timer_get_current_time(timer1, &result1);
     cfl_timer_get_current_time(timer2, &result2);
     
     TEST_ASSERT(result1.changed_mask != 0, "Timer 1 first call should show changes");
     TEST_ASSERT(result2.changed_mask != 0, "Timer 2 first call should show changes");
     
     teardown_perm_allocator();
     return true;
 }
 
 /**
  * Test 11: Stateless operations (NULL handle)
  */
 static bool test_stateless_operations(void)
 {
     cfl_tick_result_t result;
     
     // Wait with NULL handle (no change tracking)
     printf("  [Stateless wait (0.1s)...]\n");
     cfl_timer_error_t err = cfl_timer_wait(NULL, 0.1, &result);
     TEST_ASSERT_EQ(err, CFL_TIMER_SUCCESS, "Stateless wait should succeed");
     TEST_ASSERT_EQ(result.changed_mask, 0, "Stateless mode should have no change mask");
     TEST_ASSERT(result.all_values.timestamp > 0.0, "Should still get valid time info");
     
     printf("  Timestamp: %.6f\n", result.all_values.timestamp);
     
     // Get current time with NULL handle
     err = cfl_timer_get_current_time(NULL, &result);
     TEST_ASSERT_EQ(err, CFL_TIMER_SUCCESS, "Stateless get time should succeed");
     TEST_ASSERT_EQ(result.changed_mask, 0, "Stateless mode should have no change mask");
     
     return true;
 }
 
 /**
  * Test 12: Format time info
  */
 static bool test_format_time(void)
 {
     cfl_time_info_t time_info;
     char buffer[128];
     
     cfl_timer_get_time_simple(&time_info);
     
     int written = cfl_timer_format_time(&time_info, buffer, sizeof(buffer));
     TEST_ASSERT(written > 0, "Format should succeed");
     TEST_ASSERT(strlen(buffer) > 0, "Buffer should contain formatted string");
     
     printf("  Formatted time: %s\n", buffer);
     printf("  Timestamp: %.6f\n", time_info.timestamp);
     
     // Check format contains expected elements
     TEST_ASSERT(strstr(buffer, "UTC") != NULL, "Should contain UTC");
     
     // Test NULL parameters
     written = cfl_timer_format_time(NULL, buffer, sizeof(buffer));
     TEST_ASSERT_EQ(written, -1, "NULL time_info should return -1");
     
     written = cfl_timer_format_time(&time_info, NULL, sizeof(buffer));
     TEST_ASSERT_EQ(written, -1, "NULL buffer should return -1");
     
     written = cfl_timer_format_time(&time_info, buffer, 0);
     TEST_ASSERT_EQ(written, -1, "Zero buffer size should return -1");
     
     return true;
 }
 
 /**
  * Test 13: Format tick result
  */
 static bool test_format_tick_result(void)
 {
     setup_perm_allocator();
     
     cfl_timer_handle_t timer = cfl_timer_create(0.1, g_perm);
     cfl_tick_result_t result;
     char buffer[1024];
     
     cfl_timer_get_current_time(timer, &result);
     
     int written = cfl_timer_format_tick_result(&result, buffer, sizeof(buffer));
     TEST_ASSERT(written > 0, "Format tick result should succeed");
     TEST_ASSERT(strlen(buffer) > 0, "Buffer should contain formatted string");
     
     printf("  Formatted result:\n%s\n", buffer);
     
     // Test NULL parameters
     written = cfl_timer_format_tick_result(NULL, buffer, sizeof(buffer));
     TEST_ASSERT_EQ(written, -1, "NULL result should return -1");
     
     teardown_perm_allocator();
     return true;
 }
 
 /**
  * Test 14: Print functions (visual test)
  */
 static bool test_print_functions(void)
 {
     setup_perm_allocator();
     
     cfl_timer_handle_t timer = cfl_timer_create(1.0, g_perm);
     cfl_tick_result_t result;
     cfl_time_info_t time_info;
     
     printf("\n  --- Print Time Info Test ---\n");
     cfl_timer_get_time_simple(&time_info);
     cfl_timer_print_time_info(&time_info);
     
     printf("\n  --- Print Tick Result Test ---\n");
     cfl_timer_get_current_time(timer, &result);
     cfl_timer_print_tick_result(&result);
     
     // Test NULL safety
     cfl_timer_print_time_info(NULL);  // Should not crash
     cfl_timer_print_tick_result(NULL);  // Should not crash
     
     teardown_perm_allocator();
     return true;
 }
 
 /**
  * Test 15: Error strings
  */
 static bool test_error_strings(void)
 {
     const char* str;
     
     str = cfl_timer_error_string(CFL_TIMER_SUCCESS);
     TEST_ASSERT(str != NULL, "Error string should not be NULL");
     TEST_ASSERT(strlen(str) > 0, "Error string should not be empty");
     
     str = cfl_timer_error_string(CFL_TIMER_ERROR_INVALID_HANDLE);
     TEST_ASSERT(str != NULL, "Error string should not be NULL");
     
     str = cfl_timer_error_string(CFL_TIMER_ERROR_INVALID_PARAM);
     TEST_ASSERT(str != NULL, "Error string should not be NULL");
     
     str = cfl_timer_error_string(CFL_TIMER_ERROR_ALLOCATION);
     TEST_ASSERT(str != NULL, "Error string should not be NULL");
     
     str = cfl_timer_error_string(CFL_TIMER_ERROR_SYSTEM);
     TEST_ASSERT(str != NULL, "Error string should not be NULL");
     
     str = cfl_timer_error_string(CFL_TIMER_ERROR_NOT_FOUND);
     TEST_ASSERT(str != NULL, "Error string should not be NULL");
     
     str = cfl_timer_error_string((cfl_timer_error_t)-999);
     TEST_ASSERT(str != NULL, "Unknown error should return string");
     
     printf("  Sample error string: '%s'\n", str);
     
     return true;
 }
 
 /**
  * Test 16: Field changed macro
  */
 static bool test_field_changed_macro(void)
 {
     setup_perm_allocator();
     
     cfl_timer_handle_t timer = cfl_timer_create(0.1, g_perm);
     cfl_tick_result_t result;
     
     cfl_timer_get_current_time(timer, &result);
     
     // First call should mark fields as changed
     if (CFL_FIELD_CHANGED(&result, SECOND)) {
         printf("  Second changed (as expected)\n");
     }
     
     if (CFL_FIELD_CHANGED(&result, MINUTE)) {
         printf("  Minute changed (as expected)\n");
     }
     
     // Note: No TIMESTAMP flag anymore since it always changes
     
     teardown_perm_allocator();
     return true;
 }
 
 /**
  * Test 17: Wait time accuracy
  */
 static bool test_wait_accuracy(void)
 {
     setup_perm_allocator();
     
     cfl_timer_handle_t timer = cfl_timer_create(1.0, g_perm);
     cfl_tick_result_t result;
     struct timespec start, end;
     double elapsed;
     
     printf("  [Testing 0.5 second wait accuracy...]\n");
     
     clock_gettime(CLOCK_REALTIME, &start);
     cfl_timer_wait(timer, 0.5, &result);
     clock_gettime(CLOCK_REALTIME, &end);
     
     elapsed = (end.tv_sec - start.tv_sec) + 
               (end.tv_nsec - start.tv_nsec) / 1e9;
     
     printf("  Requested: 0.500s, Actual: %.6fs\n", elapsed);
     printf("  Result timestamp: %.6f\n", result.all_values.timestamp);
     
     // Allow 50ms tolerance
     TEST_ASSERT_NEAR(elapsed, 0.5, 0.05, "Wait time should be accurate within 50ms");
     
     teardown_perm_allocator();
     return true;
 }
 
 /**
  * Test 18: Shared change tracking state
  */
 static bool test_shared_change_tracking(void)
 {
     setup_perm_allocator();
     
     cfl_timer_handle_t timer = cfl_timer_create(1.0, g_perm);
     cfl_tick_result_t result;
     
     printf("  [Testing shared change tracking across functions...]\n");
     
     // First call to get_current_time - everything changes
     cfl_timer_get_current_time(timer, &result);
     TEST_ASSERT(result.changed_mask != 0, "First call should show changes");
     double ts1 = result.all_values.timestamp;
     
     // Immediate call to wait - should show minimal/no changes
     cfl_timer_wait(timer, 0.05, &result);
     printf("  Change mask after immediate wait: 0x%08X\n", result.changed_mask);
     double ts2 = result.all_values.timestamp;
     printf("  Timestamp advanced by: %.6f\n", ts2 - ts1);
     
     // Wait longer and call timer_tick - should show changes
     printf("  [Waiting 1 second...]\n");
     sleep(1);
     cfl_timer_tick(timer, &result);
     TEST_ASSERT(result.changed_mask & CFL_CHANGED_SECOND, 
                 "After 1s, second should change");
     printf("  Timestamp after 1s: %.6f (delta: %.6f)\n", 
            result.all_values.timestamp, result.all_values.timestamp - ts2);
     
     teardown_perm_allocator();
     return true;
 }
 
 /**
  * Test 19: Perm allocator usage
  */
 static bool test_perm_allocator_usage(void)
 {
     setup_perm_allocator();
     
     uint16_t used_before = cfl_perm_used_bytes(g_perm);
     
     // Create timer
     cfl_timer_handle_t timer = cfl_timer_create(1.0, g_perm);
     TEST_ASSERT(timer != NULL, "Timer creation should succeed");
     
     uint16_t used_after_timer = cfl_perm_used_bytes(g_perm);
     TEST_ASSERT(used_after_timer > used_before, "Perm allocator should have used memory");
     
     // Add tick data
     cfl_timer_add_tick_data(timer, "test_field", 123, g_perm);
     
     uint16_t used_after_data = cfl_perm_used_bytes(g_perm);
     TEST_ASSERT(used_after_data > used_after_timer, "Adding tick data should use more memory");
     
     printf("  Memory used: %u bytes\n", used_after_data);
     
     // Reset and verify
     cfl_perm_reset(g_perm);
     uint16_t used_after_reset = cfl_perm_used_bytes(g_perm);
     TEST_ASSERT_EQ(used_after_reset, 0, "After reset, used bytes should be 0");
     
     teardown_perm_allocator();
     return true;
 }
 
 /**
  * Test 20: Multiple tick data entries
  */
 static bool test_multiple_tick_data(void)
 {
     setup_perm_allocator();
     
     cfl_timer_handle_t timer = cfl_timer_create(1.0, g_perm);
     
     // Add multiple entries
     cfl_timer_add_tick_data(timer, "field1", 100, g_perm);
     cfl_timer_add_tick_data(timer, "field2", 200, g_perm);
     cfl_timer_add_tick_data(timer, "field3", 300, g_perm);
     
     // Retrieve and verify
     int64_t val1, val2, val3;
     TEST_ASSERT_EQ(cfl_timer_get_tick_data(timer, "field1", &val1), CFL_TIMER_SUCCESS, 
                    "field1 should exist");
     TEST_ASSERT_EQ(cfl_timer_get_tick_data(timer, "field2", &val2), CFL_TIMER_SUCCESS,
                    "field2 should exist");
     TEST_ASSERT_EQ(cfl_timer_get_tick_data(timer, "field3", &val3), CFL_TIMER_SUCCESS,
                    "field3 should exist");
     
     TEST_ASSERT_EQ(val1, 100, "field1 value should match");
     TEST_ASSERT_EQ(val2, 200, "field2 value should match");
     TEST_ASSERT_EQ(val3, 300, "field3 value should match");
     
     teardown_perm_allocator();
     return true;
 }
 
 /**
  * Test 21: Fractional second precision
  */
 static bool test_fractional_precision(void)
 {
     cfl_time_info_t time1, time2;
     
     printf("  [Testing fractional second precision...]\n");
     
     // Get two timestamps very close together
     cfl_timer_get_time_simple(&time1);
     cfl_timer_get_time_simple(&time2);
     
     printf("  Time 1: %.9f\n", time1.timestamp);
     printf("  Time 2: %.9f\n", time2.timestamp);
     printf("  Difference: %.9f seconds\n", time2.timestamp - time1.timestamp);
     
     // They should either be the same or time2 >= time1
     TEST_ASSERT(time2.timestamp >= time1.timestamp, 
                "Time should not go backwards");
     
     // Now with a deliberate small delay
     struct timespec delay = {0, 1000000}; // 1ms
     cfl_timer_get_time_simple(&time1);
     nanosleep(&delay, NULL);
     cfl_timer_get_time_simple(&time2);
     
     printf("  After 1ms delay:\n");
     printf("  Time 1: %.9f\n", time1.timestamp);
     printf("  Time 2: %.9f\n", time2.timestamp);
     printf("  Difference: %.9f seconds (%.3f ms)\n", 
            time2.timestamp - time1.timestamp,
            (time2.timestamp - time1.timestamp) * 1000.0);
     
     TEST_ASSERT(time2.timestamp > time1.timestamp, 
                "After delay, time2 should be greater");
     
     return true;
 }
 
 /*==============================================================================
  * Main Test Runner
  *============================================================================*/
 
 int main(void)
 {
     printf("\n");
     printf("╔══════════════════════════════════════════════════════════════╗\n");
     printf("║         CFL Timer Unit Tests                                 ║\n");
     printf("║         (Double Timestamp with Fractional Seconds)           ║\n");
     printf("╚══════════════════════════════════════════════════════════════╝\n");
     
     // Run all tests
     RUN_TEST(test_timer_creation);
     RUN_TEST(test_wait_time_configuration);
     RUN_TEST(test_get_time_simple);
     RUN_TEST(test_get_timestamp);
     RUN_TEST(test_wait_timer);
     RUN_TEST(test_get_current_time);
     RUN_TEST(test_timer_tick);
     RUN_TEST(test_change_detection);
     RUN_TEST(test_tick_data);
     RUN_TEST(test_multiple_timers);
     RUN_TEST(test_stateless_operations);
     RUN_TEST(test_format_time);
     RUN_TEST(test_format_tick_result);
     RUN_TEST(test_print_functions);
     RUN_TEST(test_error_strings);
     RUN_TEST(test_field_changed_macro);
     RUN_TEST(test_wait_accuracy);
     RUN_TEST(test_shared_change_tracking);
     RUN_TEST(test_perm_allocator_usage);
     RUN_TEST(test_multiple_tick_data);
     RUN_TEST(test_fractional_precision);
     
     // Print summary
     printf("\n");
     printf("╔══════════════════════════════════════════════════════════════╗\n");
     printf("║                     Test Summary                             ║\n");
     printf("╠══════════════════════════════════════════════════════════════╣\n");
     printf("║  Total Tests:  %-3d                                          ║\n", tests_run);
     printf("║  Passed:       %-3d                                          ║\n", tests_passed);
     printf("║  Failed:       %-3d                                          ║\n", tests_failed);
     printf("╚══════════════════════════════════════════════════════════════╝\n");
     
     if (tests_failed == 0) {
         printf("\n🎉 All tests PASSED! 🎉\n\n");
         return 0;
     } else {
         printf("\n❌ Some tests FAILED ❌\n\n");
         return 1;
     }
 }