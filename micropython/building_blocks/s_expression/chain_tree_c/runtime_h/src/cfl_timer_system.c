/**
 * @file cfl_timer_system.c
 * @brief Handle-based Calendar Timer Service Implementation
 * 
 * Debian Linux implementation matching Python CFL_Timer class
 * Uses cfl_perm permanent allocator for memory management
 */

 #include "cfl_timer_system.h"
 #include "cfl_exception.h"
 #include <stdlib.h>
 #include <string.h>
 #include <stdio.h>
 #include <time.h>
 #include <errno.h>
 #include <unistd.h>
 
 /* ========================================================================
  * INTERNAL STRUCTURES
  * ======================================================================== */
 
 /**
  * @brief Tick dictionary entry for custom data
  */
 typedef struct tick_data_entry {
     char* field_name;
     int64_t value;
     struct tick_data_entry* next;
 } tick_data_entry_t;
 
 /**
  * @brief Internal timer context structure
  * 
  * Matches Python CFL_Timer class members
  */
 struct cfl_timer_context {
     double wait_seconds;                /* self.wait_seconds */
     cfl_time_info_t last_time_info;     /* self._last_time_info */
     bool has_previous;                  /* Track if _last_time_info is valid */
     tick_data_entry_t* tick_dict_head;  /* self.tick_dict (linked list) */
 };
 
 
 /* ========================================================================
  * INTERNAL HELPER FUNCTIONS
  * ======================================================================== */
 
 /**
  * @brief Get current GMT time and populate time_info structure
  */
 static cfl_timer_error_t get_current_time_internal(cfl_time_info_t* time_info) {
     struct timespec ts;
     struct tm tm_result;
     time_t now;
     
     if (!time_info) {
         return CFL_TIMER_ERROR_INVALID_PARAM;
     }
     
     /* Get current time with high precision */
     if (clock_gettime(CLOCK_REALTIME, &ts) != 0) {
         return CFL_TIMER_ERROR_SYSTEM;
     }
     
     now = ts.tv_sec;
     
     /* Convert to GMT/UTC */
     if (gmtime_r(&now, &tm_result) == NULL) {
         return CFL_TIMER_ERROR_SYSTEM;
     }
     
     /* Populate structure matching Python datetime fields */
     time_info->year = tm_result.tm_year + 1900;
     time_info->month = tm_result.tm_mon + 1;  /* tm_mon is 0-11, we want 1-12 */
     time_info->day = tm_result.tm_mday;
     time_info->dow = (tm_result.tm_wday + 6) % 7;  /* Convert Sunday=0 to Monday=0 */
     time_info->doy = tm_result.tm_yday + 1;  /* tm_yday is 0-365, we want 1-366 */
     time_info->hour = tm_result.tm_hour;
     time_info->minute = tm_result.tm_min;
     time_info->second = tm_result.tm_sec;
     
     /* Store timestamp with fractional seconds */
     time_info->timestamp = (double)ts.tv_sec + (double)ts.tv_nsec / 1e9;
     
     return CFL_TIMER_SUCCESS;
 }
 
 /**
  * @brief Sleep for specified seconds (supports fractional seconds)
  * 
  * Matches Python: time.sleep(wait_seconds)
  */
 static cfl_timer_error_t sleep_seconds(double seconds) {
     struct timespec req, rem;
     
     if (seconds < 0.0) {
         return CFL_TIMER_ERROR_INVALID_PARAM;
     }
     
     /* Convert to timespec */
     req.tv_sec = (time_t)seconds;
     req.tv_nsec = (long)((seconds - req.tv_sec) * 1e9);
     
     /* Handle interruptions */
     while (nanosleep(&req, &rem) == -1) {
         if (errno == EINTR) {
             req = rem;  /* Continue with remaining time */
         } else {
             return CFL_TIMER_ERROR_SYSTEM;
         }
     }
     
     return CFL_TIMER_SUCCESS;
 }
 
 /**
  * @brief Compare two time_info structures and generate change mask
  * 
  * Matches Python comparison in timer_tick()
  * Note: Timestamp is NOT compared as it always changes
  */
 static uint32_t compute_change_mask(const cfl_time_info_t* old_info,
                                      const cfl_time_info_t* new_info) {
     uint32_t mask = 0;
     
     if (!old_info || !new_info) {
         return 0;
     }
     
     if (new_info->second != old_info->second) {
         mask |= CFL_CHANGED_SECOND;
     }
     if (new_info->minute != old_info->minute) {
         mask |= CFL_CHANGED_MINUTE;
     }
     if (new_info->hour != old_info->hour) {
         mask |= CFL_CHANGED_HOUR;
     }
     if (new_info->day != old_info->day) {
         mask |= CFL_CHANGED_DAY;
     }
     if (new_info->dow != old_info->dow) {
         mask |= CFL_CHANGED_DOW;
     }
     if (new_info->doy != old_info->doy) {
         mask |= CFL_CHANGED_DOY;
     }
     if (new_info->month != old_info->month) {
         mask |= CFL_CHANGED_MONTH;
     }
     if (new_info->year != old_info->year) {
         mask |= CFL_CHANGED_YEAR;
     }
     
     return mask;
 }
 
 /**
  * @brief Update internal state and compute change mask
  * 
  * Matches Python timer_tick() change detection logic
  */
 static uint32_t update_and_get_changes(cfl_timer_handle_t handle,
                                         const cfl_time_info_t* new_info) {
     uint32_t mask;
     
     if (!handle || !new_info) {
         return 0;
     }
     
     /* If this is the first call, mark everything as changed */
     if (!handle->has_previous) {
         mask = CFL_CHANGED_SECOND | CFL_CHANGED_MINUTE | CFL_CHANGED_HOUR |
                CFL_CHANGED_DAY | CFL_CHANGED_DOW | CFL_CHANGED_DOY |
                CFL_CHANGED_MONTH | CFL_CHANGED_YEAR;
         handle->has_previous = true;
     } else {
         /* Compare with last time info */
         mask = compute_change_mask(&handle->last_time_info, new_info);
     }
     
     /* Store current info for next comparison */
     handle->last_time_info = *new_info;
     
     return mask;
 }
 
 /**
  * @brief Find tick data entry by field name
  */
 static tick_data_entry_t* find_tick_data(cfl_timer_handle_t handle,
                                           const char* field_name) {
     tick_data_entry_t* entry;
     
     if (!handle || !field_name) {
         return NULL;
     }
     
     for (entry = handle->tick_dict_head; entry != NULL; entry = entry->next) {
         if (strcmp(entry->field_name, field_name) == 0) {
             return entry;
         }
     }
     
     return NULL;
 }
 
 
 /* ========================================================================
  * LIFECYCLE MANAGEMENT
  * ======================================================================== */
 
 cfl_timer_handle_t cfl_timer_create(double wait_seconds, cfl_perm_t* perm) {
     cfl_timer_handle_t handle;
     
     if (!perm) {
         EXCEPTION("cfl_timer_create: NULL perm pointer");
     }
     
     if (!perm->initialized) {
         EXCEPTION("cfl_timer_create: Perm allocator not initialized");
     }
     
     if (wait_seconds < 0.0) {
         EXCEPTION("cfl_timer_create: Negative wait_seconds");
     }
     
     /* Allocate timer handle from permanent allocator */
     handle = (cfl_timer_handle_t)cfl_perm_alloc_pointer(perm, sizeof(struct cfl_timer_context));
     if (!handle) {
         EXCEPTION("cfl_timer_create: Failed to allocate timer handle");
     }
     
     /* Initialize structure */
     memset(handle, 0, sizeof(struct cfl_timer_context));
     handle->wait_seconds = wait_seconds;
     handle->has_previous = false;
     handle->tick_dict_head = NULL;
     
     /* Initialize tick_dict with "time_tick" field (matches Python) */
     cfl_timer_add_tick_data(handle, "time_tick", (int64_t)(wait_seconds * 1000), perm);
     
     return handle;
 }
 
 
 /* ========================================================================
  * CONFIGURATION
  * ======================================================================== */
 
 cfl_timer_error_t cfl_timer_set_wait(cfl_timer_handle_t handle, double wait_seconds) {
     if (!handle) {
         return CFL_TIMER_ERROR_INVALID_HANDLE;
     }
     
     if (wait_seconds < 0.0) {
         return CFL_TIMER_ERROR_INVALID_PARAM;
     }
     
     handle->wait_seconds = wait_seconds;
     
     return CFL_TIMER_SUCCESS;
 }
 
 double cfl_timer_get_wait(cfl_timer_handle_t handle) {
     if (!handle) {
         return -1.0;
     }
     
     return handle->wait_seconds;
 }
 
 cfl_timer_error_t cfl_timer_add_tick_data(cfl_timer_handle_t handle,
                                            const char* field_name,
                                            int64_t value,
                                            cfl_perm_t* perm) {
     tick_data_entry_t* entry;
     uint16_t name_len;
     
     if (!handle || !field_name || !perm) {
         return CFL_TIMER_ERROR_INVALID_PARAM;
     }
     
     if (!perm->initialized) {
         return CFL_TIMER_ERROR_ALLOCATION;
     }
     
     /* Check if entry already exists */
     entry = find_tick_data(handle, field_name);
     if (entry) {
         /* Update existing entry */
         entry->value = value;
         return CFL_TIMER_SUCCESS;
     }
     
     /* Allocate new entry from permanent allocator */
     entry = (tick_data_entry_t*)cfl_perm_alloc_pointer(perm, sizeof(tick_data_entry_t));
     if (!entry) {
         return CFL_TIMER_ERROR_ALLOCATION;
     }
     
     /* Allocate and copy field name */
     name_len = (uint16_t)strlen(field_name) + 1;
     entry->field_name = (char*)cfl_perm_alloc_pointer(perm, name_len);
     if (!entry->field_name) {
         return CFL_TIMER_ERROR_ALLOCATION;
     }
     
     strcpy(entry->field_name, field_name);
     entry->value = value;
     entry->next = handle->tick_dict_head;
     handle->tick_dict_head = entry;
     
     return CFL_TIMER_SUCCESS;
 }
 
 cfl_timer_error_t cfl_timer_get_tick_data(cfl_timer_handle_t handle,
                                            const char* field_name,
                                            int64_t* value) {
     tick_data_entry_t* entry;
     
     if (!handle || !field_name || !value) {
         return CFL_TIMER_ERROR_INVALID_PARAM;
     }
     
     entry = find_tick_data(handle, field_name);
     if (!entry) {
         return CFL_TIMER_ERROR_NOT_FOUND;
     }
     
     *value = entry->value;
     return CFL_TIMER_SUCCESS;
 }
 
 
 /* ========================================================================
  * TIME QUERY FUNCTIONS
  * ======================================================================== */
 
 cfl_timer_error_t cfl_timer_wait(cfl_timer_handle_t handle,
                                   double wait_seconds,
                                   cfl_tick_result_t* result) {
     cfl_timer_error_t err;
     cfl_time_info_t time_info;
     
     if (!result) {
         return CFL_TIMER_ERROR_INVALID_PARAM;
     }
     
     /* Wait for specified duration */
     err = sleep_seconds(wait_seconds);
     if (err != CFL_TIMER_SUCCESS) {
         return err;
     }
     
     /* Get current time */
     err = get_current_time_internal(&time_info);
     if (err != CFL_TIMER_SUCCESS) {
         return err;
     }
     
     result->all_values = time_info;
     
     /* Compute change mask if we have a handle */
     if (handle) {
         result->changed_mask = update_and_get_changes(handle, &time_info);
     } else {
         /* Stateless mode - no change tracking */
         result->changed_mask = 0;
     }
     
     return CFL_TIMER_SUCCESS;
 }
 
 cfl_timer_error_t cfl_timer_get_current_time(cfl_timer_handle_t handle,
                                               cfl_tick_result_t* result) {
     cfl_timer_error_t err;
     cfl_time_info_t time_info;
     
     if (!result) {
         return CFL_TIMER_ERROR_INVALID_PARAM;
     }
     
     /* Get current time */
     err = get_current_time_internal(&time_info);
     if (err != CFL_TIMER_SUCCESS) {
         return err;
     }
     
     result->all_values = time_info;
     
     /* Compute change mask if we have a handle */
     if (handle) {
         result->changed_mask = update_and_get_changes(handle, &time_info);
     } else {
         /* Stateless mode - no change tracking */
         result->changed_mask = 0;
     }
     
     return CFL_TIMER_SUCCESS;
 }
 
 double cfl_timer_get_timestamp(cfl_timer_handle_t handle) {
     struct timespec ts;
     
     (void)handle;  /* Unused parameter */
     
     if (clock_gettime(CLOCK_REALTIME, &ts) != 0) {
         return -1.0;
     }
     
     return (double)ts.tv_sec + (double)ts.tv_nsec / 1e9;
 }
 
 cfl_timer_error_t cfl_timer_get_time_simple(cfl_time_info_t* time_info) {
     return get_current_time_internal(time_info);
 }
 
 cfl_timer_error_t cfl_timer_tick(cfl_timer_handle_t handle,
                                   cfl_tick_result_t* result) {
     if (!handle) {
         return CFL_TIMER_ERROR_INVALID_HANDLE;
     }
     
     /* Use configured wait_seconds */
     return cfl_timer_wait(handle, handle->wait_seconds, result);
 }
 
 
 /* ========================================================================
  * FORMATTING AND DISPLAY
  * ======================================================================== */
 
 int cfl_timer_format_time(const cfl_time_info_t* time_info,
                           char* buffer,
                           size_t buffer_size) {
     if (!time_info || !buffer || buffer_size == 0) {
         return -1;
     }
     
     /* Format directly from time_info fields */
     return snprintf(buffer, buffer_size, "%04d-%02d-%02d %02d:%02d:%02d UTC",
                     time_info->year,
                     time_info->month,
                     time_info->day,
                     time_info->hour,
                     time_info->minute,
                     time_info->second);
 }
 
 int cfl_timer_format_tick_result(const cfl_tick_result_t* result,
                                   char* buffer,
                                   size_t buffer_size) {
     char time_str[64];
     size_t offset = 0;
     int written;
     
     if (!result || !buffer || buffer_size == 0) {
         return -1;
     }
     
     /* Format time */
     cfl_timer_format_time(&result->all_values, time_str, sizeof(time_str));
     
     written = snprintf(buffer + offset, buffer_size - offset,
                       "Time: %s (%.6f)\n", time_str, result->all_values.timestamp);
     if (written < 0 || (size_t)written >= buffer_size - offset) {
         return -1;
     }
     offset += written;
     
     /* Show changed fields */
     if (result->changed_mask == 0) {
         written = snprintf(buffer + offset, buffer_size - offset,
                           "No changes detected\n");
         if (written < 0 || (size_t)written >= buffer_size - offset) {
             return -1;
         }
         offset += written;
     } else {
         written = snprintf(buffer + offset, buffer_size - offset,
                           "Changed values:\n");
         if (written < 0 || (size_t)written >= buffer_size - offset) {
             return -1;
         }
         offset += written;
         
         if (result->changed_mask & CFL_CHANGED_SECOND) {
             written = snprintf(buffer + offset, buffer_size - offset,
                               "  second: %d\n", result->all_values.second);
             if (written < 0 || (size_t)written >= buffer_size - offset) {
                 return -1;
             }
             offset += written;
         }
         
         if (result->changed_mask & CFL_CHANGED_MINUTE) {
             written = snprintf(buffer + offset, buffer_size - offset,
                               "  minute: %d\n", result->all_values.minute);
             if (written < 0 || (size_t)written >= buffer_size - offset) {
                 return -1;
             }
             offset += written;
         }
         
         if (result->changed_mask & CFL_CHANGED_HOUR) {
             written = snprintf(buffer + offset, buffer_size - offset,
                               "  hour: %d\n", result->all_values.hour);
             if (written < 0 || (size_t)written >= buffer_size - offset) {
                 return -1;
             }
             offset += written;
         }
         
         if (result->changed_mask & CFL_CHANGED_DAY) {
             written = snprintf(buffer + offset, buffer_size - offset,
                               "  day: %d\n", result->all_values.day);
             if (written < 0 || (size_t)written >= buffer_size - offset) {
                 return -1;
             }
             offset += written;
         }
         
         if (result->changed_mask & CFL_CHANGED_DOW) {
             written = snprintf(buffer + offset, buffer_size - offset,
                               "  dow: %d (0=Mon, 6=Sun)\n", result->all_values.dow);
             if (written < 0 || (size_t)written >= buffer_size - offset) {
                 return -1;
             }
             offset += written;
         }
         
         if (result->changed_mask & CFL_CHANGED_DOY) {
             written = snprintf(buffer + offset, buffer_size - offset,
                               "  doy: %d\n", result->all_values.doy);
             if (written < 0 || (size_t)written >= buffer_size - offset) {
                 return -1;
             }
             offset += written;
         }
         
         if (result->changed_mask & CFL_CHANGED_MONTH) {
             written = snprintf(buffer + offset, buffer_size - offset,
                               "  month: %d\n", result->all_values.month);
             if (written < 0 || (size_t)written >= buffer_size - offset) {
                 return -1;
             }
             offset += written;
         }
         
         if (result->changed_mask & CFL_CHANGED_YEAR) {
             written = snprintf(buffer + offset, buffer_size - offset,
                               "  year: %d\n", result->all_values.year);
             if (written < 0 || (size_t)written >= buffer_size - offset) {
                 return -1;
             }
             offset += written;
         }
     }
     
     return (int)offset;
 }
 
 void cfl_timer_print_time_info(const cfl_time_info_t* time_info) {
     char formatted[64];
     
     if (!time_info) {
         return;
     }
     
     printf("Year: %d\n", time_info->year);
     printf("Month: %d\n", time_info->month);
     printf("Day: %d\n", time_info->day);
     printf("Day of Week: %d (0=Mon, 6=Sun)\n", time_info->dow);
     printf("Day of Year: %d\n", time_info->doy);
     printf("Hour: %d\n", time_info->hour);
     printf("Minute: %d\n", time_info->minute);
     printf("Second: %d\n", time_info->second);
     printf("Unix Timestamp: %.6f\n", time_info->timestamp);
     
     cfl_timer_format_time(time_info, formatted, sizeof(formatted));
     printf("Formatted GMT: %s\n", formatted);
 }
 
 void cfl_timer_print_tick_result(const cfl_tick_result_t* result) {
     char buffer[1024];
     
     if (!result) {
         return;
     }
     
     cfl_timer_format_tick_result(result, buffer, sizeof(buffer));
     printf("%s", buffer);
 }
 
 
 /* ========================================================================
  * ERROR HANDLING
  * ======================================================================== */
 
 const char* cfl_timer_error_string(cfl_timer_error_t error) {
     switch (error) {
         case CFL_TIMER_SUCCESS:
             return "Success";
         case CFL_TIMER_ERROR_INVALID_HANDLE:
             return "Invalid handle";
         case CFL_TIMER_ERROR_INVALID_PARAM:
             return "Invalid parameter";
         case CFL_TIMER_ERROR_ALLOCATION:
             return "Memory allocation failed";
         case CFL_TIMER_ERROR_SYSTEM:
             return "System call failed";
         case CFL_TIMER_ERROR_NOT_FOUND:
             return "Item not found";
         default:
             return "Unknown error";
     }
 }