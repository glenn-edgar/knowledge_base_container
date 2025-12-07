/**
 * @file ct_timer.c
 * @brief Handle-based Calendar Timer Service Implementation
 * 
 * Debian Linux implementation matching Python CT_Timer class
 */

#include "ct_timer.h"
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
 * Matches Python CT_Timer class members
 */
struct ct_timer_context {
    double wait_seconds;                /* self.wait_seconds */
    ct_time_info_t last_time_info;      /* self._last_time_info */
    bool has_previous;                  /* Track if _last_time_info is valid */
    tick_data_entry_t* tick_dict_head;  /* self.tick_dict (linked list) */
};


/* ========================================================================
 * INTERNAL HELPER FUNCTIONS
 * ======================================================================== */

/**
 * @brief Get current GMT time and populate time_info structure
 */
static ct_error_t get_current_time_internal(ct_time_info_t* time_info) {
    struct timespec ts;
    struct tm tm_result;
    time_t now;
    
    if (!time_info) {
        return CT_ERROR_INVALID_PARAM;
    }
    
    /* Get current time with high precision */
    if (clock_gettime(CLOCK_REALTIME, &ts) != 0) {
        return CT_ERROR_SYSTEM;
    }
    
    now = ts.tv_sec;
    
    /* Convert to GMT/UTC */
    if (gmtime_r(&now, &tm_result) == NULL) {
        return CT_ERROR_SYSTEM;
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
    time_info->timestamp = (int64_t)now;
    
    return CT_SUCCESS;
}

/**
 * @brief Sleep for specified seconds (supports fractional seconds)
 * 
 * Matches Python: time.sleep(wait_seconds)
 */
static ct_error_t sleep_seconds(double seconds) {
    struct timespec req, rem;
    
    if (seconds < 0.0) {
        return CT_ERROR_INVALID_PARAM;
    }
    
    /* Convert to timespec */
    req.tv_sec = (time_t)seconds;
    req.tv_nsec = (long)((seconds - req.tv_sec) * 1e9);
    
    /* Handle interruptions */
    while (nanosleep(&req, &rem) == -1) {
        if (errno == EINTR) {
            req = rem;  /* Continue with remaining time */
        } else {
            return CT_ERROR_SYSTEM;
        }
    }
    
    return CT_SUCCESS;
}

/**
 * @brief Compare two time_info structures and generate change mask
 * 
 * Matches Python comparison in timer_tick()
 */
static uint32_t compute_change_mask(const ct_time_info_t* old_info,
                                     const ct_time_info_t* new_info) {
    uint32_t mask = 0;
    
    if (!old_info || !new_info) {
        return 0;
    }
    
    if (new_info->second != old_info->second) {
        mask |= CT_CHANGED_SECOND;
    }
    if (new_info->minute != old_info->minute) {
        mask |= CT_CHANGED_MINUTE;
    }
    if (new_info->hour != old_info->hour) {
        mask |= CT_CHANGED_HOUR;
    }
    if (new_info->day != old_info->day) {
        mask |= CT_CHANGED_DAY;
    }
    if (new_info->dow != old_info->dow) {
        mask |= CT_CHANGED_DOW;
    }
    if (new_info->doy != old_info->doy) {
        mask |= CT_CHANGED_DOY;
    }
    if (new_info->month != old_info->month) {
        mask |= CT_CHANGED_MONTH;
    }
    if (new_info->year != old_info->year) {
        mask |= CT_CHANGED_YEAR;
    }
    if (new_info->timestamp != old_info->timestamp) {
        mask |= CT_CHANGED_TIMESTAMP;
    }
    
    return mask;
}

/**
 * @brief Update internal state and compute change mask
 * 
 * Matches Python timer_tick() change detection logic
 */
static uint32_t update_and_get_changes(ct_timer_handle_t handle,
                                        const ct_time_info_t* new_info) {
    uint32_t mask;
    
    if (!handle || !new_info) {
        return 0;
    }
    
    /* If this is the first call, mark everything as changed */
    if (!handle->has_previous) {
        mask = CT_CHANGED_SECOND | CT_CHANGED_MINUTE | CT_CHANGED_HOUR |
               CT_CHANGED_DAY | CT_CHANGED_DOW | CT_CHANGED_DOY |
               CT_CHANGED_MONTH | CT_CHANGED_YEAR | CT_CHANGED_TIMESTAMP;
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
static tick_data_entry_t* find_tick_data(ct_timer_handle_t handle,
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

/**
 * @brief Free all tick data entries
 */
static void free_tick_data(ct_timer_handle_t handle) {
    tick_data_entry_t* entry;
    tick_data_entry_t* next;
    
    if (!handle) {
        return;
    }
    
    entry = handle->tick_dict_head;
    while (entry != NULL) {
        next = entry->next;
        free(entry->field_name);
        free(entry);
        entry = next;
    }
    
    handle->tick_dict_head = NULL;
}


/* ========================================================================
 * LIFECYCLE MANAGEMENT
 * ======================================================================== */

ct_timer_handle_t ct_timer_create(double wait_seconds) {
    ct_timer_handle_t handle;
    
    if (wait_seconds < 0.0) {
        return NULL;
    }
    
    handle = (ct_timer_handle_t)calloc(1, sizeof(struct ct_timer_context));
    if (!handle) {
        return NULL;
    }
    
    handle->wait_seconds = wait_seconds;
    handle->has_previous = false;
    handle->tick_dict_head = NULL;
    
    /* Initialize tick_dict with "time_tick" field (matches Python) */
    ct_timer_add_tick_data(handle, "time_tick", (int64_t)(wait_seconds * 1000));
    
    return handle;
}

void ct_timer_destroy(ct_timer_handle_t handle) {
    if (!handle) {
        return;
    }
    
    free_tick_data(handle);
    free(handle);
}


/* ========================================================================
 * CONFIGURATION
 * ======================================================================== */

ct_error_t ct_timer_set_wait(ct_timer_handle_t handle, double wait_seconds) {
    if (!handle) {
        return CT_ERROR_INVALID_HANDLE;
    }
    
    if (wait_seconds < 0.0) {
        return CT_ERROR_INVALID_PARAM;
    }
    
    handle->wait_seconds = wait_seconds;
    
    /* Update time_tick in tick_dict */
    ct_timer_add_tick_data(handle, "time_tick", (int64_t)(wait_seconds * 1000));
    
    return CT_SUCCESS;
}

double ct_timer_get_wait(ct_timer_handle_t handle) {
    if (!handle) {
        return -1.0;
    }
    
    return handle->wait_seconds;
}

ct_error_t ct_timer_add_tick_data(ct_timer_handle_t handle,
                                   const char* field_name,
                                   int64_t value) {
    tick_data_entry_t* entry;
    
    if (!handle || !field_name) {
        return CT_ERROR_INVALID_PARAM;
    }
    
    /* Check if entry already exists */
    entry = find_tick_data(handle, field_name);
    if (entry) {
        /* Update existing entry */
        entry->value = value;
        return CT_SUCCESS;
    }
    
    /* Create new entry */
    entry = (tick_data_entry_t*)malloc(sizeof(tick_data_entry_t));
    if (!entry) {
        return CT_ERROR_ALLOCATION;
    }
    
    entry->field_name = strdup(field_name);
    if (!entry->field_name) {
        free(entry);
        return CT_ERROR_ALLOCATION;
    }
    
    entry->value = value;
    entry->next = handle->tick_dict_head;
    handle->tick_dict_head = entry;
    
    return CT_SUCCESS;
}

ct_error_t ct_timer_get_tick_data(ct_timer_handle_t handle,
                                   const char* field_name,
                                   int64_t* value) {
    tick_data_entry_t* entry;
    
    if (!handle || !field_name || !value) {
        return CT_ERROR_INVALID_PARAM;
    }
    
    entry = find_tick_data(handle, field_name);
    if (!entry) {
        return CT_ERROR_NOT_FOUND;
    }
    
    *value = entry->value;
    return CT_SUCCESS;
}


/* ========================================================================
 * TIME QUERY FUNCTIONS
 * ======================================================================== */

ct_error_t ct_timer_wait(ct_timer_handle_t handle,
                         double wait_seconds,
                         ct_tick_result_t* result) {
    ct_error_t err;
    ct_time_info_t time_info;
    
    if (!result) {
        return CT_ERROR_INVALID_PARAM;
    }
    
    /* Wait for specified duration */
    err = sleep_seconds(wait_seconds);
    if (err != CT_SUCCESS) {
        return err;
    }
    
    /* Get current time */
    err = get_current_time_internal(&time_info);
    if (err != CT_SUCCESS) {
        return err;
    }
    
    result->all_values = time_info;
    
    /* Compute change mask if we have a handle */
    if (handle) {
        result->changed_mask = update_and_get_changes(handle, &time_info);
        
        /* Update timestamp in tick_dict */
        ct_timer_add_tick_data(handle, "time_stamp", time_info.timestamp);
    } else {
        /* Stateless mode - no change tracking */
        result->changed_mask = 0;
    }
    
    return CT_SUCCESS;
}

ct_error_t ct_timer_get_current_time(ct_timer_handle_t handle,
                                      ct_tick_result_t* result) {
    ct_error_t err;
    ct_time_info_t time_info;
    
    if (!result) {
        return CT_ERROR_INVALID_PARAM;
    }
    
    /* Get current time */
    err = get_current_time_internal(&time_info);
    if (err != CT_SUCCESS) {
        return err;
    }
    
    result->all_values = time_info;
    
    /* Compute change mask if we have a handle */
    if (handle) {
        result->changed_mask = update_and_get_changes(handle, &time_info);
        
        /* Update timestamp in tick_dict */
        ct_timer_add_tick_data(handle, "time_stamp", time_info.timestamp);
    } else {
        /* Stateless mode - no change tracking */
        result->changed_mask = 0;
    }
    
    return CT_SUCCESS;
}

int64_t ct_timer_get_timestamp(ct_timer_handle_t handle) {
    struct timespec ts;
    
    (void)handle;  /* Unused parameter */
    
    if (clock_gettime(CLOCK_REALTIME, &ts) != 0) {
        return -1;
    }
    
    return (int64_t)ts.tv_sec;
}

ct_error_t ct_timer_get_time_simple(ct_time_info_t* time_info) {
    return get_current_time_internal(time_info);
}

ct_error_t ct_timer_tick(ct_timer_handle_t handle,
                         ct_tick_result_t* result) {
    if (!handle) {
        return CT_ERROR_INVALID_HANDLE;
    }
    
    /* Use configured wait_seconds */
    return ct_timer_wait(handle, handle->wait_seconds, result);
}


/* ========================================================================
 * FORMATTING AND DISPLAY
 * ======================================================================== */

int ct_timer_format_time(const ct_time_info_t* time_info,
                         char* buffer,
                         size_t buffer_size) {
    struct tm tm_result;
    time_t timestamp;
    
    if (!time_info || !buffer || buffer_size == 0) {
        return -1;
    }
    
    timestamp = (time_t)time_info->timestamp;
    
    if (gmtime_r(&timestamp, &tm_result) == NULL) {
        return -1;
    }
    
    /* Format as: YYYY-MM-DD HH:MM:SS UTC */
    return snprintf(buffer, buffer_size, "%04d-%02d-%02d %02d:%02d:%02d UTC",
                    time_info->year,
                    time_info->month,
                    time_info->day,
                    time_info->hour,
                    time_info->minute,
                    time_info->second);
}

int ct_timer_format_tick_result(const ct_tick_result_t* result,
                                 char* buffer,
                                 size_t buffer_size) {
    char time_str[64];
    size_t offset = 0;
    int written;
    
    if (!result || !buffer || buffer_size == 0) {
        return -1;
    }
    
    /* Format time */
    ct_timer_format_time(&result->all_values, time_str, sizeof(time_str));
    
    written = snprintf(buffer + offset, buffer_size - offset,
                      "Time: %s\n", time_str);
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
        
        if (result->changed_mask & CT_CHANGED_SECOND) {
            written = snprintf(buffer + offset, buffer_size - offset,
                              "  second: %d\n", result->all_values.second);
            if (written < 0 || (size_t)written >= buffer_size - offset) {
                return -1;
            }
            offset += written;
        }
        
        if (result->changed_mask & CT_CHANGED_MINUTE) {
            written = snprintf(buffer + offset, buffer_size - offset,
                              "  minute: %d\n", result->all_values.minute);
            if (written < 0 || (size_t)written >= buffer_size - offset) {
                return -1;
            }
            offset += written;
        }
        
        if (result->changed_mask & CT_CHANGED_HOUR) {
            written = snprintf(buffer + offset, buffer_size - offset,
                              "  hour: %d\n", result->all_values.hour);
            if (written < 0 || (size_t)written >= buffer_size - offset) {
                return -1;
            }
            offset += written;
        }
        
        if (result->changed_mask & CT_CHANGED_DAY) {
            written = snprintf(buffer + offset, buffer_size - offset,
                              "  day: %d\n", result->all_values.day);
            if (written < 0 || (size_t)written >= buffer_size - offset) {
                return -1;
            }
            offset += written;
        }
        
        if (result->changed_mask & CT_CHANGED_DOW) {
            written = snprintf(buffer + offset, buffer_size - offset,
                              "  dow: %d (0=Mon, 6=Sun)\n", result->all_values.dow);
            if (written < 0 || (size_t)written >= buffer_size - offset) {
                return -1;
            }
            offset += written;
        }
        
        if (result->changed_mask & CT_CHANGED_DOY) {
            written = snprintf(buffer + offset, buffer_size - offset,
                              "  doy: %d\n", result->all_values.doy);
            if (written < 0 || (size_t)written >= buffer_size - offset) {
                return -1;
            }
            offset += written;
        }
        
        if (result->changed_mask & CT_CHANGED_MONTH) {
            written = snprintf(buffer + offset, buffer_size - offset,
                              "  month: %d\n", result->all_values.month);
            if (written < 0 || (size_t)written >= buffer_size - offset) {
                return -1;
            }
            offset += written;
        }
        
        if (result->changed_mask & CT_CHANGED_YEAR) {
            written = snprintf(buffer + offset, buffer_size - offset,
                              "  year: %d\n", result->all_values.year);
            if (written < 0 || (size_t)written >= buffer_size - offset) {
                return -1;
            }
            offset += written;
        }
        
        if (result->changed_mask & CT_CHANGED_TIMESTAMP) {
            written = snprintf(buffer + offset, buffer_size - offset,
                              "  timestamp: %ld\n", result->all_values.timestamp);
            if (written < 0 || (size_t)written >= buffer_size - offset) {
                return -1;
            }
            offset += written;
        }
    }
    
    return (int)offset;
}

void ct_timer_print_time_info(const ct_time_info_t* time_info) {
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
    printf("Unix Timestamp: %ld\n", time_info->timestamp);
    
    ct_timer_format_time(time_info, formatted, sizeof(formatted));
    printf("Formatted GMT: %s\n", formatted);
}

void ct_timer_print_tick_result(const ct_tick_result_t* result) {
    char buffer[1024];
    
    if (!result) {
        return;
    }
    
    ct_timer_format_tick_result(result, buffer, sizeof(buffer));
    printf("%s", buffer);
}


/* ========================================================================
 * ERROR HANDLING
 * ======================================================================== */

const char* ct_timer_error_string(ct_error_t error) {
    switch (error) {
        case CT_SUCCESS:
            return "Success";
        case CT_ERROR_INVALID_HANDLE:
            return "Invalid handle";
        case CT_ERROR_INVALID_PARAM:
            return "Invalid parameter";
        case CT_ERROR_ALLOCATION:
            return "Memory allocation failed";
        case CT_ERROR_SYSTEM:
            return "System call failed";
        case CT_ERROR_NOT_FOUND:
            return "Item not found";
        default:
            return "Unknown error";
    }
}
