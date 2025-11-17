/**
 * @file ct_timer.h
 * @brief Handle-based Calendar Timer Service
 * 
 * Provides thread-safe timer operations through opaque handles.
 * Each handle maintains independent state, allowing safe concurrent use
 * across multiple threads without external synchronization.
 * 
 * Architecture: Handle-based design ensures each timer context is isolated,
 * making operations inherently thread-safe when one thread owns one handle.
 */

 #ifndef CT_TIMER_H
 #define CT_TIMER_H
 
 #include <stdint.h>
 #include <stdbool.h>
 #include <time.h>
 #include <stddef.h>
 
 #ifdef __cplusplus
 extern "C" {
 #endif
 
 /* ========================================================================
  * OPAQUE HANDLE TYPE
  * ======================================================================== */
 
 /**
  * @brief Opaque timer context handle
  * 
  * Internal structure is not exposed to maintain architectural independence.
  * Each handle represents an independent timer context with its own state.
  */
 typedef struct ct_timer_context* ct_timer_handle_t;
 
 
 /* ========================================================================
  * TIME INFORMATION STRUCTURES
  * ======================================================================== */
 
 /**
  * @brief Comprehensive time information in GMT/UTC
  * 
  * Matches Python dict returned by wait_timer() and get_current_time()
  */
 typedef struct {
     int32_t year;       /**< Year (e.g., 2024) */
     int32_t month;      /**< Month (1-12) */
     int32_t day;        /**< Day of month (1-31) */
     int32_t dow;        /**< Day of week (0=Monday, ..., 6=Sunday) */
     int32_t doy;        /**< Day of year (1-366) */
     int32_t hour;       /**< Hour (0-23) GMT */
     int32_t minute;     /**< Minute (0-59) */
     int32_t second;     /**< Second (0-59) */
     int64_t timestamp;  /**< Unix timestamp (seconds since epoch) */
 } ct_time_info_t;
 
 /**
  * @brief Timer tick result structure
  * 
  * Matches Python dict returned by timer_tick() with 'changed' and 'all_values'
  */
 typedef struct {
     ct_time_info_t all_values;  /**< Complete time information */
     uint32_t changed_mask;      /**< Bitmask indicating which fields changed */
 } ct_tick_result_t;
 
 
 /* ========================================================================
  * CHANGE DETECTION BITMASK
  * ======================================================================== */
 
 /**
  * @brief Change detection flags for timer_tick
  * 
  * Used in ct_tick_result_t.changed_mask to indicate which time
  * components have changed since the last tick
  */
 #define CT_CHANGED_SECOND    (1U << 0)   /**< Second changed */
 #define CT_CHANGED_MINUTE    (1U << 1)   /**< Minute changed */
 #define CT_CHANGED_HOUR      (1U << 2)   /**< Hour changed */
 #define CT_CHANGED_DAY       (1U << 3)   /**< Day changed */
 #define CT_CHANGED_DOW       (1U << 4)   /**< Day of week changed */
 #define CT_CHANGED_DOY       (1U << 5)   /**< Day of year changed */
 #define CT_CHANGED_MONTH     (1U << 6)   /**< Month changed */
 #define CT_CHANGED_YEAR      (1U << 7)   /**< Year changed */
 #define CT_CHANGED_TIMESTAMP (1U << 8)   /**< Timestamp changed */
 
 
 /* ========================================================================
  * ERROR CODES
  * ======================================================================== */
 
 /**
  * @brief Error codes for API functions
  */
 typedef enum {
     CT_SUCCESS = 0,              /**< Operation succeeded */
     CT_ERROR_INVALID_HANDLE = -1,/**< NULL or invalid handle */
     CT_ERROR_INVALID_PARAM = -2, /**< Invalid parameter value */
     CT_ERROR_ALLOCATION = -3,    /**< Memory allocation failed */
     CT_ERROR_SYSTEM = -4,        /**< System call failed */
     CT_ERROR_NOT_FOUND = -5      /**< Item not found */
 } ct_error_t;
 
 
 /* ========================================================================
  * LIFECYCLE MANAGEMENT
  * ======================================================================== */
 
 /**
  * @brief Create a new timer handle
  * 
  * Matches Python: CT_Timer.__init__(self, wait_seconds)
  * 
  * Creates an independent timer context with its own state.
  * Each handle can be used by a single thread without external locking.
  * 
  * @param wait_seconds Default wait interval in seconds (can be fractional)
  * @return New timer handle or NULL on failure
  */
 ct_timer_handle_t ct_timer_create(double wait_seconds);
 
 /**
  * @brief Destroy a timer handle and free all resources
  * 
  * Matches Python object destruction.
  * 
  * @param handle Timer handle to destroy (NULL-safe)
  */
 void ct_timer_destroy(ct_timer_handle_t handle);
 
 
 /* ========================================================================
  * CONFIGURATION
  * ======================================================================== */
 
 /**
  * @brief Set the default wait interval for timer_tick
  * 
  * Matches Python: self.wait_seconds = value
  * 
  * @param handle Timer handle
  * @param wait_seconds New wait interval in seconds
  * @return CT_SUCCESS or error code
  */
 ct_error_t ct_timer_set_wait(ct_timer_handle_t handle, double wait_seconds);
 
 /**
  * @brief Get the current default wait interval
  * 
  * Matches Python: self.wait_seconds
  * 
  * @param handle Timer handle
  * @return Wait interval in seconds, or -1.0 on error
  */
 double ct_timer_get_wait(ct_timer_handle_t handle);
 
 /**
  * @brief Add custom data to tick dictionary
  * 
  * Matches Python: self.add_dict_dict(field_name, value)
  * 
  * Custom fields are stored in the internal tick dictionary.
  * The timer automatically includes "time_tick" and "time_stamp" fields.
  * The caller can retrieve this data through their own mechanism.
  * 
  * @param handle Timer handle
  * @param field_name Field name (will be copied)
  * @param value Integer value to store
  * @return CT_SUCCESS or error code
  */
 ct_error_t ct_timer_add_tick_data(
     ct_timer_handle_t handle,
     const char* field_name,
     int64_t value
 );
 
 /**
  * @brief Get custom data from tick dictionary
  * 
  * Retrieves a value previously stored with ct_timer_add_tick_data().
  * 
  * @param handle Timer handle
  * @param field_name Field name to retrieve
  * @param value Output pointer for the value
  * @return CT_SUCCESS, CT_ERROR_NOT_FOUND, or other error code
  */
 ct_error_t ct_timer_get_tick_data(
     ct_timer_handle_t handle,
     const char* field_name,
     int64_t* value
 );
 
 
 /* ========================================================================
  * TIME QUERY FUNCTIONS
  * ======================================================================== */
 
 /**
  * @note Change Tracking Behavior:
  * All time query functions (ct_timer_wait, ct_timer_get_current_time, ct_timer_tick)
  * share the same internal change tracking state (_last_time_info) in the handle.
  * This means:
  * - First call to ANY of these functions marks all fields as changed
  * - Subsequent calls compare against the last call to ANY of these functions
  * - This mirrors Python's timer_tick() behavior with _last_time_info
  */
 
 /**
  * @brief Wait for specified time and return time information with change detection
  * 
  * Matches Python: self.wait_timer(wait_seconds)
  * 
  * Sleeps for the specified duration, then returns comprehensive
  * time information in GMT/UTC along with a change mask indicating
  * which time components changed since the last call.
  * 
  * If handle is NULL, operates in stateless mode (no change tracking).
  * If result is NULL, returns CT_ERROR_INVALID_PARAM.
  * 
  * @param handle Timer handle (NULL for stateless, but no change mask)
  * @param wait_seconds Time to wait in seconds (can be fractional)
  * @param result Output structure with time info and change mask
  * @return CT_SUCCESS or error code
  */
 ct_error_t ct_timer_wait(
     ct_timer_handle_t handle,
     double wait_seconds,
     ct_tick_result_t* result
 );
 
 /**
  * @brief Get current time without waiting, with change detection
  * 
  * Matches Python: self.get_current_time()
  * 
  * Returns current GMT/UTC time and change mask without waiting.
  * Change mask shows what changed since the last call to this function,
  * ct_timer_wait(), or ct_timer_tick().
  * 
  * If handle is NULL, operates in stateless mode (no change tracking).
  * 
  * @param handle Timer handle (NULL for stateless, but no change mask)
  * @param result Output structure with time info and change mask
  * @return CT_SUCCESS or error code
  */
 ct_error_t ct_timer_get_current_time(
     ct_timer_handle_t handle,
     ct_tick_result_t* result
 );
 
 /**
  * @brief Get current Unix timestamp
  * 
  * Matches Python: self.get_timestamp()
  * 
  * Returns current timestamp without affecting change tracking state.
  * This is a lightweight call that doesn't update _last_time_info.
  * 
  * @param handle Timer handle (can be NULL for stateless operation)
  * @return Unix timestamp in seconds since epoch
  */
 int64_t ct_timer_get_timestamp(ct_timer_handle_t handle);
 
 /**
  * @brief Get simple time info without change tracking
  * 
  * Returns current time information without updating the internal
  * change tracking state. Use this when you need time info but don't
  * want to affect change detection for other calls.
  * 
  * @param time_info Output structure for time information
  * @return CT_SUCCESS or error code
  */
 ct_error_t ct_timer_get_time_simple(ct_time_info_t* time_info);
 
 
 /* ========================================================================
  * TIMER TICK - MAIN FUNCTION
  * ======================================================================== */
 
 /**
  * @brief Perform timer tick with change detection
  * 
  * Matches Python: self.timer_tick(all_start_nodes) (but without event generation)
  * 
  * This is the core function that:
  * 1. Waits for self.wait_seconds
  * 2. Gets current time information
  * 3. Compares with previous tick to detect changes
  * 4. Updates internal state for next comparison
  * 5. Returns complete time info and change mask
  * 
  * On first call, all fields are marked as changed.
  * 
  * The caller can inspect the changed_mask to determine what changed
  * and integrate with their own event system as needed.
  * 
  * @param handle Timer handle
  * @param result Output structure containing all_values and changed_mask
  * @return CT_SUCCESS or error code
  */
 ct_error_t ct_timer_tick(
     ct_timer_handle_t handle,
     ct_tick_result_t* result
 );
 
 
 /* ========================================================================
  * FORMATTING AND DISPLAY
  * ======================================================================== */
 
 /**
  * @brief Format time info to ISO 8601 string
  * 
  * Matches Python: self.format_time_info(time_info)
  * Produces: "YYYY-MM-DD HH:MM:SS UTC"
  * 
  * @param time_info Time information to format
  * @param buffer Output buffer
  * @param buffer_size Size of output buffer
  * @return Number of characters written (excluding null), or -1 on error
  */
 int ct_timer_format_time(
     const ct_time_info_t* time_info,
     char* buffer,
     size_t buffer_size
 );
 
 /**
  * @brief Format tick result to human-readable string
  * 
  * Matches Python: self.format_tick_result(tick_result)
  * 
  * Shows changed values and their new values.
  * 
  * @param result Tick result to format
  * @param buffer Output buffer
  * @param buffer_size Size of output buffer
  * @return Number of characters written (excluding null), or -1 on error
  */
 int ct_timer_format_tick_result(
     const ct_tick_result_t* result,
     char* buffer,
     size_t buffer_size
 );
 
 /**
  * @brief Print comprehensive time information to stdout
  * 
  * Matches Python: self.print_time_info(time_info)
  * 
  * @param time_info Time information to print
  */
 void ct_timer_print_time_info(const ct_time_info_t* time_info);
 
 /**
  * @brief Print tick result to stdout
  * 
  * @param result Tick result to print
  */
 void ct_timer_print_tick_result(const ct_tick_result_t* result);
 
 
 /* ========================================================================
  * ERROR HANDLING
  * ======================================================================== */
 
 /**
  * @brief Get human-readable error string
  * 
  * @param error Error code
  * @return Static error string (never NULL)
  */
 const char* ct_timer_error_string(ct_error_t error);
 
 
 /* ========================================================================
  * UTILITY MACROS
  * ======================================================================== */
 
 /**
  * @brief Check if a specific field changed in tick result
  * 
  * Example: if (CT_FIELD_CHANGED(result, MINUTE)) { ... }
  */
 #define CT_FIELD_CHANGED(result, field) \
     (((result)->changed_mask & CT_CHANGED_##field) != 0)
 
 
 #ifdef __cplusplus
 }
 #endif
 
 #endif /* CT_TIMER_H */