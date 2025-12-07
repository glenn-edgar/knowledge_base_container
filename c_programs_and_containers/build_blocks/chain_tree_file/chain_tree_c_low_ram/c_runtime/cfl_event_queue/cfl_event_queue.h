/**
 * @file cfl_event_queue.h
 * @brief Priority event queue system for ChainTree distributed control
 * 
 * Provides a dual-priority event queue with variable allocation per priority level.
 * Designed for memory-constrained embedded systems (32KB RAM) through large servers.
 * Lock-free single-accessor per queue instance.
 * 
 * @author Glenn - Onyx Engineering
 * @date 2025
 */

#ifndef CFL_EVENT_QUEUE_H
#define CFL_EVENT_QUEUE_H

#include <stdint.h>
#include <stdbool.h>
#include "cfl_heap.h"  // Assumed: your heap allocator interface

#ifdef __cplusplus
extern "C" {
#endif

/*==============================================================================
 * Platform Configuration
 *============================================================================*/

// Auto-detect platform word size
#if defined(__LP64__) || defined(_WIN64) || defined(__x86_64__) || \
    defined(__aarch64__) || defined(__64BIT__) || (UINTPTR_MAX == UINT64_MAX)
    #define CFL_64BIT
#endif

// Platform-specific type definitions
#ifdef CFL_64BIT
    typedef uint64_t    cfl_size_t;
    typedef int64_t     cfl_int_t;
    typedef double      cfl_float_t;
#else
    typedef uint32_t    cfl_size_t;
    typedef int32_t     cfl_int_t;
    typedef float       cfl_float_t;
#endif

/*==============================================================================
 * Constants and Macros
 *============================================================================*/

/** Special node ID for broadcast to all active knowledge bases */
#define CFL_EVENT_BROADCAST_NODE    0xFFFF

/** Flag bit definitions */
#define CFL_EVENT_MALLOC_FLAG       0x01    /**< Bit 0: Executive should free data pointer */

/** Priority levels */
#define CFL_EVENT_PRIORITY_LOW      0       /**< Normal priority queue */
#define CFL_EVENT_PRIORITY_HIGH     1       /**< High priority queue */

/** Event data types (user-defined semantics) */
#define CFL_EVENT_TYPE_PTR          0       /**< Data is pointer */
#define CFL_EVENT_TYPE_INT          1       /**< Data is signed integer */
#define CFL_EVENT_TYPE_UINT         2       /**< Data is unsigned integer */
#define CFL_EVENT_TYPE_FLOAT        3       /**< Data is float/double */

/** Minimum queue size (power of 2) */
#define CFL_EVENT_QUEUE_MIN_SIZE    2

/*==============================================================================
 * Data Types
 *============================================================================*/

/**
 * @brief Polymorphic event data union
 * 
 * Stores one of four data types without dynamic allocation.
 * Size: 4 bytes on 32-bit systems, 8 bytes on 64-bit systems.
 */
typedef union {
    void*           ptr;            /**< Pointer to allocated data */
    cfl_int_t       integer;        /**< Signed integer value */
    cfl_size_t      unsigned_val;   /**< Unsigned integer value */
    cfl_float_t     floating;       /**< Floating point value */
} CFL_EVENT_VALUE_T;

/**
 * @brief Individual event data structure
 * 
 * Contains all information about a single event.
 * Size: 12 bytes on 32-bit systems, 16 bytes on 64-bit systems.
 */
typedef struct {
    uint16_t            node_id;        /**< Target node (0xFFFF = broadcast) */
    uint8_t             event_type;     /**< 0-3: ptr/int/uint/float */
    uint8_t             flags;          /**< Bit 0: malloc_flag */
    uint16_t            event_id;       /**< Application-defined event ID */
    uint16_t            queue_number;   /**< Source queue identifier */
    CFL_EVENT_VALUE_T   data;           /**< Polymorphic payload */
} CFL_EVENT_DATA_T;

/**
 * @brief Ring buffer for one priority level
 * 
 * Power-of-2 sized ring buffer with mask-based wrapping.
 * Single accessor only - no thread synchronization needed.
 */
typedef struct {
    uint16_t            head;           /**< Write index (producer) */
    uint16_t            tail;           /**< Read index (consumer) */
    uint16_t            capacity;       /**< Total slots (power of 2) */
    uint16_t            mask;           /**< Capacity - 1 (for fast modulo) */
    CFL_EVENT_DATA_T*   events;         /**< Event array */
} CFL_EVENT_RING_T;

/**
 * @brief Main event queue control structure
 * 
 * Dual-priority queue with independent sizing.
 * Size: ~28 bytes on 32-bit systems, ~40 bytes on 64-bit systems.
 */
typedef struct CFL_EVENT_QUEUE_T {
    CFL_EVENT_RING_T    high_priority;  /**< Priority 1 ring buffer */
    CFL_EVENT_RING_T    low_priority;   /**< Priority 0 ring buffer */
    uint16_t            queue_id;       /**< Unique queue identifier */
    uint16_t            reserved;       /**< Alignment/future use */
} CFL_EVENT_QUEUE_T;

/*==============================================================================
 * Function Prototypes
 *============================================================================*/

/**
 * @brief Create an event queue with variable priority allocation
 * 
 * Allocates control structure and event arrays from the specified heap.
 * Both priority sizes are rounded up to the next power of 2.
 * 
 * @param high_priority_size Requested high priority entries (rounds up to power of 2)
 * @param low_priority_size  Requested low priority entries (rounds up to power of 2)
 * @param heap               Heap allocator to use
 * @return Pointer to queue control structure, or NULL on allocation failure
 * 
 * @note Minimum size per priority is 2 entries
 * @note Actual capacity is rounded_size - 1 due to full/empty detection
 * 
 * Example:
 * @code
 *   // Create queue: 8 high priority, 32 low priority
 *   // Allocates: 8 + 64 = 72 event slots (rounds 32→64)
 *   CFL_EVENT_QUEUE_T* q = cfl_create_event_queue(8, 32, my_heap);
 * @endcode
 */
CFL_EVENT_QUEUE_T* cfl_create_event_queue(
    unsigned high_priority_size,
    unsigned low_priority_size,
    CflHeap* heap);

/**
 * @brief Clear all events from queue
 * 
 * Resets head and tail indices on both priority rings.
 * Does NOT free any malloc'd event data - caller responsible.
 * 
 * @param queue_control Pointer to queue control structure
 * 
 * @warning Any events with malloc_flag set will leak memory unless
 *          manually freed before calling this function
 */
void cfl_clear_queue(
    CFL_EVENT_QUEUE_T *queue_control);

/**
 * @brief Send an event to the queue
 * 
 * Inserts event into specified priority ring buffer.
 * Returns false if the target priority queue is full.
 * 
 * @param queue_control Pointer to queue control structure
 * @param priority      Priority level (0=low, 1=high)
 * @param node_id       Target node (0xFFFF for broadcast)
 * @param event_type    Data type (0-3: ptr/int/uint/float)
 * @param malloc_flag   True if executive should free data pointer
 * @param event_id      Application-defined event identifier
 * @param data          Event payload (cast to appropriate type)
 * @return true if event queued, false if queue full
 * 
 * Example:
 * @code
 *   // Send pointer event
 *   void* mydata = malloc(100);
 *   cfl_send_event(q, CFL_EVENT_PRIORITY_HIGH, 0x0042, 
 *                  CFL_EVENT_TYPE_PTR, true, 0x1001, mydata);
 *   
 *   // Send integer event
 *   cfl_send_event(q, CFL_EVENT_PRIORITY_LOW, 0xFFFF,
 *                  CFL_EVENT_TYPE_INT, false, 0x2002, (void*)(intptr_t)42);
 * @endcode
 */
bool cfl_send_event(
    CFL_EVENT_QUEUE_T *queue_control,
    unsigned priority,
    unsigned node_id,
    unsigned event_type,
    bool malloc_flag,
    unsigned event_id,
    void *data);

/**
 * @brief Pop highest priority event from queue
 * 
 * Checks high priority queue first, then low priority.
 * Returns false if both queues are empty.
 * 
 * @param queue_control Pointer to queue control structure
 * @param event_data    Pointer to structure to receive event data
 * @return true if event retrieved, false if both queues empty
 * 
 * Example:
 * @code
 *   CFL_EVENT_DATA_T event;
 *   if (cfl_pop_event(q, &event)) {
 *       // Process event based on type
 *       switch (event.event_type) {
 *           case CFL_EVENT_TYPE_PTR:
 *               process_ptr(event.data.ptr);
 *               if (event.flags & CFL_EVENT_MALLOC_FLAG) {
 *                   free(event.data.ptr);
 *               }
 *               break;
 *           case CFL_EVENT_TYPE_INT:
 *               process_int(event.data.integer);
 *               break;
 *       }
 *   }
 * @endcode
 */
bool cfl_pop_event(
    CFL_EVENT_QUEUE_T *queue_control,
    CFL_EVENT_DATA_T *event_data);

/**
 * @brief Get queue number from event data
 * 
 * Returns the queue_number field which identifies which queue
 * the event originated from (useful in multi-queue systems).
 * 
 * @param event_data Pointer to event data structure
 * @return Queue identifier number
 */
unsigned cfl_queue_number(
    CFL_EVENT_DATA_T *event_data);

/**
 * @brief Get count of events in high priority queue
 * 
 * @param queue_control Pointer to queue control structure
 * @return Number of events in high priority queue
 */
unsigned cfl_high_priority_count(
    CFL_EVENT_QUEUE_T *queue_control);

/**
 * @brief Get count of events in low priority queue
 * 
 * @param queue_control Pointer to queue control structure
 * @return Number of events in low priority queue
 */
unsigned cfl_low_priority_count(
    CFL_EVENT_QUEUE_T *queue_control);

/**
 * @brief Get total count of events in both queues
 * 
 * @param queue_control Pointer to queue control structure
 * @return Total number of events across both priorities
 */
unsigned cfl_total_event_count(
    CFL_EVENT_QUEUE_T *queue_control);


/*==============================================================================
 * Helper Functions for Type-Specific Events
 *============================================================================*/

/**
 * @brief Send an unsigned integer event
 * 
 * Convenience wrapper for sending unsigned integer events.
 * Automatically sets event_type to CFL_EVENT_TYPE_UINT and malloc_flag to false.
 * 
 * @param queue_control Pointer to queue control structure
 * @param priority      Priority level (0=low, 1=high)
 * @param node_id       Target node (0xFFFF for broadcast)
 * @param event_id      Application-defined event identifier
 * @param value         Unsigned integer value to send
 * @return true if event queued, false if queue full
 * 
 * Example:
 * @code
 *   // Send counter value as event
 *   cfl_send_unsigned_event(q, CFL_EVENT_PRIORITY_LOW, 0x0042, 0x3001, counter);
 * @endcode
 */
 bool cfl_send_unsigned_event(
    CFL_EVENT_QUEUE_T *queue_control,
    unsigned priority,
    unsigned node_id,
    unsigned event_id,
    cfl_size_t value);

/**
 * @brief Send a signed integer event
 * 
 * Convenience wrapper for sending signed integer events.
 * Automatically sets event_type to CFL_EVENT_TYPE_INT and malloc_flag to false.
 * 
 * @param queue_control Pointer to queue control structure
 * @param priority      Priority level (0=low, 1=high)
 * @param node_id       Target node (0xFFFF for broadcast)
 * @param event_id      Application-defined event identifier
 * @param value         Signed integer value to send
 * @return true if event queued, false if queue full
 * 
 * Example:
 * @code
 *   // Send temperature reading as event
 *   cfl_send_integer_event(q, CFL_EVENT_PRIORITY_LOW, 0x0042, 0x3002, -15);
 * @endcode
 */
bool cfl_send_integer_event(
    CFL_EVENT_QUEUE_T *queue_control,
    unsigned priority,
    unsigned node_id,
    unsigned event_id,
    cfl_int_t value);

/**
 * @brief Send a floating point event
 * 
 * Convenience wrapper for sending float/double events.
 * Automatically sets event_type to CFL_EVENT_TYPE_FLOAT and malloc_flag to false.
 * 
 * @param queue_control Pointer to queue control structure
 * @param priority      Priority level (0=low, 1=high)
 * @param node_id       Target node (0xFFFF for broadcast)
 * @param event_id      Application-defined event identifier
 * @param value         Float or double value to send
 * @return true if event queued, false if queue full
 * 
 * Example:
 * @code
 *   // Send sensor reading as event
 *   cfl_send_float_event(q, CFL_EVENT_PRIORITY_HIGH, 0x0042, 0x3003, 98.6);
 * @endcode
 */
bool cfl_send_float_event(
    CFL_EVENT_QUEUE_T *queue_control,
    unsigned priority,
    unsigned node_id,
    unsigned event_id,
    cfl_float_t value);

/**
 * @brief Send a pointer/data event
 * 
 * Convenience wrapper for sending pointer events with explicit memory management.
 * Automatically sets event_type to CFL_EVENT_TYPE_PTR.
 * 
 * @param queue_control Pointer to queue control structure
 * @param priority      Priority level (0=low, 1=high)
 * @param node_id       Target node (0xFFFF for broadcast)
 * @param malloc_flag   True if executive should free data pointer after processing
 * @param event_id      Application-defined event identifier
 * @param data          Pointer to event data
 * @return true if event queued, false if queue full
 * 
 * Example:
 * @code
 *   // Send dynamically allocated data (executive will free)
 *   char* msg = malloc(100);
 *   strcpy(msg, "Hello");
 *   cfl_send_data_event(q, CFL_EVENT_PRIORITY_HIGH, 0x0042, true, 0x3004, msg);
 *   
 *   // Send static data (no free needed)
 *   cfl_send_data_event(q, CFL_EVENT_PRIORITY_LOW, 0x0042, false, 0x3005, &my_struct);
 * @endcode
 */
bool cfl_send_data_event(
    CFL_EVENT_QUEUE_T *queue_control,
    unsigned priority,
    unsigned node_id,
    bool malloc_flag,
    unsigned event_id,
    void *data);

#ifdef __cplusplus
}
#endif

#endif /* CFL_EVENT_QUEUE_H */

