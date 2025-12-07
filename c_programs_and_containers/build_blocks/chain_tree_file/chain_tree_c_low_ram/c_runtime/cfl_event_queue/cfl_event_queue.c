/**
 * @file cfl_event_queue.c
 * @brief Implementation of priority event queue system
 */

#include "cfl_event_queue.h"
#include <string.h>  // For memset

/*==============================================================================
 * Static Helper Functions
 *============================================================================*/

/**
 * @brief Round up to next power of 2
 * 
 * @param size Requested size
 * @return Next power of 2 >= size, minimum of CFL_EVENT_QUEUE_MIN_SIZE
 */
static uint16_t round_up_power_of_2(unsigned size)
{
    if (size < CFL_EVENT_QUEUE_MIN_SIZE) {
        return CFL_EVENT_QUEUE_MIN_SIZE;
    }
    
    // Check for overflow
    if (size > 32768) {
        return 32768;  // Max uint16_t power of 2
    }
    
    uint16_t power = 1;
    while (power < size) {
        power <<= 1;
    }
    
    return power;
}

/**
 * @brief Initialize a ring buffer
 * 
 * @param ring Ring buffer to initialize
 * @param capacity Capacity (must be power of 2)
 * @param events Pointer to event array
 */
static void init_ring(CFL_EVENT_RING_T* ring, uint16_t capacity, CFL_EVENT_DATA_T* events)
{
    ring->head = 0;
    ring->tail = 0;
    ring->capacity = capacity;
    ring->mask = capacity - 1;
    ring->events = events;
}

/**
 * @brief Check if ring buffer is full
 * 
 * @param ring Ring buffer to check
 * @return true if full, false otherwise
 */
static inline bool ring_is_full(const CFL_EVENT_RING_T* ring)
{
    return ((ring->head + 1) & ring->mask) == ring->tail;
}

/**
 * @brief Check if ring buffer is empty
 * 
 * @param ring Ring buffer to check
 * @return true if empty, false otherwise
 */
static inline bool ring_is_empty(const CFL_EVENT_RING_T* ring)
{
    return ring->head == ring->tail;
}

/**
 * @brief Get number of events in ring buffer
 * 
 * @param ring Ring buffer to query
 * @return Number of events currently in ring
 */
static inline uint16_t ring_count(const CFL_EVENT_RING_T* ring)
{
    return (ring->head - ring->tail) & ring->mask;
}

/**
 * @brief Insert event into ring buffer (assumes not full)
 * 
 * @param ring Ring buffer
 * @param event Event to insert
 */
static inline void ring_push(CFL_EVENT_RING_T* ring, const CFL_EVENT_DATA_T* event)
{
    ring->events[ring->head] = *event;
    ring->head = (ring->head + 1) & ring->mask;
}

/**
 * @brief Remove event from ring buffer (assumes not empty)
 * 
 * @param ring Ring buffer
 * @param event Pointer to receive event data
 */
static inline void ring_pop(CFL_EVENT_RING_T* ring, CFL_EVENT_DATA_T* event)
{
    *event = ring->events[ring->tail];
    ring->tail = (ring->tail + 1) & ring->mask;
}

/**
 * @brief Clear ring buffer
 * 
 * @param ring Ring buffer to clear
 */
static inline void ring_clear(CFL_EVENT_RING_T* ring)
{
    ring->head = 0;
    ring->tail = 0;
}

/*==============================================================================
 * Global Variables
 *============================================================================*/

/** Next queue ID to assign (increments for each created queue) */
static uint16_t g_next_queue_id = 0;

/*==============================================================================
 * Public Function Implementations
 *============================================================================*/

CFL_EVENT_QUEUE_T* cfl_create_event_queue(
    unsigned high_priority_size,
    unsigned low_priority_size,
    CflHeap* heap)
{
    // Validate heap pointer
    if (heap == NULL) {
        return NULL;
    }
    
    // Round up to powers of 2
    uint16_t high_capacity = round_up_power_of_2(high_priority_size);
    uint16_t low_capacity = round_up_power_of_2(low_priority_size);
    
    // Calculate allocation sizes
    uint16_t control_size = sizeof(CFL_EVENT_QUEUE_T);
    uint16_t high_array_size = high_capacity * sizeof(CFL_EVENT_DATA_T);
    uint16_t low_array_size = low_capacity * sizeof(CFL_EVENT_DATA_T);
    
    // Allocate control structure
    CFL_EVENT_QUEUE_T* queue = (CFL_EVENT_QUEUE_T*)cfl_heap_malloc(heap, control_size);
    if (queue == NULL) {
        return NULL;
    }
    
    // Allocate high priority event array
    CFL_EVENT_DATA_T* high_events = (CFL_EVENT_DATA_T*)cfl_heap_malloc(heap, high_array_size);
    if (high_events == NULL) {
        // Note: Assuming cfl_heap has no free function based on your API
        // If it does, should free queue here
        return NULL;
    }
    
    // Allocate low priority event array
    CFL_EVENT_DATA_T* low_events = (CFL_EVENT_DATA_T*)cfl_heap_malloc(heap, low_array_size);
    if (low_events == NULL) {
        // Should free queue and high_events if free function available
        return NULL;
    }
    
    // Initialize control structure
    memset(queue, 0, sizeof(CFL_EVENT_QUEUE_T));
    
    // Initialize ring buffers
    init_ring(&queue->high_priority, high_capacity, high_events);
    init_ring(&queue->low_priority, low_capacity, low_events);
    
    // Assign unique queue ID
    queue->queue_id = g_next_queue_id++;
    queue->reserved = 0;
    
    return queue;
}

void cfl_clear_queue(CFL_EVENT_QUEUE_T *queue_control)
{
    if (queue_control == NULL) {
        return;
    }
    
    ring_clear(&queue_control->high_priority);
    ring_clear(&queue_control->low_priority);
}

bool cfl_send_event(
    CFL_EVENT_QUEUE_T *queue_control,
    unsigned priority,
    unsigned node_id,
    unsigned event_type,
    bool malloc_flag,
    unsigned event_id,
    void *data)
{
    // Validate queue pointer
    if (queue_control == NULL) {
        return false;
    }
    
    // Select ring based on priority
    CFL_EVENT_RING_T* ring;
    if (priority == CFL_EVENT_PRIORITY_HIGH) {
        ring = &queue_control->high_priority;
    } else {
        ring = &queue_control->low_priority;
    }
    
    // Check if ring is full
    if (ring_is_full(ring)) {
        return false;
    }
    
    // Build event structure
    CFL_EVENT_DATA_T event;
    event.node_id = (uint16_t)node_id;
    event.event_type = (uint8_t)event_type;
    event.flags = malloc_flag ? CFL_EVENT_MALLOC_FLAG : 0;
    event.event_id = (uint16_t)event_id;
    event.queue_number = queue_control->queue_id;
    event.data.ptr = data;  // Union assignment - works for all types
    
    // Insert into ring
    ring_push(ring, &event);
    
    return true;
}

/*==============================================================================
 * Type-Specific Helper Function Implementations
 *============================================================================*/

 bool cfl_send_unsigned_event(
    CFL_EVENT_QUEUE_T *queue_control,
    unsigned priority,
    unsigned node_id,
    unsigned event_id,
    cfl_size_t value)
{
    // Cast unsigned value to void* for union storage
    // This works because the union interprets the bits appropriately
    void* data_ptr;
    CFL_EVENT_VALUE_T temp;
    temp.unsigned_val = value;
    data_ptr = temp.ptr;  // Reinterpret bits as pointer
    
    return cfl_send_event(
        queue_control,
        priority,
        node_id,
        CFL_EVENT_TYPE_UINT,
        false,  // Never malloc for scalar values
        event_id,
        data_ptr);
}

bool cfl_send_integer_event(
    CFL_EVENT_QUEUE_T *queue_control,
    unsigned priority,
    unsigned node_id,
    unsigned event_id,
    cfl_int_t value)
{
    // Cast signed integer to void* for union storage
    void* data_ptr;
    CFL_EVENT_VALUE_T temp;
    temp.integer = value;
    data_ptr = temp.ptr;  // Reinterpret bits as pointer
    
    return cfl_send_event(
        queue_control,
        priority,
        node_id,
        CFL_EVENT_TYPE_INT,
        false,  // Never malloc for scalar values
        event_id,
        data_ptr);
}

bool cfl_send_float_event(
    CFL_EVENT_QUEUE_T *queue_control,
    unsigned priority,
    unsigned node_id,
    unsigned event_id,
    cfl_float_t value)
{
    // Cast float/double to void* for union storage
    void* data_ptr;
    CFL_EVENT_VALUE_T temp;
    temp.floating = value;
    data_ptr = temp.ptr;  // Reinterpret bits as pointer
    
    return cfl_send_event(
        queue_control,
        priority,
        node_id,
        CFL_EVENT_TYPE_FLOAT,
        false,  // Never malloc for scalar values
        event_id,
        data_ptr);
}

bool cfl_send_data_event(
    CFL_EVENT_QUEUE_T *queue_control,
    unsigned priority,
    unsigned node_id,
    bool malloc_flag,
    unsigned event_id,
    void *data)
{
    return cfl_send_event(
        queue_control,
        priority,
        node_id,
        CFL_EVENT_TYPE_PTR,
        malloc_flag,
        event_id,
        data);
}

bool cfl_pop_event(
    CFL_EVENT_QUEUE_T *queue_control,
    CFL_EVENT_DATA_T *event_data)
{
    // Validate pointers
    if (queue_control == NULL || event_data == NULL) {
        return false;
    }
    
    // Check high priority first
    if (!ring_is_empty(&queue_control->high_priority)) {
        ring_pop(&queue_control->high_priority, event_data);
        return true;
    }
    
    // Check low priority
    if (!ring_is_empty(&queue_control->low_priority)) {
        ring_pop(&queue_control->low_priority, event_data);
        return true;
    }
    
    // Both queues empty
    return false;
}

unsigned cfl_queue_number(CFL_EVENT_DATA_T *event_data)
{
    if (event_data == NULL) {
        return 0;
    }
    
    return event_data->queue_number;
}

unsigned cfl_high_priority_count(CFL_EVENT_QUEUE_T *queue_control)
{
    if (queue_control == NULL) {
        return 0;
    }
    
    return ring_count(&queue_control->high_priority);
}

unsigned cfl_low_priority_count(CFL_EVENT_QUEUE_T *queue_control)
{
    if (queue_control == NULL) {
        return 0;
    }
    
    return ring_count(&queue_control->low_priority);
}

unsigned cfl_total_event_count(CFL_EVENT_QUEUE_T *queue_control)
{
    if (queue_control == NULL) {
        return 0;
    }
    
    return ring_count(&queue_control->high_priority) + 
           ring_count(&queue_control->low_priority);
}
