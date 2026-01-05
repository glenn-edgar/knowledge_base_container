/**
 * @file cfl_event_queue.c
 * @brief Implementation of priority event queue system with  support
 */

 #include "cfl_event_queue.h"
 #include "cfl_exception.h"
 #include <string.h>  // For memset
 #include <stdlib.h>  // For free()
 #include <stdio.h> 
 #include <stdint.h> // For printf()
 /*==============================================================================
  * Global Variables
  *============================================================================*/
 
 /** Next queue ID to assign (increments for each created queue) */
 static uint16_t g_next_queue_id = 0;
 
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
 static void init_ring( CFL_EVENT_RING_T* ring, uint16_t capacity, CFL_EVENT_DATA_T* events)
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
 static inline bool ring_is_full( const CFL_EVENT_RING_T* ring)
 {
     // Read  values once to avoid race conditions
     uint16_t head = ring->head;
     uint16_t tail = ring->tail;
     uint16_t mask = ring->mask;
     
     return ((head + 1) & mask) == tail;
 }
 
 /**
  * @brief Check if ring buffer is empty
  * 
  * @param ring Ring buffer to check
  * @return true if empty, false otherwise
  */
 static inline bool ring_is_empty( const CFL_EVENT_RING_T* ring)
 {
     // Read  values once
     uint16_t head = ring->head;
     uint16_t tail = ring->tail;
     
     return head == tail;
 }
 
 /**
  * @brief Get number of events in ring buffer
  * 
  * @param ring Ring buffer to query
  * @return Number of events currently in ring
  */
 static inline uint16_t ring_count( const CFL_EVENT_RING_T* ring)
 {
     // Read  values once
     uint16_t head = ring->head;
     uint16_t tail = ring->tail;
     uint16_t mask = ring->mask;
     
     return (head - tail) & mask;
 }
 
 /**
  * @brief Insert event into ring buffer (assumes not full)
  * 
  * @param ring Ring buffer
  * @param event Event to insert
  */
 static inline void ring_push( CFL_EVENT_RING_T* ring, const CFL_EVENT_DATA_T* event)
 {
     // Read head once
     uint16_t head = ring->head;
     uint16_t mask = ring->mask;
     CFL_EVENT_DATA_T* events = (CFL_EVENT_DATA_T*)ring->events;  // Cast away  for array access
     
     // Copy event data
     events[head] = *event;
     
     // Update head - this write is atomic for uint16_t
     ring->head = (head + 1) & mask;
 }
 
 /**
  * @brief Remove event from ring buffer (assumes not empty)
  * 
  * @param ring Ring buffer
  * @param event Pointer to receive event data
  */
 static inline void ring_pop( CFL_EVENT_RING_T* ring,  CFL_EVENT_DATA_T* event)
 {
     // Read tail once
     uint16_t tail = ring->tail;
     uint16_t mask = ring->mask;
     CFL_EVENT_DATA_T* events = (CFL_EVENT_DATA_T*)ring->events;  // Cast away  for array access
     
     // Copy event data - copy through local to handle  destination
     CFL_EVENT_DATA_T local_event = events[tail];
     
     // Copy to potentially  destination
     ((CFL_EVENT_DATA_T*)event)->node_id = local_event.node_id;
     ((CFL_EVENT_DATA_T*)event)->event_type = local_event.event_type;
     ((CFL_EVENT_DATA_T*)event)->flags = local_event.flags;
     ((CFL_EVENT_DATA_T*)event)->event_id = local_event.event_id;
     ((CFL_EVENT_DATA_T*)event)->queue_number = local_event.queue_number;
     ((CFL_EVENT_DATA_T*)event)->data = local_event.data;
     
     // Update tail - this write is atomic for uint16_t
     ring->tail = (tail + 1) & mask;
 }
 
 /**
  * @brief Peek at event in ring buffer without removing (assumes not empty)
  * 
  * @param ring Ring buffer
  * @param event Pointer to receive event data
  */
 static inline void ring_peek( const CFL_EVENT_RING_T* ring,  CFL_EVENT_DATA_T* event)
 {
     // Read tail once
     uint16_t tail = ring->tail;
     CFL_EVENT_DATA_T* events = (CFL_EVENT_DATA_T*)ring->events;  // Cast away  for array access
     
     // Copy event data through local
     CFL_EVENT_DATA_T local_event = events[tail];
     
     // Copy to potentially  destination
     ((CFL_EVENT_DATA_T*)event)->node_id = local_event.node_id;
     ((CFL_EVENT_DATA_T*)event)->event_type = local_event.event_type;
     ((CFL_EVENT_DATA_T*)event)->flags = local_event.flags;
     ((CFL_EVENT_DATA_T*)event)->event_id = local_event.event_id;
     ((CFL_EVENT_DATA_T*)event)->queue_number = local_event.queue_number;
     ((CFL_EVENT_DATA_T*)event)->data = local_event.data;
 }
 
/**
 * @brief Clear ring buffer, freeing any malloc'd event data
 * 
 * @param ring Ring buffer to clear
 */
 static void ring_clear( CFL_EVENT_RING_T* ring)
 {
     // Process and free any malloc'd events before clearing
     CFL_EVENT_DATA_T event;  // Local non- for processing
     
     while (!ring_is_empty(ring)) {
         ring_pop(ring, ( CFL_EVENT_DATA_T*)&event);
         
         // Check if event has malloc flag set
         if (event.flags & CFL_EVENT_MALLOC_FLAG) {
             // Validate that malloc flag is only used with pointer types
             if (event.event_type != CFL_EVENT_TYPE_PTR) {
                 EXCEPTION("ring_clear: malloc_flag set on non-pointer event type");
             }
             
             // Free the allocated memory if pointer is non-null
             
             // Note: NULL pointer with malloc flag is allowed - may occur
             // if pointer was already freed or intentionally nulled
         }
     }
 }

 /**
  * @brief Update queue depth statistics
  * 
  * @param queue Queue control structure
  */
 static inline void update_queue_stats( CFL_EVENT_QUEUE_T* queue)
 {
     // Update high priority depth
     uint16_t high_depth = ring_count(&queue->high_priority);
     uint16_t max_high = queue->max_high_depth;
     if (high_depth > max_high) {
         queue->max_high_depth = high_depth;
     }
     
     // Update total depth
     uint16_t total_depth = high_depth + ring_count(&queue->low_priority);
     uint16_t max_total = queue->max_total_depth;
     if (total_depth > max_total) {
         queue->max_total_depth = total_depth;
     }
 }
 
 /*==============================================================================
  * Public Function Implementations
  *============================================================================*/
 
  CFL_EVENT_QUEUE_T* cfl_create_event_queue(
     unsigned high_priority_size,
     unsigned low_priority_size,
     CflPerm* perm)
 {
     // Validate perm pointer
     if (perm == NULL) {
         EXCEPTION("cfl_create_event_queue: NULL perm pointer");
     }
     
     if (!perm->initialized) {
         EXCEPTION("cfl_create_event_queue: Perm allocator not initialized");
     }
     
     // Round up to powers of 2
     uint16_t high_capacity = round_up_power_of_2(high_priority_size);
     uint16_t low_capacity = round_up_power_of_2(low_priority_size);
     
     // Calculate allocation sizes
     uint16_t control_size = sizeof(CFL_EVENT_QUEUE_T);
     uint16_t high_array_size = high_capacity * sizeof(CFL_EVENT_DATA_T);
     uint16_t low_array_size = low_capacity * sizeof(CFL_EVENT_DATA_T);
     
     // Check if we have enough space before allocating
     uint16_t total_needed = control_size + high_array_size + low_array_size;
     if (cfl_perm_free_bytes(perm) < total_needed) {
         EXCEPTION("cfl_create_event_queue: Insufficient memory in perm allocator");
     }
     
     // Allocate control structure (use default PERM_ALIGNMENT)
     CFL_EVENT_QUEUE_T* queue = (CFL_EVENT_QUEUE_T*)cfl_perm_alloc_pointer(perm, control_size);
     if (queue == NULL) {
         EXCEPTION("cfl_create_event_queue: Failed to allocate control structure");
     }
     
     // Allocate high priority event array
     CFL_EVENT_DATA_T* high_events = (CFL_EVENT_DATA_T*)cfl_perm_alloc_pointer(perm, high_array_size);
     if (high_events == NULL) {
         EXCEPTION("cfl_create_event_queue: Failed to allocate high priority array");
     }
     
     // Allocate low priority event array
     CFL_EVENT_DATA_T* low_events = (CFL_EVENT_DATA_T*)cfl_perm_alloc_pointer(perm, low_array_size);
     if (low_events == NULL) {
         EXCEPTION("cfl_create_event_queue: Failed to allocate low priority array");
     }
     
     // Initialize control structure
     memset(queue, 0, sizeof(CFL_EVENT_QUEUE_T));
     
     // Initialize ring buffers - cast to  for init_ring
      CFL_EVENT_QUEUE_T* vqueue = ( CFL_EVENT_QUEUE_T*)queue;
     init_ring(&vqueue->high_priority, high_capacity, high_events);
     init_ring(&vqueue->low_priority, low_capacity, low_events);
     
     // Assign unique queue ID
     queue->queue_id = g_next_queue_id++;
     
     // Initialize statistics
     queue->max_total_depth = 0;
     queue->max_high_depth = 0;
     queue->reserved = 0;
     
     // Return as  pointer for runtime handle
     return vqueue;
 }
 
 void cfl_clear_queue( CFL_EVENT_QUEUE_T *queue_control)
 {
     if (queue_control == NULL) {
         EXCEPTION("cfl_clear_queue: NULL queue_control pointer");
     }
     
     ring_clear(( CFL_EVENT_RING_T*)&queue_control->high_priority);
     ring_clear(( CFL_EVENT_RING_T*)&queue_control->low_priority);
     
     // Note: Statistics are preserved across clear operations
     // Use cfl_reset_queue_stats() to clear statistics
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
         EXCEPTION("cfl_send_event: NULL queue_control pointer");
     }
     
     // Validate priority level
     if (priority != CFL_EVENT_PRIORITY_LOW && priority != CFL_EVENT_PRIORITY_HIGH) {
         EXCEPTION("cfl_send_event: Invalid priority level");
     }

     // Validate event type
     if (event_type > CFL_EVENT_TYPE_NULL) {
         EXCEPTION("cfl_send_event: Invalid event_type");
     }
     
     // Select ring based on priority
      CFL_EVENT_RING_T* ring;
     if (priority == CFL_EVENT_PRIORITY_HIGH) {
        
         ring = ( CFL_EVENT_RING_T*)&queue_control->high_priority;
         
     } else {

         ring = ( CFL_EVENT_RING_T*)&queue_control->low_priority;
         
     }
     
     // Check if ring is full
     if (ring_is_full(ring)) {
         EXCEPTION("cfl_send_event: ring is full");  // Not an error - caller should handle full queue
     }
     
     // Build event structure (local non-)
     CFL_EVENT_DATA_T event;
     event.node_id = (uint16_t)node_id;
     event.event_type = (uint8_t)event_type;
     event.flags = malloc_flag ? CFL_EVENT_MALLOC_FLAG : 0;
     event.event_id = (uint16_t)event_id;
     event.queue_number = queue_control->queue_id;  // Read from 
     event.data.ptr = data;  // Union assignment - works for all types
     
     // Insert into ring
     
     ring_push(ring, &event);
     
     // Update statistics
     update_queue_stats(( CFL_EVENT_QUEUE_T*)queue_control);
     
     return true;
 }
 
 bool cfl_pop_event(
      CFL_EVENT_QUEUE_T *queue_control,
      CFL_EVENT_DATA_T *event_data)
 {
     // Validate pointers
     if (queue_control == NULL) {
         EXCEPTION("cfl_pop_event: NULL queue_control pointer");
     }
     
     if (event_data == NULL) {
         EXCEPTION("cfl_pop_event: NULL event_data pointer");
     }
     
     // Check high priority first
     if (!ring_is_empty(&queue_control->high_priority)) {
        
         ring_pop(( CFL_EVENT_RING_T*)&queue_control->high_priority, event_data);
        
         return true;
     }
     
     // Check low priority
     if (!ring_is_empty(&queue_control->low_priority)) {
         ring_pop(( CFL_EVENT_RING_T*)&queue_control->low_priority, event_data);
        
         return true;
     }
     
     // Both queues empty - not an error
     return false;
 }
 
 bool cfl_peek_event(
      CFL_EVENT_QUEUE_T *queue_control,
      CFL_EVENT_DATA_T *event_data)
 {
     // Validate pointers
     if (queue_control == NULL) {
         EXCEPTION("cfl_peek_event: NULL queue_control pointer");
     }
     
     if (event_data == NULL) {
         EXCEPTION("cfl_peek_event: NULL event_data pointer");
     }
     
     // Check high priority first
     if (!ring_is_empty(&queue_control->high_priority)) {
         
         ring_peek(&queue_control->high_priority, event_data);
         return true;
     }
     
     // Check low priority
     if (!ring_is_empty(&queue_control->low_priority)) {
         ring_peek(&queue_control->low_priority, event_data);
         return true;
     }
     
     // Both queues empty - not an error
     return false;
 }
 
 unsigned cfl_queue_number( CFL_EVENT_DATA_T *event_data)
 {
     if (event_data == NULL) {
         EXCEPTION("cfl_queue_number: NULL event_data pointer");
     }
     
     return event_data->queue_number;
 }
 
 unsigned cfl_high_priority_count( CFL_EVENT_QUEUE_T *queue_control)
 {
     if (queue_control == NULL) {
         EXCEPTION("cfl_high_priority_count: NULL queue_control pointer");
     }
     
     return ring_count(&queue_control->high_priority);
 }
 
 unsigned cfl_low_priority_count( CFL_EVENT_QUEUE_T *queue_control)
 {
     if (queue_control == NULL) {
         EXCEPTION("cfl_low_priority_count: NULL queue_control pointer");
     }
     
     return ring_count(&queue_control->low_priority);
 }
 
 unsigned cfl_total_event_count( CFL_EVENT_QUEUE_T *queue_control)
 {
     if (queue_control == NULL) {
         EXCEPTION("cfl_total_event_count: NULL queue_control pointer");
     }
     
     return ring_count(&queue_control->high_priority) + 
            ring_count(&queue_control->low_priority);
 }
 
 unsigned cfl_get_max_total_depth( CFL_EVENT_QUEUE_T *queue_control)
 {
     if (queue_control == NULL) {
         EXCEPTION("cfl_get_max_total_depth: NULL queue_control pointer");
     }
     
     return queue_control->max_total_depth;
 }
 
 unsigned cfl_get_max_high_depth( CFL_EVENT_QUEUE_T *queue_control)
 {
     if (queue_control == NULL) {
         EXCEPTION("cfl_get_max_high_depth: NULL queue_control pointer");
     }
     
     return queue_control->max_high_depth;
 }
 
 void cfl_reset_queue_stats( CFL_EVENT_QUEUE_T *queue_control)
 {
     if (queue_control == NULL) {
         EXCEPTION("cfl_reset_queue_stats: NULL queue_control pointer");
     }
     
     queue_control->max_total_depth = 0;
     queue_control->max_high_depth = 0;
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

bool cfl_send_null_event(
     CFL_EVENT_QUEUE_T *queue_control,
    unsigned priority,
    unsigned node_id,
    unsigned event_id)
 {
     return cfl_send_event(
         queue_control,
         priority,
         node_id,
         CFL_EVENT_TYPE_NULL,
         false,
         event_id,
         NULL);
 }

 bool cfl_send_json_record_event(
     CFL_EVENT_QUEUE_T *queue_control,
    unsigned priority,
    unsigned node_id,
    unsigned event_id,
    uint32_t record_index)
 {
    void* data_ptr;
     CFL_EVENT_VALUE_T temp;
     temp.unsigned_val = record_index;
     data_ptr = temp.ptr;  
     return cfl_send_event(
         queue_control,
         priority,
         node_id,
         CFL_EVENT_TYPE_JSON_RECORD,
         false,
         event_id,
         data_ptr);
 }

 bool cfl_send_node_id_event(
    CFL_EVENT_QUEUE_T *queue_control,
   unsigned priority,
   unsigned node_id,
   unsigned event_id,
   unsigned node_index)
 {
     void* data_ptr;
     CFL_EVENT_VALUE_T temp;
     temp.unsigned_val = node_index;
     data_ptr = temp.ptr;  
     return cfl_send_event(
         queue_control,
         priority,
         node_id,
         CFL_EVENT_TYPE_NODE_ID,
         false,
         event_id,
         data_ptr);
 }

bool cfl_send_streaming_data_event(
     CFL_EVENT_QUEUE_T *queue_control,
    unsigned priority,
    unsigned node_id,
    unsigned event_id,
    void *data)
 {
     return cfl_send_event(
         queue_control,
         priority,
         node_id,
         CFL_EVENT_TYPE_STREAMING_DATA,
         false,
         event_id,
         data);
 }

bool cfl_send_streaming_collected_packets_event(
    CFL_EVENT_QUEUE_T *queue_control,
   unsigned priority,
   unsigned node_id,
   unsigned event_id,
   void *data)
{
    return cfl_send_event(
        queue_control,
        priority,
        node_id,
        CFL_EVENT_TYPE_STREAMING_COLLECTED_PACKETS,
        false,
        event_id,
        data);
}


