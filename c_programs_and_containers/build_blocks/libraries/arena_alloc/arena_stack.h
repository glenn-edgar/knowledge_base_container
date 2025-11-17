/**
 * @file arena_stack.h
 * @brief Arena-based memory allocator with block management
 * 
 * This header provides an interface for managing a fixed block of memory
 * with allocation, deallocation, and tracking capabilities.
 */

 #ifndef ARENA_STACK_H
 #define ARENA_STACK_H
 
 #include <stdint.h>
 #include <stdbool.h>
 /*
   designed for 32k RAM type systems
 
 */
 #ifdef __cplusplus
 extern "C" {
 #endif
 #define ARENA_STACK_INVALID_ID 0xFFFF
 #define ARENA_STACK_ERROR 0xFFFF
 /* Opaque handle to arena_stack instance */
 typedef struct arena_stack arena_stack;
 
 /**
  * @brief Create a new arena_stack instance
  * 
  * Creates and initializes an arena_stack handle. Must be destroyed
  * with arena_stack_destroy() when no longer needed.
  * 
  * @return Pointer to arena_stack handle, or NULL on failure
  */
 arena_stack* arena_stack_create(void);
 
 /**
  * @brief Initialize arena with a block of memory
  * 
  * Sets up the heap control record with the provided memory block.
  * Any previously initialized memory will be cleaned up.
  * 
  * @param self Pointer to arena_stack handle
  * @param memory Pointer to memory block to manage
  * @param total_size Size of the memory block in bytes
  * @return true on success, false on failure
  */
 bool arena_stack_initialize(arena_stack* self, void* memory, uint16_t total_size);
 
 /**
  * @brief Allocate a block of memory from the arena
  * 
  * Allocates a contiguous block of the specified size and assigns it
  * a unique block ID starting from 0 and incrementing.
  * 
  * @param self Pointer to arena_stack handle
  * @param size Size of block to allocate in bytes
  * @return Block ID (>= 0) on success, -1 on failure
  */
 uint16_t arena_stack_allocate(arena_stack* self, uint16_t size);
 
 /**
  * @brief Get the number of currently allocated blocks
  * 
  * Returns the count of blocks that are currently allocated
  * (not deallocated).
  * 
  * @param self Pointer to arena_stack handle
  * @return Number of allocated blocks, or 0 if self is NULL
  */
 uint16_t arena_stack_get_block_count(const arena_stack* self);
 
 /**
  * @brief Get the size of a specific block
  * 
  * Returns the size in bytes of the block with the given ID.
  * 
  * @param self Pointer to arena_stack handle
  * @param block_id ID of the block to query
  * @return Size of block in bytes, or 0 if block not found/not allocated
  */
 uint16_t arena_stack_get_block_size(const arena_stack* self, uint16_t block_id);
 
 /**
  * @brief Get remaining free space in the arena
  * 
  * Returns the number of bytes available for allocation.
  * 
  * @param self Pointer to arena_stack handle
  * @return Number of free bytes, or 0 if self is NULL
  */
 uint16_t arena_stack_get_remaining_space(const arena_stack* self);
 
 /**
  * @brief Deallocate/reclaim a memory block
  * 
  * Marks the specified block as deallocated. Note: This implementation
  * does not compact memory, so space is not immediately reusable.
  * 
  * @param self Pointer to arena_stack handle
  * @param block_id ID of the block to deallocate
  * @return true on success, false if block not found or already deallocated
  */
 bool arena_stack_deallocate(arena_stack* self, uint16_t block_id);
 
 /**
  * @brief Get pointer to block's memory
  * 
  * Returns a pointer to the actual memory for the specified block.
  * The pointer remains valid until the arena is destroyed or reinitialized.
  * 
  * @param self Pointer to arena_stack handle
  * @param block_id ID of the block
  * @return Pointer to block memory, or NULL if block not found/not allocated
  */
 void* arena_stack_get_block_pointer(const arena_stack* self, uint16_t block_id);
 
 /**
  * @brief Destroy arena_stack instance
  * 
  * Frees all internal resources associated with the arena_stack handle.
  * Does not free the managed memory block itself.
  * 
  * @param self Pointer to arena_stack handle (can be NULL)
  */
 void arena_stack_destroy(arena_stack* self);
 
 #ifdef __cplusplus
 }
 #endif
 
 #endif /* ARENA_STACK_H */