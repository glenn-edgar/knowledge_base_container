/**
 * @file arena_alloc.h
 * @brief Offset-based arena allocator with virtual method table
 * 
 * Compatible with both C and C++ compilation.
 */

 #ifndef ARENA_ALLOC_H
 #define ARENA_ALLOC_H
 
 #include <stdint.h>
 #include <stddef.h>
 #include <stdbool.h>
 
 #ifdef __cplusplus
 extern "C" {
 #endif
 
 // ARM requires 4-byte alignment
 #define ARENA_ALIGNMENT 4
 #define ARENA_ALIGN(x) (((x) + (ARENA_ALIGNMENT - 1)) & ~(ARENA_ALIGNMENT - 1))
 
 // Special offset values
 #define ARENA_OFFSET_NULL 0xFFFFFFFF
 #define ARENA_OFFSET_INVALID 0xFFFFFFFE
 
 // Forward declaration
 typedef struct ArenaAllocator ArenaAllocator;
 
 // Virtual method table (function pointers)
 typedef struct ArenaVTable {
     // Allocate memory, returns offset
     uint32_t (*alloc)(void* self, uint32_t size);
     
     // Allocate aligned memory
     uint32_t (*alloc_aligned)(void* self, uint32_t size, uint32_t alignment);
     
     // Convert offset to pointer
     void* (*to_ptr)(void* self, uint32_t offset);
     
     // Convert pointer to offset
     uint32_t (*to_offset)(void* self, const void* ptr);
     
     // Reset arena (free all)
     void (*reset)(void* self);
     
     // Get memory usage stats
     void (*stats)(void* self, uint32_t* used, uint32_t* capacity);
     
     // Check if offset is valid
     bool (*is_valid)(void* self, uint32_t offset);
     
     // Destroy arena
     void (*destroy)(void* self);
 } ArenaVTable;
 
 // Arena allocator "class"
 struct ArenaAllocator {
     // Virtual table
     const ArenaVTable* vtable;
     
     // Instance data
     uint8_t* buffer;        // Base address of arena
     uint32_t capacity;      // Total size in bytes
     uint32_t used;          // Bytes allocated
     bool owns_buffer;       // Whether we allocated the buffer
 };
 
 // Constructor - allocates buffer
 ArenaAllocator* arena_create(uint32_t capacity);
 
 // Constructor - uses provided buffer
 ArenaAllocator* arena_create_from_buffer(void* buffer, uint32_t capacity);
 
 // Destructor
 void arena_destroy(void* self);
 
 // Virtual methods (can be called directly or through vtable)
 uint32_t arena_alloc(void* self, uint32_t size);
 uint32_t arena_alloc_aligned(void* self, uint32_t size, uint32_t alignment);
 void* arena_to_ptr(void* self, uint32_t offset);
 uint32_t arena_to_offset(void* self, const void* ptr);
 void arena_reset(void* self);
 void arena_stats(void* self, uint32_t* used, uint32_t* capacity);
 bool arena_is_valid(void* self, uint32_t offset);
 
 #ifdef __cplusplus
 } // extern "C"
 #endif
 
 // Helper macros for OOP-style syntax (outside extern "C" block)
 #define ARENA_ALLOC(arena, size) \
     ((arena)->vtable->alloc((void*)(arena), (size)))
 
 #define ARENA_TO_PTR(arena, offset) \
     ((arena)->vtable->to_ptr((void*)(arena), (offset)))
 
 #define ARENA_RESET(arena) \
     ((arena)->vtable->reset((void*)(arena)))
 
 #define ARENA_DESTROY(arena) \
     ((arena)->vtable->destroy((void*)(arena)))
 
 // Convenience macro for typed allocation
 #define ARENA_ALLOC_TYPE(arena, type) \
     ((uint32_t)ARENA_ALLOC((arena), sizeof(type)))
 
 #define ARENA_ALLOC_ARRAY(arena, type, count) \
     ((uint32_t)ARENA_ALLOC((arena), sizeof(type) * (count)))
 
 // Get typed pointer from offset
 #ifdef __cplusplus
 // C++ version with proper casting
 #define ARENA_GET(arena, type, offset) \
     (static_cast<type*>(ARENA_TO_PTR((arena), (offset))))
 #else
 // C version
 #define ARENA_GET(arena, type, offset) \
     ((type*)ARENA_TO_PTR((arena), (offset)))
 #endif
 
 #endif // ARENA_ALLOC_H