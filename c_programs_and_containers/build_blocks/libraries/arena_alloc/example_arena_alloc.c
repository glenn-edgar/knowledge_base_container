#include "arena_alloc.h"
#include <stdio.h>
#include <string.h>
#include <stdlib.h>  // For malloc/free

// Example structure
typedef struct {
    uint32_t id;
    float temperature;
    uint32_t next_offset;  // Linked list using offsets!
} SensorData;

// Hash function for demonstration
uint32_t hash(const char* str) {
    uint32_t h = 2166136261u;
    while (*str) {
        h ^= (uint8_t)*str++;
        h *= 16777619u;
    }
    return h;
}

// Helper function moved OUTSIDE (no nesting)
static void generic_fill(void* self, uint32_t offset, uint8_t value, uint32_t size) {
    ArenaAllocator* arena = (ArenaAllocator*)self;
    void* ptr = arena->vtable->to_ptr(self, offset);
    if (ptr) {
        memset(ptr, value, size);
        printf("Filled %u bytes with 0x%02X at offset %u\n", 
               size, value, offset);
    }
}

void example_basic_usage(void) {
    printf("=== Basic Arena Usage ===\n\n");
    
    // Create arena
    ArenaAllocator* arena = arena_create(4096);
    
    // Allocate using vtable (OOP style)
    uint32_t offset1 = ARENA_ALLOC(arena, 100);
    printf("Allocated 100 bytes at offset: %u\n", offset1);
    
    // Convert to pointer
    void* ptr1 = ARENA_TO_PTR(arena, offset1);
    printf("Pointer: %p\n", ptr1);
    
    // Check alignment
    if ((uintptr_t)ptr1 % 4 == 0) {
        printf("✓ Pointer is 4-byte aligned\n");
    }
    
    // Allocate more
    uint32_t offset2 = arena->vtable->alloc((void*)arena, 200);
    printf("Allocated 200 bytes at offset: %u\n", offset2);
    
    // Stats
    uint32_t used, capacity;
    arena->vtable->stats((void*)arena, &used, &capacity);
    printf("Used: %u / %u bytes (%.1f%%)\n", 
           used, capacity, 100.0 * used / capacity);
    
    // Reset
    ARENA_RESET(arena);
    arena->vtable->stats((void*)arena, &used, &capacity);
    printf("After reset: %u / %u bytes\n", used, capacity);
    
    // Cleanup
    ARENA_DESTROY(arena);
    printf("\n");
}

void example_typed_allocation(void) {
    printf("=== Typed Allocation ===\n\n");
    
    ArenaAllocator* arena = arena_create(4096);
    
    // Allocate single structure
    uint32_t sensor1_offset = ARENA_ALLOC_TYPE(arena, SensorData);
    SensorData* sensor1 = ARENA_GET(arena, SensorData, sensor1_offset);
    
    sensor1->id = 1;
    sensor1->temperature = 25.5f;
    sensor1->next_offset = ARENA_OFFSET_NULL;
    
    printf("Sensor 1 at offset %u: id=%u, temp=%.1f\n",
           sensor1_offset, sensor1->id, sensor1->temperature);
    
    // Allocate array
    uint32_t array_offset = ARENA_ALLOC_ARRAY(arena, int32_t, 10);
    int32_t* array = ARENA_GET(arena, int32_t, array_offset);
    
    for (int i = 0; i < 10; i++) {
        array[i] = i * 10;
    }
    
    printf("Array at offset %u: [", array_offset);
    for (int i = 0; i < 10; i++) {
        printf("%d ", array[i]);
    }
    printf("]\n");
    
    ARENA_DESTROY(arena);
    printf("\n");
}

void example_linked_list_with_offsets(void) {
    printf("=== Linked List with Offsets ===\n\n");
    
    ArenaAllocator* arena = arena_create(4096);
    
    // Build linked list using offsets (position-independent!)
    uint32_t head_offset = ARENA_OFFSET_NULL;
    uint32_t prev_offset = ARENA_OFFSET_NULL;
    
    for (int i = 0; i < 5; i++) {
        uint32_t offset = ARENA_ALLOC_TYPE(arena, SensorData);
        SensorData* node = ARENA_GET(arena, SensorData, offset);
        
        node->id = i + 1;
        node->temperature = 20.0f + i;
        node->next_offset = ARENA_OFFSET_NULL;
        
        if (head_offset == ARENA_OFFSET_NULL) {
            head_offset = offset;
        }
        
        if (prev_offset != ARENA_OFFSET_NULL) {
            SensorData* prev = ARENA_GET(arena, SensorData, prev_offset);
            prev->next_offset = offset;
        }
        
        prev_offset = offset;
    }
    
    // Traverse list using offsets
    printf("Linked list:\n");
    uint32_t current = head_offset;
    while (current != ARENA_OFFSET_NULL) {
        SensorData* node = ARENA_GET(arena, SensorData, current);
        printf("  Node at offset %u: id=%u, temp=%.1f\n",
               current, node->id, node->temperature);
        current = node->next_offset;
    }
    
    // Save arena to buffer (serializable!)
    uint32_t used, capacity;
    arena_stats(arena, &used, &capacity);
    
    uint8_t* saved = (uint8_t*)malloc(used);
    memcpy(saved, arena->buffer, used);
    printf("\nSerialized %u bytes\n", used);
    
    // Create new arena from saved buffer
    ArenaAllocator* arena2 = arena_create_from_buffer(saved, used);
    
    // Traverse in new arena - offsets still work!
    printf("\nTraversing in new arena (different memory location):\n");
    current = head_offset;
    while (current != ARENA_OFFSET_NULL) {
        SensorData* node = ARENA_GET(arena2, SensorData, current);
        printf("  Node at offset %u: id=%u, temp=%.1f\n",
               current, node->id, node->temperature);
        current = node->next_offset;
    }
    
    arena_destroy(arena);
    arena_destroy(arena2);
    free(saved);
    printf("\n");
}

void example_alignment(void) {
    printf("=== ARM 4-Byte Alignment ===\n\n");
    
    ArenaAllocator* arena = arena_create(4096);
    
    // Allocate various sizes
    uint32_t sizes[] = {1, 2, 3, 4, 5, 7, 8, 15, 16, 17};
    
    for (int i = 0; i < 10; i++) {
        uint32_t offset = ARENA_ALLOC(arena, sizes[i]);
        void* ptr = ARENA_TO_PTR(arena, offset);
        
        printf("Requested %2u bytes -> offset=%4u, ptr=%p, aligned=%s\n",
               sizes[i], offset, ptr,
               ((uintptr_t)ptr & 3) == 0 ? "✓ YES" : "✗ NO");
    }
    
    ARENA_DESTROY(arena);
    printf("\n");
}

void example_static_buffer(void) {
    printf("=== Static Buffer (No malloc) ===\n\n");
    
    // Static buffer (could be in .bss section)
    static uint8_t static_buffer[2048] __attribute__((aligned(4)));
    
    // Create arena from static buffer
    ArenaAllocator* arena = arena_create_from_buffer(static_buffer, 
                                                     sizeof(static_buffer));
    
    // Use normally
    uint32_t offset = ARENA_ALLOC(arena, 128);
    printf("Allocated 128 bytes at offset: %u\n", offset);
    
    uint32_t used, capacity;
    arena_stats(arena, &used, &capacity);
    printf("Arena in static buffer: %u / %u bytes used\n", used, capacity);
    
    // Note: destroy won't free buffer since we don't own it
    arena_destroy(arena);
    printf("\n");
}

void example_polymorphism(void) {
    printf("=== Polymorphism via Void Pointer ===\n\n");
    
    // Function is now at file scope (not nested)
    
    ArenaAllocator* arena1 = arena_create(1024);
    ArenaAllocator* arena2 = arena_create(2048);
    
    uint32_t off1 = ARENA_ALLOC(arena1, 100);
    uint32_t off2 = ARENA_ALLOC(arena2, 100);
    
    // Same function works with different arenas
    generic_fill((void*)arena1, off1, 0xAA, 100);
    generic_fill((void*)arena2, off2, 0xBB, 100);
    
    ARENA_DESTROY(arena1);
    ARENA_DESTROY(arena2);
    printf("\n");
}

void example_custom_alignment(void) {
    printf("=== Custom Alignment ===\n\n");
    
    ArenaAllocator* arena = arena_create(4096);
    
    // Allocate with different alignments
    uint32_t off1 = arena_alloc_aligned(arena, 10, 4);   // 4-byte aligned
    uint32_t off2 = arena_alloc_aligned(arena, 10, 8);   // 8-byte aligned
    uint32_t off3 = arena_alloc_aligned(arena, 10, 16);  // 16-byte aligned
    uint32_t off4 = arena_alloc_aligned(arena, 10, 32);  // 32-byte aligned
    
    void* ptr1 = ARENA_TO_PTR(arena, off1);
    void* ptr2 = ARENA_TO_PTR(arena, off2);
    void* ptr3 = ARENA_TO_PTR(arena, off3);
    void* ptr4 = ARENA_TO_PTR(arena, off4);
    
    printf("4-byte aligned:  offset=%u, ptr=%p, aligned=%s\n",
           off1, ptr1, ((uintptr_t)ptr1 & 3) == 0 ? "✓" : "✗");
    printf("8-byte aligned:  offset=%u, ptr=%p, aligned=%s\n",
           off2, ptr2, ((uintptr_t)ptr2 & 7) == 0 ? "✓" : "✗");
    printf("16-byte aligned: offset=%u, ptr=%p, aligned=%s\n",
           off3, ptr3, ((uintptr_t)ptr3 & 15) == 0 ? "✓" : "✗");
    printf("32-byte aligned: offset=%u, ptr=%p, aligned=%s\n",
           off4, ptr4, ((uintptr_t)ptr4 & 31) == 0 ? "✓" : "✗");
    
    ARENA_DESTROY(arena);
    printf("\n");
}

void example_offset_validation(void) {
    printf("=== Offset Validation ===\n\n");
    
    ArenaAllocator* arena = arena_create(1024);
    
    uint32_t valid_offset = ARENA_ALLOC(arena, 100);
    uint32_t invalid_offset = 5000;  // Beyond arena capacity
    
    printf("Valid offset %u: %s\n", 
           valid_offset, 
           arena_is_valid(arena, valid_offset) ? "✓ Valid" : "✗ Invalid");
    
    printf("Invalid offset %u: %s\n",
           invalid_offset,
           arena_is_valid(arena, invalid_offset) ? "✓ Valid" : "✗ Invalid");
    
    printf("NULL offset: %s\n",
           arena_is_valid(arena, ARENA_OFFSET_NULL) ? "✓ Valid" : "✗ Invalid");
    
    ARENA_DESTROY(arena);
    printf("\n");
}

int main(void) {
    printf("\n");
    printf("╔════════════════════════════════════════════════════════╗\n");
    printf("║      Arena Allocator - Example Demonstrations         ║\n");
    printf("╚════════════════════════════════════════════════════════╝\n");
    printf("\n");
    
    example_basic_usage();
    example_typed_allocation();
    example_alignment();
    example_custom_alignment();
    example_linked_list_with_offsets();
    example_static_buffer();
    example_polymorphism();
    example_offset_validation();
    
    printf("╔════════════════════════════════════════════════════════╗\n");
    printf("║              All Examples Completed!                  ║\n");
    printf("╚════════════════════════════════════════════════════════╝\n");
    printf("\n");
    
    return 0;
}

