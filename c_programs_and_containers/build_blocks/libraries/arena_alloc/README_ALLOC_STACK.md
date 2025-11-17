# Arena Stack Memory Allocator

A lightweight, fixed-memory block allocator designed for embedded systems with limited RAM (typically 32KB systems). Provides deterministic memory allocation with block tracking and management.

## Features

- **Fixed Memory Pool**: Operates on a pre-allocated memory buffer
- **Block-based Allocation**: Allocates contiguous memory blocks with unique IDs
- **O(1) Allocation**: Constant-time allocation (no searching)
- **Block Tracking**: Query block sizes, count, and remaining space
- **Memory Efficient**: Optimized struct layouts for minimal overhead
- **C++ Compatible**: Header file works with both C and C++ compilers
- **16-bit Addressing**: Uses `uint16_t` for sizes (max 64KB memory pools)
- **Error Handling**: Boolean returns and error constants for robust code

## Design Goals

- **Embedded Systems**: Designed for resource-constrained environments
- **Predictability**: No dynamic allocation failures after initialization
- **Simplicity**: Straightforward API with clear semantics
- **Portability**: Standard C99, works on 32-bit and 64-bit systems

## File Structure

```
arena_stack/
├── arena_stack.h          # Public API header (C/C++ compatible)
├── arena_stack.c          # Implementation
├── example_arena_stack.c  # Usage examples and tests
├── build.sh               # Build script
└── README.md              # This file
```

## API Reference

### Types and Constants

```c
typedef struct arena_stack arena_stack;

#define ARENA_STACK_INVALID_ID 0xFFFF  // Error return value for allocations
#define ARENA_STACK_ERROR      0xFFFF  // General error constant
```

### Functions

#### `arena_stack* arena_stack_create(void)`
Creates a new arena_stack instance. Must be destroyed with `arena_stack_destroy()`.

**Returns:** Pointer to arena_stack handle, or NULL on failure

---

#### `bool arena_stack_initialize(arena_stack* self, void* memory, uint16_t total_size)`
Initializes the arena with a pre-allocated memory block.

**Parameters:**
- `self`: Pointer to arena_stack handle
- `memory`: Pointer to memory buffer to manage
- `total_size`: Size of the memory buffer in bytes

**Returns:** `true` on success, `false` on failure

---

#### `uint16_t arena_stack_allocate(arena_stack* self, uint16_t size)`
Allocates a block of memory and returns a unique block ID.

**Parameters:**
- `self`: Pointer to arena_stack handle
- `size`: Size of block to allocate in bytes

**Returns:** Block ID (0 to 65534) on success, `ARENA_STACK_INVALID_ID` on failure

---

#### `uint16_t arena_stack_get_block_count(const arena_stack* self)`
Returns the number of currently allocated blocks.

**Returns:** Number of allocated blocks

---

#### `uint16_t arena_stack_get_block_size(const arena_stack* self, uint16_t block_id)`
Returns the size of a specific block.

**Parameters:**
- `self`: Pointer to arena_stack handle
- `block_id`: ID of the block to query

**Returns:** Size in bytes, or 0 if block not found

---

#### `uint16_t arena_stack_get_remaining_space(const arena_stack* self)`
Returns the number of bytes available for allocation.

**Returns:** Number of free bytes

---

#### `bool arena_stack_deallocate(arena_stack* self, uint16_t block_id)`
Marks a block as deallocated. Note: Memory is not compacted.

**Parameters:**
- `self`: Pointer to arena_stack handle
- `block_id`: ID of the block to deallocate

**Returns:** `true` on success, `false` if block not found or already deallocated

---

#### `void* arena_stack_get_block_pointer(const arena_stack* self, uint16_t block_id)`
Returns a pointer to the block's memory.

**Parameters:**
- `self`: Pointer to arena_stack handle
- `block_id`: ID of the block

**Returns:** Pointer to block memory, or NULL if not found

---

#### `void arena_stack_destroy(arena_stack* self)`
Destroys the arena_stack instance and frees internal resources.

**Parameters:**
- `self`: Pointer to arena_stack handle (can be NULL)

## Usage Example

```c
#include "arena_stack.h"
#include <stdio.h>
#include <string.h>

int main(void) {
    // Create a 1KB memory buffer
    char memory_buffer[1024];
    
    // Create and initialize arena
    arena_stack* arena = arena_stack_create();
    if (!arena_stack_initialize(arena, memory_buffer, 1024)) {
        printf("Failed to initialize\n");
        return 1;
    }
    
    // Allocate some blocks
    uint16_t block1 = arena_stack_allocate(arena, 100);
    uint16_t block2 = arena_stack_allocate(arena, 200);
    
    if (block1 != ARENA_STACK_INVALID_ID) {
        // Get pointer to block memory
        void* ptr = arena_stack_get_block_pointer(arena, block1);
        if (ptr) {
            strcpy((char*)ptr, "Hello, Arena!");
        }
        
        // Query block size
        uint16_t size = arena_stack_get_block_size(arena, block1);
        printf("Block %u size: %u bytes\n", block1, size);
    }
    
    // Check remaining space
    printf("Remaining: %u bytes\n", arena_stack_get_remaining_space(arena));
    
    // Deallocate a block
    arena_stack_deallocate(arena, block1);
    
    // Cleanup
    arena_stack_destroy(arena);
    return 0;
}
```

## Building

### Using the build script:
```bash
chmod +x build.sh
./build.sh
./arena_example
```

### Manual compilation (C):
```bash
gcc -Wall -Wextra -std=c11 -O2 arena_stack.c example_arena_stack.c -o arena_example
```

### Manual compilation (C++):
```bash
g++ -Wall -Wextra -std=c++11 -O2 arena_stack.c example_arena_stack.c -o arena_example
```

## Memory Layout

### Block Header (8 bytes with padding)
```
┌──────────────┬──────────────┬──────────────┬──────────────────┐
│ size (2B)    │ block_id (2B)│ offset (2B)  │ allocated (1B+1) │
└──────────────┴──────────────┴──────────────┴──────────────────┘
```

### Arena Stack Structure (24 bytes on 64-bit)
```
┌──────────────────────┬──────────────────────┐
│ base_memory (8B)     │ blocks* (8B)         │
├──────────────────────┴──────────────────────┤
│ total_size | used_size | next_block_id      │
│ block_count | block_capacity | (padding)    │
└──────────────────────────────────────────────┘
```

## Important Notes

### Memory Model
- **No Compaction**: Deallocated blocks are marked as free but space is not reclaimed
- **Sequential Allocation**: Memory is allocated sequentially from the buffer
- **Block Limit**: Maximum 65534 blocks can be allocated (ID 0-65534, 65535 is error)
- **Size Limit**: Maximum arena size is 65535 bytes (uint16_t limitation)

### When to Use
✅ **Good For:**
- Embedded systems with fixed RAM
- Temporary scratch buffers
- Frame-based allocation (allocate, use, reset)
- Memory pools with known maximum size

❌ **Not Ideal For:**
- Long-running applications with many allocations/deallocations
- Applications requiring memory compaction
- Large memory pools (>64KB)
- Real-time systems requiring true O(1) deallocation

## Performance Characteristics

| Operation | Time Complexity | Notes |
|-----------|----------------|-------|
| Create | O(1) | Allocates handle |
| Initialize | O(1) | Allocates block array |
| Allocate | O(1) amortized | May resize block array |
| Get Block Count | O(n) | Linear scan |
| Get Block Size | O(n) | Linear scan |
| Get Remaining Space | O(1) | Simple subtraction |
| Deallocate | O(n) | Linear search |
| Get Block Pointer | O(n) | Linear search |
| Destroy | O(1) | Frees resources |

## Limitations

1. **No Memory Compaction**: Once allocated, memory position is fixed
2. **Linear Search**: Block lookups are O(n) - use hash table for large block counts
3. **16-bit Addressing**: Limited to 64KB memory pools
4. **No Thread Safety**: Not thread-safe, requires external synchronization
5. **Fragmentation**: Deallocated blocks create holes that can't be reused

## Future Enhancements

Potential improvements for future versions:
- [ ] Memory compaction on deallocate
- [ ] Free list for reusing deallocated blocks
- [ ] Hash table for O(1) block lookup
- [ ] Thread-safe version with mutex
- [ ] 32-bit version for larger memory pools
- [ ] Memory alignment options
- [ ] Debug mode with bounds checking

## License

This is free and unencumbered software released into the public domain.

## Contributing

Contributions welcome! Please ensure:
- Code follows existing style
- All tests pass
- Memory efficiency is maintained
- Documentation is updated

## Version History

- **v1.0** (2025) - Initial release
  - Basic arena allocation
  - Block tracking
  - C/C++ compatibility
  - Optimized memory layout
