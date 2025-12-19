# CT Tree Walker - C Implementation

A reentrant, production-ready C library for traversing tree and graph structures with support for multiple traversal methods.

## Features

- **Three traversal methods**: Recursive DFS, Iterative DFS, and BFS
- **Flexible control flow**: Multiple return codes for fine-grained control (continue, stop branch, skip children, stop level, stop siblings, stop all)
- **User and engine flags**: 4 bits reserved for engine, 4 bits available for user application
- **Stack-based iteration**: User-provided stack for iterative methods (no heap allocation)
- **Callback-driven**: User-defined functions for retrieving children and applying operations
- **Cross-compiler ready**: No install targets, easy to configure for embedded systems

## Files

- `CT_Tree_Walker.h` - Header file with C++ guards
- `CT_Tree_Walker.c` - Implementation
- `example.c` - Comprehensive usage examples
- `Makefile` - Builds static library, shared library, and example

## Building

### Default build (native):
```bash
make all          # Build everything
make libs         # Build libraries only
make example      # Build example only
make static       # Build static library only
make shared       # Build shared library only
```

### Cross-compilation:
```bash
# ARM Cortex-M (bare metal)
make CC=arm-none-eabi-gcc CFLAGS="-O2 -mcpu=cortex-m4 -mthumb"

# ARM Linux
make CC=arm-linux-gnueabihf-gcc

# RISC-V
make CC=riscv64-linux-gnu-gcc

# Custom flags
make CC=gcc CFLAGS="-O3 -march=native -I."
```

### Utilities:
```bash
make help         # Show all targets
make info         # Show build configuration
make size         # Show compiled sizes
make clean        # Clean everything
make clean-libs   # Clean libraries only
```

## Library Outputs

- **Static Library:** `libct_tree_walker.a` (~4KB)
- **Shared Library:** `libct_tree_walker.so.1.0` (~16KB)
- **Example Binary:** `example` (~17KB)

## API Overview

### Initialization
```c
CT_TreeWalker walker;
uint8_t flags[NUM_NODES];

ct_walker_init(&walker, NUM_NODES, flags, 
               get_children_func, apply_func);
```

### Walking
```c
CT_StackEntry stack[STACK_SIZE];  // For iterative/BFS only

ct_walker_walk(&walker, user_handle, root_id, 
               CT_RECURSIVE,  // or CT_ITERATIVE or CT_BFS
               stack, STACK_SIZE,  // NULL, 0 for recursive
               max_level);
```

### Return Codes
```c
typedef enum {
    CT_CONTINUE = 0,       // Continue normally
    CT_STOP_BRANCH = 1,    // Stop this branch
    CT_SKIP_CHILDREN = 2,  // Skip children, continue siblings
    CT_STOP_LEVEL = 3,     // Stop at current level
    CT_STOP_SIBLINGS = 4,  // Stop siblings, return to parent
    CT_STOP_ALL = 5        // Stop entire traversal
} CT_ReturnCode;
```

## Callback Functions

### Get Children Callback
```c
unsigned int get_children(void* user_handle,
                         unsigned int node_id,
                         unsigned int* children_out,
                         unsigned int max_children)
{
    // Populate children_out with child node IDs
    // Return number of children
}
```

### Apply Function Callback
```c
CT_ReturnCode apply_func(void* user_handle,
                        unsigned int node_id,
                        unsigned int level,
                        uint8_t* flags)
{
    // Perform operation on node
    // Return control code
}
```

## Flag System

Lower 4 bits (engine flags - managed automatically):
- `CT_FLAG_VISITED` (0x01) - Node has been visited
- `CT_FLAG_IN_STACK` (0x02) - Node is in processing stack
- `CT_FLAG_STOP_SIBS` (0x04) - Stop processing siblings
- `CT_FLAG_RESERVED` (0x08) - Reserved for future use

Upper 4 bits (user flags - available for application):
- `CT_FLAG_USER_BIT0` (0x10)
- `CT_FLAG_USER_BIT1` (0x20)
- `CT_FLAG_USER_BIT2` (0x40)
- `CT_FLAG_USER_BIT3` (0x80)

Use `ct_walker_set_user_flags()` and `ct_walker_get_user_flags()` to manipulate user flags.

## Example Usage

See `example.c` for comprehensive examples including:
- Basic DFS (recursive and iterative)
- Basic BFS
- Stopping at specific levels
- Skipping children
- Stopping siblings
- Maximum depth limiting
- Small stack handling
- Method comparison

## Memory Requirements

For embedded systems:
- Walker instance: ~40 bytes
- Flags array: 1 byte per node
- Stack (iterative/BFS): ~12 bytes per entry × stack depth

Example: 1000 nodes with stack depth 100:
- Total: 40 + 1000 + (12 × 100) = 2,240 bytes (~2.2KB)

## Integration with ChainTree

This walker is designed to integrate seamlessly with the ChainTree architecture:
- Node IDs map directly to ltree paths or integer indices
- User flags can store node state (PENDING, RUNNING, SUCCESS, FAILURE)
- Callbacks can interact with PostgreSQL, NATS, or MQTT for distributed coordination
- Stack-based design enables tick-based execution in hard real-time systems

## Thread Safety

The walker itself is reentrant but not thread-safe. For concurrent access:
- Use separate walker instances per thread, OR
- Protect walker access with mutexes

## Performance

- Recursive DFS: Minimal overhead, uses call stack
- Iterative DFS: Slightly slower, explicit stack management
- BFS: Queue simulation overhead, good for level-order operations

All methods have O(n) time complexity for n nodes.

## License

This code is designed for use in the ChainTree distributed control system.

## Author

Generated for Glenn at Onyx Engineering