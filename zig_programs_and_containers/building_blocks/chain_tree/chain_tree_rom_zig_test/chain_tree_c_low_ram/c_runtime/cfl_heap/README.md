# CFL Heap - Reentrant Small-Block Allocator

## Overview

The CFL Heap is a reentrant, index-based memory allocator designed for embedded systems with limited RAM. It returns 16-bit indices instead of pointers, allowing efficient storage in small data structures.

## Key Design Features

### 1. Fully Reentrant (No Static Globals)

All state is stored in the `CflHeap` structure, enabling:
- Multiple independent heap instances
- Thread-safe operation (each thread can have its own heap)
- Nested allocation contexts
- Testing without global state pollution

### 2. Flexible Memory Management

Two initialization modes:

**External Buffer Mode** - Caller provides buffer:
```c
uint8_t buffer[1024];
CflHeap heap;
cfl_heap_init(&heap, buffer, sizeof(buffer));
```

**Embedded Buffer Mode** - Buffer follows structure:
```c
uint8_t storage[CFL_HEAP_TOTAL_SIZE(1024)];
CflHeap* heap = (CflHeap*)storage;
cfl_heap_init_embedded(heap, 1024);
```

### 3. Index-Based Allocation

Returns 16-bit indices instead of pointers:
- Saves memory in data structures (2 bytes vs 4+ bytes)
- Enables serialization/persistence
- Works with relocatable memory regions
- Supports pointer compression

### 4. Zero Dependencies

Only requires:
- `<stdint.h>` for fixed-width types
- `<stdbool.h>` for bool
- `<string.h>` for memset
- `cfl_exception.h` for error handling

## Architecture

```
CflHeap Structure:
  ┌─────────────────┐
  │ pool*           │ → Points to memory pool
  │ pool_size       │   Size of pool
  │ initialized     │   Init flag
  │ owns_pool       │   Ownership flag
  └─────────────────┘

Memory Pool Layout:
  ┌──────────────┬─────────┬──────────────┬─────────┐
  │ BlockHeader  │  Data   │ BlockHeader  │  Data   │
  │ (4 bytes)    │         │ (4 bytes)    │         │
  └──────────────┴─────────┴──────────────┴─────────┘
  
BlockHeader:
  ┌─────────┬─────────┐
  │ size    │ flags   │  (uint16_t each)
  └─────────┴─────────┘
```

## Use Cases for ChainTree

### Per-Node Heaps

Each ChainTree node can have its own small heap:

```c
typedef struct ChainTreeNode {
    CflHeap heap;
    uint8_t heap_buffer[256];
    uint16_t blackboard_idx;  // Index into heap
    // ... other fields ...
} ChainTreeNode;

void node_init(ChainTreeNode* node) {
    cfl_heap_init(&node->heap, node->heap_buffer, sizeof(node->heap_buffer));
    
    // Allocate node-local blackboard
    node->blackboard_idx = cfl_heap_malloc(&node->heap, sizeof(Blackboard));
}
```

### Hierarchical Allocation

Parent nodes with arena allocators can contain child heaps:

```c
typedef struct ParentNode {
    ArenaAllocator arena;      // Large blocks
    CflHeap* child_heaps[16];  // Small allocations per child
} ParentNode;
```

### Temporary Scratch Space

Reset heaps for transient allocations:

```c
void process_event(CflHeap* scratch) {
    cfl_heap_reset(scratch);
    cfl_heap_init_embedded(scratch, SCRATCH_SIZE);
    
    // Allocate temporary buffers
    uint16_t buf_idx = cfl_heap_malloc(scratch, 128);
    
    // ... process ...
    
    // Reset clears everything
    cfl_heap_reset(scratch);
}
```

## Thread Safety

The heap itself is **not** thread-safe (no internal locking), but the reentrant design enables safe patterns:

### Pattern 1: Per-Thread Heaps
```c
// Each thread gets its own heap - no contention
ThreadLocal CflHeap thread_heap;
```

### Pattern 2: Single-Writer, Multiple-Reader
```c
// One thread allocates, others only read via indices
CflHeap shared_heap;
mutex_lock();
uint16_t idx = cfl_heap_malloc(&shared_heap, size);
mutex_unlock();

// Readers use idx without locking
void* ptr = cfl_heap_ptr(&shared_heap, idx);  // Safe if no free()
```

### Pattern 3: Externally Synchronized
```c
mutex_lock();
cfl_heap_malloc(&shared_heap, size);
mutex_unlock();
```

## Memory Overhead

- **Per-heap**: 8 bytes (CflHeap structure)
- **Per-allocation**: 4 bytes (BlockHeader)
- **Minimum allocation**: 8 bytes (aligned)
- **Alignment**: 4 bytes (ARM Cortex-M friendly)

## Comparison to Static Version

| Feature | Static | Reentrant |
|---------|--------|-----------|
| Global state | Yes | No |
| Multiple heaps | No | Yes |
| Thread-safe | No | Per-instance |
| Testable | Harder | Easier |
| Memory overhead | 0 bytes | 8 bytes/heap |
| API complexity | Simpler | Slightly more |

## Performance Characteristics

- **Allocation**: O(n) where n = number of free blocks
- **Free**: O(n) due to coalescing
- **Index↔Pointer**: O(1)
- **Diagnostics**: O(n)

Not suitable for:
- High-frequency allocation/free patterns
- Large numbers of small allocations
- Real-time systems needing deterministic allocation

Good for:
- Long-lived allocations
- Small number of objects
- Memory-constrained systems
- Hierarchical allocation schemes

## Integration with ChainTree

### Storage Format

Indices can be stored compactly in PostgreSQL:

```sql
CREATE TABLE chain_nodes (
    path ltree,
    heap_data BYTEA,              -- Serialized heap state
    blackboard_idx INT2,          -- uint16_t index
    child_data_idx INT2,
    -- ...
);
```

### Distributed Coordination

NATS messages can reference heap objects by index:

```c
typedef struct {
    uint32_t node_id;
    uint16_t data_idx;    // Index in node's heap
    uint16_t size;
} HeapReference;
```

### Arena + Heap Pattern

Two-tier allocation:

1. **Arena**: Large blocks, fast bump allocation, no per-object free
2. **Heap**: Small blocks, individual free, coalescing

```c
typedef struct HybridAllocator {
    ArenaAllocator arena;   // For tree structure
    CflHeap heap;          // For blackboard data
    uint8_t heap_buf[512];
} HybridAllocator;
```

## Exception Handling

Uses `cfl_exception.h` macros for error reporting:
- Throws on NULL pointers
- Throws on invalid indices
- Throws on out-of-memory
- Throws on uninitialized heap access

## Future Enhancements

Potential additions:
- Memory pool validation/integrity checks
- Allocation watermark tracking
- Custom alignment per allocation
- Defragmentation support
- Statistics callbacks

## License

Part of the ChainTree project by Glenn at Onyx Engineering.