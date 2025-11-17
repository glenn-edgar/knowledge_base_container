#ifndef MSGPACK_ARENA_H
#define MSGPACK_ARENA_H

#include <stdint.h>
#include <stdbool.h>
#include <stddef.h>
#include "arena_alloc.h"

// ============================================================================
// Hash Functions
// ============================================================================

// FNV-1a 64-bit hash (fast, good distribution, not cryptographic)
uint64_t msgpack_hash64(const char* str);
uint64_t msgpack_hash64_n(const char* str, size_t len);

// ============================================================================
// MessagePack Types
// ============================================================================

typedef enum {
    MSGPACK_TYPE_NIL = 0,
    MSGPACK_TYPE_BOOL,
    MSGPACK_TYPE_INT,
    MSGPACK_TYPE_UINT,
    MSGPACK_TYPE_FLOAT,
    MSGPACK_TYPE_DOUBLE,
    MSGPACK_TYPE_STR,
    MSGPACK_TYPE_BIN,
    MSGPACK_TYPE_ARRAY,
    MSGPACK_TYPE_MAP,
    MSGPACK_TYPE_EXT
} MsgPackType;

// ============================================================================
// MessagePack Node Structure (Offset-based)
// ============================================================================

typedef struct __attribute__((packed)) {
    uint8_t type;              // MsgPackType
    uint8_t flags;             // Reserved for future use
    uint16_t element_count;    // For arrays/maps
    uint32_t data_offset;      // Offset to data (string, binary, etc.)
    uint32_t child_offset;     // Offset to first child node (for arrays/maps)
    union {
        int64_t i64;
        uint64_t u64;
        double f64;
        struct {
            uint32_t size;     // Size of string/binary
            uint32_t reserved;
        } sized;
    } value;
} MsgPackNode;  // 24 bytes

// ============================================================================
// MessagePack Arena Buffer
// ============================================================================

typedef struct {
    const uint8_t* buffer;     // Readonly buffer (flash or ROM)
    size_t buffer_size;
    uint32_t root_offset;      // Offset to root node
    
    // String pool for lookups
    const char* string_pool;
    uint32_t string_pool_size;
} MsgPackArena;

// ============================================================================
// Initialization
// ============================================================================

// Initialize from const buffer (readonly)
bool msgpack_arena_init(MsgPackArena* arena, const void* buffer, size_t size);

// Get root node
const MsgPackNode* msgpack_arena_root(const MsgPackArena* arena);

// ============================================================================
// Navigation
// ============================================================================

// Get node at offset
const MsgPackNode* msgpack_get_node(const MsgPackArena* arena, uint32_t offset);

// Map access by hash key
const MsgPackNode* msgpack_map_get(const MsgPackArena* arena, 
                                   const MsgPackNode* map, 
                                   uint64_t key_hash);

// Map access by string key (hashes internally)
const MsgPackNode* msgpack_map_get_str(const MsgPackArena* arena,
                                       const MsgPackNode* map,
                                       const char* key);

// Array access by index
const MsgPackNode* msgpack_array_get(const MsgPackArena* arena,
                                     const MsgPackNode* array,
                                     uint16_t index);

// ============================================================================
// Value Extraction
// ============================================================================

// Get integer value
bool msgpack_get_int(const MsgPackArena* arena, const MsgPackNode* node, int64_t* out);
bool msgpack_get_uint(const MsgPackArena* arena, const MsgPackNode* node, uint64_t* out);

// Get floating point value
bool msgpack_get_float(const MsgPackArena* arena, const MsgPackNode* node, float* out);
bool msgpack_get_double(const MsgPackArena* arena, const MsgPackNode* node, double* out);

// Get boolean value
bool msgpack_get_bool(const MsgPackArena* arena, const MsgPackNode* node, bool* out);

// Get string value (points into buffer, no copy)
const char* msgpack_get_string(const MsgPackArena* arena, const MsgPackNode* node, size_t* len);

// Get binary data (points into buffer, no copy)
const uint8_t* msgpack_get_binary(const MsgPackArena* arena, const MsgPackNode* node, size_t* len);

// ============================================================================
// Subtree Extraction to RAM
// ============================================================================

// Calculate size needed for subtree
size_t msgpack_subtree_size(const MsgPackArena* arena, const MsgPackNode* node);

// Copy subtree to new arena buffer
bool msgpack_subtree_copy(const MsgPackArena* src_arena,
                         const MsgPackNode* src_node,
                         ArenaAllocator* dest_arena,
                         uint32_t* out_offset);

// Create new MsgPackArena from extracted subtree
bool msgpack_subtree_extract(const MsgPackArena* src_arena,
                            const MsgPackNode* src_node,
                            MsgPackArena* dest_arena,
                            ArenaAllocator* dest_allocator);

// ============================================================================
// Serialization to String
// ============================================================================

// Calculate string size needed
size_t msgpack_to_string_size(const MsgPackArena* arena, const MsgPackNode* node);

// Convert to JSON-like string representation
bool msgpack_to_string(const MsgPackArena* arena,
                      const MsgPackNode* node,
                      char* buffer,
                      size_t buffer_size,
                      size_t* out_len);

// Convert to string with custom indent
bool msgpack_to_string_pretty(const MsgPackArena* arena,
                             const MsgPackNode* node,
                             char* buffer,
                             size_t buffer_size,
                             int indent_level,
                             size_t* out_len);

// ============================================================================
// Debugging
// ============================================================================

// Print node information
void msgpack_print_node(const MsgPackArena* arena, const MsgPackNode* node);

// Print entire tree
void msgpack_print_tree(const MsgPackArena* arena, const MsgPackNode* node, int indent);

// Validate arena structure
bool msgpack_validate(const MsgPackArena* arena);

#endif // MSGPACK_ARENA_H

