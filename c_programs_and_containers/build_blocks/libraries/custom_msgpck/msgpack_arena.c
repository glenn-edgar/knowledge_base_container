#include "msgpack_arena.h"
#include <string.h>
#include <stdio.h>
#include <stdlib.h>

// ============================================================================
// Hash Functions (FNV-1a 64-bit)
// ============================================================================

uint64_t msgpack_hash64(const char* str) {
    uint64_t hash = 14695981039346656037ULL;  // FNV offset basis
    while (*str) {
        hash ^= (uint8_t)(*str++);
        hash *= 1099511628211ULL;  // FNV prime
    }
    return hash;
}

uint64_t msgpack_hash64_n(const char* str, size_t len) {
    uint64_t hash = 14695981039346656037ULL;
    for (size_t i = 0; i < len; i++) {
        hash ^= (uint8_t)str[i];
        hash *= 1099511628211ULL;
    }
    return hash;
}

// ============================================================================
// Initialization
// ============================================================================

bool msgpack_arena_init(MsgPackArena* arena, const void* buffer, size_t size) {
    if (!arena || !buffer || size < sizeof(MsgPackNode)) {
        return false;
    }
    
    arena->buffer = (const uint8_t*)buffer;
    arena->buffer_size = size;
    arena->root_offset = 0;
    
    // For now, assume string pool follows nodes
    // This will be set properly by generated code
    arena->string_pool = NULL;
    arena->string_pool_size = 0;
    
    return true;
}

const MsgPackNode* msgpack_arena_root(const MsgPackArena* arena) {
    if (!arena || arena->root_offset >= arena->buffer_size) {
        return NULL;
    }
    return (const MsgPackNode*)(arena->buffer + arena->root_offset);
}

// ============================================================================
// Navigation
// ============================================================================

const MsgPackNode* msgpack_get_node(const MsgPackArena* arena, uint32_t offset) {
    if (!arena || offset >= arena->buffer_size) {
        return NULL;
    }
    return (const MsgPackNode*)(arena->buffer + offset);
}

const MsgPackNode* msgpack_map_get(const MsgPackArena* arena, 
                                   const MsgPackNode* map,
                                   uint64_t key_hash) {
    if (!arena || !map || map->type != MSGPACK_TYPE_MAP) {
        return NULL;
    }
    
    uint32_t child_offset = map->child_offset;
    for (uint16_t i = 0; i < map->element_count; i++) {
        const MsgPackNode* key = msgpack_get_node(arena, child_offset);
        const MsgPackNode* val = msgpack_get_node(arena, 
                                                   child_offset + sizeof(MsgPackNode));
        
        if (!key || !val) break;
        
        // Check if key matches (assume keys are uint64 hashes)
        if (key->type == MSGPACK_TYPE_UINT && key->value.u64 == key_hash) {
            return val;
        }
        
        child_offset += 2 * sizeof(MsgPackNode);
    }
    
    return NULL;
}

const MsgPackNode* msgpack_map_get_str(const MsgPackArena* arena,
                                       const MsgPackNode* map,
                                       const char* key) {
    uint64_t hash = msgpack_hash64(key);
    return msgpack_map_get(arena, map, hash);
}

const MsgPackNode* msgpack_array_get(const MsgPackArena* arena,
                                     const MsgPackNode* array,
                                     uint16_t index) {
    if (!arena || !array || array->type != MSGPACK_TYPE_ARRAY) {
        return NULL;
    }
    
    if (index >= array->element_count) {
        return NULL;
    }
    
    uint32_t offset = array->child_offset + (index * sizeof(MsgPackNode));
    return msgpack_get_node(arena, offset);
}

// ============================================================================
// Value Extraction
// ============================================================================

bool msgpack_get_int(const MsgPackArena* arena, const MsgPackNode* node, int64_t* out) {
    if (!arena || !node || !out) return false;
    
    if (node->type == MSGPACK_TYPE_INT) {
        *out = node->value.i64;
        return true;
    }
    
    return false;
}

bool msgpack_get_uint(const MsgPackArena* arena, const MsgPackNode* node, uint64_t* out) {
    if (!arena || !node || !out) return false;
    
    if (node->type == MSGPACK_TYPE_UINT) {
        *out = node->value.u64;
        return true;
    }
    
    return false;
}

bool msgpack_get_float(const MsgPackArena* arena, const MsgPackNode* node, float* out) {
    if (!arena || !node || !out) return false;
    
    if (node->type == MSGPACK_TYPE_FLOAT) {
        *out = (float)node->value.f64;
        return true;
    }
    
    return false;
}

bool msgpack_get_double(const MsgPackArena* arena, const MsgPackNode* node, double* out) {
    if (!arena || !node || !out) return false;
    
    if (node->type == MSGPACK_TYPE_DOUBLE || node->type == MSGPACK_TYPE_FLOAT) {
        *out = node->value.f64;
        return true;
    }
    
    return false;
}

bool msgpack_get_bool(const MsgPackArena* arena, const MsgPackNode* node, bool* out) {
    if (!arena || !node || !out) return false;
    
    if (node->type == MSGPACK_TYPE_BOOL) {
        *out = (bool)node->value.u64;
        return true;
    }
    
    return false;
}

const char* msgpack_get_string(const MsgPackArena* arena, const MsgPackNode* node, size_t* len) {
    if (!arena || !node || node->type != MSGPACK_TYPE_STR) {
        if (len) *len = 0;
        return NULL;
    }
    
    if (node->data_offset >= arena->buffer_size) {
        if (len) *len = 0;
        return NULL;
    }
    
    if (len) *len = node->value.sized.size;
    return (const char*)(arena->buffer + node->data_offset);
}

const uint8_t* msgpack_get_binary(const MsgPackArena* arena, const MsgPackNode* node, size_t* len) {
    if (!arena || !node || node->type != MSGPACK_TYPE_BIN) {
        if (len) *len = 0;
        return NULL;
    }
    
    if (node->data_offset >= arena->buffer_size) {
        if (len) *len = 0;
        return NULL;
    }
    
    if (len) *len = node->value.sized.size;
    return arena->buffer + node->data_offset;
}

// ============================================================================
// Subtree Extraction
// ============================================================================

static size_t msgpack_subtree_size_recursive(const MsgPackArena* arena, const MsgPackNode* node) {
    if (!arena || !node) return 0;
    
    size_t size = sizeof(MsgPackNode);
    
    // Add data size (strings, binary)
    if (node->type == MSGPACK_TYPE_STR || node->type == MSGPACK_TYPE_BIN) {
        size += node->value.sized.size + 1;  // +1 for null terminator
    }
    
    // Recursively add children
    if (node->type == MSGPACK_TYPE_ARRAY) {
        for (uint16_t i = 0; i < node->element_count; i++) {
            const MsgPackNode* child = msgpack_array_get(arena, node, i);
            if (child) {
                size += msgpack_subtree_size_recursive(arena, child);
            }
        }
    } else if (node->type == MSGPACK_TYPE_MAP) {
        uint32_t child_offset = node->child_offset;
        for (uint16_t i = 0; i < node->element_count; i++) {
            const MsgPackNode* key = msgpack_get_node(arena, child_offset);
            const MsgPackNode* val = msgpack_get_node(arena, child_offset + sizeof(MsgPackNode));
            if (key && val) {
                size += msgpack_subtree_size_recursive(arena, key);
                size += msgpack_subtree_size_recursive(arena, val);
            }
            child_offset += 2 * sizeof(MsgPackNode);
        }
    }
    
    return size;
}

size_t msgpack_subtree_size(const MsgPackArena* arena, const MsgPackNode* node) {
    return msgpack_subtree_size_recursive(arena, node);
}

static uint32_t msgpack_copy_node_recursive(const MsgPackArena* src_arena,
                                           const MsgPackNode* src_node,
                                           ArenaAllocator* dest_arena,
                                           uint32_t* string_offset) {
    if (!src_arena || !src_node || !dest_arena) {
        return ARENA_OFFSET_NULL;
    }
    
    // Allocate node in destination
    uint32_t node_offset = ARENA_ALLOC(dest_arena, sizeof(MsgPackNode));
    if (node_offset == ARENA_OFFSET_NULL) {
        return ARENA_OFFSET_NULL;
    }
    
    MsgPackNode* dest_node = ARENA_GET(dest_arena, MsgPackNode, node_offset);
    
    // Copy basic fields
    dest_node->type = src_node->type;
    dest_node->flags = src_node->flags;
    dest_node->element_count = src_node->element_count;
    dest_node->value = src_node->value;
    
    // Handle string/binary data
    if (src_node->type == MSGPACK_TYPE_STR || src_node->type == MSGPACK_TYPE_BIN) {
        size_t data_size = src_node->value.sized.size;
        uint32_t data_offset = ARENA_ALLOC(dest_arena, data_size + 1);
        
        if (data_offset != ARENA_OFFSET_NULL) {
            void* dest_data = ARENA_TO_PTR(dest_arena, data_offset);
            const void* src_data = src_arena->buffer + src_node->data_offset;
            memcpy(dest_data, src_data, data_size);
            ((char*)dest_data)[data_size] = '\0';  // Null terminate
            dest_node->data_offset = data_offset;
        }
    } else {
        dest_node->data_offset = 0;
    }
    
    // Handle children
    if (src_node->type == MSGPACK_TYPE_ARRAY) {
        uint32_t first_child_offset = ARENA_OFFSET_NULL;
        
        for (uint16_t i = 0; i < src_node->element_count; i++) {
            const MsgPackNode* src_child = msgpack_array_get(src_arena, src_node, i);
            if (src_child) {
                uint32_t child_offset = msgpack_copy_node_recursive(src_arena, src_child, 
                                                                    dest_arena, string_offset);
                if (i == 0) {
                    first_child_offset = child_offset;
                }
            }
        }
        
        dest_node->child_offset = first_child_offset;
        
    } else if (src_node->type == MSGPACK_TYPE_MAP) {
        uint32_t first_child_offset = ARENA_OFFSET_NULL;
        uint32_t child_offset = src_node->child_offset;
        
        for (uint16_t i = 0; i < src_node->element_count; i++) {
            const MsgPackNode* src_key = msgpack_get_node(src_arena, child_offset);
            const MsgPackNode* src_val = msgpack_get_node(src_arena, 
                                                          child_offset + sizeof(MsgPackNode));
            
            if (src_key && src_val) {
                uint32_t key_offset = msgpack_copy_node_recursive(src_arena, src_key, 
                                                                  dest_arena, string_offset);
                uint32_t val_offset = msgpack_copy_node_recursive(src_arena, src_val, 
                                                                  dest_arena, string_offset);
                
                if (i == 0) {
                    first_child_offset = key_offset;
                }
            }
            
            child_offset += 2 * sizeof(MsgPackNode);
        }
        
        dest_node->child_offset = first_child_offset;
    } else {
        dest_node->child_offset = 0;
    }
    
    return node_offset;
}

bool msgpack_subtree_copy(const MsgPackArena* src_arena,
                         const MsgPackNode* src_node,
                         ArenaAllocator* dest_arena,
                         uint32_t* out_offset) {
    if (!src_arena || !src_node || !dest_arena || !out_offset) {
        return false;
    }
    
    uint32_t string_offset = 0;
    *out_offset = msgpack_copy_node_recursive(src_arena, src_node, dest_arena, &string_offset);
    
    return (*out_offset != ARENA_OFFSET_NULL);
}

bool msgpack_subtree_extract(const MsgPackArena* src_arena,
                            const MsgPackNode* src_node,
                            MsgPackArena* dest_arena,
                            ArenaAllocator* dest_allocator) {
    if (!src_arena || !src_node || !dest_arena || !dest_allocator) {
        return false;
    }
    
    uint32_t root_offset;
    if (!msgpack_subtree_copy(src_arena, src_node, dest_allocator, &root_offset)) {
        return false;
    }
    
    dest_arena->buffer = dest_allocator->buffer;
    dest_arena->buffer_size = dest_allocator->used;
    dest_arena->root_offset = root_offset;
    dest_arena->string_pool = NULL;
    dest_arena->string_pool_size = 0;
    
    return true;
}

// ============================================================================
// Serialization to String
// ============================================================================

static size_t msgpack_node_to_string_recursive(const MsgPackArena* arena,
                                               const MsgPackNode* node,
                                               char* buffer,
                                               size_t buffer_size,
                                               size_t pos,
                                               int indent) {
    if (!arena || !node || pos >= buffer_size) {
        return pos;
    }
    
    char indent_str[128] = {0};
    if (indent > 0) {
        int spaces = indent * 2;
        if (spaces > 127) spaces = 127;
        memset(indent_str, ' ', spaces);
    }
    
    switch (node->type) {
        case MSGPACK_TYPE_NIL:
            pos += snprintf(buffer + pos, buffer_size - pos, "null");
            break;
            
        case MSGPACK_TYPE_BOOL:
            pos += snprintf(buffer + pos, buffer_size - pos, 
                           node->value.u64 ? "true" : "false");
            break;
            
        case MSGPACK_TYPE_INT:
            pos += snprintf(buffer + pos, buffer_size - pos, "%lld", 
                           (long long)node->value.i64);
            break;
            
        case MSGPACK_TYPE_UINT:
            pos += snprintf(buffer + pos, buffer_size - pos, "%llu", 
                           (unsigned long long)node->value.u64);
            break;
            
        case MSGPACK_TYPE_FLOAT:
        case MSGPACK_TYPE_DOUBLE:
            pos += snprintf(buffer + pos, buffer_size - pos, "%.6f", 
                           node->value.f64);
            break;
            
        case MSGPACK_TYPE_STR: {
            size_t len;
            const char* str = msgpack_get_string(arena, node, &len);
            pos += snprintf(buffer + pos, buffer_size - pos, "\"%.*s\"", 
                           (int)len, str ? str : "");
            break;
        }
            
        case MSGPACK_TYPE_ARRAY:
            pos += snprintf(buffer + pos, buffer_size - pos, "[");
            for (uint16_t i = 0; i < node->element_count; i++) {
                if (i > 0) {
                    pos += snprintf(buffer + pos, buffer_size - pos, ", ");
                }
                const MsgPackNode* child = msgpack_array_get(arena, node, i);
                if (child) {
                    pos = msgpack_node_to_string_recursive(arena, child, buffer, 
                                                           buffer_size, pos, -1);
                }
            }
            pos += snprintf(buffer + pos, buffer_size - pos, "]");
            break;
            
        case MSGPACK_TYPE_MAP:
            pos += snprintf(buffer + pos, buffer_size - pos, "{");
            uint32_t child_offset = node->child_offset;
            for (uint16_t i = 0; i < node->element_count; i++) {
                if (i > 0) {
                    pos += snprintf(buffer + pos, buffer_size - pos, ", ");
                }
                
                const MsgPackNode* key = msgpack_get_node(arena, child_offset);
                const MsgPackNode* val = msgpack_get_node(arena, 
                                                          child_offset + sizeof(MsgPackNode));
                
                if (key && val) {
                    // Print key (as hash if uint64)
                    if (key->type == MSGPACK_TYPE_UINT) {
                        pos += snprintf(buffer + pos, buffer_size - pos, 
                                       "0x%016llX: ", 
                                       (unsigned long long)key->value.u64);
                    } else {
                        pos = msgpack_node_to_string_recursive(arena, key, buffer, 
                                                               buffer_size, pos, -1);
                        pos += snprintf(buffer + pos, buffer_size - pos, ": ");
                    }
                    
                    // Print value
                    pos = msgpack_node_to_string_recursive(arena, val, buffer, 
                                                           buffer_size, pos, -1);
                }
                
                child_offset += 2 * sizeof(MsgPackNode);
            }
            pos += snprintf(buffer + pos, buffer_size - pos, "}");
            break;
            
        default:
            pos += snprintf(buffer + pos, buffer_size - pos, "<unknown>");
            break;
    }
    
    return pos;
}

bool msgpack_to_string(const MsgPackArena* arena,
                      const MsgPackNode* node,
                      char* buffer,
                      size_t buffer_size,
                      size_t* out_len) {
    if (!arena || !node || !buffer || buffer_size == 0) {
        return false;
    }
    
    size_t pos = msgpack_node_to_string_recursive(arena, node, buffer, buffer_size, 0, -1);
    buffer[pos < buffer_size ? pos : buffer_size - 1] = '\0';
    
    if (out_len) {
        *out_len = pos;
    }
    
    return true;
}

// ============================================================================
// Debugging
// ============================================================================

void msgpack_print_node(const MsgPackArena* arena, const MsgPackNode* node) {
    char buffer[1024];
    msgpack_to_string(arena, node, buffer, sizeof(buffer), NULL);
    printf("%s\n", buffer);
}

void msgpack_print_tree(const MsgPackArena* arena, const MsgPackNode* node, int indent) {
    char buffer[2048];
    msgpack_to_string(arena, node, buffer, sizeof(buffer), NULL);
    printf("%*s%s\n", indent * 2, "", buffer);
}

bool msgpack_validate(const MsgPackArena* arena) {
    if (!arena || !arena->buffer) {
        return false;
    }
    
    const MsgPackNode* root = msgpack_arena_root(arena);
    return (root != NULL);
}

