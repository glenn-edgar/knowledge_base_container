// ============================================================================
// s_expr_binary.h
// ChainTree S-Expression Binary Module Format
// 
// Wire format: Little-endian, ARM32/ARM64 compatible
// All offsets are relative to section starts
// No pointers - fully position-independent
// ============================================================================

#ifndef S_EXPR_BINARY_H
#define S_EXPR_BINARY_H

#ifdef __cplusplus
extern "C" {
#endif

#include <stdint.h>
#include <stddef.h>
#include <stdbool.h>
#include <string.h>

// ============================================================================
// MAGIC AND VERSION
// ============================================================================

#define SEXB_MAGIC          0x42584553  // "SEXB" in little-endian
#define SEXB_VERSION        0x0100      // 1.0

// Flags
#define SEXB_FLAG_32BIT     0x0000
#define SEXB_FLAG_64BIT     0x0001
#define SEXB_FLAG_DEBUG     0x0002      // Contains debug info

// ============================================================================
// FIELD TYPE TAGS (matches DSL type system)
// ============================================================================

typedef enum {
    SEXB_TYPE_INT8      = 0x01,
    SEXB_TYPE_INT16     = 0x02,
    SEXB_TYPE_INT32     = 0x03,
    SEXB_TYPE_INT64     = 0x04,
    SEXB_TYPE_UINT8     = 0x05,
    SEXB_TYPE_UINT16    = 0x06,
    SEXB_TYPE_UINT32    = 0x07,
    SEXB_TYPE_UINT64    = 0x08,
    SEXB_TYPE_FLOAT     = 0x09,
    SEXB_TYPE_DOUBLE    = 0x0A,
    SEXB_TYPE_BOOL      = 0x0B,
    SEXB_TYPE_CHAR      = 0x0C,
    SEXB_TYPE_CHAR_ARRAY= 0x0D,         // Fixed-size char array
    SEXB_TYPE_PTR       = 0x0E,         // Pointer to record
    SEXB_TYPE_EMBEDDED  = 0x0F,         // Embedded record
} sexb_field_type_t;

// Field flags
#define SEXB_FIELD_FLAG_POINTER     0x01  // PTR_FIELD
#define SEXB_FIELD_FLAG_ARRAY       0x02  // Array field
#define SEXB_FIELD_FLAG_EMBEDDED    0x04  // Embedded record

// ============================================================================
// BYTECODE OPCODES (parameter encoding)
// ============================================================================

typedef enum {
    SEXB_OP_INT         = 0x01,         // 32-bit signed int
    SEXB_OP_UINT        = 0x02,         // 32-bit unsigned int
    SEXB_OP_FLOAT       = 0x03,         // 32-bit float
    SEXB_OP_STR_IDX     = 0x04,         // String table index
    SEXB_OP_FIELD_REF   = 0x05,         // Field reference (offset)
    SEXB_OP_NESTED_REF  = 0x06,         // Nested field (path hash)
    SEXB_OP_CONST_REF   = 0x07,         // Constant reference
    SEXB_OP_RESULT      = 0x08,         // Return code
    SEXB_OP_LIST_START  = 0x09,         // List start marker
    SEXB_OP_LIST_END    = 0x0A,         // List end marker
    SEXB_OP_CALL_START  = 0x0B,         // Function call start
    SEXB_OP_CALL_END    = 0x0C,         // Function call end
    SEXB_OP_INT64       = 0x0D,         // 64-bit signed int
    SEXB_OP_UINT64      = 0x0E,         // 64-bit unsigned int
    SEXB_OP_DOUBLE      = 0x0F,         // 64-bit float
} sexb_opcode_t;

// ============================================================================
// FUNCTION TYPE TAGS
// 
// CRITICAL: The Lua generator uses 6 types but only 3 hash tables:
//   Types 1,5 -> oneshot_hashes (o_call, io_call)
//   Types 2,4 -> main_hashes    (m_call, pt_m_call)
//   Types 3,6 -> pred_hashes    (p_call, p_call_bit)
// ============================================================================

typedef enum {
    SEXB_FUNC_ONESHOT   = 0x01,         // o_call    -> oneshot_hashes
    SEXB_FUNC_MAIN      = 0x02,         // m_call    -> main_hashes
    SEXB_FUNC_PRED      = 0x03,         // p_call    -> pred_hashes
    SEXB_FUNC_PT_MAIN   = 0x04,         // pt_m_call -> main_hashes (protothread)
    SEXB_FUNC_INIT_ONE  = 0x05,         // io_call   -> oneshot_hashes (init)
    SEXB_FUNC_BIT_PRED  = 0x06,         // p_call_bit-> pred_hashes (bit block)
} sexb_func_type_t;

// ============================================================================
// FUNCTION CATEGORY (which hash table to use)
// ============================================================================

typedef enum {
    SEXB_CAT_ONESHOT = 0,   // Use oneshot_hashes
    SEXB_CAT_MAIN    = 1,   // Use main_hashes
    SEXB_CAT_PRED    = 2,   // Use pred_hashes
    SEXB_CAT_INVALID = 255
} sexb_func_category_t;

// Map func_type (1-6) to hash table category
static inline sexb_func_category_t sexb_get_func_category(uint8_t func_type) {
    switch (func_type) {
        case SEXB_FUNC_ONESHOT:
        case SEXB_FUNC_INIT_ONE:
            return SEXB_CAT_ONESHOT;
        case SEXB_FUNC_MAIN:
        case SEXB_FUNC_PT_MAIN:
            return SEXB_CAT_MAIN;
        case SEXB_FUNC_PRED:
        case SEXB_FUNC_BIT_PRED:
            return SEXB_CAT_PRED;
        default:
            return SEXB_CAT_INVALID;
    }
}

// ============================================================================
// BINARY FILE STRUCTURES (packed, little-endian)
// ============================================================================

#pragma pack(push, 1)

// File header (32 bytes)
typedef struct {
    uint32_t magic;                     // SEXB_MAGIC
    uint16_t version;                   // SEXB_VERSION
    uint16_t flags;                     // 32/64 bit, debug, etc
    uint32_t module_name_hash;          // FNV-1a hash of module name
    uint16_t tree_count;
    uint16_t record_count;
    uint16_t string_count;
    uint16_t const_count;
    uint16_t oneshot_count;
    uint16_t main_count;
    uint16_t pred_count;
    uint16_t reserved;
    uint32_t total_size;                // Total file size
} sexb_header_t;

_Static_assert(sizeof(sexb_header_t) == 32, "Header must be 32 bytes");

// Section directory (32 bytes)
typedef struct {
    uint32_t tree_table_offset;
    uint32_t record_table_offset;
    uint32_t field_table_offset;
    uint32_t string_blob_offset;
    uint32_t const_table_offset;
    uint32_t const_data_offset;
    uint32_t func_table_offset;
    uint32_t bytecode_offset;
} sexb_directory_t;

_Static_assert(sizeof(sexb_directory_t) == 32, "Directory must be 32 bytes");

// Tree definition (16 bytes)
typedef struct {
    uint32_t name_hash;                 // Tree name hash
    uint16_t record_index;              // Index into record table
    uint16_t node_count;                // Number of nodes
    uint32_t bytecode_offset;           // Offset into bytecode blob
    uint32_t bytecode_size;             // Size in bytes
} sexb_tree_def_t;

_Static_assert(sizeof(sexb_tree_def_t) == 16, "Tree def must be 16 bytes");

// Record definition (12 bytes)
typedef struct {
    uint32_t name_hash;                 // Record name hash
    uint16_t field_count;               // Number of fields
    uint16_t size;                      // Total size in bytes
    uint32_t field_table_offset;        // Offset into field table
} sexb_record_def_t;

_Static_assert(sizeof(sexb_record_def_t) == 12, "Record def must be 12 bytes");

// Field definition (12 bytes)
typedef struct {
    uint32_t name_hash;                 // Field name hash
    uint8_t  type_tag;                  // sexb_field_type_t
    uint8_t  flags;                     // SEXB_FIELD_FLAG_*
    uint16_t offset;                    // Offset within record
    uint16_t size;                      // Size in bytes
    uint16_t aux;                       // Array length or target record index
} sexb_field_def_t;

_Static_assert(sizeof(sexb_field_def_t) == 12, "Field def must be 12 bytes");

// Constant definition (12 bytes)
typedef struct {
    uint32_t name_hash;                 // Constant name hash
    uint16_t record_index;              // Index into record table
    uint16_t data_size;                 // Size of constant data
    uint32_t data_offset;               // Offset into const data blob
} sexb_const_def_t;

_Static_assert(sizeof(sexb_const_def_t) == 12, "Const def must be 12 bytes");

// Bytecode node header (8 bytes)
typedef struct {
    uint32_t func_hash;                 // Function name hash
    uint8_t  func_type;                 // sexb_func_type_t
    uint8_t  param_count;               // Number of parameters
    uint16_t bytecode_size;             // Size of this node's bytecode
} sexb_node_header_t;

_Static_assert(sizeof(sexb_node_header_t) == 8, "Node header must be 8 bytes");

#pragma pack(pop)

// ============================================================================
// RUNTIME STRUCTURES (for loaded module in RAM)
// ============================================================================

typedef struct sexb_module sexb_module_t;
typedef struct sexb_tree sexb_tree_t;
typedef struct sexb_record sexb_record_t;
typedef struct sexb_field sexb_field_t;
typedef struct sexb_const sexb_const_t;

struct sexb_field {
    uint32_t name_hash;
    uint8_t  type_tag;
    uint8_t  flags;
    uint16_t offset;
    uint16_t size;
    uint16_t aux;
};

struct sexb_record {
    uint32_t name_hash;
    uint16_t field_count;
    uint16_t size;
    const sexb_field_t* fields;
};

struct sexb_tree {
    uint32_t name_hash;
    uint16_t record_index;
    uint16_t node_count;
    const uint8_t* bytecode;
    uint32_t bytecode_size;
};

struct sexb_const {
    uint32_t name_hash;
    uint16_t record_index;
    uint16_t data_size;
    const void* data;
};

struct sexb_module {
    uint32_t name_hash;
    uint16_t flags;
    
    uint16_t tree_count;
    uint16_t record_count;
    uint16_t string_count;
    uint16_t const_count;
    uint16_t oneshot_count;
    uint16_t main_count;
    uint16_t pred_count;
    
    const sexb_tree_t* trees;
    const sexb_record_t* records;
    const sexb_const_t* constants;
    
    const uint8_t* string_blob;
    uint32_t string_blob_size;
    
    const uint32_t* oneshot_hashes;
    const uint32_t* main_hashes;
    const uint32_t* pred_hashes;
    
    const uint8_t* raw_data;
    uint32_t raw_size;
};

// ============================================================================
// ERROR CODES
// ============================================================================

typedef enum {
    SEXB_OK = 0,
    SEXB_ERR_NULL_PTR,
    SEXB_ERR_TOO_SMALL,
    SEXB_ERR_BAD_MAGIC,
    SEXB_ERR_BAD_VERSION,
    SEXB_ERR_BAD_SIZE,
    SEXB_ERR_BAD_OFFSET,
    SEXB_ERR_ALLOC_FAILED,
    SEXB_ERR_INVALID_HEADER,
    SEXB_ERR_INVALID_BYTECODE,
} sexb_error_t;

// ============================================================================
// VALIDATION
// ============================================================================

static inline sexb_error_t sexb_validate(const void* data, size_t size) {
    if (!data) return SEXB_ERR_NULL_PTR;
    if (size < sizeof(sexb_header_t) + sizeof(sexb_directory_t)) {
        return SEXB_ERR_TOO_SMALL;
    }
    
    const sexb_header_t* hdr = (const sexb_header_t*)data;
    
    if (hdr->magic != SEXB_MAGIC) return SEXB_ERR_BAD_MAGIC;
    if (hdr->version != SEXB_VERSION) return SEXB_ERR_BAD_VERSION;
    if (hdr->total_size != size) return SEXB_ERR_BAD_SIZE;
    
    return SEXB_OK;
}

static inline uint32_t sexb_get_name_hash(const void* data) {
    const sexb_header_t* hdr = (const sexb_header_t*)data;
    return hdr->module_name_hash;
}

// ============================================================================
// STRING ACCESS
// Format: u16 length, string data, null terminator, padding to 4-byte
// ============================================================================

static inline const char* sexb_get_string(const sexb_module_t* mod, uint16_t index) {
    if (!mod || !mod->string_blob || index >= mod->string_count) {
        return NULL;
    }
    
    const uint8_t* ptr = mod->string_blob;
    const uint8_t* end = ptr + mod->string_blob_size;
    
    for (uint16_t i = 0; i < index && ptr < end; i++) {
        uint16_t len = ptr[0] | (ptr[1] << 8);
        size_t total = 2 + len + 1;  // length + data + null
        total = (total + 3) & ~3;    // align to 4
        ptr += total;
    }
    
    if (ptr >= end) return NULL;
    return (const char*)(ptr + 2);  // skip length
}

static inline uint16_t sexb_get_string_len(const sexb_module_t* mod, uint16_t index) {
    if (!mod || !mod->string_blob || index >= mod->string_count) {
        return 0;
    }
    
    const uint8_t* ptr = mod->string_blob;
    const uint8_t* end = ptr + mod->string_blob_size;
    
    for (uint16_t i = 0; i < index && ptr < end; i++) {
        uint16_t len = ptr[0] | (ptr[1] << 8);
        size_t total = 2 + len + 1;
        total = (total + 3) & ~3;
        ptr += total;
    }
    
    if (ptr >= end) return 0;
    return ptr[0] | (ptr[1] << 8);
}

// ============================================================================
// LOOKUP FUNCTIONS
// ============================================================================

static inline const sexb_tree_t* sexb_find_tree(const sexb_module_t* mod, uint32_t name_hash) {
    if (!mod || !mod->trees) return NULL;
    for (uint16_t i = 0; i < mod->tree_count; i++) {
        if (mod->trees[i].name_hash == name_hash) {
            return &mod->trees[i];
        }
    }
    return NULL;
}

static inline const sexb_record_t* sexb_find_record(const sexb_module_t* mod, uint32_t name_hash) {
    if (!mod || !mod->records) return NULL;
    for (uint16_t i = 0; i < mod->record_count; i++) {
        if (mod->records[i].name_hash == name_hash) {
            return &mod->records[i];
        }
    }
    return NULL;
}

static inline const sexb_field_t* sexb_find_field(const sexb_record_t* rec, uint32_t name_hash) {
    if (!rec || !rec->fields) return NULL;
    for (uint16_t i = 0; i < rec->field_count; i++) {
        if (rec->fields[i].name_hash == name_hash) {
            return &rec->fields[i];
        }
    }
    return NULL;
}

static inline const sexb_const_t* sexb_find_const(const sexb_module_t* mod, uint32_t name_hash) {
    if (!mod || !mod->constants) return NULL;
    for (uint16_t i = 0; i < mod->const_count; i++) {
        if (mod->constants[i].name_hash == name_hash) {
            return &mod->constants[i];
        }
    }
    return NULL;
}

// ============================================================================
// FUNCTION LOOKUP - Uses func_type to select correct hash table
// ============================================================================

static inline uint16_t sexb_find_func_index(
    const uint32_t* hashes,
    uint16_t count,
    uint32_t target_hash
) {
    for (uint16_t i = 0; i < count; i++) {
        if (hashes[i] == target_hash) {
            return i;
        }
    }
    return 0xFFFF;
}

// Find function using func_type directly (CRITICAL for correct table selection)
static inline uint16_t sexb_find_func_by_type(
    const sexb_module_t* mod,
    uint8_t func_type,
    uint32_t func_hash
) {
    if (!mod) return 0xFFFF;
    
    const uint32_t* hashes = NULL;
    uint16_t count = 0;
    
    switch (func_type) {
        case SEXB_FUNC_ONESHOT:
        case SEXB_FUNC_INIT_ONE:
            hashes = mod->oneshot_hashes;
            count = mod->oneshot_count;
            break;
        case SEXB_FUNC_MAIN:
        case SEXB_FUNC_PT_MAIN:
            hashes = mod->main_hashes;
            count = mod->main_count;
            break;
        case SEXB_FUNC_PRED:
        case SEXB_FUNC_BIT_PRED:
            hashes = mod->pred_hashes;
            count = mod->pred_count;
            break;
        default:
            return 0xFFFF;
    }
    
    if (!hashes) return 0xFFFF;
    return sexb_find_func_index(hashes, count, func_hash);
}

// ============================================================================
// BYTECODE READER
// ============================================================================

typedef struct {
    const uint8_t* bytecode;
    uint32_t size;
    uint32_t pos;
} sexb_bytecode_reader_t;

static inline void sexb_reader_init(sexb_bytecode_reader_t* r, const uint8_t* bc, uint32_t size) {
    r->bytecode = bc;
    r->size = size;
    r->pos = 0;
}

static inline bool sexb_reader_eof(const sexb_bytecode_reader_t* r) {
    return r->pos >= r->size;
}

static inline uint8_t sexb_read_u8(sexb_bytecode_reader_t* r) {
    if (r->pos >= r->size) return 0;
    return r->bytecode[r->pos++];
}

static inline uint16_t sexb_read_u16(sexb_bytecode_reader_t* r) {
    if (r->pos + 2 > r->size) return 0;
    uint16_t v = r->bytecode[r->pos] | (r->bytecode[r->pos + 1] << 8);
    r->pos += 2;
    return v;
}

static inline uint32_t sexb_read_u32(sexb_bytecode_reader_t* r) {
    if (r->pos + 4 > r->size) return 0;
    uint32_t v = r->bytecode[r->pos] |
                 (r->bytecode[r->pos + 1] << 8) |
                 (r->bytecode[r->pos + 2] << 16) |
                 (r->bytecode[r->pos + 3] << 24);
    r->pos += 4;
    return v;
}

static inline int32_t sexb_read_i32(sexb_bytecode_reader_t* r) {
    return (int32_t)sexb_read_u32(r);
}

static inline float sexb_read_f32(sexb_bytecode_reader_t* r) {
    uint32_t bits = sexb_read_u32(r);
    float f;
    memcpy(&f, &bits, sizeof(f));
    return f;
}

static inline uint64_t sexb_read_u64(sexb_bytecode_reader_t* r) {
    uint32_t lo = sexb_read_u32(r);
    uint32_t hi = sexb_read_u32(r);
    return (uint64_t)lo | ((uint64_t)hi << 32);
}

static inline int64_t sexb_read_i64(sexb_bytecode_reader_t* r) {
    return (int64_t)sexb_read_u64(r);
}

static inline double sexb_read_f64(sexb_bytecode_reader_t* r) {
    uint64_t bits = sexb_read_u64(r);
    double d;
    memcpy(&d, &bits, sizeof(d));
    return d;
}

static inline bool sexb_read_node_header(sexb_bytecode_reader_t* r, sexb_node_header_t* hdr) {
    if (r->pos + sizeof(sexb_node_header_t) > r->size) return false;
    hdr->func_hash = sexb_read_u32(r);
    hdr->func_type = sexb_read_u8(r);
    hdr->param_count = sexb_read_u8(r);
    hdr->bytecode_size = sexb_read_u16(r);
    return true;
}

static inline void sexb_skip_node(sexb_bytecode_reader_t* r, const sexb_node_header_t* hdr) {
    r->pos += hdr->bytecode_size - sizeof(sexb_node_header_t);
}

// ============================================================================
// HASH FUNCTION (FNV-1a 32-bit)
// ============================================================================

#define SEXB_FNV_OFFSET_BASIS 0x811c9dc5
#define SEXB_FNV_PRIME        0x01000193

static inline uint32_t sexb_hash32(const char* str) {
    uint32_t hash = SEXB_FNV_OFFSET_BASIS;
    while (*str) {
        hash ^= (uint8_t)*str++;
        hash *= SEXB_FNV_PRIME;
    }
    return hash;
}

static inline uint32_t sexb_hash32_n(const char* str, size_t len) {
    uint32_t hash = SEXB_FNV_OFFSET_BASIS;
    for (size_t i = 0; i < len; i++) {
        hash ^= (uint8_t)str[i];
        hash *= SEXB_FNV_PRIME;
    }
    return hash;
}

#ifdef __cplusplus
}
#endif

#endif // S_EXPR_BINARY_H