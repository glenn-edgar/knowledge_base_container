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

// Function type tags
typedef enum {
    SEXB_FUNC_ONESHOT   = 0x01,         // o_call
    SEXB_FUNC_MAIN      = 0x02,         // m_call
    SEXB_FUNC_PRED      = 0x03,         // p_call
    SEXB_FUNC_PT_MAIN   = 0x04,         // pt_m_call (protothread)
    SEXB_FUNC_INIT_ONE  = 0x05,         // io_call (init oneshot)
    SEXB_FUNC_BIT_PRED  = 0x06,         // p_call_bit (bit block)
} sexb_func_type_t;

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

// Bytecode parameter (variable size)
// Format: [opcode:u8][data:variable]
// Data sizes:
//   INT/UINT/FLOAT/STR_IDX/FIELD_REF/RESULT: 4 bytes
//   INT64/UINT64/DOUBLE: 8 bytes
//   NESTED_REF: 4 bytes (path_hash) + 2 bytes (depth)
//   CALL_START: 4 bytes (func_hash) + 1 byte (func_type)
//   LIST_START/END, CALL_END: 0 bytes

#pragma pack(pop)

// ============================================================================
// RUNTIME STRUCTURES (for loaded module in RAM)
// ============================================================================

// Forward declarations
typedef struct sexb_module sexb_module_t;
typedef struct sexb_tree sexb_tree_t;
typedef struct sexb_record sexb_record_t;
typedef struct sexb_field sexb_field_t;
typedef struct sexb_const sexb_const_t;

// Loaded field (resolved pointers)
struct sexb_field {
    uint32_t name_hash;
    uint8_t  type_tag;
    uint8_t  flags;
    uint16_t offset;
    uint16_t size;
    uint16_t aux;                       // Array len or target record index
};

// Loaded record (resolved pointers)
struct sexb_record {
    uint32_t name_hash;
    uint16_t field_count;
    uint16_t size;
    const sexb_field_t* fields;         // Pointer to field array
};

// Loaded tree (resolved pointers)
struct sexb_tree {
    uint32_t name_hash;
    uint16_t record_index;
    uint16_t node_count;
    const uint8_t* bytecode;            // Pointer to bytecode
    uint32_t bytecode_size;
};

// Loaded constant (resolved pointers)
struct sexb_const {
    uint32_t name_hash;
    uint16_t record_index;
    uint16_t data_size;
    const void* data;                   // Pointer to constant data
};

// Loaded module (master structure)
struct sexb_module {
    uint32_t name_hash;
    uint16_t flags;
    
    // Counts
    uint16_t tree_count;
    uint16_t record_count;
    uint16_t string_count;
    uint16_t const_count;
    uint16_t oneshot_count;
    uint16_t main_count;
    uint16_t pred_count;
    
    // Resolved tables
    const sexb_tree_t* trees;
    const sexb_record_t* records;
    const sexb_const_t* constants;
    
    // String blob (length-prefixed strings)
    const uint8_t* string_blob;
    uint32_t string_blob_size;
    
    // Function hash tables
    const uint32_t* oneshot_hashes;
    const uint32_t* main_hashes;
    const uint32_t* pred_hashes;
    
    // Raw binary (for reference)
    const uint8_t* raw_data;
    uint32_t raw_size;
};

// ============================================================================
// LOADER API
// ============================================================================

// Validation result
typedef enum {
    SEXB_OK = 0,
    SEXB_ERR_NULL_PTR,
    SEXB_ERR_TOO_SMALL,
    SEXB_ERR_BAD_MAGIC,
    SEXB_ERR_BAD_VERSION,
    SEXB_ERR_BAD_SIZE,
    SEXB_ERR_BAD_OFFSET,
    SEXB_ERR_ALLOC_FAILED,
} sexb_error_t;

// Validate binary data without loading
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

// Get module name hash without full load
static inline uint32_t sexb_get_name_hash(const void* data) {
    const sexb_header_t* hdr = (const sexb_header_t*)data;
    return hdr->module_name_hash;
}

// Get string from string blob by index
static inline const char* sexb_get_string(const sexb_module_t* mod, uint16_t index) {
    if (!mod || !mod->string_blob || index >= mod->string_count) {
        return NULL;
    }
    
    // Walk through length-prefixed strings
    const uint8_t* ptr = mod->string_blob;
    const uint8_t* end = ptr + mod->string_blob_size;
    
    for (uint16_t i = 0; i < index && ptr < end; i++) {
        uint16_t len = ptr[0] | (ptr[1] << 8);
        ptr += 2 + len;
        // Align to 4 bytes
        ptr += (4 - ((uintptr_t)ptr & 3)) & 3;
    }
    
    if (ptr >= end) return NULL;
    
    // Return pointer to string data (skip length)
    return (const char*)(ptr + 2);
}

// Get string length
static inline uint16_t sexb_get_string_len(const sexb_module_t* mod, uint16_t index) {
    if (!mod || !mod->string_blob || index >= mod->string_count) {
        return 0;
    }
    
    const uint8_t* ptr = mod->string_blob;
    const uint8_t* end = ptr + mod->string_blob_size;
    
    for (uint16_t i = 0; i < index && ptr < end; i++) {
        uint16_t len = ptr[0] | (ptr[1] << 8);
        ptr += 2 + len;
        ptr += (4 - ((uintptr_t)ptr & 3)) & 3;
    }
    
    if (ptr >= end) return 0;
    
    return ptr[0] | (ptr[1] << 8);
}

// Find tree by hash
static inline const sexb_tree_t* sexb_find_tree(const sexb_module_t* mod, uint32_t name_hash) {
    if (!mod || !mod->trees) return NULL;
    
    for (uint16_t i = 0; i < mod->tree_count; i++) {
        if (mod->trees[i].name_hash == name_hash) {
            return &mod->trees[i];
        }
    }
    return NULL;
}

// Find record by hash
static inline const sexb_record_t* sexb_find_record(const sexb_module_t* mod, uint32_t name_hash) {
    if (!mod || !mod->records) return NULL;
    
    for (uint16_t i = 0; i < mod->record_count; i++) {
        if (mod->records[i].name_hash == name_hash) {
            return &mod->records[i];
        }
    }
    return NULL;
}

// Find field in record by hash
static inline const sexb_field_t* sexb_find_field(const sexb_record_t* rec, uint32_t name_hash) {
    if (!rec || !rec->fields) return NULL;
    
    for (uint16_t i = 0; i < rec->field_count; i++) {
        if (rec->fields[i].name_hash == name_hash) {
            return &rec->fields[i];
        }
    }
    return NULL;
}

// Find constant by hash
static inline const sexb_const_t* sexb_find_const(const sexb_module_t* mod, uint32_t name_hash) {
    if (!mod || !mod->constants) return NULL;
    
    for (uint16_t i = 0; i < mod->const_count; i++) {
        if (mod->constants[i].name_hash == name_hash) {
            return &mod->constants[i];
        }
    }
    return NULL;
}

// Check if function hash is in oneshot table
static inline bool sexb_is_oneshot(const sexb_module_t* mod, uint32_t func_hash) {
    if (!mod || !mod->oneshot_hashes) return false;
    for (uint16_t i = 0; i < mod->oneshot_count; i++) {
        if (mod->oneshot_hashes[i] == func_hash) return true;
    }
    return false;
}

// Check if function hash is in main table
static inline bool sexb_is_main(const sexb_module_t* mod, uint32_t func_hash) {
    if (!mod || !mod->main_hashes) return false;
    for (uint16_t i = 0; i < mod->main_count; i++) {
        if (mod->main_hashes[i] == func_hash) return true;
    }
    return false;
}

// Check if function hash is in pred table
static inline bool sexb_is_pred(const sexb_module_t* mod, uint32_t func_hash) {
    if (!mod || !mod->pred_hashes) return false;
    for (uint16_t i = 0; i < mod->pred_count; i++) {
        if (mod->pred_hashes[i] == func_hash) return true;
    }
    return false;
}

// ============================================================================
// BYTECODE READER API
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

// Read node header
static inline bool sexb_read_node_header(sexb_bytecode_reader_t* r, sexb_node_header_t* hdr) {
    if (r->pos + sizeof(sexb_node_header_t) > r->size) return false;
    hdr->func_hash = sexb_read_u32(r);
    hdr->func_type = sexb_read_u8(r);
    hdr->param_count = sexb_read_u8(r);
    hdr->bytecode_size = sexb_read_u16(r);
    return true;
}

// Skip node (after reading header)
static inline void sexb_skip_node(sexb_bytecode_reader_t* r, const sexb_node_header_t* hdr) {
    r->pos += hdr->bytecode_size - sizeof(sexb_node_header_t);
}

// ============================================================================
// HASH FUNCTION (FNV-1a 32-bit, same as DSL)
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


