#include "chain_tree.h"
#include <string.h>
#include <stdarg.h>

/* =========================================================================
 * 1. CONSTANTS & MACROS
 * ========================================================================= */
#define FNV1A_PRIME 0x01000193U
#define FNV1A_INIT  0x811c9dc5U

/* =========================================================================
 * 2. INTERNAL HELPERS (Hashing & Lookup)
 * ========================================================================= */

// Incremental Hashing Helper
static inline void hash_byte(uint32_t* h, char b) {
    *h ^= (uint8_t)b;
    *h *= FNV1A_PRIME;
}

// Internal: Exposed for use by chain_tree_logic.c
// Computes hash from a printf-style format string without allocating memory
uint32_t hash_vprintf(const char* fmt, va_list args) {
    uint32_t h = FNV1A_INIT;
    const char* p = fmt;
    while(*p) {
        if(*p != '%') {
            hash_byte(&h, *p++);
            continue;
        }
        p++; // Skip '%'
        switch(*p) {
            case 'd': // Int
            case 'i': {
                int val = va_arg(args, int);
                char buf[16], *ptr = &buf[15]; *ptr=0;
                int t = (val < 0) ? -val : val;
                do { *--ptr = (t % 10) + '0'; t /= 10; } while(t);
                if(val < 0) *--ptr='-';
                while(*ptr) hash_byte(&h, *ptr++);
                break;
            }
            case 'u': { // Unsigned Int
                unsigned int val = va_arg(args, unsigned int);
                char buf[16], *ptr = &buf[15]; *ptr=0;
                do { *--ptr = (val % 10) + '0'; val /= 10; } while(val);
                while(*ptr) hash_byte(&h, *ptr++);
                break;
            }
            case 's': { // String
                const char* s = va_arg(args, const char*);
                while(s && *s) hash_byte(&h, *s++);
                break;
            }
            case '%': { // Escaped '%'
                hash_byte(&h, '%');
                break;
            }
            default: { // Unknown, treat as literal
                hash_byte(&h, *p);
                break;
            }
        }
        p++;
    }
    return h;
}

// Internal: Exposed for use by chain_tree_logic.c
// Binary Search to find the node index in the sorted layout array
int find_layout_idx(const chain_desc_t* desc, uint32_t hash) {
    int l = 0, r = desc->layout_count - 1;
    while (l <= r) {
        int mid = l + (r - l) / 2;
        if (desc->layouts[mid].hash == hash) return mid;
        if (desc->layouts[mid].hash < hash) l = mid + 1; else r = mid - 1;
    }
    return -1;
}

/* =========================================================================
 * 3. LIFECYCLE MANAGEMENT
 * ========================================================================= */

void chain_tree_init(chain_tree_t* tree, const chain_desc_t* desc, sys_handle_t sys_h) {
    tree->sys_h = sys_h;
    tree->desc  = desc;

    // 1. Allocate the "Spine" (Array of Pointers)
    // Size = Number of Bitspaces * Size of a Pointer
    size_t ptr_array_sz = sizeof(uint8_t*) * desc->bitspace_count;
    tree->arenas = (uint8_t**)alloc(sys_h, ptr_array_sz);
    
    if (!tree->arenas) return;

    // 2. Allocate the "Body" (The actual Bitspaces)
    for (uint32_t i = 0; i < desc->bitspace_count; i++) {
        uint32_t sz = desc->arena_sizes[i];
        
        if (sz > 0) {
            tree->arenas[i] = (uint8_t*)alloc(sys_h, sz);
            // Always zero-init to ensure clean state
            if (tree->arenas[i]) {
                memset(tree->arenas[i], 0, sz);
            }
        } else {
            tree->arenas[i] = NULL;
        }
    }
}

void chain_tree_destroy(chain_tree_t* tree) {
    if (!tree->arenas) return;

    // 1. Free individual arenas
    // Note: This assumes the user's 'free' function can handle the pointer directly
    // or tracks allocations via 'sys_h'. 
    // If your allocator requires size, you can access tree->desc->arena_sizes[i].
    /* for (uint32_t i = 0; i < tree->desc->bitspace_count; i++) {
           if (tree->arenas[i]) free(tree->sys_h); 
       }
    */

    // 2. Free the user handle/context itself if strictly required by the design pattern,
    // otherwise just free the spine.
    free(tree->sys_h); 
    
    tree->arenas = NULL;
    tree->desc = NULL;
}

/* =========================================================================
 * 4. PUBLIC ACCESSORS
 * ========================================================================= */

uint8_t* chain_get_bits(chain_tree_t* tree, int bank_id, const char* fmt, ...) {
    // Safety check on bank ID
    if (bank_id < 0 || bank_id >= tree->desc->bitspace_count) return NULL;

    // 1. Calculate Hash
    va_list args;
    va_start(args, fmt);
    uint32_t h = hash_vprintf(fmt, args);
    va_end(args);

    // 2. Find Node Index
    int idx = find_layout_idx(tree->desc, h);
    if (idx < 0) return NULL;

    // 3. Look up Offset
    // Indirection: Layout -> Offsets Array -> Specific Bank Offset
    int32_t offset = tree->desc->layouts[idx].offsets[bank_id];
    
    // If offset is -1, this node does not participate in this bitspace
    if (offset < 0 || !tree->arenas[bank_id]) return NULL;

    // 4. Return Pointer
    return tree->arenas[bank_id] + offset;
}