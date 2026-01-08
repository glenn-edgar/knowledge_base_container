#include "chain_tree.h"
#include <string.h>
#include <stdarg.h>

// Extern internals from chain_tree.c if needed, or expose via header.
// Assuming we link together, we use the public API/structs.

// --- Helper: Read/Write Raw Bits ---

static bool get_bit_raw(chain_tree_t* tree, const node_layout_t* node, int bank, int bit) {
    int32_t offset = node->offsets[bank];
    if (offset < 0) return false;
    
    uint8_t* byte_ptr = tree->arenas[bank] + offset + (bit / 8);
    return (*byte_ptr >> (bit % 8)) & 1;
}

static void set_bit_raw(chain_tree_t* tree, const node_layout_t* node, int bank, int bit, bool val) {
    int32_t offset = node->offsets[bank];
    if (offset < 0) return;
    
    uint8_t* byte_ptr = tree->arenas[bank] + offset + (bit / 8);
    uint8_t  mask = 1 << (bit % 8);
    
    if (val) *byte_ptr |= mask;
    else     *byte_ptr &= ~mask;
}

// --- Helper: Merge Logic ---

static bool apply_rule(bool current_agg, bool child_val, merge_op_t op, bool is_first) {
    switch (op) {
        case MERGE_OR:
            return is_first ? child_val : (current_agg || child_val);
        case MERGE_AND:
            return is_first ? child_val : (current_agg && child_val);
        case MERGE_PRIORITY:
             // Simple priority: Parent is High if ANY child is High (Similar to OR for bools)
             // Complex priority requires integer values, not bits.
             return is_first ? child_val : (current_agg || child_val); 
    }
    return current_agg;
}

// --- The Core: Recursive Rollup ---

static void propagate_node(chain_tree_t* tree, int node_idx, int bank, int bit) {
    // 1. Sanity Check
    if (node_idx < 0) return;
    const node_layout_t* node = &tree->desc->layouts[node_idx];

    // 2. Stop if Root
    if (node->parent_idx < 0) return;
    const node_layout_t* parent = &tree->desc->layouts[node->parent_idx];

    // 3. Check Rule
    bitspace_rule_t rule = tree->desc->rules[bank];
    
    // 4. Recalculate Parent State based on ALL siblings
    //    (Because if we cleared a bit, we need to know if a sibling is still holding it high)
    
    bool new_parent_val = false;
    int sibling_idx = parent->first_child_idx;
    bool is_first = true;

    while (sibling_idx >= 0) {
        const node_layout_t* sibling = &tree->desc->layouts[sibling_idx];
        
        // Read sibling's value
        bool s_val = get_bit_raw(tree, sibling, bank, bit);
        
        // Merge
        new_parent_val = apply_rule(new_parent_val, s_val, rule.op, is_first);
        
        // Next
        sibling_idx = sibling->next_sibling_idx;
        is_first = false;
    }

    // 5. Check if Parent actually changed
    bool current_parent_val = get_bit_raw(tree, parent, bank, bit);
    
    if (current_parent_val != new_parent_val) {
        set_bit_raw(tree, parent, bank, bit, new_parent_val);
        
        // 6. Recurse Up
        propagate_node(tree, node->parent_idx, bank, bit);
    }
}

// --- Public API ---

// We need to re-implement the hash lookup helper here or expose it
// For brevity, assuming find_layout_idx and hash_vprintf are available/linked.
extern int find_layout_idx(const chain_desc_t* desc, uint32_t hash);
extern uint32_t hash_vprintf(const char* fmt, va_list args);

void chain_set_bit(chain_tree_t* tree, int bank_id, int bit_idx, bool val, const char* fmt, ...) {
    // 1. Hash Path
    va_list args;
    va_start(args, fmt);
    uint32_t h = hash_vprintf(fmt, args);
    va_end(args);

    // 2. Find Node
    int node_idx = find_layout_idx(tree->desc, h);
    if (node_idx < 0) return;
    const node_layout_t* node = &tree->desc->layouts[node_idx];

    // 3. Write Bit (Leaf / Source)
    set_bit_raw(tree, node, bank_id, bit_idx, val);

    // 4. Trigger Logic
    propagate_node(tree, node_idx, bank_id, bit_idx);
}