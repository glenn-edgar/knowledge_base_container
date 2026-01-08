#pragma once
#include "chain_tree.h"

// Config Data
extern const json_record_t     g_cfg_recs[];
extern const uint32_t          g_cfg_recs_len;
extern const json_path_index_t g_cfg_index[];
extern const uint32_t          g_cfg_index_len;

// Layout & Logic Data
extern const uint32_t          g_arena_sizes[BITSPACE_COUNT];
extern const bitspace_rule_t   g_bitspace_rules[BITSPACE_COUNT];
extern const node_layout_t     g_node_layouts[];
extern const uint32_t          g_node_layouts_len;
