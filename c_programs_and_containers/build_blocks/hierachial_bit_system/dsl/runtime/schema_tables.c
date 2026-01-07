/* Auto-generated. Do not edit. */
#include "schema_tables.h"

const uint16_t g_schema_parents[SCHEMA_NODE_COUNT] = {
  0,
  0,
};

const schema_bank_desc_t g_schema_banks[SCHEMA_BANK_COUNT] = {
  { .node_id=1, .bitspace_id=6, .bits=128, .merge=3, .boundary=2 },
  { .node_id=1, .bitspace_id=1, .bits=64, .merge=1, .boundary=3 },
  { .node_id=1, .bitspace_id=4, .bits=32, .merge=1, .boundary=2 },
  { .node_id=1, .bitspace_id=5, .bits=32, .merge=2, .boundary=2 },
  { .node_id=1, .bitspace_id=3, .bits=32, .merge=1, .boundary=1 },
  { .node_id=1, .bitspace_id=2, .bits=32, .merge=1, .boundary=2 },
  { .node_id=2, .bitspace_id=6, .bits=256, .merge=3, .boundary=2 },
  { .node_id=2, .bitspace_id=1, .bits=128, .merge=1, .boundary=3 },
  { .node_id=2, .bitspace_id=4, .bits=64, .merge=1, .boundary=2 },
  { .node_id=2, .bitspace_id=5, .bits=64, .merge=2, .boundary=2 },
  { .node_id=2, .bitspace_id=3, .bits=64, .merge=1, .boundary=1 },
  { .node_id=2, .bitspace_id=2, .bits=64, .merge=1, .boundary=2 },
};

const schema_bit_desc_t g_schema_bits[SCHEMA_BIT_COUNT] = {
  { .bank_id=1, .local_idx=0 },
  { .bank_id=1, .local_idx=1 },
  { .bank_id=2, .local_idx=63 },
  { .bank_id=7, .local_idx=0 },
  { .bank_id=7, .local_idx=1 },
  { .bank_id=7, .local_idx=2 },
  { .bank_id=8, .local_idx=0 },
  { .bank_id=8, .local_idx=1 },
  { .bank_id=8, .local_idx=127 },
};
