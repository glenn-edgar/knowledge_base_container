/* main.c */
#include <stdio.h>
#include "cfg_hierarchical_bitmask.h"
#include "schema_tables.h"  // Generated for this specific plant (e.g., out/plant_a/schema_tables.h)

/* Schema descriptor — safe because cfg_bank_desc_t matches generated layout exactly */
static const cfg_schema_desc_t schema_plant_a = {
  .banks      = (const cfg_bank_desc_t *)g_schema_banks,   // Cast needed: different type names
  .parents    = g_schema_parents,
  .bank_count = SCHEMA_BANK_COUNT,
  .node_count = SCHEMA_NODE_COUNT,
  /* Optional: only if you added .bits and .bit_count to cfg_schema_desc_t */
  // .bits       = (const cfg_bit_desc_t *)g_schema_bits,
  // .bit_count  = SCHEMA_BIT_COUNT,
};

/* Bump allocator example */
typedef struct {
  uint8_t *ptr;
  uint8_t *end;
} bump_t;

static void* bump_alloc(void *ctx, size_t size) {
  bump_t *b = (bump_t *)ctx;
  /* Align to 8-byte boundary for safety (optional but recommended) */
  size = (size + 7u) & ~7u;
  if (b->ptr + size > b->end) return NULL;
  void *p = b->ptr;
  b->ptr += size;
  return p;
}

static void bump_noop(void *ctx, void *ptr) {
  (void)ctx; (void)ptr;  /* Nothing to free */
}

int main(void) {
  /* Static arena on stack or global */
  uint8_t arena[64 * 1024];
  bump_t bump = { .ptr = arena, .end = arena + sizeof(arena) };

  /* Create instance using bump allocator */
  cfg_hierarchical_bitmask_t h = cfg_hierarchical_create(
      &schema_plant_a,
      &bump,          /* alloc_ctx */
      bump_alloc,     /* alloc_fn */
      NULL,           /* dealloc_ctx — not needed */
      bump_noop       /* dealloc_fn — no-op */
  );

  if (!h) {
    /* Handle allocation failure (rare with static arena) */
    return 1;
  }

  printf("=== Alarm propagation test ===\n");

  /* Step 1: Set an alarm bit in a child node
   *     bank_id = index of the child's ALARM bank in g_schema_banks[]
   *     bit_idx = local bit index of the alarm (e.g., OverTorque = 0)
   *     Example: assume bank 4 is Robot2.ALARM (check your generated schema_tables.h)
   */
  cfg_hierarchical_set(h, 4, 0, true);   /* Robot2 → OverTorque alarm = active */
  
  /* Step 2: Propagate — merges child alarms up the hierarchy using OR */
  cfg_hierarchical_propagate_tick(h);
  
  /* Step 3: Read the parent's derived "AnyActive" summary bit
   *     Usually reserved at the highest bit in the parent ALARM bank
   *     Example: assume bank 0 is the parent (Plant.Line1.Cell3) ALARM bank
   *     and bit 127 is the exported "AnyActive" rollup
   */
  bool any_alarm_active = cfg_hierarchical_get(h, 0, 127);
  
  printf("Parent AnyAlarm active: %s\n", any_alarm_active ? "YES" : "NO");

  /* Force-test: set a bit in every ALARM bank */
for (uint16_t i = 0; i < SCHEMA_BANK_COUNT; ++i) {
    if (g_schema_banks[i].bitspace_id == 1) {  // 1 = ALARM (check your bitspace IDs)
      cfg_hierarchical_set(h, i, 0, true);
    }
  }
  cfg_hierarchical_propagate_tick(h);
  printf("Scanning bitspace IDs...\n");
for (uint16_t i = 0; i < SCHEMA_BANK_COUNT; ++i) {
  printf("Bank %u: bitspace_id=%u, bits=%u, node_id=%u\n",
         i, g_schema_banks[i].bitspace_id, g_schema_banks[i].bits, g_schema_banks[i].node_id);
}
  bool root_alarm = cfg_hierarchical_get(h, 0, 127);  // root or top-level AnyActive
  printf("Root alarm active: %s\n", root_alarm ? "YES" : "NO");

  return 0;
}