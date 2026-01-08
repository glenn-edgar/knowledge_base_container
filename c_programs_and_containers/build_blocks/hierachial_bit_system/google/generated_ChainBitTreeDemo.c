#include "generated_ChainBitTreeDemo.h"

static const uint32_t s_arena_sizes[] = {
  48,
  24,
  0,
  0,
  8,
  8,
};

static const bitspace_rule_t s_rules[] = {
  { .op = MERGE_PRIORITY }, // STATE
  { .op = MERGE_OR }, // ALARM
  { .op = MERGE_OR }, // INHIBIT
  { .op = MERGE_AND }, // PERMIT
  { .op = MERGE_OR }, // CMD_REQ
  { .op = MERGE_OR }, // CMD_ACK
};

static const int32_t s_all_offsets[] = {
  // Plant.Line1.Cell3.Robot2
  0, 0, -1, -1, 0, 0, 
  // Plant.Line1.Cell3.ConvA
  32, 16, -1, -1, -1, -1, 
};

static const node_layout_t s_layouts[] = {
  { .hash=0x0AE45A6E, .parent_idx=-1, .first_child_idx=-1, .next_sibling_idx=-1, .offsets=&s_all_offsets[0] }, // Plant.Line1.Cell3.Robot2
  { .hash=0x2DD60FC5, .parent_idx=-1, .first_child_idx=-1, .next_sibling_idx=-1, .offsets=&s_all_offsets[6] }, // Plant.Line1.Cell3.ConvA
};

static const json_record_t s_cfg_recs[17] = {
  { .type=JSON_TYPE_INT32, .value.i32_value=2 }, // Plant.Line1.Cell3.ConvA.Config.Speed.Max
  { .type=JSON_TYPE_OBJECT, .value.container_count=1 }, // Plant.Line1.Cell3.ConvA.Config.Speed
  { .type=JSON_TYPE_OBJECT, .value.container_count=1 }, // Plant.Line1.Cell3.ConvA.Config
  { .type=JSON_TYPE_OBJECT, .value.container_count=1 }, // Plant.Line1.Cell3.ConvA
  { .type=JSON_TYPE_BOOL, .value.bool_value=1 }, // Plant.Line1.Cell3.Robot2.Config.Comm.Enabled
  { .type=JSON_TYPE_INT32, .value.i32_value=250 }, // Plant.Line1.Cell3.Robot2.Config.Comm.TimeoutMs
  { .type=JSON_TYPE_OBJECT, .value.container_count=2 }, // Plant.Line1.Cell3.Robot2.Config.Comm
  { .type=JSON_TYPE_FLOAT32, .value.f32_value=3.5000 }, // Plant.Line1.Cell3.Robot2.Config.Motion.MaxAccel
  { .type=JSON_TYPE_INT32, .value.i32_value=1200 }, // Plant.Line1.Cell3.Robot2.Config.Motion.MaxSpeed
  { .type=JSON_TYPE_OBJECT, .value.container_count=2 }, // Plant.Line1.Cell3.Robot2.Config.Motion
  { .type=JSON_TYPE_STRING_HASH, .value.hash32=0xFFFFFFFFE5500BFB }, // Plant.Line1.Cell3.Robot2.Config.Name
  { .type=JSON_TYPE_OBJECT, .value.container_count=3 }, // Plant.Line1.Cell3.Robot2.Config
  { .type=JSON_TYPE_OBJECT, .value.container_count=1 }, // Plant.Line1.Cell3.Robot2
  { .type=JSON_TYPE_OBJECT, .value.container_count=2 }, // Plant.Line1.Cell3
  { .type=JSON_TYPE_OBJECT, .value.container_count=1 }, // Plant.Line1
  { .type=JSON_TYPE_OBJECT, .value.container_count=1 }, // Plant
  { .type=JSON_TYPE_OBJECT, .value.container_count=1 }, // 
};

static const json_path_index_t s_cfg_index[16] = {
  { .hash=0xFFFFFFFF864218CE, .rec_idx=4 }, // Plant.Line1.Cell3.Robot2.Config
  { .hash=0xFFFFFFFF8A502FD3, .rec_idx=0 }, // Plant.Line1.Cell3.ConvA.Config
  { .hash=0xFFFFFFFF8DC56832, .rec_idx=0 }, // Plant
  { .hash=0xFFFFFFFF9CA0BE03, .rec_idx=10 }, // Plant.Line1.Cell3.Robot2.Config.Name
  { .hash=0xFFFFFFFF9DA83E7E, .rec_idx=7 }, // Plant.Line1.Cell3.Robot2.Config.Motion.MaxAccel
  { .hash=0xFFFFFFFFB7774D70, .rec_idx=4 }, // Plant.Line1.Cell3.Robot2.Config.Comm
  { .hash=0xFFFFFFFFC216D9E5, .rec_idx=8 }, // Plant.Line1.Cell3.Robot2.Config.Motion.MaxSpeed
  { .hash=0xFFFFFFFFD48C0E22, .rec_idx=7 }, // Plant.Line1.Cell3.Robot2.Config.Motion
  { .hash=0xFFFFFFFFD7CB9C1E, .rec_idx=0 }, // Plant.Line1.Cell3
  { .hash=0xFFFFFFFFFDF2B929, .rec_idx=5 }, // Plant.Line1.Cell3.Robot2.Config.Comm.TimeoutMs
  { .hash=0xFFFFFFFFFE5BB952, .rec_idx=0 }, // Plant.Line1.Cell3.ConvA.Config.Speed
  { .hash=0x0AE45A6E, .rec_idx=4 }, // Plant.Line1.Cell3.Robot2
  { .hash=0x2DD60FC5, .rec_idx=0 }, // Plant.Line1.Cell3.ConvA
  { .hash=0x4D4A3C22, .rec_idx=0 }, // Plant.Line1.Cell3.ConvA.Config.Speed.Max
  { .hash=0x65714B35, .rec_idx=0 }, // Plant.Line1
  { .hash=0x6C9714F7, .rec_idx=4 }, // Plant.Line1.Cell3.Robot2.Config.Comm.Enabled
};

  const chain_desc_t ChainBitTreeDemo_desc = {
    .schema_name = "ChainBitTreeDemo",
    .bitspace_count = 6,
    .arena_sizes = s_arena_sizes,
    .rules = s_rules,
    .layouts = s_layouts,
    .layout_count = 2,
    .cfg_recs = s_cfg_recs,
    .cfg_index = s_cfg_index,
    .cfg_index_len = 16
  };
  