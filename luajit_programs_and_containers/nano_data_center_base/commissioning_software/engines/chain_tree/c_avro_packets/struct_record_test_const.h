// struct_record_test_const.h
// Generated const packets - DO NOT EDIT
#pragma once

#include "struct_record_test.h"

static const comm_manifest_v1_packet_t rover_1_manifest = {
    .header = {
        .timestamp   = 0.0,
        .schema_hash = COMM_MANIFEST_V1_SCHEMA_HASH,
        .seq         = 0,
        .source_node = 0,
    },
    .data = {
        .version = 1U,
        .bus_id = 0U,
        .slave_count = 2U,
        .slaves = { { .mcu = 1U, .addr = 1U, .physics_model_id = 3735928559U }, { .mcu = 1U, .addr = 2U, .physics_model_id = 3405691582U } },
        .tunables = { .max_miss = 3U, .tick_period_ms = 20U, .join_timeout_ms = 500U },
    },
};

