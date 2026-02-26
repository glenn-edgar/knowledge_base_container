/*
 * kb_data_structures.h
 * Knowledge Base C Port — Aggregator facade
 *
 * Mirrors LuaJIT kb_data_structures.lua / Python KB_Data_Structures.
 * Composes all subsystem modules into a single handle with
 * convenience accessors for each subsystem.
 *
 * Usage:
 *   kb_ds_t *ds = kb_ds_create("knowledge_base.db", "knowledge_base", "./ltree");
 *   kb_status_set_data(kb_ds_status(ds), "some.path", "{\"val\":42}");
 *   kb_ds_destroy(ds);
 */

#ifndef KB_DATA_STRUCTURES_H
#define KB_DATA_STRUCTURES_H

#include "kb_common.h"
#include "kb_query_support.h"
#include "kb_bit_structures.h"
#include "kb_status_table.h"
#include "kb_stream.h"
#include "kb_job_queue.h"
#include "kb_link_table.h"
#include "kb_link_mount_table.h"
#include "kb_rpc_server.h"
#include "kb_rpc_client.h"

#ifdef __cplusplus
extern "C" {
#endif

typedef struct kb_data_structures kb_ds_t;

/*
 * Create the aggregator. Opens the database and creates all subsystems.
 * ltree_path may be NULL to skip ltree extension loading.
 */
kb_ds_t *kb_ds_create(const char *db_path, const char *database,
                       const char *ltree_path);

/*
 * Create from an existing open sqlite3 handle (does not own/close it).
 */
kb_ds_t *kb_ds_create_from_db(sqlite3 *db, const char *database);

/*
 * Destroy and free all resources.
 */
void kb_ds_destroy(kb_ds_t *ds);

/* ================================================================
 * Subsystem accessors
 * ================================================================ */

kb_search_t           *kb_ds_search(kb_ds_t *ds);
kb_bit_structures_t   *kb_ds_bit_structures(kb_ds_t *ds);
kb_status_table_t     *kb_ds_status(kb_ds_t *ds);
kb_stream_t           *kb_ds_stream(kb_ds_t *ds);
kb_job_queue_t        *kb_ds_job_queue(kb_ds_t *ds);
kb_link_table_t       *kb_ds_link_table(kb_ds_t *ds);
kb_link_mount_table_t *kb_ds_link_mount_table(kb_ds_t *ds);
kb_rpc_server_t       *kb_ds_rpc_server(kb_ds_t *ds);
kb_rpc_client_t       *kb_ds_rpc_client(kb_ds_t *ds);

/* Get the underlying sqlite3 handle */
sqlite3 *kb_ds_get_db(kb_ds_t *ds);

#ifdef __cplusplus
}
#endif

#endif /* KB_DATA_STRUCTURES_H */
