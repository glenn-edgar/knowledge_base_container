/*
 * kb_data_structures.c
 * Knowledge Base C Port — Aggregator facade
 *
 * Mirrors LuaJIT kb_data_structures.lua.
 * Creates and wires up all subsystem modules.
 */

#include "kb_data_structures.h"
#include "kb_uuid.h"

#include <stdlib.h>

struct kb_data_structures {
    kb_search_t           *search;
    kb_bit_structures_t   *bit_structures;
    kb_status_table_t     *status;
    kb_stream_t           *stream;
    kb_job_queue_t        *job_queue;
    kb_link_table_t       *link_table;
    kb_link_mount_table_t *link_mount_table;
    kb_rpc_server_t       *rpc_server;
    kb_rpc_client_t       *rpc_client;
    bool                   owns_search;
};

static void init_subsystems(kb_ds_t *ds, const char *database)
{
    sqlite3 *db = kb_search_get_db(ds->search);

    ds->bit_structures  = kb_bit_structures_create(ds->search, database);
    ds->status          = kb_status_table_create(ds->search, database);
    ds->stream          = kb_stream_create(ds->search, database);
    ds->job_queue       = kb_job_queue_create(ds->search, database);
    ds->link_table      = kb_link_table_create(db, database);
    ds->link_mount_table = kb_link_mount_table_create(db, database);
    ds->rpc_server      = kb_rpc_server_create(ds->search, database);
    ds->rpc_client      = kb_rpc_client_create(ds->search, database);
}

kb_ds_t *kb_ds_create(const char *db_path, const char *database,
                       const char *ltree_path)
{
    if (!db_path || !database) return NULL;

    /* Seed UUID generator once */
    kb_uuid_seed();

    kb_ds_t *ds = (kb_ds_t *)calloc(1, sizeof(*ds));
    if (!ds) return NULL;

    ds->search = kb_search_create(db_path, database, ltree_path);
    if (!ds->search) {
        free(ds);
        return NULL;
    }
    ds->owns_search = true;

    init_subsystems(ds, database);
    return ds;
}

kb_ds_t *kb_ds_create_from_db(sqlite3 *db, const char *database)
{
    if (!db || !database) return NULL;

    kb_uuid_seed();

    kb_ds_t *ds = (kb_ds_t *)calloc(1, sizeof(*ds));
    if (!ds) return NULL;

    ds->search = kb_search_create_from_db(db, database);
    if (!ds->search) {
        free(ds);
        return NULL;
    }
    ds->owns_search = true;  /* we created it, so we destroy it */

    init_subsystems(ds, database);
    return ds;
}

void kb_ds_destroy(kb_ds_t *ds)
{
    if (!ds) return;

    kb_rpc_client_destroy(ds->rpc_client);
    kb_rpc_server_destroy(ds->rpc_server);
    kb_link_mount_table_destroy(ds->link_mount_table);
    kb_link_table_destroy(ds->link_table);
    kb_job_queue_destroy(ds->job_queue);
    kb_stream_destroy(ds->stream);
    kb_status_table_destroy(ds->status);
    kb_bit_structures_destroy(ds->bit_structures);

    if (ds->owns_search) {
        kb_search_destroy(ds->search);
    }

    free(ds);
}

/* ================================================================
 * Subsystem accessors
 * ================================================================ */

kb_search_t *kb_ds_search(kb_ds_t *ds)
{
    return ds ? ds->search : NULL;
}

kb_bit_structures_t *kb_ds_bit_structures(kb_ds_t *ds)
{
    return ds ? ds->bit_structures : NULL;
}

kb_status_table_t *kb_ds_status(kb_ds_t *ds)
{
    return ds ? ds->status : NULL;
}

kb_stream_t *kb_ds_stream(kb_ds_t *ds)
{
    return ds ? ds->stream : NULL;
}

kb_job_queue_t *kb_ds_job_queue(kb_ds_t *ds)
{
    return ds ? ds->job_queue : NULL;
}

kb_link_table_t *kb_ds_link_table(kb_ds_t *ds)
{
    return ds ? ds->link_table : NULL;
}

kb_link_mount_table_t *kb_ds_link_mount_table(kb_ds_t *ds)
{
    return ds ? ds->link_mount_table : NULL;
}

kb_rpc_server_t *kb_ds_rpc_server(kb_ds_t *ds)
{
    return ds ? ds->rpc_server : NULL;
}

kb_rpc_client_t *kb_ds_rpc_client(kb_ds_t *ds)
{
    return ds ? ds->rpc_client : NULL;
}

sqlite3 *kb_ds_get_db(kb_ds_t *ds)
{
    return ds ? kb_search_get_db(ds->search) : NULL;
}
