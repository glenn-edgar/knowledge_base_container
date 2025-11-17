#include "system_def.h"
#include "kb_search.h"
#include <libpq-fe.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <uuid/uuid.h>
#include <stdbool.h>



static char *json_escape(const char *str) {
    if (!str) return NULL;
    size_t len = strlen(str);
    char *esc = malloc(len * 2 + 1);
    if (!esc) return NULL;
    char *p = esc;
    while (*str) {
        switch (*str) {
            case '"': *p++ = '\\'; *p++ = '"'; break;
            case '\\': *p++ = '\\'; *p++ = '\\'; break;
            case '\b': *p++ = '\\'; *p++ = 'b'; break;
            case '\f': *p++ = '\\'; *p++ = 'f'; break;
            case '\n': *p++ = '\\'; *p++ = 'n'; break;
            case '\r': *p++ = '\\'; *p++ = 'r'; break;
            case '\t': *p++ = '\\'; *p++ = 't'; break;
            default:
                if ((unsigned char)*str < 0x20) {
                    p += sprintf(p, "\\u%04x", (unsigned char)*str);
                } else {
                    *p++ = *str;
                }
                break;
        }
        str++;
    }
    *p = 0;
    return esc;
}

static char *json_object_str(const char *key, const char *value) {
    char *esc_key = json_escape(key);
    char *esc_value = json_escape(value);
    if (!esc_key || !esc_value) {
        free(esc_key);
        free(esc_value);
        return NULL;
    }
    size_t len = strlen(esc_key) + strlen(esc_value) + 20;
    char *str = malloc(len);
    if (!str) {
        free(esc_key);
        free(esc_value);
        return NULL;
    }
    snprintf(str, len, "{\"%s\": \"%s\"}", esc_key, esc_value);
    free(esc_key);
    free(esc_value);
    return str;
}

KBQuery *kb_query_new(const char *base_table) {
    KBQuery *q = malloc(sizeof(KBQuery));
    if (!q) return NULL;
    q->base_table = strdup(base_table);
    q->conditions = NULL;
    q->param_values = NULL;
    q->num_filters = 0;
    q->max_filters = 0;
    q->results = NULL;
    q->num_results = 0;
    return q;
}

void kb_query_free(KBQuery *q) {
    if (!q) return;
    for (int i = 0; i < q->num_filters; i++) {
        free(q->conditions[i]);
        free(q->param_values[i]);
    }
    free(q->conditions);
    free(q->param_values);
    for (int i = 0; i < q->num_results; i++) {
        free(q->results[i].knowledge_base);
        free(q->results[i].label);
        free(q->results[i].name);
        free(q->results[i].properties);
        free(q->results[i].data);
        free(q->results[i].path);
    }
    free(q->results);
    free(q->base_table);
    free(q);
}

void kb_query_clear_filters(KBQuery *q) {
    for (int i = 0; i < q->num_filters; i++) {
        free(q->conditions[i]);
        free(q->param_values[i]);
    }
    q->num_filters = 0;
    for (int i = 0; i < q->num_results; i++) {
        free(q->results[i].knowledge_base);
        free(q->results[i].label);
        free(q->results[i].name);
        free(q->results[i].properties);
        free(q->results[i].data);
        free(q->results[i].path);
    }
    free(q->results);
    q->results = NULL;
    q->num_results = 0;
}

static void add_filter(KBQuery *q, const char *cond_template, char *param_value) {
    if (q->num_filters >= q->max_filters) {
        int new_max = q->max_filters + 10;
        q->conditions = realloc(q->conditions, new_max * sizeof(char *));
        q->param_values = realloc(q->param_values, new_max * sizeof(char *));
        q->max_filters = new_max;
    }
    char cond[512];
    snprintf(cond, sizeof(cond), cond_template, q->num_filters + 1);
    q->conditions[q->num_filters] = strdup(cond);
    q->param_values[q->num_filters] = param_value;  // Takes ownership
    q->num_filters++;
}

void kb_query_search_kb(KBQuery *q, const char *knowledge_base) {
    add_filter(q, "knowledge_base = $%d", strdup(knowledge_base));
}

void kb_query_search_label(KBQuery *q, const char *label) {
    add_filter(q, "label = $%d", strdup(label));
}

void kb_query_search_name(KBQuery *q, const char *name) {
    add_filter(q, "name = $%d", strdup(name));
}

void kb_query_search_property_key(KBQuery *q, const char *key) {
    add_filter(q, "properties::jsonb ? $%d", strdup(key));
}

void kb_query_search_property_value(KBQuery *q, const char *key, const char *value) {
    char *json = json_object_str(key, value);
    if (json) {
        add_filter(q, "properties::jsonb @> $%d::jsonb", json);
    }
}

void kb_query_search_starting_path(KBQuery *q, const char *starting_path) {
    add_filter(q, "path <@ $%d", strdup(starting_path));
}

void kb_query_search_path(KBQuery *q, const char *path_expression) {
    add_filter(q, "path ~ $%d", strdup(path_expression));
}

int kb_query_execute(KBQuery *q, void *connection) {
    PGconn *conn = (PGconn *)connection;
    if (PQstatus(conn) != CONNECTION_OK) {
        fprintf(stderr, "Connection is not OK\n");
        return -1;
    }

    char *esc_table = PQescapeIdentifier(conn, q->base_table, strlen(q->base_table));
    if (!esc_table) {
        fprintf(stderr, "Failed to escape table name\n");
        return -1;
    }

    char *query = NULL;
    PGresult *res = NULL;

    if (q->num_filters == 0) {
        char simple_query[256];
        snprintf(simple_query, sizeof(simple_query), "SELECT * FROM %s", esc_table);
        res = PQexecParams(conn, simple_query, 0, NULL, NULL, NULL, NULL, 0);
    } else {
        size_t query_len = 1024 + q->num_filters * 256;
        query = malloc(query_len);
        if (!query) {
            PQfreemem(esc_table);
            return -1;
        }
        snprintf(query, query_len, "WITH base_data AS (SELECT * FROM %s)", esc_table);
        char prev[32] = "base_data";
        for (int i = 0; i < q->num_filters; i++) {
            char cte[512];
            snprintf(cte, sizeof(cte), ", filter_%d AS (SELECT * FROM %s WHERE %s)", i, prev, q->conditions[i]);
            strcat(query, cte);
            snprintf(prev, sizeof(prev), "filter_%d", i);
        }
        char final_select[64];
        snprintf(final_select, sizeof(final_select), " SELECT * FROM %s", prev);
        strcat(query, final_select);

        res = PQexecParams(conn, query, q->num_filters, NULL, (const char * const *)q->param_values, NULL, NULL, 0);
        free(query);
    }

    PQfreemem(esc_table);

    if (PQresultStatus(res) != PGRES_TUPLES_OK) {
        fprintf(stderr, "Query failed: %s\n", PQerrorMessage(conn));
        PQclear(res);
        return -1;
    }

    int num_rows = PQntuples(res);
    q->num_results = num_rows;
    q->results = malloc(num_rows * sizeof(KBRow));
    if (!q->results && num_rows > 0) {
        PQclear(res);
        return -1;
    }

    for (int r = 0; r < num_rows; r++) {
        KBRow *row = &q->results[r];
        row->id = atoi(PQgetvalue(res, r, 0));
        row->knowledge_base = PQgetisnull(res, r, 1) ? NULL : strdup(PQgetvalue(res, r, 1));
        row->label = PQgetisnull(res, r, 2) ? NULL : strdup(PQgetvalue(res, r, 2));
        row->name = PQgetisnull(res, r, 3) ? NULL : strdup(PQgetvalue(res, r, 3));
        row->properties = PQgetisnull(res, r, 4) ? NULL : strdup(PQgetvalue(res, r, 4));
        row->data = PQgetisnull(res, r, 5) ? NULL : strdup(PQgetvalue(res, r, 5));
        row->has_link = PQgetisnull(res, r, 6) ? false : (strcmp(PQgetvalue(res, r, 6), "t") == 0);
        row->has_link_mount = PQgetisnull(res, r, 7) ? false : (strcmp(PQgetvalue(res, r, 7), "t") == 0);
        row->path = PQgetisnull(res, r, 8) ? NULL : strdup(PQgetvalue(res, r, 8));
    }

    PQclear(res);
    return 0;
}

KBRow *kb_query_get_results(KBQuery *q, int *num_results) {
    *num_results = q->num_results;
    return q->results;
}

char **find_path_values(KBRow *rows, int num_rows, int *out_num) {
    if (!rows || num_rows == 0) {
        *out_num = 0;
        return NULL;
    }
    *out_num = num_rows;
    char **paths = malloc(num_rows * sizeof(char *));
    if (!paths) return NULL;
    for (int i = 0; i < num_rows; i++) {
        paths[i] = rows[i].path ? strdup(rows[i].path) : NULL;
    }
    return paths;
}
KBRow *find_rpc_server_ids(void *connection, const char *base_table, const char *kb, const char *node_name, const char **prop_keys, const char **prop_values, int num_props, const char *node_path, int *num_results) {
    PGconn *conn = (PGconn *)connection;
    KBQuery *q = kb_query_new(base_table);
    if (!q) {
        *num_results = 0;
        return NULL;
    }

    kb_query_search_label(q, "KB_RPC_SERVER_FIELD");
    if (kb) kb_query_search_kb(q, kb);
    if (node_name) kb_query_search_name(q, node_name);
    for (int i = 0; i < num_props; i++) {
        if (prop_keys[i] && prop_values[i]) {
            kb_query_search_property_value(q, prop_keys[i], prop_values[i]);
        }
    }
    if (node_path) kb_query_search_path(q, node_path);

    if (kb_query_execute(q, conn) < 0) {
        kb_query_free(q);
        *num_results = 0;
        return NULL;
    }

    int nr;
    KBRow *orig = kb_query_get_results(q, &nr);

    if (nr == 0) {
        fprintf(stderr, "No node found matching path parameters\n");
        kb_query_free(q);
        *num_results = 0;
        return NULL;
    }

    KBRow *copy = malloc(nr * sizeof(KBRow));
    if (!copy) {
        kb_query_free(q);
        *num_results = 0;
        return NULL;
    }

    for (int i = 0; i < nr; i++) {
        copy[i].id = orig[i].id;
        copy[i].knowledge_base = orig[i].knowledge_base ? strdup(orig[i].knowledge_base) : NULL;
        copy[i].label = orig[i].label ? strdup(orig[i].label) : NULL;
        copy[i].name = orig[i].name ? strdup(orig[i].name) : NULL;
        copy[i].properties = orig[i].properties ? strdup(orig[i].properties) : NULL;
        copy[i].data = orig[i].data ? strdup(orig[i].data) : NULL;
        copy[i].has_link = orig[i].has_link;
        copy[i].has_link_mount = orig[i].has_link_mount;
        copy[i].path = orig[i].path ? strdup(orig[i].path) : NULL;
    }

    *num_results = nr;
    kb_query_free(q);
    return copy;
}

KBRow *find_rpc_server_id(void *connection, const char *base_table, const char *kb, const char *node_name, const char **prop_keys, const char **prop_values, int num_props, const char *node_path) {
    PGconn *conn = (PGconn *)connection;
    int num;
    KBRow *rows = find_rpc_server_ids(conn, base_table, kb, node_name, prop_keys, prop_values, num_props, node_path, &num);
    if (!rows) {
        return NULL;
    }
    if (num != 1) {
        fprintf(stderr, "Multiple nodes found matching path parameters\n");
        kb_rows_free(rows, num);
        return NULL;
    }

    KBRow *single = malloc(sizeof(KBRow));
    if (!single) {
        kb_rows_free(rows, num);
        return NULL;
    }

    single->id = rows[0].id;
    single->knowledge_base = rows[0].knowledge_base ? strdup(rows[0].knowledge_base) : NULL;
    single->label = rows[0].label ? strdup(rows[0].label) : NULL;
    single->name = rows[0].name ? strdup(rows[0].name) : NULL;
    single->properties = rows[0].properties ? strdup(rows[0].properties) : NULL;
    single->data = rows[0].data ? strdup(rows[0].data) : NULL;
    single->has_link = rows[0].has_link;
    single->has_link_mount = rows[0].has_link_mount;
    single->path = rows[0].path ? strdup(rows[0].path) : NULL;

    kb_rows_free(rows, num);
    return single;
}

void kb_rows_free(KBRow *rows, int num) {
    if (rows) {
        for (int i = 0; i < num; i++) {
            free(rows[i].knowledge_base);
            free(rows[i].label);
            free(rows[i].name);
            free(rows[i].properties);
            free(rows[i].data);
            free(rows[i].path);
        }
        free(rows);
    }
}

KBRow *find_rpc_client_ids(void *connection, const char *base_table, const char *kb, const char *node_name, const char **prop_keys, const char **prop_values, int num_props, const char *node_path, int *num_results) {
    PGconn *conn = (PGconn *)connection;
    KBQuery *q = kb_query_new(base_table);
    if (!q) {
        *num_results = 0;
        return NULL;
    }

    kb_query_search_label(q, "KB_RPC_CLIENT_FIELD");
    if (kb) kb_query_search_kb(q, kb);
    if (node_name) kb_query_search_name(q, node_name);
    for (int i = 0; i < num_props; i++) {
        if (prop_keys[i] && prop_values[i]) {
            kb_query_search_property_value(q, prop_keys[i], prop_values[i]);
        }
    }
    if (node_path) kb_query_search_path(q, node_path);

    if (kb_query_execute(q, conn) < 0) {
        kb_query_free(q);
        *num_results = 0;
        return NULL;
    }

    int nr;
    KBRow *orig = kb_query_get_results(q, &nr);

    if (nr == 0) {
        fprintf(stderr, "No node found matching path parameters\n");
        kb_query_free(q);
        *num_results = 0;
        return NULL;
    }

    KBRow *copy = malloc(nr * sizeof(KBRow));
    if (!copy) {
        kb_query_free(q);
        *num_results = 0;
        return NULL;
    }

    for (int i = 0; i < nr; i++) {
        copy[i].id = orig[i].id;
        copy[i].knowledge_base = orig[i].knowledge_base ? strdup(orig[i].knowledge_base) : NULL;
        copy[i].label = orig[i].label ? strdup(orig[i].label) : NULL;
        copy[i].name = orig[i].name ? strdup(orig[i].name) : NULL;
        copy[i].properties = orig[i].properties ? strdup(orig[i].properties) : NULL;
        copy[i].data = orig[i].data ? strdup(orig[i].data) : NULL;
        copy[i].has_link = orig[i].has_link;
        copy[i].has_link_mount = orig[i].has_link_mount;
        copy[i].path = orig[i].path ? strdup(orig[i].path) : NULL;
    }

    *num_results = nr;
    kb_query_free(q);
    return copy;
}

KBRow *find_rpc_client_id(void *connection, const char *base_table, const char *kb, const char *node_name, const char **prop_keys, const char **prop_values, int num_props, const char *node_path) {
    PGconn *conn = (PGconn *)connection;
    int num;
    KBRow *rows = find_rpc_client_ids(conn, base_table, kb, node_name, prop_keys, prop_values, num_props, node_path, &num);
    if (!rows) {
        return NULL;
    }
    if (num != 1) {
        fprintf(stderr, "Multiple nodes found matching path parameters\n");
        kb_rows_free(rows, num);
        return NULL;
    }

    KBRow *single = malloc(sizeof(KBRow));
    if (!single) {
        kb_rows_free(rows, num);
        return NULL;
    }

    single->id = rows[0].id;
    single->knowledge_base = rows[0].knowledge_base ? strdup(rows[0].knowledge_base) : NULL;
    single->label = rows[0].label ? strdup(rows[0].label) : NULL;
    single->name = rows[0].name ? strdup(rows[0].name) : NULL;
    single->properties = rows[0].properties ? strdup(rows[0].properties) : NULL;
    single->data = rows[0].data ? strdup(rows[0].data) : NULL;
    single->has_link = rows[0].has_link;
    single->has_link_mount = rows[0].has_link_mount;
    single->path = rows[0].path ? strdup(rows[0].path) : NULL;

    kb_rows_free(rows, num);
    return single;
}




KBRow *find_job_ids(void *connection, const char *base_table, const char *kb, const char *node_name, const char **prop_keys, const char **prop_values, int num_props, const char *node_path, int *num_results) {
    PGconn *conn = (PGconn *)connection;
    KBQuery *q = kb_query_new(base_table);
    if (!q) {
        *num_results = 0;
        return NULL;
    }

    kb_query_search_label(q, "KB_JOB_QUEUE");
    if (kb) kb_query_search_kb(q, kb);
    if (node_name) kb_query_search_name(q, node_name);
    for (int i = 0; i < num_props; i++) {
        if (prop_keys[i] && prop_values[i]) {
            kb_query_search_property_value(q, prop_keys[i], prop_values[i]);
        }
    }
    if (node_path) kb_query_search_path(q, node_path);

    if (kb_query_execute(q, conn) < 0) {
        kb_query_free(q);
        *num_results = 0;
        return NULL;
    }

    int nr;
    KBRow *orig = kb_query_get_results(q, &nr);

    if (nr == 0) {
        fprintf(stderr, "No job found matching path parameters\n");
        kb_query_free(q);
        *num_results = 0;
        return NULL;
    }

    KBRow *copy = malloc(nr * sizeof(KBRow));
    if (!copy) {
        kb_query_free(q);
        *num_results = 0;
        return NULL;
    }

    for (int i = 0; i < nr; i++) {
        copy[i].id = orig[i].id;
        copy[i].knowledge_base = orig[i].knowledge_base ? strdup(orig[i].knowledge_base) : NULL;
        copy[i].label = orig[i].label ? strdup(orig[i].label) : NULL;
        copy[i].name = orig[i].name ? strdup(orig[i].name) : NULL;
        copy[i].properties = orig[i].properties ? strdup(orig[i].properties) : NULL;
        copy[i].data = orig[i].data ? strdup(orig[i].data) : NULL;
        copy[i].has_link = orig[i].has_link;
        copy[i].has_link_mount = orig[i].has_link_mount;
        copy[i].path = orig[i].path ? strdup(orig[i].path) : NULL;
    }

    *num_results = nr;
    kb_query_free(q);
    return copy;
}

KBRow *find_job_id(void *connection, const char *base_table, const char *kb, const char *node_name, const char **prop_keys, const char **prop_values, int num_props, const char *node_path) {
    PGconn *conn = (PGconn *)connection;
    int num;
    KBRow *rows = find_job_ids(conn, base_table, kb, node_name, prop_keys, prop_values, num_props, node_path, &num);
    if (!rows) {
        return NULL;
    }
    if (num != 1) {
        fprintf(stderr, "Multiple jobs found matching path parameters\n");
        kb_rows_free(rows, num);
        return NULL;
    }

    KBRow *single = malloc(sizeof(KBRow));
    if (!single) {
        kb_rows_free(rows, num);
        return NULL;
    }

    single->id = rows[0].id;
    single->knowledge_base = rows[0].knowledge_base ? strdup(rows[0].knowledge_base) : NULL;
    single->label = rows[0].label ? strdup(rows[0].label) : NULL;
    single->name = rows[0].name ? strdup(rows[0].name) : NULL;
    single->properties = rows[0].properties ? strdup(rows[0].properties) : NULL;
    single->data = rows[0].data ? strdup(rows[0].data) : NULL;
    single->has_link = rows[0].has_link;
    single->has_link_mount = rows[0].has_link_mount;
    single->path = rows[0].path ? strdup(rows[0].path) : NULL;

    kb_rows_free(rows, num);
    return single;
}


KBRow *find_stream_ids(void *connection, const char *base_table, const char *kb, const char *node_name, const char **prop_keys, const char **prop_values, int num_props, const char *node_path, int *num_results) {
    PGconn *conn = (PGconn *)connection;
    KBQuery *q = kb_query_new(base_table);
    if (!q) {
        *num_results = 0;
        return NULL;
    }

    kb_query_search_label(q, "KB_STREAM_FIELD");
    if (kb) kb_query_search_kb(q, kb);
    if (node_name) kb_query_search_name(q, node_name);
    for (int i = 0; i < num_props; i++) {
        if (prop_keys[i] && prop_values[i]) {
            kb_query_search_property_value(q, prop_keys[i], prop_values[i]);
        }
    }
    if (node_path) kb_query_search_path(q, node_path);

    if (kb_query_execute(q, conn) < 0) {
        kb_query_free(q);
        *num_results = 0;
        return NULL;
    }

    int nr;
    KBRow *orig = kb_query_get_results(q, &nr);

    if (nr == 0) {
        fprintf(stderr, "No stream node found matching path parameters\n");
        kb_query_free(q);
        *num_results = 0;
        return NULL;
    }

    KBRow *copy = malloc(nr * sizeof(KBRow));
    if (!copy) {
        kb_query_free(q);
        *num_results = 0;
        return NULL;
    }

    for (int i = 0; i < nr; i++) {
        copy[i].id = orig[i].id;
        copy[i].knowledge_base = orig[i].knowledge_base ? strdup(orig[i].knowledge_base) : NULL;
        copy[i].label = orig[i].label ? strdup(orig[i].label) : NULL;
        copy[i].name = orig[i].name ? strdup(orig[i].name) : NULL;
        copy[i].properties = orig[i].properties ? strdup(orig[i].properties) : NULL;
        copy[i].data = orig[i].data ? strdup(orig[i].data) : NULL;
        copy[i].has_link = orig[i].has_link;
        copy[i].has_link_mount = orig[i].has_link_mount;
        copy[i].path = orig[i].path ? strdup(orig[i].path) : NULL;
    }

    *num_results = nr;
    kb_query_free(q);
    return copy;
}

KBRow *find_stream_id(void *connection, const char *base_table, const char *kb, const char *node_name, const char **prop_keys, const char **prop_values, int num_props, const char *node_path) {
    PGconn *conn = (PGconn *)connection;
    int num;
    KBRow *rows = find_stream_ids(conn, base_table, kb, node_name, prop_keys, prop_values, num_props, node_path, &num);
    if (!rows) {
        return NULL;
    }
    if (num != 1) {
        fprintf(stderr, "Multiple stream nodes found matching path parameters\n");
        kb_rows_free(rows, num);
        return NULL;
    }

    KBRow *single = malloc(sizeof(KBRow));
    if (!single) {
        kb_rows_free(rows, num);
        return NULL;
    }

    single->id = rows[0].id;
    single->knowledge_base = rows[0].knowledge_base ? strdup(rows[0].knowledge_base) : NULL;
    single->label = rows[0].label ? strdup(rows[0].label) : NULL;
    single->name = rows[0].name ? strdup(rows[0].name) : NULL;
    single->properties = rows[0].properties ? strdup(rows[0].properties) : NULL;
    single->data = rows[0].data ? strdup(rows[0].data) : NULL;
    single->has_link = rows[0].has_link;
    single->has_link_mount = rows[0].has_link_mount;
    single->path = rows[0].path ? strdup(rows[0].path) : NULL;

    kb_rows_free(rows, num);
    return single;
}


KBRow *find_status_node_ids(void *connection, const char *base_table, const char *kb, const char *node_name, const char **prop_keys, const char **prop_values, int num_props, const char *node_path, int *num_results) {
    PGconn *conn = (PGconn *)connection;
    KBQuery *q = kb_query_new(base_table);
    if (!q) {
        *num_results = 0;
        return NULL;
    }

    kb_query_search_label(q, "KB_STATUS_FIELD");
    if (kb) kb_query_search_kb(q, kb);
    if (node_name) kb_query_search_name(q, node_name);
    for (int i = 0; i < num_props; i++) {
        if (prop_keys[i] && prop_values[i]) {
            kb_query_search_property_value(q, prop_keys[i], prop_values[i]);
        }
    }
    if (node_path) kb_query_search_path(q, node_path);

    if (kb_query_execute(q, conn) < 0) {
        kb_query_free(q);
        *num_results = 0;
        return NULL;
    }

    int nr;
    KBRow *orig = kb_query_get_results(q, &nr);

    if (nr == 0) {
        fprintf(stderr, "No node found matching path parameters\n");
        kb_query_free(q);
        *num_results = 0;
        return NULL;
    }

    KBRow *copy = malloc(nr * sizeof(KBRow));
    if (!copy) {
        kb_query_free(q);
        *num_results = 0;
        return NULL;
    }

    for (int i = 0; i < nr; i++) {
        copy[i].id = orig[i].id;
        copy[i].knowledge_base = orig[i].knowledge_base ? strdup(orig[i].knowledge_base) : NULL;
        copy[i].label = orig[i].label ? strdup(orig[i].label) : NULL;
        copy[i].name = orig[i].name ? strdup(orig[i].name) : NULL;
        copy[i].properties = orig[i].properties ? strdup(orig[i].properties) : NULL;
        copy[i].data = orig[i].data ? strdup(orig[i].data) : NULL;
        copy[i].has_link = orig[i].has_link;
        copy[i].has_link_mount = orig[i].has_link_mount;
        copy[i].path = orig[i].path ? strdup(orig[i].path) : NULL;
    }

    *num_results = nr;
    kb_query_free(q);
    return copy;
}

KBRow *find_status_node_id(void *connection, const char *base_table, const char *kb, const char *node_name, const char **prop_keys, const char **prop_values, int num_props, const char *node_path) {
    PGconn *conn = (PGconn *)connection;
    int num;
    KBRow *rows = find_status_node_ids(conn, base_table, kb, node_name, prop_keys, prop_values, num_props, node_path, &num);
    if (!rows) {
        return NULL;
    }
    if (num != 1) {
        fprintf(stderr, "Multiple nodes found matching path parameters\n");
        kb_rows_free(rows, num);
        return NULL;
    }

    KBRow *single = malloc(sizeof(KBRow));
    if (!single) {
        fprintf(stderr, "Failed to allocate memory for single node\n");
        kb_rows_free(rows, num);
        return NULL;
    }

    single->id = rows[0].id;
    single->knowledge_base = rows[0].knowledge_base ? strdup(rows[0].knowledge_base) : NULL;
    single->label = rows[0].label ? strdup(rows[0].label) : NULL;
    single->name = rows[0].name ? strdup(rows[0].name) : NULL;
    single->properties = rows[0].properties ? strdup(rows[0].properties) : NULL;
    single->data = rows[0].data ? strdup(rows[0].data) : NULL;
    single->has_link = rows[0].has_link;
    single->has_link_mount = rows[0].has_link_mount;
    single->path = rows[0].path ? strdup(rows[0].path) : NULL;

    kb_rows_free(rows, num);
    return single;
}


KBRow *find_node_ids(void *connection, const char *base_table, const char *kb, const char *label, const char *node_name, const char **prop_keys, const char **prop_values, int num_props, const char *node_path, int *num_results) {
    PGconn *conn = (PGconn *)connection;
    KBQuery *q = kb_query_new(base_table);
    if (!q) {
        *num_results = 0;
        return NULL;
    }
    
    
    if (kb) kb_query_search_kb(q, kb);
    if (label) kb_query_search_label(q, label);
    if (node_name) kb_query_search_name(q, node_name);
    for (int i = 0; i < num_props; i++) {
        if (prop_keys[i] && prop_values[i]) {
            kb_query_search_property_value(q, prop_keys[i], prop_values[i]);
        }
    }
    if (node_path) kb_query_search_path(q, node_path);

    if (kb_query_execute(q, conn) < 0) {
        kb_query_free(q);
        *num_results = 0;
        return NULL;
    }

    int nr;
    KBRow *orig = kb_query_get_results(q, &nr);

    if (nr == 0) {
        fprintf(stderr, "No node found matching path parameters\n");
        kb_query_free(q);
        *num_results = 0;
        return NULL;
    }

    KBRow *copy = malloc(nr * sizeof(KBRow));
    if (!copy) {
        kb_query_free(q);
        *num_results = 0;
        return NULL;
    }

    for (int i = 0; i < nr; i++) {
        copy[i].id = orig[i].id;
        copy[i].knowledge_base = orig[i].knowledge_base ? strdup(orig[i].knowledge_base) : NULL;
        copy[i].label = orig[i].label ? strdup(orig[i].label) : NULL;
        copy[i].name = orig[i].name ? strdup(orig[i].name) : NULL;
        copy[i].properties = orig[i].properties ? strdup(orig[i].properties) : NULL;
        copy[i].data = orig[i].data ? strdup(orig[i].data) : NULL;
        copy[i].has_link = orig[i].has_link;
        copy[i].has_link_mount = orig[i].has_link_mount;
        copy[i].path = orig[i].path ? strdup(orig[i].path) : NULL;
    }

    *num_results = nr;
    kb_query_free(q);
    return copy;
}

KBRow *find_node_id(void *connection, const char *base_table, const char *kb,const char *label, const char *node_name, const char **prop_keys, const char **prop_values, int num_props, const char *node_path) {
    PGconn *conn = (PGconn *)connection;
    int num;
    KBRow *rows = find_node_ids(conn, base_table, kb, label, node_name, prop_keys, prop_values, num_props, node_path, &num);
    if (!rows) {
        return NULL;
    }
    if (num != 1) {
        fprintf(stderr, "Multiple nodes found matching path parameters\n");
        kb_rows_free(rows, num);
        return NULL;
    }

    KBRow *single = malloc(sizeof(KBRow));
    if (!single) {
        kb_rows_free(rows, num);
        return NULL;
    }

    single->id = rows[0].id;
    single->knowledge_base = rows[0].knowledge_base ? strdup(rows[0].knowledge_base) : NULL;
    single->label = rows[0].label ? strdup(rows[0].label) : NULL;
    single->name = rows[0].name ? strdup(rows[0].name) : NULL;
    single->properties = rows[0].properties ? strdup(rows[0].properties) : NULL;
    single->data = rows[0].data ? strdup(rows[0].data) : NULL;
    single->has_link = rows[0].has_link;
    single->has_link_mount = rows[0].has_link_mount;
    single->path = rows[0].path ? strdup(rows[0].path) : NULL;

    kb_rows_free(rows, num);
    return single;
}





#ifdef __MAIN__

PGconn *create_pg_connection(const char *dbname, const char *user, const char *password, const char *host, const char *port) {
    // Build connection string
    char conninfo[256];
    snprintf(conninfo, sizeof(conninfo), 
             "dbname=%s user=%s password=%s host=%s port=%s",
             dbname ? dbname : "",
             user ? user : "",
             password ? password : "",
             host ? host : "localhost",
             port ? port : "5432");

    // Create connection
    PGconn *conn = PQconnectdb(conninfo);
    if (PQstatus(conn) != CONNECTION_OK) {
        fprintf(stderr, "Connection failed: %s\n", PQerrorMessage(conn));
        PQfinish(conn);
        return NULL;
    }

    return conn;

}

char error_msg[512];


void individual_status_table(void *connection, const char *base_table, const char *kb, const char *node_name, 
       const char **prop_keys, const char **prop_values, int num_props, const char *node_path){
    PGconn *conn = (PGconn *)connection;
    KBQuery *q = kb_query_new(base_table);
    if (!q) {
        fprintf(stderr, "Failed to create KBQuery\n");
        return;
    }

    int num;
    KBRow *rows = find_status_node_ids(conn,base_table,kb,node_name,prop_keys,prop_values,num_props,node_path,&num);
    if (!rows) {
        fprintf(stderr, "No nodes found matching path parameters\n");
        kb_query_free(q);
        return;
    }
    printf("number of rows: %d\n", num);
    for (int i = 0; i < num; i++) {
        printf("id: %d\n", rows[i].id);
        printf("knowledge_base: %s\n", rows[i].knowledge_base);
        printf("label: %s\n", rows[i].label);
        printf("name: %s\n", rows[i].name);
        printf("properties: %s\n", rows[i].properties);
        printf("data: %s\n", rows[i].data);
        printf("has_link: %d\n", rows[i].has_link);
        printf("has_link_mount: %d\n", rows[i].has_link_mount);
        printf("path: %s\n", rows[i].path);
    }
    kb_rows_free(rows, num);

    
    kb_query_free(q);
}

void find_status_tables(void *connection, const char *base_table){
    PGconn *conn = (PGconn *)connection;
  
    printf("-------------------------------- find_status_tables\n");
    printf("-------------------------------- wide open test find all status tables\n");
    individual_status_table(conn, base_table, NULL, NULL, NULL, NULL, 0, NULL);
    printf("-------------------------------- search by kb\n");
    individual_status_table(conn, base_table, "kb1", NULL, NULL, NULL, 0, NULL);
    printf("-------------------------------- search by name\n");
    individual_status_table(conn, base_table, NULL, "info3_status", NULL, NULL, 0, NULL);
    printf("-------------------------------- search by path\n");
    individual_status_table(conn, base_table, NULL, NULL, NULL, NULL, 0, "kb1.header1_link.header1_name.KB_STATUS_FIELD.info2_status");
    printf("-------------------------------- search by property keys and values\n");
    individual_status_table(conn, base_table, NULL, NULL,(const char *[]){"prop3","description"}, (const char *[]){"val3","info3_status_description"}, 2,NULL);

}

void find_stream_tables(void *connection, const char *base_table){
    PGconn *conn = (PGconn *)connection;
    printf("-------------------------------- find_stream_tables\n");
    printf("-------------------------------- wide open test find all stream tables\n");
    KBQuery *q = kb_query_new(base_table);
    if (!q) {
        fprintf(stderr, "Failed to create KBQuery\n");
        return;
    }

    int num;
    KBRow *rows = find_stream_ids(conn,base_table,NULL,NULL,NULL,NULL,0,NULL,&num);
    if (!rows) {
        fprintf(stderr, "No nodes found matching path parameters\n");
        kb_query_free(q);
        return;
    }
    printf("number of rows: %d\n", num);
    for (int i = 0; i < num; i++) {
        printf("id: %d\n", rows[i].id);
        printf("knowledge_base: %s\n", rows[i].knowledge_base);
        printf("label: %s\n", rows[i].label);
        printf("name: %s\n", rows[i].name);
        printf("properties: %s\n", rows[i].properties);
        printf("data: %s\n", rows[i].data);
        printf("has_link: %d\n", rows[i].has_link);
        printf("has_link_mount: %d\n", rows[i].has_link_mount);
        printf("path: %s\n", rows[i].path);
    }
    kb_rows_free(rows, num);

    
    kb_query_free(q);
}
void find_job_tables(void *connection, const char *base_table){
    PGconn *conn = (PGconn *)connection;
    printf("-------------------------------- find_job_tables\n");
    printf("-------------------------------- wide open test find all job tables\n");
    KBQuery *q = kb_query_new(base_table);
    if (!q) {
        fprintf(stderr, "Failed to create KBQuery\n");
        return;
    }

    int num;
    KBRow *rows = find_job_ids(conn,base_table,NULL,NULL,NULL,NULL,0,NULL,&num);
    if (!rows) {
        fprintf(stderr, "No nodes found matching path parameters\n");
        kb_query_free(q);
        return;
    }
    printf("number of rows: %d\n", num);
    for (int i = 0; i < num; i++) {
        printf("id: %d\n", rows[i].id);
        printf("knowledge_base: %s\n", rows[i].knowledge_base);
        printf("label: %s\n", rows[i].label);
        printf("name: %s\n", rows[i].name);
        printf("properties: %s\n", rows[i].properties);
        printf("data: %s\n", rows[i].data);
        printf("has_link: %d\n", rows[i].has_link);
        printf("has_link_mount: %d\n", rows[i].has_link_mount);
        printf("path: %s\n", rows[i].path);
    }
    kb_rows_free(rows, num);

    
    kb_query_free(q);
}
void find_rpc_server_tables(void *connection, const char *base_table){
    PGconn *conn = (PGconn *)connection;
    printf("-------------------------------- find_rpc_server_tables\n");
    printf("-------------------------------- wide open test find all rpc server tables\n");
    KBQuery *q = kb_query_new(base_table);
    if (!q) {
        fprintf(stderr, "Failed to create KBQuery\n");
        return;
    }

    int num;
    KBRow *rows = find_rpc_server_ids(conn,base_table,NULL,NULL,NULL,NULL,0,NULL,&num);
    if (!rows) {
        fprintf(stderr, "No nodes found matching path parameters\n");
        kb_query_free(q);
        return;
    }
    printf("number of rows: %d\n", num);
    for (int i = 0; i < num; i++) {
        printf("id: %d\n", rows[i].id);
        printf("knowledge_base: %s\n", rows[i].knowledge_base);
        printf("label: %s\n", rows[i].label);
        printf("name: %s\n", rows[i].name);
        printf("properties: %s\n", rows[i].properties);
        printf("data: %s\n", rows[i].data);
        printf("has_link: %d\n", rows[i].has_link);
        printf("has_link_mount: %d\n", rows[i].has_link_mount);
        printf("path: %s\n", rows[i].path);
    }
    kb_rows_free(rows, num);

    
    kb_query_free(q);
}
void find_rpc_client_tables(void *connection, const char *base_table){
    PGconn *conn = (PGconn *)connection;
    printf("-------------------------------- find_rpc_client_tables\n");
    printf("-------------------------------- wide open test find all rpc client tables\n");
    KBQuery *q = kb_query_new(base_table);
    if (!q) {
        fprintf(stderr, "Failed to create KBQuery\n");
        return;
    }

    int num;
    KBRow *rows = find_rpc_client_ids(conn,base_table,NULL,NULL,NULL,NULL,0,NULL,&num);
    if (!rows) {
        fprintf(stderr, "No nodes found matching path parameters\n");
        kb_query_free(q);
        return;
    }
    printf("number of rows: %d\n", num);
    for (int i = 0; i < num; i++) {
        printf("id: %d\n", rows[i].id);
        printf("knowledge_base: %s\n", rows[i].knowledge_base);
        printf("label: %s\n", rows[i].label);
        printf("name: %s\n", rows[i].name);
        printf("properties: %s\n", rows[i].properties);
        printf("data: %s\n", rows[i].data);
        printf("has_link: %d\n", rows[i].has_link);
        printf("has_link_mount: %d\n", rows[i].has_link_mount);
        printf("path: %s\n", rows[i].path);
    }
    kb_rows_free(rows, num);

    
    kb_query_free(q);
}

void find_node_tables(void *connection, const char *base_table){
    PGconn *conn = (PGconn *)connection;
    printf("-------------------------------- find_node_tables\n");
    printf("-------------------------------- search for status nodes using label\n");
    KBQuery *q = kb_query_new(base_table);
    if (!q) {
        fprintf(stderr, "Failed to create KBQuery\n");
        return;
    }

    int num;
    KBRow *rows = find_node_ids(conn,base_table,NULL,"KB_STATUS_FIELD",NULL,NULL,NULL,0,NULL,&num);
    if (!rows) {
        fprintf(stderr, "No nodes found matching path parameters\n");
        kb_query_free(q);
        return;
    }
    printf("number of rows: %d\n", num);
    for (int i = 0; i < num; i++) {
        printf("id: %d\n", rows[i].id);
        printf("knowledge_base: %s\n", rows[i].knowledge_base);
        printf("label: %s\n", rows[i].label);
        printf("name: %s\n", rows[i].name);
        printf("properties: %s\n", rows[i].properties);
        printf("data: %s\n", rows[i].data);
        printf("has_link: %d\n", rows[i].has_link);
        printf("has_link_mount: %d\n", rows[i].has_link_mount);
        printf("path: %s\n", rows[i].path);
    }
    kb_rows_free(rows, num);
    kb_query_free(q);
}


int main(void){
    char password[256];
    printf("Enter password: "); 
    fgets(password, sizeof(password), stdin); 

    PGconn *conn = create_pg_connection("knowledge_base", "gedgar", password, "localhost", "5432");
    if (!conn) {
        fprintf(stderr, "Failed to create PostgreSQL connection\n");
        return 1;
    }
    find_status_tables(conn, "knowledge_base");
    find_stream_tables(conn, "knowledge_base");
    find_job_tables(conn, "knowledge_base");
    find_rpc_server_tables(conn, "knowledge_base");
    find_rpc_client_tables(conn, "knowledge_base");
    find_node_tables(conn, "knowledge_base");
    
}
#endif
