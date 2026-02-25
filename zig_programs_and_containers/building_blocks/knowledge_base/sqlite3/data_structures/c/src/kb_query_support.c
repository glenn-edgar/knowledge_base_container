/*
 * kb_query_support.c
 * Knowledge Base C Port — KB_Search CTE progressive filtering
 *
 * Mirrors LuaJIT kb_query_support.lua.
 * Builds SQL CTE chains from accumulated filters, executes, collects results.
 *
 * CTE pattern:
 *   WITH base_data AS (SELECT * FROM <database>),
 *        filter_0 AS (SELECT * FROM base_data WHERE label = ?),
 *        filter_1 AS (SELECT * FROM filter_0 WHERE name = ?),
 *        ...
 *   SELECT * FROM filter_N;
 */

#include "kb_query_support.h"
#include "kb_json.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>

/* ================================================================
 * Internal structure
 * ================================================================ */

struct kb_search {
    sqlite3      *db;
    char         *database;     /* table name */
    bool          owns_db;      /* true if we opened the db */

    kb_filter_t   filters[KB_MAX_FILTERS];
    int           filter_count;

    kb_result_t   results;
};

/* ================================================================
 * Lifecycle
 * ================================================================ */

kb_search_t *kb_search_create(const char *db_path, const char *database,
                              const char *ltree_path)
{
    if (!db_path || !database) return NULL;

    kb_search_t *ks = (kb_search_t *)calloc(1, sizeof(kb_search_t));
    if (!ks) return NULL;

    ks->database = kb_strdup(database);
    if (!ks->database) {
        free(ks);
        return NULL;
    }

    kb_error_t err = kb_open_database(db_path, ltree_path, &ks->db);
    if (err != KB_OK) {
        free(ks->database);
        free(ks);
        return NULL;
    }

    ks->owns_db = true;
    kb_result_init(&ks->results);
    return ks;
}

kb_search_t *kb_search_create_from_db(sqlite3 *db, const char *database)
{
    if (!db || !database) return NULL;

    kb_search_t *ks = (kb_search_t *)calloc(1, sizeof(kb_search_t));
    if (!ks) return NULL;

    ks->database = kb_strdup(database);
    if (!ks->database) {
        free(ks);
        return NULL;
    }

    ks->db = db;
    ks->owns_db = false;
    kb_result_init(&ks->results);
    return ks;
}

void kb_search_destroy(kb_search_t *ks)
{
    if (!ks) return;
    kb_search_clear_filters(ks);
    kb_result_free(&ks->results);
    if (ks->owns_db && ks->db) {
        kb_close_database(ks->db);
    }
    free(ks->database);
    free(ks);
}

/* ================================================================
 * Accessors
 * ================================================================ */

const kb_result_t *kb_search_results(const kb_search_t *ks)
{
    return ks ? &ks->results : NULL;
}

sqlite3 *kb_search_get_db(const kb_search_t *ks)
{
    return ks ? ks->db : NULL;
}

const char *kb_search_get_database(const kb_search_t *ks)
{
    return ks ? ks->database : NULL;
}

/* ================================================================
 * Filter management
 * ================================================================ */

static void filter_free(kb_filter_t *f)
{
    free(f->param1);
    free(f->param2);
    f->param1 = NULL;
    f->param2 = NULL;
}

void kb_search_clear_filters(kb_search_t *ks)
{
    if (!ks) return;
    for (int i = 0; i < ks->filter_count; i++) {
        filter_free(&ks->filters[i]);
    }
    ks->filter_count = 0;
}

static kb_error_t add_filter(kb_search_t *ks, kb_filter_type_t type,
                             const char *p1, const char *p2)
{
    if (!ks) return KB_ERR_NULL_ARG;
    if (ks->filter_count >= KB_MAX_FILTERS) return KB_ERR_OVERFLOW;

    kb_filter_t *f = &ks->filters[ks->filter_count];
    f->type = type;
    f->param1 = p1 ? kb_strdup(p1) : NULL;
    f->param2 = p2 ? kb_strdup(p2) : NULL;
    ks->filter_count++;
    return KB_OK;
}

kb_error_t kb_search_kb(kb_search_t *ks, const char *kb_name)
{
    return add_filter(ks, KB_FILTER_KB, kb_name, NULL);
}

kb_error_t kb_search_label(kb_search_t *ks, const char *label)
{
    return add_filter(ks, KB_FILTER_LABEL, label, NULL);
}

kb_error_t kb_search_name(kb_search_t *ks, const char *name)
{
    return add_filter(ks, KB_FILTER_NAME, name, NULL);
}

kb_error_t kb_search_property_key(kb_search_t *ks, const char *key)
{
    return add_filter(ks, KB_FILTER_PROPERTY_KEY, key, NULL);
}

kb_error_t kb_search_property_value(kb_search_t *ks, const char *key,
                                     const char *value)
{
    return add_filter(ks, KB_FILTER_PROPERTY_VALUE, key, value);
}

kb_error_t kb_search_has_link(kb_search_t *ks)
{
    return add_filter(ks, KB_FILTER_HAS_LINK, NULL, NULL);
}

kb_error_t kb_search_has_link_mount(kb_search_t *ks)
{
    return add_filter(ks, KB_FILTER_HAS_LINK_MOUNT, NULL, NULL);
}

kb_error_t kb_search_path(kb_search_t *ks, const char *path_expr)
{
    return add_filter(ks, KB_FILTER_PATH, path_expr, NULL);
}

kb_error_t kb_search_starting_path(kb_search_t *ks, const char *starting_path)
{
    return add_filter(ks, KB_FILTER_STARTING_PATH, starting_path, NULL);
}

/* ================================================================
 * CTE query builder and executor
 * ================================================================ */

/*
 * Build the WHERE clause fragment for a filter.
 * Returns a malloc'd string. Caller must free().
 * *bind_count is incremented for each ? placeholder added.
 */
static char *filter_to_where(const kb_filter_t *f, int *bind_count)
{
    (void)bind_count;  /* bind_count tracked externally */
    switch (f->type) {
    case KB_FILTER_KB:
        (*bind_count)++;
        return kb_strdup("knowledge_base = ?");
    case KB_FILTER_LABEL:
        (*bind_count)++;
        return kb_strdup("label = ?");
    case KB_FILTER_NAME:
        (*bind_count)++;
        return kb_strdup("name = ?");
    case KB_FILTER_PROPERTY_KEY:
        (*bind_count)++;
        return kb_sprintf("json_extract(properties, '$.' || ?) IS NOT NULL");
    case KB_FILTER_PROPERTY_VALUE:
        (*bind_count) += 2;
        return kb_sprintf("json_extract(properties, '$.' || ?) = ?");
    case KB_FILTER_HAS_LINK:
        return kb_strdup("has_link = 1");
    case KB_FILTER_HAS_LINK_MOUNT:
        return kb_strdup("has_link_mount = 1");
    case KB_FILTER_PATH:
        (*bind_count)++;
        /* Use ltree match operator if available, else LIKE */
        return kb_strdup("path GLOB ?");
    case KB_FILTER_STARTING_PATH:
        (*bind_count)++;
        return kb_strdup("ltree_descendant(?, path)");
    }
    return kb_strdup("1=1");
}

/*
 * Collect bind params from all filters into a flat array.
 * Returns malloc'd array. Caller must free().
 */
static kb_bind_param_t *collect_bind_params(const kb_filter_t *filters,
                                             int count, int *total)
{
    /* Count total params needed */
    int n = 0;
    for (int i = 0; i < count; i++) {
        switch (filters[i].type) {
        case KB_FILTER_KB:
        case KB_FILTER_LABEL:
        case KB_FILTER_NAME:
        case KB_FILTER_PROPERTY_KEY:
        case KB_FILTER_PATH:
        case KB_FILTER_STARTING_PATH:
            n++;
            break;
        case KB_FILTER_PROPERTY_VALUE:
            n += 2;
            break;
        case KB_FILTER_HAS_LINK:
        case KB_FILTER_HAS_LINK_MOUNT:
            break;
        }
    }

    *total = n;
    if (n == 0) return NULL;

    kb_bind_param_t *params = (kb_bind_param_t *)calloc((size_t)n,
                                                         sizeof(kb_bind_param_t));
    if (!params) return NULL;

    int idx = 0;
    for (int i = 0; i < count; i++) {
        switch (filters[i].type) {
        case KB_FILTER_KB:
        case KB_FILTER_LABEL:
        case KB_FILTER_NAME:
        case KB_FILTER_PATH:
        case KB_FILTER_STARTING_PATH:
            params[idx++] = (kb_bind_param_t){
                .type = KB_BIND_TEXT, .val.text = filters[i].param1
            };
            break;
        case KB_FILTER_PROPERTY_KEY:
            params[idx++] = (kb_bind_param_t){
                .type = KB_BIND_TEXT, .val.text = filters[i].param1
            };
            break;
        case KB_FILTER_PROPERTY_VALUE:
            params[idx++] = (kb_bind_param_t){
                .type = KB_BIND_TEXT, .val.text = filters[i].param1
            };
            params[idx++] = (kb_bind_param_t){
                .type = KB_BIND_TEXT, .val.text = filters[i].param2
            };
            break;
        case KB_FILTER_HAS_LINK:
        case KB_FILTER_HAS_LINK_MOUNT:
            break;
        }
    }

    return params;
}

kb_error_t kb_search_execute(kb_search_t *ks)
{
    if (!ks) return KB_ERR_NULL_ARG;

    /* Free previous results */
    kb_result_free(&ks->results);

    /* No filters: simple SELECT * */
    if (ks->filter_count == 0) {
        char *sql = kb_sprintf("SELECT * FROM %s", ks->database);
        if (!sql) return KB_ERR_NOMEM;
        kb_error_t err = kb_query_exec(ks->db, sql, NULL, 0, &ks->results);
        free(sql);
        return err;
    }

    /* Build CTE chain */
    /* Estimate buffer size: generous allocation */
    size_t buf_size = 512 + (size_t)ks->filter_count * 256;
    char *sql = (char *)malloc(buf_size);
    if (!sql) return KB_ERR_NOMEM;

    int pos = 0;

    /* WITH clause */
    pos += snprintf(sql + pos, buf_size - (size_t)pos,
                    "WITH base_data AS (SELECT * FROM %s)", ks->database);

    int bind_count = 0;
    for (int i = 0; i < ks->filter_count; i++) {
        const char *prev = (i == 0) ? "base_data" : NULL;
        char prev_name[32];
        if (i > 0) {
            snprintf(prev_name, sizeof(prev_name), "filter_%d", i - 1);
            prev = prev_name;
        }

        char *where_clause = filter_to_where(&ks->filters[i], &bind_count);
        if (!where_clause) {
            free(sql);
            return KB_ERR_NOMEM;
        }

        pos += snprintf(sql + pos, buf_size - (size_t)pos,
                        ", filter_%d AS (SELECT * FROM %s WHERE %s)",
                        i, prev, where_clause);
        free(where_clause);
    }

    /* Final SELECT */
    pos += snprintf(sql + pos, buf_size - (size_t)pos,
                    " SELECT * FROM filter_%d", ks->filter_count - 1);

    /* Collect bind params */
    int n_params = 0;
    kb_bind_param_t *params = collect_bind_params(ks->filters,
                                                   ks->filter_count,
                                                   &n_params);

    kb_error_t err = kb_query_exec(ks->db, sql, params, n_params,
                                    &ks->results);

    free(params);
    free(sql);
    return err;
}

/* ================================================================
 * Convenience: find_description
 * ================================================================ */

kb_error_t kb_search_find_description(const kb_result_t *result,
                                       kb_description_t **out,
                                       int *out_count)
{
    if (!result || !out || !out_count) return KB_ERR_NULL_ARG;

    *out_count = result->count;
    if (result->count == 0) {
        *out = NULL;
        return KB_OK;
    }

    *out = (kb_description_t *)calloc((size_t)result->count,
                                       sizeof(kb_description_t));
    if (!*out) return KB_ERR_NOMEM;

    for (int i = 0; i < result->count; i++) {
        const char *path = kb_row_get(result, i, "path");
        const char *props_str = kb_row_get(result, i, "properties");

        (*out)[i].path = kb_strdup(path ? path : "");

        /* Extract description from properties JSON */
        (*out)[i].description = NULL;
        if (props_str) {
            cJSON *obj = kb_json_decode(props_str);
            if (obj) {
                cJSON *desc = cJSON_GetObjectItemCaseSensitive(obj, "description");
                if (cJSON_IsString(desc) && desc->valuestring) {
                    (*out)[i].description = kb_strdup(desc->valuestring);
                }
                cJSON_Delete(obj);
            }
        }
        if (!(*out)[i].description) {
            (*out)[i].description = kb_strdup("");
        }
    }

    return KB_OK;
}

void kb_description_free(kb_description_t *descs, int count)
{
    if (!descs) return;
    for (int i = 0; i < count; i++) {
        free(descs[i].path);
        free(descs[i].description);
    }
    free(descs);
}

/* ================================================================
 * Convenience: find_description_paths
 * ================================================================ */

kb_error_t kb_search_find_description_paths(kb_search_t *ks,
                                             const char **paths, int n_paths,
                                             kb_result_t *result)
{
    if (!ks || !paths || !result) return KB_ERR_NULL_ARG;
    kb_result_init(result);

    if (n_paths == 0) return KB_OK;

    /* Build: SELECT path, data FROM <db> WHERE path IN (?, ?, ...) */
    size_t sql_size = 128 + (size_t)n_paths * 4;
    char *sql = (char *)malloc(sql_size);
    if (!sql) return KB_ERR_NOMEM;

    int pos = snprintf(sql, sql_size, "SELECT path, data FROM %s WHERE path IN (",
                       ks->database);

    for (int i = 0; i < n_paths; i++) {
        if (i > 0) pos += snprintf(sql + pos, sql_size - (size_t)pos, ",");
        pos += snprintf(sql + pos, sql_size - (size_t)pos, "?");
    }
    pos += snprintf(sql + pos, sql_size - (size_t)pos, ")");

    /* Build bind params */
    kb_bind_param_t *params = (kb_bind_param_t *)calloc((size_t)n_paths,
                                                         sizeof(kb_bind_param_t));
    if (!params) {
        free(sql);
        return KB_ERR_NOMEM;
    }

    for (int i = 0; i < n_paths; i++) {
        params[i] = (kb_bind_param_t){ .type = KB_BIND_TEXT, .val.text = paths[i] };
    }

    kb_error_t err = kb_query_exec(ks->db, sql, params, n_paths, result);

    free(params);
    free(sql);
    return err;
}

/* ================================================================
 * Convenience: find_path_values
 * ================================================================ */

kb_error_t kb_search_find_path_values(const kb_result_t *result,
                                       char ***out_paths, int *out_count)
{
    if (!result || !out_paths || !out_count) return KB_ERR_NULL_ARG;

    *out_count = result->count;
    if (result->count == 0) {
        *out_paths = NULL;
        return KB_OK;
    }

    *out_paths = (char **)calloc((size_t)result->count, sizeof(char *));
    if (!*out_paths) return KB_ERR_NOMEM;

    for (int i = 0; i < result->count; i++) {
        const char *path = kb_row_get(result, i, "path");
        (*out_paths)[i] = kb_strdup(path ? path : "");
    }

    return KB_OK;
}

void kb_path_values_free(char **paths, int count)
{
    if (!paths) return;
    for (int i = 0; i < count; i++) {
        free(paths[i]);
    }
    free(paths);
}

/* ================================================================
 * Convenience: decode_link_nodes
 * ================================================================ */

kb_error_t kb_search_decode_link_nodes(const char *path,
                                        char **kb_name_out,
                                        kb_link_pair_t **pairs_out,
                                        int *pair_count_out)
{
    if (!path || !kb_name_out || !pairs_out || !pair_count_out)
        return KB_ERR_NULL_ARG;

    *kb_name_out = NULL;
    *pairs_out = NULL;
    *pair_count_out = 0;

    /* Split path by '.' into tokens */
    char *dup = kb_strdup(path);
    if (!dup) return KB_ERR_NOMEM;

    /* Count tokens */
    int token_count = 0;
    char *tokens[256];
    char *tok = strtok(dup, ".");
    while (tok && token_count < 256) {
        tokens[token_count++] = tok;
        tok = strtok(NULL, ".");
    }

    /* Must have: kb_name + pairs of (uuid, name)
     * So token_count must be odd and >= 3 */
    if (token_count < 1) {
        free(dup);
        return KB_ERR_INVALID;
    }

    *kb_name_out = kb_strdup(tokens[0]);

    int remaining = token_count - 1;
    int n_pairs = remaining / 2;

    if (n_pairs > 0) {
        *pairs_out = (kb_link_pair_t *)calloc((size_t)n_pairs,
                                               sizeof(kb_link_pair_t));
        if (!*pairs_out) {
            free(*kb_name_out);
            *kb_name_out = NULL;
            free(dup);
            return KB_ERR_NOMEM;
        }

        for (int i = 0; i < n_pairs; i++) {
            int idx = 1 + i * 2;
            (*pairs_out)[i].link = kb_strdup(tokens[idx]);
            (*pairs_out)[i].name = kb_strdup(tokens[idx + 1]);
        }
    }

    *pair_count_out = n_pairs;
    free(dup);
    return KB_OK;
}

void kb_link_pairs_free(kb_link_pair_t *pairs, int count)
{
    if (!pairs) return;
    for (int i = 0; i < count; i++) {
        free(pairs[i].link);
        free(pairs[i].name);
    }
    free(pairs);
}
