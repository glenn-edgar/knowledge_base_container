#ifndef KB_QUERY_SUPPORT_H
#define KB_QUERY_SUPPORT_H

#include "kb_common.h"

#ifdef __cplusplus
extern "C" {
#endif

typedef enum {
    KB_FILTER_KB, KB_FILTER_LABEL, KB_FILTER_NAME,
    KB_FILTER_PROPERTY_KEY, KB_FILTER_PROPERTY_VALUE,
    KB_FILTER_HAS_LINK, KB_FILTER_HAS_LINK_MOUNT,
    KB_FILTER_PATH, KB_FILTER_STARTING_PATH,
} kb_filter_type_t;

#define KB_MAX_FILTERS 16

typedef struct {
    kb_filter_type_t type;
    char *param1;
    char *param2;
} kb_filter_t;

typedef struct kb_search kb_search_t;

kb_search_t *kb_search_create(const char *db_path, const char *database, const char *ltree_path);
kb_search_t *kb_search_create_from_db(sqlite3 *db, const char *database);
void kb_search_destroy(kb_search_t *ks);

void kb_search_clear_filters(kb_search_t *ks);
kb_error_t kb_search_kb(kb_search_t *ks, const char *kb_name);
kb_error_t kb_search_label(kb_search_t *ks, const char *label);
kb_error_t kb_search_name(kb_search_t *ks, const char *name);
kb_error_t kb_search_property_key(kb_search_t *ks, const char *key);
kb_error_t kb_search_property_value(kb_search_t *ks, const char *key, const char *value);
kb_error_t kb_search_has_link(kb_search_t *ks);
kb_error_t kb_search_has_link_mount(kb_search_t *ks);
kb_error_t kb_search_path(kb_search_t *ks, const char *path_expr);
kb_error_t kb_search_starting_path(kb_search_t *ks, const char *starting_path);
kb_error_t kb_search_execute(kb_search_t *ks);

const kb_result_t *kb_search_results(const kb_search_t *ks);
sqlite3 *kb_search_get_db(const kb_search_t *ks);
const char *kb_search_get_database(const kb_search_t *ks);

typedef struct { char *path; char *description; } kb_description_t;
kb_error_t kb_search_find_description(const kb_result_t *result, kb_description_t **out, int *out_count);
void kb_description_free(kb_description_t *descs, int count);

kb_error_t kb_search_find_description_paths(kb_search_t *ks, const char **paths, int n_paths, kb_result_t *result);
kb_error_t kb_search_find_path_values(const kb_result_t *result, char ***out_paths, int *out_count);
void kb_path_values_free(char **paths, int count);

typedef struct { char *link; char *name; } kb_link_pair_t;
kb_error_t kb_search_decode_link_nodes(const char *path, char **kb_name_out, kb_link_pair_t **pairs_out, int *pair_count_out);
void kb_link_pairs_free(kb_link_pair_t *pairs, int count);

#ifdef __cplusplus
}
#endif
#endif
