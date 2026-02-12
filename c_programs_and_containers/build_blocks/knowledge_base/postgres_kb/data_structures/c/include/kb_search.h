#ifndef KB_SEARCH_H
#define KB_SEARCH_H

#ifdef __cplusplus
extern "C" {
#endif

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdbool.h>


typedef struct {
    int id;
    char *knowledge_base;
    char *label;
    char *name;
    char *properties;
    char *data;
    bool has_link;
    bool has_link_mount;
    char *path;
} KBRow;

typedef struct {
    char *base_table;
    char **conditions;
    char **param_values;
    int num_filters;
    int max_filters;
    KBRow *results;
    int num_results;
} KBQuery;

// KBQuery management functions
KBQuery *kb_query_new(const char *base_table);
void kb_query_free(KBQuery *q);
void kb_query_clear_filters(KBQuery *q);

// KBQuery search filter functions
void kb_query_search_kb(KBQuery *q, const char *knowledge_base);
void kb_query_search_label(KBQuery *q, const char *label);
void kb_query_search_name(KBQuery *q, const char *name);
void kb_query_search_property_key(KBQuery *q, const char *key);
void kb_query_search_property_value(KBQuery *q, const char *key, const char *value);
void kb_query_search_starting_path(KBQuery *q, const char *starting_path);
void kb_query_search_path(KBQuery *q, const char *path_expression);

// KBQuery execution and results
int kb_query_execute(KBQuery *q, void *conn);
KBRow *kb_query_get_results(KBQuery *q, int *num_results);

// Utility functions
char **find_path_values(KBRow *rows, int num_rows, int *out_num);
void kb_rows_free(KBRow *rows, int num);

// RPC Server functions
KBRow *find_rpc_server_ids(void *conn, const char *base_table, const char *kb, const char *node_name,
     const char **prop_keys, const char **prop_values, int num_props, const char *node_path, int *num_results);

KBRow *find_rpc_server_id(void *conn, const char *base_table, const char *kb, const char *node_name, 
    const char **prop_keys, const char **prop_values, int num_props, const char *node_path);

// RPC Client functions
KBRow *find_rpc_client_ids(void *conn, const char *base_table, const char *kb, const char *node_name, 
    const char **prop_keys, const char **prop_values, int num_props, const char *node_path, int *num_results);
    
KBRow *find_rpc_client_id(void *conn, const char *base_table, const char *kb, const char *node_name, 
    const char **prop_keys, const char **prop_values, int num_props, const char *node_path);

// Job functions
KBRow *find_job_ids(void *conn, const char *base_table, const char *kb, const char *node_name,
     const char **prop_keys, const char **prop_values, int num_props, const char *node_path, int *num_results);

KBRow *find_job_id(void *conn, const char *base_table, const char *kb, const char *node_name,
     const char **prop_keys, const char **prop_values, int num_props, const char *node_path);

// Stream functions
KBRow *find_stream_ids(void *conn, const char *base_table, const char *kb, const char *node_name, 
    const char **prop_keys, const char **prop_values, int num_props, const char *node_path, int *num_results);

KBRow *find_stream_id(void *conn, const char *base_table, const char *kb, const char *node_name,
     const char **prop_keys, const char **prop_values, int num_props, const char *node_path);

// Status node functions
KBRow *find_status_node_ids(void *conn, const char *base_table, const char *kb, const char *node_name, 
    const char **prop_keys, const char **prop_values, int num_props, const char *node_path, int *num_results);

KBRow *find_status_node_id(void *conn, const char *base_table, const char *kb, const char *node_name, 
    const char **prop_keys, const char **prop_values, int num_props, const char *node_path);

KBRow *find_node_ids(void *conn, const char *base_table, const char *kb, const char *label, const char *node_name, 
    const char **prop_keys, const char **prop_values, int num_props, const char *node_path, int *num_results);

KBRow *find_node_id(void *conn, const char *base_table, const char *kb, const char *label, const char *node_name,
     const char **prop_keys, const char **prop_values, int num_props, const char *node_path);

#ifdef __cplusplus
}
#endif

#endif
