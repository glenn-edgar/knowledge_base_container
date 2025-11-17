#include "system_def.h"

#include <libpq-fe.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <ctype.h>
#include <math.h>
#include <uuid/uuid.h>
#include <stdbool.h>
#include "kb_rpc_server_table.h"


/* Caller is responsible for freeing strings in ServerRow using free() */

static unsigned long hash_string(const char *str) {
    unsigned long hash = 5381LL;
    int c;
    while ((c = *str++)) {
        hash = ((hash << 5) + hash) + c;
    }
    return hash;
}

static char *escape_table_identifier(PGconn *conn, const char *base_table) {
    const char *dot = strchr(base_table, '.');
    if (dot) {
        size_t schema_len = dot - base_table;
        char *schema = malloc(schema_len + 1);
        strncpy(schema, base_table, schema_len);
        schema[schema_len] = '\0';
        char *table = strdup(dot + 1);

        char *esc_schema = PQescapeIdentifier(conn, schema, strlen(schema));
        char *esc_table = PQescapeIdentifier(conn, table, strlen(table));

        char *result = malloc(strlen(esc_schema) + strlen(esc_table) + 2);
        sprintf(result, "%s.%s", esc_schema, esc_table);

        free(schema);
        free(table);
        PQfreemem(esc_schema);
        PQfreemem(esc_table);
        return result;
    } else {
        return PQescapeIdentifier(conn, base_table, strlen(base_table));
    }
}

static bool is_valid_ltree_path(const char *path) {
    if (!path || !*path) return false;

    char *copy = strdup(path);
    if (!copy) return false;

    char *token = strtok(copy, ".");
    while (token) {
        if (!*token) {
            free(copy);
            return false;
        }
        if (!isalpha((unsigned char)token[0]) && token[0] != '_') {
            free(copy);
            return false;
        }
        for (const char *c = token + 1; *c; c++) {
            if (!isalnum((unsigned char)*c) && *c != '_') {
                free(copy);
                return false;
            }
        }
        token = strtok(NULL, ".");
    }
    free(copy);
    return true;
}

static char *generate_uuid() {
    uuid_t uuid;
    uuid_generate_random(uuid);
    char *str = malloc(37);
    if (!str) return NULL;
    
    uuid_unparse_lower(uuid, str);
    return str;
}

int count_jobs_job_types(void *connection, const char *base_table, const char *server_path, const char *state) {
    PGconn *conn = (PGconn *)connection;
    if (!is_valid_ltree_path(server_path)) {
        fprintf(stderr, "Invalid ltree path: %s\n", server_path);
        return -2;
    }

    const char *valid_states[] = {"empty", "new_job", "processing", "completed_job", NULL};
    bool valid = false;
    for (int i = 0; valid_states[i]; i++) {
        if (strcmp(valid_states[i], state) == 0) {
            valid = true;
            break;
        }
    }
    if (!valid) {
        fprintf(stderr, "Invalid state: %s\n", state);
        return -1;
    }

    if (PQstatus(conn) != CONNECTION_OK) {
        fprintf(stderr, "Connection bad\n");
        return -1;
    }

    char *esc_table = escape_table_identifier(conn, base_table);
    char query[1024];
    snprintf(query, sizeof(query), 
             "SELECT COUNT(*) AS job_count "
             "FROM %s "
             "WHERE server_path = $1 "
             "AND state = $2", esc_table);
    PQfreemem(esc_table);

    const char *params[2] = {server_path, state};
    PGresult *res = PQexecParams(conn, "BEGIN;", 0, NULL, NULL, NULL, NULL, 0);
    if (PQresultStatus(res) != PGRES_COMMAND_OK) {
        fprintf(stderr, "BEGIN failed: %s\n", PQerrorMessage(conn));
        PQclear(res);
        return -1;
    }
    PQclear(res);

    res = PQexecParams(conn, query, 2, NULL, params, NULL, NULL, 0);
    if (PQresultStatus(res) != PGRES_TUPLES_OK) {
        fprintf(stderr, "Query failed: %s\n", PQerrorMessage(conn));
        PQexec(conn, "ROLLBACK");
        PQclear(res);
        return -1;
    }

    long count = atol(PQgetvalue(res, 0, 0));
    PQclear(res);

    res = PQexec(conn, "COMMIT");
    PQclear(res);

    return (int)count;
}

int count_processing_jobs(void *connection, const char *base_table, const char *server_path) {
    PGconn *conn = (PGconn *)connection;
    return count_jobs_job_types(conn, base_table, server_path, "processing");
}

int count_new_jobs(void *connection, const char *base_table, const char *server_path) {
    PGconn *conn = (PGconn *)connection;
    return count_jobs_job_types(conn, base_table, server_path, "new_job");
}

int count_empty_jobs(void *connection, const char *base_table, const char *server_path) {
    PGconn *conn = (PGconn *)connection;
    return count_jobs_job_types(conn, base_table, server_path, "empty");
}

ServerRow *push_rpc_server_queue(void *connection, const char *base_table, const char *server_path,
                          const char *request_id, const char *rpc_action, const char *request_payload_json, const char *transaction_tag,
                          int priority, const char *rpc_client_queue, int max_retries, float wait_time) {
    PGconn *conn = (PGconn *)connection;
    if (!is_valid_ltree_path(server_path)) {
        fprintf(stderr, "Invalid server_path\n");
        return NULL;
    }

    char *my_request_id = NULL;
    if (!request_id || !*request_id) {
        my_request_id = generate_uuid();
    } else {
        uuid_t temp;
        if (uuid_parse(request_id, temp) != 0) {
            fprintf(stderr, "Invalid UUID for request_id\n");
            return NULL;
        }
        my_request_id = strdup(request_id);
    }

    if (!my_request_id) {
        return NULL;
    }

    if (!rpc_action || !*rpc_action) {
        fprintf(stderr, "Invalid rpc_action\n");
        free(my_request_id);
        return NULL;
    }

    if (!request_payload_json) {
        fprintf(stderr, "request_payload_json cannot be NULL\n");
        free(my_request_id);
        return NULL;
    }

    // Assume request_payload_json is valid JSON string

    if (!transaction_tag || !*transaction_tag) {
        fprintf(stderr, "Invalid transaction_tag\n");
        free(my_request_id);
        return NULL;
    }

    if (rpc_client_queue && !is_valid_ltree_path(rpc_client_queue)) {
        fprintf(stderr, "Invalid rpc_client_queue\n");
        free(my_request_id);
        return NULL;
    }

    if (PQstatus(conn) != CONNECTION_OK) {
        fprintf(stderr, "Connection bad\n");
        free(my_request_id);
        return NULL;
    }

    char *esc_table = escape_table_identifier(conn, base_table);
    char priority_str[20];
    snprintf(priority_str, sizeof(priority_str), "%d", priority);

    int attempt = 0;
    const double max_wait = 8.0;

    while (attempt < max_retries) {
        PGresult *res = PQexec(conn, "BEGIN");
        if (PQresultStatus(res) != PGRES_COMMAND_OK) {
            fprintf(stderr, "BEGIN failed: %s\n", PQerrorMessage(conn));
            PQclear(res);
            free(my_request_id);
            return NULL;
        }
        PQclear(res);

        res = PQexec(conn, "SET TRANSACTION ISOLATION LEVEL SERIALIZABLE");
        if (PQresultStatus(res) != PGRES_COMMAND_OK) {
            fprintf(stderr, "SET ISOLATION failed: %s\n", PQerrorMessage(conn));
            PQexec(conn, "ROLLBACK");
            PQclear(res);
            free(my_request_id);
            return NULL;
        }
        PQclear(res);

        // Advisory lock
        char temp_key[1024];
        snprintf(temp_key, sizeof(temp_key), "%s:%s", base_table, server_path);
        unsigned long lock_key = hash_string(temp_key);
        char lock_query[256];
        snprintf(lock_query, sizeof(lock_query), "SELECT pg_advisory_xact_lock(%lu)", lock_key);
        res = PQexec(conn, lock_query);
        if (PQresultStatus(res) != PGRES_TUPLES_OK) {
            fprintf(stderr, "Advisory lock failed: %s\n", PQerrorMessage(conn));
            PQclear(res);
            PQexec(conn, "ROLLBACK");
            free(my_request_id);
            PQfreemem(esc_table);
            return NULL;
        }
        PQclear(res);

        // Select
        char select_query[1024];
        snprintf(select_query, sizeof(select_query),
             "SELECT id FROM %s "
             "WHERE server_path = $1 AND state = 'empty' "
             "ORDER BY priority DESC, request_timestamp ASC "
             "LIMIT 1 "
             "FOR UPDATE",
             esc_table);
        const char *sel_params[1] = {server_path};
        res = PQexecParams(conn, select_query, 1, NULL, sel_params, NULL, NULL, 0);

        const char *sqlstate = PQresultErrorField(res, PG_DIAG_SQLSTATE);
        if (PQresultStatus(res) != PGRES_TUPLES_OK) {
            if (sqlstate && (strcmp(sqlstate, "40001") == 0 || strcmp(sqlstate, "40P01") == 0)) {
                PQclear(res);
                PQexec(conn, "ROLLBACK");
                attempt += 1;
                if (attempt < max_retries) {
                    double sleep_time = fmin(wait_time * pow(2.0, attempt), max_wait);
                    usleep(sleep_time * 1000000);
                } else {
                    fprintf(stderr, "Max retries exceeded\n");
                    free(my_request_id);
                    PQfreemem(esc_table);
                    return NULL;
                }
                continue;
            } else {
                fprintf(stderr, "Select failed: %s\n", PQerrorMessage(conn));
                PQclear(res);
                PQexec(conn, "ROLLBACK");
                free(my_request_id);
                PQfreemem(esc_table);
                return NULL;
            }
        }

        if (PQntuples(res) == 0) {
            PQclear(res);
            PQexec(conn, "ROLLBACK");
            free(my_request_id);
            fprintf(stderr, "No matching empty slot found\n");
            PQfreemem(esc_table);
            return NULL;
        }

        int record_id = atoi(PQgetvalue(res, 0, 0));
        PQclear(res);

        // Update
        char update_query[4096];
        snprintf(update_query, sizeof(update_query),
             "UPDATE %s "
             "SET "
             "server_path = $1, "
             "request_id = $2, "
             "rpc_action = $3, "
             "request_payload = $4, "
             "transaction_tag = $5, "
             "priority = $6, "
             "rpc_client_queue = $7, "
             "state = 'new_job', "
             "request_timestamp = NOW() AT TIME ZONE 'UTC', "
             "completed_timestamp = NULL "
             "WHERE id = $8 "
             "RETURNING *", esc_table);

        char id_str[20];
        snprintf(id_str, sizeof(id_str), "%d", record_id);

        const char *upd_params[8] = {server_path, my_request_id, rpc_action, request_payload_json, transaction_tag, priority_str, rpc_client_queue, id_str};

        res = PQexecParams(conn, update_query, 8, NULL, upd_params, NULL, NULL, 0);

        sqlstate = PQresultErrorField(res, PG_DIAG_SQLSTATE);
        if (PQresultStatus(res) != PGRES_TUPLES_OK) {
            if (sqlstate && (strcmp(sqlstate, "40001") == 0 || strcmp(sqlstate, "40P01") == 0)) {
                PQclear(res);
                PQexec(conn, "ROLLBACK");
                attempt += 1;
                if (attempt < max_retries) {
                    double sleep_time = fmin(wait_time * pow(2.0, attempt), max_wait);
                    usleep(sleep_time * 1000000);
                } else {
                    fprintf(stderr, "Max retries exceeded\n");
                    free(my_request_id);
                    PQfreemem(esc_table);
                    return NULL;
                }
                continue;
            } else {
                fprintf(stderr, "Update failed: %s\n", PQerrorMessage(conn));
                PQclear(res);
                PQexec(conn, "ROLLBACK");
                free(my_request_id);
                PQfreemem(esc_table);
                return NULL;
            }
        }

        if (PQntuples(res) == 0) {
            PQclear(res);
            PQexec(conn, "ROLLBACK");
            fprintf(stderr, "Failed to update record\n");
            free(my_request_id);
            PQfreemem(esc_table);
            return NULL;
        }

        ServerRow *row = malloc(sizeof(ServerRow));
        if (!row) {
            PQclear(res);
            PQexec(conn, "ROLLBACK");
            free(my_request_id);
            PQfreemem(esc_table);
            return NULL;
        }

        int num_fields = PQnfields(res);
        for (int i = 0; i < num_fields; i++) {
            const char *field_name = PQfname(res, i);
            const char *value = PQgetvalue(res, 0, i);
            if (PQgetisnull(res, 0, i)) {
                value = NULL;
            }

            if (strcmp(field_name, "id") == 0) {
                row->id = value ? atoi(value) : 0; // Though id not null
            } else if (strcmp(field_name, "server_path") == 0) {
                row->server_path = value ? strdup(value) : NULL;
            } else if (strcmp(field_name, "request_id") == 0) {
                row->request_id = value ? strdup(value) : NULL;
            } else if (strcmp(field_name, "rpc_action") == 0) {
                row->rpc_action = value ? strdup(value) : NULL;
            } else if (strcmp(field_name, "request_payload") == 0) {
                row->request_payload = value ? strdup(value) : NULL;
            } else if (strcmp(field_name, "request_timestamp") == 0) {
                row->request_timestamp = value ? strdup(value) : NULL;
            } else if (strcmp(field_name, "transaction_tag") == 0) {
                row->transaction_tag = value ? strdup(value) : NULL;
            } else if (strcmp(field_name, "state") == 0) {
                row->state = value ? strdup(value) : NULL;
            } else if (strcmp(field_name, "priority") == 0) {
                row->priority = value ? atoi(value) : 0;
            } else if (strcmp(field_name, "processing_timestamp") == 0) {
                row->processing_timestamp = value ? strdup(value) : NULL;
            } else if (strcmp(field_name, "completed_timestamp") == 0) {
                row->completed_timestamp = value ? strdup(value) : NULL;
            } else if (strcmp(field_name, "rpc_client_queue") == 0) {
                row->rpc_client_queue = value ? strdup(value) : NULL;
            }
        }

        PQclear(res);
        PQexec(conn, "COMMIT");
        free(my_request_id);
        PQfreemem(esc_table);
        return row;
    }

    free(my_request_id);
    PQfreemem(esc_table);
    return NULL;
}

ServerRow *peak_server_queue(void *connection, const char *base_table, const char *server_path, int retries, float wait_time) {
    PGconn *conn = (PGconn *)connection;
    int attempt = 0;

    char *esc_table = escape_table_identifier(conn, base_table);

    while (attempt < retries) {
        PGresult *res = PQexec(conn, "BEGIN");
        if (PQresultStatus(res) != PGRES_COMMAND_OK) {
            fprintf(stderr, "BEGIN failed: %s\n", PQerrorMessage(conn));
            PQclear(res);
            PQfreemem(esc_table);
            return NULL;
        }
        PQclear(res);

        res = PQexec(conn, "SET TRANSACTION ISOLATION LEVEL SERIALIZABLE");
        if (PQresultStatus(res) != PGRES_COMMAND_OK) {
            fprintf(stderr, "SET TRANSACTION failed: %s\n", PQerrorMessage(conn));
            PQexec(conn, "ROLLBACK");
            PQclear(res);
            PQfreemem(esc_table);
            return NULL;
        }
        PQclear(res);

        char select_query[1024];
        snprintf(select_query, sizeof(select_query),
                 "SELECT * "
                 "FROM %s "
                 "WHERE server_path = $1 "
                 "AND state = 'new_job' "
                 "ORDER BY priority DESC, request_timestamp ASC "
                 "LIMIT 1 "
                 "FOR UPDATE SKIP LOCKED", esc_table);

        const char *params[1] = {server_path};
        res = PQexecParams(conn, select_query, 1, NULL, params, NULL, NULL, 0);

        const char *sqlstate = PQresultErrorField(res, PG_DIAG_SQLSTATE);
        if (PQresultStatus(res) != PGRES_TUPLES_OK) {
            if (sqlstate && (strcmp(sqlstate, "40001") == 0 || strcmp(sqlstate, "40P01") == 0)) {
                PQclear(res);
                PQexec(conn, "ROLLBACK");
                attempt += 1;
                if (attempt < retries) {
                    double sleep_time = wait_time * pow(2.0, attempt);
                    usleep(sleep_time * 1000000);
                } else {
                    fprintf(stderr, "Failed after %d attempts\n", retries);
                    PQfreemem(esc_table);
                    return NULL;
                }
                continue;
            } else {
                fprintf(stderr, "Select failed: %s\n", PQerrorMessage(conn));
                PQclear(res);
                PQexec(conn, "ROLLBACK");
                PQfreemem(esc_table);
                return NULL;
            }
        }

        if (PQntuples(res) == 0) {
            PQclear(res);
            PQexec(conn, "ROLLBACK");
            PQfreemem(esc_table);
            return NULL;
        }

        ServerRow *row = malloc(sizeof(ServerRow));
        if (!row) {
            PQclear(res);
            PQexec(conn, "ROLLBACK");
            PQfreemem(esc_table);
            return NULL;
        }

        int num_fields = PQnfields(res);
        for (int i = 0; i < num_fields; i++) {
            const char *field_name = PQfname(res, i);
            const char *value = PQgetvalue(res, 0, i);
            if (PQgetisnull(res, 0, i)) {
                value = NULL;
            }

            if (strcmp(field_name, "id") == 0) {
                row->id = value ? atoi(value) : 0;
            } else if (strcmp(field_name, "server_path") == 0) {
                row->server_path = value ? strdup(value) : NULL;
            } else if (strcmp(field_name, "request_id") == 0) {
                row->request_id = value ? strdup(value) : NULL;
            } else if (strcmp(field_name, "rpc_action") == 0) {
                row->rpc_action = value ? strdup(value) : NULL;
            } else if (strcmp(field_name, "request_payload") == 0) {
                row->request_payload = value ? strdup(value) : NULL;
            } else if (strcmp(field_name, "request_timestamp") == 0) {
                row->request_timestamp = value ? strdup(value) : NULL;
            } else if (strcmp(field_name, "transaction_tag") == 0) {
                row->transaction_tag = value ? strdup(value) : NULL;
            } else if (strcmp(field_name, "state") == 0) {
                row->state = value ? strdup(value) : NULL;
            } else if (strcmp(field_name, "priority") == 0) {
                row->priority = value ? atoi(value) : 0;
            } else if (strcmp(field_name, "processing_timestamp") == 0) {
                row->processing_timestamp = value ? strdup(value) : NULL;
            } else if (strcmp(field_name, "completed_timestamp") == 0) {
                row->completed_timestamp = value ? strdup(value) : NULL;
            } else if (strcmp(field_name, "rpc_client_queue") == 0) {
                row->rpc_client_queue = value ? strdup(value) : NULL;
            }
        }

        PQclear(res);

        char update_query[1024];
        snprintf(update_query, sizeof(update_query),
                 "UPDATE %s "
                 "SET state = 'processing', "
                 "processing_timestamp = NOW() AT TIME ZONE 'UTC' "
                 "WHERE id = $1 "
                 "RETURNING id", esc_table);

        char id_str[20];
        snprintf(id_str, sizeof(id_str), "%d", row->id);

        const char *upd_params[1] = {id_str};
        res = PQexecParams(conn, update_query, 1, NULL, upd_params, NULL, NULL, 0);

        sqlstate = PQresultErrorField(res, PG_DIAG_SQLSTATE);
        if (PQresultStatus(res) != PGRES_TUPLES_OK) {
            if (sqlstate && (strcmp(sqlstate, "40001") == 0 || strcmp(sqlstate, "40P01") == 0)) {
                PQclear(res);
                PQexec(conn, "ROLLBACK");
                free_server_row(row);
                attempt += 1;
                if (attempt < retries) {
                    double sleep_time = wait_time * pow(2.0, attempt);
                    usleep(sleep_time * 1000000);
                } else {
                    fprintf(stderr, "Failed after %d attempts\n", retries);
                    PQfreemem(esc_table);
                    return NULL;
                }
                continue;
            } else {
                fprintf(stderr, "Update failed: %s\n", PQerrorMessage(conn));
                PQclear(res);
                PQexec(conn, "ROLLBACK");
                free_server_row(row);
                PQfreemem(esc_table);
                return NULL;
            }
        }

        if (PQntuples(res) == 0) {
            PQclear(res);
            PQexec(conn, "ROLLBACK");
            free_server_row(row);
            PQfreemem(esc_table);
            return NULL;
        }

        PQclear(res);
        PQexec(conn, "COMMIT");
        PQfreemem(esc_table);
        return row;
    }

    PQfreemem(esc_table);
    return NULL;
}

int mark_job_completion(void *connection, const char *base_table, const char *server_path, int id, int retries, float wait_time) {
    PGconn *conn = (PGconn *)connection;
    int attempt = 0;

    char *esc_table = escape_table_identifier(conn, base_table);

    while (attempt < retries) {
        PGresult *res = PQexec(conn, "BEGIN");
        if (PQresultStatus(res) != PGRES_COMMAND_OK) {
            fprintf(stderr, "BEGIN failed: %s\n", PQerrorMessage(conn));
            PQclear(res);
            PQfreemem(esc_table);
            return -1;
        }
        PQclear(res);

        res = PQexec(conn, "SET TRANSACTION ISOLATION LEVEL SERIALIZABLE");
        if (PQresultStatus(res) != PGRES_COMMAND_OK) {
            fprintf(stderr, "SET TRANSACTION failed: %s\n", PQerrorMessage(conn));
            PQexec(conn, "ROLLBACK");
            PQclear(res);
            PQfreemem(esc_table);
            return -1;
        }
        PQclear(res);

        char verify_query[1024];
        snprintf(verify_query, sizeof(verify_query),
                 "SELECT id FROM %s "
                 "WHERE id = $1 "
                 "AND server_path = $2 "
                 "AND state = 'processing' "
                 "FOR UPDATE", esc_table);

        char id_str[20];
        snprintf(id_str, sizeof(id_str), "%d", id);

        const char *params[2] = {id_str, server_path};
        res = PQexecParams(conn, verify_query, 2, NULL, params, NULL, NULL, 0);

        const char *sqlstate = PQresultErrorField(res, PG_DIAG_SQLSTATE);
        if (PQresultStatus(res) != PGRES_TUPLES_OK) {
            if (sqlstate && (strcmp(sqlstate, "40001") == 0 || strcmp(sqlstate, "40P01") == 0)) {
                PQclear(res);
                PQexec(conn, "ROLLBACK");
                attempt++;
                if (attempt < retries) {
                    double sleep_time = wait_time * pow(2.0, attempt);
                    usleep(sleep_time * 1000000);
                } else {
                    PQfreemem(esc_table);
                    return -1;
                }
                continue;
            } else {
                fprintf(stderr, "Verify failed: %s\n", PQerrorMessage(conn));
                PQclear(res);
                PQexec(conn, "ROLLBACK");
                PQfreemem(esc_table);
                return -1;
            }
        }

        if (PQntuples(res) == 0) {
            PQclear(res);
            PQexec(conn, "ROLLBACK");
            PQfreemem(esc_table);
            return 0; // false
        }

        PQclear(res);

        char update_query[1024];
        snprintf(update_query, sizeof(update_query),
                 "UPDATE %s "
                 "SET state = 'empty', "
                 "completed_timestamp = NOW() AT TIME ZONE 'UTC' "
                 "WHERE id = $1 "
                 "RETURNING id", esc_table);

        res = PQexecParams(conn, update_query, 1, NULL, (const char *const []) {id_str}, NULL, NULL, 0);

        sqlstate = PQresultErrorField(res, PG_DIAG_SQLSTATE);
        if (PQresultStatus(res) != PGRES_TUPLES_OK) {
            if (sqlstate && (strcmp(sqlstate, "40001") == 0 || strcmp(sqlstate, "40P01") == 0)) {
                PQclear(res);
                PQexec(conn, "ROLLBACK");
                attempt++;
                if (attempt < retries) {
                    double sleep_time = wait_time * pow(2.0, attempt);
                    usleep(sleep_time * 1000000);
                } else {
                    PQfreemem(esc_table);
                    return -1;
                }
                continue;
            } else {
                fprintf(stderr, "Update failed: %s\n", PQerrorMessage(conn));
                PQclear(res);
                PQexec(conn, "ROLLBACK");
                PQfreemem(esc_table);
                return -1;
            }
        }

        int updated = PQntuples(res);
        PQclear(res);

        PQexec(conn, "COMMIT");
        PQfreemem(esc_table);
        return (updated > 0) ? 1 : 0;
    }

    PQfreemem(esc_table);
    return -1;
}

int clear_server_queue(void *connection, const char *base_table, const char *server_path, int max_retries, float retry_delay) {
    PGconn *conn = (PGconn *)connection;
    int attempt = 0;

    char *esc_table = escape_table_identifier(conn, base_table);

    while (attempt < max_retries) {
        PGresult *res = PQexec(conn, "BEGIN");
        if (PQresultStatus(res) != PGRES_COMMAND_OK) {
            fprintf(stderr, "BEGIN failed: %s\n", PQerrorMessage(conn));
            PQclear(res);
            PQfreemem(esc_table);
            return -1;
        }
        PQclear(res);

        char lock_query[1024];
        snprintf(lock_query, sizeof(lock_query),
                 "SELECT 1 FROM %s "
                 "WHERE server_path = $1 "
                 "FOR UPDATE NOWAIT", esc_table);

        const char *params[1] = {server_path};
        res = PQexecParams(conn, lock_query, 1, NULL, params, NULL, NULL, 0);

        const char *sqlstate = PQresultErrorField(res, PG_DIAG_SQLSTATE);
        if (PQresultStatus(res) != PGRES_TUPLES_OK) {
            if (sqlstate && strcmp(sqlstate, "55P03") == 0) {
                PQclear(res);
                PQexec(conn, "ROLLBACK");
                attempt++;
                if (attempt < max_retries) {
                    usleep(retry_delay * 1000000);
                } else {
                    fprintf(stderr, "Failed to acquire lock after %d retries\n", max_retries);
                    PQfreemem(esc_table);
                    return -1;
                }
                continue;
            } else {
                fprintf(stderr, "Lock query failed: %s\n", PQerrorMessage(conn));
                PQclear(res);
                PQexec(conn, "ROLLBACK");
                PQfreemem(esc_table);
                return -1;
            }
        }

        PQclear(res);

        char update_query[1024];
        snprintf(update_query, sizeof(update_query),
                 "UPDATE %s "
                 "SET request_id = gen_random_uuid(), "
                 "request_payload = '{}', "
                 "completed_timestamp = CURRENT_TIMESTAMP AT TIME ZONE 'UTC', "
                 "state = 'empty', "
                 "rpc_client_queue = NULL "
                 "WHERE server_path = $1", esc_table);

        res = PQexecParams(conn, update_query, 1, NULL, params, NULL, NULL, 0);

        if (PQresultStatus(res) != PGRES_COMMAND_OK) {
            fprintf(stderr, "Update failed: %s\n", PQerrorMessage(conn));
            PQclear(res);
            PQexec(conn, "ROLLBACK");
            PQfreemem(esc_table);
            return -1;
        }

        int row_count = atoi(PQcmdTuples(res));
        PQclear(res);

        PQexec(conn, "COMMIT");
        PQfreemem(esc_table);
        return row_count;
    }

    PQfreemem(esc_table);
    return -1;
}

void free_server_row(ServerRow *row){
    if (row){
        if (row->server_path){
            free(row->server_path);
        }
        if (row->request_id){
            free(row->request_id);
        }
        if (row->rpc_action){
            free(row->rpc_action);
        }
        if (row->request_payload){
            free(row->request_payload);
        }
        if (row->request_timestamp){
            free(row->request_timestamp);
        }
        if (row->transaction_tag){
            free(row->transaction_tag);
        }
        if (row->state){
            free(row->state);
        }
        if (row->processing_timestamp){
            free(row->processing_timestamp);
        }
        if (row->completed_timestamp){
            free(row->completed_timestamp);
        }
        if (row->rpc_client_queue){
            free(row->rpc_client_queue);
        }
        free(row);
    }
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


// Function to generate a UUID string (caller must free the returned string)
char* generate_request_uuid() {
    uuid_t uuid_bin;
    uuid_generate_random(uuid_bin);  // Generate a random UUID (v4)
    
    char* uuid_str = (char*)malloc(37);  // 36 chars + null terminator
    if (uuid_str == NULL) {
        fprintf(stderr, "Memory allocation failed\n");
        return NULL;
    }
    
    uuid_unparse(uuid_bin, uuid_str);  // Convert to string format
    return uuid_str;
}

void print_row_data(ServerRow *row){
    if (row){
    printf("id: %d\n", row->id);
    printf("server_path: %s\n", row->server_path);
    printf("request_id: %s\n", row->request_id);
    printf("rpc_action: %s\n", row->rpc_action);
    printf("request_payload: %s\n", row->request_payload);
    printf("request_timestamp: %s\n", row->request_timestamp);
    printf("transaction_tag: %s\n", row->transaction_tag);
    printf("state: %s\n", row->state);
    printf("processing_timestamp: %s\n", row->processing_timestamp);
    printf("completed_timestamp: %s\n", row->completed_timestamp);
    printf("rpc_client_queue: %s\n", row->rpc_client_queue);
    printf("priority: %d\n", row->priority);
    }
}

int main(void){
    ServerRow *row = NULL;
    char password[256];
    char *base_table = "knowledge_base_rpc_server";
    char *client_path = "kb1.header1_link.header1_name.KB_RPC_CLIENT_FIELD.info1_client";
    char *server_path = "kb1.header1_link.header1_name.KB_RPC_SERVER_FIELD.info1_server";
    int priority = 1;
    char *server_payload_json = "{\"prop1\":\"value1\",\"prop2\":\"value2\",\"prop3\":\"value3\"}";


    printf("Enter password: "); 
    fgets(password, sizeof(password), stdin); 
    password[strcspn(password, "\n")] = '\0';
    PGconn *conn = create_pg_connection("knowledge_base", "gedgar", password, "localhost", "5432");
    if (!conn) {
        fprintf(stderr, "Failed to create PostgreSQL connection\n");
        return -1;
    }

    
    int updated_records = clear_server_queue(conn, base_table, server_path, 3, 1.0);
    printf("updated_records: %d\n", updated_records);
    int new_jobs = count_new_jobs(conn, base_table, server_path);
    int empty_jobs = count_empty_jobs(conn, base_table, server_path);
    int processing_jobs = count_processing_jobs(conn, base_table, server_path);
    printf("new_jobs: %d\n", new_jobs);
    printf("empty_jobs: %d\n", empty_jobs);
    printf("processing_jobs: %d\n", processing_jobs);
    
    row = push_rpc_server_queue(conn, base_table, server_path,
        NULL, "rpc_action", server_payload_json, "transaction_tag",
        priority, client_path, 3, 1.0);
    if (row==NULL){
        printf("push_rpc_server_queue failed\n");
        return -1;
    }   
    free_server_row(row);
    printf("push_rpc_server_queue success\n");


    new_jobs = count_new_jobs(conn, base_table, server_path);
    empty_jobs = count_empty_jobs(conn, base_table, server_path);
    processing_jobs = count_processing_jobs(conn, base_table, server_path);
    printf("new_jobs: %d\n", new_jobs);
    printf("empty_jobs: %d\n", empty_jobs);
    printf("processing_jobs: %d\n", processing_jobs);

    row = peak_server_queue(conn, base_table, server_path, 3, 1.0);
    if (row==NULL){
        printf("No row found for peak_server_queue\n");
        return -1;
    }   
    int id = row->id;
    print_row_data(row);
    
    free_server_row(row);

    new_jobs = count_new_jobs(conn, base_table, server_path);
    empty_jobs = count_empty_jobs(conn, base_table, server_path);
    processing_jobs = count_processing_jobs(conn, base_table, server_path);
    printf("new_jobs: %d\n", new_jobs);
    printf("empty_jobs: %d\n", empty_jobs);
    printf("processing_jobs: %d\n", processing_jobs);

    int success = mark_job_completion(conn, base_table, server_path, id,3, 1.0);
    printf("mark_job_completion success: %d\n", success);
    if (success==0){
        printf("mark_job_completion failed\n");
        return -1;
    }   
    printf("mark_job_completion success\n");

    new_jobs = count_new_jobs(conn, base_table, server_path);
    empty_jobs = count_empty_jobs(conn, base_table, server_path);
    processing_jobs = count_processing_jobs(conn, base_table, server_path);
    printf("new_jobs: %d\n", new_jobs);
    printf("empty_jobs: %d\n", empty_jobs);
    printf("processing_jobs: %d\n", processing_jobs);
    
   
   return 0;
}
#endif
