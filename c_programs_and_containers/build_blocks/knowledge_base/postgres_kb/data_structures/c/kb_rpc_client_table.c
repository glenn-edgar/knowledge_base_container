#include "system_def.h"

#include <libpq-fe.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <uuid/uuid.h>
#include "kb_rpc_client_table.h"


/* Note: Caller is responsible for freeing the strings in RPCRow using free() */

int find_free_slots(void *connection, const char *base_table, const char *client_path) {
    PGconn *conn = (PGconn *)connection;
    char query[1024];
    char *esc_table = PQescapeIdentifier(conn, base_table, strlen(base_table));
    if (!esc_table) {
        fprintf(stderr, "Failed to escape table identifier\n");
        return -1;
    }
    snprintf(query, sizeof(query),
             "SELECT "
             "COUNT(*) as total_records, "
             "COUNT(*) FILTER (WHERE is_new_result = FALSE) as free_slots "
             "FROM %s "
             "WHERE client_path = $1", esc_table);
    PQfreemem(esc_table);

    const char *params[1] = {client_path};
    PGresult *res = PQexecParams(conn, query, 1, NULL, params, NULL, NULL, 0);
    if (PQresultStatus(res) != PGRES_TUPLES_OK) {
        fprintf(stderr, "Query failed: %s\n", PQerrorMessage(conn));
        PQclear(res);
        return -1;
    }
    if (PQntuples(res) == 0) {
        PQclear(res);
        return -1;
    }

    long total_records = atol(PQgetvalue(res, 0, 0));
    long free_slots = atol(PQgetvalue(res, 0, 1));
    PQclear(res);

    if (total_records == 0) {
        fprintf(stderr, "No records found for client_path: %s\n", client_path);
        return -2;  // Distinct error code for no records
    }

    return (int)free_slots;
}

int find_queued_slots(void *connection, const char *base_table, const char *client_path) {
    PGconn *conn = (PGconn *)connection;
    char query[1024];
    char *esc_table = PQescapeIdentifier(conn, base_table, strlen(base_table));
    if (!esc_table) {
        fprintf(stderr, "Failed to escape table identifier\n");
        return -1;
    }
    snprintf(query, sizeof(query),
             "SELECT "
             "COUNT(*) as total_records, "
             "COUNT(*) FILTER (WHERE is_new_result = TRUE) as queued_slots "
             "FROM %s "
             "WHERE client_path = $1", esc_table);
    PQfreemem(esc_table);

    const char *params[1] = {client_path};
    PGresult *res = PQexecParams(conn, query, 1, NULL, params, NULL, NULL, 0);
    if (PQresultStatus(res) != PGRES_TUPLES_OK) {
        fprintf(stderr, "Query failed: %s\n", PQerrorMessage(conn));
        PQclear(res);
        return -1;
    }
    if (PQntuples(res) == 0) {
        PQclear(res);
        return -1;
    }

    long total_records = atol(PQgetvalue(res, 0, 0));
    long queued_slots = atol(PQgetvalue(res, 0, 1));
    PQclear(res);

    if (total_records == 0) {
        fprintf(stderr, "No records found for client_path: %s\n", client_path);
        return -2;  // Distinct error code for no records
    }

    return (int)queued_slots;
}

RPCRow *peak_and_claim_reply_data(void *connection, const char *base_table, const char *client_path,
                                  int max_retries, float retry_delay) {
    PGconn *conn = (PGconn *)connection;
    int attempt = 0;
    char update_query[2048];
    char check_query[1024];
    char *esc_table = PQescapeIdentifier(conn, base_table, strlen(base_table));
    if (!esc_table) {
        fprintf(stderr, "Failed to escape table identifier\n");
        return NULL;
    }
    snprintf(update_query, sizeof(update_query),
             "UPDATE %s "
             "SET is_new_result = FALSE "
             "WHERE id = ("
             "    SELECT id "
             "    FROM %s "
             "    WHERE client_path = $1 "
             "    AND is_new_result = TRUE "
             "    ORDER BY response_timestamp ASC "
             "    FOR UPDATE SKIP LOCKED "
             "    LIMIT 1"
             ") "
             "RETURNING *", esc_table, esc_table);
    snprintf(check_query, sizeof(check_query),
             "SELECT EXISTS ("
             "    SELECT 1 FROM %s "
             "    WHERE client_path = $1 AND is_new_result = TRUE"
             ")", esc_table);
    PQfreemem(esc_table);

    while (attempt < max_retries) {
        PGresult *res = PQexec(conn, "BEGIN");
        if (PQresultStatus(res) != PGRES_COMMAND_OK) {
            fprintf(stderr, "BEGIN failed: %s\n", PQerrorMessage(conn));
            PQclear(res);
            return NULL;
        }
        PQclear(res);

        const char *params[1] = {client_path};
        res = PQexecParams(conn, update_query, 1, NULL, params, NULL, NULL, 0);

        const char *sqlstate = PQresultErrorField(res, PG_DIAG_SQLSTATE);
        if (PQresultStatus(res) != PGRES_TUPLES_OK) {
            if (sqlstate && strcmp(sqlstate, "55P03") == 0) {
                PQclear(res);
                PQexec(conn, "ROLLBACK");
                attempt++;
                usleep((useconds_t)(retry_delay * 1000000));
                continue;
            } else {
                fprintf(stderr, "Update failed: %s\n", PQerrorMessage(conn));
                PQclear(res);
                PQexec(conn, "ROLLBACK");
                return NULL;
            }
        }

        if (PQntuples(res) > 0) {
            RPCRow *row = (RPCRow *)malloc(sizeof(RPCRow));
            if (!row) {
                PQclear(res);
                PQexec(conn, "ROLLBACK");
                return NULL;
            }
            row->id = atoi(PQgetvalue(res, 0, 0));
            row->request_id = strdup(PQgetvalue(res, 0, 1));
            row->client_path = strdup(PQgetvalue(res, 0, 2));
            row->server_path = strdup(PQgetvalue(res, 0, 3));
            row->transaction_tag = strdup(PQgetvalue(res, 0, 4));
            row->rpc_action = strdup(PQgetvalue(res, 0, 5));
            row->response_payload = strdup(PQgetvalue(res, 0, 6));
            row->response_timestamp = strdup(PQgetvalue(res, 0, 7));
            row->is_new_result = (strcmp(PQgetvalue(res, 0, 8), "t") == 0) ? 1 : 0;

            PQclear(res);
            PQexec(conn, "COMMIT");
            return row;
        }
        PQclear(res);

        res = PQexecParams(conn, check_query, 1, NULL, params, NULL, NULL, 0);
        if (PQresultStatus(res) != PGRES_TUPLES_OK) {
            fprintf(stderr, "Check query failed: %s\n", PQerrorMessage(conn));
            PQclear(res);
            PQexec(conn, "ROLLBACK");
            return NULL;
        }
        int exists = (strcmp(PQgetvalue(res, 0, 0), "t") == 0) ? 1 : 0;
        PQclear(res);
        PQexec(conn, "ROLLBACK");

        if (!exists) {
            return NULL;
        }

        attempt++;
        usleep((useconds_t)(retry_delay * 1000000));
    }

    fprintf(stderr, "Could not lock a new-reply row after %d attempts\n", max_retries);
    return NULL;
}

int clear_reply_queue(void *connection, const char *base_table, const char *client_path,
                      int max_retries, float retry_delay) {
    PGconn *conn = (PGconn *)connection;
    int attempt = 0;
    char select_query[1024];
    char update_query[2048];
    char *esc_table = PQescapeIdentifier(conn, base_table, strlen(base_table));
    if (!esc_table) {
        fprintf(stderr, "Failed to escape table identifier\n");
        return -1;
    }
    snprintf(select_query, sizeof(select_query),
             "SELECT id "
             "FROM %s "
             "WHERE client_path = $1 "
             "FOR UPDATE NOWAIT", esc_table);
    snprintf(update_query, sizeof(update_query),
             "UPDATE %s "
             "SET "
             "    request_id = $1, "
             "    server_path = $2, "
             "    response_payload = $3, "
             "    response_timestamp = NOW(), "
             "    is_new_result = FALSE "
             "WHERE id = $4", esc_table);
    PQfreemem(esc_table);

    while (attempt < max_retries) {
        PGresult *res = PQexec(conn, "BEGIN");
        if (PQresultStatus(res) != PGRES_COMMAND_OK) {
            fprintf(stderr, "BEGIN failed: %s\n", PQerrorMessage(conn));
            PQclear(res);
            return -1;
        }
        PQclear(res);

        const char *sel_params[1] = {client_path};
        res = PQexecParams(conn, select_query, 1, NULL, sel_params, NULL, NULL, 0);

        const char *sqlstate = PQresultErrorField(res, PG_DIAG_SQLSTATE);
        if (PQresultStatus(res) != PGRES_TUPLES_OK) {
            if (sqlstate && strcmp(sqlstate, "55P03") == 0) {
                PQclear(res);
                PQexec(conn, "ROLLBACK");
                attempt++;
                usleep((useconds_t)(retry_delay * 1000000));
                continue;
            } else {
                fprintf(stderr, "Select failed: %s\n", PQerrorMessage(conn));
                PQclear(res);
                PQexec(conn, "ROLLBACK");
                return -1;
            }
        }

        int num_rows = PQntuples(res);
        if (num_rows == 0) {
            PQclear(res);
            PQexec(conn, "COMMIT");
            return 0;
        }

        int updated = 0;
        for (int i = 0; i < num_rows; ++i) {
            const char *row_id = PQgetvalue(res, i, 0);

            uuid_t uuid_bin;
            uuid_generate_random(uuid_bin);
            char new_uuid[37];
            uuid_unparse(uuid_bin, new_uuid);

            const char *empty_json = "{}";
            const char *upd_params[4] = {new_uuid, client_path, empty_json, row_id};

            PGresult *upd_res = PQexecParams(conn, update_query, 4, NULL, upd_params, NULL, NULL, 0);
            if (PQresultStatus(upd_res) != PGRES_COMMAND_OK) {
                fprintf(stderr, "Update failed: %s\n", PQerrorMessage(conn));
                PQclear(upd_res);
                PQexec(conn, "ROLLBACK");
                PQclear(res);
                return -1;
            }
            updated += atoi(PQcmdTuples(upd_res));
            PQclear(upd_res);
        }

        PQclear(res);
        PQexec(conn, "COMMIT");
        return updated;
    }

    fprintf(stderr, "Could not acquire lock after %d retries\n", max_retries);
    return -1;
}

int push_and_claim_reply_data(void *connection, const char *base_table, const char *client_path,
    const char *request_uuid, const char *server_path,
    const char *rpc_action, const char *transaction_tag,
    const char *reply_payload,
    int max_retries, float retry_delay) {
    PGconn *conn = (PGconn *)connection;
    int attempt = 0;
    char query[4096];
    char *esc_table = PQescapeIdentifier(conn, base_table, strlen(base_table));
    if (!esc_table) {
        fprintf(stderr, "Failed to escape table identifier\n");
        return -1;
    }

    // Handle NULL request_uuid case
    if (request_uuid == NULL) {
        snprintf(query, sizeof(query),
            "WITH candidate AS ("
            "    SELECT id "
            "    FROM %s "
            "    WHERE client_path = $1 "
            "    AND is_new_result = FALSE "
            "    ORDER BY response_timestamp ASC "
            "    FOR UPDATE SKIP LOCKED "
            "    LIMIT 1"
            ") "
            "UPDATE %s "
            "SET "
            "    request_id = gen_random_uuid(), "
            "    server_path = $2, "
            "    rpc_action = $3, "
            "    transaction_tag = $4, "
            "    response_payload = $5, "
            "    is_new_result = TRUE, "
            "    response_timestamp = CURRENT_TIMESTAMP "
            "FROM candidate "
            "WHERE %s.id = candidate.id "
            "RETURNING %s.id", esc_table, esc_table, esc_table, esc_table);
    } else {
        snprintf(query, sizeof(query),
            "WITH candidate AS ("
            "    SELECT id "
            "    FROM %s "
            "    WHERE client_path = $1 "
            "    AND is_new_result = FALSE "
            "    ORDER BY response_timestamp ASC "
            "    FOR UPDATE SKIP LOCKED "
            "    LIMIT 1"
            ") "
            "UPDATE %s "
            "SET "
            "    request_id = $2, "
            "    server_path = $3, "
            "    rpc_action = $4, "
            "    transaction_tag = $5, "
            "    response_payload = $6, "
            "    is_new_result = TRUE, "
            "    response_timestamp = CURRENT_TIMESTAMP "
            "FROM candidate "
            "WHERE %s.id = candidate.id "
            "RETURNING %s.id", esc_table, esc_table, esc_table, esc_table);
    }

    PQfreemem(esc_table);

    char *last_error = NULL;
    while (attempt <= max_retries) {
        PGresult *res = PQexec(conn, "BEGIN");
        if (PQresultStatus(res) != PGRES_COMMAND_OK) {
            free(last_error);
            last_error = strdup(PQerrorMessage(conn));
            fprintf(stderr, "BEGIN failed: %s\n", last_error);
            PQclear(res);
            attempt++;
            if (attempt > max_retries) {
                free(last_error);
                return -1;
            }
            usleep((useconds_t)(retry_delay * 1000000));
            continue;
        }
        PQclear(res);

        // Prepare parameters based on whether request_uuid is NULL
        if (request_uuid == NULL) {
            const char *params[5] = {client_path, server_path, rpc_action, transaction_tag, reply_payload};
            res = PQexecParams(conn, query, 5, NULL, params, NULL, NULL, 0);
        } else {
            const char *params[6] = {client_path, request_uuid, server_path, rpc_action, transaction_tag, reply_payload};
            res = PQexecParams(conn, query, 6, NULL, params, NULL, NULL, 0);
        }

        if (PQresultStatus(res) != PGRES_TUPLES_OK) {
            free(last_error);
            last_error = strdup(PQerrorMessage(conn));
            PQclear(res);
            PQexec(conn, "ROLLBACK");
            attempt++;
            if (attempt > max_retries) {
                fprintf(stderr, "Failed after %d retries: %s\n", max_retries, last_error);
                free(last_error);
                return -1;
            }
            usleep((useconds_t)(retry_delay * 1000000));
            continue;
        }

        if (PQntuples(res) == 0) {
            PQclear(res);
            PQexec(conn, "ROLLBACK");
            fprintf(stderr, "No available record with is_new_result=FALSE found\n");
            free(last_error);
            return -2;
        }

        PQclear(res);
        PQexec(conn, "COMMIT");
        free(last_error);
        return 0;
    }

    free(last_error);
    return -1;
}


void free_rpc_row(RPCRow *row) {
    if (row) {
        if (row->request_id) {
            free(row->request_id);
        }
        if (row->client_path) {
            free(row->client_path);
        }
        if (row->server_path) {
            free(row->server_path);
        }
        if (row->transaction_tag) {
            free(row->transaction_tag);
        }
        if (row->rpc_action) {
            free(row->rpc_action);
        }
        if (row->response_payload) { 
            free(row->response_payload);
        }
        if (row->response_timestamp) {
            free(row->response_timestamp);
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

int main(void) {
    RPCRow *row = NULL;
    char password[256];
    char *base_table = "knowledge_base_rpc_client";
    char *client_path = "kb1.header1_link.header1_name.KB_RPC_CLIENT_FIELD.info1_client";
    char *request_uuid = generate_request_uuid();
    char *server_path = "kb1.header1_link.header1_name.KB_RPC_SERVER_FIELD.info1_server";
    char *rpc_action = "response_reply";
    char *transaction_tag = "1234567890";
    char *reply_payload = "{\"prop1\":\"value1\",\"prop2\":\"value2\",\"prop3\":\"value3\"}";

    printf("Enter password: "); 
    fgets(password, sizeof(password), stdin); 
    password[strcspn(password, "\n")] = '\0';
    PGconn *conn = create_pg_connection("knowledge_base", "gedgar", password, "localhost", "5432");
    if (!conn) {
        fprintf(stderr, "Failed to create PostgreSQL connection\n");
        return -1;
    }

    int free_slots = find_free_slots(conn, base_table, client_path);
    printf("free_slots: %d\n", free_slots);
    int queued_slots = find_queued_slots(conn, base_table, client_path);
    printf("queued_slots: %d\n", queued_slots);
    
    int updated_records = clear_reply_queue(conn, base_table, client_path, 3, 1.0);
    printf("updated_records: %d\n", updated_records);
    
    int success = push_and_claim_reply_data(conn, base_table, client_path, request_uuid, server_path, rpc_action, transaction_tag, reply_payload, 3, 1.0);
    printf("success: %d\n", success);
    free_slots = find_free_slots(conn, base_table, client_path);
    printf("free_slots: %d\n", free_slots);
    queued_slots = find_queued_slots(conn, base_table, client_path);
    printf("queued_slots: %d\n", queued_slots);

    row = peak_and_claim_reply_data(conn, base_table, client_path, 3, 1.0);
    printf("row->id: %d\n", row->id);
    printf("row->request_id: %s\n", row->request_id);
    printf("row->client_path: %s\n", row->client_path);
    printf("row->server_path: %s\n", row->server_path);
    printf("row->transaction_tag: %s\n", row->transaction_tag);
    printf("row->rpc_action: %s\n", row->rpc_action);
    printf("row->response_payload: %s\n", row->response_payload);
    printf("row->response_timestamp: %s\n", row->response_timestamp);
    printf("row->is_new_result: %d\n", row->is_new_result);
    free_rpc_row(row);

    free_slots = find_free_slots(conn, base_table, client_path);
    printf("free_slots: %d\n", free_slots);
    queued_slots = find_queued_slots(conn, base_table, client_path);
    printf("queued_slots: %d\n", queued_slots);

    return 0;
}

#endif