


#include "system_def.h"

#include <libpq-fe.h>
#include <string.h>
#include <stdlib.h>
#include <stdio.h>
#include <unistd.h>
//#include <cjson/cJSON.h>
#include "kb_stream_table.h"

void print_error(char **error_msg,char *message){
    if (error_msg){
        *error_msg = message;
    }
}
/*
 * Find the oldest record (by recorded_at) for the given path,
 * update it with new data, fresh timestamp, and set valid=TRUE.
 * This implements a true circular buffer pattern that ignores the valid status
 * and always replaces the oldest record by time.
 *
 * Args:
 *     conn (PGconn *): The PostgreSQL connection.
 *     base_table (const char *): The base table name.
 *     path (const char *): The path in LTREE format.
 *     data (const char *): The serialized JSON string to write.
 *     max_retries (int): Max attempts if rows are locked.
 *     retry_delay (double): Seconds to wait between retries.
 *     error_msg (char **): Output pointer for error message (caller must free if not NULL).
 *
 * Returns:
 *     int: 0 on success, -1 on failure.
 *     If -1, check *error_msg for details.
 */
int push_stream_data(void *connection, const char *base_table, const char *path, const char *data, int max_retries, double retry_delay, char **error_msg) {
    PGconn *conn = (PGconn *)connection;
    print_error(error_msg, NULL);

    if (!path || strlen(path) == 0) {
        print_error(error_msg, "Path cannot be empty or None");
        return -1;
    }

  

    char query_buf[1024];
    char *inner_error = NULL;
    
    for (int attempt = 1; attempt <= max_retries; attempt++) {
        inner_error = NULL;

        PGresult *res = PQexec(conn, "BEGIN");
        if (PQresultStatus(res) != PGRES_COMMAND_OK) {
            print_error(error_msg, PQerrorMessage(conn));
            PQclear(res);
            return -1;
        }
        PQclear(res);
        

        // 1) ensure there's at least one record to update
        snprintf(query_buf, sizeof(query_buf), "SELECT COUNT(*) as count FROM %s WHERE path = $1", base_table);
        const char *param_values[1] = {path};
        res = PQexecParams(conn, query_buf, 1, NULL, param_values, NULL, NULL, 0);
        if (PQresultStatus(res) != PGRES_TUPLES_OK) {
            inner_error = strdup(PQerrorMessage(conn));
            PQclear(res);
            goto rollback_retry;
        }
        int total = atoi(PQgetvalue(res, 0, 0));
        PQclear(res);
        if (total == 0) {
            inner_error = malloc(256);
            snprintf(inner_error, 256, "No records found for path='%s'. Records must be pre-allocated for stream tables.", path);
            goto rollback_retry;
        }

        // 2) try to lock the oldest record regardless of valid status (true circular buffer)
        snprintf(query_buf, sizeof(query_buf), "SELECT id FROM %s WHERE path = $1 ORDER BY recorded_at ASC FOR UPDATE SKIP LOCKED LIMIT 1", base_table);
        res = PQexecParams(conn, query_buf, 1, NULL, param_values, NULL, NULL, 0);
        if (PQresultStatus(res) != PGRES_TUPLES_OK) {
            inner_error = strdup(PQerrorMessage(conn));
            PQclear(res);
            goto rollback_retry;
        }
        if (PQntuples(res) == 0) {
            PQclear(res);
            PGresult *rb = PQexec(conn, "ROLLBACK");
            PQclear(rb);
            if (attempt < max_retries) {
                usleep((useconds_t)(retry_delay * 1000000.0));
                continue;
            } else {
                if (error_msg){
                    *error_msg = malloc(256);
                    snprintf(*error_msg, 256, "Could not lock any row for path='%s' after %d attempts", path, max_retries);
                }
                return -1;
            }
        }

        char *record_id_str = strdup(PQgetvalue(res, 0, 0));
        PQclear(res);
        
        // 3) perform the update with valid=TRUE (always overwrites oldest record)
        snprintf(query_buf, sizeof(query_buf), "UPDATE %s SET data = $1, recorded_at = NOW(), valid = TRUE WHERE id = $2 RETURNING id", base_table);
        const char *update_params[2] = {data, record_id_str};
        res = PQexecParams(conn, query_buf, 2, NULL, update_params, NULL, NULL, 0);
        free(record_id_str);
        if (PQresultStatus(res) != PGRES_TUPLES_OK || PQntuples(res) != 1) {
            inner_error = strdup(PQresultStatus(res) != PGRES_TUPLES_OK ? PQerrorMessage(conn) : "Failed to update record");
            PQclear(res);
            goto rollback_retry;
        }
        PQclear(res);

        res = PQexec(conn, "COMMIT");
        if (PQresultStatus(res) != PGRES_COMMAND_OK) {
            inner_error = strdup(PQerrorMessage(conn));
            PQclear(res);
            goto rollback_retry;
        }
        PQclear(res);

        return 0;

    rollback_retry:
        {
            PGresult *rb = PQexec(conn, "ROLLBACK");
            PQclear(rb);
        }
        if (inner_error) {
            if (strstr(inner_error, "No records found")) {
                print_error(error_msg, inner_error);
                return -1;
            } else {
                if (attempt < max_retries) {
                    free(inner_error);
                    usleep((useconds_t)(retry_delay * 1000000.0));
                    continue;
                } else {
                    if (error_msg){
                        char *buf = malloc(strlen(inner_error) + 256);
                         snprintf(buf, strlen(inner_error) + 256, "Error pushing stream data for path '%s': %s", path, inner_error);
                        free(inner_error);
                        *error_msg = buf;
                    }
                    return -1;
                }
            }
        }
    }

    print_error(error_msg, "Unexpected error in push_stream_data");
    return -1;
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

int main(void){
    char password[256];
    printf("Enter password: "); 
    fgets(password, sizeof(password), stdin); 

    PGconn *conn = create_pg_connection("knowledge_base", "gedgar", password, "localhost", "5432");
    if (!conn) {
        fprintf(stderr, "Failed to create PostgreSQL connection\n");
        return 1;
    }
    int success = push_stream_data((void *)conn, "knowledge_base_stream", "kb1.header1_link.header1_name.KB_STREAM_FIELD.info1_stream", 
        "{\"prop1\":\"value1\",\"prop2\":\"value2\",\"prop3\":\"value3\"}", 3, 1.0, NULL);
    printf("success: %d\n", success);
    if (success != 0){
        printf("error: %s\n", error_msg);
    }
    return 0;

}

#endif