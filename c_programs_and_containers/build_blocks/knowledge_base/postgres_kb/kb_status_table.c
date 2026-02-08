#include "system_def.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <libpq-fe.h> // PostgreSQL library
#include <unistd.h> // For usleep
#include "kb_status_table.h"



// Function to retrieve status data for a given path
int get_status_data(void *connection, const char *base_table, const char *path, char **data_str) {
    PGconn *conn = (PGconn *)connection;
    if (!path || strlen(path) == 0) {
        fprintf(stderr, "Path cannot be empty or NULL\n");
        return -1;
    }

    // Prepare parameterized query
    char query[512];
    snprintf(query, sizeof(query), "SELECT data FROM %s WHERE path = $1 LIMIT 1", base_table);

    // Set up parameters
    const char *param_values[1] = {path};
    int param_lengths[1] = {(int)strlen(path)};
    int param_formats[1] = {0}; // Text format

    // Execute query
    PGresult *res = PQexecParams(conn, query, 1, NULL, param_values, param_lengths, param_formats, 0);
    if (PQresultStatus(res) != PGRES_TUPLES_OK) {
        fprintf(stderr, "Error executing query: %s\n", PQresultErrorMessage(res));
        PQclear(res);
        return -1;
    }

    // Check if result is empty
    if (PQntuples(res) == 0) {
        fprintf(stderr, "No data found for path: %s\n", path);
        PQclear(res);
        return -1;
    }

    // Extract data
    char *data = PQgetvalue(res, 0, 0);

    // Allocate and copy data string
    *data_str = strdup(data);
    if (!*data_str) {
        fprintf(stderr, "Memory allocation failed for data\n");
        PQclear(res);
        return -1;
    }

    // Clean up
    PQclear(res);
    return 0;
}

int set_status_data(void *connection, const char *base_table, const char *path, const char *data, int retry_count, double retry_delay, int *success, char *message) {
    PGconn *conn = (PGconn *)connection;
    if (!path || strlen(path) == 0) {
        fprintf(stderr, "Path cannot be empty or NULL\n");
        return -1;
    }
    if (!data || strlen(data) == 0) {
        fprintf(stderr, "Data cannot be empty or NULL\n");
        return -1;
    }
    if (retry_count < 0) {
        fprintf(stderr, "Retry count must be non-negative\n");
        return -1;
    }
    if (retry_delay < 0) {
        fprintf(stderr, "Retry delay must be non-negative\n");
        return -1;
    }

    // Prepare parameterized query
    char query[512];
    snprintf(query, sizeof(query), 
        "INSERT INTO %s (path, data) "
        "VALUES ($1, $2) "
        "ON CONFLICT (path) "
        "DO UPDATE SET data = EXCLUDED.data "
        "RETURNING path, (xmax = 0) AS was_inserted", base_table);

    // Set up parameters
    const char *param_values[2] = {path, data};
    int param_lengths[2] = {(int)strlen(path), (int)strlen(data)};
    int param_formats[2] = {0, 0}; // Text format

    char *last_error = NULL;
    int attempt = 0;
    int result = -1;

    while (attempt <= retry_count) {
        // Start transaction
        PGresult *begin_res = PQexec(conn, "BEGIN");
        if (PQresultStatus(begin_res) != PGRES_COMMAND_OK) {
            fprintf(stderr, "Error starting transaction: %s\n", PQresultErrorMessage(begin_res));
            PQclear(begin_res);
            free(last_error);
            return -1;
        }
        PQclear(begin_res);

        // Execute query
        PGresult *res = PQexecParams(conn, query, 2, NULL, param_values, param_lengths, param_formats, 0);
        ExecStatusType status = PQresultStatus(res);

        if (status == PGRES_TUPLES_OK) {
            if (PQntuples(res) > 0) {
                char *returned_path = PQgetvalue(res, 0, 0);
                int was_inserted = strcmp(PQgetvalue(res, 0, 1), "t") == 0 ? 1 : 0;
                const char *operation = was_inserted ? "inserted" : "updated";

                // Commit transaction
                PGresult *commit_res = PQexec(conn, "COMMIT");
                if (PQresultStatus(commit_res) != PGRES_COMMAND_OK) {
                    fprintf(stderr, "Error committing transaction: %s\n", PQresultErrorMessage(commit_res));
                    PQclear(commit_res);
                    PQclear(res);
                    free(last_error);
                    return -1;
                }
                PQclear(commit_res);

                // Set success and message (if provided)
                *success = 1;
                if (message) {
                    snprintf(message, 256, "Successfully %s data for path: %s", operation, returned_path);
                }
                PQclear(res);
                result = 0;
                break;
            } else {
                // Rollback transaction
                PGresult *rollback_res = PQexec(conn, "ROLLBACK");
                PQclear(rollback_res);
                fprintf(stderr, "Database operation completed but no result was returned\n");
                PQclear(res);
                free(last_error);
                return -1;
            }
        } else {
            // Handle transient errors
            last_error = strdup(PQresultErrorMessage(res));
            PQclear(res);

            // Rollback transaction
            PGresult *rollback_res = PQexec(conn, "ROLLBACK");
            PQclear(rollback_res);

            if (attempt < retry_count) {
                // Sleep for retry_delay (convert seconds to microseconds)
                usleep((unsigned int)(retry_delay * 1000000));
                attempt++;
            } else {
                // Exhausted retries
                *success = 0;
                if (message) {
                    snprintf(message, 512, "Failed to set status data for path '%s' after %d attempts: %s", 
                             path, retry_count + 1, last_error ? last_error : "Unknown error");
                }
                result = -1;
                break;
            }
        }
    }

    // Clean up
    free(last_error);
    return result;
}
#ifdef __MAIN__

// Instantiate a PostgreSQL connection
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

int main(void){
    char password[256];
    
    printf("Enter password: "); 
    fgets(password, sizeof(password), stdin); 
    password[strcspn(password, "\n")] = '\0';
    PGconn *conn = create_pg_connection("knowledge_base", "gedgar", password, "localhost", "5432");
    if (!conn) {
        fprintf(stderr, "Failed to create PostgreSQL connection\n");
        return 1;
    }
    void *conn_ptr = (void *)conn;
    const char *base_table = "knowledge_base_status";

    char *data_str;
    get_status_data((void *)conn,base_table, "kb1.header1_link.header1_name.KB_STATUS_FIELD.info2_status", &data_str);
    printf("Data: %s\n", data_str);
    free(data_str);

    char *data_write_1 = "{\"prop1\":\"value1\",\"prop2\":\"value2\",\"prop3\":\"value3\"}";
    int success;
    char message[512];
    set_status_data((void *)conn,base_table, "kb1.header1_link.header1_name.KB_STATUS_FIELD.info2_status", data_write_1, 3, 1.0, &success, message);
    printf("Success: %d\n", success);
    printf("Message: %s\n", message);
    get_status_data((void *)conn,base_table, "kb1.header1_link.header1_name.KB_STATUS_FIELD.info2_status", &data_str);
    printf("Data: %s\n", data_str);
    free(data_str);
    char *data_write_2 = "{\"prop1\":\"value1\",\"prop2\":\"value2\"}";
    set_status_data((void *)conn,base_table, "kb1.header1_link.header1_name.KB_STATUS_FIELD.info2_status", data_write_2, 3, 1.0, &success, message);
    printf("Success: %d\n", success);
    printf("Message: %s\n", message);
    get_status_data((void *)conn,base_table, "kb1.header1_link.header1_name.KB_STATUS_FIELD.info2_status", &data_str);
    printf("Data: %s\n", data_str);
    free(data_str);
   
    return 0;
}

#endif