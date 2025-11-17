#include "system_def.h"
#include <libpq-fe.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <math.h>
#include "kb_job_table.h"

// get_queued_number: Count valid jobs for a given path
int get_queued_number(void *connection, const char *base_table, const char *path, int *count, char *message) {
    PGconn *conn = (PGconn *)connection;
    if (!path || strlen(path) == 0) {
        fprintf(stderr, "Path cannot be empty or NULL\n");
        if (message) snprintf(message, 256, "Path cannot be empty or NULL");
        return -1;
    }

    // Prepare query
    char query[512];
    snprintf(query, sizeof(query),
        "SELECT COUNT(*) as count "
        "FROM %s "
        "WHERE path = $1 "
        "AND valid = TRUE", base_table);

    const char *param_values[1] = {path};
    int param_lengths[1] = {(int)strlen(path)};
    int param_formats[1] = {0}; // Text format

    // Start transaction
    PGresult *begin_res = PQexec(conn, "BEGIN");
    if (PQresultStatus(begin_res) != PGRES_COMMAND_OK) {
        fprintf(stderr, "Error starting transaction: %s\n", PQresultErrorMessage(begin_res));
        if (message) snprintf(message, 256, "Error starting transaction: %s", PQresultErrorMessage(begin_res));
        PQclear(begin_res);
        return -1;
    }
    PQclear(begin_res);

    // Execute query
    PGresult *res = PQexecParams(conn, query, 1, NULL, param_values, param_lengths, param_formats, 0);
    ExecStatusType status = PQresultStatus(res);

    if (status == PGRES_TUPLES_OK && PQntuples(res) > 0) {
        *count = atoi(PQgetvalue(res, 0, 0));

        // Commit transaction
        PGresult *commit_res = PQexec(conn, "COMMIT");
        if (PQresultStatus(commit_res) != PGRES_COMMAND_OK) {
            fprintf(stderr, "Error committing transaction: %s\n", PQresultErrorMessage(commit_res));
            if (message) snprintf(message, 256, "Error committing transaction: %s", PQresultErrorMessage(commit_res));
            PQclear(commit_res);
            PQclear(res);
            return -1;
        }
        PQclear(commit_res);
        PQclear(res);
        return 0;
    } else {
        // Rollback transaction
        PGresult *rollback_res = PQexec(conn, "ROLLBACK");
        PQclear(rollback_res);
        fprintf(stderr, "Error counting queued jobs for path '%s': %s\n", path, PQresultErrorMessage(res));
        if (message) snprintf(message, 256, "Error counting queued jobs for path '%s': %s", path, PQresultErrorMessage(res));
        PQclear(res);
        return -1;
    }
}

int get_free_number(void *connection, const char *base_table, const char *path, int *count, char *message) {
    PGconn *conn = (PGconn *)connection;
    // Check input parameters
    if (!conn || !base_table) {
        fprintf(stderr, "Invalid JobQueueContext: connection, or base_table is NULL\n");
        if (message) snprintf(message, 256, "Invalid JobQueueContext: connection, or base_table is NULL");
        return -1;
    }
    if (!path || strlen(path) == 0) {
        fprintf(stderr, "Path cannot be empty or NULL\n");
        if (message) snprintf(message, 256, "Path cannot be empty or NULL");
        return -1;
    }
    if (!count) {
        fprintf(stderr, "Count pointer cannot be NULL\n");
        if (message) snprintf(message, 256, "Count pointer cannot be NULL");
        return -1;
    }

    // Prepare query
    char query[512];
    if (snprintf(query, sizeof(query),
                 "SELECT COUNT(*) as count "
                 "FROM %s "
                 "WHERE path = $1 "
                 "AND valid = FALSE", base_table) >= sizeof(query)) {
        fprintf(stderr, "Query buffer overflow for path '%s'\n", path);
        if (message) snprintf(message, 256, "Query buffer overflow for path '%s'", path);
        return -1;
    }

    const char *param_values[1] = {path};
    int param_lengths[1] = {(int)strlen(path)};
    int param_formats[1] = {0}; // Text format

    // Start transaction
    PGresult *begin_res = PQexec(conn, "BEGIN");
    if (!begin_res || PQresultStatus(begin_res) != PGRES_COMMAND_OK) {
        fprintf(stderr, "Error starting transaction: %s\n", begin_res ? PQresultErrorMessage(begin_res) : "No result returned");
        if (message) snprintf(message, 256, "Error starting transaction: %s", begin_res ? PQresultErrorMessage(begin_res) : "No result returned");
        PQclear(begin_res);
        return -1;
    }
    PQclear(begin_res);

    // Execute query
    PGresult *res = PQexecParams(conn, query, 1, NULL, param_values, param_lengths, param_formats, 0);
    if (!res || PQresultStatus(res) != PGRES_TUPLES_OK || PQntuples(res) == 0) {
        fprintf(stderr, "Error counting free jobs for path '%s': %s\n", path, res ? PQresultErrorMessage(res) : "No result returned");
        if (message) snprintf(message, 256, "Error counting free jobs for path '%s': %s", path, res ? PQresultErrorMessage(res) : "No result returned");
        if (res) {
            // Rollback transaction
            PGresult *rollback_res = PQexec(conn, "ROLLBACK");
            PQclear(rollback_res);
            PQclear(res);
        }
        return -1;
    }

    // Safely extract count
    const char *count_str = PQgetvalue(res, 0, 0);
    if (!count_str || strlen(count_str) == 0) {
        fprintf(stderr, "Invalid count value for path '%s'\n", path);
        if (message) snprintf(message, 256, "Invalid count value for path '%s'", path);
        // Rollback transaction
        PGresult *rollback_res = PQexec(conn, "ROLLBACK");
        PQclear(rollback_res);
        PQclear(res);
        return -1;
    }

    // Debug: Print count string
   
    *count = atoi(count_str);
    

    // Commit transaction
    PGresult *commit_res = PQexec(conn, "COMMIT");
    if (!commit_res || PQresultStatus(commit_res) != PGRES_COMMAND_OK) {
        fprintf(stderr, "Error committing transaction: %s\n", commit_res ? PQresultErrorMessage(commit_res) : "No result returned");
        if (message) snprintf(message, 256, "Error committing transaction: %s", commit_res ? PQresultErrorMessage(commit_res) : "No result returned");
        PQclear(commit_res);
        PQclear(res);
        return -1;
    }
    PQclear(commit_res);
    PQclear(res);
    return 0;
}
// peak_job_data: Find and activate earliest scheduled job


int peak_job_data(void *connection, const char *base_table, const char *path, int max_retries, double retry_delay, JobInfo *job_info, char *message) {
    PGconn *conn = (PGconn *)connection;
    if (!path || strlen(path) == 0) {
        fprintf(stderr, "Path cannot be empty or NULL\n");
        if (message) snprintf(message, 256, "Path cannot be empty or NULL");
        return -1;
    }
    if (max_retries < 0 || retry_delay < 0) {
        fprintf(stderr, "Invalid max_retries or retry_delay\n");
        if (message) snprintf(message, 256, "Invalid max_retries or retry_delay");
        return -1;
    }
    job_info->found = 0;

    char find_query[512];
    snprintf(find_query, sizeof(find_query),
        "SELECT id, data, schedule_at "
        "FROM %s "
        "WHERE path = $1 "
        "AND valid = TRUE "
        "AND is_active = FALSE "
        "AND (schedule_at IS NULL OR schedule_at <= NOW()) "
        "ORDER BY schedule_at ASC NULLS FIRST "
        "FOR UPDATE SKIP LOCKED "
        "LIMIT 1", base_table);

    char update_query[512];
    snprintf(update_query, sizeof(update_query),
        "UPDATE %s "
        "SET started_at = NOW(), is_active = TRUE "
        "WHERE id = $1 "
        "AND is_active = FALSE AND valid = TRUE "
        "RETURNING id, started_at", base_table);

    int attempt = 0;
    while (attempt < max_retries) {
        // Start transaction
        PGresult *begin_res = PQexec(conn, "BEGIN");
        if (PQresultStatus(begin_res) != PGRES_COMMAND_OK) {
            fprintf(stderr, "Error starting transaction: %s\n", PQresultErrorMessage(begin_res));
            if (message) snprintf(message, 256, "Error starting transaction: %s", PQresultErrorMessage(begin_res));
            PQclear(begin_res);
            return -1;
        }
        PQclear(begin_res);

        // Find job
        const char *param_values[1] = {path};
        int param_lengths[1] = {(int)strlen(path)};
        int param_formats[1] = {0};
        PGresult *res = PQexecParams(conn, find_query, 1, NULL, param_values, param_lengths, param_formats, 0);
        ExecStatusType status = PQresultStatus(res);

        if (status == PGRES_TUPLES_OK && PQntuples(res) > 0) {
            int job_id = atoi(PQgetvalue(res, 0, 0));
            job_info->id = job_id;
            job_info->data = strdup(PQgetvalue(res, 0, 1));
        

            // Update job
            char job_id_str[32];
            snprintf(job_id_str, sizeof(job_id_str), "%d", job_id);
            const char *update_params[1] = {job_id_str};
            int update_lengths[1] = {(int)strlen(job_id_str)};
            PGresult *update_res = PQexecParams(conn, update_query, 1, NULL, update_params, update_lengths, param_formats, 0);
            status = PQresultStatus(update_res);

            if (status == PGRES_TUPLES_OK && PQntuples(update_res) > 0) {
                

                // Commit transaction
                PGresult *commit_res = PQexec(conn, "COMMIT");
                if (PQresultStatus(commit_res) != PGRES_COMMAND_OK) {
                    fprintf(stderr, "Error committing transaction: %s\n", PQresultErrorMessage(commit_res));
                    if (message) snprintf(message, 256, "Error committing transaction: %s", PQresultErrorMessage(commit_res));
                    PQclear(commit_res);
                    PQclear(update_res);
                    PQclear(res);
                    free(job_info->data);
                    
                    return -1;
                }
                PQclear(commit_res);
                PQclear(update_res);
                PQclear(res);
                job_info->found = 1;
                return 0;
            } else {
                // Rollback transaction
                PGresult *rollback_res = PQexec(conn, "ROLLBACK");
                PQclear(rollback_res);
                PQclear(update_res);
                PQclear(res);
                free(job_info->data);
                
                attempt++;
                if (attempt < max_retries) usleep((unsigned int)(retry_delay * 1000000 * pow(1.5, attempt)));
                continue;
            }
        } else {
            // Rollback transaction
            PGresult *rollback_res = PQexec(conn, "ROLLBACK");
            PQclear(rollback_res);
            PQclear(res);
            job_info->found = 0;
            return 0; // No job found
        }
    }

    fprintf(stderr, "Could not lock job for path '%s' after %d retries\n", path, max_retries);
    if (message) snprintf(message, 256, "Could not lock job for path '%s' after %d retries", path, max_retries);
    return -1;
}



int mark_job_completed(void *connection, const char *base_table, int job_id, int max_retries, double retry_delay,  char *message) {
    PGconn *conn = (PGconn *)connection;
    if (job_id <= 0) {
        fprintf(stderr, "job_id must be a valid integer\n");
        if (message) snprintf(message, 256, "job_id must be a valid integer");
        return -1;
    }

    char lock_query[512];
    snprintf(lock_query, sizeof(lock_query),
        "SELECT id FROM %s WHERE id = $1 FOR UPDATE NOWAIT", base_table);

    char update_query[512];
    snprintf(update_query, sizeof(update_query),
        "UPDATE %s "
        "SET completed_at = NOW(), valid = FALSE, is_active = FALSE "
        "WHERE id = $1 "
        "RETURNING id, completed_at", base_table);

    int attempt = 0;
    while (attempt < max_retries) {
        // Start transaction
        PGresult *begin_res = PQexec(conn, "BEGIN");
        if (PQresultStatus(begin_res) != PGRES_COMMAND_OK) {
            fprintf(stderr, "Error starting transaction: %s\n", PQresultErrorMessage(begin_res));
            if (message) snprintf(message, 256, "Error starting transaction: %s", PQresultErrorMessage(begin_res));
            PQclear(begin_res);
            return -1;
        }
        PQclear(begin_res);

        // Lock job
        char job_id_str[32];
        snprintf(job_id_str, sizeof(job_id_str), "%d", job_id);
        const char *param_values[1] = {job_id_str};
        int param_lengths[1] = {(int)strlen(job_id_str)};
        int param_formats[1] = {0};
        PGresult *res = PQexecParams(conn, lock_query, 1, NULL, param_values, param_lengths, param_formats, 0);
        ExecStatusType status = PQresultStatus(res);

        if (status == PGRES_TUPLES_OK && PQntuples(res) > 0) {
            // Update job
            PGresult *update_res = PQexecParams(conn, update_query, 1, NULL, param_values, param_lengths, param_formats, 0);
            status = PQresultStatus(update_res);

            if (status == PGRES_TUPLES_OK && PQntuples(update_res) > 0) {
            
            

                // Commit transaction
                PGresult *commit_res = PQexec(conn, "COMMIT");
                if (PQresultStatus(commit_res) != PGRES_COMMAND_OK) {
                    fprintf(stderr, "Error committing transaction: %s\n", PQresultErrorMessage(commit_res));
                    if (message) snprintf(message, 256, "Error committing transaction: %s", PQresultErrorMessage(commit_res));
                    PQclear(commit_res);
                    PQclear(update_res);
                    PQclear(res);
                    
                    return -1;
                }
                PQclear(commit_res);
                PQclear(update_res);
                PQclear(res);
                return 0;
            } else {
                // Rollback transaction
                PGresult *rollback_res = PQexec(conn, "ROLLBACK");
                PQclear(rollback_res);
                PQclear(update_res);
                PQclear(res);
                fprintf(stderr, "Failed to mark job %d as completed\n", job_id);
                if (message) snprintf(message, 256, "Failed to mark job %d as completed", job_id);
                return -1;
            }
        } else if (status == PGRES_TUPLES_OK) {
            // Rollback transaction
            PGresult *rollback_res = PQexec(conn, "ROLLBACK");
            PQclear(rollback_res);
            PQclear(res);
            fprintf(stderr, "No job found with id %d\n", job_id);
            if (message) snprintf(message, 256, "No job found with id %d", job_id);
            return -1;
        } else if (strstr(PQresultErrorMessage(res), "lock not available")) {
            // Rollback transaction
            PGresult *rollback_res = PQexec(conn, "ROLLBACK");
            PQclear(rollback_res);
            PQclear(res);
            attempt++;
            if (attempt < max_retries) usleep((unsigned int)(retry_delay * 1000000));
            continue;
        } else {
            // Rollback transaction
            PGresult *rollback_res = PQexec(conn, "ROLLBACK");
            PQclear(rollback_res);
            fprintf(stderr, "Error marking job %d as completed: %s\n", job_id, PQresultErrorMessage(res));
            if (message) snprintf(message, 256, "Error marking job %d as completed: %s", job_id, PQresultErrorMessage(res));
            PQclear(res);
            return -1;
        }
    }

    fprintf(stderr, "Could not lock job id %d after %d attempts\n", job_id, max_retries);
    if (message) snprintf(message, 256, "Could not lock job id %d after %d attempts", job_id, max_retries);
    return -1;
}

// push_job_data: Update an available job slot with new data

int push_job_data(void *connection, const char *base_table, const char *path, const char *data, int max_retries, double retry_delay, char *message) {
    PGconn *conn = (PGconn *)connection;
    if (!conn) {
        fprintf(stderr, "Invalid JobQueueContext or database connection\n");
        if (message) snprintf(message, 256, "Invalid JobQueueContext or database connection");
        return -1;
    }

    if (!path || strlen(path) == 0) {
        fprintf(stderr, "Path cannot be empty or NULL\n");
        if (message) snprintf(message, 256, "Path cannot be empty or NULL");
        return -1;
    }
    if (!data || strlen(data) == 0) {
        fprintf(stderr, "Data cannot be empty or NULL\n");
        if (message) snprintf(message, 256, "Data cannot be empty or NULL");
        return -1;
    }

    char select_query[512];
    snprintf(select_query, sizeof(select_query),
        "SELECT id FROM %s "
        "WHERE path = $1 AND valid = FALSE "
        "ORDER BY completed_at ASC "
        "FOR UPDATE NOWAIT "
        "LIMIT 1", base_table);

    char update_query[512];
    snprintf(update_query, sizeof(update_query),
        "UPDATE %s "
        "SET data = $1, schedule_at = timezone('UTC', NOW()), "
        "started_at = NULL, completed_at = NULL, "
        "valid = TRUE, is_active = FALSE "
        "WHERE id = $2 "
        "RETURNING id", base_table);

    int attempt = 0;
    while (attempt < max_retries) {
        // Start transaction
        PGresult *begin_res = PQexec(conn, "BEGIN");
        if (PQresultStatus(begin_res) != PGRES_COMMAND_OK) {
            fprintf(stderr, "Error starting transaction: %s\n", PQresultErrorMessage(begin_res));
            if (message) snprintf(message, 256, "Error starting transaction: %s", PQresultErrorMessage(begin_res));
            PQclear(begin_res);
            return -1;
        }
        PQclear(begin_res);

        // Select job
        const char *select_params[1] = {path};
        int select_lengths[1] = {(int)strlen(path)};
        int param_formats[2] = {0, 0};  // Sized for max params (update uses 2)
        PGresult *res = PQexecParams(conn, select_query, 1, NULL, select_params, select_lengths, param_formats, 0);
        ExecStatusType status = PQresultStatus(res);

        if (status == PGRES_TUPLES_OK && PQntuples(res) > 0) {
            // Update job
            const char *update_params[2] = {data, PQgetvalue(res, 0, 0)};
            int update_lengths[2] = {(int)strlen(data), (int)strlen(PQgetvalue(res, 0, 0))};
            PGresult *update_res = PQexecParams(conn, update_query, 2, NULL, update_params, update_lengths, param_formats, 0);
            status = PQresultStatus(update_res);

            if (status == PGRES_TUPLES_OK && PQntuples(update_res) > 0) {
                // Commit transaction
                PGresult *commit_res = PQexec(conn, "COMMIT");
                if (PQresultStatus(commit_res) != PGRES_COMMAND_OK) {
                    fprintf(stderr, "Error committing transaction: %s\n", PQresultErrorMessage(commit_res));
                    if (message) snprintf(message, 256, "Error committing transaction: %s", PQresultErrorMessage(commit_res));
                    PQclear(commit_res);
                    PQclear(update_res);
                    PQclear(res);
                    return -1;
                }
                PQclear(commit_res);
                PQclear(update_res);
                PQclear(res);
                return 0;
            } else {
                // Rollback transaction
                PGresult *rollback_res = PQexec(conn, "ROLLBACK");
                PQclear(rollback_res);
                PQclear(update_res);
                PQclear(res);
                fprintf(stderr, "Failed to update job slot for path '%s'\n", path);
                if (message) snprintf(message, 256, "Failed to update job slot for path '%s'", path);
                return -1;
            }
        } else if (status == PGRES_TUPLES_OK) {
            // Rollback transaction
            PGresult *rollback_res = PQexec(conn, "ROLLBACK");
            PQclear(rollback_res);
            PQclear(res);
            fprintf(stderr, "No available job slot for path '%s'\n", path);
            if (message) snprintf(message, 256, "No available job slot for path '%s'", path);
            return -1;
        } else if (strstr(PQresultErrorMessage(res), "could not obtain lock")) {
            // Rollback transaction
            PGresult *rollback_res = PQexec(conn, "ROLLBACK");
            PQclear(rollback_res);
            PQclear(res);
            attempt++;
            if (attempt < max_retries) usleep((unsigned int)(retry_delay * 1000000));
            continue;
        } else {
            // Rollback transaction
            PGresult *rollback_res = PQexec(conn, "ROLLBACK");
            PQclear(rollback_res);
            fprintf(stderr, "Error pushing job data for path '%s': %s\n", path, PQresultErrorMessage(res));
            if (message) snprintf(message, 256, "Error pushing job data for path '%s': %s", path, PQresultErrorMessage(res));
            PQclear(res);
            return -1;
        }
    }

    fprintf(stderr, "Could not acquire lock for path '%s' after %d attempts\n", path, max_retries);
    if (message) snprintf(message, 256, "Could not acquire lock for path '%s' after %d attempts", path, max_retries);
    return -1;
}

// clear_job_queue: Clear all jobs for a given path
int clear_job_queue(void *connection, const char *base_table, const char *path, char *message) {
    PGconn *conn = (PGconn *)connection;
    if (!path || strlen(path) == 0) {
        fprintf(stderr, "Path cannot be empty or NULL\n");
        if (message) snprintf(message, 256, "Path cannot be empty or NULL");
        return -1;
    }

    // Lock table
    char lock_query[512];
    snprintf(lock_query, sizeof(lock_query), "LOCK TABLE %s IN EXCLUSIVE MODE", base_table);

    char update_query[512];
    snprintf(update_query, sizeof(update_query),
        "UPDATE %s "
        "SET schedule_at = NOW(), started_at = NOW(), completed_at = NOW(), "
        "is_active = FALSE, valid = FALSE, data = '{}' "
        "WHERE path = $1 "
        "RETURNING id", base_table);

    // Start transaction
    PGresult *begin_res = PQexec(conn, "BEGIN");
    if (PQresultStatus(begin_res) != PGRES_COMMAND_OK) {
        fprintf(stderr, "Error starting transaction: %s\n", PQresultErrorMessage(begin_res));
        if (message) snprintf(message, 256, "Error starting transaction: %s", PQresultErrorMessage(begin_res));
        PQclear(begin_res);
        return -1;
    }
    PQclear(begin_res);

    // Lock table
    PGresult *lock_res = PQexec(conn, lock_query);
    if (PQresultStatus(lock_res) != PGRES_COMMAND_OK) {
        fprintf(stderr, "Error locking table: %s\n", PQresultErrorMessage(lock_res));
        if (message) snprintf(message, 256, "Error locking table: %s", PQresultErrorMessage(lock_res));
        PQclear(lock_res);
        PGresult *rollback_res = PQexec(conn, "ROLLBACK");
        PQclear(rollback_res);
        return -1;
    }
    PQclear(lock_res);

    // Execute update
    const char *param_values[1] = {path};
    int param_lengths[1] = {(int)strlen(path)};
    int param_formats[1] = {0};
    PGresult *res = PQexecParams(conn, update_query, 1, NULL, param_values, param_lengths, param_formats, 0);
    ExecStatusType status = PQresultStatus(res);

    if (status == PGRES_TUPLES_OK) {
        // Commit transaction
        PGresult *commit_res = PQexec(conn, "COMMIT");
        if (PQresultStatus(commit_res) != PGRES_COMMAND_OK) {
            fprintf(stderr, "Error committing transaction: %s\n", PQresultErrorMessage(commit_res));
            if (message) snprintf(message, 256, "Error committing transaction: %s", PQresultErrorMessage(commit_res));
            PQclear(commit_res);
            PQclear(res);
            return -1;
        }
        PQclear(commit_res);
        PQclear(res);
        return 0;
    } else {
        // Rollback transaction
        PGresult *rollback_res = PQexec(conn, "ROLLBACK");
        PQclear(rollback_res);
        fprintf(stderr, "Error in clear_job_queue for path '%s': %s\n", path, PQresultErrorMessage(res));
        if (message) snprintf(message, 256, "Error in clear_job_queue for path '%s': %s", path, PQresultErrorMessage(res));
        PQclear(res);
        return -1;
    }
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

    const char *base_table = "knowledge_base_job";
    const char *queue_path = "kb1.header1_link.header1_name.KB_JOB_QUEUE.info1_job";
    clear_job_queue(conn, base_table, queue_path, NULL);
    
    int queued_number = 0;
    int success = get_queued_number(conn, base_table, queue_path, &queued_number, NULL);

    printf("queued_number: %d %d\n", queued_number, success);
    
    int free_number = 0;
    success = get_free_number(conn, base_table, queue_path, &free_number, NULL);
    printf("free_number: %d %d\n", free_number, success);
    JobInfo job_info;
    success = peak_job_data(conn, base_table, queue_path, 3, 1.0, &job_info, NULL);
    printf("success: %d\n", success);
    printf("job_info.found: %d\n", job_info.found);
    printf("job_info.id: %d\n", job_info.id);
    printf("job_info.data: %s\n", job_info.data);
    if (job_info.found == 1) {
        free(job_info.data);
    }

    const char *push_data = "{\"prop1\": \"val1\", \"prop2\": \"val2\"}";
    printf("push_data: %s\n", push_data);
    success = push_job_data(conn, base_table, queue_path, push_data, 3, 1.0, NULL);
    printf("success: %d\n", success);
    success = get_queued_number(conn, base_table, queue_path, &queued_number, NULL);
    printf("queued_number: %d %d\n", queued_number, success);
    
    success = get_free_number(conn, base_table, queue_path, &free_number, NULL);
    printf("free_number: %d %d\n", free_number, success);
    success = peak_job_data(conn, base_table, queue_path, 3, 1.0, &job_info, NULL);
    printf("success: %d\n", success);
    printf("job_info.found: %d\n", job_info.found);
    printf("job_info.id: %d\n", job_info.id);
    printf("job_info.data: %s\n", job_info.data);
    if (job_info.found == 1) {
        free(job_info.data);
    }
    
    success = get_free_number(conn, base_table, queue_path, &free_number, NULL);
    printf("free_number: %d %d\n", free_number, success);
    success = peak_job_data(conn, base_table, queue_path, 3, 1.0, &job_info, NULL);
    printf("success: %d\n", success);
    success = get_free_number(conn, base_table, queue_path, &free_number, NULL);
    printf("free_number: %d %d\n", free_number, success);
    
    
    success = mark_job_completed(conn, base_table, job_info.id, 3, 1.0, NULL);
    printf("success: %d\n", success);
    success = get_free_number(conn, base_table, queue_path, &free_number, NULL);
    printf("free_number: %d %d\n", free_number, success);
    
    return 0;
    
    
}









#endif