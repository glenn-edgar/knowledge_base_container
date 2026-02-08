#ifndef KB_RPC_SERVER_TABLE_H
#define KB_RPC_SERVER_TABLE_H

#ifdef __cplusplus
extern "C" {
#endif



typedef struct {
    int id;
    char *server_path;
    char *request_id;
    char *rpc_action;
    char *request_payload;
    char *request_timestamp;
    char *transaction_tag;
    char *state;
    int priority;
    char *processing_timestamp;
    char *completed_timestamp;
    char *rpc_client_queue;
} ServerRow;


int count_jobs_job_types(void *conn, const char *base_table, const char *server_path, const char *state);
int count_processing_jobs(void *conn, const char *base_table, const char *server_path);

int count_new_jobs(void *conn, const char *base_table, const char *server_path);
int count_empty_jobs(void *conn, const char *base_table, const char *server_path);

ServerRow *push_rpc_server_queue(void *conn, const char *base_table, const char *server_path,
    const char *request_id, const char *rpc_action, const char *request_payload_json, const char *transaction_tag,
    int priority, const char *rpc_client_queue, int max_retries, float wait_time);
ServerRow *peak_server_queue(void *conn, const char *base_table, const char *server_path, int retries, float wait_time);
int mark_job_completion(void *conn, const char *base_table, const char *server_path, int id, int retries, float wait_time);
int clear_server_queue(void *conn, const char *base_table, const char *server_path, int max_retries, float retry_delay);
void free_server_row(ServerRow *row);

#ifdef __cplusplus
}
#endif

#endif


