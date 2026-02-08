#ifndef KB_RPC_CLIENT_TABLE_H
#define KB_RPC_CLIENT_TABLE_H




typedef struct {
    int id;
    char *request_id;
    char *client_path;
    char *server_path;
    char *transaction_tag;
    char *rpc_action;
    char *response_payload;
    char *response_timestamp;
    int is_new_result;
} RPCRow;

int find_free_slots(void *conn, const char *base_table, const char *client_path);
int find_queued_slots(void *conn, const char *base_table, const char *client_path);
int clear_reply_queue(void *conn, const char *base_table, const char *client_path, int max_retries, float retry_delay);
int push_and_claim_reply_data(void *conn, const char *base_table, const char *client_path, const char *request_uuid, 
    const char *server_path, const char *rpc_action, const char *transaction_tag, const char *reply_payload, int max_retries, float retry_delay);
RPCRow *peak_and_claim_reply_data(void *conn, const char *base_table, const char *client_path,
                                  int max_retries, float retry_delay);
void free_rpc_row(RPCRow *row);

#endif