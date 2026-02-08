#include "kb_all.h"
#include <libpq-fe.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <uuid/uuid.h>
#include <stdbool.h>
#include "postgres_setup.h"

char error_msg[512];


void individual_status_table(void  *conn, const char *base_table, const char *kb, const char *node_name, 
       const char **prop_keys, const char **prop_values, int num_props, const char *node_path){
    KBQuery *q = kb_query_new(base_table);
    if (!q) {
        fprintf(stderr, "Failed to create KBQuery\n");
        return;
    }

    int num;
    KBRow *rows = find_status_node_ids(conn,base_table,kb,node_name,prop_keys,prop_values,num_props,node_path,&num);
    if (!rows) {
        fprintf(stderr, "No nodes found matching path parameters\n");
        kb_query_free(q);
        return;
    }
    printf("number of rows: %d\n", num);
    for (int i = 0; i < num; i++) {
        printf("id: %d\n", rows[i].id);
        printf("knowledge_base: %s\n", rows[i].knowledge_base);
        printf("label: %s\n", rows[i].label);
        printf("name: %s\n", rows[i].name);
        printf("properties: %s\n", rows[i].properties);
        printf("data: %s\n", rows[i].data);
        printf("has_link: %d\n", rows[i].has_link);
        printf("has_link_mount: %d\n", rows[i].has_link_mount);
        printf("path: %s\n", rows[i].path);
    }
    kb_rows_free(rows, num);

    
    kb_query_free(q);
}

void find_status_tables(void  *conn, const char *base_table){
  
    printf("-------------------------------- find_status_tables\n");
    printf("-------------------------------- wide open test find all status tables\n");
    individual_status_table(conn, base_table, NULL, NULL, NULL, NULL, 0, NULL);
    printf("-------------------------------- search by kb\n");
    individual_status_table(conn, base_table, "kb1", NULL, NULL, NULL, 0, NULL);
    printf("-------------------------------- search by name\n");
    individual_status_table(conn, base_table, NULL, "info3_status", NULL, NULL, 0, NULL);
    printf("-------------------------------- search by path\n");
    individual_status_table(conn, base_table, NULL, NULL, NULL, NULL, 0, "kb1.header1_link.header1_name.KB_STATUS_FIELD.info2_status");
    printf("-------------------------------- search by property keys and values\n");
    individual_status_table(conn, base_table, NULL, NULL,(const char *[]){"prop3","description"}, (const char *[]){"val3","info3_status_description"}, 2,NULL);

}

void find_stream_tables(void  *conn, const char *base_table){
    printf("-------------------------------- find_stream_tables\n");
    printf("-------------------------------- wide open test find all stream tables\n");
    KBQuery *q = kb_query_new(base_table);
    if (!q) {
        fprintf(stderr, "Failed to create KBQuery\n");
        return;
    }

    int num;
    KBRow *rows = find_stream_ids(conn,base_table,NULL,NULL,NULL,NULL,0,NULL,&num);
    if (!rows) {
        fprintf(stderr, "No nodes found matching path parameters\n");
        kb_query_free(q);
        return;
    }
    printf("number of rows: %d\n", num);
    for (int i = 0; i < num; i++) {
        printf("id: %d\n", rows[i].id);
        printf("knowledge_base: %s\n", rows[i].knowledge_base);
        printf("label: %s\n", rows[i].label);
        printf("name: %s\n", rows[i].name);
        printf("properties: %s\n", rows[i].properties);
        printf("data: %s\n", rows[i].data);
        printf("has_link: %d\n", rows[i].has_link);
        printf("has_link_mount: %d\n", rows[i].has_link_mount);
        printf("path: %s\n", rows[i].path);
    }
    kb_rows_free(rows, num);

    
    kb_query_free(q);
}
void find_job_tables(void  *conn, const char *base_table){
    printf("-------------------------------- find_job_tables\n");
    printf("-------------------------------- wide open test find all job tables\n");
    KBQuery *q = kb_query_new(base_table);
    if (!q) {
        fprintf(stderr, "Failed to create KBQuery\n");
        return;
    }

    int num;
    KBRow *rows = find_job_ids(conn,base_table,NULL,NULL,NULL,NULL,0,NULL,&num);
    if (!rows) {
        fprintf(stderr, "No nodes found matching path parameters\n");
        kb_query_free(q);
        return;
    }
    printf("number of rows: %d\n", num);
    for (int i = 0; i < num; i++) {
        printf("id: %d\n", rows[i].id);
        printf("knowledge_base: %s\n", rows[i].knowledge_base);
        printf("label: %s\n", rows[i].label);
        printf("name: %s\n", rows[i].name);
        printf("properties: %s\n", rows[i].properties);
        printf("data: %s\n", rows[i].data);
        printf("has_link: %d\n", rows[i].has_link);
        printf("has_link_mount: %d\n", rows[i].has_link_mount);
        printf("path: %s\n", rows[i].path);
    }
    kb_rows_free(rows, num);

    
    kb_query_free(q);
}
void find_rpc_server_tables(void  *conn, const char *base_table){
    printf("-------------------------------- find_rpc_server_tables\n");
    printf("-------------------------------- wide open test find all rpc server tables\n");
    KBQuery *q = kb_query_new(base_table);
    if (!q) {
        fprintf(stderr, "Failed to create KBQuery\n");
        return;
    }

    int num;
    KBRow *rows = find_rpc_server_ids(conn,base_table,NULL,NULL,NULL,NULL,0,NULL,&num);
    if (!rows) {
        fprintf(stderr, "No nodes found matching path parameters\n");
        kb_query_free(q);
        return;
    }
    printf("number of rows: %d\n", num);
    for (int i = 0; i < num; i++) {
        printf("id: %d\n", rows[i].id);
        printf("knowledge_base: %s\n", rows[i].knowledge_base);
        printf("label: %s\n", rows[i].label);
        printf("name: %s\n", rows[i].name);
        printf("properties: %s\n", rows[i].properties);
        printf("data: %s\n", rows[i].data);
        printf("has_link: %d\n", rows[i].has_link);
        printf("has_link_mount: %d\n", rows[i].has_link_mount);
        printf("path: %s\n", rows[i].path);
    }
    kb_rows_free(rows, num);

    
    kb_query_free(q);
}
void find_rpc_client_tables(void  *conn, const char *base_table){
    printf("-------------------------------- find_rpc_client_tables\n");
    printf("-------------------------------- wide open test find all rpc client tables\n");
    KBQuery *q = kb_query_new(base_table);
    if (!q) {
        fprintf(stderr, "Failed to create KBQuery\n");
        return;
    }

    int num;
    KBRow *rows = find_rpc_client_ids(conn,base_table,NULL,NULL,NULL,NULL,0,NULL,&num);
    if (!rows) {
        fprintf(stderr, "No nodes found matching path parameters\n");
        kb_query_free(q);
        return;
    }
    printf("number of rows: %d\n", num);
    for (int i = 0; i < num; i++) {
        printf("id: %d\n", rows[i].id);
        printf("knowledge_base: %s\n", rows[i].knowledge_base);
        printf("label: %s\n", rows[i].label);
        printf("name: %s\n", rows[i].name);
        printf("properties: %s\n", rows[i].properties);
        printf("data: %s\n", rows[i].data);
        printf("has_link: %d\n", rows[i].has_link);
        printf("has_link_mount: %d\n", rows[i].has_link_mount);
        printf("path: %s\n", rows[i].path);
    }
    kb_rows_free(rows, num);

    
    kb_query_free(q);
}

void test_job_table(void  *conn, const char *base_table, const char *queue_path){

    
    printf("-------------------------------- test_job_table\n");
    printf("-------------------------------- \n");
    KBQuery *q = kb_query_new(base_table);
    if (!q) {
        fprintf(stderr, "Failed to create KBQuery\n");
        return;

    }
   
    
    clear_job_queue(conn, base_table, queue_path, NULL);
        
    int queued_number = 0;
    int success = get_queued_number(conn, base_table, queue_path, &queued_number, NULL);

    printf("queued_number: %d %d\n", queued_number, success);
    
    int free_number = 0;
    success = get_free_number(conn, base_table, queue_path, &free_number, NULL);
    printf("free_number: %d %d\n", free_number, success);
    JobInfo job_info;
    success = peak_job_data(conn, base_table, queue_path, 3, 1.0, &job_info, NULL);
    if(success != 0){
        printf("peak_job_data failed\n");
        return;
    }
    if(job_info.found == 1){
        printf("success: %d\n", success);
        printf("job_info.found: %d\n", job_info.found);
        printf("job_info.id: %d\n", job_info.id);
        printf("job_info.data: %s\n", job_info.data);
        free(job_info.data);
    }else {
        printf("no job found\n");
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
    
    
    kb_query_free(q);
}

void test_status_table(void  *conn, const char *base_table, const char *status_path){
    
    printf("-------------------------------- test_status_table\n");
    printf("-------------------------------- \n");
    KBQuery *q = kb_query_new(base_table);
    if (!q) {
        fprintf(stderr, "Failed to create KBQuery\n");
        return;
    }



    char *data_str;
    get_status_data(conn, base_table, status_path, &data_str);
    printf("Data: %s\n", data_str);
    free(data_str);

    char *data_write_1 = "{\"prop1\":\"value1\",\"prop2\":\"value2\",\"prop3\":\"value3\"}";
    int success;
    char message[512];
    set_status_data(conn, base_table, status_path, data_write_1, 3, 1.0, &success, message);
    printf("Success: %d\n", success);
    printf("Message: %s\n", message);
    get_status_data(conn, base_table, status_path, &data_str);
    printf("Data: %s\n", data_str);
    free(data_str);
    char *data_write_2 = "{\"prop1\":\"value1\",\"prop2\":\"value2\"}";
    set_status_data(conn, base_table, "kb1.header1_link.header1_name.KB_STATUS_FIELD.info2_status", data_write_2, 3, 1.0, &success, message);
    printf("Success: %d\n", success);
    printf("Message: %s\n", message);
    get_status_data(conn, base_table, status_path, &data_str);
    printf("Data: %s\n", data_str);
    free(data_str);
    
}

void test_stream_table(void  *conn, const char *base_table, const char *stream_path){
    printf("-------------------------------- test_stream_table\n");
    printf("-------------------------------- push stream data\n");
    KBQuery *q = kb_query_new(base_table);
    if (!q) {
        fprintf(stderr, "Failed to create KBQuery\n");
        return;
    }
   
    int success = push_stream_data(conn, base_table,stream_path, 
        "{\"prop1\":\"value1\",\"prop2\":\"value2\",\"prop3\":\"value3\"}", 3, 1.0, NULL);
    printf("success: %d\n", success);
    if (success != 0){
        printf("error: %s\n", error_msg);
    }
    

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



void test_rpc_client_table(void  *conn, const char *base_table ){
    printf("-------------------------------- test_rpc_client_table\n");
    printf("-------------------------------- \n");
    KBQuery *q = kb_query_new(base_table);
    RPCRow *row = NULL;
    if (!q) {
        fprintf(stderr, "Failed to create KBQuery\n");
        return;
    }

    

    char *client_path = "kb1.header1_link.header1_name.KB_RPC_CLIENT_FIELD.info1_client";
    //char *request_uuid = generate_request_uuid();
    char *request_uuid = NULL;
    char *server_path = "kb1.header1_link.header1_name.KB_RPC_SERVER_FIELD.info1_server";
    char *rpc_action = "response_reply";
    char *transaction_tag = "1234567890";
    char *reply_payload = "{\"prop1\":\"value1\",\"prop2\":\"value2\",\"prop3\":\"value3\"}";


    

    int free_slots = find_free_slots(conn, base_table, client_path);
    printf("free_slots: %d\n", free_slots);
    int queued_slots = find_queued_slots(conn, base_table, client_path);
    printf("queued_slots: %d\n", queued_slots);
    
    int updated_records  = clear_reply_queue(conn, base_table, client_path, 3, 1.0);
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
    free(request_uuid);
    kb_query_free(q);

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

void test_rpc_server_table(void  *conn, const char *base_table ){
    printf("-------------------------------- test_rpc_server_table\n");
    printf("-------------------------------- \n");
    KBQuery *q = kb_query_new(base_table);
    if (!q) {
        fprintf(stderr, "Failed to create KBQuery\n");
        return;
    }
    ServerRow *row = NULL;

    
    char *client_path = "kb1.header1_link.header1_name.KB_RPC_CLIENT_FIELD.info1_client";
    char *server_path = "kb1.header1_link.header1_name.KB_RPC_SERVER_FIELD.info1_server";
    int priority = 1;
    char *server_payload_json = "{\"prop1\":\"value1\",\"prop2\":\"value2\",\"prop3\":\"value3\"}";

    
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
        return;
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
        
    }   
    printf("mark_job_completion success\n");

    new_jobs = count_new_jobs(conn, base_table, server_path);
    empty_jobs = count_empty_jobs(conn, base_table, server_path);
    processing_jobs = count_processing_jobs(conn, base_table, server_path);
    printf("new_jobs: %d\n", new_jobs);
    printf("empty_jobs: %d\n", empty_jobs);
    printf("processing_jobs: %d\n", processing_jobs);
    kb_query_free(q);
}

int main(void){
    char password[256];
    printf("Enter password: "); 
    fgets(password, sizeof(password), stdin); 

    void  *conn = create_pg_connection("knowledge_base", "gedgar", password, "localhost", "5432");
    if (!conn) {
        fprintf(stderr, "Failed to create PostgreSQL connection\n");
        return 1;
    }
    find_status_tables(conn, "knowledge_base");
    find_stream_tables(conn, "knowledge_base");
    find_job_tables(conn, "knowledge_base");
    find_rpc_server_tables(conn, "knowledge_base");
    find_rpc_client_tables(conn, "knowledge_base");
    test_job_table(conn, "knowledge_base_job", "kb1.header1_link.header1_name.KB_JOB_QUEUE.info1_job");
    test_status_table(conn, "knowledge_base_status", "kb1.header1_link.header1_name.KB_STATUS_FIELD.info2_status");
    test_stream_table(conn, "knowledge_base_stream", "kb1.header1_link.header1_name.KB_STREAM_FIELD.info1_stream");
    test_rpc_client_table(conn, "knowledge_base_rpc_client");
    test_rpc_server_table(conn, "knowledge_base_rpc_server");
    return 0;
}