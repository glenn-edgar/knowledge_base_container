#ifndef KB_JOB_TABLE_H
#define KB_JOB_TABLE_H

#ifdef __cplusplus
extern "C" {
#endif


#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>




typedef struct {
    int found;
    int id;
    char *data;
} JobInfo;





int clear_job_queue(void *connection, const char *base_table, const char *path,  char *message);
int push_job_data(void *connection, const char *base_table, const char *path, const char *data, int max_retries, double retry_delay,  char *message);
int mark_job_completed(void *connection, const char *base_table, int job_id, int max_retries, double retry_delay,  char *message);
int peak_job_data(void *connection, const char *base_table, const char *path, int max_retries, double retry_delay, JobInfo *job_info, char *message);
int get_free_number(void *connection, const char *base_table, const char *path, int *count, char *message);
int get_queued_number(void *connection, const char *base_table, const char *path, int *count, char *message);

#ifdef __cplusplus
}
#endif

#endif