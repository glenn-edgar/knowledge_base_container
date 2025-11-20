#ifndef CFL_COMMON_FUNCTION_HEADERS_H
#define CFL_COMMON_FUNCTION_HEADERS_H

#ifdef __cplusplus
extern "C" {
#endif


typedef struct {
    double wait_time_out;
} cfl_wait_time_out_data_t;


typedef struct{
    char *error_message;
    uint32_t timeout;
    uint32_t time_out_event;
    uint32_t error_function;
    bool reset_flag;
    uint32_t event_count;
    void *auxiliary_data;
}cfl_wait_fn_data_t;



typedef struct{
  bool reset_flag;
  uint32_t error_function;
  char *failure_data;
  void *auxiliary_data;
}cfl_verify_fn_data_t;

#ifdef __cplusplus
}
#endif

#endif