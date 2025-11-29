#ifndef CFL_EXCEPTION_SUPPORT_H
#define CFL_EXCEPTION_SUPPORT_H

#ifdef __cplusplus
extern "C" {
#endif


typedef enum {
    CFL_EXCEPTION_MAIN_LINK     = 0,
    CFL_EXCEPTION_RECOVERY_LINK = 1,
    CFL_EXCEPTION_FINALIZE_LINK = 2
} cfl_exception_stage_t;


typedef struct{
    void    *auxiliary_data;
    uint16_t parent_node_id;
    uint16_t logging_function_id;
    uint16_t original_node_id;
    uint16_t heartbeat_time_out;
    uint16_t exception_catch_links[CFL_EXCEPTION_FINALIZE_LINK + 1];
    cfl_exception_stage_t  exception_stage; 
    uint8_t  max_steps;
    uint8_t step_count;
    bool     heartbeat_enabled;
    
}cfl_exception_support_data_t;


   

  



#ifdef __cplusplus
}
#endif

#endif