from .column_flow import ColumnFlow



class WaitCfLinks(ColumnFlow):
    
    def __init__(self, ctb):
        
        self.ctb = ctb
        ColumnFlow.__init__(self,ctb)
        

  
    def asm_wait(self,  wait_fn ,wait_fn_data, reset_flag = False, timeout=0,time_out_event="CF_TIMER_EVENT",
                 error_fn =None,error_data = None):
        element_data = {}
        element_data["wait_fn_data"] = wait_fn_data
        element_data["reset_flag"] = reset_flag
        element_data["timeout"] = timeout
        element_data["time_out_event"] = self.ctb.register_event(time_out_event)
        element_data["error_function"] = error_fn
        if error_data == None:
            error_data = {}
        element_data["error_data"] = error_data
        
        if error_fn is not None:
            self.ctb.add_one_shot_function(error_fn)
        
        
        return self.define_column_link(main_function_name="CFL_WAIT",
                            aux_function_name=wait_fn,
                            initialization_function_name="CFL_WAIT_INIT",
                            termination_function_name="CFL_WAIT_TERM",
                            node_data=element_data)
       
        
    
   

    def asm_wait_for_event(self,event_id,event_count = 1,reset_flag = False,timeout=0,
                           error_fn = "CFL_NULL",time_out_event ="CF_TIMER_EVENT",error_data = {}):
        element_data = {}
        element_data["event_id"] = self.ctb.register_event(event_id)
        element_data["event_count"] = event_count
        return self.asm_wait("CFL_WAIT_FOR_EVENT",element_data,reset_flag,timeout,time_out_event,error_fn,error_data)
    
 
    
    def asm_wait_time(self,time_delay):
        element_data = {}
        element_data["time_delay"] = time_delay #time delay in seconds
    
        return self.define_column_link(main_function_name="CFL_WAIT_TIME",
                
                            aux_function_name="CFL_NULL",
                            initialization_function_name="CFL_WAIT_TIME_INIT",
                            termination_function_name="CFL_NULL",
                            node_data=element_data)
 
    def asm_wait_for_bitmask(self,bitmask_event_list,reset_flag = False,timeout=None,
                           error_fn = "CFL_NULL",time_out_event ="CF_TIMER_EVENT",error_data = None):
        element_data = {}
        if not isinstance(bitmask_event_list, list):
            raise TypeError("Event list must be a list")
        bit_position_list = []
        for event in bitmask_event_list:
            bit_position = self.ctb.register_bitmask(event)
            bit_position_list.append(bit_position)
        bit_mask = 0
        for bit_position in bit_position_list:
            bit_mask |= 1 << bit_position
        bitmask_data = {"bit_mask": bit_mask}
        element_data["bitmask_data"] = bitmask_data
        return self.asm_wait("CFL_WAIT_FOR_BITMASK",element_data,reset_flag,timeout,time_out_event,error_fn,error_data)
    
    
   
    
    
    def asm_wait_for_tests_complete(self,test_ids :list, reset_flag = False,timeout=30,
                           error_fn = "CFL_NULL",time_out_event ="CF_TIMER_EVENT",error_data = None):
        if not isinstance(test_ids, list):
            raise TypeError("Test ids must be a list")
        element_data = {}
        element_data["test_ids"] = test_ids
        element_data["reset_flag"] = reset_flag
        element_data["error_function"] = error_fn
        element_data["error_data"] = error_data
        return self.asm_wait("CFL_WAIT_FOR_TESTS_COMPLETE",element_data,reset_flag,timeout,time_out_event,error_fn,error_data)