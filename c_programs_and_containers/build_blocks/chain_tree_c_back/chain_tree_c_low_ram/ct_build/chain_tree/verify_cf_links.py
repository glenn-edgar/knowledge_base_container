from .column_flow import ColumnFlow

class VerifyCfLinks(ColumnFlow):
    
    def __init__(self,ctb):
        self.ctb = ctb
        ColumnFlow.__init__(self,ctb)
        
        
   
    def asm_verify(self,verify_fn ,fn_data=None, reset_flag = False, error_fn = "CFL_NULL", error_data = None ):
        
        
        
        
        element_data = {}
        element_data["fn_data"] = fn_data
        element_data["reset_flag"] = reset_flag    
        element_data["error_function"] = error_fn
        if error_fn != None:
            self.ctb.add_one_shot_function(error_fn)
            element_data["error_data"] = error_data
        else:
            element_data["error_function"] = None
            element_data["error_data"] = None
    
        
       
        return self.define_column_link(main_function_name="CFL_VERIFY",
                            aux_function_name=verify_fn,
                            initialization_function_name="CFL_VERIFY_INIT",
                            termination_function_name="CFL_VERIFY_TERM",
                            node_data=element_data)
        
        
        
    def asm_verify_timeout(self,time_out,reset_flag = False,error_fn = "CFL_NULL",error_data = None):
        fn_data = {}
        fn_data["time_out"] = time_out
        fn_data["current_time"] = 0
        
        return self.asm_verify("CFL_VERIFY_TIME_OUT",fn_data, reset_flag, error_fn, error_data )
        
       
    def asm_verify_bitmask(self,required_bitmask_list,excluded_bitmask_list,reset_flag = False,error_fn = "CFL_NULL",error_data = None):
        bit_position_list = []
        for event in required_bitmask_list:
            bit_position = self.ctb.register_bitmask(event)
            bit_position_list.append(bit_position)
        bit_mask = 0
        for bit_position in bit_position_list:
            bit_mask |= 1 << bit_position
        required_bitmask = bit_mask
        excluded_bit_mask = 0
        for event in excluded_bitmask_list:
            bit_position = self.ctb.register_bitmask(event)
            bit_position_list.append(bit_position)
            excluded_bit_mask |= 1 << bit_position
        fn_data = {"required_bitmask": required_bitmask, "excluded_bitmask": excluded_bit_mask}
        self.ctb.add_one_shot_function(error_fn)
        return self.asm_verify("CFL_VERIFY_BITMASK",fn_data, reset_flag, error_fn, error_data )
    

    def asm_verify_tests_active(self,test_ids :list, reset_flag = False,
                           error_fn = "CFL_NULL",error_data = None):
        if not isinstance(test_ids, list):
            raise TypeError("Test ids must be a list")
        element_data = {}
        element_data["test_ids"] = test_ids
        element_data["reset_flag"] = reset_flag
        element_data["error_function"] = error_fn
        element_data["error_data"] = error_data
        element_data["error_function"] = error_fn
        self.ctb.add_one_shot_function(error_fn)
        return self.asm_verify("CFL_VERIFY_TESTS_ACTIVE",element_data, reset_flag, error_fn, error_data )