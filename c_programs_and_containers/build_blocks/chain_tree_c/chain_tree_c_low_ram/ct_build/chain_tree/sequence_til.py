from operator import not_
from .column_flow import ColumnFlow

class SequenceTil(ColumnFlow):
    def __init__(self, ctb):
        ColumnFlow.__init__(self, ctb)
        self.ctb = ctb
        
        
    def define_sequence_start_node(self,column_name:str,main_function ="CFL_SEQUENCE_START_MAIN",
                                   initialization_function ="CFL_SEQUENCE_START_INIT",
                                   termination_function ="CFL_SEQUENCE_START_TERM",
                                   aux_function ="CFL_NULL",initialize_function="CFL_NULL",finalize_function="CFL_NULL",
                                   user_data:dict = {},auto_start = False):
        if not isinstance(initialize_function, str):
            raise TypeError("initialize_function must be a string")
        if not isinstance(finalize_function, str):
            raise TypeError("finalize_function must be a string")
        self.ctb.add_one_shot_function(initialize_function)
        self.ctb.add_one_shot_function(finalize_function)
        column_data = {}
        column_data["initialize_function"] = initialize_function
        column_data["finalize_function"] = finalize_function
        column_data["user_data"] = user_data
        self.sequence_active = True
        return_node = self.define_column(column_name,main_function,initialization_function,termination_function,
                                         aux_function,column_data,auto_start,label="SEQ_ST")
        
        return return_node
    
    def define_sequence_til_pass_node (self,column_name:str,main_function ="CFL_SEQUENCE_PASS_MAIN",
                                   initialization_function ="CFL_SEQUENCE_PASS_INIT",
                                   termination_function ="CFL_SEQUENCE_PASS_TERM",
                                   aux_function ="CFL_NULL",finalize_function="CFL_NULL",user_data:dict = {},auto_start = False):
        
        if not isinstance(finalize_function, str):
            raise TypeError("finalize_function must be a string")
        self.ctb.add_one_shot_function(finalize_function)
        column_data = {}
        column_data["finalize_function"] = finalize_function
        column_data["user_data"] = user_data
        
        return_node = self.define_column(column_name,main_function,initialization_function,termination_function,aux_function,
                                         column_data,auto_start,"SEQ_PASS")
        self.sequence_dict[return_node] = True
        return return_node
    
    def define_sequence_til_fail_node (self,column_name:str,main_function =f"CFL_SEQUENCE_FAIL_MAIN",
                                   initialization_function ="CFL_SEQUENCE_FAIL_INIT",
                                   termination_function ="CFL_SEQUENCE_FAIL_TERM",
                                   aux_function ="CFL_NULL",finalize_function="CFL_NULL",user_data:dict = {},auto_start = False):
        if not isinstance(finalize_function, str):
            raise TypeError("finalize_function must be a string")
        self.ctb.add_one_shot_function(finalize_function)
        column_data = {}
        column_data["finalize_function"] = finalize_function
        column_data["user_data"] = user_data
        return_node = self.define_column(column_name,main_function,initialization_function,termination_function,
                                        aux_function,column_data,auto_start,label="SEQ_FAIL")
        self.sequence_dict[return_node] = True
        return return_node
    
    
    def define_supervisor_node(self, column_name:str,  main_function ="CFL_SUPERVISOR_MAIN",
                              initialization_function ="CFL_SUPERVISOR_INIT", termination_function ="CFL_SUPERVISOR_TERM", 
                              aux_function ="CFL_NULL",user_data:dict = None, restart_enabled:bool=True,termination_type:int = 0,
                              reset_limited_enabled:bool=False,max_reset_number:int=1,reset_window:int=10,auto_start = False,
                              finalize_function:str="CFL_NULL",finalize_function_data:dict={},label:str="SUP"):
        if not isinstance(column_name, str):
            raise TypeError("Column name must be a string")
        if not isinstance(main_function, str):
            raise TypeError("Main function must be a string")
        if not isinstance(initialization_function, str):
            raise TypeError("Initialization function must be a string")
        if not isinstance(termination_function, str):
            raise TypeError("Termination function must be a string")
        if not isinstance(aux_function, str):
            raise TypeError("Aux function must be a string")
        if not isinstance(termination_type, int):
            raise TypeError("Termination type must be a integer")
        if not isinstance(reset_limited_enabled, bool):
            raise TypeError("Reset limited enabled must be a boolean")
        if not isinstance(max_reset_number, int):
            raise TypeError("Max reset number must be an integer")
        if not isinstance(reset_window, int):
            raise TypeError("Reset window must be an integer")
        if not isinstance(restart_enabled, bool):
            raise TypeError("Restart enabled must be a boolean")
        supervisor_data = {
                        "termination_type": termination_type, "restart_enabled": restart_enabled,
                       "reset_limited_enabled": reset_limited_enabled, "max_reset_number": max_reset_number, "reset_window": reset_window,
                       "finalize_function": finalize_function, "finalize_function_data": finalize_function_data}
        column_data = {"user_data": user_data,"supervisor_data": supervisor_data}
        self.ctb.add_one_shot_function(finalize_function)
        return self.define_column(column_name,main_function,initialization_function,termination_function,
                                  aux_function,column_data,auto_start,label)
                                        
    
    def define_supervisor_one_for_one_node(self, column_name:str,  aux_function ="CFL_NULL",
                                           user_data:dict = {},restart_enabled:bool=True,reset_limited_enabled:bool=False,
                                           max_reset_number:int=1,reset_window:int=10,auto_start = False,
                                           finalize_function:str="CFL_NULL",finalize_function_data:dict={}):
        
        return self.define_supervisor_node(column_name = column_name, aux_function= aux_function, user_data =user_data,
                                           termination_type=0, restart_enabled=restart_enabled,
                                           reset_limited_enabled=reset_limited_enabled,
                                           max_reset_number=max_reset_number, reset_window=reset_window, auto_start=auto_start,
                                           finalize_function=finalize_function,
                                           finalize_function_data=finalize_function_data,label="SUP_1_1")
    
    def define_supervisor_one_for_all_node(self, column_name:str,    aux_function ="CFL_NULL", user_data:dict = {}
                                           ,restart_enabled:bool=True,reset_limited_enabled:bool=False,max_reset_number:int=1,
                                           reset_window:int=10, auto_start = False,
                                           finalize_function:str="CFL_NULL",
                                           finalize_function_data:dict={}): 
        
         return self.define_supervisor_node(column_name = column_name, aux_function= aux_function, user_data =user_data,
                                            termination_type=1, restart_enabled=restart_enabled,
                                           reset_limited_enabled=reset_limited_enabled,
                                           max_reset_number=max_reset_number, reset_window=reset_window, auto_start=auto_start,
                                           finalize_function=finalize_function,
                                           finalize_function_data=finalize_function_data,label="SUP_1_ALL")
    
    def define_supervisor_rest_for_all_node(self, column_name:str,  aux_function ="CFL_NULL", user_data:dict = {},
                                            restart_enabled:bool=True,
                                            reset_limited_enabled:bool=False,max_reset_number:int=1,reset_window:int=10,auto_start = False,
                                            finalize_function:str="CFL_NULL",finalize_function_data:dict={}):
                                                
        return self.define_supervisor_node(column_name = column_name, aux_function= aux_function, user_data =user_data,
                                           termination_type=2, restart_enabled=restart_enabled,
                                           reset_limited_enabled=reset_limited_enabled,
                                           max_reset_number=max_reset_number, reset_window=reset_window, auto_start=auto_start,
                                           finalize_function=finalize_function,
                                           finalize_function_data=finalize_function_data,label="SUP_REST_ALL")
        
        
    def define_mark_supervisor_node_failure(self,data:dict):
        if not isinstance(data, dict):
            raise TypeError("Data must be a dictionary")
        return self.define_column_link(main_function_name="CFL_DISABLE",initialization_function_name="CFL_MARK_SUPERVISOR_NODE_FAILURE_INIT",
                                       aux_function_name="CFL_NULL",termination_function_name="CFL_NULL",node_data=data)
    
    def end_sequence_node(self,column_name:str):
        if column_name not in self.sequence_dict:
            raise ValueError("Sequence is not active")
        del self.sequence_dict[column_name] 
        self.end_column(column_name)
        self.join_sequence_element(column_name)
    
    def mark_sequence_true_link(self,parent_node_name:str,data:dict = {}):
        result = True
        node_data = {"parent_node_name":parent_node_name,"result":1,"data":data}
        self.define_column_link(main_function_name="CFL_DISABLE",initialization_function_name="CFL_MARK_SEQUENCE",
                                aux_function_name="CFL_NULL",termination_function_name="CFL_NULL",node_data=node_data)

    
    def mark_sequence_false_link(self,parent_node_name:str,data:dict = {}):
        result = False
        node_data = {"parent_node_name":parent_node_name,"result":0,"data":data}
        self.define_column_link(main_function_name="CFL_DISABLE",initialization_function_name="CFL_MARK_SEQUENCE",
                                aux_function_name="CFL_NULL",termination_function_name="CFL_NULL",node_data=node_data)
    def join_sequence_element(self,parent_node_name:str):
        return self.define_column_link(main_function_name="CFL_JOIN_SEQUENCE_ELEMENT",
                                       aux_function_name="CFL_NULL",
                                       initialization_function_name="CFL_JOIN_SEQUENCE_ELEMENT_INIT",
                                       termination_function_name="CFL_JOIN_SEQUENCE_ELEMENT_TERM",
                                       node_data={"parent_node_name":parent_node_name})

    
    # aux_function_name is used to override default behavior
   