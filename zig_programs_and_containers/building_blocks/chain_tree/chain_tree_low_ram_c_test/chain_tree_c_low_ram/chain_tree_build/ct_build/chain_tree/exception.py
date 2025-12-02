

class ExceptionHandler():
    def __init__(self, ctb):
        self.ctb = ctb
        self.main_flag = False
        self.recovery_flag = False
        self.finalize_flag = False
        self.exception_catch_stack = []
        self.exception_catch_flags = []
        self.exception_catch_links = []    
         
    def define_exception_catch(self,column_name:str,
                        aux_function_name:str,
                        aux_function_data:dict,
                        logging_function_name:str,
                        logging_function_data:dict={},
                        auto_start:bool=True):
        
        
        if not isinstance(column_name, str):
            raise TypeError("Column name must be a string")
        if not isinstance(aux_function_name, str):
            raise TypeError("Exception function name must be a string")
        if not isinstance(aux_function_data, dict):
            raise TypeError("Aux function data must be a dictionary")
        
        if not isinstance(logging_function_name, str):
            raise TypeError("Logging function name must be a string")
        if not isinstance(logging_function_data, dict):
            raise TypeError("Logging function data must be a dictionary")
 
    
        
        self.ctb.add_one_shot_function(logging_function_name)
    
        column_data = {
                       "logging_function": logging_function_name,
                       "logging_function_data": logging_function_data,
                       "aux_function_data": aux_function_data}
        
        self.exception_catch_links.append([-1,-1,-1])
        self.exception_catch_flags.append([False,False,False])
       
    
        return_column_name = self.define_column(column_name,
                                  aux_function=aux_function_name,
                                  main_function="CFL_EXCEPTION_CATCH_MAIN",
                                  initialization_function="CFL_EXCEPTION_CATCH_INIT",
                                  termination_function="CFL_EXCEPTION_CATCH_TERM",
                                  column_data=column_data,label= "EXCEP_CATCH",auto_start=auto_start)
    
        self.exception_catch_stack.append(return_column_name)
        return return_column_name
    
    def define_main_exception_column(self, name:str, main_function ="CFL_COLUMN_MAIN",
                      initialization_function ="CFL_COLUMN_INIT", termination_function ="CFL_COLUMN_TERM", 
                      aux_function ="CFL_COLUMN_NULL",column_data:dict = {},auto_start = True):
        if not isinstance(name, str):
            raise TypeError("Name must be a string")
        if not isinstance(main_function, str):
            raise TypeError("Main function must be a string")
        if not isinstance(initialization_function, str):
            raise TypeError("Initialization function must be a string")
        if not isinstance(aux_function, str):
            raise TypeError("Aux function must be a string")
        if not isinstance(termination_function, str):
            raise TypeError("Termination function must be a string")
        if not isinstance(column_data, dict):
            raise TypeError("Column data must be a dictionary")
        if not isinstance(auto_start, bool):
            raise TypeError("Auto start must be a boolean")
        
        self.exception_catch_flags[-1][0] = True
        column_name = self.define_column(column_name=name,main_function=main_function,
                                  initialization_function=initialization_function,aux_function=aux_function,
                                termination_function=termination_function,column_data=column_data,label="EXCEP_MAIN",
                                auto_start=auto_start)
        self.exception_catch_links[-1][0] = self.ctb.get_node_index(column_name)
        return column_name

    def end_main_exception_column(self,name:str):
        name_link = self.ctb.get_node_index(name)
        if name_link != self.exception_catch_links[-1][0]:
            raise ValueError("Main exception column mismatch")
        
        self.end_column(name)
    
    def define_recovery_column(self,name:str,max_steps:int,skip_condition_function:str,skip_condition_data:dict={}):
        if not isinstance(name, str):
            raise TypeError("Name must be a string")
        if not isinstance(max_steps, int):
            raise TypeError("Max steps must be an integer")
        if not isinstance(skip_condition_function, str):
            raise TypeError("Skip condition function must be a string")
        if not isinstance(skip_condition_data, dict):
            raise TypeError("Skip condition data must be a dictionary")
        if max_steps <= 0:
            raise ValueError("Max steps must be greater than 0")
        column_data = {
            "max_steps": max_steps,
            "skip_condition_data": skip_condition_data
        }
        column_name = self.define_column(column_name=name,main_function="CFL_RECOVERY_MAIN",aux_function=skip_condition_function,initialization_function="CFL_RECOVERY_INIT",
                                       termination_function="CFL_RECOVERY_TERM",column_data=column_data,label="RECOVERY_LINK")
        self.exception_catch_links[-1][1] = self.ctb.get_node_index(column_name)
        self.exception_catch_flags[-1][1] = True
        return column_name
    
    def end_recovery_column(self,name:str):
        name_link = self.ctb.get_node_index(name)
        if name_link != self.exception_catch_links[-1][1]:
            raise ValueError("Recovery column mismatch")
        ref_data = self.ctb.yaml_data[name]
        links_number = len(ref_data["label_dict"]["links"])
        max_steps = ref_data["node_dict"]["column_data"]["max_steps"]
        
        if links_number < max_steps+2:
            raise ValueError("Recovery column has not the correct number of links")
        self.end_column(name)
    
    def define_finalize_column(self, name:str, main_function ="CFL_COLUMN_MAIN",
                      initialization_function ="CFL_COLUMN_INIT", termination_function ="CFL_COLUMN_TERM", 
                      aux_function ="CFL_COLUMN_NULL",column_data:dict = {},auto_start = True):
        if not isinstance(name, str):
            raise TypeError("Column name must be a string")
        if not isinstance(main_function, str):
            raise TypeError("Main function must be a string")
        if not isinstance(initialization_function, str):
            raise TypeError("Initialization function must be a string")
        if not isinstance(aux_function, str):
            raise TypeError("Aux function must be a string")
        if not isinstance(termination_function, str):
            raise TypeError("Termination function must be a string")
        if not isinstance(column_data, dict):
            raise TypeError("Column data must be a dictionary")
        if not isinstance(auto_start, bool):
            raise TypeError("Auto start must be a boolean")
        column_name = self.define_column(column_name=name,main_function=main_function,
                                  initialization_function=initialization_function,aux_function=aux_function,
                                termination_function=termination_function,column_data=column_data,label="EXCEP_MAIN",
                                auto_start=auto_start)
        self.exception_catch_links[-1][2] = self.ctb.get_node_index(column_name)
        self.exception_catch_flags[-1][2] = True
        return column_name
    
    def end_finalize_column(self,name:str):
        name_link = self.ctb.get_node_index(name)
        if name_link != self.exception_catch_links[-1][2]:
            raise ValueError("Finalize column mismatch")
        self.end_column(name)
    
    def exception_catch_end(self,exception_catch_name:str):
        if not isinstance(exception_catch_name, str):
            raise TypeError("Exception catch name must be a string")
        if len(self.exception_catch_stack) == 0:
            raise ValueError("Exception catch stack is empty")
        poped_exception_catch = self.exception_catch_stack.pop()
        print(poped_exception_catch,exception_catch_name)
        if poped_exception_catch != exception_catch_name:
            raise ValueError("Exception catch mismatch")
        check_flag = self.exception_catch_flags.pop()
        
        if not check_flag[0]:
            raise ValueError("Main Link not started")
        if not check_flag[1]:
            raise ValueError("Recovery Link not started")
        if not check_flag[2]:
            raise ValueError("Finalize Link not started")
        link_data = self.exception_catch_links.pop()
        ref_data = self.ctb.yaml_data[exception_catch_name]
        ref_data["node_dict"]["column_data"]["exception_catch_links"] = [link_data[0],link_data[1],link_data[2]]
        self.end_column(exception_catch_name)
        
        
    def catch_all_exception(self,column_name:str,aux_function:str,aux_data:dict={},auto_start:bool=True):
        if not isinstance(column_name, str):
            raise TypeError("Column name must be a string")
        if not isinstance(aux_function, str):
            raise TypeError("Aux function must be a string")
        if not isinstance(aux_data, dict):
            raise TypeError("Aux data must be a dictionary")
        if not isinstance(auto_start, bool):
            raise TypeError("Auto start must be a boolean")
        return self.define_column(column_name=column_name,aux_function=aux_function,
                                  main_function="CFL_EXCEPTION_CATCH_ALL_MAIN",
                                  initialization_function="CFL_CATCH_ALL_EXCEPTION_INIT",
                                  termination_function="CFL_CATCH_ALL_EXCEPTION_TERM",
                                  column_data=aux_data,label="CATCH_ALL_EXCEPTION",
                                  auto_start=auto_start)
    
    def end_catch_all_exception(self,name:str):
        if not isinstance(name, str):
            raise TypeError("Name must be a string")
        self.end_column(name)
        
    def asm_turn_heartbeat_on(self,time_out:int):
        return self.asm_one_shot_handler("CFL_TURN_HEARTBEAT_ON",{"time_out":time_out})
    
    def asm_turn_heartbeat_off(self):
        return self.asm_one_shot_handler("CFL_TURN_HEARTBEAT_OFF",{})
    
    def asm_heartbeat_event(self):
        return self.asm_one_shot_handler("CFL_HEARTBEAT_EVENT",{})
    
    def asm_raise_exception(self,exception_id:int,exception_data:dict={}):
    
        if not isinstance(exception_id, int):
            raise TypeError("Exception id must be an integer")
        if not isinstance(exception_data, dict):
            raise TypeError("Exception data must be a dictionary")
        return self.asm_one_shot_handler("CFL_RAISE_EXCEPTION",{"exception_id": exception_id,
                                                                "exception_data": exception_data})
        
    def asm_set_exception_step(self,step:int):
        if not isinstance(step, int):
            raise TypeError("Step must be an integer")
        if step < 0:
            raise ValueError("Step must be greater than 0")
        return self.asm_one_shot_handler("CFL_SET_EXCEPTION_STEP",{"step": step})