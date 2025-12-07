from .column_flow import ColumnFlow

class Streaming(ColumnFlow):
    def __init__(self,ctb):
        ColumnFlow.__init__(self,ctb)
        self.ctb = ctb
        self.streaming_stack = []
        self.streaming_name_dict = {}

    def define_streaming_node(self,column_name:str, aux_function_name:str,column_data:dict = {},auto_start:bool=True):
        return self.define_column(column_name=column_name,  main_function ="CFL_STREAMING_MAIN",
                      initialization_function ="CFL_STREAMING_INIT", termination_function ="CFL_STREAMING_TERM", 
                      aux_function_name =aux_function_name,column_data = column_data,auto_start = False,label="STREAMING",links_flag = True)
        
    def define_streaming_aggregate_node(self,column_name:str, aux_function_name:str,column_data:dict = {},auto_start:bool=True):
        return self.define_column(column_name=column_name,  main_function ="CFL_STREAMING_AGGREGATE_MAIN",
                      initialization_function ="CFL_STREAMING_AGGREGATE_INIT", termination_function ="CFL_STREAMING_AGGREGATE_TERM", 
                      aux_function_name =aux_function_name,column_data = column_data,auto_start = False,label="STREAMING_AGGREGATE",links_flag = True)
    
    def define_streaming_emit_link(self,aux_function_name:str,column_data:dict = {}):
        return self.define_column_link(main_function_name="CFL_STREAMING_EMIT_MAIN", initialization_function_name="CFL_STREAMING_EMIT_INIT", 
                                       aux_function_name=aux_function_name, termination_function_name="CFL_STREAMING_EMIT_TERM", node_data=column_data)
        
    
    def define_streaming_filter_link(self,aux_function_name:str,column_data:dict = {}):
        return self.define_column_link(main_function_name="CFL_STREAMING_FILTER_MAIN", initialization_function_name="CFL_STREAMING_FILTER_INIT", 
                                       aux_function_name=aux_function_name, termination_function_name="CFL_STREAMING_FILTER_TERM", node_data=column_data)  
        
        
    def define_streaming_request_link(self,aux_function_name:str,column_data:dict = {}):
        return self.define_column_link(main_function_name="CFL_STREAMING_REQUEST_MAIN", initialization_function_name="CFL_STREAMING_REQUEST_INIT", 
                                       aux_function_name=aux_function_name, termination_function_name="CFL_STREAMING_REQUEST_TERM", node_data=column_data)
     
    def define_streaming_verify_link(self,aux_function_name:str,aux_data:dict = {},reset_flag:bool=False,error_function_name:str="CFL_NULL",error_data:dict = {}):
        column_data = {}
        column_data["reset_flag"] = reset_flag
        column_data["error_function_name"] = error_function_name
        column_data["error_data"] = error_data
        column_data["aux_data"] = aux_data
        return self.define_column_link(main_function_name="CFL_STREAMING_VERIFY_MAIN", initialization_function_name="CFL_STREAMING_VERIFY_INIT", 
                                       aux_function_name=aux_function_name, termination_function_name="CFL_STREAMING_VERIFY_TERM", node_data=column_data)
        
 