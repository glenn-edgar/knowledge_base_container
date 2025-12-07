from .column_flow import ColumnFlow

class SExpressionNodes(ColumnFlow):
    def __init__(self,ctb):
        ColumnFlow.__init__(self,ctb)
        self.ctb = ctb
    
        
    def define_s_expression_link(self,aux_function_name:str,column_data:dict = {}):
        return self.define_column_link(main_function_name="CFL_S_EXPRESSION_NODE_MAIN", initialization_function_name="CFL_S_EXPRESSION_NODE_INIT", 
                      initialization_function ="CFL_S_EXPRESSION_NODE_INIT", termination_function ="CFL_S_EXPRESSION_NODE_TERM", 
                      aux_function_name =aux_function_name,column_data = column_data,auto_start = False,label="S_EXPRESSION_NODE",links_flag = True)
        
   