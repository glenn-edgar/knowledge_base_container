from .column_flow import ColumnFlow

class SExpressionNodes(ColumnFlow):
    def __init__(self,ctb):
        ColumnFlow.__init__(self,ctb)
        self.ctb = ctb
    
        
    def define_s_expression_link(self,module_name:str,tree_name:str,user_data:dict = {}) :
        column_data = {"module_name": module_name, "tree_name": tree_name, "user_data": user_data}
        return self.define_column_link(main_function_name="CFL_S_EXPRESSION_LINK_MAIN", 
                      initialization_function_name ="CFL_S_EXPRESSION_LINK_INIT", termination_function_name ="CFL_S_EXPRESSION_LINK_TERM", 
                      aux_function_name ="CFL_NULL",node_data = column_data,label="S_EXP_LINK_NODE")
        
   
    def define_s_expression_node(self,column_name:str,module_name:str,tree_name:str,user_data:dict = {}):
        column_data = {"module_name": module_name, "tree_name": tree_name, "user_data": user_data}
        return self.define_column(column_name=column_name, main_function="CFL_S_EXPRESSION_NODE_MAIN", initialization_function="CFL_S_EXPRESSION_NODE_INIT", 
                                  termination_function="CFL_S_EXPRESSION_NODE_TERM", aux_function="CFL_NULL", column_data=column_data, label="S_EXP_NODE",auto_start=True)