from .column_flow import ColumnFlow

#
# Making a class if more data flow column types are needed
#
#

class ControlledNodes(ColumnFlow):
    def __init__(self, ctb):
        self.ctb = ctb
        ColumnFlow.__init__(self, ctb)
        
    def mark_node_as_controlled(self, alias_name:str, node_name:str):
        return_value = self.ctb.register_node_alias(alias_name, node_name)
        return return_value
        
    def start_controlled_node_collection(self):
        return_value = {}
        return_value["alias_list"] = []
        return_value["event_data_list"] = []
        return return_value
    
    def add_controlled_node(self,controlled_node_collection:dict, node_index:int, event_data:dict):
        controlled_node_collection["node_index_list"].append(node_index)
        controlled_node_collection["event_data_list"].append(event_data)
        return controlled_node_collection
    
    def define_controlled_node(self, column_name:str, aux_function_name:str, aux_data:dict, controlled_node_collection:dict):
        column_data = {"controlled_node_collection": controlled_node_collection,"aux_data": aux_data}
        
        return self.define_column_link(column_name=column_name, main_function="CFL_CONTROLLED_NODE_MAIN", 
                                  initialization_function="CFL_CONTROLLED_NODE_INIT", termination_function="CFL_CONTROLLED_NODE_TERM", 
                                  aux_function=aux_function_name, column_data=aux_data)