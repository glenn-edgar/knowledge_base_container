import copy
import json
from .column_flow import ColumnFlow

class SNodeControl(ColumnFlow):
    """Controls S-node expressions and manages node lifecycles."""
    
    def __init__(self, ds, ctb, lisp_sequencer):
        super().__init__(ds, ctb)
        self.ds = ds
        self.ctb = ctb
        self.lisp_sequencer = lisp_sequencer
        self.s_expr_active = False
        
        
    def define_s_node_control(self, column_name: str, aux_function_name: str, 
                              s_expression: str, user_data: dict = None, 
                              auto_start: bool = False):
        """
        Define an S-node control with the given expression.
        
        Args:
            column_name: Name of the column
            aux_function_name: Auxiliary function name
            s_expression: S-expression string to parse
            user_data: Optional user data dictionary
            auto_start: Whether to auto-start the node
            
        Returns:
            Node identifier
        """
        if user_data is None:
            user_data = {}
            
        template_node = {
            "node_dict": {"user_data": copy.deepcopy(user_data)},
            "enabled": False,
            "initialized": False,
            "parent_node_name": None
        }
        
        self.s_expr_active = True
        
        # Type validation
        if not isinstance(s_expression, str):
            raise TypeError("s_expression must be a string")
        if not isinstance(user_data, dict):
            raise TypeError("user_data must be a dictionary")
        if not isinstance(auto_start, bool):
            raise TypeError("auto_start must be a boolean")
            
        print(f"s_expression: {s_expression}")
        s_dict = self.lisp_sequencer.check_lisp_instruction(s_expression)
       
        if not s_dict["valid"]:
            print(f"s_dict: {s_dict}")
            raise ValueError("Invalid s_expression")
            
        
        function_dispatch_dict = {}
        local_nodes = {}
        
        for fn_name_full_type in s_dict["functions"]:
            fn_type = fn_name_full_type[0]
            fn_full_name = fn_name_full_type[1:]
            fn_list = fn_full_name.split("__")
            fn_name = fn_list[0]
            if fn_name == "RESET_CODES":
                function_dispatch_dict[fn_name_full_type] = {
                    "type": "reset_control_codes",
                    "function_name": fn_name
                }
                continue
          
 
            else:
                local_nodes[fn_name_full_type] = copy.deepcopy(template_node)
                
                if fn_type == "@":
                    self.ctb.s_one_shot_function_mapping[fn_name] = True
                    function_dispatch_dict[fn_name_full_type] = {
                        "type": "one_shot",
                        "function_name": fn_name
                    }
                elif fn_type == "?":
                    self.ctb.s_boolean_function_mapping[fn_name] = True
                    function_dispatch_dict[fn_name_full_type] = {
                        "type": "boolean",
                        "function_name": fn_name
                    }
                elif fn_type == "!":
                    self.ctb.s_main_function_mapping[fn_name] = True
                    function_dispatch_dict[fn_name_full_type] = {
                        "type": "main",
                        "function_name": fn_name
                    }
                else:
                    raise ValueError(f"Invalid function type: {fn_type}")
                
        s_dict_string = json.dumps(s_dict)
        sdata = {"s_dict": s_dict_string}
        column_data = {
            "user_data": user_data,
            "s_data": sdata,
            "function_dispatch_dict": function_dispatch_dict,
            "local_nodes": local_nodes
        }
        
        return_node = self.define_column(
            column_name, "CFL_S_NODE_CONTROL_MAIN", "CFL_S_NODE_CONTROL_INIT", 
            "CFL_S_NODE_CONTROL_TERM", aux_function_name, column_data, 
            auto_start, label="S_NODE"
        )
        
        self.s_expr_dict[return_node] = True
        
        for node_name in local_nodes.keys():
            local_nodes[node_name]["parent_node_name"] = return_node
           
        return return_node
    
    def end_s_node_control(self, column_name_id: str):
        """End an S-node control and validate """
        if not self.s_expr_active:
            raise ValueError(f"S node control {column_name_id} is not active")
            
        self.s_expr_active = False
        self.s_expr_dict.pop(column_name_id)
    
 
        self.end_column(column_name_id)