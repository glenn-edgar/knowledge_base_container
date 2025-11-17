

class CFLBooleanFunctions:
    def __init__(self,virtual_boolean_functions):
        self.virtual_boolean_functions = virtual_boolean_functions
        self.boolean_functions = {}
        
        
    def load_boolean_functions(self):
        function_names = self.boolean_functions.keys()
        for function_name in function_names:
            self.virtual_boolean_functions.add_boolean_function_python(function_name,
                    self.boolean_functions[function_name]["python_function"],self.boolean_functions[function_name]["description"])
        
    def load_default_boolean_functions(self):
        self.boolean_functions["CFL_NULL"] = {
            "python_function": cfl_null,
            "description": "null boolean function"
           
        }
        self.boolean_functions["CFL_COLUMN_NULL"] = {
            "python_function": cfl_column_null,
            "description": "column null boolean function"
        }
        self.boolean_functions["CFL_WAIT_FOR_EVENT"] = {
            "python_function": cfl_wait_for_event,
            "description": "wait for event event and return true if event occurs"
        }
        self.boolean_functions["CFL_GATE_NODE_NULL"] = {
            "python_function": cfl_gate_node_null,
            "description": "gate node null boolean function"
        }
        self.boolean_functions["CFL_BOOL_FALSE"] = {
            "python_function": cfl_bool_false,
            "description": "boolean false function"
         
        }
        self.boolean_functions["CFL_BOOL_TRUE"] = {
            "python_function": cfl_bool_true,
            "description": "boolean true function"
        }
        self.boolean_functions["CFL_STATE_MACHINE_NULL"] = {
            "python_function": cfl_state_machine_null,
            "description": "state machine null function"
        }
        self.boolean_functions["CFL_VERIFY_TIME_OUT"] = {
            "python_function": cfl_verify_time_out,
            "description": "verify time out function"
        }
        self.load_boolean_functions()
        
        
def cfl_null(handle,node,event_id,event_data):
    return None #place holder function
    
def cfl_column_null(handle,node,event_id,event_data):
    return "CFL_CONTINUE"
    
def cfl_wait_for_event(handle,node,event_id,event_data):

    if event_id == "CFL_INIT_EVENT":
        node_data = node["node_dict"]
        node_data["wait_fn_data"]["current_event_count"] = 0
        return False
    if event_id == node["node_dict"]["wait_fn_data"]["event_id"]:
        node_data = node["node_dict"]
        node_data["wait_fn_data"]["current_event_count"] = node_data["wait_fn_data"]["current_event_count"] + 1
        if node_data["wait_fn_data"]["current_event_count"] >= node_data["wait_fn_data"]["event_count"]:
            return True
    return False
    
def cfl_gate_node_null(handle,node,event_id,event_data):
    return None

def cfl_bool_false(handle,node,event_id,event_data):

    return False

def cfl_bool_true(handle,node,event_id,event_data):
    return True

def cfl_state_machine_null(handle,node,event_id,event_data):
    return None

def cfl_verify_time_out(handle,node,event_id,event_data):

    chain_tree = handle["chain_tree"]
    if event_id == "CFL_INIT_EVENT":
        fn_data = node["node_dict"]["fn_data"]
        fn_data["current_time"] = chain_tree.ct_timer.get_timestamp()
        return False
    if event_id == "CFL_TIMER_EVENT":
        fn_data = node["node_dict"]["fn_data"]
        current_time = chain_tree.ct_timer.get_timestamp()
        elapsed_time = current_time - fn_data["current_time"]
        if elapsed_time >= fn_data["time_out"]:
            return False
    return True