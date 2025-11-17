from .ct_tree_walker import CT_Tree_Walker



class CT_Engine:
    def __init__(self,handle):
        
        self.handle= handle
        self.handle["ct_engine"] = self
        self.chain_tree = handle["chain_tree"]
        
  
   
 
        
    ################################ initialize data structures     ########################
    #########################################################################################
    def setup_initial_runtime_data_fields(self,node_id):
        ct_walker = CT_Tree_Walker(self.handle["chain_tree"].python_dict, self.setup_node_link_control, self.get_node_forward_links)
        ct_walker.walk(start_node=node_id, method='bfs', user_handle=self.handle)
        
    def setup_node_link_control(self,node,user_handle):
        
        node["ct_control"] = {}
        node["ct_control"]["initialized"] = False
        node["ct_control"]["enabled"] = False
        node["ct_control"]["result_status"] = False
        node["ct_control"]["result_data"] = {}
        
        return True
        
    ######################## execute event       ############################################
    #########################################################################################
 
        
    def reset_start_node(self,node_id):
        
        node = self.handle["chain_tree"].python_dict[node_id]
        if node is None:
            raise Exception("node not found")
        self.reset_node(node)
        
    def initialize_ct_walker(self):
        self.ct_walker = CT_Tree_Walker(self.handle["chain_tree"].python_dict, self.execute_node, self.get_node_forward_enabled_links)  
              
    def execute_event(self,node_id,event_id,event_data ):
        #print("\n\n^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^execute_event", node_id, event_id, event_data,"^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^\n\n")
        self.event_id = event_id
        self.event_data = event_data
        if self.node_id_enabled(node_id) == False:
            #raise Exception("node is not active for execute event")
            return False
        user_handle = [self.handle,event_id,event_data,0,True]
        return_value = self.ct_walker.walk(start_node=node_id, method='iterative', user_handle=user_handle)
        
        
        if user_handle[3] == 0 or user_handle[4] == False:
            return False
        return True
        
        
    def execute_node(self,node,user_handle):
        #print("*****************************execute_node", node["label_dict"]["ltree_name"],user_handle[1] ,node["ct_control"])
        if "ct_control" not in node:
            raise Exception("ct_control not found")
        if node["ct_control"]["enabled"] == False:
            #print("***************************** FALSE", node["label_dict"]["ltree_name"])
            return "FALSE"
        handle = user_handle[0]
        event_id = user_handle[1]
        event_data = user_handle[2]
        if node["ct_control"]["initialized"] == False:
            
            self.execute_initialization_function(node,handle,event_id,event_data)
            self.execute_auxiliary_function(node,handle,"CFL_INIT_EVENT",None)
            node["ct_control"]["initialized"] = True
        number_executed_nodes = user_handle[3]
        user_handle[4] = True
        user_handle[3] = number_executed_nodes + 1
        
        return_code = self.execute_main_function(node,handle,event_id,event_data)
        #print("^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^return_code", return_code,"^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^\n")
        if (return_code == "CFL_CONTINUE") or (return_code == "CFL_FUNCTION_HALT"):
            #print("***************************** CFL_CONTINUE", node["label_dict"]["ltree_name"])
            #print("***************************** True")
            return True
        
        if return_code == "CFL_HALT":
            #print("***************************** STOP_SIBLINGS")
            return "STOP_SIBLINGS"
        
        if (return_code == "CFL_DISABLE") or (return_code == "CFL_TERMINATE_FUNCTION"):
            self.terminate_node_tree(node["label_dict"]["ltree_name"])
            #print("***************************** True")
            return True
        if return_code == "CFL_TEMPLATE_UNLOAD":
            self.terminate_node_id(node["label_dict"]["ltree_name"])
            return "STOP_LEVEL"
    
        if return_code == "CFL_RESET":
            parent_node = self.get_parent_node(node)
            self.terminate_node_tree(parent_node)
            self.reset_node_id(parent_node)
            #print("***************************** STOP_SIBLINGS")
            return "STOP_SIBLINGS"
        
        if return_code == "CFL_TERMINATE":
            
            
        
            if node["label_dict"]["ltree_name"] == self.handle["chain_tree"].root_node_id:
                self.terminate_node(node)
                user_handle[4] = True
                user_handle[3] = 0
                #print("***************************** STOP_ALL")
                return "STOP_ALL"
            parent_node = self.get_parent_node(node)
            self.terminate_node_tree(parent_node)
            return "STOP_SIBLINGS"
        

        
            
        if return_code == "CFL_TERMINATE_SYSTEM":
           user_handle[4] = False
           user_handle[3] = 0
           #print("***************************** STOP_ALL")
           return "STOP_ALL"
        print("*****************************invalid return code", return_code)
        raise Exception("invalid return code {return_code}")    
    
    def execute_initialization_function(self,node,handle,event_id,event_data):
        label_dict = node["label_dict"]
        initialization_function_name = label_dict["initialization_function_name"]
        
        self.handle["chain_tree"].Vo.run_one_shot_function(initialization_function_name,handle,node)

    def execute_main_function(self,node,handle,event_id,event_data):
        label_dict = node["label_dict"]
        main_function_name = label_dict["main_function_name"]
        auxiliary_function_name = label_dict["aux_function_name"]
        
        return_code =self.handle["chain_tree"].Vm.run_main_function(main_function_name,auxiliary_function_name,handle,node,event_id,event_data)
        return return_code
    
    def execute_auxiliary_function(self,node,handle,event_id,event_data):
        label_dict = node["label_dict"]
        auxiliary_function_name = label_dict["aux_function_name"]
        return_code =self.handle["chain_tree"].Vb.run_boolean_function(auxiliary_function_name,handle,node,event_id,event_data)
        return return_code      
    
    def execute_termination_function(self,handle,node):
        label_dict = node["label_dict"]
        chain_tree = handle["chain_tree"]
        termination_function_name = label_dict["termination_function_name"]
        auxiliary_function_name = node["label_dict"]["aux_function_name"]
        chain_tree.Vb.run_boolean_function(auxiliary_function_name,handle,node,"CFL_TERM_EVENT",{})
        chain_tree.Vo.run_one_shot_function(termination_function_name,handle,node,termination_flag=True)
        
    ##############################  terminate node tree ########################3############
    #########################################################################################
    
    
    def terminate_node_tree(self,starting_node_id):
        self.handle["terminate_list"] = []
        ct_walker = CT_Tree_Walker(self.handle["chain_tree"].python_dict,self.collect_node_list, self.get_node_forward_links)
        ct_walker.walk(start_node=starting_node_id, method='iterative', user_handle=self.handle)
        self.handle["terminate_list"].reverse()
        terminate_list = self.handle["terminate_list"]
        for node in terminate_list:
            self.terminate_node(node)
    

    
    def collect_node_list(self,node,user_handle):
        
        if "ct_control" not in node:
            raise Exception("ct_control not found")
        if node["ct_control"]["enabled"] == True and node["ct_control"]["initialized"] == True:
            
            user_handle["terminate_list"].append(node)
            
        node["ct_control"]["enabled"] = False
        node["ct_control"]["initialized"] = False
    
        return True
        
  
  ###################################################################################
  ########################### support functions #####################################
  ###################################################################################
    
    def terminate_node_id(self,node_id):
    
        node = self.handle["chain_tree"].python_dict[node_id]
        if node is None:
            raise Exception("node not found")
        self.terminate_node(node)
    
    
    def terminate_initial_node_id(self,node_id):
        node = self.handle["chain_tree"].python_dict[node_id]
        if node is None:
            raise Exception("node not found")
        node["ct_control"]["initialized"] = False
        node["ct_control"]["enabled"] = False
    
    def terminate_node(self,node):
        
        if "ct_control" not in node:
            raise Exception("ct_control not found")
        node["ct_control"]["enabled"] = False
        node["ct_control"]["initialized"] = False
        
        self.execute_termination_function(self.handle,node)
    
    def reset_node_id(self,node_id):
        node =  self.chain_tree.python_dict[node_id]
        if node is None:
            raise Exception("node not found")
        parent_node_id = self.get_parent_node(node)
        if self.node_id_enabled(parent_node_id) == True:
            self.reset_node(node)
        else:
            raise Exception("parent node is not enabled for enabling a node")
        
    def reset_node(self,node):
        if "ct_control" not in node:
            raise Exception("ct_control not found")
        node["ct_control"]["initialized"] = False
        node["ct_control"]["enabled"] = True
        
    
    def set_node_state(self,node_id,state,data):
        node = self.handle["python_dict"][node_id]
        if node is None:
            raise Exception("node not found")
        node["ct_control"]["result_status"] = state
        node["ct_control"]["result_data"] = data
        
    def get_node_state(self,node_id):
        node = self.handle["python_dict"][node_id]
        if node is None:
            raise Exception("node not found")
        return node["ct_control"]["result_status"], node["ct_control"]["result_data"]
    
    def get_node_forward_links(self,node):
        if "label_dict" not in node:
            raise Exception("label_dict not found")
        if "links" not in node["label_dict"]:
            return []
        return node["label_dict"]["links"]
    
    def get_node_forward_enabled_links(self,node):
       
        if "label_dict" not in node:
            raise Exception("label_dict not found")
        if "links" not in node["label_dict"]:
            return []
        node_links = node["label_dict"]["links"]
        return_value = []
        for link in node_links:
            
            if self.node_id_enabled(link) == True:
                return_value.append(link)
        
        return return_value
        
    def get_node_backward_links(self,node):
        if "label_dict" not in node:
            raise Exception("label_dict not found")
        if "links" not in node["label_dict"]:
            return []
        node_links = node["label_dict"]["links"]
        reversed_node_links = node_links[::-1]
        return reversed_node_links   
    
    def get_parent_node(self,node):
        if "label_dict" not in node:
            raise Exception("label_dict not found")
        return node["label_dict"]["parent_ltree_name"]
    
    def get_node_data(self,node_id):
        node = self.handle["chain_tree"].python_dict[node_id]
        if node is None:
            raise Exception("node not found")
        return node
        
        
    def node_id_enabled(self,node_id):
        node = self.handle["chain_tree"].python_dict[node_id]
        if node is None:
            raise Exception("node not found")
        return node["ct_control"]["enabled"]
    
    def enable_selected_node(self,state_number,state_links):
        state_node = state_links[state_number]
        self.enable_node_id(state_node)
        
    
    
    def enable_node_id(self,node_id):
        node = self.handle["chain_tree"].python_dict[node_id]
        if node is None:
            raise Exception("node not found")
        node["ct_control"]["enabled"] = True
        node["ct_control"]["initialized"] = False
        
    ##################### watch dog functions #####################
    
    def enable_watch_dog_node_id(self,node_id):
        node = self.handle["chain_tree"].python_dict[node_id]
        if node is None:
            raise Exception("node not found")
        self.enable_watch_dog_node(node)
        
        
    def enable_watch_dog_node(self,node):
        parent_node_id = self.get_parent_node(node)
        parent_node = self.handle["chain_tree"].python_dict[parent_node_id]
        if parent_node is None:
            raise Exception("parent node not found")
        parent_node["wd_control"]["enabled"] = True
        parent_node["wd_control"]["pat_enabled"] = False
        
    def disable_watch_dog_node_id(self,node_id):
        node = self.handle["chain_tree"].python_dict[node_id]
        if node is None:
            raise Exception("node not found")
        self.disable_watch_dog_node(node)
        
    def disable_watch_dog_node(self,node):
        parent_node_id = self.get_parent_node(node)
        parent_node = self.handle["chain_tree"].python_dict[parent_node_id]
        if parent_node is None:
            raise Exception("parent node not found")
        parent_node["wd_control"]["enabled"] = False
        parent_node["wd_control"]["pat_enabled"] = False
        
        
    def pat_watch_dog_node_id(self,node_id):
        node = self.handle["chain_tree"].python_dict[node_id]
        if node is None:
            raise Exception("node not found")
        self.pat_watch_dog_node(node)
        
    def pat_watch_dog_node(self,node):
        parent_node_id = self.get_parent_node(node)
        parent_node = self.handle["chain_tree"].python_dict[parent_node_id]
        if parent_node is None:
            raise Exception("parent node not found")
        parent_node["wd_control"]["pat_enabled"] = True
        
    
    ###############################################################
    #######################LISP SEQUENCER FUNCTIONS ########################
    #########################################################################################
    def run_s_expression(self, handle, function_type, function_name, node, 
                     event_id, event_data, params=None):
        """
        Execute an S-expression function with the given parameters.
        
        Args:
            handle: Execution handle
            function_type: Type prefix of the function (e.g., '@', '?', '!')
            function_name: Name of the function to execute
            node: Node containing function metadata
            event_id: Event identifier
            event_data: Event data dictionary
            params: Optional list of parameters
            
        Returns:
            Function result (type depends on function_type)
            
        Raises:
            KeyError: If function not found in dispatch dictionary
            NotImplementedError: If link tasks are not implemented
            ValueError: If invalid function type encountered
        """
        if params is None:
            params = []
        
        # Build full function identifier
        full_name = function_type + function_name
        
        # Get dispatch and local node information
        column_data = node["node_dict"]["column_data"]
        function_dispatch_dict = column_data["function_dispatch_dict"]
        local_nodes = column_data["local_nodes"]
        
        # Validate function exists
        if full_name not in function_dispatch_dict:
            raise KeyError(f"Function '{full_name}' not found in dispatch dictionary")
        
        dispatch_dict = function_dispatch_dict[full_name]
        dispatch_type = dispatch_dict["type"]
        dispatch_function_name = dispatch_dict.get("function_name")
  
        if dispatch_type == "reset_control_codes":
            return self.reset_control_codes(local_nodes)

       
        
        # Get local node information
        local_node = local_nodes[full_name]
        is_enabled = local_node["enabled"]
        is_initialized = local_node["initialized"]
        
        # Handle disabled nodes - return default values
        if not is_enabled:
            if dispatch_type == "one_shot":
                return None
            elif dispatch_type == "boolean":
                return False
            elif dispatch_type == "main":
                return "CFL_CONTINUE"
            
            else:
                raise ValueError(f"Unknown function type: {dispatch_type}")
        
        # Initialize node on first run (only for certain types)
        if not is_initialized:
            if dispatch_type == "boolean":
                self.chain_tree.Vsb.run_s_boolean_function(
                    dispatch_function_name, handle, local_node, 
                    "CFL_INIT_EVENT", {}, params
                )
                local_node["initialized"] = True
            elif dispatch_type == "main":
                self.chain_tree.Vsm.run_s_main_function(
                    dispatch_function_name, handle, local_node, 
                    "CFL_INIT_EVENT", {}, params
                )
                local_node["initialized"] = True
            # Note: one_shot and link types don't need initialization
        
        # Execute based on function type
    
        
        if dispatch_type == "one_shot":
            self.chain_tree.Vso.run_s_one_shot_function(
                dispatch_function_name, handle, local_node, 
                event_id, event_data, params
            )
            local_node["enabled"] = False  # Disable after one execution
            return None
        
        elif dispatch_type == "boolean":
            return self.chain_tree.Vsb.run_s_boolean_function(
                dispatch_function_name, handle, local_node, 
                event_id, event_data, params
            )
        
        elif dispatch_type == "main":
            return_value = self.chain_tree.Vsm.run_s_main_function(
                dispatch_function_name, handle, local_node, 
                event_id, event_data, params)
        
            
            
            if return_value == "CFL_DISABLE":
            
                local_node["enabled"] = False
                self.chain_tree.Vsm.run_s_main_function(
                    dispatch_function_name, handle, local_node, 
                    "CFL_DISABLE_EVENT", {}, params )
                return "CFL_CONTINUE"
            if return_value == "CFL_TERMINATE_FUNCTION":
                self.terminate_s_nodes(column_data)
                return "CFL_DISABLE"
            return return_value
        else:
            # Should never reach here due to earlier validation
            raise ValueError(
                f"Invalid function type: {dispatch_type} for function {dispatch_function_name}"
            )
            
    def terminate_s_nodes(self,column_data):
        
        local_nodes = column_data["local_nodes"]
        function_dispatch_dict = column_data["function_dispatch_dict"]
    
        for node_name, local_node in column_data["function_dispatch_dict"].items():
            dispatch_dict = function_dispatch_dict[node_name]
            if dispatch_dict["type"] == "main":
                self.chain_tree.Vsm.run_s_main_function(dispatch_dict["function_name"], None, local_node, "CFL_TERM_EVENT", {}, {})
                local_node["enabled"] = False
                local_node["initialized"] = False
            if dispatch_dict["type"] == "boolean":
                self.chain_tree.Vsb.run_s_boolean_function(dispatch_dict["function_name"], None, local_node, "CFL_TERM_EVENT", {}, {})
                local_node["enabled"] = False
                local_node["initialized"] = False
            
            
 
    
    def reset_control_codes(self,local_nodes):
        
        for node_name, local_node in local_nodes.items():
            local_node["initialized"] = False
            local_node["enabled"] = True
        return "CFL_FUNCTION_RETURN"
            