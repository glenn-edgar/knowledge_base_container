import time
import json
from pathlib import Path
from datetime import datetime, timezone

from .chain_tree.chain_tree_yaml import ChainTreeYaml
from .chain_tree.basic_cf_links import BasicCfLinks
from .chain_tree.wait_cf_links import WaitCfLinks
from .chain_tree.verify_cf_links import VerifyCfLinks
from .chain_tree.state_machine import StateMachine

from .chain_tree.column_flow import ColumnFlow
from .chain_tree.sequence_til import SequenceTil
from .chain_tree.data_flow import DataFlow
from .chain_tree.exception import ExceptionHandler




class ChainTreeMaster(BasicCfLinks, WaitCfLinks, VerifyCfLinks, StateMachine, SequenceTil, DataFlow, ExceptionHandler):
    

    
    def __init__(self, yaml_file: Path):
        self.ctb = ChainTreeYaml(yaml_file)
        BasicCfLinks.__init__(self,self.ctb)
        WaitCfLinks.__init__(self,self.ctb)
        VerifyCfLinks.__init__(self,self.ctb)
        StateMachine.__init__(self,self.ctb)

        ColumnFlow.__init__(self,self.ctb)
        SequenceTil.__init__(self,self.ctb)
        DataFlow.__init__(self,self.ctb)
        ExceptionHandler.__init__(self,self.ctb)
        
        self.ctb.register_event("CFL_INIT_EVENT")
        self.ctb.register_event("CFL_TERMINATE_EVENT")
        self.ctb.register_event("CFL_START_TESTS")
        self.ctb.register_event("CFL_TERMINATE_TESTS")
        self.ctb.register_event("CFL_TIMER_EVENT")
        self.ctb.register_event("CFL_SECOND_EVENT")
        self.ctb.register_event("CFL_MINUTE_EVENT")
        self.ctb.register_event("CFL_HOUR_EVENT")
        self.ctb.register_event("CFL_DAY_EVENT")
        self.ctb.register_event("CFL_WEEK_EVENT")
        self.ctb.register_event("CFL_MONTH_EVENT")
        self.ctb.register_event("CFL_YEAR_EVENT")
        self.ctb.register_event("CFL_RAISE_EXCEPTION_EVENT")
        self.ctb.register_event("CFL_TURN_HEARTBEAT_ON_EVENT")
        self.ctb.register_event("CFL_TURN_HEARTBEAT_OFF_EVENT")
        self.ctb.register_event("CFL_HEARTBEAT_EVENT")
        self.ctb.register_event("CFL_SET_EXCEPTION_STEP_EVENT")
        self.ctb.register_event("CFL_START_TESTS")
        self.ctb.register_event("CFL_TERMINATE_TESTS")
        self.ctb.register_event("CFL_CHANGE_STATE_EVENT")
        self.ctb.register_event("CFL_RESET_STATE_MACHINE_EVENT")
        self.ctb.register_event("CFL_TERMINATE_STATE_MACHINE_EVENT")
        
        

        
        
        
    
    def check_and_generate_yaml(self):
    
    
        self.check_valid_chain_tree_configuration()
        self.dump_kb_functions()
        self.dump_complete_functions()
        self.generate_yaml()
        
        
        
        
    def  generate_yaml(self):
        self.ctb.generate_yaml()

    def select_kb(self,kb_name):
        self.ctb.select_kb(kb_name)
            
    def debug_function(self, handle, message, node=None, event_id=None):
        """Debug logging with timestamp."""
        timestamp = datetime.now().isoformat()
        print(f"[{timestamp}] DEBUG: {message}")
        if node is not None or event_id is not None:
            print(f"  Node: {node}, Event: {event_id}")
            
        
    def define_root_node(self,version:str):
       self.ctb.link_number = 0
       self.ctb.link_number_stack = []
       self.root_node = self.define_gate_node("root_node", column_data={"version":version},auto_start = True,links_flag = True)
       self.sequence_dict = {}
       self.s_expr_dict = {}
       self.initialize_state_machine_stack()
                 
 
    
                 
    def start_test(self,test_name:str,
                  kb_memory_factor=10):  
        
        self.select_kb(test_name)
        self.ctb.kb_metadata[test_name] = {"node_memory_factor": kb_memory_factor}
        self.define_root_node(version="1.0.0")            
       
    
     
        
    def end_test(self):
        self.finalize_and_check()
    
        
        
    def pop_root_node(self):
        self.ctb.pop_node_element(self.root_node)
        
    def add_state_machine_node(self):
       node_data = self.sm_name_dict 
       self.sm_node = self.ctb.add_node_element("sm_node","sm_node","CFL_NULL","CFL_NULL","CFL_NULL","CFL_NULL",node_data)
       self.pop_state_machine_node()                

    def pop_state_machine_node(self):
        self.ctb.pop_node_element(self.sm_node)

    def check_valid_chain_tree_configuration(self):
        self.check_for_balance_sm()
        #self.check_for_balance_ltree()
        
        
    def finalize_and_check(self):
        self.check_for_balance_sm()
        if len(self.sequence_dict) > 0:
            raise ValueError(f"Unfinished sequence ends {self.sequence_dict.keys()}")
        if len(self.s_expr_dict) > 0:
            raise ValueError(f"Unfinished s_expression ends {self.s_expr_dict.keys()}")
        self.end_column(self.root_node)
        self.ctb.leave_kb()
    
            

    
     
    def get_all_virtual_functions(self):
        """Retrieve all virtual functions from the complete_functions node."""
        
        # The ltree path includes the KB prefix: kb.complete_functions_kb.complete_functions.complete_functions.complete_functions.<function_type>
        base_path = "kb.complete_functions_kb.complete_functions.complete_functions.complete_functions"
        
        main_functions = self.ctb.yaml_data[f"{base_path}.main_functions"]["node_dict"].keys()
        one_shot_functions = self.ctb.yaml_data[f"{base_path}.one_shot_functions"]["node_dict"].keys()
        boolean_functions = self.ctb.yaml_data[f"{base_path}.boolean_functions"]["node_dict"].keys()
       
        
        return {
            "main_functions": main_functions,
            "one_shot_functions": one_shot_functions,
            "boolean_functions": boolean_functions,
            
        } 
    
    def display_chain_tree_function_mapping(self):
        all_virtual_functions = self.get_all_virtual_functions()
        print("complete function mapping:")
        print("main_functions:")
        for function in all_virtual_functions["main_functions"]:
            print("--------------------------------",function)
        print("display one_shot_functions:")
        print("one_shot_functions:")
        for function in all_virtual_functions["one_shot_functions"]:
            print("--------------------------------",function)
        
        print("boolean_functions:")
        for function in all_virtual_functions["boolean_functions"]:
            print("--------------------------------",function)
 
        
      

        
    def dump_kb_functions(self):
        """Dump all function mappings as nodes in the YAML structure."""
        
        # Create a list copy of the keys to avoid "dictionary changed during iteration" error
        kb_names = list(self.ctb.kb_log_dict.keys())
        
        for kb_name in kb_names:
            
            # Skip temporary KBs
            if kb_name.endswith("_functions"):
                continue
            
            # Temporarily re-add and select the KB
            temp_kb_name = kb_name + "_functions"
            self.ctb.add_kb(temp_kb_name)
            self.ctb.select_kb(temp_kb_name)
            
            # Create top-level node for this KB's function mappings
            top_node = self.ctb.add_node_element(
                "kb", 
                kb_name,
                "CFL_NULL", "CFL_NULL", "CFL_NULL", "CFL_NULL",
                {}
            )
            
            # Create virtual_functions container node
            function_node = self.ctb.add_node_element(
                "virtual_functions", 
                "virtual_functions",
                "CFL_NULL", "CFL_NULL", "CFL_NULL", "CFL_NULL",
                {}
            )
            
            # Add leaf nodes for each function type
            node_data = self.ctb.main_functions[kb_name]
            self.ctb.add_leaf_element(
                "virtual_functions", "main_functions",
                "CFL_NULL", "CFL_NULL", "CFL_NULL", "CFL_NULL",
                node_data
            )
            
            node_data = self.ctb.one_shot_functions[kb_name]
            self.ctb.add_leaf_element(
                "virtual_functions", "one_shot_functions",
                "CFL_NULL", "CFL_NULL", "CFL_NULL", "CFL_NULL",
                node_data
            )
            
            node_data = self.ctb.boolean_functions[kb_name]
            self.ctb.add_leaf_element(
                "virtual_functions", "boolean_functions",
                "CFL_NULL", "CFL_NULL", "CFL_NULL", "CFL_NULL",
                node_data
            )
            
            node_data = self.ctb.main_functions[kb_name]
            self.ctb.add_leaf_element(
                "virtual_functions", "main_functions",
                "CFL_NULL", "CFL_NULL", "CFL_NULL", "CFL_NULL",
                node_data
            )
            
            # Close the nodes
            self.ctb.pop_node_element(function_node)
            self.ctb.pop_node_element(top_node)
            
            # Leave this temporary KB
            self.ctb.leave_kb()
            
            # Remove from kb_log_dict since this is just a temporary KB
            if temp_kb_name in self.ctb.kb_log_dict:
                del self.ctb.kb_log_dict[temp_kb_name]
 
            
    def dump_complete_functions(self):
        """Dump a consolidated list of all unique functions across all KBs."""
        
        # Temporarily add and select a KB for this operation
        self.ctb.add_kb("complete_functions_kb")
        self.ctb.select_kb("complete_functions_kb")
        
        # Create top-level node
        top_node = self.ctb.add_node_element(
            "complete_functions", "complete_functions",
            "CFL_NULL", "CFL_NULL", "CFL_NULL", "CFL_NULL",
            {}
        )
        
        # Initialize maps for collecting unique functions
        one_shot_map = {}
        boolean_map = {}
        main_map = {}
        s_one_shot_map = {}
        s_boolean_map = {}
        s_main_map = {}
        
        # Create list copy to avoid "dictionary changed during iteration" error
        kb_names = list(self.ctb.kb_log_dict.keys())
        
        # Collect all unique functions from all KBs
        for kb_name in kb_names:
            
            # Collect one_shot functions
            test_one_shot_map = self.ctb.one_shot_functions[kb_name].keys()
            for function in test_one_shot_map:
                if function not in one_shot_map:
                    one_shot_map[function] = True
            
            # Collect boolean functions
            test_boolean_map = self.ctb.boolean_functions[kb_name].keys()
            for function in test_boolean_map:
                if function not in boolean_map:
                    boolean_map[function] = True
            
            # Collect main functions
            test_main_map = self.ctb.main_functions[kb_name].keys()
            for function in test_main_map:
                if function not in main_map:
                    main_map[function] = True
            
            # Collect s_one_shot functions
           
        
        # Add leaf elements for each function type
        self.ctb.add_leaf_element(
            "complete_functions", "one_shot_functions",
            "CFL_NULL", "CFL_NULL", "CFL_NULL", "CFL_NULL",
            one_shot_map
        )
        
        self.ctb.add_leaf_element(
            "complete_functions", "boolean_functions",
            "CFL_NULL", "CFL_NULL", "CFL_NULL", "CFL_NULL",
            boolean_map
        )
        
        self.ctb.add_leaf_element(
            "complete_functions", "main_functions",
            "CFL_NULL", "CFL_NULL", "CFL_NULL", "CFL_NULL",
            main_map
        )
        
      
        # Close the top node
        self.ctb.pop_node_element(top_node)
        
        # Leave the temporary KB
        self.ctb.leave_kb()
        
    def list_kbs(self):
        """List all knowledge bases (excluding temporary ones)."""
        # Filter out temporary KBs that end with "_functions"
        return [kb for kb in self.ctb.kb_log_dict.keys() if (not kb.endswith("_functions") and not kb.startswith("complete_functions_kb"))]
