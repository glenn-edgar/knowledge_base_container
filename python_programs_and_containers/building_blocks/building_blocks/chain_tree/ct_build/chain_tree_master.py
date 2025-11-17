import time

from .chain_tree.chain_tree_basic import ChainTreeBasic
from .chain_tree.basic_cf_links import BasicCfLinks
from .chain_tree.wait_cf_links import WaitCfLinks
from .chain_tree.verify_cf_links import VerifyCfLinks
from .chain_tree.state_machine import StateMachine

from .chain_tree.column_flow import ColumnFlow
from .chain_tree.sequence_til import SequenceTil
from .chain_tree.data_flow import DataFlow
from .chain_tree.templates import Templates
from .chain_tree.s_node_control import SNodeControl
from .s_functions.lisp_sequencer import LispSequencer


class ChainTreeMaster(BasicCfLinks, WaitCfLinks, VerifyCfLinks, StateMachine, SequenceTil, DataFlow, Templates, SNodeControl):
    
    def __init__(self, data_structures):
        self.ds = data_structures
        self.ctb = ChainTreeBasic(self.ds)
        BasicCfLinks.__init__(self, self.ds,self.ctb)
        WaitCfLinks.__init__(self, self.ds,self.ctb)
        VerifyCfLinks.__init__(self, self.ds,self.ctb)
        StateMachine.__init__(self, self.ds,self.ctb)

        ColumnFlow.__init__(self, self.ds,self.ctb)
        SequenceTil.__init__(self, self.ds,self.ctb)
        DataFlow.__init__(self, self.ds,self.ctb)
        Templates.__init__(self, self.ds,self.ctb)
        
        
        self.ctb.link_number = 0
        self.ctb.link_number_stack = []
        self.lisp_sequencer = LispSequencer(self, self.run_function, self.debug_function)
        SNodeControl.__init__(self, self.ds,self.ctb,self.lisp_sequencer)

                     
    # not needed in generation
    def run_function(self, handle, func_type, func_name, node, event_id, event_data,params=[]):
        exit()
        pass
    
    def debug_function(self, handle, message, node, event_id, event_data):
      timestamp = time.now().isoformat()
      print(f"[{timestamp}] DEBUG: {message}")
      print(f"  Node: {node}, Event: {event_id}")
      
    def define_root_node(self,version:str):
       self.ctb.link_number = 0
       self.ctb.link_number_stack = []
       self.root_node = self.define_gate_node("root_node", column_data={"version":version},auto_start = True,links_flag = True)
       self.sequence_dict = {}
       self.s_expr_dict = {}
                               

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
        self.check_for_balance_ltree()
        
        
    def finalize_and_check(self):
        self.check_for_balance_sm()
        if len(self.sequence_dict) > 0:
            raise ValueError(f"Unfinished sequence ends {self.sequence_dict.keys()}")
        if len(self.s_expr_dict) > 0:
            raise ValueError(f"Unfinished s_expression ends {self.s_expr_dict.keys()}")
        self.end_column(self.root_node)
        self.ds.leave_kb()
    
            
    def write_yaml(self):
 
        
        self.dump_function_mapping()
        self.dump_micro_instructions()
     
    
    
    
    def display_chain_tree_function_mapping(self):
        print("Main function mapping:")
        
        print(self.ctb.main_function_mapping.keys())
        print("One shot function mapping:")
        print(self.ctb.one_shot_function_mapping.keys())
        print("Boolean function mapping:")
        print(self.ctb.boolean_function_mapping.keys())
        
      

        
    def dump_function_mapping(self):
        function_node = self.ctb.add_node_element("virtual_functions","virtual_functions","CFL_NULL","CFL_NULL","CFL_NULL","CFL_NULL",{})
        node_data =  self.ctb.main_function_mapping 
        self.ctb.add_leaf_element("virtual_functions","main_functions","CFL_NULL","CFL_NULL","CFL_NULL","CFL_NULL",node_data)
        node_data = self.ctb.one_shot_function_mapping
        self.ctb.add_leaf_element("virtual_functions","one_shot_functions","CFL_NULL","CFL_NULL","CFL_NULL","CFL_NULL",node_data)
        node_data = self.ctb.boolean_function_mapping
        self.ctb.add_leaf_element("virtual_functions","boolean_functions","CFL_NULL","CFL_NULL","CFL_NULL","CFL_NULL",node_data)
        node_data = self.ctb.s_one_shot_function_mapping
        self.ctb.add_leaf_element("virtual_functions","s_one_shot_functions","CFL_NULL","CFL_NULL","CFL_NULL","CFL_NULL",node_data)
        node_data = self.ctb.s_boolean_function_mapping
        self.ctb.add_leaf_element("virtual_functions","s_boolean_functions","CFL_NULL","CFL_NULL","CFL_NULL","CFL_NULL",node_data)
        node_data = self.ctb.s_main_function_mapping
        self.ctb.add_leaf_element("virtual_functions","s_main_functions","CFL_NULL","CFL_NULL","CFL_NULL","CFL_NULL",node_data)
        self.ctb.pop_node_element(function_node)
        
            
    def dump_micro_instructions(self):
       pass
        
        
        