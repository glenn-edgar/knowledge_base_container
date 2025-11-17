from datetime import time
import yaml
from pathlib import Path
from .ct_functions.virtual_main_functions import VirtualMainFunctions
from .ct_functions.virtual_one_shot_functions import VirtualOneShotFunctions
from .ct_functions.virtual_boolean_functions import VirtualBooleanFunctions
from .ct_functions.virtual_s_one_shot_functions import VirtualSOneShotFunctions
from .ct_functions.virtual_s_boolean_functions import VirtualSBooleanFunctions
from .ct_functions.virtual_s_main_functions import VirtualSMainFunctions    
from .ct_functions.cfl_main_functions import CFLMainFunctions
from .ct_functions.cfl_one_shot_functions import CFLOneShotFunctions
from .ct_functions.cfl_boolean_functions import CFLBooleanFunctions
from .ct_functions.cfl_s_one_shot_functions import CFLSOneShotFunctions
from .ct_functions.cfl_s_boolean_functions import CFLSBooleanFunctions
from .ct_functions.cfl_s_main_functions import CFLSMainFunctions
from .ct_events.ct_timer import CT_Timer
from .ct_engine.ct_engine import CT_Engine
from .sequence_data.data_storeage import SequenceDataStorage
from .supervisor_failure_counter import SupervisorFailureCounter
from .data_flow.token_dictionary import TokenDictionary
from .template_functions import TemplateFunctions
from .sequence_data.exception_handler import ExceptionCatchHandler
from .s_functions.lisp_sequencer import LispSequencer

class ChainTreeRun():
    """
    This class is designed to run a chain tree.
    """
    @classmethod
    def get_root_node_id(cls,system_kb):
        return "kb." + system_kb + ".GATE_root._0"
    
    def __init__(self, wait_seconds,handle_dict = None):
        if handle_dict is None:
            handle_dict = {}
        self.handle_dict = handle_dict
        self.valid_return_codes = ["CFL_CONTINUE","CFL_HALT","CFL_TERMINATE","CFL_RESET","CFL_DISABLE",
                              "CFL_TERMINATE_SYSTEM","CFL_TEMPLATE_UNLOAD"]
        self.Vm = VirtualMainFunctions(self,self.valid_return_codes)
        self.Vo = VirtualOneShotFunctions(self,self.valid_return_codes)
        self.Vb = VirtualBooleanFunctions(self,self.valid_return_codes)
        self.Vso = VirtualSOneShotFunctions(self,self.valid_return_codes)
        self.Vsb = VirtualSBooleanFunctions(self,self.valid_return_codes)
        self.Vsm = VirtualSMainFunctions(self,self.valid_return_codes)
        self.cfl_one_shot_functions = CFLOneShotFunctions(self.Vo)
        self.cfl_s_one_shot_functions = CFLSOneShotFunctions(self.Vso)
        self.cfl_boolean_functions = CFLBooleanFunctions(self.Vb)
        self.cfl_s_boolean_functions = CFLSBooleanFunctions(self.Vsb)
        self.cfl_main_functions = CFLMainFunctions(self.Vm)
        self.cfl_s_main_functions = CFLSMainFunctions(self.Vsm)
           
        
        self.wait_seconds = wait_seconds
    
        
        self.python_dict = {}
       
        self.wait_seconds = wait_seconds
        self.subscribed_events = {}
        
        self.handle_dict["chain_tree"] = self 
        self.ct_engine = CT_Engine(self.handle_dict)
        self.token_dictionary = TokenDictionary()
        self.template_functions = TemplateFunctions(self,self.ct_engine)
        
        
        
    def load_system_functions(self):
        self.cfl_main_functions.load_default_main_functions()
        self.cfl_one_shot_functions.load_default_one_shot_functions()
        self.cfl_boolean_functions.load_default_boolean_functions()
        self.cfl_s_one_shot_functions.load_default_s_one_shot_functions()
        self.cfl_s_boolean_functions.load_default_s_boolean_functions()
        self.cfl_s_main_functions.load_default_s_main_functions()
             
   
        
    def load_chain_tree(self,yaml_file):
        self.yaml_file = yaml_file
        """Load the chain tree configuration from a YAML file."""
        try:
            # Ensure we have a Path object
            yaml_path = Path(self.yaml_file)

            # Open and load the YAML file
            with yaml_path.open('r', encoding='utf-8') as file:
                self.python_dict = yaml.safe_load(file) or {}

        except FileNotFoundError:
            raise FileNotFoundError(f"YAML file not found: {self.yaml_file}")

        except yaml.YAMLError as e:
            raise ValueError(f"Error parsing YAML file: {e}")
        self.load_system_functions()
        

    def verify_one_shot_functions(self):
        key = "virtual_functions.virtual_functions.virtual_functions.one_shot_functions"
        data = self.python_dict[key]
        self.required_one_shot_functions = data["node_dict"]
        
        self.Vo.detect_one_shot_coverage(self.required_one_shot_functions)
    
        
    
    def verify_boolean_functions(self):
        key = "virtual_functions.virtual_functions.virtual_functions.boolean_functions"
        data = self.python_dict[key]
        self.required_boolean_functions = data["node_dict"]
    
        self.Vb.detect_boolean_coverage(self.required_boolean_functions)
    
    
    def verify_main_functions(self):
        key = "virtual_functions.virtual_functions.virtual_functions.main_functions"
        data = self.python_dict[key]
        self.required_main_functions = data["node_dict"]
        
        self.Vm.detect_main_coverage(self.required_main_functions)
    
    
    
    def verify_s_one_shot_functions(self):
        key = "virtual_functions.virtual_functions.virtual_functions.s_one_shot_functions"
        data = self.python_dict[key]
        self.required_s_one_shot_functions = data["node_dict"]
        self.Vso.detect_s_one_shot_coverage(self.required_s_one_shot_functions)
    
    def verify_s_boolean_functions(self):
        key = "virtual_functions.virtual_functions.virtual_functions.s_boolean_functions"
        data = self.python_dict[key]
        self.required_s_boolean_functions = data["node_dict"]
        self.Vsb.detect_s_boolean_coverage(self.required_s_boolean_functions)
    
    def verify_s_main_functions(self):
        key = "virtual_functions.virtual_functions.virtual_functions.s_main_functions"
        data = self.python_dict[key]
        self.required_s_main_functions = data["node_dict"]
        self.Vsm.detect_s_main_coverage(self.required_s_main_functions)
         
    
  
    
   
        
    def debug_s_function(self, handle, message, node, event_id, event_data):
      timestamp = time.now().isoformat()
      print(f"[{timestamp}] DEBUG: {message}")
      print(f"  Node: {node}, Event: {event_id}")
      
    def run(self, starting_kb, start_node = None):
        self.verify_one_shot_functions()
        self.verify_boolean_functions()
        self.verify_main_functions()
        self.verify_s_one_shot_functions()
        self.verify_s_boolean_functions()
        self.verify_s_main_functions()
        if start_node is None:
            start_node = self.get_root_node_id(starting_kb)
        self.root_node_id = start_node
        self.system_kb = starting_kb
        self.ct_timer = CT_Timer(self.wait_seconds,start_node)
        
   
        
    
        self.s_lisp_engine = LispSequencer(self,self.ct_engine.run_s_expression,debug_function= self.debug_s_function)
        self.ct_engine.setup_initial_runtime_data_fields(start_node)
        self.ct_engine.reset_start_node(start_node)
        self.ct_engine.initialize_ct_walker()
        self.sequence_storage = SequenceDataStorage(self)
        self.sequence_storage.build_tree(start_node)
        self.exception_catch_storage = ExceptionCatchHandler(self,self.ct_engine)
        self.exception_catch_storage.build_tree(start_node)
        self.token_dictionary.reset_token_dictionary()
        self.template_functions = TemplateFunctions(self,self.ct_engine)
        self.template_functions.reset_instanciated_template_functions()
        self.running = True
        while self.running:
            mask = self.token_dictionary.get_current_event_mask()
            self.ct_timer.add_dict_dict("event_mask",mask)
            self.ct_timer.timer_tick(start_node)
            
            
            while self.ct_timer.event_queue_length() > 0:
                event = self.ct_timer.pop_event()
                #print("\n\n--------------------------------------- new event", event["node_id"], event["event_id"], event["event_data"])
            
            
                return_value = self.ct_engine.execute_event(event["node_id"],event["event_id"],event["event_data"])
                
                if return_value == False:
                    self.running = False
                    break
                
    def send_immediate_event(self,node_id,event_id,event_data):       
        
        node = self.python_dict[node_id]
        if node is None:
            raise ValueError(f"Node {node_id} not found")
        ct_control = node["ct_control"]
        if ct_control is None:
            raise ValueError(f"ct_control not found for node {node_id}")
        if ct_control["enabled"] == False:
            return
        if ct_control["initialized"] == False:
            return
        self.ct_timer.add_immediate_event(node_id,event_id,event_data)
        
    #def find_one_shot_function(self,function_name):
    #    function_code = self.virtual_functions.find_one_shot_function(function_name)
    #   return function_code
    
    def send_system_event(self,event_id,event_data):
        self.ct_timer.add_event(self.root_node_id,event_id,event_data)
        
    def send_system_named_event(self,node_id,event_id,event_data):
        node = self.python_dict[node_id]
        if node is None:
            raise ValueError(f"Node {node_id} not found")
        ct_control = node["ct_control"]
        if ct_control is None:
            raise ValueError(f"ct_control not found for node {node_id}")
        if ct_control["enabled"] == False:
            return
        if ct_control["initialized"] == False:
            return  
        self.ct_timer.add_event(node_id,event_id,event_data)
        
    def subscribe_events(self,node_id,event_list):
        for event_id in event_list:
        
            if event_id not in self.subscribed_events:
                self.subscribed_events[event_id] = {}
        
            self.subscribed_events[event_id][node_id] = True
            
    def unsubscribe_events(self,node_id,event_list):
        for event_id in event_list:
            if node_id not in self.subscribed_events[event_id]:
                continue
            del self.subscribed_events[event_id][node_id]
        
    def publish_event(self,event_id,event_data):
        if event_id not in self.subscribed_events:
            return
        for event in self.subscribed_events[event_id]:
            self.send_system_named_event(event,event_id,event_data)
            
        
    
    
    
    def find_state_number(self,state_name,state_names):
        if state_name not in state_names:
            raise ValueError(f"State {state_name} not found in state names {state_names}")
        return state_names.index(state_name)
    
    
    def create_supervisor_failure_counter(self,max_failures,time_window_seconds):
        return SupervisorFailureCounter(max_failures,time_window_seconds)
    
######################### DATA FLOW FUNCTIONS #########################
    