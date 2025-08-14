
from datetime import datetime
from asm_support_functions import Support_Functions
from cf_events import Event_id_dict
from memory_kb.construct_mem_db import Behavior_Control_Dict

class Behavior_Tree(Support_Functions):
    def __init__(self,cf):
        self.cf = cf
        self.support_functions = Support_Functions(cf)
        self.event_id_dict = Event_id_dict()
        
    def exec_init_bt(self, selector_data ):
        if selector_data["root_node"] == True:
            bt_control = Behavior_Control_Dict(self.cf)
            if  hasattr(self.cf,"bt_dict") == False:
                self.cf.bt_dict = bt_control
                self.cf.dict.exec_path = ["root_node"]
            else:
                self.cf.bt_dict[selector_data["chain_name"]] = bt_control
        else:
            if  hasattr(self.cf,"bt_dict") == False:
                raise Exception("bt_dict not found")
            if selector_data["chain_name"] not in self.cf.bt_dict:
                raise Exception("chain_name not found in bt_dict")
           
            bt_control = self.cf.bt_dict[selector_data["chain_name"]]
            if bt_control is None:
                raise Exception("bt_control not found in bt_dict")
            data = self.cf.get_column_data(selector_data["chain_name"])
            bt_control = data["bt"]

        self.cf.set_column_data(selector_data["chain_name"],{"bt":bt_control})
        column_data = {}
        column_data["bt_control"] = bt_control
        column_data["chain_name"] = selector_data["chain_name"]
        selector_data["init_function"](selector_data,column_data)
        bt_control.add_composite_element(selector_data["chain_name"],column_data)
        
        for chain_name in selector_data["chain_list"]:
            data = {}
            data.status = False
            data.active = False
            data.output_data = {}
            data.input_data = {}
            self.bt.add_composite_element(chain_name,data)
            
            link = self.bt.chain_data_link[chain_name]
            self.bt.leave_composite_element()
            self.cf.set_column_data(chain_name,
                                    {"parent_name":selector_data["chain_name"],"chain_name":chain_name,
                                     "link":link,"data":data,"bt":bt_control})
            if selector_data["init_function"] is not None:
                selector_data["init_function"](selector_data,data)
            
             
        for chain_name in selector_data["error_list"]:
            data = {}
            data.status = False
            data.active = False
            data.output_data = {}
            data.input_data = {}
            self.bt.add_composite_element(chain_name,data)
           
            link = self.bt.chain_data_link[chain_name]
            self.bt.leave_composite_element()
            self.cf.set_column_data(chain_name,
                                    {"parent_name":selector_data["chain_name"],"chain_name":chain_name,
                                     "link":link,"data":data,"bt":bt_control})
            if selector_data["init_function"] is not None:
                selector_data["init_function"](selector_data,data)
            
 
            
        data = self.cf.get_column_data(selector_data["chain_name"])
        data["index"] = 0
        data["error_mode"] = False
        self.cf.set_column_data(selector_data["chain_name"], data)
        
        self.cf.enable_chain(selector_data["chain_list"][0])
        self.cf.dict.exec_path.append(selector_data["chain_list"][0])
     
    def exec_process_bt(self, selector_data , event):
        if event.event_id == 'CF_TIMER_EVENT':
            if selector_data["error_mode"] == False:
                chain_name = selector_data["chain_list"][selector_data["index"]]
                if  self.cf.is_chain_active(chain_name) == True:
                    return "CF_HALT"
                else:
                    column_data = self.cf.get_column_data(chain_name)
                    status = selector_data["status"]
                    if ( column_data["status"] == status) and (column_data["enable"] == False):
                            
                            parent_name = selector_data["chain_name"]
                            parent_dict = self.cf.bt_dict[parent_name]
                            parent_dict.abort_flag = False
                            parent_dict.status = True
                            selector_data["finalize_function"](selector_data,parent_dict)
                            data = self.cf.get_column_data(chain_name)
                            data["status"] = True
                            data["final_link"] = self.bt.chain_data_link[chain_name]
                            self.cf.set_column_data(chain_name,data)
                            return "CF_DISABLE"
                        
                        
                    else:
                        selector_data["index"] = selector_data["index"] + 1
                        if selector_data["index"] < len(selector_data["chain_list"]):
                            self.cf.enable_chain(selector_data["chain_list"][selector_data["index"]])
                            self.cf.dict.exec_path.append(selector_data["chain_list"][selector_data["index"]])
                            
                        else:
                            selector_data["error_mode"] = True
                            selector_data["index"] = 0
                            self.cf.enable_chain(selector_data["error_list"][0])
                            return "CF_HALT"
            else:
                chain_name = selector_data["error_list"][selector_data["index"]]
                if  self.cf.is_chain_active(chain_name) == True:
                    return "CF_HALT"
                else:
                    column_data = self.cf.get_column_data(chain_name)
                    if (column_data["status"] == True) and (column_data["enable"] == False):
                        parent_name = selector_data["chain_name"]
                        parent_dict = self.cf.bt_dict[parent_name]
                        selector_data["finalize_function"](selector_data,parent_dict)
                        return "CF_DISABLE"
                    else:
                        selector_data["index"] = selector_data["index"] + 1
                        if selector_data["index"] < len(selector_data["error_list"]):
                            self.cf.enable_chain(selector_data["error_list"][selector_data["index"]])
                            self.cf.dict.exec_path.append(selector_data["error_list"][selector_data["index"]])
                        else:
                            parent_name = selector_data["chain_name"]
                            parent_dict = self.cf.bt_dict[parent_name]
                            parent_dict.abort_flag = True
                            selector_data["finalize_function"](selector_data,parent_dict)
                            
                            data = self.cf.get_column_data(chain_name)
                            data["status"] = False
                            data["final_link"] = self.bt.chain_data_link[chain_name]
                            self.cf.set_column_data(chain_name,data)
                            return "CF_DISABLE"
                        
                        
    def exec_finalize_bt(self,selector_data,parent_dict,chain_lis):
        self.disable_chain(selector_data["chain_name"])
        self.disable_chain(selector_data["error_list"])
        parent_name = selector_data["chain_name"]
        parent_dict = self.cf.bt_dict[parent_name]
        parent_dict.abort_flag = True
        self.cf.bt_dict[parent_name] = parent_dict
        
        
    def asm_selector_node(self,chain_name,init_function,init_data,finalize_function,finalize_data,chain_list,error_list,root_node = False,name = None):
        selector_data = {}
        selector_data["status"] = True
        selector_data["chain_name"] = [chain_name]
        selector_data["chain_list"] = chain_list
        selector_data["error_list"] = error_list
        selector_data["root_node"] = root_node
        selector_data["finalize_function"] = finalize_function
        selector_data["finalize_data"] = finalize_data
        selector_data["init_function"] = init_function
        self.cf.add_element(process_function=self.exec_process_bt,
                            initialization_function=self.exec_init_bt,
                            termination_function=self.exec_finalize_bt,
                            data=selector_data, name=name)
    
   
   
           
    def asm_selector_node(self,init_function,init_data,finalize_function,finalize_data,chain_list,error_list,root_node = False,name = None):
        status = True
        self.asm_composite_node(status,init_function,init_data,finalize_function,finalize_data,chain_list,error_list,root_node = False,name = None)
        
    def asm_sequence_node(self,init_function,init_data,finalize_function,finalize_data,chain_list,error_list,root_node = False,name = None):
        status = False
        self.asm_composite_node(status,init_function,init_data,finalize_function,finalize_data,chain_list,error_list,root_node = False,name = None)
               
        
    def asm_composite_node(self,status,init_function,init_data,finalize_function,finalize_data,chain_list,error_list,root_node = False,name = None):    
        chain_name = self.cf._current_chain
        if type(chain_name) != str:
            raise Exception("chain_name must be a string")
        if type(chain_list) != list:
            raise Exception("chain_list must be a list")
        if type(error_list) != list:
            raise Exception("error_list must be a list")
        if type(root_node) != bool: 
            raise Exception("root_node must be a boolean")
        if type(name) != str:
            raise Exception("name must be a string")
        if type(init_function) != function:
            raise Exception("init_function must be a function")
        if type(finalize_function) != function:
            raise Exception("finalize_function must be a function")
        selector_data = {}
        selector_data["status"] = False
        selector_data["chain_name"] = [chain_name]
        selector_data["chain_list"] = chain_list
        selector_data["error_list"] = error_list
        selector_data["root_node"] = root_node
        selector_data["finalize_function"] = finalize_function
        selector_data["finalize_data"] = finalize_data
        selector_data["init_function"] = init_function
        selector_data["init_data"] = init_data
        self.cf.add_element(process_function=self.exec_process_sequence,
                            initialization_function=self.exec_init_sequence,
                            termination_function= self.exec_finalize_sequence,
                            data=selector_data, name=name)
  
    def asm_parallel_node(self,chain_list,bt_data_fn, join_function= None,match_list = None, number_of_nodes=None, error_list = None, root_node = False,name = None):
       self.cf.asm_construct_composite_node(init_function,chain_list,root_node)
       self.cf.asm_enable_disable_chains(chain_list)
       self.cf.asm_join_chains(chain_list,join_function=join_function,match_list = match_list,number_of_chains = number_of_nodes,name = name)
       self.asm_examine_bt_results(chain_list,bt_data_fn,error_list,root_node,name)
       
       
        
        
        
        
   
   
   
    """
      This function is called when the chain is done or externally terminated
    """
    
    def exec_finalize_leaf(self,leaf_data):
        
        if leaf_data["finalize_function"] is not None:
            leaf_data["finalize_function"](leaf_data)
        
        
    def exec_init_leaf(self,leaf_data,data):
        data = self.cf.get_column_data(leaf_data["chain_name"])
        if data is None:
            raise Exception("column data not found")
        if data["parent_name"] is None:
            raise Exception("parent_name not found")
        if data["link"] is None:
            raise Exception("bt link not found")
        if data["bt"] is None:
            raise Exception("bt dictnot found")
        
        
        
        data.status = False
        data.enable = True
        data.output_data = {}
        data.input_data = {}
        data.abort_flag = True
        
        self.cf.set_column_data(leaf_data["chain_name"],data)
        
        if leaf_data["init_function"] is not None:
            leaf_data["init_function"](leaf_data)
        
        
        
        
    def asm_leaf_node_start_stop_element(self,selector_data,init_function,init_data,finalize_function,finalize_data,name = None):
        self.cf.add_element(
                            initialization_function=self.exec_init_leaf,
                            termination_function= self.exec_finalize_leaf,
                            data=selector_data, name=name)
        pass
  
    def exec_action_init_leaf(self,leaf_data):
        data = self.cf.get_column_data(leaf_data["chain_name"])
        if data is None:
            raise Exception("column data not found")
        if data["parent_name"] is None:
            raise Exception("parent_name not found")
        if data["link"] is None:
            raise Exception("bt link not found")
        if data["bt"] is None:
            raise Exception("bt dict not found")
        if data["data"]["status"] is None:
            raise Exception("status not found")
        if data["data"]["enable"] is None:
            raise Exception("enable not found")
        if data["data"]["output_data"] is None:
            raise Exception("output_data not found")
        if data["data"]["input_data"] is None:
            raise Exception("input_data not found")
        if data["data"]["abort_flag"] is None:
            raise Exception("abort_flag not found")
        if leaf_data["init_function"] is not None:
            leaf_data["init_function"](leaf_data)
        
        
    def exec_action_process_leaf(self,leaf_data,event):
        data = self.cf.get_column_data(leaf_data["chain_name"])
        return_code = leaf_data["process_function"](leaf_data,event)
        if return_code == "CF_RESET":
            raise Exception("CF_RESET not allowed")
        if return_code
  
    def asm_leaf_action_element(self,selector_data,init_function,init_data,finalize_function,finalize_data,name = None):
        self.cf.add_element(process_function=self.exec_action_process_leaf,
                            initialization_function=self.exec_action_init_leaf,
                            termination_function= self.exec_action_finalize_leaf,
                            data=selector_data, name=name)
        pass
  
