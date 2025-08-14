from knowledge_base.construct_kb.construct_data_tables import Construct_Data_Tables
from typing import Dict, Any

class Construct_Utilities:	
  def __init__(self,kb_constructor:Construct_Data_Tables):
        self.kb_constructor = kb_constructor
        self.no_run = ""
        self.temp_run = "./run.bsh"
        self.managed_run = "./process_control.bsh"
        self.defined_kb = {}
        self.working_kb = None
        self.system_name = None
        self.system_flag = False
        self.site_name = None
        self.site_flag = False
        self.node_name = None
        self.node_flag = False
        self.container_flag = False
        self.active_container = None

       

  
  def define_kb(self,kb_name:str,description:str):
        if self.kb_constructor.kb_exists(kb_name):
            raise ValueError(f"Knowledge base {kb_name} already exists")
        self.kb_constructor.add_kb(kb_name,description)
        self.defined_kb[kb_name] = description
        
  def select_kb(self,kb_name:str):
        if self.kb_constructor.kb_exists(kb_name):
            self.kb_constructor.select_kb(kb_name)
            self.working_kb = kb_name
        else:
            raise ValueError(f"Knowledge base {kb_name} does not exist")
          
    
    
  def construct_system(self,kb_name:str,system_name:str,system_properties:Dict[str,Any],system_description:str):
        if self.system_flag == True:
            raise ValueError("In process of being contructed ")
        self.system_flag = True
        self.select_kb(kb_name)
        self.system_name = system_name
        kb_description = self.defined_kb[kb_name]
        self.kb_constructor.add_header_node("KB",kb_name,{},{},kb_description)
        self.kb_constructor.add_header_node("SYSTEM",system_name,{},system_properties,system_description)
        
        
    

  def end_system(self):
        if self.system_flag == False:
            raise ValueError("Not in process of being contructed ")
        self.system_flag = False
        self.kb_constructor.leave_header_node("SYSTEM",self.system_name)
        self.kb_constructor.leave_header_node("KB",self.working_kb)
        self.working_kb = None
        self.system_name = None
        self.kb_constructor.check_installation()
  
  
  def construct_site(self,site_name:str,site_properties:Dict[str,Any],site_description:str):
        if self.site_flag == True:
            raise ValueError("In process of being contructed ")
        self.site_flag = True
        self.site_name = site_name
        self.kb_constructor.add_header_node("SITE",site_name,{},site_properties,site_description)
        
  def end_site(self):
        if self.site_flag == False:
            raise ValueError("Not in process of being contructed ")
        self.site_flag = False
        self.kb_constructor.end_header_node("SITE",self.site_name)
        self.site_name = None
        
  def construct_node(self,node_name:str,node_properties:Dict[str,Any],node_description:str):
        if self.node_flag == True:
            raise ValueError("In process of being contructed ")
        self.node_flag = True
        self.node_name = node_name
        self.kb_constructor.add_header_node("NODE",node_name,{},node_properties,node_description)
        
  def end_node(self):
        if self.node_flag == False:
            raise ValueError("Not in process of being contructed ")
        self.node_flag = False

        self.kb_constructor.end_header_node("NODE",self.node_name)
        self.node_name = None
        
  def construct_container(self, container_name:str,command_map:Dict[str,Any],container_properties:Dict[str,Any],container_description:str):
    if self.container_flag == True:
      raise ValueError("In process of being contructed ")
    self.container_flag = True
    self.active_container = container_name
    container_properties["command_map"] = command_map
    self.kb_constructor.add_header_node("CONTAINER",container_name,{},container_properties,container_description)
   
       
  def end_container(self):
    if self.container_flag == False:
      raise ValueError("Not in process of being contructed ")
    self.container_flag = False
    self.active_container = None
    self.kb_constructor.end_header_node("CONTAINER",self.active_container)
    self.active_container = None
  
    
    

