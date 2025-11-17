from .column_flow import ColumnFlow
import uuid

class Templates(ColumnFlow):
    def __init__(self, ds, ctb,template_kb_name="T"):
        ColumnFlow.__init__(self, ds, ctb)
        self.ds = ds
        self.ctb = ctb
        self.template_kb_name = template_kb_name
        self.template_active = False
        self.template_initialized = False
        
    def init_template_kb(self):
        if self.template_initialized:
            raise ValueError("Template kb already initialized")
        temp = self.ds.get_working_kb()
        self.ds.add_kb(self.template_kb_name)
        self.ds.select_kb(self.template_kb_name)
        self.template_dict = {}
        self.instanciated_template_dict = {} 
        ltree_name = "kb." + self.template_kb_name 
        self.ds.yaml_data[ltree_name] = {
            "label": "kb",
            "node_name": self.template_kb_name,
            "label_dict": {},
            "node_dict": self.template_dict
        }
        if temp != None:
            self.ds.select_kb(temp)
        self.template_active = False
        self.template_initialized = True
        
    def start_template(self, template_name:str):
        self.sequence_dict = {}
        self.ref_kb = self.ds.get_working_kb()
        #print("Starting template",template_name,self.ref_kb)
        if not self.template_initialized:
            raise ValueError("Template kb not initialized")
        if not isinstance(template_name, str):
            raise TypeError("Template name must be a string")
        if template_name in self.template_dict:
            raise ValueError(f"Template {template_name} already exists")
        self.ref_path = self.ds.get_current_path()
        self.ds.select_kb(self.template_kb_name)
        self.ds.set_path_list(["kb",self.template_kb_name])
        
        if self.template_active:
            raise ValueError("Template defined while another template is active")
        self.template_active = True
        self.ds.set_path_list(["kb",self.template_kb_name])
        
        self.ts_start_node = self.define_gate_node("ts_"+template_name,column_data={},links_flag = False)
        self.template_dict[template_name] = self.ts_start_node
        return self.ts_start_node
        
    def end_template(self):
        if not self.template_active:
            raise ValueError("Template not active")
        self.template_active = False
        self.end_column(self.ts_start_node)
        #print("Ending template",self.ts_start_node,self.ref_kb)
        self.ds.set_path_list(self.ref_path)
        #print("setting path list",self.ref_kb)
        if self.ref_kb is not None:
            self.ds.select_kb(self.ref_kb)
    
    
    def generate_instanciated_template_name(self):
          return uuid.uuid4()
    
    
    def finalize_template_kb(self):
        temp = self.ds.get_working_kb()
        
        self.ds.select_kb(self.template_kb_name)
        self.ds.set_path_list(["kb",self.template_kb_name])
        self.ds.leave_kb()
        if temp != self.template_kb_name:
            self.ds.select_kb(temp)
        else:
            raise ValueError("Template kb name is not the same as the working kb")
     
        
    def asm_use_variable_templates(self,aux_function_name:str="CFL_NULL",user_data:dict={},
                                   load_function_name:str="CFL_NULL",load_function_data:dict={},
                                   finalize_function_name:str="CFL_NULL",finalize_function_data:dict={}):
    
        if not isinstance(aux_function_name, str):
            raise TypeError("Aux function name must be a string")
        if not isinstance(load_function_name, str):
            raise TypeError("Load function name must be a string")
        if not isinstance(finalize_function_name, str):
            raise TypeError("Finalize function name must be a string")
        if not isinstance(user_data, dict):
            raise TypeError("User data must be a dictionary")
        if not isinstance(load_function_data, dict):
            raise TypeError("Load function data must be a dictionary")
        if not isinstance(finalize_function_data, dict):
            raise TypeError("Finalize function data must be a dictionary")
        
        node_data = {"finalize_function_name":finalize_function_name,"finalize_function_data":finalize_function_data,
                    "user_data":user_data,"load_function_name":load_function_name,"load_function_data":load_function_data}
        
        self.ctb.one_shot_function_mapping[finalize_function_name] = True
        self.ctb.one_shot_function_mapping[load_function_name] = True
        node_id = self.define_column_link( main_function_name="CFL_USE_TEMPLATE_MAIN", initialization_function_name="CFL_VARIABLE_TEMPLATE_INIT",
                                            aux_function_name=aux_function_name , termination_function_name="CFL_VARIABLE_TEMPLATE_TERM", 
                                            node_data=node_data, label="USE_VARIABLE_TEMPLATES" )
        return node_id
   
   
    def asm_use_templates(self,template_list:list[str,dict],aux_function_name:str="CFL_NULL",user_data:dict={},
                          finalize_function_name:str="CFL_NULL",finalize_function_data:dict={}):
        # second element is a dictionary of initial conditions for the template        
        for items in template_list:
            if not isinstance(items[0], str):
                raise TypeError("Each item in template list must be a string")
            if items[0] not in self.template_dict:
                raise ValueError(f"template {items[0]} not found")
            if len(items) < 2:
                items[2] = {}
        node_data = {"template_list":template_list,"finalize_function_name":finalize_function_name,"finalize_function_data":finalize_function_data,
                    "user_data":user_data}
        self.ctb.one_shot_function_mapping[finalize_function_name] = True
        
        

        node_id = self.define_column_link( main_function_name="CFL_USE_TEMPLATE_MAIN", initialization_function_name="CFL_USE_TEMPLATE_INIT",
                                            aux_function_name=aux_function_name , termination_function_name="CFL_USE_TEMPLATE_TERM", 
                                            node_data=node_data, label="USE_TEMPLATES" )
        return node_id
   