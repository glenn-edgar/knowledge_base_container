from .filtered_tree_builder import FilteredTreeBuilder

class SequenceDataStorage:
    def __init__(self,chain_tree):
        self.chain_tree = chain_tree
        self.sequence_data = {}
        
    
    def get_sequence_data(self):
        return self.sequence_data
    
    def get_sequence_element(self,node_name):
        if node_name not in self.sequence_data:
            raise ValueError(f"Node {node_name} not found in sequence data")
        return self.sequence_data[node_name]
    
    def build_tree(self,starting_node):
    
        
        self.filtered_tree_builder = FilteredTreeBuilder(self.chain_tree.python_dict,self.filter_func,self.links_func,self.node_constructor)
        self.sequence_data = self.filtered_tree_builder.construct_tree(starting_node)
        return self.sequence_data
        
    def check_node_exists(self,node):
        if node["label_dict"]["ltree_name"] not in self.sequence_data:
            raise ValueError(f"Node {node['node_id']} not found in sequence data")
        return True
    
    def filter_func(self,node):
        
        label_dict = node["label_dict"]
        
            
            
        
        if label_dict["main_function_name"] in ["CFL_SEQUENCE_PASS_MAIN", "CFL_SEQUENCE_FAIL_MAIN", "CFL_SEQUENCE_START_MAIN",
                                                "CFL_SUPERVISOR_MAIN"]:
            
            return True
        else:
            return False
        
    def links_func(self,node):
        if "label_dict" not in node:
            raise ValueError(f"label_dict not found for node {node['node_id']}")
        if "links" not in node["label_dict"]:
            return []
        links = node["label_dict"]["links"]
        return links
        
    def node_constructor(self,node):
        return_node = {}
        return_node["node_id"] = node["label_dict"]["ltree_name"]
        return_node["processed"] = False
        return_node["results"] = []
        return_node["overal_status"] = False
        return_node["failed_data"] = {}
        return_node["results_length"] = 0
        return_node["failed_element"] = 0  
        return_node["finalized_results"] = {}
        return return_node

    def register_node(self,node_id:str):
        if node_id not in self.sequence_data:
            raise ValueError(f"Node {node_id} not found in sequence data")
        #self.sequence_data[node_id]["processed"] = True
        self.sequence_data[node_id]["processed"] = False
        
    def append_results(self,node_id:str,element_id:str,status:bool,data):
        if node_id not in self.sequence_data:
            raise ValueError(f"Node {node_id} not found in sequence data")
        self.sequence_data[node_id]["processed"] = True
        self.sequence_data[node_id]["results"].append({"element_id":element_id,"status":status,"data":data})
        self.sequence_data[node_id]["results_length"] = len(self.sequence_data[node_id]["results"])
        
    def get_results_length(self,node_id:str):
        if node_id not in self.sequence_data:
            raise ValueError(f"Node {node_id} not found in sequence data")
        return self.sequence_data[node_id]["results_length"]
    
    def get_last_result(self,node_id:str):
        if node_id not in self.sequence_data:
            raise ValueError(f"Node {node_id} not found in sequence data")
        return self.sequence_data[node_id]["results"][-1]
        
    
    
    def get_overall_status(self,node_id:str):
        if node_id not in self.sequence_data:
            raise ValueError(f"Node {node_id} not found in sequence data")
        return self.sequence_data[node_id]["overal_status"]
        
    def set_overall_status(self,node_id:str,status:bool,failed_element:int,finalized_results:dict):
    
        if node_id not in self.sequence_data:
            raise ValueError(f"Node {node_id} not found in sequence data")
        self.sequence_data[node_id]["overal_status"] = status
        self.sequence_data[node_id]["failed_element"] = failed_element
        self.sequence_data[node_id]["finalized_results"] = finalized_results
    
    def modify_index_results(self,node_id:str,index:int,result):
        if node_id not in self.sequence_data:
            raise ValueError(f"Node {node_id} not found in sequence data")
        self.sequence_data[node_id]["results"][index] = result
    
    def get_index_result(self,node_id:str,index:int):
        if node_id not in self.sequence_data:
            raise ValueError(f"Node {node_id} not found in sequence data")
        return self.sequence_data[node_id]["results"][index]
    
    def get_finalized_results(self,node_id:str):
        if node_id not in self.sequence_data:
            raise ValueError(f"Node {node_id} not found in sequence data")
        return self.sequence_data[node_id]["finalized_results"]
    
    def set_processed(self,node_id:str):
        if node_id not in self.sequence_data:
            raise ValueError(f"Node {node_id} not found in sequence data")
        self.sequence_data[node_id]["processed"] = True
    
    def get_topological_sort(self):
        return self.filtered_tree_builder.topological_sort(self.sequence_data)
    
    def get_processed_topological_sort(self,sequence_data,sort_filter_func):
        return self.filtered_tree_builder.filtered_topological_sort(sequence_data,self.sort_filter_func)
    
    def sort_filter_func(self,node_id):
        return self.sequence_data[node_id]["processed"]
    
    def get_missing_topological_sort(self,sequence_data,sort_filter_func):
        return self.filtered_tree_builder.filtered_topological_sort(sequence_data,self.sort_filter_missing_func)
        
    def sort_filter_missing_func(self,node_id):
        return not self.sort_filter_func(node_id)
    
    def reset_from_node(self,start_node,reset_function):
        return self.filtered_tree_builder.reset_from_node(self.sequence_data, start_node, self.reset_node)
    
    def reset_node(self,node_id):
        node = self.sequence_data[node_id]
        print("reset_node",node)
        node["processed"] = False
        node["results"] = []
        node["failed_data"] = {}
        node["overal_status"] = False
        node["resluts_length"] = 0
        node["failed_element"] = 0
        node["finalized_results"] = {}
        return node
    
    def collect_to_json(self,start_node,filter_func):
        return self.filtered_tree_builder.collect_to_json(self.sequence_data,start_node,filter_func)
    
    def collect_to_list(self,start_node,filter_func):   
        return self.filtered_tree_builder.collect_to_list(self.sequence_data,start_node,filter_func)