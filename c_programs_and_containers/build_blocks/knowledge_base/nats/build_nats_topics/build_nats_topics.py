


from ..nats_dict.nats_dict import NatsDict
from ..nats.nats_key_store.nats_kb_store import NatsKbStore
class BuildNatsTopics:

    def __init__(self,server:str="nats://127.0.0.1:4222", bucket:str="node_topics", description:str="NAS Topics"):
        self.kb_store = NatsKbStore(server=server, bucket=bucket, description=description)
        self.nats_dict = NatsDict()
        self.path = [bucket]
        self.remove_all_nats_topics()
         
        
    def remove_all_nats_topics(self):
        keys = self.kb_store.list_kb_keys()
        
        for key in keys:
            self.kb_store.delete_sync(key)
        
       
    def list_nats_topics(self):
        print(self.kb_store.list_kb_keys())
        
    def build_nats_node_composite(self,label_name:str,node_name:str, label_data:dict, node_data:dict, metadata:dict,description:str = ""):
        
        label_dict = self.nats_dict.build_nats_label_dict(label_name,node_name,label_data,description)
        node_dict = self.nats_dict.build_nats_node_dict(label_name,node_name,node_data,metadata)
        if len(self.path) == 0:
            test_path = label_name+"."+node_name
        else:
            test_path = ".".join(self.path)+"."+label_name+"."+node_name
        
        if self.kb_store.exists_sync(test_path):
            raise ValueError(f"Node {node_name} with label {label_name} already exists in path {test_path}")
        key =self.kb_store.store_kb_key(base_topic=".".join(self.path), label_name=label_name, node_name=node_name, label_dict=label_dict, node_dict=node_dict, composite_node=True)
        
        self.path.extend([label_name,node_name])
        return key
    
   
    def build_nats_node_simple(self,label_name:str,node_name:str, label_data:dict, node_data:dict, metadata:dict,description:str = ""):
        
        path_join = ".".join(self.path)+"."+label_name+"."+node_name
        if self.kb_store.exists_sync(path_join):
            raise ValueError(f"Node {node_name} with label {label_name} already exists in path {path_join}")
        label_dict = self.nats_dict.build_nats_label_dict(label_name,node_name,label_data,description)
        node_dict = self.nats_dict.build_nats_node_dict(label_name,node_name,node_data,metadata)
        
        key = self.kb_store.store_kb_key(base_topic=".".join(self.path), label_name=label_name, node_name=node_name, label_dict=label_dict, node_dict=node_dict, composite_node=False)
        return key
  
        
    
    def pop_path(self,label_name:str,node_name:str):
        ref_node_name = self.path[-1]
        ref_label_name = self.path[-2]
        if ref_label_name != label_name:
            raise ValueError(f"Label {label_name} not found in path")
        if ref_node_name != node_name:
            raise ValueError(f"Node {node_name} not found in path")
        self.path = self.path[:-2]
        
 
        
    def get_path(self):
        return self.path
        
class TestNatsTopics:  
    def create_test_top_node(self,build_nats_topics):
        build_nats_topics.build_nats_node_composite("master_label","master_node",{"name":"master","description":"Master node"},{"ip":"192.168.1.66","node_name":"master"},{"created":"2024-01-15"})
    def create_child_node_1(self,build_nats_topics):
        build_nats_topics.build_nats_node_simple("child_1_label","child_1_node",{"name":"child_1","description":"Child node 1"},{"ip":"192.168.1.67","node_name":"child_1"},{"created":"2024-01-15"})
    def create_child_node_2(self,build_nats_topics):
        build_nats_topics.build_nats_node_simple("child_2_label","child_2_node",{"name":"child_2","description":"Child node 2"},{"ip":"192.168.1.68","node_name":"child_2"},{"created":"2024-01-15"})
    def pop_top_node(self,build_nats_topics):
        build_nats_topics.pop_path("master_label","master_node")

if __name__ == "__main__":
    build_nats_topics = BuildNatsTopics(server="nats://127.0.0.1:4222", bucket="xxx", description="NAS Topics")
    test_nats_topics = TestNatsTopics()
    test_nats_topics.create_test_top_node(build_nats_topics)
    test_nats_topics.create_child_node_1(build_nats_topics)
    test_nats_topics.create_child_node_2(build_nats_topics)
    test_nats_topics.pop_top_node(build_nats_topics)
    build_nats_topics.list_nats_topics()
    build_nats_topics.remove_all_nats_topics()
    build_nats_topics.kb_store.disconnect_sync()
    
