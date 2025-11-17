
import uuid

class NatsDict:
    def __init__(self):
        pass

    def build_nats_label_dict(self,label_name:str,node_name:str, data:dict, description = ""):
        return_value = {}
        if isinstance(label_name,str) == False:
            raise ValueError("label_name must be a string")
        if isinstance(node_name,str) == False:
            raise ValueError("node_name must be a string")
        if isinstance(data,dict) == False:
            raise ValueError("data must be a dictionary")
        if isinstance(description,str) == False:
            raise ValueError("description must be a string")
        return_value["type"] = "entity"
        return_value["description"] = description
        return_value["label_name"] = label_name
        return_value["node_name"] = node_name
        return_value["data"] = data
        return return_value

    def build_nats_node_dict(self, label_name:str,node_name:str, data:dict, metadata:dict):
        if isinstance(label_name,str) == False:
            raise ValueError("label_name must be a string")
        if isinstance(node_name,str) == False:
            raise ValueError("node_name must be a string")
        if isinstance(data,dict) == False:
            raise ValueError("data must be a dictionary")
        if isinstance(metadata,dict) == False:
            raise ValueError("metadata must be a dictionary")
        return_value = {}
        return_value["id"] = str(uuid.uuid4())
        return_value["label_name"] = label_name
        return_value["node_name"] = node_name
        return_value["data"] = data
        return_value["metadata"] = metadata
        return return_value

    def update_data(self, input_dict:dict, data:dict):
        if isinstance(input_dict,dict) == False:
            raise ValueError("input_dict must be a dictionary")
        if isinstance(data,dict) == False:
            raise ValueError("data must be a dictionary")
        if "data" not in input_dict:
            raise ValueError("data not found in input_dict")
        input_dict["data"] = data
        return input_dict

    def update_metadata(self, input_dict:dict, metadata:dict):
        if isinstance(input_dict,dict) == False:
            raise ValueError("input_dict must be a dictionary")
        if isinstance(metadata,dict) == False:
            raise ValueError("metadata must be a dictionary")
        if "metadata" not in input_dict:
            raise ValueError("metadata not found in input_dict")
        input_dict["metadata"] = metadata
        return input_dict

if __name__ == "__main__":
    nats_dict = NatsDict()
    label_dict = nats_dict.build_nats_label_dict("person","alice_johnson",{"name":"Alice Johnson","age":30},"Person entity with attributes")
    node_dict = nats_dict.build_nats_node_dict("person","alice_johnson",{"name":"Alice Johnson","age":30},{"created":"2024-01-15","confidence":0.95})
    print(label_dict)
    print(node_dict)
    print(nats_dict.update_data(label_dict,{"name":"new name","age":30}))
    print(nats_dict.update_metadata(node_dict,{"created":"2024-01-15","confidence":0.85}))
    