import json
from .bit_mask_operations import BitMaskOperations

class Construct_Bit_Mask_Store:
    def __init__(self, conn, construct_kb,upload_flag=False):
        self.bit_mask_operations = BitMaskOperations(conn)
        self.construct_kb = construct_kb 
        self.conn = conn
        self.upload_flag = upload_flag
        if self.upload_flag == False:
            self.bit_mask_operations.create_table()
        self.bit_mask_flags = {}
        
    def clear_flags(self):
        self.bit_mask_flags = {}
        
    def add_flag(self, flag_name, bit_position, flag_description):
        self.bit_mask_flags[flag_name] = {'bit': bit_position, 'description': flag_description}
        
    def create_bit_mask_entry(self,user_name, name, mask_size, bit_mask, description=""):
        if not isinstance(name, str):
            raise TypeError("name must be a string")
        if not isinstance(mask_size, int):
            raise TypeError("mask_size must be an integer")
        if not isinstance(bit_mask, int):
            raise TypeError("bit_mask must be an integer")
        if mask_size < 1 or mask_size > 64:
            raise ValueError("mask_size must be between 1 and 64")
        if bit_mask < 0 or bit_mask > 2**mask_size - 1:
            raise ValueError("bit_mask must be between 0 and 2**mask_size - 1")
        if mask_size != len(self.bit_mask_flags):
            raise ValueError("bit_mask size must be equal to the number of flags")
        temp_mask = {}
        for i in range(mask_size):
            temp_mask[i] = 0
        ### checking for dumplicate bits
        for flag_name, flag_data in self.bit_mask_flags.items():
            if temp_mask[flag_data['bit']] == 1:
                raise ValueError("flag already exists in bit_mask")
            temp_mask[flag_data['bit']] = 1
            
        label = "KB_BIT_MASK"
        ltree_node_name = '.'.join(self.construct_kb.path[self.construct_kb.working_kb])+"."+label+"."+name
        ltree_node_name = ltree_node_name.replace(".", "_").lower()
    
        self.bit_mask_operations.create_entry(ltree_node_name, bit_mask)
        node_properties = {
            "user_name": user_name,
            "mask_size": mask_size,
            "bit_mask": bit_mask,
            "flag_dictionary": json.dumps(self.bit_mask_flags),
            'record_id': ltree_node_name                   
        }
        self.construct_kb.add_info_node(label, name, node_properties, {}, description=description)