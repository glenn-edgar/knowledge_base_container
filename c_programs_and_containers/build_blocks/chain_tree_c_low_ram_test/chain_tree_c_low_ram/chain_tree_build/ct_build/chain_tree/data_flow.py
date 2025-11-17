from .column_flow import ColumnFlow

#
# Making a class if more data flow column types are needed
#
#

class DataFlow(ColumnFlow):
    def __init__(self, ctb):
        self.ctb = ctb
        ColumnFlow.__init__(self, ctb)
        
        
        
    def define_data_flow_event_mask(self, column_name:str,  aux_function:str, 
                                    user_data:dict = {},
                                    event_list:list[str] = [], auto_start:bool = False):
        
        bit_mask_event_list = []
        for event in event_list:
            bit_mask_event_list.append(self.ctb.register_bitmask(event))
        bit_mask = 0
        
        for event in bit_mask_event_list:
            bit_mask |= 1 << event
        user_data["bit_mask"] = bit_mask
        
        self.ctb.add_boolean_function(aux_function)

        return self.define_column(column_name,main_function = "CFL_DF_MASK_MAIN",initialization_function = "CFL_DF_MASK_INIT",
               termination_function = "CFL_DF_MASK_TERM", aux_function = aux_function, 
               column_data = {"bit_mask": bit_mask}, auto_start = auto_start,label="CFL_DF_MASK")

   
    def asm_set_bitmask(self,event_list :list):
        if not isinstance(event_list, list):
            raise TypeError("Event list must be a list")
        bit_position_list = []
        for event in event_list:
            bit_position = self.ctb.register_bitmask(event)
            bit_position_list.append(bit_position)
        bit_mask = 0
        for bit_position in bit_position_list:
            bit_mask |= 1 << bit_position
        
        bitmask_data = {"bit_mask": bit_mask}
        self.asm_one_shot_handler("CFL_SET_BITMASK",bitmask_data)
    
    def asm_clear_bitmask(self,event_list :list):
        if not isinstance(event_list, list):
            raise TypeError("Event list must be a list")
        bit_position_list = []
        for event in event_list:
            bit_position = self.ctb.register_bitmask(event)
            bit_position_list.append(bit_position)
        bit_mask = 0
        for bit_position in bit_position_list:
            bit_mask |= 1 << bit_position
        bitmask_data = {"bit_mask": bit_mask}
        self.asm_one_shot_handler("CFL_CLEAR_BITMASK",bitmask_data)