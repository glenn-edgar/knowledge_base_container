from .column_flow import ColumnFlow

class Streaming(ColumnFlow):
    def __init__(self,ctb):
        ColumnFlow.__init__(self,ctb)
        self.ctb = ctb
        
    def make_port(self,file_name:str,handler_id):
        if not isinstance(file_name, str):
            raise TypeError("File name must be a string")
        if not isinstance(handler_id, int):
            raise TypeError("Handler id must be an integer")
        port_data = {"file_name": file_name, "handler_id": handler_id}
        return port_data
    
    
    def asm_streaming_emit_packet(self,aux_function:str,aux_function_data:dict,event_column:str,streaming_event_name:str,outport:dict):
        event_column_id = self.ctb.get_node_index(event_column)
        event_id = self.ctb.register_event(streaming_event_name)
        node_data = {"aux_data": aux_function_data, "event_id": event_id, "outport": outport, "event_column": event_column_id}
        self.asm_one_shot_handler(aux_function, node_data)
        
        
    def asm_streaming_sink_packet(self,aux_function:str,aux_function_data:dict,streaming_event_name:str,inport:dict):
       
        event_id = self.ctb.register_event(streaming_event_name)
        node_data = {"aux_data": aux_function_data, "event_id": event_id, "inport": inport}
        self.define_column_link(main_function_name="CFL_STREAMING_SINK_PACKET", initialization_function_name="CFL_STREAMING_SINK_PACKET_INIT", 
                                       aux_function_name=aux_function, termination_function_name="CFL_STREAMING_SINK_PACKET_TERM", node_data=node_data)
        
        
    def asm_streaming_transform_packet(self,aux_function:str,aux_function_data:dict,streaming_input_event_name:str,streaming_output_event_name:str,
                                      inport:dict,outport:dict):
       
        event_id = self.ctb.register_event(streaming_input_event_name)
        input_event_id = self.ctb.register_event(streaming_input_event_name)
        output_event_id = self.ctb.register_event(streaming_output_event_name)
        node_data = {"aux_data": aux_function_data, "event_id": input_event_id, "inport": inport, "output_event_id": output_event_id, "outport": outport}
        self.define_column_link(main_function_name="CFL_STREAMING_TRANSFORM_PACKET", initialization_function_name="CFL_STREAMING_TRANSFORM_PACKET_INIT", 
                                       aux_function_name=aux_function, termination_function_name="CFL_STREAMING_TRANSFORM_PACKET_TERM", node_data=node_data)
        
    def asm_streaming_filter_packet(self,aux_function:str,aux_function_data:dict,streaming_event_name:str,inport:dict):
       
   
        event_id = self.ctb.register_event(streaming_event_name)
        node_data = {"aux_data": aux_function_data, "event_id": event_id, "inport": inport}
        self.define_column_link(main_function_name="CFL_STREAMING_FILTER_PACKET", initialization_function_name="CFL_STREAMING_FILTER_PACKET_INIT", 
                                       aux_function_name=aux_function, termination_function_name="CFL_STREAMING_FILTER_PACKET_TERM", node_data=node_data)
        
    def asm_streaming_merge_packets(self,aux_function:str,aux_function_data:dict,streaming_event_name:str,inports:list[dict],outport:dict):
       
        event_id = self.ctb.register_event(streaming_event_name)
        node_data = {"aux_data": aux_function_data, "event_id": event_id, "inports": inports, "outport": outport}
        self.define_column_link(main_function_name="CFL_STREAMING_MERGE_PACKETS", initialization_function_name="CFL_STREAMING_MERGE_PACKETS_INIT", 
                                       aux_function_name=aux_function, termination_function_name="CFL_STREAMING_MERGE_PACKETS_TERM", node_data=node_data)
        
    def asm_streaming_tap_packet(self,aux_function:str,aux_function_data:dict,streaming_event_name:str,inport:dict):
        
        event_id = self.ctb.register_event(streaming_event_name)
        node_data = {"aux_data": aux_function_data, "event_id": event_id, "inport": inport}
        self.define_column_link(main_function_name="CFL_STREAMING_TAP_PACKET", initialization_function_name="CFL_STREAMING_TAP_PACKET_INIT", 
                                       aux_function_name=aux_function, termination_function_name="CFL_STREAMING_TAP_PACKET_TERM", node_data=node_data)
        
    def asm_streaming_verify_packet(self,aux_function:str,aux_function_data:dict,streaming_event_name:str,inport:dict,reset_flag = False,error_fn = "CFL_NULL",error_data = None):
        
        
        event_id = self.ctb.register_event(streaming_event_name)
        node_data = {"aux_data": aux_function_data, "event_id": event_id, "inport": inport}
        self.asm_verify(aux_function ,fn_data=node_data, reset_flag = reset_flag, error_fn = error_fn, error_data = error_data )
        
    