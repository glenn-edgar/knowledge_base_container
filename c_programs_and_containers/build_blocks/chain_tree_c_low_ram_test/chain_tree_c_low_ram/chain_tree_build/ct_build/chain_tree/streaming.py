from .column_flow import ColumnFlow

class Streaming(ColumnFlow):
    def __init__(self, ctb):
        ColumnFlow.__init__(self, ctb)
        self.ctb = ctb
        
    def make_port(self, file_name: str, handler_id: int, event: str):
        if not isinstance(file_name, str):
            raise TypeError("File name must be a string")
        if not isinstance(handler_id, int):
            raise TypeError("Handler id must be an integer")
        if not isinstance(event, str):
            raise TypeError("Event must be a string")
        event_id = self.ctb.register_event(event)
        port_data = {"file_name": file_name, "handler_id": handler_id, "event_id": event_id}
        return port_data
    
    def asm_streaming_emit_packet(self, aux_function: str, aux_function_data: dict, event_column: str, outport: dict):
        event_column_id = self.ctb.get_node_index(event_column)
        node_data = {"aux_data": aux_function_data, "event_id": outport["event_id"], "outport": outport, "event_column": event_column_id}
        self.asm_one_shot_handler(aux_function, node_data)
        
    def asm_streaming_sink_packet(self, aux_function: str, aux_function_data: dict, inport: dict):
        node_data = {"aux_data": aux_function_data, "event_id": inport["event_id"], "inport": inport}
        self.define_column_link(main_function_name="CFL_STREAMING_SINK_PACKET", initialization_function_name="CFL_STREAMING_SINK_PACKET_INIT", 
                                aux_function_name=aux_function, termination_function_name="CFL_STREAMING_SINK_PACKET_TERM", node_data=node_data)
        
    def asm_streaming_transform_packet(self, aux_function: str, aux_function_data: dict, inport: dict, outport: dict, output_event_column: str):
        output_event_column_id = self.ctb.get_node_index(output_event_column)
        node_data = {
            "aux_data": aux_function_data, 
            "event_id": inport["event_id"],  # input event from inport
            "inport": inport, 
            "output_event_id": outport["event_id"],  # output event from outport
            "outport": outport, 
            "output_event_column_id": output_event_column_id
        }
        self.define_column_link(main_function_name="CFL_STREAMING_TRANSFORM_PACKET", initialization_function_name="CFL_STREAMING_TRANSFORM_PACKET_INIT", 
                                aux_function_name=aux_function, termination_function_name="CFL_STREAMING_TRANSFORM_PACKET_TERM", node_data=node_data)
        
    def asm_streaming_filter_packet(self, aux_function: str, aux_function_data: dict, inport: dict):
        node_data = {"aux_data": aux_function_data, "event_id": inport["event_id"], "inport": inport}
        self.define_column_link(main_function_name="CFL_STREAMING_FILTER_PACKET", initialization_function_name="CFL_STREAMING_FILTER_PACKET_INIT", 
                                aux_function_name=aux_function, termination_function_name="CFL_STREAMING_FILTER_PACKET_TERM", node_data=node_data)
        
    def asm_streaming_collect_packets(self, aux_function: str, aux_function_data: dict, 
                                    inports: list[dict], output_event: str, output_event_column: str):
        """Collector node - multiple verified inports, event-only output (no schema verification)."""
        output_event_column_id = self.ctb.get_node_index(output_event_column)
        output_event_id = self.ctb.register_event(output_event)
        node_data = {
            "aux_data": aux_function_data,
            "inports": inports,
            "output_event_id": output_event_id,
            "output_event_column_id": output_event_column_id
        }
        self.define_column_link(
            main_function_name="CFL_STREAMING_COLLECT_PACKETS",
            initialization_function_name="CFL_STREAMING_COLLECT_PACKETS_INIT",
            aux_function_name=aux_function,
            termination_function_name="CFL_STREAMING_COLLECT_PACKETS_TERM",
            node_data=node_data
        )
        
    def asm_streaming_tap_packet(self, aux_function: str, aux_function_data: dict, inport: dict):
        node_data = {"aux_data": aux_function_data, "event_id": inport["event_id"], "inport": inport}
        self.define_column_link(main_function_name="CFL_STREAMING_TAP_PACKET", initialization_function_name="CFL_STREAMING_TAP_PACKET_INIT", 
                                aux_function_name=aux_function, termination_function_name="CFL_STREAMING_TAP_PACKET_TERM", node_data=node_data)
        
 
        
        
    def asm_streaming_sink_collected_packets(self, aux_function: str, aux_function_data: dict, event_name: str):
        """Sink for collector output packets - no port/schema verification, just event matching."""
        event_id = self.ctb.register_event(event_name)
        node_data = {
            "aux_data": aux_function_data,
            "event_id": event_id
        }
        self.define_column_link(
            main_function_name="CFL_STREAMING_SINK_COLLECTED_PACKETS",
            initialization_function_name="CFL_STREAMING_SINK_COLLECTED_PACKETS_INIT",
            aux_function_name=aux_function,
            termination_function_name="CFL_STREAMING_SINK_COLLECTED_PACKETS_TERM",
            node_data=node_data
        )
        
        
    def asm_streaming_verify_packet(self, aux_function: str, aux_function_data: dict, inport: dict,
                                 reset_flag=False, error_fn="CFL_NULL", error_data=None):
        """Verify packet with user-defined test function.
        
        Library boolean function handles:
        - Generic streaming setup (inport matching, event matching)
        - Calls user's aux_function for specific verification logic
        
        User's aux_function returns true if packet passes verification, false otherwise.
        """

        node_data = {
            "aux_data": aux_function_data,
            "inport": inport,
            "user_aux_function": aux_function  # User's verification function
        }
        self.asm_verify(
            verify_fn="CFL_STREAMING_VERIFY_PACKET",
            fn_data=node_data,
            reset_flag=reset_flag,
            error_fn=error_fn,
            error_data=error_data
        )