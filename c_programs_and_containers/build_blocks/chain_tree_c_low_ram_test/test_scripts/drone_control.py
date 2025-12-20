from ct_build.chain_tree.controlled_nodes import ControlledNodes


class DroneControl(ControlledNodes):
    def __init__(self, ctb, h_file: str):
        ControlledNodes.__init__(self, ctb)
        self.h_file = h_file
        self.command_container ={}
        fly_straight_container = {}
        fly_straight_container["request_port"] = self.make_port(self.h_file, 0, "fly_straight_request")
        fly_straight_container["response_port"] = self.make_port(self.h_file, 1, "fly_straight_response")
        fly_straight_container["api_name"] = "drone_control_fly_straight"
        self.command_container["fly_straight"] = fly_straight_container
        fly_arc_container = {}
        fly_arc_container["request_port"] = self.make_port(self.h_file, 0, "fly_arc_request")
        fly_arc_container["response_port"] = self.make_port(self.h_file, 1, "fly_arc_response")
        fly_arc_container["api_name"] = "drone_control_fly_arc"
        self.command_container["fly_arc"] = fly_arc_container
        fly_up_container = {}
        fly_up_container["request_port"] = self.make_port(self.h_file, 0, "fly_up_request")
        fly_up_container["response_port"] = self.make_port(self.h_file, 1, "fly_up_response")
        fly_up_container["api_name"] = "drone_control_fly_up"
        self.command_container["fly_up"] = fly_up_container
        fly_down_container = {}
        fly_down_container["request_port"] = self.make_port(self.h_file, 0, "fly_down_request")
        fly_down_container["response_port"] = self.make_port(self.h_file, 1, "fly_down_response")
        fly_down_container["api_name"] = "drone_control_fly_down"
        self.command_container["fly_down"] = fly_down_container
        
    def fly_straight_client(self, distance: float, final_altitude: float, final_speed: float, heading: float, finalize_fn: str,finalize_data: dict):
        fly_straight_container = self.command_container["fly_straight"]
        monitor_data = {
            "distance": distance,
            "final_altitude": final_altitude,
            "final_speed": final_speed,
            "heading": heading,
            "finalize_data": finalize_data
        }
        self.client_controlled_node(fly_straight_container["api_name"],  finalize_fn,monitor_data, 
                                    fly_straight_container["request_port"], fly_straight_container["response_port"])
    
    def fly_straight_server(self,column_name: str, monitor_fn: str,monitor_data: dict):
        fly_straight_container = self.command_container["fly_straight"]
        
        return self.controlled_node( fly_straight_container["api_name"], column_name , monitor_fn,monitor_data, 
                             fly_straight_container["request_port"], fly_straight_container["response_port"]) 
        
        
        
 
        
        
        
    def fly_arc_client(self,  distance: float, final_altitude: float, final_speed: float, heading: float, finalize_fn: str,finalize_data: dict):
        fly_straight_container = self.command_container["fly_arc"]
        monitor_data = {
            "distance": distance,
            "final_altitude": final_altitude,
            "final_speed": final_speed,
            "heading": heading,
            "finalize_data": finalize_data
        }
        self.client_controlled_node( fly_straight_container["api_name"], finalize_fn,monitor_data, 
                                    fly_straight_container["request_port"], fly_straight_container["response_port"])
    
    def fly_arc_server(self,column_name: str, monitor_fn: str,monitor_data: dict):
        fly_arc_container = self.command_container["fly_arc"]
        return self.controlled_node(fly_arc_container["api_name"], column_name , monitor_fn,monitor_data, 
                             fly_arc_container["request_port"], fly_arc_container["response_port"])  
        
        
        
    def fly_up_client(self, final_altitude: float, final_speed: float, finalize_fn: str,finalize_data: dict):
        fly_up_container = self.command_container["fly_up"]
        monitor_data = {
            
            "final_altitude": final_altitude,
            "final_speed": final_speed,
            "finalize_data": finalize_data
                 
            }
        self.client_controlled_node( fly_up_container["api_name"], finalize_fn,monitor_data, 
                                    fly_up_container["request_port"], fly_up_container["response_port"])
    
    def fly_up_server(self,column_name: str, monitor_fn: str,monitor_data: dict):
        fly_up_container = self.command_container["fly_up"]
        return self.controlled_node(fly_up_container["api_name"], column_name , monitor_fn,monitor_data, 
                             fly_up_container["request_port"], fly_up_container["response_port"])  
        
    def fly_down_client(self,  final_altitude: float, final_speed: float,  finalize_fn: str,finalize_data: dict):
        fly_down_container = self.command_container["fly_down"]
        monitor_data = {
            "final_altitude": final_altitude,
            "final_speed": final_speed,
            "finalize_data": finalize_data,
            
        }
        return self.client_controlled_node( fly_down_container["api_name"], finalize_fn,monitor_data, 
                                    fly_down_container["request_port"], fly_down_container["response_port"])
    
    def fly_down_server(self,column_name: str, monitor_fn: str,monitor_data: dict):
        fly_down_container = self.command_container["fly_down"]
        return self.controlled_node(fly_down_container["api_name"], column_name , monitor_fn,monitor_data, 
                             fly_down_container["request_port"], fly_down_container["response_port"])  