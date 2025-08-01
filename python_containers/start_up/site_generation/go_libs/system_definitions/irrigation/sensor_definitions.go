package irrigation

import (
    "lacima.com/go_setup_containers/site_generation_base/site_generation_utilities"
)

func Add_irrigation_sensors(){
    
   su.Bc_Rec.Add_header_node("IRRIGATION_SENSORS","IRRIGATION_SENSORS",make(map[string]interface{}))
   
 
    
   su.Bc_Rec.End_header_node("IRRIGATION_SENSORS","IRRIGATION_SENSORS") 
    
}


