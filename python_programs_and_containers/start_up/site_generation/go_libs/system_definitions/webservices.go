package sys_defs

import "lacima.com/go_setup_containers/site_generation_base/site_generation_utilities"

func Construct_web_services(){
 
    su.Bc_Rec.Add_header_node("WEB_DISCOVERY","WEB_DISCOVERY",make(map[string]interface{}))
  
    properties := make(map[string]interface{})
	properties["port"] = 8001
    su.Bc_Rec.Add_info_node("WEB_SERVICES","irrigation",properties)

    su.Cd_Rec.Construct_package("IRRIGIGATION_IP_LOOK_UP")
    su.Cd_Rec.Add_hash("WEB_IP_LOOK_UP") 
	su.Cd_Rec.Close_package_construction()
    
    su.Bc_Rec.End_header_node("WEB_DISCOVERY","WEB_DISCOVERY")
 

}
        
    
  
