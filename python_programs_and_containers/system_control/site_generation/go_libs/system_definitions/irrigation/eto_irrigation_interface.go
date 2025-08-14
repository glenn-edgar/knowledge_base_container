
package irrigation

import (
    "lacima.com/go_setup_containers/site_generation_base/site_generation_utilities"
)

func Eto_valve_data_structures(){

    su.Bc_Rec.Add_header_node("ETO_SETUP","ETO_SETUP",make(map[string]interface{}))

    eto_info := make(map[string]interface{})
    su.Bc_Rec.Add_info_node("ETO_SETUP_PROPERTIES","ETO_SETUP_PROPERTIES",eto_info )    
    su.Cd_Rec.Construct_package("ETO_DATA_STRUCTURES")
    su.Cd_Rec.Add_hash("ETO_ACCUMULATION") 
    su.Cd_Rec.Add_hash("ETO_RESERVE")
    su.Cd_Rec.Add_hash("ETO_MIN_LEVEL")
    su.Cd_Rec.Add_hash("ETO_RECHARGE_RATE")
	su.Cd_Rec.Close_package_construction()    
    
    su.Bc_Rec.End_header_node("ETO_SETUP","ETO_SETUP")    
    
    
    
}
