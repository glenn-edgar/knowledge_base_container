package sys_defs

import "lacima.com/go_setup_containers/site_generation_base/system_definitions/irrigation"
import "lacima.com/go_setup_containers/site_generation_base/site_generation_utilities"

const eto_image    string   = "nanodatacenter/eto"

func construct_irrigation( master_flag bool, node_name string){
 
   containers := []string{"eto"}
   eto_command_map  := make(map[string]string)
   eto_command_map["eto"] = "./eto"   
   su.Add_container( false,"eto",eto_image, su.Managed_run,eto_command_map, su.Data_mount)
   su.Construct_service_def("irrigation",master_flag,node_name, containers, generate_irrigation_component_graph)   
}   
  
func generate_irrigation_component_graph(){
 
    // ETO  Setups
    irrigation.Construct_weather_stations()
    irrigation.Eto_valve_data_structures()
    
    
    
    // setup irrigation data
    irrigation.Add_irrigation_actions()
    irrigation.Add_irrigation_sensors()
    irrigation.Add_station_definitions()
    irrigation.Add_irrigation_servers()
    irrigation.Add_irrigation_data_structures()
    
    
    
    
    
    
}   

      
      

      
