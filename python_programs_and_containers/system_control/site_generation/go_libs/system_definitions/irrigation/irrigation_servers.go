package irrigation

import (
    "lacima.com/go_setup_containers/site_generation_base/site_generation_utilities"
)

/*
actions 
      clean filter
      check for valve leak
      valve check
      null irrigation step
      valid irrigatigatin step
      open master valve
      close master valve
      
*/

/*
 * need to handle subservers
 *
 */ 

func Add_irrigation_servers(){
    
    
  su.Bc_Rec.Add_header_node("IRRIGATION_SERVERS","IRRIGATION_SERVERS",make(map[string]interface{}))
  
   master_valves := make(map[string]interface{})
   master_valves["station_1"] = 43
   cleaning_valves := make(map[string]interface{})
   cleaning_valves["station_1"] = 44
  
   master_irrigation_node(   "main_server",all_master_actions,
                                  master_valves,cleaning_valves)
 
   construct_sub_server_a("main_server")
   
   master_irrigation_end("main_server")                    

 
/*
  su.Cd_Rec.Construct_package("IRRIGATION_DATA_STRUCTURES")
  su.Cd_Rec.Create_postgres_table( "IRRIGATION_SCHEDULES","admin","password","admin")
  su.Cd_Rec.Create_postgres_json("IRRIGATION_ACTIONS","admin","password","admin")
  su.Cd_Rec.Create_postgres_json("IRRIGATION_JOBS","admin","password","admin")
  su.Cd_Rec.Close_package_construction()
*/
  su.Bc_Rec.End_header_node("IRRIGATION_SERVERS","IRRIGATION_SERVERS")     
        
    
}

func construct_sub_server_a(master_node string){
   nil_exclusion := make([]int,0)
   supported_stations := map[string][]int{"station_1":nil_exclusion,
                                        "station_2":nil_exclusion,
                                        "station_3":nil_exclusion,
                                        "station_4":nil_exclusion}
 
   station_valve_number := map[string]int{"station_1":42,
                                        "station_2":22,
                                        "station_3":22,
                                        "station_4":20}
                                        
   slave_irrigation_server(master_node+"/sub_server_1",supported_stations,station_valve_number ,    all_slave_actions)
 
    
    
}







func master_irrigation_node( server_name               string,
                             supported_actions         []string,
                             master_valves             map[string]interface{},
                             cleaning_valves           map[string]interface{}){
                                  
  
  server_properties                        := make(map[string]interface{})
  server_properties["supported_actions"]   = supported_actions
  
  su.Bc_Rec.Add_header_node("IRRIGATION_SERVER",server_name,server_properties)
  su.Cd_Rec.Add_rpc_server(server_name ,10,10)
 
  su.Bc_Rec.Add_info_node("MASTER_VALVES",server_name,master_valves)
  su.Bc_Rec.Add_info_node("CLEANING_VALVES",server_name,cleaning_valves)
  
}

func master_irrigation_end(server_name string){
  su.Bc_Rec.End_header_node("IRRIGATION_SERVER",server_name)
  
}


func slave_irrigation_server(server_name        string,
                      supported_stations        map[string][]int,
                      station_valve_number      map[string]int,
                      supported_actions         []string){
                                 
  server_properties                        := make(map[string]interface{})
  supported_station_map                    := make(map[string][]int)
  
  
  
  for station_name,exclude_list := range supported_stations{
    valve_number := station_valve_number[station_name]
    supported_station_map[station_name] =  construct_station_data(valve_number,exclude_list)
  }
      
  
  
  
  server_properties["supported_actions"]   = supported_actions
  server_properties["supported_stations"]  = supported_station_map
  su.Bc_Rec.Add_header_node("IRRIGATION_SUBSERVER",server_name,server_properties)
  su.Cd_Rec.Add_rpc_server(server_name ,10,10)
  /*
   * supporting data structures
   * 
   */
  su.Bc_Rec.End_header_node("IRRIGATION_SUBSERVER",server_name)
  
}

func construct_station_data( valve_number int ,exclude_array []int)[]int{
   
    return_value := make([]int,0)
    include_map := make(map[int]bool)
    for i:=1;i<valve_number+1;i++{
      include_map[i] = true   
    }
    include_map[0] = false
    for _,value := range exclude_array{
        include_map[value] = false
    }
    for i:= 1;i<valve_number+1;i++ {
        if include_map[i] == true{
            return_value = append(return_value,i)
        }
    }
    return return_value
    
    
}
  
