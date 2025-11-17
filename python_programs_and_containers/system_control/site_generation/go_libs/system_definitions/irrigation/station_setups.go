package irrigation

import (
    "lacima.com/go_setup_containers/site_generation_base/site_generation_utilities"
)





func Add_station_definitions(){
    
   su.Bc_Rec.Add_header_node("IRRIGATION_STATIONS","IRRIGATION_STATIONS",make(map[string]interface{}))
   
   Add_valve_group_definitions()
   
   r :=construct_station_control()
   description_1 := make([]string,0)
   rainbird_resistance := float64(45)
   r1 := r.construct_valve_array(44,rainbird_resistance)
   r1[44] = 45./2.
   r.Add_Click_PLC_RS485( "station_1", "CLICK_1" , len(r1),100,r1,description_1 )
   
   r2 := r.construct_valve_array(22,rainbird_resistance)
   description_2 := make([]string,0)
   r.Add_Click_PLC_RS485( "station_2", "CLICK_2" , len(r2),125,r2,description_2 )
   
   r3 := r.construct_valve_array(22,rainbird_resistance)
   description_3 := make([]string,0)
   r.Add_Click_PLC_RS485( "station_3", "CLICK_2" , len(r3),170,r3,description_3 )
   
   r4 := r.construct_valve_array(20,rainbird_resistance)
   description_4 := make([]string,0)
   r.Add_Click_PLC_RS485( "station_4", "CLICK_1" , len(r4),135,r4 ,description_4)
   r.generate_stations()
   su.Bc_Rec.End_header_node("IRRIGATION_STATIONS","IRRIGATION_STATIONS") 
    
}


type station_control_type struct{
    station_map  map[string]map[string]interface{}
    valid_types  map[string]bool
}    


func construct_station_control()station_control_type{
    var return_value station_control_type
    
    return_value.station_map = make(map[string]map[string]interface{})
    return_value.valid_types              = make(map[string]bool)
    return_value.valid_types["CLICK_1"] = true
    return_value.valid_types["CLICK_2"] = true
    
    return return_value
}

func ( r *station_control_type ) generate_stations(){
    
    for key,value := range r.station_map{
        su.Bc_Rec.Add_info_node("IRRIGATION_STATION",key,value )
    }
    
}

func ( r *station_control_type)Add_Click_PLC_RS485( name , plc_type string,  valve_number,modbus_address int ,resistance []float64,description []string ){
    
    r.check_name(name)
    r.check_type(plc_type)
   
    
    entry := make(map[string]interface{})
    entry["name"]                        = name
    entry["plc"]                            = plc_type
    entry["valve_number"]         = valve_number
    entry["modbus_address"]    = modbus_address
    entry["resistance"]                = resistance
    entry["description"]               = description
    r.station_map[name] = entry
    
}
 
 
func ( r *station_control_type)construct_valve_array( valve_number int, resistance float64)[]float64{
    
   return_value := make([]float64,valve_number+1)

   for i := 0; i< valve_number+1; i++{
       return_value[i] = resistance
   }
   return return_value
    
    
    
}
    
 
 
func ( r station_control_type)check_name(name string){
  
 if _,ok := r.station_map[name];ok==true {
        panic("duplicate station name")
    }
}    
    
func ( r *station_control_type)check_type(plc_type string){
    
 if _,ok := r.valid_types[plc_type];ok==false {
        panic("invalid plc type")
    }    
    
}
                                         
