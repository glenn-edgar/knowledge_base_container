package irrigation

import (
    "lacima.com/go_setup_containers/site_generation_base/site_generation_utilities"
)



var all_master_actions []string
var all_slave_actions  []string


func Add_irrigation_actions(){
    
    
  su.Bc_Rec.Add_header_node("IRRIGATION_ACTIONS","IRRIGATION_ACTIONS",make(map[string]interface{}))
  
  /* ********************************************************************************************* */
 
  
       properties := make(map[string]interface{})
       properties["immediate"] = false
       properties["charge_time"]   = 60 //seconds
       properties["back_flush"]    = 30 // seconds
       properties["forward_flush"] = 30 // seconds
       su.Bc_Rec.Add_info_node("IRRIGATION_ACTION","CLEAN_FILTER", properties)
       

   
  
  /* ------------------------------------------------------------------------------------------- */
  
  
  
  
  /* ********************************************************************************************* */
  
   
      properties = make(map[string]interface{})  
      properties["immediate"] = false
      properties["hold_time"] = 5
      properties["measure_dt"] = 1
      properties["meas_number"] = 5
      properties["next_time"] = 5
      su.Bc_Rec.Add_info_node("IRRIGATION_ACTION","VALVE_RESISTANCE",properties)
   
 
  
  /* ------------------------------------------------------------------------------------------- */
   
   /* ********************************************************************************************* */
  

  
     properties = make(map[string]interface{})  
     properties["immediate"] = false
     properties["charge_time"] = 300
     properties["wait_time"] = 300
     su.Bc_Rec.Add_info_node("IRRIGATION_ACTION","VALVE_LEAK",properties)
   
  
   
  /* ------------------------------------------------------------------------------------------- */
  
  
   /* ********************************************************************************************* */
   
 
 
      properties = make(map[string]interface{})  
     properties["immediate"] = true
     su.Bc_Rec.Add_info_node("IRRIGATION_ACTION","OPEN_MASTER_VALVE",properties)
 
 
   
  /* ------------------------------------------------------------------------------------------- */
  
   /* ********************************************************************************************* */
 
     properties = make(map[string]interface{})  
     properties["immediate"] = true
    su.Bc_Rec.Add_info_node("IRRIGATION_ACTION","CLOSE_MASTER_VALVES",properties)

  
  /* ------------------------------------------------------------------------------------------- */ 
   
  /* ********************************************************************************************* */

    properties = make(map[string]interface{})  
     properties["immediate"] = true
    su.Bc_Rec.Add_info_node("IRRIGATION_ACTION","SKIP_ENTRY",properties)

 
  /* ------------------------------------------------------------------------------------------- */   
   /* ********************************************************************************************* */
 
    properties = make(map[string]interface{})  
    properties["immediate"] = true
    su.Bc_Rec.Add_info_node( "IRRIGATION_ACTION","CLEAR_QUEUE",properties)

  
  
  
    /* ********************************************************************************************* */
 
    properties = make(map[string]interface{})  
    properties["immediate"] = true
    su.Bc_Rec.Add_info_node( "IRRIGATION_ACTION","PAUSE",properties)

  
  /* ------------------------------------------------------------------------------------------- */ 
  /* ------------------------------------------------------------------------------------------- */
   
 su.Bc_Rec.End_header_node("IRRIGATION_ACTIONS","IRRIGATION_ACTIONS")   
 
 all_master_actions = []string{"CLEAN_FILTER","VALVE_LEAK","OPEN_MASTER_VALVE","CLOSE_MASTER_VALVES","VALVE_RESISTANCE",
                     "CLEAR_QUEUE"}
 all_slave_actions  = []string{"CLEAR_QUEUE","SKIP_ENTRY"}


   
}   
  
   
   
   
 
 
    
  
   
   
   
