package error_detection

import "lacima.com/go_setup_containers/site_generation_base/site_generation_utilities"



func Construct_definitions(){
    
  error_detection_properties := make(map[string]interface{})
  error_detection_properties["subsystems"] = []string{"watch_dog","incident","rpc","streaming"}
  su.Bc_Rec.Add_header_node("ERROR_DETECTION","ERROR_DETECTION",error_detection_properties)  
    
  
       su.Cd_Rec.Construct_package("OVERALL_STATUS")
           su.Cd_Rec.Add_hash("OVERALL_STATUS")
       su.Cd_Rec.Close_package_construction()
   
  
  
       wd_detection_properties := make(map[string]interface{})
       wd_detection_properties["trim_time"]       = 3600*24*30
       wd_detection_properties["sample_time"]     = 10  // 30 seconds
       wd_detection_properties["debounce_count"]       = 5
       wd_detection_properties["subsystem_id"]    = "watch_dog"
       su.Bc_Rec.Add_header_node("WD_DETECTION","WD_DETECTION",wd_detection_properties)
  
 
            su.Cd_Rec.Construct_package("WATCH_DOG_DATA")
                su.Cd_Rec.Add_hash("DEBOUNCED_STATUS")   
                su.Cd_Rec.Add_hash("STATUS")   
                su.Cd_Rec.Add_hash("TIME_STAMP") 
                su.Cd_Rec.Add_hash("DESCRIPTION")
                su.Cd_Rec.Create_postgres_stream( "WATCH_DOG_LOG","admin","password","admin",30*24*3600)  
            su.Cd_Rec.Close_package_construction()
  
       su.Bc_Rec.End_header_node("WD_DETECTION","WD_DETECTION")  
  
  
       incident_properties := make(map[string]interface{})
       incident_properties["trim_time"]       = 3600*24*30 // 30 days
       incident_properties["sample_time"]     = 15  // 30 seconds
       incident_properties["subsystem_id"]     = "incident"
       
       su.Bc_Rec.Add_header_node("INCIDENT_STREAMS","INCIDENT_STREAMS",incident_properties)

           su.Cd_Rec.Construct_package("INCIDENT_DATA")
               su.Cd_Rec.Add_hash("DESCRIPTION")
               su.Cd_Rec.Add_hash("TIME") 
               su.Cd_Rec.Add_hash("STATUS") 
               su.Cd_Rec.Add_hash("LAST_ERROR")
               su.Cd_Rec.Add_hash("ERROR_TIME") 
               su.Cd_Rec.Add_hash("REVIEW_STATE")
               su.Cd_Rec.Add_hash("ACKNOWLEGE_STATE")
               su.Cd_Rec.Add_hash("OLD_STATUS") 
               su.Cd_Rec.Create_postgres_stream( "INCIDENT_LOG","admin","password","admin",30*24*3600)  
           su.Cd_Rec.Close_package_construction()
  
       su.Bc_Rec .End_header_node("INCIDENT_STREAMS","INCIDENT_STREAMS")  
       
       streaming_properties := make(map[string]interface{})
       streaming_properties["sample_time"]     = 60*30 // 30 minutes
       streaming_properties["trim_time"]       = 3600*24*30*3  // 3 months
       su.Bc_Rec.Add_header_node("STREAMING_LOGS","STREAMING_LOGS",streaming_properties)
             su.Cd_Rec.Construct_package("STREAM_SUMMARY_DATA")           
               su.Cd_Rec.Add_hash("STREAM_TABLE") 
               su.Cd_Rec.Add_hash("TIME_TABLE") 
               
               su.Cd_Rec.Add_hash("Z_TABLE") 
               su.Cd_Rec.Add_hash("Z_TIME")
               su.Cd_Rec.Create_postgres_stream( "LOG_STREAM","admin","password","admin",30*24*3600)
               su.Cd_Rec.Create_postgres_stream( "FILTERED_STREAM","admin","password","admin",30*24*3600)  
               su.Cd_Rec.Create_postgres_stream( "INCIDENT_STREAM","admin","password","admin",30*24*3600)  
             su.Cd_Rec.Close_package_construction()
  
    
       su.Bc_Rec .End_header_node("STREAMING_LOGS","STREAMING_LOGS")    
  
       rpc_properties := make(map[string]interface{})
       
       rpc_properties["sample_time"]     = 15  // 15 minutes
       rpc_properties["trim_time"]       = 3600*24*30*3  // 3 months
       
       su.Bc_Rec.Add_header_node("RPC_ANALYSIS","RPC_ANALYSIS",rpc_properties)
  

          su.Construct_incident_logging("RPC_FAILURE" ,"RPC FAILURES",su.Emergency)
       
          su.Construct_incident_logging("RPC_LOADING" ,"RPC_LOADING",su.Emergency)  
          
           su.Cd_Rec.Construct_package("RPC_ANALYSIS_DATA")
               su.Cd_Rec.Add_hash("DESCRIPTION")
               su.Cd_Rec.Add_hash("TIME") 
               su.Cd_Rec.Add_hash("STATUS") 
               su.Cd_Rec.Add_hash("LOADING")
               su.Cd_Rec.Add_hash("LENGTH")
               su.Cd_Rec.Add_hash("ERROR_TIME") 
               su.Cd_Rec.Create_postgres_stream( "INCIDENT_LOG","admin","password","admin",30*24*3600)  
            su.Cd_Rec.Close_package_construction()
  
       su.Bc_Rec.End_header_node("RPC_ANALYSIS","RPC_ANALYSIS")    
       
       alert_properties := make(map[string]interface{})
       su.Bc_Rec.Add_header_node("ALERT_NOTIFICATION","ALERT_NOTIFICATION",alert_properties)
       
           telegram_properties := make(map[string]interface{})
           telegram_properties["valid_users"] = []string{"1575166855"}    
           su.Bc_Rec.Add_header_node("TELEGRAM_SERVER","TELEGRAM_SERVER",telegram_properties)
              su.Construct_RPC_Server("TELEGRAPH_RPC","rpc for controlling system",10,15, make( map[string]interface{}) )
           su.Bc_Rec.End_header_node("TELEGRAM_SERVER","TELEGRAM_SERVER")    
       
           
        su.Bc_Rec.End_header_node("ALERT_NOTIFICATION","ALERT_NOTIFICATION")   
       
       
       
   su.Bc_Rec.End_header_node("ERROR_DETECTION","ERROR_DETECTION")  
    
    
    
 
    
}
