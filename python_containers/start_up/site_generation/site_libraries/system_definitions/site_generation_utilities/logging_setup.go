package su
//import "fmt"

/*
 *  used to  
 *   redundant data structures were put in so that logging element doesnot have to dig into the stream
 *   This data structures could be used on arduino type processors
 Logging Levels

 

 
 
 
 */

// log levels

const Emergency        uint64 = 7
const Alert            uint64 = 6
const Critical         uint64 = 5
const Error            uint64 = 4
const Warning          uint64 = 3
const Notice           uint64 = 2
const Informational    uint64 = 1 
const Debug            uint64 = 0



func Construct_incident_logging(command_code ,description string,log_level uint64 ){
    //fmt.Println("command_code",command_code)
    if log_level > 7 {
        log_level = 7
    }
    
    properties := make(map[string]interface{})
    properties["description"] = description
    properties["log_level"] = log_level
    
    Bc_Rec.Add_header_node("INCIDENT_LOG",command_code,properties)
	Cd_Rec.Construct_package("INCIDENT_LOG")
    Cd_Rec.Add_single_element("TIME_STAMP")
    Cd_Rec.Add_single_element("STATUS")
    Cd_Rec.Add_single_element("LAST_ERROR")
    Cd_Rec.Add_single_element("ERROR_TIME")
    Cd_Rec.Close_package_construction()
	Bc_Rec.End_header_node("INCIDENT_LOG",command_code)

}



func Construct_postgres_streaming_logs( description, stream_name,user,password, database_name string, time_limit int64){
     properties := make(map[string]interface{})
     properties["description"] = description
     Bc_Rec.Add_header_node("POSTGRES_LOG",stream_name,properties)
     Cd_Rec.Construct_package("POSTGRES_LOG")
     Cd_Rec.Create_postgres_stream( "POSTGRES_LOG",user,password, database_name,time_limit )
     Cd_Rec.Close_package_construction()
     Bc_Rec.End_header_node("POSTGRES_LOG",stream_name)
}
   
/*
 *   
 * 
 * 
 * 
 * 
 */

func Construct_streaming_logs(stream_name , description string, keys []string ){

  properties := make(map[string]interface{})
  properties["keys"] = keys
  properties["descrption"] = description
  Bc_Rec.Add_header_node("STREAMING_LOG",stream_name,properties)
  
  Bc_Rec.End_header_node("STREAMING_LOG",stream_name)
}

/*
 * This data structure allows rpc servers to be scanned by diagnostic programs
 * 
 */
func  Construct_RPC_Server( command_code, description string,depth,timeout int64, properties map[string]interface{} ){
    
    
    properties["description"] = description
    
    Bc_Rec.Add_header_node("RPC_SERVER",command_code,properties)
    Cd_Rec.Construct_package("RPC_SERVER")
    Cd_Rec.Add_rpc_server("RPC_SERVER",depth,timeout)
    Cd_Rec.Close_package_construction()
    Construct_streaming_logs(command_code ,description+" performance log", []string{"queue_depth","utilization"} )
    
    Bc_Rec.End_header_node("RPC_SERVER",command_code)    
}






func Construct_watchdog_logging(command_code , description string, max_time_interval int){
 properties := make(map[string]interface{})
 properties["description"] = description
 properties["max_time_interval"] = max_time_interval
  Bc_Rec.Add_header_node("WATCH_DOG",command_code,properties)

  Cd_Rec.Construct_package("WATCH_DOG")
  Cd_Rec.Add_single_element("WATCH_DOG_TS")   // used to stored timestamp
  Cd_Rec.Close_package_construction()
  Bc_Rec.End_header_node("WATCH_DOG",command_code)

}
