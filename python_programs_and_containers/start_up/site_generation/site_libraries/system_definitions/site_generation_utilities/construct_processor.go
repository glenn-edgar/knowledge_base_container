package su



func construct_node(name string, containers []string){

      properties := make(map[string]interface{})
      properties["containers"] = containers
      Bc_Rec.Add_header_node("NODE",name, properties )
   
      
      var description string
      
      description = name + "  node reboot"
      Construct_incident_logging("NODE_REBOOT",description,Emergency)
      
      // this is a watchdog failure log for this container
      //description = name+"  node controller watchdog"
      //Construct_watchdog_logging("node_control",description,20)
      
      keys := []string{"FREE_CPU","RAM","TEMPERATURE","DISK_SPACE","SWAP_SPACE","CONTEXT_SWITCHES","BLOCK_DEV","IO_SPACE","RUN_QUEUE","EDEV"}
      Bc_Rec.Add_header_node("NODE_MONITORING","NODE_MONITORING", make(map[string]interface{}))
	  description = name+" node_monitor"
	  Construct_streaming_logs("node_monitor",description,keys) //wait until flush out
      for _,key := range keys{
          //Bc_Rec.Add_header_node("INCIDENT_LOG",key, make(map[string]interface{}))
          Construct_incident_logging(key ,"CPU_Mon:"+key,Emergency )
          //Bc_Rec.End_header_node("INCIDENT_LOG",key)
      }
	  Bc_Rec.End_header_node("NODE_MONITORING","NODE_MONITORING")

      
      
 

      
      
      Construct_RPC_Server("NODE_CONTROL","rpc for controlling node:    "+name ,10,15,  make(map[string]interface{}) )
      
      
      
      
      Bc_Rec.End_header_node("NODE",name)

 
}    
