package su

var working_site string



func Construct_Site(site, configuration_file_path string){
    // find containers for the master node
    system_containers,startup_containers := determine_master_containers()
    // generate top node for site
    start_site_definitions(site ,configuration_file_path, system_containers, startup_containers)
    // generate data structures for services
    expand_service_definitions()
    // add containers to graph
    add_containers_to_graph()
    // add node to graph
    add_nodes_to_graph()
    // add ending site def
    end_site_definitions(site)
}



func end_site_definitions(site_name string){

   Bc_Rec.End_header_node( "SITE",site_name )

}




func start_site_definitions(site_name , configuration_file_path string, system_containers, startup_containers   []string){
    
    working_site = site_name
    properties := make(map[string]interface{})
	properties["startup_containers"] = startup_containers
	properties["containers"] = system_containers
	properties["file_path"] = configuration_file_path
    Bc_Rec.Add_header_node( "SITE",site_name,  properties  )
      
}


func determine_master_containers()([]string,[]string) {
  return_value_1 := make([]string,0)
  return_value_2 := make([]string,0)
  master_containers :=  find_containers(true,"")
  for _,container := range master_containers {
     temp := container_map[container]
     if temp.temporary == true{
         return_value_2 = append(return_value_2,container)
     }else{
        return_value_1 = append(return_value_1,container)
     }
      
  }
  return return_value_1,return_value_2
}
    
    
    
func add_containers_to_graph(){
  Bc_Rec.Add_header_node( "CONTAINER_LIST","CONTAINER_LIST",make(map[string]interface{}) )  
  expand_container_definitions()
  Bc_Rec.End_header_node( "CONTAINER_LIST","CONTAINER_LIST" )  
}


func add_nodes_to_graph(){
   Bc_Rec.Add_header_node( "NODE_LIST","NODE_LIST",make(map[string]interface{})  )  
   for node,_ := range node_set {
       
       containers := determine_node_containers(node)
        construct_node(node, containers)
       
       
   }
   Bc_Rec.End_header_node( "NODE_LIST","NODE_LIST")     
    
}

func determine_node_containers(node string)[]string { 
  return_value_1 := make([]string,0)
 
  node_containers :=  find_containers(false,node)
  
  for _,container := range node_containers {
     temp := container_map[container]
     if temp.temporary == true{
         panic("temporary containers can only be assigned to master not node container "+container)
     }else{
        return_value_1 = append(return_value_1,container)
     }
      
  }
  return return_value_1
}
