package su // site_utilities

/*
 *  Support utilities for site generation
 * 
 */


// function to generate data structures
type service_graph_generation func()



type service_definition struct {
   name               string
   master_flag        bool
   node               string
   containers         []string
   graph_generation   service_graph_generation  
}

var service_map      map[string]service_definition
var service_list     []service_definition
var node_set         map[string]bool
var container_set    map[string]bool


func init_service_generation(){
  
    service_map    = make(map[string]service_definition)
    service_list    = make([]service_definition,0)
    node_set  = make(map[string]bool)  // make sure that node is defined and only one
    container_set  = make(map[string]bool)  // make sure a container is defined and used only once
   
}



func expand_container_definitions(){
   for _,element := range service_list {
      register_containers(element.containers)
   }
       
}

func expand_service_definitions(){
 
    for _,element := range service_list {
        element.graph_generation()
    }
    
    
}

func find_containers(master_flag bool, node string )[]string{
   return_value := make([]string,0)
   
   for _,element := range service_list {
      
      if element.master_flag != master_flag{
          continue
      }
      if master_flag == true {
          
          return_value = add_containers(return_value,element.containers)
      }else if node == element.node {
          
          return_value = add_containers(return_value,element.containers)
      }
       
   }
   return return_value
}



func add_containers( input []string, new_elements []string )[]string {
    for _,element := range new_elements {
        input = append(input,element)
    }
    return input
}


func Add_node( node_name string ){
 
    check_for_duplicate_node(node_name)
    node_set[node_name] = true
}

func Construct_service_def(service_name string,master_flag bool, node_name string, containers []string, graph_generation   service_graph_generation){
   
    var service_element service_definition
    check_for_duplicate_system(service_name)
    register_service_containers(containers)
    service_element.name   = service_name      
    service_element.master_flag   = master_flag
    service_element.node   = node_name
    service_element.containers    = containers
    service_element.graph_generation = graph_generation
   
    service_map[service_name] = service_element
    service_list = append(service_list,service_element)
    
    
}  






func check_for_duplicate_node( node_name string){
    if _,ok := node_set[node_name]; ok== true {
       panic("duplicate node")
    }    
    
}

func check_for_existing_node( node_name string ){
    
     if _,ok := node_set[node_name]; ok== false {
       panic("node not defined")
    }
}    

func check_for_duplicate_system( service_name string){
     if _,ok := service_map[service_name]; ok== true {
       panic("duplicate system")
    }     
    
}

func check_for_duplicate_container( container string ){
    
    
    if _,ok := container_set[container]; ok== true {
       panic("duplicate container")
    }      
    
}

func register_service_containers(containers []string){
    
    for _,container := range containers{
        check_for_duplicate_container(container)
        container_set[container] = true
        
    }
    
    
    
}














