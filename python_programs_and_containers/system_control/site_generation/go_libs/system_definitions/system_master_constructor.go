package sys_defs


/*
 * This is the generic part of building a system
 * 
 * 
 * 
 * 
 */

type system_dict_type func(bool,string )

var system_dict = map[string]system_dict_type{ 
        "system_component": generate_system_components ,
        "tp_managed_switch":generate_tp_monitored_switches,
        "irrigation":construct_irrigation,
}



func Add_Component_To_Master(component_name string){
     
    check_system_components(component_name )
    system_dict[component_name](true,"")    
    
}


func Add_Component_To_Node(node_name string,component_name string){
    
    check_system_components(component_name )
    system_dict[component_name](false,node_name)    
    
}



  



func check_system_components( system_component string ){
    if _,ok := system_dict[system_component]; ok == false{
        panic("non existant compontent "+system_component)
    }
}
