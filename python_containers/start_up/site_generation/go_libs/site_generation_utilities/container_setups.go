package su

//import "fmt"
import "strings"

type container_descriptor struct {
    temporary          bool
    managed_container  bool
    docker_image       string
    command_string     string
    command_map map[string]string

}


var drive_mounts map[string]string

var container_map map[string]container_descriptor

var command_string_first_part string  // continually execute container
var command_string_run_part string   // container executes a script and terminates

func initialialize_container_data_structures(start_part,run_part string){
   
   container_map = make(map[string]container_descriptor)
   command_string_first_part = start_part
   command_string_run_part =  run_part
}


func Setup_Mount_Points(){
    
  drive_mounts = make(map[string]string)    
    
}


/*
 * 
 *  All mount points have to be registers
 *  This is important as there will be many container configurations
 *  Registering the mount points will allow changing mountpoints to be done without changing each script
 * 
 */

func Add_mount_point( mount_name string , mount_path string ){
 
   if _,ok := drive_mounts[mount_name]; ok == true {
     panic("duplicate mount name "+mount_name)
   }

   drive_mounts[mount_name] = mount_path


}

/*
 *  temp_flag bool  -- true container runs continually
 *                  -- false container executes a function and terminates
 *                  -- container_name  -- name of constructed container
 *                                        note at system boot all containers are created from their images
 *                                        this allow updates to be applied on system startup_command
 *                  -- command_string --- name of the container controller.
 *                                        container controller allows multiple processes to run within the container
 *                  -- command_map    --- is the command_string for each of the processes which run inside the container
 * 
 * 
 * 
 */

func Add_container( temp_flag bool, container_name, docker_image, command_string string ,command_map map[string]string, mounts []string){
   
   var expanded_mount []string
   //fmt.Println("################################          "+container_name+"                @@@@@@@@@@@@@@@@@@@@@@@@@@@@@@")
   //fmt.Println(" mounts   ",mounts)
   for _,name := range mounts {
      //fmt.Println("name-mount",name,"",drive_mounts[name])
      expanded_mount = append( expanded_mount,  drive_mounts[name] )
   
   }
   //fmt.Println("expanded_mount----------------------------",expanded_mount)
   if _,ok := container_map[container_name]; ok == true {
     panic("duplicate container name "+container_name)
   }

   var temp container_descriptor
   if command_string == Managed_run {
      temp.managed_container = true
   }else{
    temp.managed_container = false
   }
   temp.command_map = command_map
   temp.docker_image = docker_image
   if temp_flag == false {
        temp.temporary = false
        temp.command_string = command_string_first_part+"  "+container_name+"  "+strings.Join(expanded_mount,"  ")+" "+docker_image+" "+command_string
   }else{
      temp.temporary = true
      temp.command_string = command_string_run_part+"  "+container_name+"  "+strings.Join(expanded_mount,"  ")+" "+docker_image+" "+command_string
   }
   //fmt.Println("temp++++++++++++++++++++++++++++++++++++",temp)
   container_map[container_name] = temp
   
}

/*
 * 
 *  register containers writes containers to configuration graph
 *  this call can be done in two places.  At the system level where the
 *  specified containers run on the master node.  At the node level
 *  where the containers run on the specified node.  The master node can
 *  have containers allocated at the system and node level
 * 
 * 
 */


func register_containers( container_list []string ){
 
   for _,container_name := range container_list {
       //fmt.Println("container name",container_name)
       register_container(container_name)
   
   
   }

}


func register_container( container_name string){
   if _,ok := container_map[container_name]; ok == false{
      panic("container does not exist  "+container_name)
   }
   var description string
   // properties of the container
   properties := make(map[string]interface{})
   properties["container_image"] = container_map[container_name].docker_image
   properties["startup_command"] = container_map[container_name].command_string
   properties["command_map"] = container_map[container_name].command_map
   Bc_Rec.Add_header_node("CONTAINER",container_name,properties)
   
   // these streams contain performance data for the container
   description = container_name+" container resource"
   Construct_streaming_logs("container_resource",description,[]string{"vsz","rss","cpu"})
   if container_map[container_name].managed_container == true {
       
       // this is a log of the container controller failure
       description = container_name+" process_control_failure"
       Construct_incident_logging("process_control_failure",description,Emergency)
   
       // this is a log of failures for processes that container controller manages
       description = container_name+" managed_process_failure"
       Construct_incident_logging("managed_process_failure",description,Emergency)
   
       // this is a watchdog failure log for this container
       description = container_name+" container controller watchdog"
       Construct_watchdog_logging("process_control",description,20)
   }
   Cd_Rec.Construct_package("CONTAINER_STRUCTURES" )
   Cd_Rec.Add_hash("PROCESS_STATUS")
   Cd_Rec.Close_package_construction()
   Bc_Rec.End_header_node("CONTAINER",container_name)
}



