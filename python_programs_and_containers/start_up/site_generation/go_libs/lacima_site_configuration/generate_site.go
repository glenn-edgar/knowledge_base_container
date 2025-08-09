package main

//import "fmt"
import "lacima.com/go_setup_containers/site_generation_base/site_generation_utilities"
import "lacima.com/go_setup_containers/site_generation_base/system_definitions"





func main(){
    
  
  system_properties := make(map[string]interface{})
  system_properties["jquery_files"] = 7
  su.Construct_System("farm_system",4,system_properties) 
  /*
   *  now construct lacima site
   */
  generate_lacima_site()

  
  /*
   * generate other sites
   * if needed
   */
  su.End_System()
  su.Done() //finalize graph
  
 
}

func generate_lacima_site(){
  su.Initialize_Site_Enviroment()
  setup_lacima_container_mount_points()
  setup_lacima_nodes()
  add_lacima_components()
  su.Construct_Site("LACIMA_SITE","/home/pi/system_config/redis_configuration.json")    

}






func setup_lacima_container_mount_points(){
  drive_path     := "--mount type=bind,source=/home/pi/system_config/,target=/data/"  // path to get configuration data
  file_path      := "--mount type=bind,source=/home/pi/mountpoint/files/,target=/files/"   // path for file server to get files
  redis_path     := "--mount type=bind,source=/home/pi/mountpoint/redis/,target=/data/"  // path for redis server to store data
  secret_path    := "--mount type=bind,source=/home/pi/mountpoint/secrets/,target=/secrets/"
  postgres_path  := "--mount type=bind,source=/home/pi/mountpoint/postgres_files/,target=/var/lib/postgresql/data"
  
  su.Setup_Mount_Points()  
  su.Add_mount_point("DATA",drive_path)
  su.Add_mount_point("FILE",file_path)
  su.Add_mount_point("REDIS_DATA",redis_path)    
  su.Add_mount_point("SECRETS",secret_path)
  su.Add_mount_point("POSTGRES",postgres_path)
  

    
}

func setup_lacima_nodes(){
    
   su.Add_node("site_controller") 
   su.Add_node("irrigation_controller")
   
   
    
    
}    

func add_lacima_components(){
    
    sys_defs.Add_Component_To_Master("system_component")
    sys_defs.Add_Component_To_Node("site_controller", "irrigation")
    sys_defs.Add_Component_To_Node("site_controller", "tp_managed_switch")
    
}    

   
