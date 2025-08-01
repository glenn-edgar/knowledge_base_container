package su // site_utilities


import "lacima.com/site_data"
import "lacima.com/go_setup_containers/site_generation_base/graph_generation/build_configuration"
import "lacima.com/go_setup_containers/site_generation_base/graph_generation/construct_data_structures"
	
var config_file = "/data/redis_configuration.json"
var site_data_store map[string]interface{}

var Ip    string
var Port  int

var Bc_Rec *bc.Build_Configuration 
var Cd_Rec *cd.Package_Constructor

var Data_mount = []string{"DATA"}
var Null_mount = []string{}
const No_run string       = ""
const Temp_run string     = "./run.bsh"
const Managed_run string  = "./process_control.bsh"

var system_name string

func Construct_System(sys_name string ,data_db   int,properties map[string]interface{}){
    properties["data_db"] = data_db
    setup_Site_File()
    bc.Graph_support_init(Ip,Port)
    Bc_Rec = bc.Construct_build_configuration()
    Cd_Rec = cd.Construct_Data_Structures(Bc_Rec)
    system_name = sys_name
    Bc_Rec.Add_header_node( "SYSTEM",system_name,  properties  )
    
}

func End_System(){

   Bc_Rec.End_header_node( "SYSTEM",system_name )

}

func setup_Site_File(){

	site_data_store = get_site_data.Get_site_data(config_file)
    
	Ip   = site_data_store["host"].(string)
	Port = int(site_data_store["port"].(float64))
   
}

func Initialize_Site_Enviroment(){

  init_service_generation()
  setup_container_run_commands()
  
    
}
  



const command_start string = "docker run -d  --network host --log-driver  local  --name"          // preambe script to start container
const command_run   string = "docker run   -it --network host --log-driver  local  --rm  --name"  // preamble script for container to run and exit


func setup_container_run_commands(){
    
  initialialize_container_data_structures(command_start,command_run)   
    
}



func Done(){

 Bc_Rec.Check_namespace()
 Bc_Rec.Store_keys() 
 Bc_Rec.Store_dictionary()


}

