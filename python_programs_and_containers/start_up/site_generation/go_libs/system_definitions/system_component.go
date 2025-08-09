package sys_defs
import "lacima.com/go_setup_containers/site_generation_base/site_generation_utilities"
import "lacima.com/go_setup_containers/site_generation_base/system_definitions/mqtt_in"
import "lacima.com/go_setup_containers/site_generation_base/system_definitions/mqtt_out"
import "lacima.com/go_setup_containers/site_generation_base/system_definitions/error_detection"


const redis_image                    string   = "nanodatacenter/redis"
const lacima_site_image              string   = "nanodatacenter/lacima_site_generation"
const lacima_secrets_image           string   = "nanodatacenter/lacima_secrets"
const file_server_image              string   = "nanodatacenter/file_server"
const redis_monitor_image            string   = "nanodatacenter/redis_monitoring"
const postgres_image                 string   = "nanodatacenter/postgres"
const mqtt_image                     string   = "nanodatacenter/mosquitto"
const mqtt_to_db_image               string   = "nanodatacenter/mqtt_to_db"
const system_error_detection_image   string   = "nanodatacenter/system_error_detection"
const alert_generation_image         string   = "nanodatacenter/alert_generation"

func generate_system_components(master_flag bool,node_name string ){
   file_server_mount    := []string {"DATA","FILE"}
   redis_mount          := []string{"REDIS_DATA"}
   secrets_mount        := []string{"DATA","SECRETS"}
   postgres_mount       := []string{"DATA","POSTGRES"}
   mqtt_mount           := []string{}
   
   redis_monitor_command_map  := make(map[string]string)
   redis_monitor_command_map["redis_monitor"] = "./redis_monitor"

   file_server_command_map  := make(map[string]string)
   file_server_command_map["file_server"] = "./file_server"   

   mqtt_to_db_command_map  := make(map[string]string)
   mqtt_to_db_command_map["mqtt_to_db"] = "./mqtt_to_db"
   mqtt_to_db_command_map["mqtt_out"]  = "./mqtt_out"
   
   system_error_detection_command_map  := make(map[string]string)
   system_error_detection_command_map["wd_monitoring"] = "./wd_monitoring"  
   system_error_detection_command_map["incident_monitoring"] = "./incident_monitoring"  
   system_error_detection_command_map["rpc_monitoring"] = "./rpc_monitoring"  
   system_error_detection_command_map["stream_monitoring"] = "./stream_monitoring"  
   system_error_detection_command_map["web_services"] = "./web_services"  
   
   alert_generation_command_map  := make(map[string]string)
   alert_generation_command_map["telegram"] = "./telegram"   
    
   null_map := make(map[string]string)
   su.Add_container( false,"redis", redis_image, "./redis_control.bsh", null_map,redis_mount)
   su.Add_container( true, "lacima_site_generation",lacima_site_image,su.Temp_run ,null_map, su.Data_mount )
   su.Add_container( true, "lacima_secrets",lacima_secrets_image,su.Temp_run ,null_map, secrets_mount)
   su.Add_container( false, "file_server",file_server_image, su.Managed_run ,file_server_command_map ,file_server_mount)
   su.Add_container( false,"postgres",postgres_image,su.No_run, null_map, postgres_mount)
   su.Add_container( false,"mqtt",mqtt_image,su.No_run, null_map, mqtt_mount)
   su.Add_container( false,"mqtt_to_db",mqtt_to_db_image, su.Managed_run,mqtt_to_db_command_map, su.Data_mount)
   su.Add_container( false,"redis_monitor",redis_monitor_image, su.Managed_run,redis_monitor_command_map, su.Data_mount)
   su.Add_container( false,"alert_generation",alert_generation_image, su.Managed_run,alert_generation_command_map, su.Data_mount)
   su.Add_container( false,"system_error_detection",system_error_detection_image, su.Managed_run,system_error_detection_command_map, su.Data_mount)
   
   
   containers := []string{"redis","lacima_secrets","file_server","postgres","mqtt","mqtt_to_db","redis_monitor","alert_generation","system_error_detection"}
   su.Construct_service_def("system_monitoring",master_flag,"", containers, generate_system_component_graph) 
    
    
}


func generate_system_component_graph(){
    
    su.Cd_Rec.Construct_package("DATA_MAP")
    su.Cd_Rec.Add_single_element("DATA_MAP") // map of site data
    su.Cd_Rec.Close_package_construction()
    
    su.Construct_incident_logging("SITE_REBOOT","site_reboot",su.Emergency)
    
    su.Cd_Rec.Construct_package("REBOOT_FLAG")
    su.Cd_Rec.Add_single_element("REBOOT_FLAG") // determine if site has done all initialization
    su.Cd_Rec.Close_package_construction()
    
    
    su.Cd_Rec.Construct_package("ENVIRONMENTAL_VARIABLES")
    su.Cd_Rec.Add_single_element("ENVIRONMENTAL_VARIABLES") // determine if site has done all initialization
    su.Cd_Rec.Close_package_construction()    
    
    su.Cd_Rec.Construct_package("NODE_MAP")
    su.Cd_Rec.Add_hash("NODE_MAP") // map of node ip's
    su.Cd_Rec.Close_package_construction()
    
    port_map_properties                         := make(map[string]interface{})
    port_map                                    := make(map[string]string)
    port_map["site_controller"]                 = ":8080"
    port_map["mqtt_to_db"]                      = ":2021"
    port_map["mqtt_status_out"]                 = ":2022"
    port_map["error_detection"]                 = ":2023"
    port_map["eto"]                                     = ":2024"
    port_map["irrigation_setup"]                = ":2025"
    port_map["irrigation_manage"]                = ":2026"
    
    port_description_map                        := make(map[string]string)
    port_description_map["site_controller"]     = "Site Controller Web Site"
    port_description_map["mqtt_to_db"]          = "MQTT TO DB Web Services"
    port_description_map["mqtt_status_out"]     = "MQTT OUTPUT Web Services"
    port_description_map["error_detection"]     = "Display Error Monitoring Results"
    port_description_map["eto"]                 = "ETO Weather Station Results"
    port_description_map["irrigation_setup"]    = "Irrigation Setup"
    port_description_map["irrigation_manage"] ="Irrigation Manage"
    
    port_start_label_map                        := make(map[string]string)
    port_start_label_map["site_controller"]       = "site_controller"
    port_start_label_map["mqtt_to_db"]            = "mqtt_to_db"
    port_start_label_map["mqtt_status_out"]    = "mqtt_status_out"
    port_start_label_map["eto"]                            = "eto"
    port_start_label_map["irrigation_setup"]     = "irrigation_setup"
    port_start_label_map["irrigation_manage"] = "irrigation_manage"
   port_start_label_map["error_detection"]     ="error_detection"
    
    
    port_map_properties["port_map"]             = port_map
    port_map_properties["description"]          = port_description_map
    port_map_properties["start_label"]          = port_start_label_map
    
    su.Bc_Rec.Add_info_node("WEB_MAP","WEB_MAP",port_map_properties)
    
    
    port_mqtt_id_properties                     := make(map[string]interface{})
    port_mqtt_id_map                            := make(map[string]string)
    port_mqtt_id_map["mqtt_input_server"]       = "mqtt_input_server"
    port_mqtt_id_map["mqtt_output_server"]      = "mqtt_output_server"
    
    port_mqtt_id_properties["mqtt_client_id_map"]  = port_mqtt_id_map
    su.Bc_Rec.Add_info_node("MQTT_CLIENT_ID","MQTT_CLIENT_ID",port_mqtt_id_properties )    
    
    su.Cd_Rec.Construct_package("WEB_IP")
    su.Cd_Rec.Add_hash("WEB_IP")           // map of all subsystem web servers
    su.Cd_Rec.Close_package_construction()    
    
    su.Construct_incident_logging("CONTAINER_ERROR_STREAM" ,"container error stream",su.Emergency)
    
    su.Cd_Rec.Construct_package("DOCKER_CONTROL")
    su.Cd_Rec.Add_hash("DOCKER_DISPLAY_DICTIONARY")
    su.Cd_Rec.Close_package_construction()
    
    
    
    su.Construct_RPC_Server("SYSTEM_CONTROL","rpc for controlling system",10,15, make( map[string]interface{}) )

    su.Cd_Rec.Construct_package("NODE_STATUS")
    su.Cd_Rec.Add_hash("NODE_STATUS")
    su.Cd_Rec.Close_package_construction()
   
    
   
    su.Bc_Rec.Add_header_node("REDIS_MONITORING","REDIS_MONITORING",make(map[string]interface{}))
    su.Construct_incident_logging("REDIS_MONITORING","redis_monitor",su.Emergency)
    su.Cd_Rec.Construct_package("REDIS_MONITORING")
    su.Cd_Rec.Add_single_element("REDIS_MONITORING")
    su.Cd_Rec.Close_package_construction() 
    su.Bc_Rec.End_header_node("REDIS_MONITORING","REDIS_MONITORING")
   
    file_server_properties := make(map[string]interface{})
    file_server_properties["directory"] = "/files"
    su.Construct_RPC_Server( "SITE_FILE_SERVER","site_file_server",30,10,file_server_properties)
    
    su.Bc_Rec.Add_header_node("POSTGRES_TEST","driver_test",make(map[string]interface{}))
    su.Construct_postgres_streaming_logs("postgres driver test","postgress_test","admin","password","admin",30*24*3600)
    su.Cd_Rec.Construct_package("POSTGRES_REGISTY_TEST")
    su.Cd_Rec.Create_postgres_registry("postgress_registry_test","admin","password","admin" )
    su.Cd_Rec.Close_package_construction()
    su.Bc_Rec.End_header_node("POSTGRES_TEST","driver_test")
    
    mqtt_in.Construct_mqtt_in_defintions()
    mqtt_out.Construct_mqtt_out_definitions()
    error_detection.Construct_definitions()
   
    
}
