package sys_defs

import "lacima.com/go_setup_containers/site_generation_base/site_generation_utilities"


func generate_tp_monitored_switches(master_flag bool,node_name string ){


   manage_switch_logger_command_map  := make(map[string]string)
   manage_switch_logger_command_map[" manage_switch_logger"] = "./tp_manage_switch_logger"   
   su.Add_container( false,"tp_managed_switch_logger","nanodatacenter/tp_managed_switch_logger", su.Managed_run,manage_switch_logger_command_map, su.Data_mount)
   
   containers := []string{"tp_managed_switch_logger"}
   su.Construct_service_def("tp_managed_switch_logger",master_flag,node_name, containers, tp_construct_monitored_switches_graph) 
    
    
}






func tp_construct_monitored_switches_graph(){ 
   
    
    su.Bc_Rec.Add_header_node("TP_SWITCHES","TP_SWITCHES", make(map[string]interface{}))
    
    
    properties := make(map[string]interface{})
    properties["ip"] = "192.168.1.45"
    su.Bc_Rec.Add_header_node("TP_SWITCH","switch_office",properties)
	su.Construct_incident_logging("switch_office","office_switch",su.Emergency)
    su.Bc_Rec.End_header_node("TP_SWITCH","switch_office")

    properties = make(map[string]interface{})
    properties["ip"] = "192.168.1.56"
    su.Bc_Rec.Add_header_node("TP_SWITCH","switch_garage",properties)
    su.Construct_incident_logging("switch_garage","garage_switch",su.Emergency)
    su.Bc_Rec.End_header_node("TP_SWITCH","switch_garage")
    
    
    su.Bc_Rec.End_header_node("TP_SWITCHES","TP_SWITCHES")
}    
