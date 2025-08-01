package mqtt_out
import "lacima.com/go_setup_containers/site_generation_base/site_generation_utilities"


type topic_type struct {
    name          string
    description   string
    handler_type  string
}

type class_type struct {
  
    name          string
    description   string
    instance_map    map[string]string
    instance_list   []string
    topic_list    []string
    
    
}

type instance_type struct {
    name        string
    description string
    class       string
}




var topic_map  map[string]topic_type

var class_map  map[string]class_type

var instance_map  map[string]instance_type

func mqtt_structure_init(){
    topic_map  = make(map[string]topic_type)
    class_map  = make(map[string]class_type)
    instance_map = make(map[string]instance_type)
}


func verify_topic(topic_list []string){
    
    for _,topic := range topic_list {
      if _,ok := topic_map[topic]; ok == false {
        panic("topic doesnot exit")
      }
    }
}        

func verify_class(class string ){
    
    if _,ok := class_map[class]; ok == false {
       panic("nonexistant mqtt class")
    }
}


func add_topic( name, description,handler_type string){
   
    if _,ok := topic_map[name]; ok==true{
      panic("duplicate topic "+name)
      
    }
    
    var topic  topic_type
    topic.name          = name
    topic.description   = description
    topic.handler_type  = handler_type
 
    topic_map[name] = topic
    
}

func add_class( name, description string ,topic_list []string, ){
   
    if _,ok := class_map[name]; ok==true{
      panic("duplicate class "+name)
      
    }
    verify_topic(topic_list)
    
    var class  class_type
    class.name          = name
    class.description   = description
    class.topic_list    = topic_list
    class.instance_map    = make(map[string]string)
   
 
    class_map[name] = class
    
}

func add_instance( name,class, description string){

    if _,ok := instance_map[name]; ok==true{
      panic("duplicate instance "+name)
      
    }
    verify_class(class)
    
    var instance       instance_type
    instance.name          = name
    instance.class         = class
    instance.description   = description
    

    instance_map[name] = instance
    class_map[class].instance_map[name] = "true"
    
}

func generate_list( input map[string]string )[]string{
   
    
    return_value := make([]string,len(input))
    i := 0
    for key,_ := range input{
        
        return_value[i] = key
        i = i+1
    }
    
    return return_value
}


func construct_instance_list(){
   instance_list := make(map[string][]string)
   for key, element := range class_map{
       instance_list[key] = generate_list(element.instance_map)
   }
   for key,item := range instance_list {
      element := class_map[key]
      element.instance_list = item
      class_map[key] = element
   }
}
    
func topic_map_conversion()map[string]interface{}{
    
   return_value := make(map[string]interface{})
   for key,element := range topic_map {
       item := make(map[string]interface{})
       item["name"]         = element.name
       item["description"]  = element.description
       item["handler_type"] = element.handler_type
       return_value[key] = item
   }
   return return_value
}

func class_map_conversion()map[string]interface{}{
    
   return_value := make(map[string]interface{})
   for key,element := range class_map {
       item := make(map[string]interface{})
       item["name"]            = element.name
       item["description"]     = element.description
       item["instance_list"]   = element.instance_list   
       item["topic_list"]      = element.topic_list
       return_value[key]       = item
   }
   return return_value
}

func instance_map_conversion()map[string]interface{}{
    
   return_value := make(map[string]interface{})
   for key,element := range instance_map {
       item := make(map[string]interface{})
       item["name"]         = element.name
       item["description"]  = element.description
       item["class"]        = element.class
       return_value[key] = item
   }
   return return_value
}
   


func add_topics(){
   add_topic( "heart_beat","string output","string" )
  
    
}
  
  
func add_classes(){
  add_class( "mqtt_output", "class for mqtt heartbeat" ,[]string{"heart_beat" } )
 
  
}


func add_instances(){
    
    add_instance( "test_message","mqtt_output", "used as an test of mqtt ouptut status")
    
    
}



func Construct_mqtt_out_definitions() {



  
  su.Bc_Rec.Add_header_node("MQTT_OUTPUT_SETUP","site_out_server",make(map[string]interface{}))
  su.Construct_incident_logging("MQTT_TX_CONNECTION_LOST","MQTT_TX_CONNECTION_LOST",su.Error)
  su.Construct_RPC_Server( "MQTT_OUT_RPC_SERVER","MQTT_OUT_RPC_SERVER",30,10,make(map[string]interface{}))
  su.Cd_Rec.Construct_package("TOPIC_STATUS")
  su.Cd_Rec.Add_hash("TOPIC_ERROR_TIME_STAMP")   // a full length topic and a marshalled data value
  su.Cd_Rec.Add_hash("TOPIC_VALUE")   // a full length topic and a marshalled data value
  su.Cd_Rec.Add_hash("TOPIC_TIME_STAMP") // a full length topic and a unix time in seconds as a string
  su.Cd_Rec.Add_hash("TOPIC_HANDLER")
  su.Cd_Rec.Create_postgres_stream( "POSTGRES_DATA_STREAM","admin","password","admin",30*24*3600)
  su.Cd_Rec.Close_package_construction()
  
  
  mqtt_structure_init()
  add_topics()
  add_classes()
  add_instances()
  construct_instance_list()
  
  properties := make(map[string]interface{})
  properties["topics"]  = topic_map_conversion()
  properties["classes"] = class_map_conversion()
  properties["instances"] = instance_map_conversion()
  su.Bc_Rec.Add_info_node("MQTT_INSTANCES","MQTT_INSTANCES",properties)
  
  su.Bc_Rec.End_header_node("MQTT_OUTPUT_SETUP","site_out_server")
}

