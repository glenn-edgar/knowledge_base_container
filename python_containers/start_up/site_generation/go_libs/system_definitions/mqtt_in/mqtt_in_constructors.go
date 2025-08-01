package mqtt_in
import "lacima.com/go_setup_containers/site_generation_base/site_generation_utilities"


type topic_type struct {
    name          string
    description   string
    handler_type  string
}

type class_type struct {
  
    name          string
    description   string
    device_map    map[string]string
    device_list   []string
    topic_list    []string
    contact_time  int64
    
}

type device_type struct {
    name        string
    description string
    class       string
}




var topic_map  map[string]topic_type

var class_map  map[string]class_type

var device_map  map[string]device_type

func mqtt_structure_init(){
    topic_map  = make(map[string]topic_type)
    class_map  = make(map[string]class_type)
    device_map = make(map[string]device_type)
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

func add_class( name, description string ,topic_list []string,contact_time int64 ){
   
    if _,ok := class_map[name]; ok==true{
      panic("duplicate class "+name)
      
    }
    verify_topic(topic_list)
    
    var class  class_type
    class.name          = name
    class.description   = description
    class.topic_list    = topic_list
    class.device_map    = make(map[string]string)
    class.contact_time  = contact_time
 
    class_map[name] = class
    
}

func add_device( name,class, description string){

    if _,ok := device_map[name]; ok==true{
      panic("duplicate device "+name)
      
    }
    verify_class(class)
    
    var device       device_type
    device.name          = name
    device.class         = class
    device.description   = description
    

    device_map[name] = device
    class_map[class].device_map[name] = "true"
    
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


func construct_device_list(){
   device_list := make(map[string][]string)
   for key, element := range class_map{
       device_list[key] = generate_list(element.device_map)
   }
   for key,item := range device_list {
      element := class_map[key]
      element.device_list = item
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
       item["name"]         = element.name
       item["description"]  = element.description
       item["contact_time"] = element.contact_time
       item["device_list"]  = element.device_list   
       item["topic_list"]   = element.topic_list
       return_value[key] = item
   }
   return return_value
}

func device_map_conversion()map[string]interface{}{
    
   return_value := make(map[string]interface{})
   for key,element := range device_map {
       item := make(map[string]interface{})
       item["name"]         = element.name
       item["description"]  = element.description
       item["class"]        = element.class
       return_value[key] = item
   }
   return return_value
}
   


func add_topics(){
   add_topic( "test_string","test of string topic","string" )
   add_topic( "test_int","test of integer topic","int32" )
   add_topic( "test_float","test of float topic","float64" )
   add_topic( "test_map","test of a message pack topic","map[string]interface" )
   add_topic( "test_array","test of a message pack topic","[]float32" )
    
}
  
  
func add_classes(){
  add_class( "test_class", "class for self test unit" ,[]string{"test_string","test_int","test_float","test_map","test_array"    }, 60 )
  
}


func add_devices(){
    add_device( "test_device","test_class", "used as a self check for mqtt system")
    
    
}



func Construct_mqtt_in_defintions() {

  
  su.Bc_Rec.Add_header_node("MQTT_IN_SETUP","site_in_server",make(map[string]interface{}))
  su.Construct_incident_logging("MQTT_RX_CONNECTION_LOST","MQTT_RX_CONNECTION_LOST",su.Error)
  su.Cd_Rec.Construct_package("TOPIC_STATUS")
  su.Cd_Rec.Add_hash("TOPIC_ERROR_TIME_STAMP")   // a full length topic and a marshalled data value
  su.Cd_Rec.Add_hash("TOPIC_VALUE")   // a full length topic and a marshalled data value
  su.Cd_Rec.Add_hash("TOPIC_TIME_STAMP") // a full length topic and a unix time in seconds as a string
  su.Cd_Rec.Add_hash("DEVICE_STATUS") // for all devices  the status of the device values "true" or "false"
  su.Cd_Rec.Add_hash("DEVICE_TIME_STAMP") // for all devices  the status of the device values "true" or "false"
  su.Cd_Rec.Add_hash("TOPIC_HANDLER")
  su.Cd_Rec.Create_postgres_stream( "POSTGRES_DATA_STREAM","admin","password","admin",30*24*3600)
  su.Cd_Rec.Create_postgres_stream( "POSTGRES_INCIDENT_STREAM","admin","password","admin",30*24*3600)
  su.Cd_Rec.Create_postgres_stream( "POSTGRES_SYS_STREAM","admin","password","admin",30*24*3600)
  
  su.Cd_Rec.Close_package_construction()
  
  
  mqtt_structure_init()
  add_topics()
  add_classes()
  add_devices()
  construct_device_list()
  
  properties := make(map[string]interface{})
  properties["topics"]  = topic_map_conversion()
  properties["classes"] = class_map_conversion()
  properties["devices"] = device_map_conversion()
  su.Bc_Rec.Add_info_node("MQTT_DEVICES","MQTT_DEVICES",properties)
  
  su.Bc_Rec.End_header_node("MQTT_IN_SETUP","site_in_server")
}

