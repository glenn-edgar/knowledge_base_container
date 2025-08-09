package bc  // build configuration

import "fmt"
import "context"
import "encoding/json"
import "strconv"
import "sort"
//import "strings"
//import "import "container/list"
import "github.com/go-redis/redis/v8"
import  "lacima.com/Patterns/sets"


var ctx    = context.TODO()
var client *redis.Client


type Build_Configuration struct {

   sep                string
   rel_sep            string
   label_sep          string
   namespace          *name_space_type
   keys               *set.Set 

}



  

func Graph_support_init(address string, port int) {
 
	var address_port = address+":"+strconv.Itoa(port)
	client = redis.NewClient(&redis.Options{
                                                 Addr: address_port,
												
												 DB: 3,
                                               })
	err := client.Ping(ctx).Err();     
	if err != nil{
	         panic("redis graph connection")
	 }
    fmt.Println("redis graph ping")	
}   



func Construct_build_configuration( ) *Build_Configuration {

   var return_value Build_Configuration
   return_value.sep                 = "["
   return_value.rel_sep             = ":"
   return_value.label_sep           = "]"
   return_value.namespace           = construct_namespace_manager()
   return_value.keys                = set.New()
  
   client.FlushDB(ctx)
   
   return &return_value
}



 




func (v *Build_Configuration) Add_header_node( relation,label string, properties map[string]interface{} ){
  
   properties["name"] = label
   v.construct_node( true, relation, label, properties )
}


func (v *Build_Configuration)  Add_info_node( relation,label string, properties map[string]interface{}  ){
     
   properties["name"] = label
   v.construct_node( false, relation, label, properties )
}

func (v *Build_Configuration)  Add_info_node_and_data_keys( relation,label string, properties map[string]interface{}  ){
     
   properties["name"] = label
   v.construct_node( false, relation, label, properties )
   v.construct_data_structure_keys(relation,label,properties)
}

func (v *Build_Configuration)construct_data_structure_keys( relation,label string, properties map[string]interface{}  ){
    data_structures := properties["data_structures"].(map[string]interface{})
    v.namespace.push_namespace( relation,label)
    name_space:=  v.convert_namespace()
    (*v.namespace).pop_namespace()
    
     
    for _,v := range data_structures{
        k := v.(map[string]interface{}) 
        key := name_space +"["+k["type"].(string)+":"+k["name"].(string) +"]"
        client.HSet(ctx,"data_set",key,k["type"]) // used a a set later by remove superious key
        client.RPush(ctx,"data_list",key)
   }
    
    
}    
    
func (v *Build_Configuration) End_header_node( assert_relation,assert_label string ){

       last_relation,last_label := (*v.namespace).pop_namespace()
	   
	   
	   if (last_relation != assert_relation)||(last_label != assert_label) {
	      panic("unmatched namespace ")
	   }

}

func (v *Build_Configuration)construct_node( push_namespace bool,relationship ,label string  ,properties map[string]interface{}  ){
 

       redis_key := v.construct_basic_node( push_namespace, relationship,label  ) 
	   
       if v.keys.Has(redis_key ) == true {
           panic("Duplicate Key "+redis_key)
	   }
	  
      v.keys.Insert(redis_key)
	  for key,value := range properties {
	      	b, err := json.Marshal(value)
	        if err != nil {
		       panic("json marshall error ")
	        }
	        client.HSet(ctx,redis_key,key,b)
	  }
	  
	  
      
}



func (v *Build_Configuration) Check_namespace(  ){
       if  (*v.namespace).namespace_len != 0{
	     fmt.Println(v.namespace)
	     panic("non zero name space")
	   }
}


   
  
func (v *Build_Configuration) construct_basic_node( push_namespace bool,  relationship,label string )string{
       
       v.namespace.push_namespace( relationship,label)
       redis_string :=  v.convert_namespace()

       redis_string_json,err1  := json.Marshal(redis_string)  
	   label_json ,err2        := json.Marshal(label)
       if (err1 != nil) || (err2 != nil) {
          panic("bad json")
        }		  

       client.HSet(ctx,redis_string,"namespace",redis_string_json)
	   client.HSet(ctx,redis_string,"name",label_json)
       v.update_terminals( relationship, label, redis_string)
       v.update_relationship( redis_string )
       
	   if push_namespace == false {
	       (*v.namespace).pop_namespace()
		}
	   return redis_string
}



 
func (v *Build_Configuration)  update_relationship(   redis_string string ){
       for _,value := range (*v.namespace).namespace{
	       relationship := value[0]
		   label        := value[1]
           client.SAdd(ctx,"@RELATIONSHIPS",relationship)
           client.SAdd(ctx,"%"+relationship,redis_string)
           client.SAdd(ctx,"#"+relationship+v.rel_sep+label,redis_string)
		}
}


func (v *Build_Configuration)update_terminals( relationship ,label, redis_string string ){
       client.SAdd(ctx,"@TERMINALS",relationship)
       client.SAdd(ctx,"&"+relationship,redis_string)
       client.SAdd(ctx,"$"+relationship+v.rel_sep+label,redis_string)
}

 
func (v *Build_Configuration)  Store_keys( ){
    for i,_ := range (*(*v.keys).Get_hash_map()) {
       client.SAdd(ctx,"@GRAPH_KEYS", i )
	}
}  

func (v *Build_Configuration)      Store_dictionary(){
  keys, _ := client.Keys(ctx,"*").Result()
  sort.Strings(keys)
  for _,key := range keys {
     client.HSet(ctx,"key_set",key,"true")
     client.RPush(ctx,"key_list",key)
      
  }
       
    
}
