package cd

import (
 //"fmt"
 "lacima.com/go_setup_containers/site_generation_base/graph_generation/build_configuration"
)
type Package_Constructor struct {
  site                string
  bc                  *bc.Build_Configuration
  name                string
  package_active      bool
  package_properties  map[string]interface{}

}



func Construct_Data_Structures(  bc *bc.Build_Configuration)*Package_Constructor {

   var return_value Package_Constructor
   return_value.bc              = bc
   return_value.package_active  = false   
   return &return_value

}

func ( v *Package_Constructor)Construct_package( package_name string ){
     if v.package_active == true {
	     panic("current package not closed")
	 }
	 v.name = package_name
	 v.package_active = true
     v.package_properties = make(map[string]interface{})
     v.package_properties["data_structures"]  = make(map[string]interface{})
}


func ( v *Package_Constructor)Close_package_construction(){
        v.package_active = false
        v.bc.Add_info_node_and_data_keys("PACKAGE",v.name,v.package_properties)
        
}


func ( v *Package_Constructor)check_for_duplicates( name string ){
       temp := v.package_properties["data_structures"].(map[string]interface{})
       if _,ok := temp[name].(map[string]interface{}); ok==true{
	      panic("duplicate package entries")
	   }

}

func ( v *Package_Constructor)update_entry( name string,properties *map[string]interface{} ){
    temp := v.package_properties["data_structures"].(map[string]interface{})
	temp[name] = (*properties)
     v.package_properties["data_structures"] = temp

}



// table name is in the derived key
func ( v *Package_Constructor)Create_postgres_registry( name,user,password, database_name string ){
       v.check_for_duplicates( name)
	   
	   properties := make(map[string]interface{})
       properties["type"]           = "POSTGRES_Registry"
       properties["name"]           = name  
       properties["user"]           = user
       properties["password"]       = password
       properties["database_name"]  = database_name
      
       v.update_entry(name,&properties) 
}




// table name is in the derived key
func ( v *Package_Constructor)Create_postgres_stream( name,user,password, database_name string ,time_limit int64){
       v.check_for_duplicates( name)
	   
	   properties := make(map[string]interface{})
       properties["type"]           = "POSTGRES_STREAM"
       properties["name"]           = name  
       properties["user"]           = user
       properties["password"]       = password
       properties["database_name"]  = database_name
       properties["time_limit"]     = time_limit

       v.update_entry(name,&properties) 
}

// table name is in the derived key
func ( v *Package_Constructor)Create_postgres_table( name,user,password, database_name string ){
       v.check_for_duplicates( name)
	   
	   properties := make(map[string]interface{})
       properties["type"]           = "POSTGRES_TABLE"
       properties["name"]           = name  
       properties["user"]           = user
       properties["password"]       = password
       properties["database_name"]  = database_name

       v.update_entry(name,&properties) 
}        
        
// table name is in the derived key
func ( v *Package_Constructor)Create_postgres_float( name,user,password, database_name string ){
       v.check_for_duplicates( name)

	   properties := make(map[string]interface{})
       properties["type"]           = "POSTGRES_FLOAT"
       properties["name"]           = name  
       properties["user"]           = user
       properties["password"]       = password
       properties["database_name"]  = database_name

       v.update_entry(name,&properties) 
}        


// table name is in the derived key
func ( v *Package_Constructor)Create_postgres_json( name,user,password, database_name string ){
       v.check_for_duplicates( name)
	   
	   properties := make(map[string]interface{})
       properties["type"]           = "POSTGRES_JSON"
       properties["name"]           = name  
       properties["user"]           = user
       properties["password"]       = password
       properties["database_name"]  = database_name

       v.update_entry(name,&properties) 
}        
   
func ( v *Package_Constructor) Add_single_element(name string){
       v.check_for_duplicates( name)

       properties := make(map[string]interface{})
       properties["name"] = name
       properties["type"]  = "SINGLE_ELEMENT"
        v.update_entry(name,&properties) 
}       
func ( v *Package_Constructor) Add_managed_hash(name string,fields []string){
       v.check_for_duplicates( name)

       properties := make(map[string]interface{})
       properties["name"] = name
       properties["type"]  = "MANAGED_HASH"

       properties["fields"] = fields
       v.update_entry(name,&properties) 
}
      
func ( v *Package_Constructor) Add_hash(name string){
       v.check_for_duplicates( name)

       properties := make(map[string]interface{})
       properties["name"] = name
       properties["type"]  = "HASH"
      
       v.update_entry(name,&properties) 
        
}


func ( v *Package_Constructor) Add_redis_stream(name string,depth int64){
       v.check_for_duplicates( name)

       properties := make(map[string]interface{})
       properties["name"] = name
       properties["type"]  = "STREAM_REDIS"
       properties["depth"]  =depth
    

       v.update_entry(name,&properties) 
}

func ( v *Package_Constructor) Add_zset(name string,depth int64){
       v.check_for_duplicates( name)

       properties := make(map[string]interface{})
       properties["name"] = name
       properties["type"]  = "ZSET_REDIS"
       v.update_entry(name,&properties) 
}









       

       
func ( v *Package_Constructor)Add_job_queue(name string,depth int64){
       v.check_for_duplicates( name)

       properties := make(map[string]interface{})
       properties["name"] = name
       properties["depth"] = depth
       properties["type"]  = "JOB_QUEUE"

      v.update_entry(name,&properties) 
}
      
      
func ( v *Package_Constructor)Add_rpc_server(name string ,depth,timeout int64 ){
       v.check_for_duplicates( name)
       properties:= make(map[string]interface{})
       properties["name"]     = name
       properties["type"]     = "RPC_SERVER"
	   properties["depth"]    = depth
       properties["timeout"]  = timeout
       v.update_entry(name,&properties)  
}
      

