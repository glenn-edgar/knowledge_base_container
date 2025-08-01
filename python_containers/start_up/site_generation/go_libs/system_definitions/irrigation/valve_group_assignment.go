package irrigation

import (
   // "fmt"
    "lacima.com/go_setup_containers/site_generation_base/site_generation_utilities"
)


var io_description_map  map[string]map[int64]string

var valve_group_io            map[string]interface{}
var valve_group_names  []string



func Add_valve_group_definitions(){
  
 io_description_map    =  make(map[string]map[int64]string)
  valve_group_io           = make(map[string]interface{})
  valve_group_names   = make([]string,0)
  add_valve_group_1( )
   add_valve_group_2( )
   add_valve_group_3( )
   add_valve_group_4( )
    add_valve_group_5( )
  add_valve_group_6( )
   add_valve_group_7( )
   add_valve_group_8( )
   add_valve_group_9( )
   add_valve_group_10( )
   properties := make(map[string]interface{})
   properties["valve_io"]                           = valve_group_io
   properties["valve_group_names"]      = valve_group_names
   properties["io_map"]           = io_description_map
   
  
  su.Bc_Rec.Add_info_node("VALVE_GROUP_DEFS","VALVE_GROUP_DEFS",properties)
}    

func add_valve_group_entry( name ,controller string, channel int64){
 
    
    if _, ok := io_description_map[controller] ; ok == false {
         temp := make(map[int64]string)
         io_description_map[controller] = temp
    }
    io_description_map[controller][channel]    = name 
   valve_descriptions = append(valve_descriptions,name)
   stations                   = append(stations,controller)
    io                   = append(io,channel)
    
}

var valve_descriptions []string
var stations   []string
var io            []int64

func  valve_group_init(){
    valve_descriptions = make([]string,0)
    stations                   = make([]string,0)
    io                              = make([]int64,0)
}

func valve_group_dump(name, description string){
  valve_group_names = append(valve_group_names,name)
  temp := make(map[string]interface{})
  temp["description"] = description
  temp["valve_descriptions"] = valve_descriptions
  temp["stations"] = stations
  temp["io"]             = io
  valve_group_io[name]= temp
  
   
    
}

func add_valve_group_1(){
    name            :=  "valve group 1"
    description  := "xxxxxxxxxxxxxxx"
    valve_group_init()
    add_valve_group_entry(   "Flowers along side walk" ,"station_1",20)
    add_valve_group_entry(  "Well Water Drip Line","station_1",19)
    add_valve_group_entry(  "Barbecue Clover Area","station_1",14)
    add_valve_group_entry(  "Well Clover Area","station_1",18)
    add_valve_group_entry(  "Triangle Pool Area","station_1",15)
    add_valve_group_entry(  "Dragon Fruit — Fruit Tree Drip Line","station_1",17)
   add_valve_group_entry(  "Pool Fence Area","station_1",16)
    
   valve_group_dump(name,description)
}

func add_valve_group_2(){
    name            :=  "valve group 2"
     description  := "xxxxxxxxxxxxxxx"
    valve_group_init()
    add_valve_group_entry(   "Lemon Tree Drip Line near Steps" ,"station_1",13)
    add_valve_group_entry(  "Middle Clover Near Well" ,"station_1",25)
     add_valve_group_entry(  "Middle Clover Near Barbecue","station_1",12)
    add_valve_group_entry(  "Drip Line along garage","station_1",11)
    
 valve_group_dump(name,description)

}
func add_valve_group_3(){
    name            :=  "valve group 3"
     description  := "xxxxxxxxxxxxxxx"
    valve_group_init()
    add_valve_group_entry(    "Flowers Toward Garage" ,"station_1",24)
    add_valve_group_entry(  "Flowers on Opposite Side of Garage" ,"station_1",21)
    add_valve_group_entry(   "Grass Zone Away From Door","station_1",22)
    add_valve_group_entry(  "Grass Toward Door","station_1",23)
    
valve_group_dump(name,description)
}
    

func add_valve_group_4(){
    name            :=  "valve group 4"
     description  := "xxxxxxxxxxxxxxx"
    valve_group_init()
    add_valve_group_entry(    "Upper area Near Property Line"  ,   "station_2"     ,2)
    add_valve_group_entry(   "On Property Side of Valve #3"     ,    "station_2"     ,4)
    add_valve_group_entry(   "Next to Valve 4"                             ,     "station_2"    ,3)
    add_valve_group_entry(   "Spray Area Next to Remote"       ,     "station_2"    ,1)
    add_valve_group_entry(    "Sprayers along Drive Way"         ,      "station_2"   ,5)
    add_valve_group_entry(    "Lower area near Property Line" ,      "station_2"   ,6)    
     add_valve_group_entry(    "Drip Line along bank" ,                      "station_2"   ,7)    
 valve_group_dump(name,description)
}

func add_valve_group_5(){
    name            :=  "valve group 5"
     description  := "xxxxxxxxxxxxxxx"
    valve_group_init()  
    add_valve_group_entry(    "Fruit Trees on Block #9"  ,                    "station_4"     ,9)
    add_valve_group_entry(   "Waters Bank Closest to House"     ,    "station_4"     ,10)
    add_valve_group_entry(   "Sprinkler on Bank"                             ,    "station_4"    ,11)
    add_valve_group_entry(   "Avocado Block6"       ,                            "station_4"    ,12)
    add_valve_group_entry(    "Fruit Trees on Block #9"  ,                    "station_4"     ,13)
    add_valve_group_entry(   "Waters Bank Closest to House"     ,    "station_4"     ,14)
    add_valve_group_entry(   "Sprinkler on Bank"                             ,    "station_4"    ,15)
    add_valve_group_entry(   "Avocado Block6"       ,                            "station_4"    ,16)
    
valve_group_dump(name,description)
}
  

func add_valve_group_6(){
    name            :=  "valve group 6"
     description  := "xxxxxxxxxxxxxxx"
    valve_group_init()
    add_valve_group_entry(     "Avocado Block #4 Top",                             "station_2"     ,13)
    add_valve_group_entry(  "Baby Tress"     ,                                                 "station_2"     ,14)
    add_valve_group_entry(  "Avocado Block 4 Bottom",                             "station_2"    ,15)
    add_valve_group_entry(    "Drip Line and Sprayers along Road"   ,     "station_2"    ,17)
    add_valve_group_entry(    "Block 3 Androus Site",                                 "station_2"   ,16)
    add_valve_group_entry(     "No Connection",                                           "station_2"   , 20)    
     add_valve_group_entry(     "No Connection",                                          "station_2"   ,21 )    
valve_group_dump(name,description)
}

    

func add_valve_group_7(){
    name            :=  "valve group 7"
     description  := "xxxxxxxxxxxxxxx"
    valve_group_init()
    add_valve_group_entry(     "????????????????",                             "station_3"   ,4)
    add_valve_group_entry(       "????????????"     ,                               "station_3"   ,3)
    add_valve_group_entry(       "Avocado Block 5",                             "station_3"   ,1)
    add_valve_group_entry(       "????????????????"   ,                          "station_3"   ,6)
    add_valve_group_entry(       "Baby Tres Cindy Side",                     "station_3"   ,5)
    add_valve_group_entry(     "???????????????????",                          "station_3"   ,2)    
 valve_group_dump(name,description)
}
     
    
func add_valve_group_8(){
    name            :=  "valve group 8"
     description  := "xxxxxxxxxxxxxxx"
    valve_group_init()    
    add_valve_group_entry(     "Block 1",                                    "station_3"   ,13)
    add_valve_group_entry(       "Block 2"     ,                               "station_3"   ,14)
    add_valve_group_entry(       "Block 1.5" ,                                 "station_3"   ,18)
    add_valve_group_entry(       "????????????????"   ,                 "station_3"   ,17)
    add_valve_group_entry(      "Baby Tres Cindy Side",             "station_3"   ,21)
    add_valve_group_entry(     "Block 3 Cindy Side",                   "station_3"   ,15)    
    add_valve_group_entry(     "???????????????????",                 "station_3"   ,19)    
valve_group_dump(name,description)
}      

     
func add_valve_group_9(){
    name            :=  "valve group 9"
     description  := "xxxxxxxxxxxxxxx"
    valve_group_init()
    add_valve_group_entry(       "Bottom Hill on Cindys House Side",                                    "station_3"   ,11)
    add_valve_group_entry(       "Bottom Hill on the opposite site of drive way"    ,              "station_3"   ,12)
 valve_group_dump(name,description)
}
func add_valve_group_10(){
    name            :=  "valve group 10"
     description  := "xxxxxxxxxxxxxxx"
    valve_group_init()
    add_valve_group_entry(     "??????????????????",                 "station_4"   ,1)
    add_valve_group_entry(      "?????????????"     ,                     "station_4"   ,2)
    add_valve_group_entry(      "?????????????"     ,                    "station_4"   ,3)
    add_valve_group_entry(       "????????????????"   ,                 "station_4"   ,4)
    add_valve_group_entry(      "?????????????????",                   "station_4"   ,5)
    add_valve_group_entry(     "??????????????????",                   "station_4"   ,6)    
    add_valve_group_entry(     "???????????????????",                 "station_4"   ,7)    
    add_valve_group_entry(       "????????????????"   ,                 "station_4"   ,8)
    
valve_group_dump(name,description)
}
    
    
    
