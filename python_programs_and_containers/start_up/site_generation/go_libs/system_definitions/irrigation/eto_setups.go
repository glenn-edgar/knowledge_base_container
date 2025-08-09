package irrigation

import (
    
    "lacima.com/go_setup_containers/site_generation_base/site_generation_utilities"

)



func Construct_weather_stations(){
 
    
    su.Bc_Rec.Add_header_node("WEATHER_DATA_STRUCTURE","WEATHER_DATA_STRUCTURE",make(map[string]interface{}))
    
    su.Bc_Rec.Add_header_node("ROLL_OVER","ROLL_OVER",make(map[string]interface{}))
    su.Construct_incident_logging("ETO_MONITOR_ROLLOVER","eto_monitor_day_rollover",su.Alert)
    su.Bc_Rec.End_header_node("ROLL_OVER","ROLL_OVER")
    
    su.Bc_Rec.Add_header_node("ETO_UPDATE","ETO_UPDATE",make(map[string]interface{}))
    su.Construct_incident_logging("ETO_MONITOR_UPDATE_ETO","eto_monitor_update_eto",su.Alert)
    su.Bc_Rec.End_header_node("ETO_UPDATE","ETO_UPDATE")
    
    
    su.Cd_Rec.Construct_package("WEATHER_DATA")
    su.Cd_Rec.Add_hash("ETO_CONTROL") 
    su.Cd_Rec.Add_hash("EXCEPTION_VALUES")
	su.Cd_Rec.Add_hash("RAIN_VALUES") // rain flag and stuff
    su.Cd_Rec.Add_hash("ETO_VALUES") 
    su.Cd_Rec.Add_hash("ETO_STREAM_DATA")
	su.Cd_Rec.Create_postgres_stream( "ETO_HISTORY","admin","password","admin",2*365*24*3600)
    su.Cd_Rec.Create_postgres_stream( "RAIN_HISTORY","admin","password","admin",2*365*24*3600)
   
	su.Cd_Rec.Close_package_construction()
    
    su.Bc_Rec.End_header_node("WEATHER_DATA_STRUCTURE","WEATHER_DATA_STRUCTURE")
    
    su.Bc_Rec.Add_header_node("WEATHER_STATIONS","WEATHER_STATIONS",make(map[string]interface{}))
    add_station_cimis()
    add_station_cimis_station()
    add_station_messo_west_sruc1_eto()
    add_station_messo_west_sruc1_rain()
    add_station_wunderground()
    add_station_wunderground_hybrid()
    
    su.Bc_Rec.End_header_node("WEATHER_STATIONS","WEATHER_STATIONS")
    
    
    
    
}


func add_station_wunderground(){
    
 
  
  properties := make(map[string]interface{})
  properties["access_key"]     = "WUNDERGROUND"
  properties["type"]           = "WUNDERGROUND"
  properties["sub_id"]         = "KCAMURRI101"
  properties["lat"]            = float64(33.2)
  properties["alt"]            = float64(2400.)
  properties["priority"]       = 1000
  properties["hybrid_flag"]    = 0
  su.Bc_Rec.Add_info_node("WEATHER_STATION","WUNDERGROUND",properties)
}       

func add_station_cimis_station() {
    
       
    properties := make(map[string]interface{})
    properties["sub_id"]        = "177:33"
    properties["access_key"]     = "ETO_CIMIS_SATELLITE"
    properties["type"]           = "CIMIS_SAT"
    properties["url"]            = "http://et.water.ca.gov/api/data"
    properties["longitude"]      = float64(-117.29945)
    properties["latitude"]       = float64(33.578156)
    properties["priority"]       = 4     
    properties["hybrid_flag"]    = 0
    su.Bc_Rec.Add_info_node("WEATHER_STATION","ETO_CIMIS_SATELLITE",properties)
}

func add_station_cimis(){
    
 
    properties := make(map[string]interface{})
    properties["sub_id"]        = "62"
    properties["access_key"]     = "ETO_CIMIS"
    properties["type"]           = "CIMIS"
    properties["url"]            = "http://et.water.ca.gov/api/data"
    properties["station"]        = "62"
    properties["priority"]       = 2  
    properties["hybrid_flag"]    = 0
    su.Bc_Rec.Add_info_node("WEATHER_STATION","ETO_CIMIS",properties)
}


func add_station_messo_west_sruc1_eto() {

    properties := make(map[string]interface{})
    properties["sub_id"]         = "SRUC1"
    properties["access_key"]     = "MESSOWEST"
    properties["type"]           = "MESSO_ETO"
    properties["url"]            = "http://api.mesowest.net/v2/stations/timeseries"
    properties["station"]        = "SRUC1"
    properties["altitude"]       = float64(2400.)
    properties["latitude"]       = float64(33.578156)
    properties["priority"]       = 3 
    properties["hybrid_flag"]    = 0
    su.Bc_Rec.Add_info_node("WEATHER_STATION","SRUC1",properties)
}

func add_station_messo_west_sruc1_rain() {
 
    properties := make(map[string]interface{})
    properties["sub_id"]        = "SRUC1"
    properties["access_key"]     = "MESSOWEST"
    properties["type"]           = "MESSO_RAIN"
    properties["url"]            =  "http://api.mesowest.net/v2/stations/precip"
    properties["station"]        = "SRUC1"
    properties["priority"]       = 3  
    properties["hybrid_flag"]    = 0
    su.Bc_Rec.Add_info_node("WEATHER_STATION","SRUC1_RAIN",properties)
          
}       

       
func add_station_wunderground_hybrid(){
    
    var setup Hybrid_station_type
    setup.sub_type          = "KCAMURRI101"
    setup.priority          = 5
    setup.altitude          = 2400.
    setup.latitude          = float64(33.578156)
    setup.base_type         = "WUNDERGROUND"
    setup.base_sub_type     = "KCAMURRI101"
    setup.variant_type      = "MESSO_ETO"
    setup.variant_sub_type  = "SRUC1"
    setup.variant_fields    = []string{"SolarRadiationWatts_m_squared"}
    add_hybrid_station(setup)
    
}

type Hybrid_station_type struct{
    sub_type     string
    priority         int
    altitude         float64
    latitude        float64
    base_type        string
    base_sub_type    string
    variant_type     string
    variant_sub_type string
    variant_fields    []string
}


var valid_map =  map[string]bool{"wind_speed":true,"temp_C":true,"humidity":true,"SolarRadiationWatts_m_squared":true }

func check_for_valid_fields( input  []string ){
    for _,value := range input {
        if _,ok := valid_map[value];ok== false{
            panic("bad hybrid field "+value)
        }
    }
}



func add_hybrid_station( r Hybrid_station_type ){
    
    check_for_valid_fields( r.variant_fields)
    
    properties := make(map[string]interface{})
    properties["sub_id"]                = r.sub_type
    properties["type"]                  = "HYBRID_SETUP"    
    properties["base_type"]             = r.base_type
    properties["base_sub_type"]         = r.base_sub_type
    properties["variant_type"]          = r.variant_type
    properties["variant_sub_type"]      = r.variant_sub_type
    properties["variant_fields"]        = r.variant_fields
    properties["priority"]              = r.priority
    properties["altitude"]              = r.altitude
    properties["latitude"]              = r.latitude
    properties["hybrid_flag"]           = 1
    su.Bc_Rec.Add_info_node("WEATHER_STATION","WUNDERGROUND_HYBRID:"+r.sub_type,properties)       
}
