from typing import Dict, List, Callable, Any, Optional
import sys_utilities as su  # Assuming this module exists
import mqtt_in
import mqtt_out
import error_detection


class SystemDefinitions:
    """Python translation of the Go sys_defs package for managing system components"""
    
    # Docker image constants
    REDIS_IMAGE = "nanodatacenter/redis"
    LACIMA_SITE_IMAGE = "nanodatacenter/lacima_site_generation"
    LACIMA_SECRETS_IMAGE = "nanodatacenter/lacima_secrets"
    FILE_SERVER_IMAGE = "nanodatacenter/file_server"
    REDIS_MONITOR_IMAGE = "nanodatacenter/redis_monitoring"
    POSTGRES_IMAGE = "nanodatacenter/postgres"
    MQTT_IMAGE = "nanodatacenter/mosquitto"
    MQTT_TO_DB_IMAGE = "nanodatacenter/mqtt_to_db"
    SYSTEM_ERROR_DETECTION_IMAGE = "nanodatacenter/system_error_detection"
    ALERT_GENERATION_IMAGE = "nanodatacenter/alert_generation"
    
    def __init__(self):
        """Initialize the system definitions with component dictionary"""
        self.system_dict: Dict[str, Callable[[bool, str], None]] = {
            "system_component": self.generate_system_components,
            "tp_managed_switch": self.generate_tp_monitored_switches,
            "irrigation": self.construct_irrigation,
        }
    
    def generate_system_components(self, master_flag: bool, node_name: str) -> None:
        """Generate system components with containers and configurations"""
        
        # Mount configurations
        file_server_mount = ["DATA", "FILE"]
        redis_mount = ["REDIS_DATA"]
        secrets_mount = ["DATA", "SECRETS"]
        postgres_mount = ["DATA", "POSTGRES"]
        mqtt_mount = []
        
        # Command maps for different containers
        redis_monitor_command_map = {
            "redis_monitor": "./redis_monitor"
        }
        
        file_server_command_map = {
            "file_server": "./file_server"
        }
        
        mqtt_to_db_command_map = {
            "mqtt_to_db": "./mqtt_to_db",
            "mqtt_out": "./mqtt_out"
        }
        
        system_error_detection_command_map = {
            "wd_monitoring": "./wd_monitoring",
            "incident_monitoring": "./incident_monitoring",
            "rpc_monitoring": "./rpc_monitoring",
            "stream_monitoring": "./stream_monitoring",
            "web_services": "./web_services"
        }
        
        alert_generation_command_map = {
            "telegram": "./telegram"
        }
        
        null_map = {}
        
        # Add containers using the utilities module
        su.add_container(False, "redis", self.REDIS_IMAGE, "./redis_control.bsh", null_map, redis_mount)
        su.add_container(True, "lacima_site_generation", self.LACIMA_SITE_IMAGE, su.TEMP_RUN, null_map, su.DATA_MOUNT)
        su.add_container(True, "lacima_secrets", self.LACIMA_SECRETS_IMAGE, su.TEMP_RUN, null_map, secrets_mount)
        su.add_container(False, "file_server", self.FILE_SERVER_IMAGE, su.MANAGED_RUN, file_server_command_map, file_server_mount)
        su.add_container(False, "postgres", self.POSTGRES_IMAGE, su.NO_RUN, null_map, postgres_mount)
        su.add_container(False, "mqtt", self.MQTT_IMAGE, su.NO_RUN, null_map, mqtt_mount)
        su.add_container(False, "mqtt_to_db", self.MQTT_TO_DB_IMAGE, su.MANAGED_RUN, mqtt_to_db_command_map, su.DATA_MOUNT)
        su.add_container(False, "redis_monitor", self.REDIS_MONITOR_IMAGE, su.MANAGED_RUN, redis_monitor_command_map, su.DATA_MOUNT)
        su.add_container(False, "alert_generation", self.ALERT_GENERATION_IMAGE, su.MANAGED_RUN, alert_generation_command_map, su.DATA_MOUNT)
        su.add_container(False, "system_error_detection", self.SYSTEM_ERROR_DETECTION_IMAGE, su.MANAGED_RUN, system_error_detection_command_map, su.DATA_MOUNT)
        
        containers = [
            "redis", "lacima_secrets", "file_server", "postgres", "mqtt",
            "mqtt_to_db", "redis_monitor", "alert_generation", "system_error_detection"
        ]
        
        su.construct_service_def("system_monitoring", master_flag, "", containers, self.generate_system_component_graph)
    
    def generate_system_component_graph(self) -> None:
        """Generate the system component graph with various configurations"""
        
        # Construct data packages
        su.cd_rec.construct_package("DATA_MAP")
        su.cd_rec.add_single_element("DATA_MAP")  # map of site data
        su.cd_rec.close_package_construction()
        
        su.construct_incident_logging("SITE_REBOOT", "site_reboot", su.EMERGENCY)
        
        su.cd_rec.construct_package("REBOOT_FLAG")
        su.cd_rec.add_single_element("REBOOT_FLAG")  # determine if site has done all initialization
        su.cd_rec.close_package_construction()
        
        su.cd_rec.construct_package("ENVIRONMENTAL_VARIABLES")
        su.cd_rec.add_single_element("ENVIRONMENTAL_VARIABLES")  # determine if site has done all initialization
        su.cd_rec.close_package_construction()
        
        su.cd_rec.construct_package("NODE_MAP")
        su.cd_rec.add_hash("NODE_MAP")  # map of node ip's
        su.cd_rec.close_package_construction()
        
        # Port mapping configuration
        port_map = {
            "site_controller": ":8080",
            "mqtt_to_db": ":2021",
            "mqtt_status_out": ":2022",
            "error_detection": ":2023",
            "eto": ":2024",
            "irrigation_setup": ":2025",
            "irrigation_manage": ":2026"
        }
        
        port_description_map = {
            "site_controller": "Site Controller Web Site",
            "mqtt_to_db": "MQTT TO DB Web Services",
            "mqtt_status_out": "MQTT OUTPUT Web Services",
            "error_detection": "Display Error Monitoring Results",
            "eto": "ETO Weather Station Results",
            "irrigation_setup": "Irrigation Setup",
            "irrigation_manage": "Irrigation Manage"
        }
        
        port_start_label_map = {
            "site_controller": "site_controller",
            "mqtt_to_db": "mqtt_to_db",
            "mqtt_status_out": "mqtt_status_out",
            "eto": "eto",
            "irrigation_setup": "irrigation_setup",
            "irrigation_manage": "irrigation_manage",
            "error_detection": "error_detection"
        }
        
        port_map_properties = {
            "port_map": port_map,
            "description": port_description_map,
            "start_label": port_start_label_map
        }
        
        su.bc_rec.add_info_node("WEB_MAP", "WEB_MAP", port_map_properties)
        
        # MQTT client ID configuration
        port_mqtt_id_map = {
            "mqtt_input_server": "mqtt_input_server",
            "mqtt_output_server": "mqtt_output_server"
        }
        
        port_mqtt_id_properties = {
            "mqtt_client_id_map": port_mqtt_id_map
        }
        
        su.bc_rec.add_info_node("MQTT_CLIENT_ID", "MQTT_CLIENT_ID", port_mqtt_id_properties)
        
        su.cd_rec.construct_package("WEB_IP")
        su.cd_rec.add_hash("WEB_IP")  # map of all subsystem web servers
        su.cd_rec.close_package_construction()
        
        su.construct_incident_logging("CONTAINER_ERROR_STREAM", "container error stream", su.EMERGENCY)
        
        su.cd_rec.construct_package("DOCKER_CONTROL")
        su.cd_rec.add_hash("DOCKER_DISPLAY_DICTIONARY")
        su.cd_rec.close_package_construction()
        
        su.construct_rpc_server("SYSTEM_CONTROL", "rpc for controlling system", 10, 15, {})
        
        su.cd_rec.construct_package("NODE_STATUS")
        su.cd_rec.add_hash("NODE_STATUS")
        su.cd_rec.close_package_construction()
        
        # Redis monitoring configuration
        su.bc_rec.add_header_node("REDIS_MONITORING", "REDIS_MONITORING", {})
        su.construct_incident_logging("REDIS_MONITORING", "redis_monitor", su.EMERGENCY)
        su.cd_rec.construct_package("REDIS_MONITORING")
        su.cd_rec.add_single_element("REDIS_MONITORING")
        su.cd_rec.close_package_construction()
        su.bc_rec.end_header_node("REDIS_MONITORING", "REDIS_MONITORING")
        
        # File server configuration
        file_server_properties = {
            "directory": "/files"
        }
        su.construct_rpc_server("SITE_FILE_SERVER", "site_file_server", 30, 10, file_server_properties)
        
        # PostgreSQL configuration
        su.bc_rec.add_header_node("POSTGRES_TEST", "driver_test", {})
        su.construct_postgres_streaming_logs("postgres driver test", "postgress_test", "admin", "password", "admin", 30*24*3600)
        su.cd_rec.construct_package("POSTGRES_REGISTY_TEST")
        su.cd_rec.create_postgres_registry("postgress_registry_test", "admin", "password", "admin")
        su.cd_rec.close_package_construction()
        su.bc_rec.end_header_node("POSTGRES_TEST", "driver_test")
        
        # Construct MQTT and error detection definitions
        mqtt_in.construct_mqtt_in_definitions()
        mqtt_out.construct_mqtt_out_definitions()
        error_detection.construct_definitions()
    
    def generate_tp_monitored_switches(self, master_flag: bool, node_name: str) -> None:
        """Generate TP monitored switches - placeholder implementation"""
        # This function was referenced but not implemented in the original Go code
        pass
    
    def construct_irrigation(self, master_flag: bool, node_name: str) -> None:
        """Construct irrigation system - placeholder implementation"""
        # This function was referenced but not implemented in the original Go code
        pass
    
    def add_component_to_master(self, component_name: str) -> None:
        """Add a component to the master node"""
        self._check_system_components(component_name)
        self.system_dict[component_name](True, "")
    
    def add_component_to_node(self, node_name: str, component_name: str) -> None:
        """Add a component to a specific node"""
        self._check_system_components(component_name)
        self.system_dict[component_name](False, node_name)
    
    def _check_system_components(self, system_component: str) -> None:
        """Check if system component exists, raise exception if not"""
        if system_component not in self.system_dict:
            raise ValueError(f"non existent component {system_component}")


# Usage example:
if __name__ == "__main__":
    sys_def = SystemDefinitions()
    
    # Add component to master
    sys_def.add_component_to_master("system_component")
    
    # Add component to specific node
    sys_def.add_component_to_node("node1", "system_component")