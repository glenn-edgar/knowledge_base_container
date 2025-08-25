import json
import time
from pathlib import Path
from typing import List, Tuple
import sys

cwd = Path.cwd()

build_block_path = cwd / "building_blocks"
sys.path.append(str(build_block_path))
from container_management.container_manager import ContainerManager
from container_management.postgres_connection_waiter import PostgresConnectionWaiter

def determine_initial_containers(config_file: Path, site_db: Path, node_db: Path) -> Tuple[bool, List[str]]:
    """
    Determine which containers to start based on configuration.
    
    Args:
        config_file: Path to the configuration file
        
    Returns:
        Tuple of (is_master: bool, container_list: List[str])
        
    Raises:
        FileNotFoundError: If configuration file doesn't exist
        KeyError: If required fields are missing from config
    """
    
    if not config_file.exists():
        raise FileNotFoundError(f"Configuration file not found: {config_file}")
    

    if not node_db.exists():
        raise FileNotFoundError(f"Node database file not found: {node_db}")
   
    # Load configuration file
    with open(config_file, 'r') as f:
        config = json.load(f)
        
    print(f"Loaded configuration: {config}")
    required_config_fields = ["site", "node_name", "master"]
    for field in required_config_fields:
        if field not in config:
            raise KeyError(f"Required field '{field}' not found in configuration")
    
    
    with open(node_db, 'r') as f:
        container_list = json.load(f)
    return False, container_list


def start_containers(container_list: List[str]) -> None:
     for container in container_list:
         container_manager = ContainerManager(container)
         if container_manager.is_container_running() == False:
             container_manager.start_container()
         else:
             print(f"Container {container} is already running")
         time.sleep(2)
         if container_manager.is_container_running() == False:
             raise RuntimeError(f"Container {container} failed to start")
    
 


def main() -> None:
    
   
    
    home_dir = Path.home()
    
    config_file = home_dir / "mount_startup" / "node_definition.json"
    site_db = home_dir / "mount_startup" / "site_db.json"
    node_db = home_dir / "mount_startup" / "node_db.json"
    postgres_cfg_file = home_dir / "mount_startup" / "postgres_connector.json"


    master_flag, container_list = determine_initial_containers(config_file,site_db,node_db)
    print(f"Master flag: {master_flag}")
    print(f"Containers to manage: {container_list}")
    start_containers(container_list)
    

        
 


if __name__ == "__main__":
    main()