import json
import time
import shutil
from pathlib import Path
from typing import List, Tuple
import sys

cwd = Path.cwd()
build_block_path = cwd / "building_blocks"
sys.path.append(str(build_block_path))

home_dir = Path.home()
build_block_path = home_dir/ "knowledge_base_assembly" / "python_programs_and_containers" / "building_blocks"
sys.path.append(str(build_block_path))
from container_management.container_manager import ContainerManager
from container_management.postgres_connection_waiter import PostgresConnectionWaiter

class NodeControl:
    def __init__(self, force_update:bool=False,
                 node_def_file:Path=None,
                 update_file:Path=None,
                 site_db_file:Path=None,
                 node_db_file:Path=None,
                 postgres_cfg_file:Path=None,
                 script_dir:Path=None,
                 config_defs:dict=None,
                 config_name:str=None):
        
        if isinstance(force_update, bool) is False:
            raise ValueError("Force update must be a boolean")
        
        self.force_update = force_update
        home_dir = Path.home()
        
        if node_def_file is not None:
            self.node_def_file = node_def_file
        else:
            self.node_def_file = home_dir / "mount_startup" / "node_definition.json"
            
        if update_file is not None:
            self.update_file = update_file
        else:
            self.update_file = home_dir / "mount_startup" / "update.json"
            
        if site_db_file is not None:
            self.site_db_file = site_db_file
        else:
            self.site_db_file = home_dir / "mount_startup" / "site_db.json"
            
        if node_db_file is not None:
            self.node_db_file = node_db_file
        else:
            self.node_db_file = home_dir / "mount_startup" / "node_db.json"
            
        if postgres_cfg_file is not None:
            self.postgres_cfg_file = postgres_cfg_file
        else:
            self.postgres_cfg_file = home_dir / "mount_startup" / "postgres_connector.json"
            
        if script_dir is not None:
            self.script_dir = script_dir
        else:
            self.script_dir = home_dir / "mount_kb_definitions"

        if config_defs is not None:
            self.config_defs = config_defs
        else:
            self.config_defs = {}
            
        if config_name is not None:
            self.config_name = config_name
        else:
            self.config_name = ""
       
    def validate_config_files_and_associated_containers(self):
        if not self.node_def_file.exists():
            raise FileNotFoundError(f"Configuration file not found: {self.node_def_file}")
        if not self.site_db_file.exists():
            raise FileNotFoundError(f"Site database file not found: {self.site_db_file}")
        if not self.node_db_file.exists():
            raise FileNotFoundError(f"Node database file not found: {self.node_db_file}")
        if not self.postgres_cfg_file.exists():
            raise FileNotFoundError(f"Postgres configuration file not found: {self.postgres_cfg_file}")
        if not self.update_file.exists():
            raise FileNotFoundError(f"Update file not found: {self.update_file}")
        
        required_config_fields = ["site", "node_name", "master"]
        with open(self.node_def_file, 'r') as f:
            self.node_def_data = json.load(f)
        for field in required_config_fields:
            if field not in self.node_def_data:
                raise KeyError(f"Required field '{field}' not found in configuration")
        required_update_fields = ["update_flag","config_name"]
        with open(self.update_file, 'r') as f:
            self.update_file_data = json.load(f)
        for field in required_update_fields:
            if field not in self.update_file_data:
                raise KeyError(f"Required field '{field}' not found in update file")
        if isinstance(self.update_file_data["update_flag"], bool) is False:
            raise ValueError("Update flag must be a boolean")
        if isinstance(self.update_file_data["config_name"], str) is False:
            raise ValueError("Config name must be a string")
       
        
        with open(self.postgres_cfg_file, 'r') as f:
            self.postgres_cfg_data = json.load(f)
        required_fields = ["host", "port", "dbname", "user", "password"]
        for field in required_fields:
            if field not in self.postgres_cfg_data:
                raise KeyError(f"Required field '{field}' not found in configuration")   
            
        with open(self.site_db_file, 'r') as f:
            self.site_db_data = json.load(f)
        with open(self.node_db_file, 'r') as f:
            self.node_db_data = json.load(f)
        if self.node_def_data["master"]:
            # verify postgres container
            self.master_containers = self.site_db_data
            if not isinstance(self.master_containers, list):
                raise ValueError("Master containers must be a list")
            for container in self.master_containers:
                if not ContainerManager.container_exists(container):
                    raise RuntimeError(f"Container '{container}' not found")
                
                
        
        self.node_containers = self.node_db_data
        
        if not isinstance(self.node_containers, list):
            raise ValueError("Node containers must be a list")
        for container in self.node_containers:
            if not ContainerManager.container_exists(container):
                raise RuntimeError(f"Container '{container}' not found")
            
        if self.script_dir.is_dir() is False:
            raise FileNotFoundError(f"Script directory not found: {self.script_dir}")
        if isinstance(self.config_defs, dict) is False:
            raise ValueError("Config definitions must be a dictionary")
        if isinstance(self.config_name, str) is False:
            raise ValueError("Config name must be a string")
        if self.config_name == None:
            self.config_name = self.update_file_data["config_name"]
        if self.config_name != "":
            if self.config_name not in self.config_defs:
                raise ValueError(f"Config name '{self.config_name}' not found in config definitions")
            self.transfer_dir = self.config_defs[self.config_name]["config_dir"]
            if self.transfer_dir.is_dir() is False:
                raise FileNotFoundError(f"Transfer directory not found: {self.transfer_dir}")
        
        
            
    def start_containers(self,containers:list,sleep_time:int=10):
         
         for container in containers:
            manager = ContainerManager(container)
            manager.start_container()
            start_time = 0
            while start_time < sleep_time:
                if manager.is_container_running():
                    break
                time.sleep(1)
                start_time += 1
            if start_time == sleep_time:
                raise RuntimeError(f"Container '{container}' failed to start after {sleep_time} seconds")
            print(f"Container '{container}' started after {start_time} seconds")
            
    
    def start_node_containers(self):
        self.start_containers(self.node_containers)
        
    def start_master_containers(self):
        self.start_containers(self.master_containers)
        
    def verify_postgres_connection_cfg(self) -> Tuple[str, str, str, str, str]:
        required_fields = ["host", "port", "dbname", "user", "password"]
        for field in required_fields:
            if field not in self.postgres_cfg_data:
                raise KeyError(f"Required field '{field}' not found in configuration")
        return self.postgres_cfg_data["host"], self.postgres_cfg_data["port"], self.postgres_cfg_data["dbname"], self.postgres_cfg_data["user"], self.postgres_cfg_data["password"]
    
    def verify_postgres_connection(self) -> None:
        host, port, database, user, password = self.verify_postgres_connection_cfg()
        
        postgres_connection_waiter = PostgresConnectionWaiter(host=host, port=port, database=database, user=user, password=password)
        if postgres_connection_waiter.wait_for_connection() == False:
            raise RuntimeError("Failed to connect to postgres")
        test_connection = postgres_connection_waiter.test_connection()
        print(f"Test connection: {test_connection}")
        print("Postgres connection verified")  
    
    
    def load_configuration_scripts(self):
        #
        for item in self.script_dir.iterdir():
            try:
                if item.is_file():
                    item.unlink()
                    print(f"Removed file: {item}")
                elif item.is_dir():
                    shutil.rmtree(item)
                    print(f"Removed directory: {item}")
            except Exception as e:
                print(f"Warning: Could not remove {item}: {e}")
    
        # Copy files from transfer_dir to exec_dir
        print(f"Copying files from {self.transfer_dir} to {self.exec_dir}")
        files_transferred = []
        
        for item in self.transfer_dir.iterdir():
            try:
                if item.is_file():
                    shutil.copy2(item, self.exec_dir)
                    files_transferred.append(item.name)
                    print(f"Copied file: {item.name}")
                elif item.is_dir():
                    dest_dir = self.exec_dir / item.name
                    shutil.copytree(item, dest_dir)
                    files_transferred.append(f"{item.name}/")
                    print(f"Copied directory: {item.name}")
            except Exception as e:
                print(f"Error copying {item}: {e}")
                # Optionally, you might want to raise here or continue
                # depending on your error handling requirements
        
        print(f"Configuration data is loaded. Files transferred: {files_transferred}")
        return True , files_transferred
    
    def initialize_nast_topics(self):
        pass
    
    def execute_startup(self):       
        self.validate_config_files_and_associated_containers()
        self.start_node_containers()
        self.initialize_nast_topics()
        if self.node_def_data["master"]:
            self.start_master_containers()
            self.verify_postgres_connection()
            print(f"Update flag: {self.update_file_data['update_flag']}")
            print(f"Force update: {self.force_update}")
            if self.update_file_data["update_flag"] is True or self.force_update is True:
                print(f"Updating configuration for {self.config_name}")
                if self.config_name != "":
                    self.load_configuration_scripts()
                pass # execute configuration script
                pass # do file loader
                pass # do secrets loader
            pass ### set up master fields on postgres container
        else:
            pass ## find master nasts
            pass #connect to postgres db
        # start application containers
        
        
    def run(self):
        self.execute_startup()
        # monitor system
        sys.exit(0)
         
            

    
if __name__ == "__main__":
    kb_config_dir = Path.cwd() / "kb_configs"
    kb_config_name = "kb_test"
    config_defs = {kb_config_name: {"config_dir": kb_config_dir}}
    node_control = NodeControl(force_update=True, config_defs=config_defs, config_name=kb_config_name)
    node_control.run()


 
