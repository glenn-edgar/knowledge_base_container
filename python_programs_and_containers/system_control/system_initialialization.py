import os
import json
from postgres_manager import Postgres_Manager
from knowledge_base.construct_kb.construct_data_tables import Construct_Data_Tables
from knowledge_base.data_structures.kb_data_structures import KB_Data_Structures
class System_Initialization:
    def __init__(self):
        self.read_mount_data()
        print(self.postgres_connector)
        self.postgres_manager = Postgres_Manager(self.postgres_connector)
        system_create = os.getenv('system_create')
        secrets_load = os.getenv('secrets_load')
        print(system_create)
        print(secrets_load)
        host = self.postgres_connector['host']
        port = self.postgres_connector['port']
        dbname = self.postgres_connector['dbname']
        user = self.postgres_connector['user']
        password = self.postgres_connector['password']
        base_table = self.postgres_connector['base_table']
        print("postgres connector", self.postgres_connector)
        if system_create == 'true':
            self.construct_data_tables = Construct_Data_Tables(host, port, dbname, user, password, base_table)
            
        self.kb_data_structures = KB_Data_Structures(host, port, dbname, user, password, base_table)
        print("done")
        #postgres has started at this point
        #load knowledge base connection
 
        
        
    
    


    def read_mount_data(self):
        """Read and parse JSON files from /mount_startup directory."""
        mount_path = "/mount_startup"
        
        try:
            # Read file_key.json
            file_key_path = os.path.join(mount_path, "file_key.json")
            with open(file_key_path, 'r') as f:
                self.file_key = json.loads(f.read())
            
            # Read node_definition.json
            node_def_path = os.path.join(mount_path, "node_definition.json")
            with open(node_def_path, 'r') as f:
                self.node_definition = json.loads(f.read())
            
            # Read postgres_connector.json
            postgres_path = os.path.join(mount_path, "postgres_connector.json")
            with open(postgres_path, 'r') as f:
                self.postgres_connector = json.loads(f.read())
                
            
            
        except FileNotFoundError as e:
            print(f"Error: File not found - {e}")
        except json.JSONDecodeError as e:
            print(f"Error: Invalid JSON format - {e}")
        except Exception as e:
            print(f"Error reading mount data: {e}")
            
   