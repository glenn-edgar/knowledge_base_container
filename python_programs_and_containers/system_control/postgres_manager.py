import os
import json
import time
from common_libraries.libraries.container_management.container_manager import ContainerManager
from common_libraries.knowledge_base.construct_kb.construct_kb import Construct_KB
from common_libraries.knowledge_base.data_structures.kb_data_structures import KB_Data_Structures
from common_libraries.libraries.file_loader.file_manager import File_Manager
from common_libraries.libraries.secrets_loader.secrets_loader import SecretsLoader

from postgres_connection_manager.postgres_connection import Postgres_Connection_Manager


class Postgres_Manager:
    def __init__(self,postgres_object:dict,container_flag:bool):
        self.postgres_object = postgres_object
        self.container_manager = ContainerManager(self.postgres_object['container'])
        self.postgres_connection_manager = Postgres_Connection_Manager(self.postgres_object,container_flag)
        self.start_postgres_container()
        self.postgres_connection_manager.wait_for_connection(max_attempts=2)
        
        
    def start_postgres_container(self):
        if not self.container_manager.is_container_running():
            print("Starting Postgres container")
            print(self.container_manager.start_container())
        else:
            print("Postgres container is already running")
        time.sleep(3)
        if self.container_manager.is_container_running()==False:
            raise Exception("Postgres container failed to start")
        
        
    
    def is_postgres_container_running(self):
        if self.container_manager.is_container_running():
            return True
        else:
            return False
        
    def postgres_monitor(self):
        self.postgres_connection_manager.wait_for_connection(max_attempts,container_flag )