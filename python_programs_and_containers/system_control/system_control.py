import os
import json
import time
from common_libraries.libraries.container_management.container_manager import ContainerManager
from common_libraries.knowledge_base.construct_kb.construct_kb import Construct_KB
from common_libraries.knowledge_base.data_structures.kb_data_structures import KB_Data_Structures
from common_libraries.libraries.file_loader.file_manager import File_Manager
from common_libraries.libraries.secrets_loader.secrets_loader import SecretsLoader

from system_initialialization import System_Initialization



class System_Monitoring(System_Initialization):
    def __init__(self):
        System_Initialization.__init__(self)
        
   
 

if __name__ == "__main__":
    system_monitor = System_Monitoring()
    print("system is up")