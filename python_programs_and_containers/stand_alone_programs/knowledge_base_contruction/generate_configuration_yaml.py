from building_blocks.libraries.yaml_handler.yaml_handler import YAMLHandler
from pathlib import Path
import os



yaml_handler = YAMLHandler()

postgress_dict = {
    "dbname": "knowledge_base",
    "user": "gedgar",
    "password": os.getenv("POSTGRES_PASSWORD"),
    "host": "localhost",
    "port": "5432",
    "base_table": "knowledge_base",
}

site_db_dict = {
    "system": "main",
    "site": "main",
    "node_name":"main",
    "master": True
}
                
                
db_list = [postgress_dict, site_db_dict]

yaml_handler.generate_yaml_file(db_list, Path("/home/gedgar/mount_startup/kb_settings.yaml"))
