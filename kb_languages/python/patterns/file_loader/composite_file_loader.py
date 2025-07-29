import os
from pathlib import Path
import psycopg2
from psycopg2.extras import RealDictCursor



from file_manager import File_manager
from volume_manager import Volume_Manager
from postgres_file_storeage import File_Table

class Composite_File_Loader(File_manager, Volume_Manager, File_Table):
    def __init__(self, base_table :str, volumn_table_name:str, postgres_connector):
        
        
        self.base_table = base_table
        self.volumn_table_name = volumn_table_name
        self.postgres_connector = postgres_connector
        File_manager.__init__(self)
        Volume_Manager.__init__(self, postgres_connector, volumn_table_name)
        
        File_Table.__init__(self, postgres_connector, base_table)


    
    
    def load_directory(self, update:bool, volume_name: str, extension_list:list):
        
        path = self.get_volume_path(volume_name)
        starting_directory = Path(path)
        if not starting_directory.exists():
            raise ValueError(f"starting directory {starting_directory} does not exist")
        if not starting_directory.is_dir():
            raise ValueError(f"starting directory {starting_directory} is not a directory")
        starting_directory = Path(path)
        
        def call_back(file_path, data):
            print("update", update, volume_name, file_path)
            self.store_file(update, volume_name, file_path, data)
        self.read_directory(call_back, starting_directory, extension_list)
        
    def update_content(self, volume_name:str, file_path:str, file_name:str, file_extension:str, data:str):
        if self.record_exists(volume_name, file_path, file_name, file_extension):
            self.update_content(volume_name, file_path, file_name, file_extension, data)
        else:
            self.store_file(True, volume_name, file_path, data)
            
            
        
if __name__ == "__main__":
    dbname="knowledge_base"
    user="gedgar"
    password=os.getenv("POSTGRES_PASSWORD")
    host="localhost"
    port="5432"
    database="knowledge_base"
    
    conn = psycopg2.connect(dbname=dbname, user=user, password=password, host=host, port=port)
    cursor = conn.cursor()
    
    #current_directory = os.getcwd()
    #current_directory = current_directory+"/test_files"
    
    composite_file_loader = Composite_File_Loader( base_table="text_files", volumn_table_name="volume_definition_table", postgres_connector=conn)
    composite_file_loader.delete_all_volumes()
    volume_path = os.getcwd()+"/test_files/kb_go/kb_memory"
    postgres_path = os.getcwd()+"/test_files/kb_go/postgres"
    composite_file_loader.add_volume("kb_memory", volume_path, "kb_memory")
    composite_file_loader.add_volume("postgres", postgres_path, "postgres")
    composite_file_loader.load_directory(update=False, volume_name="kb_memory", extension_list=[".go"])
    composite_file_loader.load_directory(update=False, volume_name="postgres", extension_list=[".go"])
    #composite_file_loader.delete_by_volume("kb_memory")
   
    conn.close()
        
        