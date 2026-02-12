import psycopg2
import json
import os
import sys
from psycopg2 import sql
from psycopg2.extensions import adapt, AsIs

from .construct_kb import Construct_KB
from .construct_status_table import Construct_Status_Table
from .construct_job_table import Construct_Job_Table
from .construct_stream_table import Construct_Stream_Table
from .construct_rpc_client_table import Construct_RPC_Client_Table
from .construct_rpc_server_table import Construct_RPC_Server_Table
from .construct_bit_mask_store import Construct_Bit_Mask_Store
from .construct_jsonb_table import Construct_Jsonb_Table

class Construct_Data_Tables:
    """
    This class is designed to construct data tables with header
    and info nodes, using a stack-based approach to manage the path. It also
    manages a connection to a PostgreSQL database and sets up the schema.
    """
    def __init__(self, host, port, dbname, user, password, database,upload_flag = False):
        """
        Initializes the Construct_Data_Tables object by creating instances of all required
        table constructor classes and connecting to the PostgreSQL database.
        
        Args:
            host (str): The database host.
            port (str): The database port.
            dbname (str): The name of the database.
            user (str): The database user.
            password (str): The password for the database user.
            database (str): base knowledge base table name
        """
        # Create KB as an attribute instead of inheriting from it
        self.kb = Construct_KB(host, port, dbname, user, password, database,upload_flag)
        
        
        
        # Create instances of all table constructors as attributes
        self.status_table = Construct_Status_Table(self.kb.conn, self.kb.cursor, construct_kb=self.kb,database=database,upload_flag=upload_flag)
        
        self.job_table = Construct_Job_Table(self.kb.conn, self.kb.cursor, self.kb,database=database,upload_flag=upload_flag)
        self.stream_table = Construct_Stream_Table(self.kb.conn, self.kb.cursor, self.kb,database=database,upload_flag=upload_flag)
        self.rpc_client_table = Construct_RPC_Client_Table(self.kb.conn, self.kb.cursor, self.kb,database=database,upload_flag=upload_flag)
        self.rpc_server_table = Construct_RPC_Server_Table(self.kb.conn, self.kb.cursor, self.kb,database=database,upload_flag=upload_flag)
        self.bit_mask_store = Construct_Bit_Mask_Store(self.kb.conn, self.kb,upload_flag=upload_flag)
        self.jsonb_table = Construct_Jsonb_Table(self.kb.conn,self.kb.cursor, self.kb, database=database,upload_flag=upload_flag)
        
        self.path = self.kb.path
        self.add_kb = self.kb.add_kb
        self.select_kb = self.kb.select_kb
        self.add_link_node = self.kb.add_link_node
        self.add_link_mount = self.kb.add_link_mount
        self.add_header_node = self.kb.add_header_node
        self.add_info_node = self.kb.add_info_node
        self.leave_header_node = self.kb.leave_header_node
        self.disconnect = self.kb.disconnect
        self.add_stream_field = self.stream_table.add_stream_field
        self.add_rpc_client_field = self.rpc_client_table.add_rpc_client_field
        self.add_rpc_server_field = self.rpc_server_table.add_rpc_server_field
        self.add_status_field = self.status_table.add_status_field
        self.add_job_field = self.job_table.add_job_field
        self.create_bit_mask_entry = self.bit_mask_store.create_bit_mask_entry
        self.add_bit_mask_flag = self.bit_mask_store.add_flag
        self.clear_bit_mask_flags = self.bit_mask_store.clear_flags
        self.add_jsonb_field = self.jsonb_table.add_jsonb_field
        
        
        
        
    def check_installation(self):
        """
        Checks the installation status of all table components
        """
        # Call check_installation on each component instance
        self.kb.check_installation()
        self.status_table.check_installation()
        self.job_table.check_installation()
      
        self.stream_table.check_installation()
      
        self.rpc_client_table.check_installation()
        self.rpc_server_table.check_installation()
        self.jsonb_table.check_installation()
        
        
if __name__ == '__main__':
    unit_test = False
    password = os.getenv("POSTGRES_PASSWORD")
    if password is None:
        raise ValueError("POSTGRES_PASSWORD environment variable is not set")
    if len(sys.argv) < 3:
        unit_test = False
    else:
        if sys.argv[2] == "True":
            unit_test = True
        else:
            unit_test = False
    
    if len(sys.argv) <2:
        upload_flag = False
    else:
        if sys.argv[1] == "True":
            upload_flag = True
        else:
            upload_flag = False
        
    
    # Replace with your actual database credentials
    DB_HOST = "localhost"
    DB_PORT = "5432"
    DB_NAME = "knowledge_base"
    DB_USER = "gedgar"
    DB_PASSWORD = password
    DATABASE = "knowledge_base"
    
    kb = Construct_Data_Tables(DB_HOST, DB_PORT, DB_NAME, DB_USER, DB_PASSWORD, DATABASE, upload_flag)
    if upload_flag == False:
        print("Initial state:")
        print(f"Path: {kb.path}")
        kb.add_kb("kb1", "First knowledge base")
        kb.select_kb("kb1")
        kb.add_header_node("header1_link", "header1_name", {"prop1": "val1"}, {"data":"header1_data"})
        print("\nAfter add_header_node:")
        print(f"Path: {kb.path}")

        kb.add_info_node("info1_link", "info1_name", {"prop2": "val2"}, {"data":"info1_data"})
        print("\nAfter add_info_node:")
        print(f"Path: {kb.path}")

        kb.add_rpc_server_field("info1_server",25,"info1_server_data")
        kb.add_status_field("info1_status", {"prop3": "val3"},  "info1_status_description",{"prop3": "val3"})
        kb.add_status_field("info2_status", {"prop3": "val3"},  "info2_status_description",{"prop3": "val3"})
        kb.add_status_field("info3_status", {"prop3": "val3"},  "info3_status_description",{"prop3": "val3"})
        kb.add_job_field("info1_job", 100, "info1_job_description")
        kb.add_stream_field("info1_stream",95, "info1_stream")
        kb.add_rpc_client_field("info1_client", 10,"info1_client_description") 
        kb.add_jsonb_field("info1_jsonb", "type_1", "info1_jsonb_description", {"data":"info1_jsonb_data"})
        kb.add_jsonb_field("info2_jsonb", "type_1", "info2_jsonb_description", {"data":"info2_jsonb_data"})
        kb.add_jsonb_field("info3_jsonb", "type_2", "info3_jsonb_description", {"data":"info3_jsonb_data"})
        kb.add_link_mount("info1_link_mount", "info1_link_mount_description")  
        
        
        
        kb.clear_bit_mask_flags()
        kb.add_bit_mask_flag("A", 0, "A_description")
        kb.add_bit_mask_flag("B", 1, "B_description")
        kb.add_bit_mask_flag("C", 2, "C_description")
        kb.add_bit_mask_flag("D", 3, "D_description")
        kb.add_bit_mask_flag("E", 4, "E_description")
        kb.create_bit_mask_entry("user_2", "info2_bit_mask", 5, 0, "info2_bit_mask_description")
        
        kb.clear_bit_mask_flags()
        kb.add_bit_mask_flag("F", 0, "F_description")
        kb.add_bit_mask_flag("G", 1, "G_description")
        kb.add_bit_mask_flag("H", 2, "H_description")
        kb.add_bit_mask_flag("I", 3, "I_description")
        kb.add_bit_mask_flag("J", 4, "J_description")
        kb.create_bit_mask_entry("user_1", "info1_bit_mask", 5, 0, "info1_bit_mask_description")
        
        kb.leave_header_node("header1_link", "header1_name")
        print("\nAfter leave_header_node:")
        print(f"Path: {kb.path}")

        kb.add_header_node("header2_link", "header2_name", {"prop3": "val3"}, {"data":"header2_data"})
        kb.add_info_node("info2_link", "info2_name", {"prop4": "val4"}, {"data":"info2_data"})
        kb.add_link_node("info1_link_mount")
        kb.leave_header_node("header2_link", "header2_name")
        print("\nAfter adding and leaving another header node:")
        print(f"Path: {kb.path}")
 
    # Example of check_installation
    try:
        kb.check_installation()
        kb.disconnect()
    except RuntimeError as e:
        print(f"Error during installation check: {e}")
    if unit_test == False:
        exit(0)
    
    kb = Construct_Data_Tables(DB_HOST, DB_PORT, DB_NAME, DB_USER, DB_PASSWORD, DATABASE)

    print("Initial state:")
    print(f"Path: {kb.path}")
    kb.add_kb("kb1", "First knowledge base")
    kb.select_kb("kb1")
    kb.add_header_node("header1_link", "header1_name", {"prop1": "val1"}, {"data":"header1_data"})
    print("\nAfter add_header_node:")
    print(f"Path: {kb.path}")

    kb.add_info_node("info1_link", "info1_name", {"prop2": "val2"}, {"data":"info1_data"})
    print("\nAfter add_info_node:")
    print(f"Path: {kb.path}")

    kb.add_rpc_server_field("info1_server",25,"info1_server_data")
    kb.add_status_field("info1_status", {"prop3": "val3"},  "info1_status_description",{"prop3": "val3"})
    kb.add_status_field("info2_status", {"prop3": "val3"},  "info2_status_description",{"prop3": "val3"})
    kb.add_status_field("info3_status", {"prop3": "val3"},  "info3_status_description",{"prop3": "val3"})
   
    kb.add_job_field("info2_job", 100, "info1_job_description")
    kb.add_stream_field("info2_status",100, "info1_stream")
    kb.add_rpc_client_field("info2_client", 10,"info1_client_description")   
    
    kb.leave_header_node("header1_link", "header1_name")
    print("\nAfter leave_header_node:")
    print(f"Path: {kb.path}")

    kb.add_header_node("header2_link", "header2_name", {"prop3": "val3"}, {"data":"header2_data"})
    kb.add_info_node("info2_link", "info2_name", {"prop4": "val4"}, {"data":"info2_data"})
 
    
    kb.leave_header_node("header2_link", "header2_name")
    print("\nAfter adding and leaving another header node:")
    print(f"Path: {kb.path}")

    # Example of check_installation
    try:
        kb.check_installation()
        kb.disconnect()
    except RuntimeError as e:
        print(f"Error during installation check: {e}")
    
    
    
    # teests for complete testinge
    kb = Construct_Data_Tables(DB_HOST, DB_PORT, DB_NAME, DB_USER, DB_PASSWORD, DATABASE)

    print("Initial state:")
    print(f"Path: {kb.path}")
    kb.add_kb("kb1", "First knowledge base")
    kb.select_kb("kb1")
    kb.add_header_node("header1_link", "header1_name", {"prop1": "val1"}, {"data":"header1_data"})
    print("\nAfter add_header_node:")
    print(f"Path: {kb.path}")

    kb.add_info_node("info1_link", "info1_name", {"prop2": "val2"}, {"data":"info1_data"})
    print("\nAfter add_info_node:")
    print(f"Path: {kb.path}")

    kb.add_rpc_server_field("info1_server",25,"info1_server_data")
   
    kb.add_job_field("info1_job", 50, "info1_job_description")
    kb.add_stream_field("info1_status",50, "info1_stream")
    kb.add_rpc_client_field("info1_client", 5,"info1_client_description")   
    kb.leave_header_node("header1_link", "header1_name")
    print("\nAfter leave_header_node:")
    print(f"Path: {kb.path}")

    kb.add_header_node("header2_link", "header2_name", {"prop3": "val3"}, {"data":"header2_data"})
    kb.add_info_node("info2_link", "info2_name", {"prop4": "val4"}, {"data":"info2_data"})
    kb.leave_header_node("header2_link", "header2_name")
    print("\nAfter adding and leaving another header node:")
    print(f"Path: {kb.path}")

    # Example of check_installation
    try:
        kb.check_installation()
        kb.disconnect()
    except RuntimeError as e:
        print(f"Error during installation check: {e}")
