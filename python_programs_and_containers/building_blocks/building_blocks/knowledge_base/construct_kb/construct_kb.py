import psycopg2
import json
from typing import Union, Optional, Dict, Any
from psycopg2 import sql
from psycopg2.extensions import adapt, AsIs
from .base_construct_kb import KnowledgeBaseManager




class Construct_KB(KnowledgeBaseManager):
    """
    This class is designed to construct a knowledge base structure with header
    and info nodes, using a stack-based approach to manage the path. It also
    manages a connection to a PostgreSQL database and sets up the schema.
    """

    def __init__(self, host, port, dbname, user, password, table_name="knowledge_base"):
        """
        Initializes the Construct_KB object and connects to the PostgreSQL database.
        Also sets up the database schema.

        Args:
            host (str): The database host.
            port (str): The database port.
            dbname (str): The name of the database.
            user (str): The database user.
            password (str): The password for the database user.
            table_name (str): The name of the table (default: "knowledge_base")
        """
        self.path = {}  # Stack to keep track of the path (levels/nodes)
        self.path_values = {}
        self.conn = None  # Connection object
        self.cursor = None  # Cursor object
        self.table_name = table_name
        
        connection_params = {
            'host': host,
            'port': port,
            'database': dbname,  # Note: changed 'dbname' to 'database' for psycopg2
            'user': user,
            'password': password
        }
        
        # Call parent class __init__
        KnowledgeBaseManager.__init__(self, table_name, connection_params)
        
    @classmethod
    def from_connection(cls, connection: psycopg2.extensions.connection, 
                       table_name: str = "knowledge_base"):
        """
        Create Construct_KB instance from an existing psycopg2 connection.
        
        Args:
            connection: Existing psycopg2 connection object
            table_name: The name of the table (default: "knowledge_base")
            
        Returns:
            Construct_KB instance using the provided connection
        """
        # Create instance without calling __init__
        instance = cls.__new__(cls)
        
        # Initialize Construct_KB specific attributes
        instance.path = {}
        instance.path_values = {}
        instance.table_name = table_name
        
        # Use KnowledgeBaseManager's from_connection to handle the connection setup
        # We need to manually set up the inheritance chain
        kb_manager = KnowledgeBaseManager.from_connection(table_name, connection)
        
        # Copy attributes from kb_manager to our instance
        instance.conn = kb_manager.conn
        instance.cursor = kb_manager.cursor
        instance.connection_params = kb_manager.connection_params
        instance._external_connection = kb_manager._external_connection
        
        # If there are other KnowledgeBaseManager attributes/methods needed, 
        # ensure they're available
        for attr in dir(kb_manager):
            if not attr.startswith('_') and not hasattr(instance, attr):
                setattr(instance, attr, getattr(kb_manager, attr))
        
        return instance
        
    def get_db_objects(self):
        """
        Returns both the database connection and cursor objects.

        Returns:
            tuple: A tuple containing (connection, cursor)
                - connection (psycopg2.extensions.connection): The PostgreSQL database connection
                - cursor (psycopg2.extensions.cursor): The PostgreSQL database cursor
        """
        return self.conn, self.cursor

    def kb_exists(self, kb_name):
        if kb_name in self.path:
            return True
        return False
    
    def add_kb(self, kb_name, description=""):
        if kb_name in self.path:
            raise ValueError(f"Knowledge base {kb_name} already exists")
        self.path[kb_name] = [kb_name]
        self.path_values[kb_name] = {}
        KnowledgeBaseManager.add_kb(self, kb_name, description)
      
    def select_kb(self, kb_name):
        if kb_name not in self.path:
            raise ValueError(f"Knowledge base {kb_name} does not exist")
        
        self.working_kb = kb_name
        
    def get_working_kb(self):
        return self.working_kb
    
   
    def add_header_node(self, link, node_name, node_properties, node_data,description=""):
        """
        Adds a header node to the knowledge base.

        Args:
            link: The link associated with the header node.
            node_name: The name of the header node.
            node_properties: Properties associated with the header node.
            node_data: Data associated with the header node.
        """
        
        
        if not isinstance(description, str):
            raise TypeError("description must be a string")
        if not isinstance(node_properties, dict):
            raise TypeError("node_properties must be a dictionary")
        
        if description != None:
            node_properties["description"] = description
            
       

        self.path[self.working_kb].append(link)
        self.path[self.working_kb].append(node_name)
        node_path = ".".join(self.path[self.working_kb])
        

        if node_path in self.path_values[self.working_kb]:
            raise ValueError(f"Path {node_path} already exists in knowledge base")
        
        self.path_values[self.working_kb][node_path] = True
       
        path = ".".join(self.path[self.working_kb])
        print("path", path)
        KnowledgeBaseManager.add_node(self, self.working_kb, link, node_name, node_properties, node_data,path)
       

    def add_info_node(self, link, node_name, node_properties, node_data,description=""):
        self.add_header_node(link, node_name, node_properties, node_data,description)
     
        self.path[self.working_kb].pop()  # Remove node_name
        self.path[self.working_kb].pop()  # Remove link
        
    
    def leave_header_node(self, label, name):
        """
        Leaves a header node, verifying the label and name.
        If an error occurs, the knowledge_base table is deleted if it exists
        and the PostgreSQL connection is terminated.

        Args:
            label: The expected link of the header node.
            name: The expected name of the header node.
        """
        # Try to pop the expected values
        if not self.path:
            raise ValueError("Cannot leave a header node: path is empty")
        
        ref_name = self.path[self.working_kb].pop()
        if not self.path[self.working_kb]:
            # Put the name back and raise an error
            self.path[self.working_kb].append(ref_name)
            raise ValueError("Cannot leave a header node: not enough elements in path")
            
        ref_label = self.path[self.working_kb].pop()
        
        # Verify the popped values
        if ref_name != name or ref_label != label:
            # Create a descriptive error message
            error_msg = []
            if ref_name != name:
                error_msg.append(f"Expected name '{name}', but got '{ref_name}'")
            if ref_label != label:
                error_msg.append(f"Expected label '{label}', but got '{ref_label}'")
                
            raise AssertionError(", ".join(error_msg))
        
        
    def add_link_node(self,link_name):
        KnowledgeBaseManager.add_link(self, self.working_kb,".".join(self.path[self.working_kb]), link_name)
   
    def add_link_mount(self, link_mount_name, description = ""):
        KnowledgeBaseManager.add_link_mount(self, self.working_kb,".".join(self.path[self.working_kb]), 
                                            link_mount_name, description)
    
    def check_installation(self):
        """
        Checks if the installation is correct by verifying that the path is empty.
        If the path is not empty, the knowledge_base table is deleted if present,
        the database connection is closed, and an exception is raised.
        If the path is empty, the database connection is closed normally.

        Returns:
            bool: True if installation check passed

        Raises:
            RuntimeError: If the path is not empty
        """
        for kb_name in self.path:
            if len(self.path[kb_name]) != 1:
                
                raise RuntimeError(f"Installation check failed: Path is not empty for knowledge base {kb_name}. Path: {self.path[kb_name]}")
            if self.path[kb_name][0] != kb_name:
                raise RuntimeError(f"Installation check failed: Path is not empty for knowledge base {kb_name}. Path: {self.path[kb_name]}")
       

if __name__ == '__main__':
    # Example Usage
    # Replace with your actual database credentials
    DB_HOST = "localhost"
    DB_PORT = "5432"
    DB_NAME = "knowledge_base"
    DB_USER = "gedgar"
    
    DB_TABLE = "knowledge_base"
    DB_PASSWORD = input("Enter your password: ")
    print("starting unit test")
    kb = Construct_KB(DB_HOST, DB_PORT, DB_NAME, DB_USER, DB_PASSWORD, DB_TABLE )



 
    kb.add_kb("kb1", "First knowledge base")
    kb.select_kb("kb1")
    kb.add_header_node("header1_link", "header1_name", {"prop1": "val1"}, {"data":"header1_data"})
   
    kb.add_info_node( "info1_link", "info1_name", {"prop2": "val2"}, {"data":"info1_data"})

    kb.leave_header_node("header1_link", "header1_name")
 
    kb.add_header_node("header2_link", "header2_name", {"prop3": "val3"}, {"data":"header2_data"})
    kb.add_info_node("info2_link", "info2_name", {"prop4": "val4"}, {"data":"info2_data"})
    kb.add_link_mount( "link1", "link1 description")
    kb.leave_header_node("header2_link", "header2_name")
  
    kb.add_kb("kb2", "Second knowledge base")
    kb.select_kb("kb2")
    kb.add_header_node("header1_link", "header1_name", {"prop1": "val1"}, {"data":"header1_data"})
   
    
    kb.add_info_node( "info1_link", "info1_name", {"prop2": "val2"}, {"data":"info1_data"})
  

    kb.leave_header_node("header1_link", "header1_name")
   
    kb.add_header_node("header2_link", "header2_name", {"prop3": "val3"}, {"data":"header2_data"})
    kb.add_info_node("info2_link", "info2_name", {"prop4": "val4"}, {"data":"info2_data"})
    kb.add_link_node("link1")
    kb.leave_header_node("header2_link", "header2_name")
   


    # Example of check_installation
    try:
        kb.check_installation()
        kb.disconnect()
    except RuntimeError as e:
        print(f"Error during installation check: {e}")
    print("ending unit test")

