from basic_contruct_db import BasicConstructDB


class ConstructMemDB(BasicConstructDB):
    def __init__(self,host,port,dbname,user,password,database):
        BasicConstructDB.__init__(self,host,port,dbname,user,password,database)
        self.kb_name = None
        self.working_kb = None
        self.composite_path = {}
        self.composite_path_values = {}
        
    

        
  
    
    def add_kb(self, kb_name, description=""):
        if kb_name in self.composite_path:
            raise ValueError(f"Knowledge base {kb_name} already exists")
        self.composite_path[kb_name] = [kb_name]
        self.composite_path_values[kb_name] = {}
        BasicConstructDB.add_kb(self, kb_name, description)
      
    def select_kb(self, kb_name):
        if kb_name not in self.composite_path:
            raise ValueError(f"Knowledge base {kb_name} does not exist")
        self.working_kb = kb_name
        
   
    def add_header_node(self, link, node_name, node_data,description=""):
        """
        Adds a header node to the knowledge base.

        Args:
            link: The link associated with the header node.
            node_name: The name of the header node.
            node_data: Data associated with the header node. Must be a dictionary.  
            description: Description of the header node.
        """
        
        
        if not isinstance(description, str):
            raise TypeError("description must be a string")
        if not isinstance(node_data, dict):
            raise TypeError("node_data must be a dictionary")
        
        if description != None:
            node_data["description"] = description
            


        self.composite_path[self.working_kb].append(link)
        self.composite_path[self.working_kb].append(node_name)
        node_path = ".".join(self.composite_path[self.working_kb])
        

        if node_path in self.composite_path_values[self.working_kb]:
            raise ValueError(f"Path {node_path} already exists in knowledge base")
        
        self.composite_path_values[self.working_kb][node_path] = True
       
        path = ".".join(self.composite_path[self.working_kb])
        print("path", path)
        BasicConstructDB.store(self, path, node_data)
       

    def add_info_node(self, link, node_name,  node_data,description=""):
        self.add_header_node(link, node_name,  node_data,description)
     
        self.composite_path[self.working_kb].pop()  # Remove node_name
        self.composite_path[self.working_kb].pop()  # Remove link
        
    
    def leave_header_node(self, label, name ):
        """
        Leaves a header node, verifying the label and name.
        If an error occurs, the knowledge_base table is deleted if it exists
        and the PostgreSQL connection is terminated.

        Args:
            label: The expected link of the header node.
            name: The expected name of the header node.
        """
        # Try to pop the expected values
        if not self.composite_path[self.working_kb]:
            raise ValueError("Cannot leave a header node: path is empty")
        
        ref_name = self.composite_path[self.working_kb].pop()
        if not self.composite_path[self.working_kb]:
            # Put the name back and raise an error
            self.composite_path[self.working_kb].append(ref_name)
            raise ValueError("Cannot leave a header node: not enough elements in path")
            
        ref_label = self.composite_path[self.working_kb].pop()
        
        # Verify the popped values
        if ref_name != name or ref_label != label:
            # Create a descriptive error message
            error_msg = []
            if ref_name != name:
                error_msg.append(f"Expected name '{name}', but got '{ref_name}'")
            if ref_label != label:
                error_msg.append(f"Expected label '{label}', but got '{ref_label}'")
                
            raise AssertionError(", ".join(error_msg))
        
        
   
    
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
        for kb_name in self.composite_path:
            if len(self.composite_path[kb_name]) != 1:
                
                raise RuntimeError(f"Installation check failed: Path is not empty for knowledge base {kb_name}. Path: {self.composite_path[kb_name]}")
            if self.composite_path[kb_name][0] != kb_name:
                raise RuntimeError(f"Installation check failed: Path is not empty for knowledge base {kb_name}. Path: {self.path[kb_name]}")
                
                
                
                
                
                
if __name__ == '__main__':
    password = input("Enter PostgreSQL password: ")
    # Example Usage
    # Replace with your actual database credentials
    DB_HOST = "localhost"
    DB_PORT = "5432"
    DB_NAME = "knowledge_base"
    DB_USER = "gedgar"
    DB_PASSWORD = password
    DB_TABLE = 'composite_memory_kb'

    print("starting unit test")
    kb = ConstructMemDB(DB_HOST,DB_PORT,DB_NAME,DB_USER,DB_PASSWORD,DB_TABLE)



 
    kb.add_kb("kb1", "First knowledge base")
    kb.select_kb("kb1")
    kb.add_header_node("header1_link", "header1_name",  {"data":"header1_data"},"header1_description")
   
    kb.add_info_node( "info1_link", "info1_name",  {"data":"info1_data"},"info1_description")

    kb.leave_header_node("header1_link", "header1_name")
 
    kb.add_header_node("header2_link", "header2_name",  {"data":"header2_data"},"header2_description")
    kb.add_info_node("info2_link", "info2_name",  {"data":"info2_data"},"info2_description")
    
    kb.leave_header_node("header2_link", "header2_name")
  
    kb.add_kb("kb2", "Second knowledge base")
    kb.select_kb("kb2")
    kb.add_header_node("header1_link", "header1_name",  {"data":"header1_data"},"header1_description")
   
    
    kb.add_info_node( "info1_link", "info1_name",  {"data":"info1_data"},"info1_description")
  

    kb.leave_header_node("header1_link", "header1_name")
   
    kb.add_header_node("header2_link", "header2_name",  {"data":"header2_data"},"header2_description")
    kb.add_info_node("info2_link", "info2_name",  {"data":"info2_data"},"info2_description")
    
    kb.leave_header_node("header2_link", "header2_name")
   


    # Example of check_installation
    try:
        kb.check_installation()
        print(kb.export_to_postgres(create_table=True,clear_existing=True))
        print(kb.import_from_postgres(table_name='composite_memory_kb'))
    except RuntimeError as e:
        print(f"Error during installation check: {e}")
    print("ending unit test")

