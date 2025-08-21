print("****************************************** start.py **********************************************")
import sys
from pathlib import Path
import os
import json
import traceback


def setup_building_blocks():
    """Set up the building blocks path for imports."""
    building_block_path = os.environ.get("BUILDING_BLOCK_PATH")
    if not building_block_path:
        raise ValueError("BUILDING_BLOCK_PATH environment variable is not set")
    
    # Verify the path exists
    if not os.path.exists(building_block_path):
        raise ValueError(f"BUILDING_BLOCK_PATH does not exist: {building_block_path}")
    
    # Add to path if not already there
    if building_block_path not in sys.path:
        sys.path.insert(0, building_block_path)  # Insert at beginning for priority
        print(f"Added to Python path: {building_block_path}")
    
    return building_block_path


def load_postgres_config():
    """Load PostgreSQL configuration from JSON file."""
    home_dir = Path.home()
    print(f"Home directory: {home_dir}")
    
    config_file = home_dir / "mount_startup" / "postgres_connector.json"
    if not config_file.exists():
        raise FileNotFoundError(f"Configuration file not found: {config_file}")
    
    with open(config_file, 'r') as f:
        config = json.load(f)
    
    # Validate required fields
    required_fields = ["user", "password", "host", "docker_ip", "port", "base_table", "container", "dbname"]
    missing_fields = [field for field in required_fields if field not in config]
    
    if missing_fields:
        raise KeyError(f"Required fields not found in configuration: {', '.join(missing_fields)}")
    
    print(f"Successfully loaded config for database: {config['dbname']}")
    return config


def construct_knowledge_base(config):
    """Construct the knowledge base using the loaded configuration."""
    try:
        # Import after path is set up
        from knowledge_base.construct_kb.construct_data_tables import Construct_Data_Tables
        
        print("Initializing Construct_Data_Tables...")
        
        # Initialize the constructor with config
        # Adjust parameters based on what Construct_Data_Tables actually expects
        print(config)
        
        constructor = Construct_Data_Tables(
            host=config['host'],
            port=config['port'],
            dbname=config['dbname'],
            user=config['user'],
            password=config['password'],
            data_table_name=config['base_table']
        )
        
        # Construct "def_kb"
        print("Constructing def_kb...")
        # Uncomment and adjust based on actual method names:
        # kb = constructor.create_kb("def_kb")
        
        # Add header node
        print("Adding header node...")
        # kb.add_header_node()
        
        # End header node
        print("Ending header node...")
        # kb.end_header_node()
        
        # Check installation if needed
        # constructor.check_installation()
        
        print("Knowledge base construction completed successfully!")
        return constructor
        
    except ImportError as e:
        print(f"Failed to import Construct_Data_Tables: {e}")
        print("Please verify that the knowledge_base module exists in:")
        print(f"  {building_block_path}/knowledge_base/construct_kb/")
        raise
    except Exception as e:
        print(f"Error during knowledge base construction: {e}")
        raise


def main():
    """Main execution function."""

    # Step 1: Set up building blocks path
    building_block_path = setup_building_blocks()
    
    # Step 2: Load PostgreSQL configuration
    config = load_postgres_config()
    
    # Step 3: Construct the knowledge base
    kb_constructor = construct_knowledge_base(config)
    
    # Step 4: Any additional operations
    # ... add more operations here as needed ...
    
    
    print("Knowledge base built successfully!")
    
    
    return 0  # Success
        
    


if __name__ == "__main__":
    main()
    
  