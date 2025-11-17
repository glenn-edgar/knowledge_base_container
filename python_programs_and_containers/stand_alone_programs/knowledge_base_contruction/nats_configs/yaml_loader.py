from pathlib import Path
import yaml


class YamlLoader:
    def __init__(self, yaml_file: Path, db_store_function):
        self.yaml_file = yaml_file
        self.db_store_function = db_store_function
        
        # verify that yaml_file is a file
        if not self.yaml_file.is_file():
            raise FileNotFoundError(f"Yaml file not found: {self.yaml_file}")
        
        # verify that db_store_function is a function
        if not callable(self.db_store_function):
            raise ValueError("Db store function must be a function")

    def load_yaml(self):
        """Load YAML file and call db_store_function for each entry"""
        # Open and parse YAML file
        with open(self.yaml_file, 'r') as f:
            yaml_data = yaml.safe_load(f)
            
        if not yaml_data:
            print("Warning: YAML file is empty or could not be parsed")
            return 0
        
        # Track statistics
        success_count = 0
        error_count = 0
        errors = []
        
        # Process each dictionary item
        for ltree_path, entry in yaml_data.items():
            try:
                # Extract the four required fields from the entry
                label_name = entry.get('label', '')
                node_name = entry.get('node_name', '')
                label_dict = entry.get('label_dict', {})
                node_dict = entry.get('node_dict', {})
                
                # Call the db_store_function with all parameters
                self.db_store_function(
                    path=ltree_path,
                    label_name=label_name,
                    node_name=node_name,
                    label_dict=label_dict,
                    node_dict=node_dict
                )
                
                success_count += 1
                
            except Exception as e:
                error_count += 1
                errors.append(f"Error processing {ltree_path}: {str(e)}")
                print(f"Error processing entry '{ltree_path}': {e}")
        
        # Print summary
        print(f"\nLoad Summary:")
        print(f"  Successfully processed: {success_count} entries")
        if error_count > 0:
            print(f"  Errors encountered: {error_count}")
            for error in errors[:5]:  # Show first 5 errors
                print(f"    - {error}")
            if error_count > 5:
                print(f"    ... and {error_count - 5} more errors")
        
        return success_count


def test_yaml_write(path, label_name, node_name, label_dict, node_dict):
    """Test function to demonstrate the db_store_function signature"""
    print("+" * 40)
    print(f"Path: {path}")
    print(f"  Label: {label_name}, Node: {node_name}")
    print(f"  Label Dict: {label_dict}")
    print(f"  Node Dict: {node_dict}")
    print("-" * 40)


def db_store_to_dict(storage_dict):
    """Factory function to create a db_store function that saves to a dictionary"""
    def store_function(path, label_name, node_name, label_dict, node_dict):
        storage_dict[path] = {
            'label_name': label_name,
            'node_name': node_name,
            'label_dict': label_dict,
            'node_dict': node_dict
        }
    return store_function


def db_store_to_list(storage_list):
    """Factory function to create a db_store function that appends to a list"""
    def store_function(path, label_name, node_name, label_dict, node_dict):
        storage_list.append({
            'path': path,
            'label_name': label_name,
            'node_name': node_name,
            'label_dict': label_dict,
            'node_dict': node_dict
        })
    return store_function


if __name__ == "__main__":
    yaml_file = Path.cwd() / "config.yaml"
    
    print("=" * 60)
    print("YAML Loader Test")
    print("=" * 60)
    
    # Check if the YAML file exists
    if not yaml_file.exists():
        print(f"Error: {yaml_file} does not exist!")
        print("Please run the YamlGenerator test first to create the config.yaml file.")
        exit(1)
    
    print(f"Loading YAML from: {yaml_file.absolute()}")
    print("-" * 60)
    
    # Test 1: Load and print all entries
    print("\nTest 1: Load and print all entries")
    print("-" * 60)
    
    loader = YamlLoader(yaml_file, test_yaml_write)
    count = loader.load_yaml()
    
    print(f"\nTotal entries loaded: {count}")
    
    # Test 2: Load into a dictionary
    print("\n" + "=" * 60)
    print("Test 2: Load into a dictionary")
    print("-" * 60)
    
    storage_dict = {}
    loader2 = YamlLoader(yaml_file, db_store_to_dict(storage_dict))
    loader2.load_yaml()
    
    print(f"\nStored {len(storage_dict)} entries in dictionary")
    print("\nFirst 3 dictionary keys:")
    for i, key in enumerate(sorted(storage_dict.keys())[:3]):
        print(f"  {i+1}. {key}")
    
    # Test 3: Load into a list
    print("\n" + "=" * 60)
    print("Test 3: Load into a list")
    print("-" * 60)
    
    storage_list = []
    loader3 = YamlLoader(yaml_file, db_store_to_list(storage_list))
    loader3.load_yaml()
    
    print(f"\nStored {len(storage_list)} entries in list")
    print("\nFirst 3 list entries (paths only):")
    for i, entry in enumerate(storage_list[:3]):
        print(f"  {i+1}. {entry['path']}")
    
    # Test 4: Demonstrate hierarchy by filtering paths
    print("\n" + "=" * 60)
    print("Test 4: Filter database-related entries")
    print("-" * 60)
    
    database_entries = []
    
    def db_store_filtered(path, label_name, node_name, label_dict, node_dict):
        if path.startswith("database"):
            database_entries.append(path)
            print(f"  Database entry: {path}")
    
    loader4 = YamlLoader(yaml_file, db_store_filtered)
    loader4.load_yaml()
    
    print(f"\nFound {len(database_entries)} database-related entries")
    
    # Test 5: Error handling
    print("\n" + "=" * 60)
    print("Test 5: Error handling")
    print("-" * 60)
    
    # Test with non-existent file
    try:
        bad_loader = YamlLoader(Path("nonexistent.yaml"), test_yaml_write)
        print("ERROR: Should have raised FileNotFoundError")
    except FileNotFoundError as e:
        print(f"✓ Correctly caught FileNotFoundError: {e}")
    
    # Test with non-callable
    try:
        bad_loader = YamlLoader(yaml_file, "not a function")
        print("ERROR: Should have raised ValueError")
    except ValueError as e:
        print(f"✓ Correctly caught ValueError: {e}")
    
    print("\n" + "=" * 60)
    print("All tests completed!")
    print("=" * 60)