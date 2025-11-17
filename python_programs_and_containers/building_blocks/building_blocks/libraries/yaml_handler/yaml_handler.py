import yaml
from pathlib import Path
from typing import Union, Dict, List, Any


class YAMLHandler:
    def __init__(self):
        pass
    
    def generate_yaml_file(self, data: Any, filepath: Path, 
                          indent: int = 2, sort_keys: bool = False) -> bool:
        """
        Generate a YAML file from Python objects.
        
        Args:
            data: Python object to convert to YAML
            filepath: Path object for the output file
            indent: Number of spaces for indentation
            sort_keys: Whether to sort dictionary keys
            
        Returns:
            True if successful, False otherwise
        """
        try:
            # Ensure parent directory exists
            filepath.parent.mkdir(parents=True, exist_ok=True)
            
            with filepath.open('w', encoding='utf-8') as file:
                yaml.dump(data, file,
                         default_flow_style=False,
                         indent=indent,
                         sort_keys=sort_keys,
                         allow_unicode=True)
            print(f"✅ YAML file '{filepath}' generated successfully!")
            return True
        except Exception as e:
            print(f"❌ Error generating YAML file: {e}")
            return False

    def decode_yaml_file(self, filepath: Path) -> Union[Dict, List, None]:
        """
        Decode a YAML file back to Python objects.
        
        Args:
            filepath: Path object for the YAML file to decode
            
        Returns:
            Decoded Python object (dict, list, etc.) or None if error
        """
        try:
            if not filepath.exists():
                print(f"❌ File '{filepath}' not found!")
                return None
                
            with filepath.open('r', encoding='utf-8') as file:
                data = yaml.safe_load(file)
            print(f"✅ YAML file '{filepath}' decoded successfully!")
            return data
        except Exception as e:
            print(f"❌ Error decoding YAML file: {e}")
            return None

   
