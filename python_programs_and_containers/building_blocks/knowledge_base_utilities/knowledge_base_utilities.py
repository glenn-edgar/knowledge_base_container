
from pathlib import Path

class KnowledgeBaseUtilities:
    def __init__(self):
        pass
    
 

    def get_kb_version_lines(self,filename):
        """
        Read lines from a file and return a list of lines that contain 'kb.kb_version.KB_VERSION'.
        
        Args:
            filename (str or Path): Path to the file to read
            
        Returns:
            list: List of tuples (line_number, line_content) for matching lines
        """
        matching_lines = []
        
        # Handle both string and Path inputs
        if isinstance(filename, Path):
            file_path = filename
        else:
            file_path = Path(filename)
        
        
        try:
            with file_path.open('r', encoding='utf-8') as file:
                
                for line_number, line in enumerate(file, 1):
                    if 'kb.kb_version.KB_VERSION' in line:
                        matching_lines.append((line_number, line.rstrip()))
                
                        
        except FileNotFoundError:
            print(f"Error: File '{file_path}' not found.")
            return []
        except PermissionError:
            print(f"Error: Permission denied to read file '{file_path}'.")
            return []
        except Exception as e:
            print(f"Error reading file: {e}")
            return []
        
        return matching_lines

# Example usage:
if __name__ == "__main__":
    # Using Path object
    filename = Path("/home/gedgar/mount_startup/kb_definitions.yaml")
    # Or with a full path:
    # filename = Path("/path/to/your/file.txt")
    kb_util = KnowledgeBaseUtilities()
    matches = kb_util.get_kb_version_lines(filename)
    
    print(f"Matches: {matches}")