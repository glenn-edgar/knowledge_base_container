
from .file_manager import File_Manager
from .volume_manager import Volume_Manager  # Using your existing class name
from .postgres_file_storage import File_Table
from .composite_file_loader import Composite_File_Loader

# Package metadata
__version__ = "1.0.0"
__author__ = "System"
__description__ = "Comprehensive file loading and management system with database integration"

# Define what gets imported with "from file_loader import *"
__all__ = [
    "File_Manager",
    "Volume_Manager",  # Using your existing class name
    "File_Table",
    "Composite_File_Loader"
]

# Package-level constants
