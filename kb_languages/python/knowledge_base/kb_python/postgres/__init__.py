"""
Knowledge Base Event Bus - Python PostgreSQL Implementation

This package provides a comprehensive knowledge base system with event bus capabilities,
built on PostgreSQL. It includes data structures, construction utilities, and management
classes for building and maintaining knowledge bases.

Main Components:
- construct_kb: Classes for building and managing knowledge base tables
- data_structures: Core data structures and utilities for knowledge base operations
"""

# Import from construct_kb subdirectory
from .construct_kb import (
    KnowledgeBaseManager,
    Construct_KB,
    Construct_Data_Tables,
    Construct_Job_Table,
    Construct_RPC_Client_Table,
    Construct_RPC_Server_Table,
    Construct_Status_Table,
    Construct_Stream_Table
)

# Import from data_structures subdirectory
from .data_structures import (
    KB_Data_Structures,
    KB_Job_Queue,
    KB_Search,
    KB_RPC_Client,
    KB_RPC_Server,
    KB_Status_Data,
    KB_Stream,
    NoMatchingRecordError
)

# Version information
__version__ = "1.0.0"
__author__ = "Knowledge Base Event Bus Team"

# Package-level exports
__all__ = [
    # Construct KB classes
    "KnowledgeBaseManager",
    "Construct_KB", 
    "Construct_Data_Tables",
    "Construct_Job_Table",
    "Construct_RPC_Client_Table",
    "Construct_RPC_Server_Table",
    "Construct_Status_Table",
    "Construct_Stream_Table",
    
    # Data structure classes
    "KB_Data_Structures",
    "KB_Job_Queue",
    "KB_Search", 
    "KB_RPC_Client",
    "KB_RPC_Server",
    "KB_Status_Data",
    "KB_Stream",
    "NoMatchingRecordError"
] 