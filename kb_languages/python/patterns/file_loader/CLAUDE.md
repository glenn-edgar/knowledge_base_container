# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

This is a Python file loading and storage system that uses PostgreSQL to manage files across different volumes. The system provides a modular architecture for reading files from the filesystem and storing them in a PostgreSQL database with LTREE indexing for efficient path-based queries.

## Architecture

The codebase consists of four main components:

1. **File_manager** (file_manager.py): Handles filesystem operations
   - Recursively reads directories with file extension filtering
   - Provides callbacks for processing files
   - Supports reading only new files (not in existing list)
   - Handles file writing with safety checks

2. **File_Table** (postgres_file_storeage.py): PostgreSQL storage layer
   - Creates and manages a table with LTREE path indexing
   - Stores files with volume, path, name, extension, content, and size
   - Supports both insert and upsert operations
   - Uses GIST indexing for efficient path queries

3. **Volume_Definition_Table** (volume_manager.py): Volume management
   - Manages volume definitions in PostgreSQL
   - Tracks volume names, paths, and descriptions
   - Prevents duplicate volumes
   - Provides CRUD operations for volumes

4. **Composite_File_Loader** (composite_file_loader.py): Orchestration layer
   - Combines file_manager, volume_manager, and postgres_file_loader
   - Provides high-level interface for loading files into specific volumes

## Key Technical Details

- Uses PostgreSQL's LTREE extension for hierarchical path storage
- File paths are converted from "folder/subfolder/file.ext" to LTREE format "folder.subfolder.file"
- Implements unique constraints on (volume, file_path, file_extension)
- All database operations use proper cursor management and transactions
- File reading uses UTF-8 encoding with error ignoring for robustness

## Development Notes

- No build system, test framework, or linting configuration found
- Direct PostgreSQL usage with psycopg2 (ensure ltree extension is enabled in database)
- Error handling uses generic Exception raising - consider specific exception types
- Database connection management is handled externally (passed to constructors)