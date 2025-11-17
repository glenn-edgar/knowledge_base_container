# YAML LTREE Generator and Loader

A Python library for generating and loading hierarchical configuration data in a flat LTREE structure, perfect for storing in databases that support LTREE data types (like PostgreSQL) or any system that benefits from flat key-value storage with hierarchical semantics.

## Overview

This library provides two main classes:
- **`YamlGenerator`**: Creates YAML files with flat LTREE structure from hierarchical data
- **`YamlLoader`**: Loads YAML files and processes each entry through a custom storage function

## Features

- 🌳 **LTREE Structure**: Hierarchical data stored with dot-separated paths
- 📁 **Flat Storage**: All data at root level, hierarchy encoded in keys
- 🏷️ **Rich Metadata**: Separate storage for label and node metadata
- 🔄 **Flexible Loading**: Custom storage functions for any backend
- ✅ **Type Safety**: Input validation and error handling
- 📝 **Persistent Files**: YAML files remain for inspection and reuse

## Installation

```bash
# Required dependencies
pip install pyyaml
```

## YAML Structure

Each entry in the generated YAML has:
- **Key**: LTREE path (e.g., `database.postgresql.connection.host`)
- **Value**: Dictionary containing:
  - `label`: The label name for this node
  - `node_name`: The node name
  - `label_dict`: Metadata/configuration for the label
  - `node_dict`: The actual data for the node

### Example YAML Output

```yaml
app.name:
  label: app
  node_name: name
  label_dict: {description: Application name}
  node_dict: {value: MyApplication, type: string}

database.postgresql:
  label: database
  node_name: postgresql
  label_dict: {description: Database configuration}
  node_dict: {port: 5432, enabled: true}

database.postgresql.connection.host:
  label: connection
  node_name: host
  label_dict: {required: true}
  node_dict: {value: localhost, type: hostname}
```

## Usage

### YamlGenerator

```python
from pathlib import Path
from yaml_generator import YamlGenerator

# Initialize generator
generator = YamlGenerator(Path("config.yaml"))

# Add simple nodes (leaf nodes)
generator.define_simple_node("app", "name", 
                            {"description": "App name"}, 
                            {"value": "MyApp"})

generator.define_simple_node("app", "version", 
                            {"description": "Version"}, 
                            {"value": "1.0.0", "build": 42})

# Create composite node (can contain children)
generator.define_composite_node("database", "postgresql", 
                               {"description": "DB config"}, 
                               {"port": 5432, "enabled": True})

# Add children to composite node
generator.define_simple_node("connection", "host", 
                            {"required": True}, 
                            {"value": "localhost"})

generator.define_simple_node("auth", "username", 
                            {"required": True}, 
                            {"value": "db_user"})

# Navigate back up the hierarchy
generator.pop_path("database", "postgresql")

# Generate the YAML file
generator.generate_yaml()
```

### YamlLoader

```python
from pathlib import Path
from yaml_loader import YamlLoader

# Define your storage function
def store_in_database(path, label_name, node_name, label_dict, node_dict):
    # Your database storage logic here
    print(f"Storing {path}: {node_dict}")

# Load and process YAML
loader = YamlLoader(Path("config.yaml"), store_in_database)
count = loader.load_yaml()
print(f"Processed {count} entries")
```

### Storage Function Examples

```python
# Simple print function
def print_entries(path, label_name, node_name, label_dict, node_dict):
    print(f"{path}: {node_dict}")

# Store to dictionary
storage = {}
def store_to_dict(path, label_name, node_name, label_dict, node_dict):
    storage[path] = {
        'label': label_name,
        'node': node_name,
        'metadata': label_dict,
        'data': node_dict
    }

# Filter and process specific entries
def process_database_config(path, label_name, node_name, label_dict, node_dict):
    if path.startswith("database"):
        # Process database configurations
        configure_database(node_dict)

# Store to PostgreSQL with LTREE
def store_to_postgres(path, label_name, node_name, label_dict, node_dict):
    cursor.execute("""
        INSERT INTO config (path, label, node_name, label_meta, node_data)
        VALUES (%s::ltree, %s, %s, %s::jsonb, %s::jsonb)
    """, (path, label_name, node_name, json.dumps(label_dict), json.dumps(node_dict)))
```

## API Reference

### YamlGenerator

#### `__init__(yaml_file: Path, path_list: list = None)`
Initialize the generator with a target YAML file.

#### `define_simple_node(label_name: str, node_name: str, label_dict: dict = None, node_dict: dict = None)`
Define a leaf node that doesn't change the current path.

#### `define_composite_node(label_name: str, node_name: str, label_dict: dict = None, node_dict: dict = None)`
Define a composite node that can contain children. Updates the current path.

#### `pop_path(label_name: str, node_name: str)`
Navigate back up the hierarchy by removing the specified label and node from the path.

#### `generate_yaml() -> dict`
Write the YAML file and return the data dictionary.

#### `get_current_path() -> list`
Get the current path as a list of strings.

### YamlLoader

#### `__init__(yaml_file: Path, db_store_function: callable)`
Initialize the loader with a YAML file and storage function.

#### `load_yaml() -> int`
Load the YAML file and process each entry. Returns the count of successfully processed entries.

## Use Cases

1. **Database Configuration Storage**: Store hierarchical configs in PostgreSQL with LTREE indexing
2. **Key-Value Stores**: Use in Redis, etcd, or other flat key-value stores
3. **Configuration Management**: Manage application settings with clear hierarchy
4. **Multi-tenant Systems**: Organize settings by tenant using path prefixes
5. **Feature Flags**: Store feature flags with hierarchical organization
6. **API Gateway Routing**: Define route configurations with path-based organization

## Advanced Examples

### Building Complex Hierarchies

```python
generator = YamlGenerator(Path("complex_config.yaml"))

# Root level configuration
generator.define_simple_node("global", "timeout", {}, {"seconds": 30})

# Service configuration
generator.define_composite_node("services", "api", {}, {"version": "v2"})

# API endpoints
generator.define_composite_node("endpoints", "users", {}, {"base": "/api/v2/users"})
generator.define_simple_node("methods", "GET", {}, {"rate_limit": 100})
generator.define_simple_node("methods", "POST", {}, {"rate_limit": 10})
generator.pop_path("endpoints", "users")

# Another endpoint
generator.define_composite_node("endpoints", "products", {}, {"base": "/api/v2/products"})
generator.define_simple_node("methods", "GET", {}, {"rate_limit": 200})
generator.pop_path("endpoints", "products")

generator.pop_path("services", "api")
generator.generate_yaml()
```

### Custom Validation in Loader

```python
def validating_store(path, label_name, node_name, label_dict, node_dict):
    # Validate required fields
    if label_dict.get("required") and not node_dict:
        raise ValueError(f"Required field {path} is empty")
    
    # Validate data types
    if "type" in node_dict:
        expected_type = node_dict["type"]
        if expected_type == "integer" and not isinstance(node_dict.get("value"), int):
            raise ValueError(f"{path} should be an integer")
    
    # Store if valid
    store_to_database(path, label_name, node_name, label_dict, node_dict)

loader = YamlLoader(Path("config.yaml"), validating_store)
loader.load_yaml()
```

## Benefits of LTREE Structure

1. **Efficient Queries**: Use LTREE operators for powerful path-based queries
2. **Hierarchy Preservation**: Maintain logical structure in flat storage
3. **Simple Loading**: No recursive parsing needed
4. **Flexibility**: Easy to filter, search, and manipulate paths
5. **Database Friendly**: Direct storage in PostgreSQL with LTREE type
6. **Human Readable**: Clear dot-notation paths

## Error Handling

Both classes include comprehensive error handling:

- `FileNotFoundError`: When YAML file or parent directory doesn't exist
- `ValueError`: For invalid inputs or path mismatches
- Detailed error reporting during load operations
- Path validation to ensure consistency

## Testing

Run the test suites:

```bash
# Test YamlGenerator
python yaml_generator.py

# Test YamlLoader
python yaml_loader.py
```

## License

MIT License - Feel free to use in your projects

## Contributing

Contributions are welcome! Please feel free to submit pull requests or open issues for bugs and feature requests.

## Future Enhancements

- [ ] Support for path wildcards in queries
- [ ] Batch operations for better performance
- [ ] Schema validation support
- [ ] Export to different formats (JSON, TOML)
- [ ] Path compression options
- [ ] Merge strategies for existing keys
- [ ] Transaction support for atomic updates

