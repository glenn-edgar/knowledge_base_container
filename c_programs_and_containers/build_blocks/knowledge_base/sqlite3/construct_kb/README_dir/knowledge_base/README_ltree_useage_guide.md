# SQLite KnowledgeBaseManager with Ltree Extension

## Overview

This version of KnowledgeBaseManager integrates with your custom SQLite ltree extension to provide powerful hierarchical path queries.

## Installation

1. Build the ltree extension:
   ```bash
   make
   make test
   ```

2. Use the ltree-enabled manager:
   ```python
   from knowledge_base_manager_sqlite_ltree import KnowledgeBaseManager
   
   # Initialize with path to ltree extension
   # IMPORTANT: Pass path WITHOUT .so/.dylib extension!
   # SQLite automatically adds the appropriate suffix
   kb_manager = KnowledgeBaseManager(
       table_name='knowledge_base',
       db_path='my_database.db',
       ltree_extension_path='./ltree'  # NOT './ltree.so'!
   )
   ```

**Why no extension suffix?**  
SQLite's `load_extension()` automatically appends `.so` (Linux), `.dylib` (macOS), or `.dll` (Windows). Passing `./ltree.so` would result in trying to load `./ltree.so.so`!

## New Ltree Query Methods

### 1. Pattern Matching: `find_by_pattern(pattern, kb_name=None)`

Find nodes matching ltree patterns with wildcards and quantifiers.

**Supported Patterns:**

- **Exact match**: `'kb.test.node'`
- **Single wildcard**: `'kb.*.node'` (matches any single label)
- **Multiple wildcards**: `'*.*.*'` (matches any 3-level path)
- **Prefix matching**: `'kb.*.GATE*.*'` (matches labels starting with "GATE")
- **Quantified wildcards**:
  - `'kb.*{2}.node'` - exactly 2 levels between kb and node
  - `'kb.*{1,3}.node'` - 1 to 3 levels
  - `'kb.*{2,}.node'` - 2 or more levels
  - `'kb.*{,2}.node'` - 0 to 2 levels

**Examples:**

```python
# Find all GATE nodes
results = kb_manager.find_by_pattern('*.*.GATE*.*', 'kb1')

# Find nodes with exactly 2 levels between kb and GATE
results = kb_manager.find_by_pattern('kb.*{2}.GATE*', 'kb1')

# Complex pattern
results = kb_manager.find_by_pattern('kb.*{2}.GATE*.*.COL*.*', 'kb1')

for row in results:
    print(f"Path: {row['path']}, Name: {row['name']}")
```

### 2. Find Descendants: `find_descendants(parent_path, kb_name=None)`

Find all nodes that are descendants of a given path.

```python
# Find all descendants of 'people.john'
descendants = kb_manager.find_descendants('people.john', 'kb1')

for row in descendants:
    print(f"Descendant: {row['path']}")
```

### 3. Find Ancestors: `find_ancestors(child_path, kb_name=None)`

Find all nodes that are ancestors of a given path.

```python
# Find all ancestors of 'people.john.children.little_john'
ancestors = kb_manager.find_ancestors('people.john.children.little_john', 'kb1')

for row in ancestors:
    print(f"Ancestor: {row['path']}")
```

### 4. Get Node Depth: `get_node_depth(path)`

Get the depth (number of levels) of a path.

```python
depth = kb_manager.get_node_depth('people.john.children.little_john')
print(f"Depth: {depth}")  # Output: Depth: 4
```

### 5. Find by Depth: `find_by_depth(depth, kb_name=None)`

Find all nodes at a specific depth level.

```python
# Find all nodes at depth 2
nodes = kb_manager.find_by_depth(2, 'kb1')

for row in nodes:
    print(f"Level 2 node: {row['path']}")
```

### 6. Find Children: `find_children(parent_path, kb_name=None)`

Find immediate children (direct descendants, not grandchildren).

```python
# Find direct children of 'people'
children = kb_manager.find_children('people', 'kb1')

for row in children:
    print(f"Child: {row['path']}")
```

## Complete Example

```python
from knowledge_base_manager_sqlite_ltree import KnowledgeBaseManager

# Initialize
kb_manager = KnowledgeBaseManager(
    table_name='my_kb',
    db_path='knowledge.db',
    ltree_extension_path='./ltree.so'
)

try:
    # Create knowledge base
    kb_manager.add_kb('robotics', 'Robotics control system')
    
    # Add hierarchical nodes
    kb_manager.add_node('robotics', 'gate', 'Main Selector', 
                       {}, {}, 'system.GATE_main._0')
    
    kb_manager.add_node('robotics', 'sequence', 'Initialize',
                       {}, {}, 'system.GATE_main._0.SEQ_init._1')
    
    kb_manager.add_node('robotics', 'action', 'Home Motors',
                       {}, {}, 'system.GATE_main._0.SEQ_init._1.ACT_home._0')
    
    kb_manager.add_node('robotics', 'action', 'Enable Controllers',
                       {}, {}, 'system.GATE_main._0.SEQ_init._1.ACT_enable._1')
    
    # Query 1: Find all GATE nodes
    print("\n=== All GATE nodes ===")
    gates = kb_manager.find_by_pattern('*.*.GATE*.*', 'robotics')
    for row in gates:
        print(f"  {row['path']}")
    
    # Query 2: Find all descendants of the main gate
    print("\n=== Descendants of main gate ===")
    descendants = kb_manager.find_descendants('system.GATE_main._0', 'robotics')
    for row in descendants:
        print(f"  {row['path']} ({row['label']})")
    
    # Query 3: Find nodes with exactly 3 levels between system and action
    print("\n=== Nodes with pattern system.*{3}.ACT* ===")
    actions = kb_manager.find_by_pattern('system.*{3}.ACT*', 'robotics')
    for row in actions:
        print(f"  {row['path']}")
    
    # Query 4: Find all nodes at depth 4
    print("\n=== All depth 4 nodes ===")
    depth4 = kb_manager.find_by_depth(4, 'robotics')
    for row in depth4:
        print(f"  {row['path']}")
    
    # Query 5: Find immediate children of SEQ_init
    print("\n=== Direct children of SEQ_init ===")
    children = kb_manager.find_children('system.GATE_main._0.SEQ_init._1', 'robotics')
    for row in children:
        print(f"  {row['path']}")
    
finally:
    kb_manager.disconnect()
```

## Performance Notes

1. **Indexes**: The path column is indexed for better performance
2. **KB Filtering**: Always specify `kb_name` when possible to use the composite index
3. **Pattern Complexity**: More specific patterns perform better than broad wildcards
4. **Quantifiers**: Use specific ranges rather than open-ended quantifiers when possible

## Pattern Matching Reference

| Pattern | Description | Example Match |
|---------|-------------|---------------|
| `kb.test.node` | Exact path | `kb.test.node` |
| `kb.*.node` | Single wildcard | `kb.anything.node` |
| `kb.*.*.node` | Multiple wildcards | `kb.a.b.node` |
| `*.*.*` | Any 3-level path | `any.three.levels` |
| `kb.*.GATE*.*` | Prefix match | `kb.test.GATE_root._0` |
| `kb.*{2}.node` | Exactly 2 levels | `kb.a.b.node` |
| `kb.*{1,3}.node` | 1-3 levels | `kb.a.node` to `kb.a.b.c.node` |
| `kb.*{2,}.node` | 2+ levels | `kb.a.b.node`, `kb.a.b.c.node`, etc. |
| `kb.*{,2}.node` | 0-2 levels | `kb.node` to `kb.a.b.node` |

## Differences from PostgreSQL Version

1. **Extension Loading**: Must explicitly provide path to ltree.so/.dylib
2. **Data Types**: Uses INTEGER for booleans (0/1) instead of BOOLEAN
3. **Timestamps**: Stored as TEXT in ISO format
4. **JSON**: Stored as TEXT instead of native JSON type
5. **Indexes**: No GIST indexes (ltree extension provides matching functions)

## Error Handling

```python
try:
    results = kb_manager.find_by_pattern('*.invalid.*{-1}')
except sqlite3.Error as e:
    print(f"Invalid pattern: {e}")
```

## Testing

Run the unit test:

```bash
python knowledge_base_manager_sqlite_ltree.py
```

This will create a test database and demonstrate all ltree query methods.

