# SQLite KnowledgeBaseManager - PostgreSQL to SQLite Translation

This package contains the SQLite3 translation of your PostgreSQL KnowledgeBaseManager class, with full integration for your custom ltree extension.

## Files Included

### Core Implementation
1. **knowledge_base_manager_sqlite_ltree.py** - Full-featured SQLite version with ltree integration
   - Loads and uses your custom ltree extension
   - Includes all original CRUD operations
   - Adds 6 new ltree-powered query methods

### Documentation
2. **LTREE_USAGE_GUIDE.md** - Comprehensive usage guide
   - Pattern matching syntax reference
   - Examples for all query methods
   - Performance tips
   - Migration notes from PostgreSQL

### Testing
3. **test_ltree_integration.py** - Comprehensive test suite
   - Tests all basic operations
   - Validates pattern matching
   - Verifies hierarchy queries
   - Checks depth calculations
   - Exercises complex patterns

## Quick Start

### 1. Build Your Ltree Extension

```bash
cd /path/to/your/ltree/extension
make
make test
```

### 2. Run the Integration Test

```bash
python test_ltree_integration.py ./ltree.so
# Or on macOS:
python test_ltree_integration.py ./ltree.dylib
```

### 3. Use in Your Code

```python
from knowledge_base_manager_sqlite_ltree import KnowledgeBaseManager

# Initialize - NOTE: Pass path WITHOUT .so/.dylib extension!
# SQLite automatically adds the appropriate suffix
kb = KnowledgeBaseManager(
    table_name='my_kb',
    db_path='database.db',
    ltree_extension_path='./ltree'  # NOT './ltree.so'
)

# Add data
kb.add_kb('robotics', 'Robot control system')
kb.add_node('robotics', 'gate', 'Main Gate', 
            {}, {}, 'system.GATE_main._0')

# Query with ltree patterns
results = kb.find_by_pattern('system.*.GATE*.*', 'robotics')
descendants = kb.find_descendants('system.GATE_main._0', 'robotics')
depth = kb.get_node_depth('system.GATE_main._0')

kb.disconnect()
```

### Important: Extension Path Format

⚠️ **Critical**: SQLite's `load_extension()` automatically appends `.so`/`.dll`/`.dylib`

```python
# ✅ CORRECT - Pass path without extension
ltree_extension_path='./ltree'

# ❌ WRONG - Will try to load ./ltree.so.so
ltree_extension_path='./ltree.so'
```

See [EXTENSION_LOADING_GUIDE.md](computer:///mnt/user-data/outputs/EXTENSION_LOADING_GUIDE.md) for details.

## Key Differences from PostgreSQL Version

| Feature | PostgreSQL | SQLite |
|---------|-----------|--------|
| Connection | Host/port/credentials | File path |
| Booleans | BOOLEAN | INTEGER (0/1) |
| Timestamps | TIMESTAMP | TEXT (ISO format) |
| JSON | Native JSON | TEXT (JSON strings) |
| Ltree | Built-in extension | Custom extension (.so/.dylib) |
| Placeholders | `%s` | `?` |
| Insert conflict | `ON CONFLICT DO NOTHING` | `INSERT OR IGNORE` |

## New Ltree Query Methods

Your SQLite version includes these powerful query methods that leverage your ltree extension:

### 1. `find_by_pattern(pattern, kb_name=None)`
Pattern matching with wildcards and quantifiers:
- `'kb.*.GATE*.*'` - Find GATE nodes with wildcard
- `'kb.*{2}.node'` - Exactly 2 levels between kb and node
- `'kb.*{1,3}.node'` - 1 to 3 levels

### 2. `find_descendants(parent_path, kb_name=None)`
Find all nodes below a parent in the hierarchy

### 3. `find_ancestors(child_path, kb_name=None)`
Find all nodes above a child in the hierarchy

### 4. `get_node_depth(path)`
Get the depth (number of levels) of a path

### 5. `find_by_depth(depth, kb_name=None)`
Find all nodes at a specific depth level

### 6. `find_children(parent_path, kb_name=None)`
Find immediate children only (not grandchildren)

## Pattern Syntax Reference

Your ltree extension supports these patterns:

```python
# Exact match
'kb.test.node'

# Single wildcard (matches one label)
'kb.*.node'

# Multiple wildcards
'kb.*.*.node'

# Prefix matching
'kb.*.GATE*.*'      # Labels starting with GATE
'kb.*.COL*.*'       # Labels starting with COL

# Quantified wildcards
'kb.*{2}.node'      # Exactly 2 levels
'kb.*{1,3}.node'    # 1 to 3 levels
'kb.*{2,}.node'     # 2 or more levels
'kb.*{,2}.node'     # 0 to 2 levels

# Complex combinations
'kb.*{2}.GATE*.*.COL*.*'
```

## Example Use Cases

### ChainTree Integration
```python
# Find all GATE nodes in a behavior tree
gates = kb.find_by_pattern('*.*.*.GATE*.*', 'my_chain')

# Find all descendants of a supervisor node
children = kb.find_descendants('system.supervisor._0', 'my_chain')

# Find all leaf nodes (actions, typically at depth 5)
leaves = kb.find_by_depth(5, 'my_chain')
```

### Hierarchical Queries
```python
# Find all nodes at a specific level
level3 = kb.find_by_depth(3)

# Get immediate children only
direct_children = kb.find_children('root.branch')

# Check node depth before processing
if kb.get_node_depth(path) > max_depth:
    print("Path too deep!")
```

## Migration Checklist from PostgreSQL

- [x] Replace `psycopg2` imports with `sqlite3`
- [x] Update connection parameters (file path vs host/port)
- [x] Change data types (BOOLEAN→INTEGER, JSON→TEXT)
- [x] Update query placeholders (`%s` → `?`)
- [x] Remove `sql.SQL()` and `sql.Identifier()` usage
- [x] Update INSERT conflict syntax
- [x] Load ltree extension explicitly
- [x] Add ltree query methods
- [x] Test all functionality

## Performance Tips

1. **Always specify kb_name** when possible to use indexes
2. **Use specific patterns** rather than broad wildcards
3. **Prefer exact quantifiers** like `{2}` over ranges like `{1,10}`
4. **Test patterns** on small datasets first
5. **Create indexes** on frequently queried columns

## Troubleshooting

### Extension Not Loading - `./ltree.so.so: cannot open shared object file`
```
Error: ./ltree.so.so: cannot open shared object file
```
**Solution**: Don't include the `.so`/`.dylib` extension in the path. Use `'./ltree'` not `'./ltree.so'`

SQLite automatically adds the platform-specific suffix, so passing `./ltree.so` results in `./ltree.so.so`!

### Extension Not Loading - Other Issues
```
Error: Could not load ltree extension
```
**Solution**: 
1. Verify extension file exists: `ls ltree.so` (Linux) or `ls ltree.dylib` (macOS)
2. Ensure extension is built for your architecture
3. Check file permissions
4. Use absolute path if relative path fails

### Pattern Not Matching
```
No results for pattern 'kb.*.GATE*.*'
```
**Solution**: Check that paths in database match pattern syntax (case-sensitive)

### Import Error
```
ModuleNotFoundError: No module named 'knowledge_base_manager_sqlite_ltree'
```
**Solution**: Ensure the .py file is in your Python path or current directory

## Testing Your Setup

Run the comprehensive test suite:

```bash
# Run with default ltree.so location
python test_ltree_integration.py

# Specify custom extension path
python test_ltree_integration.py /path/to/ltree.so
```

Expected output:
```
✓ Testing Basic Operations
✓ Testing Pattern Matching  
✓ Testing Hierarchy Queries
✓ Testing Depth Queries
✓ Testing Complex Patterns
✓ All ltree integration tests passed successfully!
```

## Architecture Notes

Your ChainTree system's hierarchical structure maps perfectly to ltree paths:

```
kb.system.GATE_root._0                    # Selector gate
├── SEQ_init._1                           # Sequence
│   ├── ACT_home._0                       # Action
│   └── COL_wait._1                       # Collection
└── GATE_fallback._2                      # Fallback gate
```

The ltree queries enable efficient traversal and pattern matching across your distributed control hierarchy, whether running on 32KB microcontrollers or 8GB+ servers.

## Next Steps

1. Integrate with your NATS messaging system
2. Add checkpoint/restore to YAML using ltree paths
3. Create Mako templates for code generation from ltree patterns
4. Build comprehensive documentation system
5. Add validation frameworks for path consistency

## Support

For issues or questions about:
- **SQLite translation**: Check LTREE_USAGE_GUIDE.md
- **Ltree extension**: See your test_ltree.c test suite
- **Pattern syntax**: Reference the pattern table in this README

---

**Note**: This SQLite version maintains full API compatibility with your PostgreSQL version, making migration seamless for your ChainTree distributed control system.
