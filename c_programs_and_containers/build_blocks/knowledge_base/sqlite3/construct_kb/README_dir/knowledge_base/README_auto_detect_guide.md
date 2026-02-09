# Updated: Auto-Detection of Installed ltree Extension

## What Changed

Your ltree extension is installed in `/usr/local/lib/ltree.so`. The code now **auto-detects** common installation locations!

## Quick Usage

### Option 1: Auto-detect (Recommended)
```python
from knowledge_base_manager_sqlite_ltree import KnowledgeBaseManager

# No path needed - automatically finds extension
kb = KnowledgeBaseManager('my_kb', 'database.db')
```

The auto-detect searches these locations in order:
1. `./ltree` (current directory)
2. `/usr/local/lib/ltree` (your installed location) ✓
3. `/usr/lib/ltree` (system location)

### Option 2: Explicit Path
```python
# Specify installed location
kb = KnowledgeBaseManager(
    'my_kb', 
    'database.db',
    ltree_extension_path='/usr/local/lib/ltree'  # No .so!
)
```

### Option 3: Local Build
```python
# Use local build
kb = KnowledgeBaseManager(
    'my_kb',
    'database.db', 
    ltree_extension_path='./ltree'
)
```

## Testing

### Run Verification Script (Auto-detects)
```bash
python verify_extension.py
```

Output will show:
```
No path specified, searching common locations:
  Checking ./ltree.so... ✗
  Checking /usr/local/lib/ltree.so... ✓ FOUND

Using extension path: /usr/local/lib/ltree
```

### Run with Specific Path
```bash
python verify_extension.py /usr/local/lib/ltree
```

### Run Full Test Suite
```bash
python test_ltree_integration.py
```

### Run Quick Start Examples
```bash
python quick_start.py
```

## File Locations Reference

### Your System
- **Installed extension**: `/usr/local/lib/ltree.so` ✓
- **Source code**: Current directory (where Makefile is)

### Common Locations Searched
1. `./ltree.so` - Local build in current directory
2. `/usr/local/lib/ltree.so` - Installed (where yours is) ✓
3. `/usr/lib/ltree.so` - System-wide installation

## Important Notes

### ✅ Do This
```python
# Auto-detect (recommended)
kb = KnowledgeBaseManager('my_kb', 'db.sqlite')

# Or specify without extension
kb = KnowledgeBaseManager('my_kb', 'db.sqlite', '/usr/local/lib/ltree')
```

### ❌ Don't Do This
```python
# Don't include .so - SQLite adds it automatically!
kb = KnowledgeBaseManager('my_kb', 'db.sqlite', '/usr/local/lib/ltree.so')  # WRONG
```

## Verification Commands

```bash
# Verify extension is installed
ls -l /usr/local/lib/ltree.so

# Test direct loading
python verify_extension.py

# Test with KnowledgeBaseManager
python quick_start.py

# Full integration tests
python test_ltree_integration.py
```

## Example Output (verify_extension.py)

```
SQLite ltree Extension - Verification Script
============================================================
No path specified, searching common locations:
  Checking ./ltree.so... ✗
  Checking /usr/local/lib/ltree.so... ✓ FOUND

Using extension path: /usr/local/lib/ltree

============================================================
  Testing Direct Extension Loading
============================================================
Original path: /usr/local/lib/ltree
Cleaned path:  /usr/local/lib/ltree
Looking for:   /usr/local/lib/ltree.so
✓ Extension file exists
✓ Extension loaded successfully
✓ ltree_depth('a.b.c') = 3
✓ ltree_match('kb.test.GATE_root', 'kb.*.GATE*') = 1

============================================================
  Testing with KnowledgeBaseManager
============================================================
Loaded ltree extension from: /usr/local/lib/ltree
✓ Added test nodes
✓ Pattern match found 1 results
✓ Node depth: 3
✓ Found 1 children of root
✓ All KnowledgeBaseManager tests passed

============================================================
Summary
============================================================
Direct loading:        ✓ PASS
KnowledgeBaseManager:  ✓ PASS

✓ All tests passed! Extension is working correctly.
```

## Files Updated

All files now support auto-detection:

1. ✅ `knowledge_base_manager_sqlite_ltree.py` - Auto-detects common locations
2. ✅ `verify_extension.py` - Searches for extension automatically
3. ✅ `test_ltree_integration.py` - Auto-detects before running tests
4. ✅ `quick_start.py` - NEW: Multiple usage examples

## ChainTree Integration Example

```python
from knowledge_base_manager_sqlite_ltree import KnowledgeBaseManager

# Auto-detect extension
kb = KnowledgeBaseManager('chaintree', 'robot.db')

# Build your behavior tree
kb.add_kb('robot', 'Robot control system')
kb.add_node('robot', 'gate', 'Root Gate', {}, {}, 
           'system.GATE_root._0')
kb.add_node('robot', 'sequence', 'Init', {}, {}, 
           'system.GATE_root._0.SEQ_init._1')
kb.add_node('robot', 'action', 'Home', {}, {}, 
           'system.GATE_root._0.SEQ_init._1.ACT_home._0')

# Query with ltree patterns
gates = kb.find_by_pattern('*.*.GATE*.*', 'robot')
actions = kb.find_by_pattern('*.*.*.*.ACT*', 'robot')
children = kb.find_descendants('system.GATE_root._0', 'robot')

kb.disconnect()
```

## Next Steps

1. **Test it now**: `python verify_extension.py`
2. **Try examples**: `python quick_start.py`
3. **Integrate with your ChainTree system**
4. **Add NATS messaging integration**
5. **Create YAML checkpoint/restore**

## Need Help?

- Extension not found: Run `ls -l /usr/local/lib/ltree.so`
- Build extension: `make && sudo make install`
- See detailed guide: `EXTENSION_LOADING_GUIDE.md`
- Test extension: `make test`

Your system is ready to go with the installed extension at `/usr/local/lib/ltree.so`!
