#!/usr/bin/env python3
"""
Quick Start Examples for KnowledgeBaseManager with ltree
"""

from knowledge_base_manager_sqlite_ltree import KnowledgeBaseManager

print("="*60)
print("KnowledgeBaseManager - Quick Start Examples")
print("="*60)

# ========================================
# Example 1: Auto-detect extension (RECOMMENDED)
# ========================================
print("\n1. Auto-detect ltree extension from common locations")
print("   (checks ./ltree, /usr/local/lib/ltree, /usr/lib/ltree)")

kb = KnowledgeBaseManager(
    table_name='my_kb',
    db_path=':memory:'  # Use in-memory database for demo
    # ltree_extension_path not specified - will auto-detect
)

# Add some test data
kb.add_kb('demo', 'Demo knowledge base')
kb.add_node('demo', 'root', 'Root', {}, {}, 'system')
kb.add_node('demo', 'gate', 'Main Gate', {}, {}, 'system.GATE_main._0')
kb.add_node('demo', 'sequence', 'Init Seq', {}, {}, 'system.GATE_main._0.SEQ_init._1')
kb.add_node('demo', 'action', 'Home', {}, {}, 'system.GATE_main._0.SEQ_init._1.ACT_home._0')

print("   ✓ Added 4 nodes")

# Test ltree queries
print("\n   Testing ltree queries:")
gates = kb.find_by_pattern('*.*.GATE*.*', 'demo')
print(f"   - Found {len(gates)} GATE nodes")

descendants = kb.find_descendants('system.GATE_main._0', 'demo')
print(f"   - Found {len(descendants)} descendants of main gate")

depth = kb.get_node_depth('system.GATE_main._0.SEQ_init._1.ACT_home._0')
print(f"   - Action node depth: {depth}")

kb.disconnect()
print("   ✓ Auto-detect mode working!")

# ========================================
# Example 2: Explicit path - installed location
# ========================================
print("\n2. Using installed extension (e.g., after 'sudo make install')")

kb = KnowledgeBaseManager(
    table_name='my_kb2',
    db_path=':memory:',
    ltree_extension_path='/usr/local/lib/ltree'  # No .so suffix!
)

kb.add_kb('test', 'Test KB')
kb.add_node('test', 'node', 'Test Node', {}, {}, 'root.test')
results = kb.find_by_pattern('root.*', 'test')
print(f"   ✓ Found {len(results)} nodes using installed extension")
kb.disconnect()

# ========================================
# Example 3: Explicit path - local build
# ========================================
print("\n3. Using local build in current directory")

kb = KnowledgeBaseManager(
    table_name='my_kb3',
    db_path=':memory:',
    ltree_extension_path='./ltree'  # Local build
)

kb.add_kb('local', 'Local KB')
kb.add_node('local', 'node', 'Local Node', {}, {}, 'local.test')
results = kb.find_by_pattern('local.*', 'local')
print(f"   ✓ Found {len(results)} nodes using local extension")
kb.disconnect()

# ========================================
# Example 4: Practical ChainTree Usage
# ========================================
print("\n4. ChainTree Behavior Tree Example")

kb = KnowledgeBaseManager('chaintree', ':memory:')

# Create a behavior tree
kb.add_kb('robot_control', 'Robot control behavior tree')

# Root selector
kb.add_node('robot_control', 'gate', 'Root Selector',
           {'type': 'selector'}, {},
           'kb.robot.GATE_root._0')

# Initialization sequence
kb.add_node('robot_control', 'sequence', 'Init Sequence',
           {'type': 'sequence'}, {},
           'kb.robot.GATE_root._0.SEQ_init._1')

# Actions in init sequence
kb.add_node('robot_control', 'action', 'Home Motors',
           {'action': 'home'}, {},
           'kb.robot.GATE_root._0.SEQ_init._1.ACT_home._0')

kb.add_node('robot_control', 'action', 'Enable Controllers',
           {'action': 'enable'}, {},
           'kb.robot.GATE_root._0.SEQ_init._1.ACT_enable._1')

# Main operation sequence
kb.add_node('robot_control', 'sequence', 'Main Operation',
           {'type': 'sequence'}, {},
           'kb.robot.GATE_root._0.SEQ_main._2')

print("   ✓ Created robot control behavior tree")

# Query examples
print("\n   ChainTree Queries:")

# Find all GATE nodes
gates = kb.find_by_pattern('kb.*.GATE*.*', 'robot_control')
print(f"   - All GATE nodes: {len(gates)}")
for g in gates:
    print(f"     • {g['path']} - {g['name']}")

# Find all actions
actions = kb.find_by_pattern('*.*.*.*.ACT*', 'robot_control')
print(f"   - All ACT nodes: {len(actions)}")
for a in actions:
    print(f"     • {a['path']} - {a['name']}")

# Find all descendants of init sequence
init_children = kb.find_descendants('kb.robot.GATE_root._0.SEQ_init._1', 'robot_control')
print(f"   - Init sequence children: {len(init_children)}")
for c in init_children:
    print(f"     • {c['path']} - {c['name']}")

# Find nodes at specific depth
depth_4 = kb.find_by_depth(4, 'robot_control')
print(f"   - Nodes at depth 4: {len(depth_4)}")

kb.disconnect()

print("\n" + "="*60)
print("✓ All examples completed successfully!")
print("="*60)

print("\nKey Points:")
print("  • No extension suffix needed (SQLite adds .so/.dylib automatically)")
print("  • Auto-detect searches: ./ltree, /usr/local/lib/ltree, /usr/lib/ltree")
print("  • Pattern syntax: wildcards (*), prefix (GATE*), quantifiers (*{2})")
print("  • Hierarchical queries: descendants, ancestors, children, depth")
print("\nNext Steps:")
print("  • See LTREE_USAGE_GUIDE.md for full pattern syntax")
print("  • Run test_ltree_integration.py for comprehensive tests")
print("  • Check EXTENSION_LOADING_GUIDE.md for troubleshooting")
