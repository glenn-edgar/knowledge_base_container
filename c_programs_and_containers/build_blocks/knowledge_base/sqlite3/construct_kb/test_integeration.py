#!/usr/bin/env python3
"""
Test script to verify ltree extension integration with KnowledgeBaseManager
"""

import sys
import os
from knowledge_base_manager import KnowledgeBaseManager

def print_section(title):
    """Print a formatted section header"""
    print(f"\n{'='*60}")
    print(f"  {title}")
    print('='*60)

def test_basic_operations(kb_manager):
    """Test basic KB operations"""
    print_section("Testing Basic Operations")
    
    # Add knowledge bases
    print("\n1. Adding knowledge bases...")
    kb_manager.add_kb('test_kb', 'Test knowledge base')
    kb_manager.add_kb('behavior_trees', 'Behavior tree nodes')
    print("   ✓ Knowledge bases created")
    
    # Add hierarchical nodes
    print("\n2. Adding hierarchical nodes...")
    nodes = [
        ('test_kb', 'root', 'Root Node', {}, {}, 'root'),
        ('test_kb', 'branch', 'Branch A', {}, {}, 'root.branch_a'),
        ('test_kb', 'branch', 'Branch B', {}, {}, 'root.branch_b'),
        ('test_kb', 'leaf', 'Leaf A1', {}, {}, 'root.branch_a.leaf_a1'),
        ('test_kb', 'leaf', 'Leaf A2', {}, {}, 'root.branch_a.leaf_a2'),
        ('test_kb', 'leaf', 'Leaf B1', {}, {}, 'root.branch_b.leaf_b1'),
        
        ('behavior_trees', 'gate', 'Main Selector', {'type': 'selector'}, {}, 
         'kb.main.GATE_root._0'),
        ('behavior_trees', 'sequence', 'Init Sequence', {'type': 'sequence'}, {}, 
         'kb.main.GATE_root._0.SEQ_init._1'),
        ('behavior_trees', 'action', 'Home Motors', {'action': 'home'}, {}, 
         'kb.main.GATE_root._0.SEQ_init._1.ACT_home._0'),
        ('behavior_trees', 'collection', 'Wait Collection', {'type': 'wait'}, {}, 
         'kb.main.GATE_root._0.SEQ_init._1.COL_wait._1'),
        ('behavior_trees', 'gate', 'Fallback Gate', {'type': 'fallback'}, {}, 
         'kb.main.GATE_root._0.GATE_fallback._2'),
    ]
    
    for node_data in nodes:
        kb_manager.add_node(*node_data)
    print(f"   ✓ Added {len(nodes)} nodes")

def test_pattern_matching(kb_manager):
    """Test ltree pattern matching"""
    print_section("Testing Pattern Matching")
    
    tests = [
        ("Exact match", "root.branch_a.leaf_a1", 'test_kb', 1),
        ("Single wildcard", "root.*.leaf_a1", 'test_kb', 1),
        ("Multiple wildcards", "root.*.*", 'test_kb', 4),
        ("Prefix matching GATE", "kb.*.GATE*.*", 'behavior_trees', 2),
        ("Prefix matching COL", "kb.*.*.*.COL*.*", 'behavior_trees', 1),
        ("Quantified {2}", "kb.*{2}.GATE*.*", 'behavior_trees', 1),
        ("Quantified {1,3}", "root.*{1,2}", 'test_kb', 6),
    ]
    
    for test_name, pattern, kb_name, expected_min in tests:
        results = kb_manager.find_by_pattern(pattern, kb_name)
        count = len(results)
        status = "✓" if count >= expected_min else "✗"
        print(f"\n{status} {test_name}")
        print(f"   Pattern: {pattern}")
        print(f"   Found: {count} nodes")
        if count > 0 and count <= 3:
            for row in results:
                print(f"      - {row['path']}")

def test_hierarchy_queries(kb_manager):
    """Test hierarchical queries"""
    print_section("Testing Hierarchy Queries")
    
    # Test descendants
    print("\n1. Finding descendants of 'root.branch_a':")
    descendants = kb_manager.find_descendants('root.branch_a', 'test_kb')
    print(f"   Found {len(descendants)} descendants:")
    for row in descendants:
        print(f"      - {row['path']} ({row['name']})")
    
    # Test ancestors
    print("\n2. Finding ancestors of 'root.branch_a.leaf_a1':")
    ancestors = kb_manager.find_ancestors('root.branch_a.leaf_a1', 'test_kb')
    print(f"   Found {len(ancestors)} ancestors:")
    for row in ancestors:
        print(f"      - {row['path']} ({row['name']})")
    
    # Test children
    print("\n3. Finding immediate children of 'root':")
    children = kb_manager.find_children('root', 'test_kb')
    print(f"   Found {len(children)} children:")
    for row in children:
        print(f"      - {row['path']} ({row['name']})")

def test_depth_queries(kb_manager):
    """Test depth-related queries"""
    print_section("Testing Depth Queries")
    
    # Test depth calculation
    paths = [
        'root',
        'root.branch_a',
        'root.branch_a.leaf_a1',
        'kb.main.GATE_root._0.SEQ_init._1.ACT_home._0'
    ]
    
    print("\n1. Path depths:")
    for path in paths:
        depth = kb_manager.get_node_depth(path)
        print(f"   {path:45} → depth {depth}")
    
    # Test finding by depth
    print("\n2. Finding all nodes at depth 3:")
    depth3_nodes = kb_manager.find_by_depth(3, 'test_kb')
    print(f"   Found {len(depth3_nodes)} nodes at depth 3:")
    for row in depth3_nodes:
        print(f"      - {row['path']}")

def test_complex_patterns(kb_manager):
    """Test complex pattern combinations"""
    print_section("Testing Complex Patterns")
    
    complex_tests = [
        ("All GATE nodes anywhere", "*.*.*.GATE*.*", 'behavior_trees'),
        ("SEQ with exactly 2 levels before", "kb.*{2}.SEQ*.*", 'behavior_trees'),
        ("Actions nested in sequences", "*.*.*.*.SEQ*.*.ACT*", 'behavior_trees'),
        ("Any node 5 levels deep", "*{5}", 'behavior_trees'),
    ]
    
    for test_name, pattern, kb_name in complex_tests:
        results = kb_manager.find_by_pattern(pattern, kb_name)
        print(f"\n✓ {test_name}")
        print(f"   Pattern: {pattern}")
        print(f"   Results: {len(results)} matches")
        for row in results[:3]:  # Show first 3
            print(f"      - {row['path']}")

def main():
    """Main test function"""
    # Note: SQLite's load_extension() automatically adds .so/.dll/.dylib
    # so we pass the path WITHOUT the extension
    
    if len(sys.argv) > 1:
        # User specified path
        ltree_ext = sys.argv[1]
        # Strip extension if provided
        ltree_ext = os.path.splitext(ltree_ext)[0]
    else:
        # Auto-detect from common locations
        search_paths = [
            './ltree',
            '/usr/local/lib/ltree',
            '/usr/lib/ltree',
        ]
        
        suffix = '.dylib' if sys.platform == 'darwin' else '.so'
        ltree_ext = None
        
        print("Searching for ltree extension in common locations:")
        for path in search_paths:
            ext_file = path + suffix
            print(f"  Checking {ext_file}...", end=" ")
            if os.path.exists(ext_file):
                print("✓ FOUND")
                ltree_ext = path
                break
            else:
                print("✗")
        
        if ltree_ext is None:
            print(f"\n❌ ERROR: Ltree extension not found in common locations")
            print("Please build and install the extension:")
            print("  make")
            print("  sudo make install")
            print("\nOr specify the path:")
            print("  python test_ltree_integration.py /path/to/ltree")
            sys.exit(1)
    
    print(f"\nUsing ltree extension: {ltree_ext}")
    
    # Verify file exists
    if sys.platform == 'darwin':
        ext_file = ltree_ext + '.dylib'
    else:
        ext_file = ltree_ext + '.so'
    
    if not os.path.exists(ext_file):
        print(f"\n❌ ERROR: Ltree extension not found at {ext_file}")
        print("Please build the extension first:")
        print("  make")
        print("  sudo make install")
        sys.exit(1)
    
    # Create test database in memory
    db_path = ':memory:'
    
    print(f"\nInitializing KnowledgeBaseManager...")
    kb_manager = KnowledgeBaseManager('test', db_path, ltree_ext)
    
    try:
        # Run all tests
        test_basic_operations(kb_manager)
        test_pattern_matching(kb_manager)
        test_hierarchy_queries(kb_manager)
        test_depth_queries(kb_manager)
        test_complex_patterns(kb_manager)
        
        print_section("All Tests Complete")
        print("\n✓ All ltree integration tests passed successfully!")
        
    except Exception as e:
        print(f"\n❌ Test failed with error: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)
        
    finally:
        kb_manager.disconnect()

if __name__ == '__main__':
    main()
