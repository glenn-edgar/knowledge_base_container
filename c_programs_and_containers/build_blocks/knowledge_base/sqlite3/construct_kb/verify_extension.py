#!/usr/bin/env python3
"""
Simple verification that ltree extension loads correctly
"""

import sqlite3
import sys
import os

def test_direct_load(ext_path):
    """Test loading extension directly with sqlite3"""
    print(f"\n{'='*60}")
    print("Testing Direct Extension Loading")
    print('='*60)
    
    # Strip extension if present
    ext_path_clean = os.path.splitext(ext_path)[0]
    print(f"Original path: {ext_path}")
    print(f"Cleaned path:  {ext_path_clean}")
    
    # Check if file exists with proper suffix
    if sys.platform == 'darwin':
        ext_file = ext_path_clean + '.dylib'
    else:
        ext_file = ext_path_clean + '.so'
    
    print(f"Looking for:   {ext_file}")
    
    if not os.path.exists(ext_file):
        print(f"✗ Error: Extension file not found!")
        return False
    
    print(f"✓ Extension file exists")
    
    # Try to load
    try:
        conn = sqlite3.connect(':memory:')
        conn.enable_load_extension(True)
        conn.load_extension(ext_path_clean)
        print(f"✓ Extension loaded successfully")
        
        # Test a function
        cursor = conn.cursor()
        cursor.execute("SELECT ltree_depth('a.b.c')")
        result = cursor.fetchone()[0]
        print(f"✓ ltree_depth('a.b.c') = {result}")
        
        cursor.execute("SELECT ltree_match('kb.test.GATE_root', 'kb.*.GATE*')")
        result = cursor.fetchone()[0]
        print(f"✓ ltree_match('kb.test.GATE_root', 'kb.*.GATE*') = {result}")
        
        conn.close()
        return True
        
    except Exception as e:
        print(f"✗ Failed to load extension: {e}")
        return False

def test_with_manager(ext_path):
    """Test with KnowledgeBaseManager"""
    print(f"\n{'='*60}")
    print("Testing with KnowledgeBaseManager")
    print('='*60)
    
    try:
        from knowledge_base_manager import KnowledgeBaseManager
        
        # Strip extension - manager will handle it
        ext_path_clean = os.path.splitext(ext_path)[0]
        
        kb = KnowledgeBaseManager('test', ':memory:', ext_path_clean)
        
        # Add test data
        kb.add_kb('test_kb', 'Test')
        kb.add_node('test_kb', 'root', 'Root', {}, {}, 'root')
        kb.add_node('test_kb', 'gate', 'Gate', {}, {}, 'root.GATE_main')
        kb.add_node('test_kb', 'action', 'Action', {}, {}, 'root.GATE_main.ACT_test')
        
        print("✓ Added test nodes")
        
        # Test ltree queries
        results = kb.find_by_pattern('root.*.ACT*', 'test_kb')
        print(f"✓ Pattern match found {len(results)} results")
        
        depth = kb.get_node_depth('root.GATE_main.ACT_test')
        print(f"✓ Node depth: {depth}")
        
        children = kb.find_children('root', 'test_kb')
        print(f"✓ Found {len(children)} children of root")
        
        kb.disconnect()
        print("✓ All KnowledgeBaseManager tests passed")
        return True
        
    except Exception as e:
        print(f"✗ Manager test failed: {e}")
        import traceback
        traceback.print_exc()
        return False

def main():
    print("SQLite ltree Extension - Verification Script")
    print("=" * 60)
    
    # Get extension path
    if len(sys.argv) > 1:
        ext_path = sys.argv[1]
    else:
        # Search common locations
        search_paths = [
            './ltree',
            '/usr/local/lib/ltree',
            '/usr/lib/ltree',
        ]
        
        suffix = '.dylib' if sys.platform == 'darwin' else '.so'
        ext_path = None
        
        print("No path specified, searching common locations:")
        for path in search_paths:
            full_path = path + suffix
            print(f"  Checking {full_path}...", end=" ")
            if os.path.exists(full_path):
                print("✓ FOUND")
                ext_path = path
                break
            else:
                print("✗")
        
        if ext_path is None:
            print("\n✗ ERROR: ltree extension not found in common locations")
            print("\nPlease specify the path:")
            print("  python verify_extension.py /path/to/ltree")
            print("\nOr install with: sudo make install")
            return 1
    
    print(f"\nUsing extension path: {ext_path}")
    
    # Run tests
    test1_pass = test_direct_load(ext_path)
    test2_pass = test_with_manager(ext_path)
    
    # Summary
    print(f"\n{'='*60}")
    print("Summary")
    print('='*60)
    print(f"Direct loading:        {'✓ PASS' if test1_pass else '✗ FAIL'}")
    print(f"KnowledgeBaseManager:  {'✓ PASS' if test2_pass else '✗ FAIL'}")
    
    if test1_pass and test2_pass:
        print("\n✓ All tests passed! Extension is working correctly.")
        return 0
    else:
        print("\n✗ Some tests failed. Check error messages above.")
        return 1

if __name__ == '__main__':
    sys.exit(main())