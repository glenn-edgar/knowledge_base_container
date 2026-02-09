#!/usr/bin/env python3
"""
Test script for KB_Search SQLite implementation
Demonstrates the main functionality of the converted class
"""

import sqlite3
import json
from .kb_query_support import KB_Search

def create_test_database(db_path):
    """
    Create a test database with sample data for demonstration.
    """
    # Connect and load ltree extension
    conn = sqlite3.connect(db_path)
    conn.enable_load_extension(True)
    try:
        conn.load_extension('/usr/local/lib/ltree.so')
        print(f"✓ Loaded ltree extension from /usr/local/lib/ltree.so")
    except sqlite3.OperationalError as e:
        print(f"✗ Failed to load ltree extension: {e}")
        conn.close()
        return False
    conn.enable_load_extension(False)
    
    cursor = conn.cursor()
    
    # Create knowledge_base table
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS knowledge_base (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            knowledge_base TEXT NOT NULL,
            label TEXT,
            name TEXT,
            path TEXT NOT NULL UNIQUE,
            properties TEXT,  -- JSON stored as text
            data TEXT,
            has_link INTEGER DEFAULT 0,
            has_link_mount INTEGER DEFAULT 0
        )
    ''')
    
    # Clear existing data
    cursor.execute('DELETE FROM knowledge_base')
    
    # Insert sample data
    sample_data = [
        {
            'knowledge_base': 'tech_docs',
            'label': 'root',
            'name': 'documentation',
            'path': 'tech_docs',
            'properties': json.dumps({'description': 'Root of technical documentation'}),
            'data': 'Root node data',
            'has_link': 0,
            'has_link_mount': 0
        },
        {
            'knowledge_base': 'tech_docs',
            'label': 'section',
            'name': 'python',
            'path': 'tech_docs.python',
            'properties': json.dumps({'description': 'Python documentation', 'version': '3.11'}),
            'data': 'Python section data',
            'has_link': 1,
            'has_link_mount': 0
        },
        {
            'knowledge_base': 'tech_docs',
            'label': 'article',
            'name': 'basics',
            'path': 'tech_docs.python.basics',
            'properties': json.dumps({'description': 'Python basics tutorial', 'difficulty': 'beginner'}),
            'data': 'Basic Python tutorial content',
            'has_link': 0,
            'has_link_mount': 0
        },
        {
            'knowledge_base': 'tech_docs',
            'label': 'article',
            'name': 'advanced',
            'path': 'tech_docs.python.advanced',
            'properties': json.dumps({'description': 'Advanced Python concepts', 'difficulty': 'advanced'}),
            'data': 'Advanced Python content',
            'has_link': 0,
            'has_link_mount': 1
        },
        {
            'knowledge_base': 'tech_docs',
            'label': 'section',
            'name': 'javascript',
            'path': 'tech_docs.javascript',
            'properties': json.dumps({'description': 'JavaScript documentation', 'version': 'ES2023'}),
            'data': 'JavaScript section data',
            'has_link': 0,
            'has_link_mount': 0
        },
        {
            'knowledge_base': 'tech_docs',
            'label': 'article',
            'name': 'intro',
            'path': 'tech_docs.javascript.intro',
            'properties': json.dumps({'description': 'JavaScript introduction', 'difficulty': 'beginner'}),
            'data': 'JavaScript intro content',
            'has_link': 1,
            'has_link_mount': 0
        },
        {
            'knowledge_base': 'api_docs',
            'label': 'root',
            'name': 'api',
            'path': 'api_docs',
            'properties': json.dumps({'description': 'API documentation root'}),
            'data': 'API root data',
            'has_link': 0,
            'has_link_mount': 0
        },
        {
            'knowledge_base': 'api_docs',
            'label': 'endpoint',
            'name': 'users',
            'path': 'api_docs.users',
            'properties': json.dumps({'description': 'User management API', 'version': 'v2'}),
            'data': 'User API data',
            'has_link': 0,
            'has_link_mount': 0
        }
    ]
    
    for row in sample_data:
        cursor.execute('''
            INSERT INTO knowledge_base 
            (knowledge_base, label, name, path, properties, data, has_link, has_link_mount)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        ''', (
            row['knowledge_base'],
            row['label'],
            row['name'],
            row['path'],
            row['properties'],
            row['data'],
            row['has_link'],
            row['has_link_mount']
        ))
    
    conn.commit()
    conn.close()
    print(f"✓ Created test database with {len(sample_data)} records")
    return True

def print_results(results, title):
    """Pretty print query results."""
    print(f"\n{'='*60}")
    print(f"{title}")
    print(f"{'='*60}")
    
    if not results:
        print("No results found")
        return
    
    for i, row in enumerate(results, 1):
        print(f"\n{i}. Path: {row.get('path', 'N/A')}")
        print(f"   Knowledge Base: {row.get('knowledge_base', 'N/A')}")
        print(f"   Label: {row.get('label', 'N/A')}")
        print(f"   Name: {row.get('name', 'N/A')}")
        
        props = row.get('properties', '{}')
        if isinstance(props, str):
            try:
                props_dict = json.loads(props)
                if props_dict:
                    print(f"   Properties: {props_dict}")
            except json.JSONDecodeError:
                pass
    
    print(f"\nTotal results: {len(results)}")

def test_kb_search():
    """Run various tests on the KB_Search class."""
    
    db_path = "test_kb.db"
    #ltree_extension_path = './ltree'  # Adjust this to your ltree extension path
    
    print("Initializing test database...")
    if not create_test_database(db_path):
        print("Failed to create test database. Exiting.")
        return
    
    print("\nInitializing KB_Search...")
    try:
        kb = KB_Search(
            db_path=db_path,
            database='knowledge_base',
            ltree_extension_path=None
        )
    except Exception as e:
        print(f"Failed to initialize KB_Search: {e}")
        return
    
    # Test 1: Search by knowledge base
    print("\n" + "="*60)
    print("TEST 1: Search by knowledge base")
    print("="*60)
    kb.clear_filters()
    kb.search_kb('tech_docs')
    results = kb.execute_query()
    print_results(results, "All tech_docs entries")
    
    # Test 2: Search by label
    print("\n" + "="*60)
    print("TEST 2: Search by label")
    print("="*60)
    kb.clear_filters()
    kb.search_label('article')
    results = kb.execute_query()
    print_results(results, "All articles")
    
    # Test 3: Search by name
    print("\n" + "="*60)
    print("TEST 3: Search by name")
    print("="*60)
    kb.clear_filters()
    kb.search_name('python')
    results = kb.execute_query()
    print_results(results, "Items named 'python'")
    
    # Test 4: Path pattern matching - exact
    print("\n" + "="*60)
    print("TEST 4: Exact path match")
    print("="*60)
    kb.clear_filters()
    kb.search_path('tech_docs.python')
    results = kb.execute_query()
    print_results(results, "Exact match: tech_docs.python")
    
    # Test 5: Path pattern matching - wildcard
    print("\n" + "="*60)
    print("TEST 5: Path wildcard match")
    print("="*60)
    kb.clear_filters()
    kb.search_path('tech_docs.*')
    results = kb.execute_query()
    print_results(results, "Direct children of tech_docs")
    
    # Test 6: Path pattern matching - quantified wildcard
    print("\n" + "="*60)
    print("TEST 6: Path quantified wildcard")
    print("="*60)
    kb.clear_filters()
    kb.search_path('tech_docs.*{1,2}')
    results = kb.execute_query()
    print_results(results, "tech_docs descendants 1-2 levels deep")
    
    # Test 7: Ancestor search
    print("\n" + "="*60)
    print("TEST 7: Ancestor search")
    print("="*60)
    kb.clear_filters()
    kb.search_starting_path('tech_docs.python')
    results = kb.execute_query()
    print_results(results, "All descendants of tech_docs.python")
    
    # Test 8: Property key search
    print("\n" + "="*60)
    print("TEST 8: Property key existence")
    print("="*60)
    kb.clear_filters()
    kb.search_property_key('difficulty')
    results = kb.execute_query()
    print_results(results, "Items with 'difficulty' property")
    
    # Test 9: Property value search
    print("\n" + "="*60)
    print("TEST 9: Property value match")
    print("="*60)
    kb.clear_filters()
    kb.search_property_value('difficulty', 'beginner')
    results = kb.execute_query()
    print_results(results, "Items with difficulty='beginner'")
    
    # Test 10: Has link search
    print("\n" + "="*60)
    print("TEST 10: Has link search")
    print("="*60)
    kb.clear_filters()
    kb.search_has_link()
    results = kb.execute_query()
    print_results(results, "Items with has_link=TRUE")
    
    # Test 11: Combined filters
    print("\n" + "="*60)
    print("TEST 11: Combined filters")
    print("="*60)
    kb.clear_filters()
    kb.search_kb('tech_docs')
    kb.search_label('article')
    kb.search_property_key('difficulty')
    results = kb.execute_query()
    print_results(results, "tech_docs articles with difficulty property")
    
    # Test 12: find_description_paths
    print("\n" + "="*60)
    print("TEST 12: Find data by paths")
    print("="*60)
    paths = ['tech_docs.python.basics', 'tech_docs.javascript.intro', 'nonexistent.path']
    data_dict = kb.find_description_paths(paths)
    print("Data for specified paths:")
    for path, data in data_dict.items():
        print(f"  {path}: {data}")
    
    # Test 13: decode_link_nodes
    print("\n" + "="*60)
    print("TEST 13: Decode link nodes")
    print("="*60)
    test_path = 'kb_main.uuid1.parent.uuid2.child.uuid3.grandchild'
    try:
        kb_name, node_pairs = kb.decode_link_nodes(test_path)
        print(f"Path: {test_path}")
        print(f"KB: {kb_name}")
        print(f"Node pairs:")
        for link, name in node_pairs:
            print(f"  [{link}, {name}]")
    except ValueError as e:
        print(f"Error: {e}")
    
    # Cleanup
    kb.disconnect()
    print("\n" + "="*60)
    print("All tests completed!")
    print("="*60)

if __name__ == '__main__':
    test_kb_search()

