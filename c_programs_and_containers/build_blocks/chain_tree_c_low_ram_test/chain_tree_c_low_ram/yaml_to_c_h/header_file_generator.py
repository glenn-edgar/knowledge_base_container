"""
Header File Generator Pipeline for ChainTree

This module implements a multi-stage pipeline for generating C header files
from ChainTree YAML data:
  Stage 1: Load YAML data into handle (ChainTreeYamlHandle)
  Stage 2: Build node ordering and indices
  Stage 3: Build function indices
  Stage 4: Build link tables
  Stage 5: Encode node data (JSON)
  Stage 6: Generate C header and implementation files
"""

from pathlib import Path
from typing import Dict, List, Set, Tuple, Optional
import random
import string
from .chain_tree_yaml_handle import ChainTreeYamlHandle
from .validated_function_index import ValidatedFunctionIndexer
from .node_data_encode import NodeDataEncoder


class NodeIndexBuilder:
    """
    Stage 2: Build node ordering and index mappings.
    
    Creates the final node array ordering using breadth-first traversal
    and builds index mappings for parent/child relationships.
    
    Filters out non-operational metadata nodes used only for function cataloging.
    """
    
    # Node labels that indicate function definition metadata (not operational)
    METADATA_NODE_LABELS = {
        'virtual_functions',
        'complete_functions',
        'main_functions',
        'one_shot_functions',
        'boolean_functions',
    }
    
    def __init__(self, handle: ChainTreeYamlHandle):
        self.handle = handle
        
        # Node ordering by KB
        self.kb_node_order: Dict[str, List[str]] = {}  # kb_name -> [ltree_names in order]
        self.kb_start_index: Dict[str, int] = {}  # kb_name -> starting index
        
        # Computed indices
        self.ltree_to_final_index: Dict[str, int] = {}  # ltree_name -> final array index
        self.final_index_to_ltree: List[str] = []  # Array of ltree names in final order
        
        # Track filtered nodes
        self.filtered_nodes: Set[str] = set()  # ltree names of filtered nodes Array of ltree names in final order
    
    
    def get_node_depth(self, ltree_name: str) -> int:
        """
        Calculate tree depth from ltree name.
        Format: kb.kb_name.level1.level2...
        Depth is the number of levels after kb.kb_name
        
        Args:
            ltree_name: Full ltree path (e.g., "kb.first_test.root.child1")
            
        Returns:
            Depth (0 for root, 1 for first level children, etc.)
        """
        parts = ltree_name.split('.')
        # Subtract 2 for "kb" and "kb_name" prefix
        if len(parts) < 2:
            return 0
        return int(len(parts) / 2)
    
    def _is_metadata_node(self, ltree_name: str) -> bool:
            """
            Check if a node is a function definition metadata node (should be filtered).
            
            Metadata nodes are used only during build for function cataloging,
            not for runtime execution.
            
            Args:
                ltree_name: Full ltree path of the node
                
            Returns:
                True if node should be filtered out, False if operational
            """
            node_data = self.handle.get_node_data(ltree_name)
            if not node_data:
                return False
            
            # Check node label
            label = node_data.get('label', '')
            if label in self.METADATA_NODE_LABELS:
                return True
            
            # Check if parent is a metadata node (entire subtree should be filtered)
            parent_ltree = self.handle.get_node_parent(ltree_name)
            if parent_ltree and parent_ltree in self.filtered_nodes:
                return True
            
            return False
  
    def build_node_ordering(self) -> None:
            """Build the node ordering for all knowledge bases, filtering metadata nodes."""
            current_index = 0
            total_filtered = 0
            
            for kb_name in self.handle.get_kb_names():
                self.kb_start_index[kb_name] = current_index
                
                # Use breadth-first traversal to get node order
                all_nodes = self.handle.traverse_kb_breadth_first(kb_name)
                
                # Filter out metadata nodes
                operational_nodes = []
                for ltree_name in all_nodes:
                    if self._is_metadata_node(ltree_name):
                        self.filtered_nodes.add(ltree_name)
                        total_filtered += 1
                    else:
                        operational_nodes.append(ltree_name)
                
                self.kb_node_order[kb_name] = operational_nodes
                
                # Assign final indices only to operational nodes
                for ltree_name in operational_nodes:
                    self.ltree_to_final_index[ltree_name] = current_index
                    self.final_index_to_ltree.append(ltree_name)
                    current_index += 1
            
            if total_filtered > 0:
                print(f"  Filtered out {total_filtered} function definition metadata nodes")
    
    def get_node_final_index(self, ltree_name: str) -> int:
        """Get the final array index for a node."""
        return self.ltree_to_final_index[ltree_name]
    
    def get_node_by_index(self, index: int) -> str:
        """Get the ltree name for a final index."""
        return self.final_index_to_ltree[index]
    
    def get_kb_range(self, kb_name: str) -> Tuple[int, int]:
        """Get the index range (start, end) for a knowledge base."""
        start = self.kb_start_index[kb_name]
        count = len(self.kb_node_order[kb_name])
        return (start, start + count)
    
    def get_total_nodes(self) -> int:
        """Get total number of nodes."""
        return len(self.final_index_to_ltree)
    
    def print_summary(self) -> None:
        """Print summary of node ordering."""
        print("=" * 70)
        print("Node Index Builder Summary")
        print("=" * 70)
        print(f"Total nodes: {self.get_total_nodes()}")
        
        for kb_name in self.kb_node_order.keys():
            start, end = self.get_kb_range(kb_name)
            count = end - start
            print(f"  {kb_name}: indices [{start}..{end-1}] ({count} nodes)")


class FunctionIndexBuilder:
    """
    Stage 3: Build function index tables.
    
    Creates index mappings for all function types with type suffixes:
    - Main functions: append "_main"
    - One-shot functions: append "_one_shot"
    - Boolean functions: append "_boolean"
    """
    
    def __init__(self, handle: ChainTreeYamlHandle):
        self.handle = handle
        
        # Function indexers with validity tracking
        self.main_indexer = ValidatedFunctionIndexer("main_function")
        self.one_shot_indexer = ValidatedFunctionIndexer("one_shot_function")
        self.boolean_indexer = ValidatedFunctionIndexer("boolean_function")
        
        # Track original names to typed names mapping
        self.main_name_map: Dict[str, str] = {}  # original -> name_main
        self.one_shot_name_map: Dict[str, str] = {}  # original -> name_one_shot
        self.boolean_name_map: Dict[str, str] = {}  # original -> name_boolean
    
    def _make_typed_name(self, func_name: str, suffix: str) -> str:
        """
        Convert function name to typed name with suffix.
        
        Args:
            func_name: Original function name (e.g., "CFL_NULL", "Init_System")
            suffix: Type suffix (e.g., "main", "one_shot", "boolean")
            
        Returns:
            Lowercase typed name (e.g., "cfl_null_main", "init_system_main")
        """
        return f"{func_name.lower()}_{suffix}"
    
    def build_function_indices(self) -> None:
        """Build function index tables from all nodes using original names."""
        
        # Get all functions from handle
        all_functions = self.handle.get_all_functions()
        
        # Add null functions as index 0 and mark them as valid
        self.main_indexer.add_function("CFL_NULL")
        self.main_indexer.set_function_valid(
            "CFL_NULL", 
            True, 
            "unsigned cfl_null_main_fn(void *handle, unsigned bool_function_index, unsigned node_index, unsigned event_type, unsigned event_id, void *event_data)", 
            "builtin"
        )
        self.main_name_map["CFL_NULL"] = self._make_typed_name("CFL_NULL", "main")
        
        self.one_shot_indexer.add_function("CFL_NULL")
        self.one_shot_indexer.set_function_valid(
            "CFL_NULL", 
            True,
            "void cfl_null_one_shot_fn(void *handle, unsigned node_index)",
            "builtin"
        )
        self.one_shot_name_map["CFL_NULL"] = self._make_typed_name("CFL_NULL", "one_shot")
        
        self.boolean_indexer.add_function("CFL_NULL")
        self.boolean_indexer.set_function_valid(
            "CFL_NULL", 
            True,
            "bool cfl_null_boolean_fn(void *handle, unsigned node_index, unsigned event_type, unsigned event_id, void *event_data)",
            "builtin"
        )
        self.boolean_name_map["CFL_NULL"] = self._make_typed_name("CFL_NULL", "boolean")
        
        # Index main functions with original names (skip CFL_NULL as it's already added)
        for func_name in sorted(all_functions['main']):
            if func_name == "CFL_NULL":
                continue
            typed_name = self._make_typed_name(func_name, "main")
            self.main_name_map[func_name] = typed_name
            self.main_indexer.add_function(func_name)  # Store original name
        
        # Index one-shot functions with original names (skip CFL_NULL as it's already added)
        for func_name in sorted(all_functions['one_shot']):
            if func_name == "CFL_NULL":
                continue
            typed_name = self._make_typed_name(func_name, "one_shot")
            self.one_shot_name_map[func_name] = typed_name
            self.one_shot_indexer.add_function(func_name)  # Store original name
        
        # Index boolean functions with original names (skip CFL_NULL as it's already added)
        for func_name in sorted(all_functions['boolean']):
            if func_name == "CFL_NULL":
                continue
            typed_name = self._make_typed_name(func_name, "boolean")
            self.boolean_name_map[func_name] = typed_name
            self.boolean_indexer.add_function(func_name)  # Store original name
        
    def get_typed_main_name(self, original_name: str) -> str:
        """Get the typed name for a main function."""
        return self.main_name_map.get(original_name, self._make_typed_name(original_name, "main"))

    def get_typed_one_shot_name(self, original_name: str) -> str:
        """Get the typed name for a one-shot function."""
        return self.one_shot_name_map.get(original_name, self._make_typed_name(original_name, "one_shot"))

    def get_typed_boolean_name(self, original_name: str) -> str:
        """Get the typed name for a boolean function."""
        return self.boolean_name_map.get(original_name, self._make_typed_name(original_name, "boolean"))
    
    def print_summary(self) -> None:
        """Print summary of function indices."""
        print("=" * 70)
        print("Function Index Builder Summary")
        print("=" * 70)
        print(f"Main functions: {self.main_indexer.get_count()}")
        print(f"One-shot functions: {self.one_shot_indexer.get_count()}")
        print(f"Boolean functions: {self.boolean_indexer.get_count()}")
        
        # Optionally show some examples of name mappings
        if self.main_name_map:
            print("\n  Sample main function mappings:")
            for original, typed in list(self.main_name_map.items())[:5]:
                print(f"    {original} -> {typed}")
        
        if self.one_shot_name_map:
            print("\n  Sample one-shot function mappings:")
            for original, typed in list(self.one_shot_name_map.items())[:5]:
                print(f"    {original} -> {typed}")
        
        if self.boolean_name_map:
            print("\n  Sample boolean function mappings:")
            for original, typed in list(self.boolean_name_map.items())[:5]:
                print(f"    {original} -> {typed}")


class LinkTableBuilder:
    """
    Stage 4: Build link tables for node relationships.
    
    Creates flat arrays of child node indices for each parent node.
    """
    
    def __init__(self, handle: ChainTreeYamlHandle, node_builder: NodeIndexBuilder):
        self.handle = handle
        self.node_builder = node_builder
        
        # Link table data
        self.link_table: List[int] = []  # Flat array of child indices
        self.node_link_info: Dict[str, Dict] = {}  # ltree_name -> {start, count}
    
    def build_link_table(self) -> None:
            """Build the link table for all nodes, skipping filtered children."""
            
            for ltree_name in self.node_builder.final_index_to_ltree:
                children = self.handle.get_node_children(ltree_name)
                
                # Filter out children that were excluded from node array
                operational_children = []
                for child_ltree in children:
                    # Skip if child was filtered as metadata node
                    if child_ltree in self.node_builder.filtered_nodes:
                        continue
                    # Skip if child doesn't have a final index (shouldn't happen, but safe)
                    if child_ltree not in self.node_builder.ltree_to_final_index:
                        continue
                    operational_children.append(child_ltree)
                
                # Record where this node's links start and how many there are
                link_start = len(self.link_table)
                link_count = len(operational_children)
                
                self.node_link_info[ltree_name] = {
                    'link_start': link_start,
                    'link_count': link_count
                }
                
                # Add child indices to link table (only operational children)
                for child_ltree in operational_children:
                    child_index = self.node_builder.get_node_final_index(child_ltree)
                    self.link_table.append(child_index)
    
    def get_node_link_info(self, ltree_name: str) -> Dict:
        """Get link information for a node."""
        return self.node_link_info.get(ltree_name, {'link_start': 0, 'link_count': 0})
    
    def get_link_table_size(self) -> int:
        """Get total size of link table."""
        return len(self.link_table)
    
    def print_summary(self) -> None:
        """Print summary of link table."""
        print("=" * 70)
        print("Link Table Builder Summary")
        print("=" * 70)
        print(f"Total link entries: {self.get_link_table_size()}")
        
        # Calculate some statistics
        max_children = max([info['link_count'] for info in self.node_link_info.values()] or [0])
        nodes_with_children = sum(1 for info in self.node_link_info.values() if info['link_count'] > 0)
        
        print(f"Nodes with children: {nodes_with_children}")
        print(f"Maximum children per node: {max_children}")


class HeaderFileGenerator:
    """
    Main pipeline orchestrator for header file generation.
    
    Coordinates all stages of the pipeline:
    1. Load YAML (via handle)
    2. Build node ordering
    3. Build function indices
    4. Build link tables
    5. Encode node data
    6. Generate C headers and implementation files
    
    All function implementations must be provided by the user with _fn suffix.
    """
    
    def __init__(
        self, 
        yaml_file: Path,
        handle_name: str,
        generate_support_header: bool = True,
    ):
        """
        Initialize HeaderFileGenerator.
        
        Args:
            yaml_file: Path to ChainTree YAML configuration
            handle_name: Name for the handle type (used in generated code)
        """
        self.yaml_file = yaml_file
        self.handle_name = handle_name
        self.generate_support_header = generate_support_header
        # Generate unique 8-character identifier for this instance
        self.unique_id = 'ct_' + ''.join(random.choices(string.ascii_lowercase + string.digits, k=8))
        
        # Stage 1: Load YAML data
        print("Stage 1: Loading YAML data...")
        print(f"  Handle name: {self.handle_name}")
        print(f"  Unique ID: {self.unique_id}")
        self.handle = ChainTreeYamlHandle(yaml_file)
        self.handle.print_summary()
        
        # Initialize other stages
        self.node_builder: Optional[NodeIndexBuilder] = None
        self.function_builder: Optional[FunctionIndexBuilder] = None
        self.data_encoder: Optional[NodeDataEncoder] = None
        self.link_builder: Optional[LinkTableBuilder] = None
        
        # Main function usage tracking
        self.main_function_usage: Dict[int, int] = {}  # function_index -> usage_count
    
    def _filter_executable_kbs(self, all_kb_names: List[str]) -> List[str]:
        """
        Filter out function mapping KBs that are not executable ChainTree KBs.
        
        Function mapping KBs include:
        - KBs ending with '_test_functions'
        - 'complete_functions_kb'
        
        These are used for function cataloging, not tree execution.
        """
        executable_kbs = []
        filtered_out = []
        
        for kb in all_kb_names:
            if kb.endswith('_test_functions') or kb == 'complete_functions_kb':
                filtered_out.append(kb)
            else:
                executable_kbs.append(kb)
        
        if filtered_out:
            print(f"\n  Note: Filtered out {len(filtered_out)} function mapping KB(s):")
            for kb in filtered_out:
                print(f"    - {kb}")
        
        return executable_kbs
    
    def _count_main_function_usage(self) -> None:
        """Count how many times each main function is used across all nodes."""
        # Initialize all counts to 0
        for i in range(self.function_builder.main_indexer.get_count()):
            self.main_function_usage[i] = 0
        
        # Count usage in all nodes
        for ltree_name in self.node_builder.final_index_to_ltree:
            functions = self.handle.get_node_functions(ltree_name)
            main_func = functions.get('main', 'CFL_NULL')
            
            if main_func and main_func != 'CFL_NULL':
                try:
                    # Convert to typed name before lookup
                    typed_name = self.function_builder.get_typed_main_name(main_func)
                    func_index = self.function_builder.main_indexer.get_index(typed_name)
                    self.main_function_usage[func_index] += 1
                except KeyError:
                    # Function not in index, skip
                    pass
    
    def run_pipeline(self) -> None:
        """Execute the complete pipeline."""
        
        # Stage 2: Build node ordering
        print("\nStage 2: Building node ordering...")
        self.node_builder = NodeIndexBuilder(self.handle)
        self.node_builder.build_node_ordering()
        self.node_builder.print_summary()
    
        # Stage 3: Build function indices (all invalid initially)
        print("\nStage 3: Building function indices...")
        self.function_builder = FunctionIndexBuilder(self.handle)
        self.function_builder.build_function_indices()
        self.function_builder.print_summary()
        
        # Stage 4: Build link tables
        print("\nStage 4: Building link tables...")
        self.link_builder = LinkTableBuilder(self.handle, self.node_builder)
        self.link_builder.build_link_table()
        self.link_builder.print_summary()
        
        # Stage 5: Encode node data with JsonRecordEncoder
        print("\nStage 5: Encoding node data...")
        self.data_encoder = NodeDataEncoder(self.handle, self.node_builder, self.function_builder)
        self.data_encoder.encode_node_data()
        self.data_encoder.print_summary()
        
        # Count main function usage
        print("\nCounting main function usage...")
        self._count_main_function_usage()
        print(f"  Total main function references: {sum(self.main_function_usage.values())}")
        
        # Stage 6: Generate headers
        print("\nStage 6: Generating C header and implementation files...")
        self.generate_headers()
        self.generate_implementations()
    
    def _generate_handle_header(self) -> None:
        """Generate the main handle structure header."""
        output_file = self.yaml_file.parent / f"{self.handle_name}.h"
        
        guard_name = f"{self.handle_name.upper()}_H"
        
        lines = [
            "/* Auto-generated by ChainTree Pipeline */",
            f"#ifndef {guard_name}",
            f"#define {guard_name}",
            "",
            "#include <stdint.h>",
            "#include <stdbool.h>",
            "",
            "#include \"chaintree_support.h\"",
            "",
            
            "typedef unsigned (*main_function_t)(void *handle, unsigned bool_function_index, unsigned node_index, unsigned event_type, unsigned event_id, void *event_data);",
            "typedef void (*one_shot_function_t)(void *handle, unsigned node_index);",
            "typedef bool (*boolean_function_t)(void *handle, unsigned node_index, unsigned event_type, unsigned event_id, void *event_data);",
            "",
        
             f'#include "{self.handle_name}_nodes.h"',
             f'#include "{self.handle_name}_links.h"',
             f'#include "{self.handle_name}_events.h"',
             f'#include "{self.handle_name}_bitmasks.h"',
             f'#include "{self.handle_name}_kb_info.h"',
             f'#include "{self.handle_name}_node_data.h"',
             f'#include "{self.handle_name}_functions.h"',
            "/* Get handle instance (const structure in flash) */",
            f"extern const chaintree_handle_t g_{self.handle_name};",
            "",
            f"#endif /* {guard_name} */"
        ]
        
        with open(output_file, 'w') as f:
            f.write("\n".join(lines))
        
        print(f"  Generated: {output_file}")

   
    def _generate_support_header(self) -> None:
        """Generate generic ChainTree support header (NO instance-specific content)."""
        output_file = self.yaml_file.parent / "chaintree_support.h"
        
        lines = [
            "/* Auto-generated by ChainTree Pipeline - Generic Runtime */",
            "#ifndef CHAINTREE_SUPPORT_H",
            "#define CHAINTREE_SUPPORT_H",
            "",
            "#include <stdint.h>",
            "#include <stdbool.h>",
            "",
            "/* ===== Function Pointer Types ===== */",
            "typedef unsigned (*main_function_t)(void *handle, unsigned bool_function_index,",
            "    unsigned node_index, unsigned event_type, unsigned event_id, void *event_data);",
            "typedef void (*one_shot_function_t)(void *handle, unsigned node_index);",
            "typedef bool (*boolean_function_t)(void *handle, unsigned node_index,",
            "    unsigned event_type, unsigned event_id, void *event_data);",
            "",
            "/* ===== Node Structure ===== */",
            "typedef struct {",
            "    uint16_t node_index;",
            "    uint16_t parent_index;",
            "    uint16_t depth;                /* Tree depth (pairs: 0=root, 1=children, 2=grandchildren, etc.) */",
            "    uint16_t link_start;",
            "    uint16_t link_count;          /* Bits 0-14: count, Bit 15: auto_start flag */",
            "    uint16_t main_function_index;",
            "    uint16_t init_function_index;",
            "    uint16_t aux_function_index;",
            "    uint16_t term_function_index;",
            "    uint16_t node_data_id;",
            "} chaintree_node_t;",
            "",
            "/* ===== Link Count Bit Packing Macros ===== */",
            "#define LINK_COUNT_MASK     0x7FFF  /* Bits 0-14: max 32767 children */",
            "#define AUTO_START_BIT      0x8000  /* Bit 15: auto_start flag */",
            "",
            "#define GET_LINK_COUNT(node)     ((node)->link_count & LINK_COUNT_MASK)",
            "#define GET_AUTO_START(node)     (((node)->link_count & AUTO_START_BIT) != 0)",
            "#define PACK_LINK_COUNT(count, auto_start) \\",
            "    (((count) & LINK_COUNT_MASK) | ((auto_start) ? AUTO_START_BIT : 0))",
            "",
            "/* ===== Knowledge Base Info ===== */",
            "typedef struct {",
            "    const char *kb_name;",
            "    uint16_t root_node_index;",
            "    uint16_t start_index;",
            "    uint16_t node_count;",
            "    uint16_t max_depth;           /* Maximum tree depth in this KB */",
            "} chaintree_kb_info_t;",
            "",
            "/* ===== Node Data Structures ===== */",
            "typedef enum {",
            "    JSON_TYPE_STRING = 0,",
            "    JSON_TYPE_INT32 = 1,",
            "    JSON_TYPE_FLOAT32 = 2,",
            "    JSON_TYPE_NULL = 3,",
            "    JSON_TYPE_BOOL = 4,",
            "    JSON_TYPE_ARRAY = 5,",
            "    JSON_TYPE_OBJECT = 6",
            "} json_type_t;",
            "",
            "typedef struct {",
            "    json_type_t object_type;",
            "    union {",
            "        uint32_t string_offset;",
            "        int32_t i32_value;",
            "        float f32_value;",
            "        uint8_t bool_value;",
            "        uint32_t container_count;",
            "    } value;",
            "} json_record_t;",
            "",
            "typedef struct {",
            "    uint32_t start_position;",
            "    uint32_t num_records;",
            "} record_control_t;",
            "",
            "/* ===== Handle Structure ===== */",
            "typedef struct {",
            "    const char *unique_id;",
            "    const chaintree_node_t *nodes;",
            "    uint16_t node_count;",
            "    const main_function_t *main_functions;",
            "    uint16_t main_function_count;",
            "    const one_shot_function_t *one_shot_functions;",
            "    uint16_t one_shot_function_count;",
            "    const boolean_function_t *boolean_functions;",
            "    uint16_t boolean_function_count;",
            "    const char **main_function_names;",
            "    const char **one_shot_function_names;",
            "    const char **boolean_function_names;",
            "    const uint16_t *main_function_usage_count;",
            "    const uint16_t *link_table;",
            "    uint16_t link_table_size;",
            "    const char **event_strings;",
            "    uint16_t event_count;",
            "    const char **bitmask_names;",
            "    uint16_t bitmask_count;",
            "    const chaintree_kb_info_t *kb_table;",
            "    uint16_t kb_count;",
            "    const json_record_t *node_data_records;",
            "    uint16_t node_data_records_count;",
            "    const char *node_data_strings;",
            "    uint16_t node_data_strings_size;",
            "    const record_control_t *node_data_controls;",
            "    uint16_t node_data_controls_count;",
            "} chaintree_handle_t;",
            "",
            "/* ===== Generic Lookup Functions ===== */",
            "const char* ct_get_main_function_name(const chaintree_handle_t *handle, uint16_t func_index);",
            "int ct_get_main_function_index(const chaintree_handle_t *handle, const char *func_name);",
            "const char* ct_get_one_shot_function_name(const chaintree_handle_t *handle, uint16_t func_index);",
            "int ct_get_one_shot_function_index(const chaintree_handle_t *handle, const char *func_name);",
            "const char* ct_get_boolean_function_name(const chaintree_handle_t *handle, uint16_t func_index);",
            "int ct_get_boolean_function_index(const chaintree_handle_t *handle, const char *func_name);",
            "const char* ct_get_event_name(const chaintree_handle_t *handle, uint16_t event_index);",
            "int ct_get_event_index(const chaintree_handle_t *handle, const char *name);",
            "const char* ct_get_bitmask_name(const chaintree_handle_t *handle, uint8_t bit_index);",
            "int ct_get_bitmask_index(const chaintree_handle_t *handle, const char *name);",
            "uint16_t ct_get_kb_count(const chaintree_handle_t *handle);",
            "",
            "#endif /* CHAINTREE_SUPPORT_H */"
        ]
        
        with open(output_file, 'w') as f:
            f.write("\n".join(lines))
        
        print(f"  Generated: {output_file}")
        
    def _generate_instance_header(self) -> None:
        """Generate instance-specific top-level header."""
        output_file = self.yaml_file.parent / f"{self.handle_name}.h"  # Use handle_name, not unique_id
        
        guard_name = f"{self.handle_name.upper()}_H"  # Use handle_name for guard too
        
        lines = [
            "/* Auto-generated by ChainTree Pipeline */",
            f"#ifndef {guard_name}",
            f"#define {guard_name}",
            "",
            '#include "chaintree_support.h"',
            f'#include "{self.handle_name}_nodes.h"',
            f'#include "{self.handle_name}_links.h"',
            f'#include "{self.handle_name}_events.h"',
            f'#include "{self.handle_name}_bitmasks.h"',
            f'#include "{self.handle_name}_kb_info.h"',
            f'#include "{self.handle_name}_node_data.h"',
            f'#include "{self.handle_name}_functions.h"',
            "",
            "/* The actual handle instance */",
            f"extern const chaintree_handle_t g_{self.handle_name};",
            "",
            f"#endif /* {guard_name} */"
        ]
        
        with open(output_file, 'w') as f:
            f.write("\n".join(lines))
        
        print(f"  Generated: {output_file}")
            
    def _generate_support_implementation(self) -> None:
        """Generate generic ChainTree support functions implementation."""
        output_file = self.yaml_file.parent / "chaintree_support.c"
        
        lines = [
            "/* Auto-generated by ChainTree Pipeline - Generic Support Functions */",
            '#include "chaintree_support.h"',
            "#include <string.h>",
            "",
            "/* Get main function name by index */",
            "const char* ct_get_main_function_name(const chaintree_handle_t *handle, uint16_t func_index) {",
            "    if (handle == NULL || handle->main_function_names == NULL) {",
            "        return NULL;",
            "    }",
            "    if (func_index >= handle->main_function_count) {",
            "        return NULL;",
            "    }",
            "    return handle->main_function_names[func_index];",
            "}",
            "",
            "/* Get main function index by name */",
            "int ct_get_main_function_index(const chaintree_handle_t *handle, const char *func_name) {",
            "    if (handle == NULL || func_name == NULL || handle->main_function_names == NULL) {",
            "        return -1;",
            "    }",
            "    for (uint16_t i = 0; i < handle->main_function_count; i++) {",
            "        if (strcmp(func_name, handle->main_function_names[i]) == 0) {",
            "            return i;",
            "        }",
            "    }",
            "    return -1;",
            "}",
            "",
            "/* Get one-shot function name by index */",
            "const char* ct_get_one_shot_function_name(const chaintree_handle_t *handle, uint16_t func_index) {",
            "    if (handle == NULL || handle->one_shot_function_names == NULL) {",
            "        return NULL;",
            "    }",
            "    if (func_index >= handle->one_shot_function_count) {",
            "        return NULL;",
            "    }",
            "    return handle->one_shot_function_names[func_index];",
            "}",
            "",
            "/* Get one-shot function index by name */",
            "int ct_get_one_shot_function_index(const chaintree_handle_t *handle, const char *func_name) {",
            "    if (handle == NULL || func_name == NULL || handle->one_shot_function_names == NULL) {",
            "        return -1;",
            "    }",
            "    for (uint16_t i = 0; i < handle->one_shot_function_count; i++) {",
            "        if (strcmp(func_name, handle->one_shot_function_names[i]) == 0) {",
            "            return i;",
            "        }",
            "    }",
            "    return -1;",
            "}",
            "",
            "/* Get boolean function name by index */",
            "const char* ct_get_boolean_function_name(const chaintree_handle_t *handle, uint16_t func_index) {",
            "    if (handle == NULL || handle->boolean_function_names == NULL) {",
            "        return NULL;",
            "    }",
            "    if (func_index >= handle->boolean_function_count) {",
            "        return NULL;",
            "    }",
            "    return handle->boolean_function_names[func_index];",
            "}",
            "",
            "/* Get boolean function index by name */",
            "int ct_get_boolean_function_index(const chaintree_handle_t *handle, const char *func_name) {",
            "    if (handle == NULL || func_name == NULL || handle->boolean_function_names == NULL) {",
            "        return -1;",
            "    }",
            "    for (uint16_t i = 0; i < handle->boolean_function_count; i++) {",
            "        if (strcmp(func_name, handle->boolean_function_names[i]) == 0) {",
            "            return i;",
            "        }",
            "    }",
            "    return -1;",
            "}",
            "",
            "/* Get event name by index */",
            "const char* ct_get_event_name(const chaintree_handle_t *handle, uint16_t event_index) {",
            "    if (handle == NULL || handle->event_strings == NULL) {",
            "        return NULL;",
            "    }",
            "    if (event_index >= handle->event_count) {",
            "        return NULL;",
            "    }",
            "    return handle->event_strings[event_index];",
            "}",
            "",
            "/* Get event index by name */",
            "int ct_get_event_index(const chaintree_handle_t *handle, const char *name) {",
            "    if (handle == NULL || name == NULL || handle->event_strings == NULL) {",
            "        return -1;",
            "    }",
            "    for (uint16_t i = 0; i < handle->event_count; i++) {",
            "        if (strcmp(name, handle->event_strings[i]) == 0) {",
            "            return i;",
            "        }",
            "    }",
            "    return -1;",
            "}",
            "",
            "/* Get bitmask name by bit index */",
            "const char* ct_get_bitmask_name(const chaintree_handle_t *handle, uint8_t bit_index) {",
            "    if (handle == NULL || handle->bitmask_names == NULL) {",
            "        return NULL;",
            "    }",
            "    if (bit_index >= handle->bitmask_count) {",
            "        return NULL;",
            "    }",
            "    return handle->bitmask_names[bit_index];",
            "}",
            "",
            "/* Get bitmask bit index by name */",
            "int ct_get_bitmask_index(const chaintree_handle_t *handle, const char *name) {",
            "    if (handle == NULL || name == NULL || handle->bitmask_names == NULL) {",
            "        return -1;",
            "    }",
            "    for (uint8_t i = 0; i < handle->bitmask_count; i++) {",
            "        if (strcmp(name, handle->bitmask_names[i]) == 0) {",
            "            return i;",
            "        }",
            "    }",
            "    return -1;",
            "}",
            "",
            "/* Get total number of knowledge bases */",
            "uint16_t ct_get_kb_count(const chaintree_handle_t *handle) {",
            "    if (handle == NULL) {",
            "        return 0;",
            "    }",
            "    return handle->kb_count;",
            "}",
            ""
        ]
        
        with open(output_file, 'w') as f:
            f.write("\n".join(lines))
        
        print(f"  Generated: {output_file}")
    
    def _generate_handle_implementation(self) -> None:
        """Generate handle implementation with const initialized structure."""
        output_file = self.yaml_file.parent / f"{self.handle_name}.c"
        
        # Get the counts from data encoder FIRST
        if self.data_encoder:
            records_count = self.data_encoder.get_records_count()
            strings_size = self.data_encoder.get_strings_size()
            controls_count = self.data_encoder.get_controls_count()
        else:
            records_count = 0
            strings_size = 0
            controls_count = 0
        
        lines = [
            "/* Auto-generated by ChainTree Pipeline */",
            "",
            f'#include "{self.handle_name}.h"',
            "",
            f"/* Const handle instance in flash memory */",
            f"const chaintree_handle_t g_{self.handle_name} = {{",
            f'    .unique_id = "{self.unique_id}",',
            "    ",
            "    /* Node data */",
            f"    .nodes = {self.unique_id}_nodes,",
            f"    .node_count = {self.node_builder.get_total_nodes()},",
            "    ",
            "    /* Function arrays */",
            f"    .main_functions = {self.unique_id}_main_functions,",
            f"    .main_function_count = {self.function_builder.main_indexer.get_count()},",
            f"    .one_shot_functions = {self.unique_id}_one_shot_functions,",
            f"    .one_shot_function_count = {self.function_builder.one_shot_indexer.get_count()},",
            f"    .boolean_functions = {self.unique_id}_boolean_functions,",
            f"    .boolean_function_count = {self.function_builder.boolean_indexer.get_count()},",
            "    ",
            "    /* Function names */",
            f"    .main_function_names = {self.unique_id}_main_function_names,",
            f"    .one_shot_function_names = {self.unique_id}_one_shot_function_names,",
            f"    .boolean_function_names = {self.unique_id}_boolean_function_names,",
            "    ",
            "    /* Main function usage counts */",
            f"    .main_function_usage_count = {self.unique_id}_main_function_usage_count,",
            "    ",
            "    /* Link table */",
            f"    .link_table = {self.unique_id}_link_table,",
            f"    .link_table_size = {self.link_builder.get_link_table_size()},",
            "    ",
            "    /* Event strings */",
            f"    .event_strings = (const char **){self.unique_id}_event_strings,",
            f"    .event_count = {len(self.handle.get_event_string_table())},",
            "    ",
            "    /* Bitmask names */",
            f"    .bitmask_names = (const char **){self.unique_id}_bitmask_names,",
            f"    .bitmask_count = {len(self.handle.get_bitmask_table())},",
            "    ",
            "    /* Knowledge base info */",
            f"    .kb_table = {self.unique_id}_kb_table,",
            f"    .kb_count = {len(self._filter_executable_kbs(self.handle.get_kb_names()))},",
            "    ",
            "    /* Node data (JSON records) */",
        ]
        
        # Add node data fields with unique_id prefix
        if self.data_encoder and records_count > 0:
            lines.extend([
                f"    .node_data_records = {self.unique_id}_node_data_records,",
                f"    .node_data_records_count = {records_count},",
                f"    .node_data_strings = {self.unique_id}_node_data_strings,",
                f"    .node_data_strings_size = {strings_size},",
                f"    .node_data_controls = {self.unique_id}_node_data_controls,",
                f"    .node_data_controls_count = {controls_count}",
            ])
        else:
            lines.extend([
                "    .node_data_records = NULL,",
                "    .node_data_records_count = 0,",
                "    .node_data_strings = NULL,",
                "    .node_data_strings_size = 0,",
                "    .node_data_controls = NULL,",
                "    .node_data_controls_count = 0",
            ])
        
        lines.extend([
            "};",
            "",
        ])
        
        with open(output_file, 'w') as f:
            f.write("\n".join(lines))
        
        print(f"  Generated: {output_file}")
            
    def generate_headers(self) -> None:
        """Generate C header files from pipeline data."""
        
        # Generate handle structure header (Step 2)
        ##self._generate_handle_header()
        print("Support ",self.generate_support_header)
        
        if self.generate_support_header:
            print("Generating support header")
            self._generate_support_header()
        
        # Generate node structure header
        self._generate_node_struct_header()
        
        # Generate function table headers
        self._generate_function_headers()
        
        # Generate link table header
        self._generate_link_table_header()
        
        # Generate node data header (typedefs only)
        if self.data_encoder:
            self._generate_node_data_header()
        
        # Always generate event header (even if empty)
        self._generate_event_table_header()
        
        # Always generate bitmask header (even if empty)
        self._generate_bitmask_table_header()
        
        # Generate knowledge base info header
        self._generate_kb_info_header()
        
          # Generate instance top-level header (NEW)
        self._generate_instance_header()

    def generate_implementations(self) -> None:
        """Generate C implementation files."""
        
        # Generate handle implementation (Step 2 & 5)
        self._generate_handle_implementation()
        
        # Generate support functions implementation (Step 4)
        if self.generate_support_header:
            self._generate_support_implementation()
        
        # Generate function implementations
        self._generate_function_implementations()
        
        # Generate node array implementation
        self._generate_node_array_implementation()
        
        # Generate link table implementation
        self._generate_link_table_implementation()
        
        # Generate node data implementation (actual arrays)
        if self.data_encoder:
            self._generate_node_data_implementation()
        
        # Always generate event implementation (even if empty)
        self._generate_event_table_implementation()
        
        # Always generate bitmask implementation (even if empty)
        self._generate_bitmask_table_implementation()
        
        # Generate knowledge base info implementation
        self._generate_kb_info_implementation()
        
    def _generate_node_struct_header(self) -> None:
        """Generate the node structure header."""
        output_file = self.yaml_file.parent / f"{self.handle_name}_nodes.h"
        
        guard_name = f"{self.handle_name.upper()}_NODES_H"
        
        lines = [
            "/* Auto-generated by ChainTree Pipeline */",
            f"#ifndef {guard_name}",
            f"#define {guard_name}",
            "",
            '#include "chaintree_support.h"',
            "",
            f"#define {self.unique_id.upper()}_NODE_COUNT {self.node_builder.get_total_nodes()}",
            "",
            f"extern const chaintree_node_t {self.unique_id}_nodes[{self.unique_id.upper()}_NODE_COUNT];",
            "",
            f"#endif /* {guard_name} */"
        ]
        
        with open(output_file, 'w') as f:
            f.write("\n".join(lines))
        
        print(f"  Generated: {output_file}")
    
    def _generate_function_headers(self) -> None:
        """Generate function table headers with unique prefixes."""
        output_file = self.yaml_file.parent / f"{self.handle_name}_functions.h"
        
        guard_name = f"{self.handle_name.upper()}_FUNCTIONS_H"
        
        lines = [
            "/* Auto-generated by ChainTree Pipeline */",
            f"#ifndef {guard_name}",
            f"#define {guard_name}",
            "",
            "#include <stdint.h>",
            "#include <stdbool.h>",
            "#include <string.h>",
            '#include "chaintree_support.h"',
            "",
            "/* Main function enum */",
            self.function_builder.main_indexer.generate_c_enum("MAIN_FUNC"),
            "",
            "/* One-shot function enum */",
            self.function_builder.one_shot_indexer.generate_c_enum("ONE_SHOT_FUNC"),
            "",
            "/* Boolean function enum */",
            self.function_builder.boolean_indexer.generate_c_enum("BOOL_FUNC"),
            "",
            "/* Function pointer arrays */",
            f"extern const main_function_t {self.unique_id}_main_functions[{self.function_builder.main_indexer.get_count()}];",
            f"extern const one_shot_function_t {self.unique_id}_one_shot_functions[{self.function_builder.one_shot_indexer.get_count()}];",
            f"extern const boolean_function_t {self.unique_id}_boolean_functions[{self.function_builder.boolean_indexer.get_count()}];",
            "",
            "/* Main function usage count (indexed by function enum) */",
            f"extern const uint16_t {self.unique_id}_main_function_usage_count[{self.function_builder.main_indexer.get_count()}];",
            "",
            "/* Function name arrays (for debugging) */",
            f"extern const char *{self.unique_id}_main_function_names[{self.function_builder.main_indexer.get_count()}];",
            f"extern const char *{self.unique_id}_one_shot_function_names[{self.function_builder.one_shot_indexer.get_count()}];",
            f"extern const char *{self.unique_id}_boolean_function_names[{self.function_builder.boolean_indexer.get_count()}];",
            "",
            f"#endif /* {guard_name} */"
        ]
        
        with open(output_file, 'w') as f:
            f.write("\n".join(lines))
        
        print(f"  Generated: {output_file}")
    
    def _generate_link_table_header(self) -> None:
        """Generate link table header."""
        output_file = self.yaml_file.parent / f"{self.handle_name}_links.h"
        
        guard_name = f"{self.handle_name.upper()}_LINKS_H"
        
        lines = [
            "/* Auto-generated by ChainTree Pipeline */",
            f"#ifndef {guard_name}",
            f"#define {guard_name}",
            "",
            "#include <stdint.h>",
            "",
            f"#define {self.unique_id.upper()}_LINK_TABLE_SIZE {self.link_builder.get_link_table_size()}",
            "",
            f"extern const uint16_t {self.unique_id}_link_table[{self.unique_id.upper()}_LINK_TABLE_SIZE];",
            "",
            f"#endif /* {guard_name} */"
        ]
        
        with open(output_file, 'w') as f:
            f.write("\n".join(lines))
        
        print(f"  Generated: {output_file}")
    
    def _generate_event_table_header(self) -> None:
        """Generate event string table header with name array."""
        output_file = self.yaml_file.parent / f"{self.handle_name}_events.h"
        
        guard_name = f"{self.handle_name.upper()}_EVENTS_H"
        events = self.handle.get_event_string_table()
        
        lines = [
            "/* Auto-generated by ChainTree Pipeline */",
            f"#ifndef {guard_name}",
            f"#define {guard_name}",
            "",
            "#include <stdint.h>",
            "#include <stdbool.h>",
            "#include <string.h>",
            "",
        ]
        
        if events:
            lines.extend([
                f"#define {self.unique_id.upper()}_EVENT_STRING_COUNT {len(events)}",
                "",
                "/* Event indices */",
                "typedef enum {"
            ])
            
            for event_id, index in sorted(events.items(), key=lambda x: x[1]):
                safe_name = event_id.upper().replace(" ", "_").replace("-", "_")
                lines.append(f"    EVENT_{safe_name} = {index},")
            
            lines.extend([
                f"    EVENT_COUNT = {len(events)}",
                "} event_index_t;",
                "",
                "/* Event name array for debugging/lookup */",
                f"extern const char *{self.unique_id}_event_strings[{len(events)}];",
                ""
            ])
        else:
            lines.extend([
                "/* No events defined */",
                f"#define {self.unique_id.upper()}_EVENT_STRING_COUNT 0",
                "#define EVENT_COUNT 0",
                "",
                "/* Empty array */",
                f"extern const char *{self.unique_id}_event_strings[1];",
                ""
            ])
        
        lines.append(f"#endif /* {guard_name} */")
        
        with open(output_file, 'w') as f:
            f.write("\n".join(lines))
        
        print(f"  Generated: {output_file}")
    
    def _generate_event_table_implementation(self) -> None:
        """Generate event table implementation with name array."""
        output_file = self.yaml_file.parent / f"{self.handle_name}_events.c"
        
        events = self.handle.get_event_string_table()
        
        lines = [
            "/* Auto-generated by ChainTree Pipeline */",
            f'#include "{self.handle_name}_events.h"',
            "",
        ]
        
        if events:
            # Generate name array
            lines.extend([
                "/* Event names indexed by event index */",
                f"const char *{self.unique_id}_event_strings[{len(events)}] = {{"
            ])
            
            for event_id, index in sorted(events.items(), key=lambda x: x[1]):
                lines.append(f'    "{event_id}",')
            
            lines.extend([
                "};",
                ""
            ])
        else:
            lines.extend([
                "/* No events defined - empty array */",
                f'const char *{self.unique_id}_event_strings[1] = {{ NULL }};',
                ""
            ])
        
        with open(output_file, 'w') as f:
            f.write("\n".join(lines))
        
        print(f"  Generated: {output_file}")
    
    def _generate_bitmask_table_header(self) -> None:
        """Generate bitmask table header with name array."""
        output_file = self.yaml_file.parent / f"{self.handle_name}_bitmasks.h"
        
        guard_name = f"{self.handle_name.upper()}_BITMASKS_H"
        bitmasks = self.handle.get_bitmask_table()
        
        lines = [
            "/* Auto-generated by ChainTree Pipeline */",
            f"#ifndef {guard_name}",
            f"#define {guard_name}",
            "",
            "#include <stdint.h>",
            "#include <stdbool.h>",
            "#include <string.h>",
            "",
        ]
        
        if bitmasks:
            lines.extend([
                "/* Bitmask bit positions */"
            ])
            
            for event_name, bit_num in sorted(bitmasks.items(), key=lambda x: x[1]):
                safe_name = event_name.upper().replace(" ", "_").replace("-", "_")
                mask_val = 1 << bit_num
                lines.append(f"#define BIT_{safe_name:30s} {bit_num:2d}  /* 0x{mask_val:08X} */")
            
            lines.extend([
                "",
                "/* Bitmask values */",
            ])
            
            for event_name, bit_num in sorted(bitmasks.items(), key=lambda x: x[1]):
                safe_name = event_name.upper().replace(" ", "_").replace("-", "_")
                lines.append(f"#define MASK_{safe_name:29s} (1U << BIT_{safe_name})")
            
            lines.extend([
                "",
                f"#define {self.unique_id.upper()}_BITMASK_COUNT {len(bitmasks)}",
                "",
                "/* Bitmask name array for debugging/lookup */",
                f"extern const char *{self.unique_id}_bitmask_names[{len(bitmasks)}];",
                ""
            ])
        else:
            lines.extend([
                "/* No bitmasks defined */",
                f"#define {self.unique_id.upper()}_BITMASK_COUNT 0",
                "",
                "/* Empty array */",
                f"extern const char *{self.unique_id}_bitmask_names[1];",
                ""
            ])
        
        lines.append(f"#endif /* {guard_name} */")
        
        with open(output_file, 'w') as f:
            f.write("\n".join(lines))
        
        print(f"  Generated: {output_file}")
    
    def _generate_node_data_header(self) -> None:
        """Generate node data header with extern declarations only."""
        output_file = self.yaml_file.parent / f"{self.handle_name}_node_data.h"
        
        guard_name = f"{self.handle_name.upper()}_NODE_DATA_H"
        
        lines = [
            "/* Auto-generated by ChainTree Pipeline */",
            f"#ifndef {guard_name}",
            f"#define {guard_name}",
            "",
            '#include "chaintree_support.h"',
            "",
        ]
        
        # Add extern declarations with unique_id prefix
        if self.data_encoder:
            records_count = self.data_encoder.get_records_count()
            strings_size = self.data_encoder.get_strings_size()
            controls_count = self.data_encoder.get_controls_count()
            
            if records_count > 0:
                lines.extend([
                    "/* Node data arrays (defined in .c file) */",
                    f"extern const json_record_t {self.unique_id}_node_data_records[{records_count}];",
                    f"extern const char {self.unique_id}_node_data_strings[{strings_size}];",
                    f"extern const record_control_t {self.unique_id}_node_data_controls[{controls_count}];",
                    ""
                ])
        
        lines.extend([
            f"#endif /* {guard_name} */",
            ""
        ])
        
        with open(output_file, 'w') as f:
            f.write("\n".join(lines))
        
        print(f"  Generated: {output_file}")
        
    def _generate_node_data_implementation(self) -> None:
        """Generate node data implementation with actual data arrays."""
        output_file = self.yaml_file.parent / f"{self.handle_name}_node_data.c"
        
        if not self.data_encoder or self.data_encoder.get_records_count() == 0:
            # Generate empty file if no data
            lines = [
                "/* Auto-generated by ChainTree Pipeline */",
                f'#include "{self.handle_name}_node_data.h"',
                "",
                "/* No node data */",
                ""
            ]
            with open(output_file, 'w') as f:
                f.write("\n".join(lines))
            print(f"  Generated: {output_file} (empty)")
            return
        
        lines = [
            "/* Auto-generated by ChainTree Pipeline */",
            f'#include "{self.handle_name}_node_data.h"',
            "",
        ]
        
        # Generate the actual data arrays with unique_id prefix
        self.data_encoder.generate_c_arrays(lines, self.unique_id)
        
        with open(output_file, 'w') as f:
            f.write("\n".join(lines))
        
        print(f"  Generated: {output_file}")
        
    def _generate_function_implementations(self) -> None:
        """Generate function implementation .c file with typed names."""
        output_file = self.yaml_file.parent / f"{self.handle_name}_functions.c"
        
        lines = [
            "/* Auto-generated by ChainTree Pipeline */",
            f'#include "{self.handle_name}_functions.h"',
            "",
            "/* Function implementations */",
            "",
            "/* Placeholder for extracted function implementations */",
            "/* Functions should be linked from their source files */",
            "",
            "/* Function pointer arrays */",
            ""
        ]
        
        # Generate forward declarations using TYPED names
        lines.append("/* Forward declarations - user must provide these functions */")
        for original_name in self.function_builder.main_indexer.get_all_functions():
            typed_name = self.function_builder.get_typed_main_name(original_name)
            fn_name = typed_name + "_fn"
            lines.append(f"extern unsigned {fn_name}(void *handle, unsigned bool_function_index, unsigned node_index, unsigned event_type, unsigned event_id, void *event_data);")
        lines.append("")
        
        for original_name in self.function_builder.one_shot_indexer.get_all_functions():
            typed_name = self.function_builder.get_typed_one_shot_name(original_name)
            fn_name = typed_name + "_fn"
            lines.append(f"extern void {fn_name}(void *handle, unsigned node_index);")
        lines.append("")
        
        for original_name in self.function_builder.boolean_indexer.get_all_functions():
            typed_name = self.function_builder.get_typed_boolean_name(original_name)
            fn_name = typed_name + "_fn"
            lines.append(f"extern bool {fn_name}(void *handle, unsigned node_index, unsigned event_type, unsigned event_id, void *event_data);")
        lines.append("")
        
        # Generate function pointer arrays using TYPED names
        lines.append(f"const main_function_t {self.unique_id}_main_functions[] = {{")
        for original_name in self.function_builder.main_indexer.get_all_functions():
            typed_name = self.function_builder.get_typed_main_name(original_name)
            fn_name = typed_name + "_fn"
            lines.append(f"    {fn_name},")
        lines.append("};")
        lines.append("")
        
        lines.append(f"const one_shot_function_t {self.unique_id}_one_shot_functions[] = {{")
        for original_name in self.function_builder.one_shot_indexer.get_all_functions():
            typed_name = self.function_builder.get_typed_one_shot_name(original_name)
            fn_name = typed_name + "_fn"
            lines.append(f"    {fn_name},")
        lines.append("};")
        lines.append("")
        
        lines.append(f"const boolean_function_t {self.unique_id}_boolean_functions[] = {{")
        for original_name in self.function_builder.boolean_indexer.get_all_functions():
            typed_name = self.function_builder.get_typed_boolean_name(original_name)
            fn_name = typed_name + "_fn"
            lines.append(f"    {fn_name},")
        lines.append("};")
        lines.append("")
        
        # Generate main function usage count array
        lines.append("/* Main function usage count */")
        lines.append(f"const uint16_t {self.unique_id}_main_function_usage_count[{self.function_builder.main_indexer.get_count()}] = {{")
        for i, original_name in enumerate(self.function_builder.main_indexer.get_all_functions()):
            usage = self.main_function_usage.get(i, 0)
            lines.append(f"    {usage},  /* {original_name} */")
        lines.append("};")
        lines.append("")
        
        # Generate name arrays using ORIGINAL names
        lines.append(self.function_builder.main_indexer.generate_c_string_array(f"{self.unique_id}_main_function_names"))
        lines.append("")
        lines.append(self.function_builder.one_shot_indexer.generate_c_string_array(f"{self.unique_id}_one_shot_function_names"))
        lines.append("")
        lines.append(self.function_builder.boolean_indexer.generate_c_string_array(f"{self.unique_id}_boolean_function_names"))
        lines.append("")
        
        with open(output_file, 'w') as f:
            f.write("\n".join(lines))
        
        print(f"  Generated: {output_file}")    
        
    def _generate_node_array_implementation(self) -> None:
            """Generate node array implementation with actual data using typed names."""
            output_file = self.yaml_file.parent / f"{self.handle_name}_nodes.c"
            
            lines = [
                "/* Auto-generated by ChainTree Pipeline */",
                f'#include "{self.handle_name}_nodes.h"',
                "",
                "/* Node array with complete initialization */",
                f"const chaintree_node_t {self.unique_id}_nodes[{self.node_builder.get_total_nodes()}] = {{"
            ]
            
            # Generate each node entry
            for i, ltree_name in enumerate(self.node_builder.final_index_to_ltree):
                node_data = self.handle.get_node_data(ltree_name)
                
                if not node_data:
                    lines.append(f"    /* [{i}] ERROR: Node not found: {ltree_name} */")
                    continue
                
                # Get node functions
                functions = self.handle.get_node_functions(ltree_name)
                
                # Convert to typed names and get function indices
                typed_main = self.function_builder.get_typed_main_name(functions['main'])
                typed_init = self.function_builder.get_typed_one_shot_name(functions['init'])
                typed_aux = self.function_builder.get_typed_boolean_name(functions['aux'])
                typed_term = self.function_builder.get_typed_one_shot_name(functions['term'])
                
                main_idx = self.function_builder.main_indexer.get_index(functions['main'])
                init_idx = self.function_builder.one_shot_indexer.get_index(functions['init'])
                aux_idx = self.function_builder.boolean_indexer.get_index(functions['aux'])
                term_idx = self.function_builder.one_shot_indexer.get_index(functions['term'])
                
                # Get link info
                link_info = self.link_builder.get_node_link_info(ltree_name)
                link_start = link_info['link_start']
                link_count = link_info['link_count']
                
                # Get auto_start flag from node_dict
                node_dict = node_data.get('node_dict', {})
                auto_start = node_dict.get('auto_start', False)
                
                # Pack link_count with auto_start in bit 15
                packed_link_count = link_count & 0x7FFF
                if auto_start:
                    packed_link_count |= 0x8000
                
                # Get parent index - UPDATED LOGIC HERE
                parent_ltree = self.handle.get_node_parent(ltree_name)
                
                # Check if parent exists and is operational (not filtered)
                if parent_ltree and parent_ltree in self.node_builder.ltree_to_final_index:
                    parent_idx = self.node_builder.get_node_final_index(parent_ltree)
                else:
                    parent_idx = 0xFFFF  # No parent or parent was filtered
                
                # Get data ID
                data_id = self.data_encoder.get_node_data_id(ltree_name)
                
                # Generate node entry with comment showing auto_start status
                node_name = node_data.get('node_name', 'unknown')
                auto_start_comment = " [AUTO_START]" if auto_start else ""
                lines.append(f"    /* [{i}] {node_name}{auto_start_comment} */")
                lines.append(f"    {{")
                lines.append(f"        .node_index = {i},")
                lines.append(f"        .parent_index = {parent_idx},")
                lines.append(f"        .link_start = {link_start},")
                lines.append(f"        .link_count = 0x{packed_link_count:04X},  /* count={link_count}, auto_start={auto_start} */")
                lines.append(f"        .main_function_index = {main_idx},")
                lines.append(f"        .init_function_index = {init_idx},")
                lines.append(f"        .aux_function_index = {aux_idx},")
                lines.append(f"        .term_function_index = {term_idx},")
                lines.append(f"        .node_data_id = {data_id}")
                
                if i < self.node_builder.get_total_nodes() - 1:
                    lines.append(f"    }},")
                else:
                    lines.append(f"    }}")
            
            lines.append("};")
            lines.append("")
            
            with open(output_file, 'w') as f:
                f.write("\n".join(lines))
            
            print(f"  Generated: {output_file}")
        
    def _generate_link_table_implementation(self) -> None:
        """Generate link table implementation."""
        output_file = self.yaml_file.parent / f"{self.handle_name}_links.c"
        
        lines = [
            "/* Auto-generated by ChainTree Pipeline */",
            f'#include "{self.handle_name}_links.h"',
            "",
            "/* Link table - flat array of child node indices */",
            f"const uint16_t {self.unique_id}_link_table[{self.link_builder.get_link_table_size()}] = {{"
        ]
        
        # Generate link table entries
        if self.link_builder.link_table:
            line = "    "
            for i, child_index in enumerate(self.link_builder.link_table):
                is_last = (i == len(self.link_builder.link_table) - 1)
                is_line_start = (i % 10 == 0)
                
                # Start new line if needed (but not on first element)
                if is_line_start and i > 0:
                    lines.append(line + ",")  # Add comma to previous line
                    line = "    "
                
                # Add element
                if is_line_start:
                    line += f"{child_index}"
                else:
                    line += f", {child_index}"
            
            # Append the last line (no trailing comma)
            lines.append(line)
        
        lines.append("};")
        lines.append("")
        
        with open(output_file, 'w') as f:
            f.write("\n".join(lines))
        
        print(f"  Generated: {output_file}")
    
    def _generate_bitmask_table_implementation(self) -> None:
        """Generate bitmask table implementation with name array."""
        output_file = self.yaml_file.parent / f"{self.handle_name}_bitmasks.c"
        
        bitmasks = self.handle.get_bitmask_table()
        
        lines = [
            "/* Auto-generated by ChainTree Pipeline */",
            f'#include "{self.handle_name}_bitmasks.h"',
            "",
        ]
        
        if bitmasks:
            # Generate name array
            lines.extend([
                "/* Bitmask names indexed by bit position */",
                f"const char *{self.unique_id}_bitmask_names[{len(bitmasks)}] = {{"
            ])
            
            for event_name, bit_num in sorted(bitmasks.items(), key=lambda x: x[1]):
                lines.append(f'    "{event_name}",')
            
            lines.extend([
                "};",
                ""
            ])
        else:
            lines.extend([
                "/* No bitmasks defined - empty array */",
                f'const char *{self.unique_id}_bitmask_names[1] = {{ NULL }};',
                ""
            ])
        
        with open(output_file, 'w') as f:
            f.write("\n".join(lines))
        
        print(f"  Generated: {output_file}")
    
    def _generate_kb_info_header(self) -> None:
        """Generate knowledge base info header."""
        output_file = self.yaml_file.parent / f"{self.handle_name}_kb_info.h"
        
        guard_name = f"{self.handle_name.upper()}_KB_INFO_H"
        
        # Filter out function mapping KBs (not executable ChainTree KBs)
        all_kb_names = self.handle.get_kb_names()
        kb_names = self._filter_executable_kbs(all_kb_names)
        
        lines = [
            "/* Auto-generated by ChainTree Pipeline */",
            f"#ifndef {guard_name}",
            f"#define {guard_name}",
            "",
            '#include "chaintree_support.h"',
            "",
            f"#define {self.unique_id.upper()}_KB_COUNT {len(kb_names)}",
            "",
            f"extern const chaintree_kb_info_t {self.unique_id}_kb_table[{len(kb_names)}];",
            "",
            f"#endif /* {guard_name} */"
        ]
        
        with open(output_file, 'w') as f:
            f.write("\n".join(lines))
        
        print(f"  Generated: {output_file}")
    
    def _generate_kb_info_implementation(self) -> None:
        """Generate knowledge base info implementation."""
        output_file = self.yaml_file.parent / f"{self.handle_name}_kb_info.c"
        
        # Filter out function mapping KBs (not executable ChainTree KBs)
        all_kb_names = self.handle.get_kb_names()
        kb_names = self._filter_executable_kbs(all_kb_names)
        
        lines = [
            "/* Auto-generated by ChainTree Pipeline */",
            f'#include "{self.handle_name}_kb_info.h"',
            "",
            "/* Knowledge base information table */",
            f"const chaintree_kb_info_t {self.unique_id}_kb_table[{len(kb_names)}] = {{"
        ]
        
        # Generate KB info entries
        for kb_name in kb_names:
            start_idx, end_idx = self.node_builder.get_kb_range(kb_name)
            node_count = end_idx - start_idx
            
            # Root node is the first node in breadth-first traversal
            root_node_index = start_idx
            
            # Calculate max depth for this KB
            max_depth = 0
            for i in range(start_idx, end_idx):
                ltree_name = self.node_builder.final_index_to_ltree[i]
                depth = self.node_builder.get_node_depth(ltree_name)
                max_depth = max(max_depth, depth)
            
            lines.extend([
                "    {",
                f'        .kb_name = "{kb_name}",',
                f"        .root_node_index = {root_node_index},",
                f"        .start_index = {start_idx},",
                f"        .node_count = {node_count},",
                f"        .max_depth = {max_depth}",
                "    },"
            ])
        
        lines.extend([
            "};",
            ""
        ])
        
        with open(output_file, 'w') as f:
            f.write("\n".join(lines))
        
        print(f"  Generated: {output_file}")

if __name__ == "__main__":
    yaml_file = Path("chaintree_config.yaml")
    
    if not yaml_file.exists():
        print(f"Error: {yaml_file} not found. Run ChainTreeYaml test first.")
        exit(1)
    
    # Run the pipeline
    generator = HeaderFileGenerator(
        yaml_file,
        handle_name="chaintree_handle"
    )
    
    generator.run_pipeline()
    
    print("\n" + "=" * 70)
    print("✓ Pipeline completed successfully!")
    print("=" * 70)