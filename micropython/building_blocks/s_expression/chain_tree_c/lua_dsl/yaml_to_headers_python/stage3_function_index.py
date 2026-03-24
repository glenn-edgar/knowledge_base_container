"""
Stage 3: Function Index Builder

Builds function index tables for all function types with typed name suffixes:
  - Main functions:     append "_main"
  - One-shot functions: append "_one_shot"
  - Boolean functions:  append "_boolean"

Each type gets its own ValidatedFunctionIndexer with CFL_NULL at index 0.
"""

from typing import Dict

from .stage1_handle import ChainTreeHandle
from .validated_function_index import ValidatedFunctionIndexer


class FunctionIndexBuilder:
    """
    Build function index tables from ChainTree YAML.
    """
    
    def __init__(self, handle: ChainTreeHandle):
        self.handle = handle
        
        # Function indexers with validity tracking
        self.main_indexer = ValidatedFunctionIndexer("main_function")
        self.one_shot_indexer = ValidatedFunctionIndexer("one_shot_function")
        self.boolean_indexer = ValidatedFunctionIndexer("boolean_function")
        
        # Original name -> typed name mappings
        self.main_name_map: Dict[str, str] = {}
        self.one_shot_name_map: Dict[str, str] = {}
        self.boolean_name_map: Dict[str, str] = {}
    
    def _make_typed_name(self, func_name: str, suffix: str) -> str:
        """Convert function name to typed name: e.g. 'Init_System' -> 'init_system_main'"""
        return f"{func_name.lower()}_{suffix}"
    
    def build(self) -> None:
        """Build function index tables from all nodes using original names."""
        all_functions = self.handle.get_all_functions()
        
        # ---- Main functions ----
        self.main_indexer.add_function("CFL_NULL")
        self.main_indexer.set_function_valid(
            "CFL_NULL", True,
            "unsigned cfl_null_main_fn(void *handle, unsigned bool_function_index, "
            "unsigned node_index, unsigned event_type, unsigned event_id, void *event_data)",
            "builtin"
        )
        self.main_name_map["CFL_NULL"] = self._make_typed_name("CFL_NULL", "main")
        
        for func_name in sorted(all_functions['main']):
            if func_name == "CFL_NULL":
                continue
            self.main_name_map[func_name] = self._make_typed_name(func_name, "main")
            self.main_indexer.add_function(func_name)
        
        # ---- One-shot functions ----
        self.one_shot_indexer.add_function("CFL_NULL")
        self.one_shot_indexer.set_function_valid(
            "CFL_NULL", True,
            "void cfl_null_one_shot_fn(void *handle, unsigned node_index)",
            "builtin"
        )
        self.one_shot_name_map["CFL_NULL"] = self._make_typed_name("CFL_NULL", "one_shot")
        
        for func_name in sorted(all_functions['one_shot']):
            if func_name == "CFL_NULL":
                continue
            self.one_shot_name_map[func_name] = self._make_typed_name(func_name, "one_shot")
            self.one_shot_indexer.add_function(func_name)
        
        # ---- Boolean functions ----
        self.boolean_indexer.add_function("CFL_NULL")
        self.boolean_indexer.set_function_valid(
            "CFL_NULL", True,
            "bool cfl_null_boolean_fn(void *handle, unsigned node_index, "
            "unsigned event_type, unsigned event_id, void *event_data)",
            "builtin"
        )
        self.boolean_name_map["CFL_NULL"] = self._make_typed_name("CFL_NULL", "boolean")
        
        for func_name in sorted(all_functions['boolean']):
            if func_name == "CFL_NULL":
                continue
            self.boolean_name_map[func_name] = self._make_typed_name(func_name, "boolean")
            self.boolean_indexer.add_function(func_name)
    
    # =========================================================================
    # Typed Name Accessors
    # =========================================================================
    
    def get_typed_main_name(self, original_name: str) -> str:
        return self.main_name_map.get(original_name, self._make_typed_name(original_name, "main"))

    def get_typed_one_shot_name(self, original_name: str) -> str:
        return self.one_shot_name_map.get(original_name, self._make_typed_name(original_name, "one_shot"))

    def get_typed_boolean_name(self, original_name: str) -> str:
        return self.boolean_name_map.get(original_name, self._make_typed_name(original_name, "boolean"))
    
    def print_summary(self) -> None:
        print("=" * 70)
        print("Stage 3: Function Index Builder Summary")
        print("=" * 70)
        print(f"Main functions: {self.main_indexer.get_count()}")
        print(f"One-shot functions: {self.one_shot_indexer.get_count()}")
        print(f"Boolean functions: {self.boolean_indexer.get_count()}")
        
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