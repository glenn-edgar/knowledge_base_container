"""
Validated Function Indexer

Manages function name to index mapping with validity tracking.
Functions start as invalid (valid=False) until their implementation
is registered via set_function_valid() or register_implementation().
"""

from typing import Dict, List


class ValidatedFunctionIndexer:
    """
    Manages function name to index mapping with validity tracking.
    """
    
    def __init__(self, name: str = "functions"):
        self.name = name
        self.function_to_index: Dict[str, int] = {}
        self.index_to_function: List[str] = []
        self.function_valid: Dict[str, bool] = {}
        self.function_prototypes: Dict[str, str] = {}
        self.function_source_files: Dict[str, str] = {}
    
    def add_function(self, function_name: str) -> int:
        """Add a function and return its index. New functions default to invalid."""
        if function_name in self.function_to_index:
            return self.function_to_index[function_name]
        
        index = len(self.index_to_function)
        self.function_to_index[function_name] = index
        self.index_to_function.append(function_name)
        self.function_valid[function_name] = False
        self.function_prototypes[function_name] = ""
        self.function_source_files[function_name] = ""
        return index
    
    def set_function_valid(
        self,
        function_name: str,
        valid: bool = True,
        prototype: str = "",
        source_file: str = ""
    ) -> None:
        """Mark a function as valid (has implementation) or invalid."""
        if function_name not in self.function_to_index:
            raise KeyError(f"Function not indexed: {function_name}")
        
        self.function_valid[function_name] = valid
        if prototype:
            self.function_prototypes[function_name] = prototype
        if source_file:
            self.function_source_files[function_name] = source_file
    
    def register_implementation(
        self,
        function_name: str,
        prototype: str,
        source_file: str
    ) -> int:
        """Register a function implementation. Adds if missing, marks as valid."""
        index = self.add_function(function_name)
        self.set_function_valid(function_name, True, prototype, source_file)
        return index
    
    def is_valid(self, function_name: str) -> bool:
        if function_name not in self.function_to_index:
            raise KeyError(f"Function not indexed: {function_name}")
        return self.function_valid[function_name]
    
    def get_invalid_functions(self) -> List[str]:
        return [name for name, valid in self.function_valid.items() if not valid]
    
    def get_valid_functions(self) -> List[str]:
        return [name for name, valid in self.function_valid.items() if valid]
    
    def all_functions_valid(self) -> bool:
        return all(self.function_valid.values())
    
    def get_validation_report(self) -> Dict:
        total = len(self.function_valid)
        valid_count = sum(1 for v in self.function_valid.values() if v)
        invalid_count = total - valid_count
        return {
            'total_functions': total,
            'valid_functions': valid_count,
            'invalid_functions': invalid_count,
            'all_valid': invalid_count == 0,
            'invalid_list': self.get_invalid_functions(),
            'valid_list': self.get_valid_functions()
        }
    
    def get_index(self, function_name: str) -> int:
        if function_name not in self.function_to_index:
            raise KeyError(f"Function not indexed: {function_name}")
        return self.function_to_index[function_name]
    
    def get_function(self, index: int) -> str:
        if index < 0 or index >= len(self.index_to_function):
            raise IndexError(f"Function index out of range: {index}")
        return self.index_to_function[index]
    
    def get_prototype(self, function_name: str) -> str:
        if function_name not in self.function_to_index:
            raise KeyError(f"Function not indexed: {function_name}")
        return self.function_prototypes.get(function_name, "")
    
    def get_source_file(self, function_name: str) -> str:
        if function_name not in self.function_to_index:
            raise KeyError(f"Function not indexed: {function_name}")
        return self.function_source_files.get(function_name, "")
    
    def get_all_functions(self) -> List[str]:
        return self.index_to_function.copy()
    
    def get_count(self) -> int:
        return len(self.index_to_function)
    
    # =========================================================================
    # C Code Generation Helpers
    # =========================================================================
    
    def generate_c_enum(self, enum_name: str) -> str:
        lines = [f"typedef enum {{"]
        for i, func_name in enumerate(self.index_to_function):
            lines.append(f"    {enum_name}_{func_name.upper()} = {i},")
        lines.append(f"    {enum_name}_COUNT = {len(self.index_to_function)}")
        lines.append(f"}} {enum_name}_t;")
        return "\n".join(lines)
    
    def generate_c_string_array(self, array_name: str) -> str:
        lines = [f"const char *{array_name}[{len(self.index_to_function)}] = {{"]
        for func_name in self.index_to_function:
            lines.append(f'    "{func_name}",')
        lines.append("};")
        return "\n".join(lines)
    
    def generate_c_validity_array(self, array_name: str) -> str:
        lines = [f"const bool {array_name}[{len(self.index_to_function)}] = {{"]
        for func_name in self.index_to_function:
            valid = "true" if self.function_valid[func_name] else "false"
            lines.append(f"    {valid},  /* {func_name} */")
        lines.append("};")
        return "\n".join(lines)
    
    def print_summary(self) -> None:
        report = self.get_validation_report()
        print(f"  {self.name}:")
        print(f"    Total functions: {report['total_functions']}")
        print(f"    Valid functions: {report['valid_functions']}")
        print(f"    Invalid functions: {report['invalid_functions']}")
        
        if report['invalid_functions'] > 0:
            print(f"    Missing implementations:")
            for func in report['invalid_list']:
                print(f"      - {func}")