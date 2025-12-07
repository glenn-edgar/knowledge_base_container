"""
Enhanced Function Indexer with Validity Tracking

Extends the basic FunctionIndexer to track whether each function has been
defined (has actual C code) or is just registered by name.
"""

from typing import Dict, List, Set, Optional
from pathlib import Path


class ValidatedFunctionIndexer:
    """
    Manages function name to index mapping with validity tracking.
    
    Functions start as invalid (valid=False) until their implementation
    is registered via set_function_valid() or register_implementation().
    """
class ValidatedFunctionIndexer:
    """
    Manages function name to index mapping with validity tracking.
    
    Functions start as invalid (valid=False) until their implementation
    is registered via set_function_valid() or register_implementation().
    """
    
    def __init__(self, name: str = "functions"):
        self.name = name
        self.function_to_index: Dict[str, int] = {}
        self.index_to_function: List[str] = []
        self.function_valid: Dict[str, bool] = {}  # Track if function has code
        self.function_prototypes: Dict[str, str] = {}  # Store prototypes
        self.function_source_files: Dict[str, str] = {}  # Track source file
        
        # Don't auto-add CFL_NULL - let the caller add the typed version
    # Optionally reserve index 0 for typed CFL_NULL
    # The caller should add the appropriately-typed null function
    # (e.g., "cfl_null_main", "cfl_null_one_shot", "cfl_null_boolean")
    
    def add_function(self, function_name: str) -> int:
        """
        Add a function and return its index.
        If already exists, return existing index.
        New functions default to invalid (valid=False).
        """
        if function_name in self.function_to_index:
            return self.function_to_index[function_name]
        
        index = len(self.index_to_function)
        self.function_to_index[function_name] = index
        self.index_to_function.append(function_name)
        self.function_valid[function_name] = False  # Default to invalid
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
        """
        Mark a function as valid (has implementation) or invalid.
        
        Args:
            function_name: Name of the function
            valid: Whether the function has valid implementation
            prototype: Function prototype (optional)
            source_file: Source file containing implementation (optional)
        """
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
        """
        Register a function implementation.
        If function doesn't exist, adds it. Marks as valid.
        
        Args:
            function_name: Name of the function
            prototype: Function prototype
            source_file: Source file containing implementation
            
        Returns:
            Function index
        """
        index = self.add_function(function_name)
        self.set_function_valid(function_name, True, prototype, source_file)
        return index
    
    def is_valid(self, function_name: str) -> bool:
        """Check if a function has valid implementation."""
        if function_name not in self.function_to_index:
            raise KeyError(f"Function not indexed: {function_name}")
        return self.function_valid[function_name]
    
    def get_invalid_functions(self) -> List[str]:
        """Get list of all functions that are invalid (no implementation)."""
        return [name for name, valid in self.function_valid.items() if not valid]
    
    def get_valid_functions(self) -> List[str]:
        """Get list of all functions that are valid (have implementation)."""
        return [name for name, valid in self.function_valid.items() if valid]
    
    def all_functions_valid(self) -> bool:
        """Check if all functions have valid implementations."""
        return all(self.function_valid.values())
    
    def get_validation_report(self) -> Dict[str, any]:
        """
        Get a validation report.
        
        Returns:
            Dictionary with validation statistics
        """
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
        """Get the index for a function name."""
        if function_name not in self.function_to_index:
            raise KeyError(f"Function not indexed: {function_name}")
        return self.function_to_index[function_name]
    
    def get_function(self, index: int) -> str:
        """Get the function name for an index."""
        if index < 0 or index >= len(self.index_to_function):
            raise IndexError(f"Function index out of range: {index}")
        return self.index_to_function[index]
    
    def get_prototype(self, function_name: str) -> str:
        """Get the prototype for a function."""
        if function_name not in self.function_to_index:
            raise KeyError(f"Function not indexed: {function_name}")
        return self.function_prototypes.get(function_name, "")
    
    def get_source_file(self, function_name: str) -> str:
        """Get the source file for a function."""
        if function_name not in self.function_to_index:
            raise KeyError(f"Function not indexed: {function_name}")
        return self.function_source_files.get(function_name, "")
    
    def get_all_functions(self) -> List[str]:
        """Get all functions in index order."""
        return self.index_to_function.copy()
    
    def get_count(self) -> int:
        """Get the total number of indexed functions."""
        return len(self.index_to_function)
    
    def generate_c_enum(self, enum_name: str) -> str:
        """Generate C enum for function indices."""
        lines = [f"typedef enum {{"]
        for i, func_name in enumerate(self.index_to_function):
            lines.append(f"    {enum_name}_{func_name.upper()} = {i},")
        lines.append(f"    {enum_name}_COUNT = {len(self.index_to_function)}")
        lines.append(f"}} {enum_name}_t;")
        return "\n".join(lines)
    
    def generate_c_string_array(self, array_name: str) -> str:
        """Generate C string array for function names."""
        lines = [f"const char *{array_name}[{len(self.index_to_function)}] = {{"]
        for func_name in self.index_to_function:
            lines.append(f'    "{func_name}",')
        lines.append("};")
        return "\n".join(lines)
    
    def generate_c_validity_array(self, array_name: str) -> str:
        """Generate C boolean array for function validity."""
        lines = [f"const bool {array_name}[{len(self.index_to_function)}] = {{"]
        for func_name in self.index_to_function:
            valid = "true" if self.function_valid[func_name] else "false"
            lines.append(f"    {valid},  /* {func_name} */")
        lines.append("};")
        return "\n".join(lines)
    
    def print_summary(self) -> None:
        """Print summary of function validity."""
        report = self.get_validation_report()
        print(f"  {self.name}:")
        print(f"    Total functions: {report['total_functions']}")
        print(f"    Valid functions: {report['valid_functions']}")
        print(f"    Invalid functions: {report['invalid_functions']}")
        
        if report['invalid_functions'] > 0:
            print(f"    Missing implementations:")
            for func in report['invalid_list']:
                print(f"      - {func}")


if __name__ == "__main__":
    # Test the validated function indexer
    print("Testing ValidatedFunctionIndexer")
    print("=" * 70)
    
    indexer = ValidatedFunctionIndexer("test_functions")
    
    # Add some functions (invalid by default)
    indexer.add_function("init_system")
    indexer.add_function("update_sensors")
    indexer.add_function("process_data")
    
    print("\nAfter adding functions (all invalid):")
    indexer.print_summary()
    
    # Register implementations for some
    indexer.register_implementation(
        "init_system",
        "void init_system(void)",
        "system.c"
    )
    indexer.register_implementation(
        "update_sensors",
        "void update_sensors(void)",
        "sensors.c"
    )
    
    print("\nAfter registering implementations:")
    indexer.print_summary()
    
    # Check validation status
    print("\nValidation Report:")
    report = indexer.get_validation_report()
    print(f"  All valid: {report['all_valid']}")
    print(f"  Valid functions: {report['valid_list']}")
    print(f"  Invalid functions: {report['invalid_list']}")
    
    # Generate C code
    print("\n" + "=" * 70)
    print("Generated C Code:")
    print("=" * 70)
    
    print("\nEnum:")
    print(indexer.generate_c_enum("TEST_FUNC"))
    
    print("\nFunction names:")
    print(indexer.generate_c_string_array("test_function_names"))
    
    print("\nFunction validity:")
    print(indexer.generate_c_validity_array("test_function_valid"))
    
    print("\n" + "=" * 70)
    print("✓ Test completed!")
