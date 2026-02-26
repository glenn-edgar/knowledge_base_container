"""
C Function Extractor - Pipeline stage for extracting and validating C functions

This stage uses CFunctionRegistry to extract function implementations from
C source files and validate them against the function tables.
"""

from pathlib import Path
from typing import Dict, List, Optional, Set
from .validated_function_index import ValidatedFunctionIndexer


# Copy of CFunctionRegistry from the document
"""
C Function Registry - Extract and manage C functions from source files.
"""
from pathlib import Path
from typing import Dict, Iterator, List, Union, Optional
import re


class CFunctionRegistry:
    """
    Registry for C functions marked with BEGIN/END comment blocks.
    
    Scans C source files for functions delimited by:
        /* === BEGIN: <prototype> === */
        ...function code...
        /* === END: <prototype> === */
    
    Functions are registered by name and include code and prototype.
    """
    
    # Regex patterns for parsing
    _BEGIN_PATTERN = re.compile(r"/\*\s*===\s*BEGIN:\s*(?P<proto>.+?)\s*===\s*\*/")
    _END_PATTERN = re.compile(r"/\*\s*===\s*END:\s*(?P<proto>.+?)\s*===\s*\*/")
    _NAME_PATTERN = re.compile(r"\b(?P<name>\w+)\s*\(")
    
    def __init__(self):
        """Initialize empty registry."""
        self.paths: List[Path] = []
        self._functions: Dict[str, Dict[str, str]] = {}
    
    def set_path(self, source_path: Union[str, Path]) -> 'CFunctionRegistry':
        """
        Set the source path for parsing.
        
        Args:
            source_path: Path to C source file
            
        Returns:
            Self for method chaining
        """
        self.paths = [Path(source_path)]
        return self
    
    def parse_file(self, source_path: Union[str, Path]) -> 'CFunctionRegistry':
        """
        Parse a single C source file and add functions to registry.
        
        Args:
            source_path: Path to C source file to parse
            
        Returns:
            Self for method chaining
            
        Raises:
            ValueError: If file has malformed BEGIN/END blocks
            FileNotFoundError: If source_path doesn't exist
        """
        path = Path(source_path)
        if path not in self.paths:
            self.paths.append(path)
        
        self._parse_source(path)
        return self
    
    def parse_files(self, source_paths: List[Union[str, Path]]) -> 'CFunctionRegistry':
        """
        Parse multiple C source files and add functions to registry.
        
        Args:
            source_paths: List of paths to C source files
            
        Returns:
            Self for method chaining
            
        Raises:
            ValueError: If any file has malformed BEGIN/END blocks
            FileNotFoundError: If any source_path doesn't exist
        """
        for path in source_paths:
            self.parse_file(path)
        return self
    
    def build_registry(self) -> 'CFunctionRegistry':
        """
        Build registry by parsing all set paths.
        
        Returns:
            Self for method chaining
            
        Raises:
            ValueError: If no paths have been set
        """
        if not self.paths:
            raise ValueError("No source paths set. Use set_path() or parse_file() first.")
        
        self._functions.clear()
        
        for path in self.paths:
            self._parse_source(path)
        
        return self
    
    def clear(self) -> 'CFunctionRegistry':
        """
        Clear all functions and paths from registry.
        
        Returns:
            Self for method chaining
        """
        self.paths.clear()
        self._functions.clear()
        return self
    
    def _parse_source(self, source_path: Path) -> None:
        """
        Parse source file and extract all marked functions.
        
        Validates:
        - END prototype matches BEGIN prototype
        - No END without BEGIN
        - No nested BEGIN blocks
        
        Args:
            source_path: Path to C source file
            
        Raises:
            ValueError: On any parsing error
        """
        text = source_path.read_text(encoding="utf-8")
        lines = text.splitlines(keepends=True)
        
        current_proto = None
        current_line_num = None
        block_lines = []
        
        for line_num, line in enumerate(lines, start=1):
            # Check for BEGIN marker
            begin_match = self._BEGIN_PATTERN.match(line)
            if begin_match:
                if current_proto is not None:
                    raise ValueError(
                        f"{source_path.name}:{line_num}: Nested BEGIN found. "
                        f"Previous BEGIN at line {current_line_num} for '{current_proto}' was not closed."
                    )
                current_proto = begin_match.group('proto').strip()
                current_line_num = line_num
                block_lines = [line]
                continue
            
            # Check for END marker
            end_match = self._END_PATTERN.match(line)
            if end_match:
                end_proto = end_match.group('proto').strip()
                
                # Verify we're inside a block
                if current_proto is None:
                    raise ValueError(
                        f"{source_path.name}:{line_num}: END marker found without matching BEGIN. "
                        f"Found END for '{end_proto}'."
                    )
                
                # Verify END matches BEGIN
                if end_proto != current_proto:
                    raise ValueError(
                        f"{source_path.name}:{line_num}: END prototype mismatch. "
                        f"BEGIN at line {current_line_num} has '{current_proto}', "
                        f"but END has '{end_proto}'."
                    )
                
                # Complete the block
                block_lines.append(line)
                name = self._extract_function_name(current_proto)
                code = ''.join(block_lines)
                
                # Check for duplicate function name
                if name in self._functions:
                    raise ValueError(
                        f"{source_path.name}:{line_num}: Duplicate function name '{name}'. "
                        f"Already registered with prototype '{self._functions[name]['prototype']}'."
                    )
                
                # Register function by name
                self._functions[name] = {
                    'code': code,
                    'prototype': current_proto,
                    'source_file': str(source_path)
                }
                
                current_proto = None
                current_line_num = None
                block_lines = []
                continue
            
            # If inside a block, accumulate lines
            if current_proto is not None:
                block_lines.append(line)
        
        # Verify all blocks were closed
        if current_proto is not None:
            raise ValueError(
                f"{source_path.name}:{current_line_num}: Missing END marker for BEGIN. "
                f"Function '{current_proto}' starting at line {current_line_num} was never closed."
            )
    
    def _extract_function_name(self, prototype: str) -> str:
        """
        Extract function name from prototype.
        
        Args:
            prototype: Function prototype string (e.g., "int foo(double x)")
            
        Returns:
            Function name (e.g., "foo")
            
        Raises:
            ValueError: If function name cannot be extracted
        """
        match = self._NAME_PATTERN.search(prototype)
        if not match:
            raise ValueError(
                f"Cannot extract function name from prototype: '{prototype}'"
            )
        return match.group("name")
    
    def get(self, name: str) -> Dict[str, str]:
        """
        Get function by name.
        
        Args:
            name: Function name
            
        Returns:
            Dictionary with 'code', 'prototype', and 'source_file' keys
            
        Raises:
            KeyError: If function name not found
        """
        if name not in self._functions:
            raise KeyError(f"Function '{name}' not found in registry.")
        return self._functions[name]
    
    def get_code(self, name: str) -> str:
        """
        Get code block for a function by name.
        
        Args:
            name: Function name
            
        Returns:
            Code block as string
            
        Raises:
            KeyError: If function name not found
        """
        return self.get(name)['code']
    
    def get_prototype(self, name: str) -> str:
        """
        Get prototype for a function by name.
        
        Args:
            name: Function name
            
        Returns:
            Prototype string
            
        Raises:
            KeyError: If function name not found
        """
        return self.get(name)['prototype']
    
    @property
    def names(self) -> List[str]:
        """Get list of all registered function names."""
        return list(self._functions.keys())
    
    def items(self) -> Iterator[tuple[str, Dict[str, str]]]:
        """
        Iterate over all functions.
        
        Yields:
            Tuple of (function_name, {'code': str, 'prototype': str, 'source_file': str})
        """
        return iter(self._functions.items())
    
    def __len__(self) -> int:
        """Return number of functions in registry."""
        return len(self._functions)
    
    def __contains__(self, name: str) -> bool:
        """Check if function exists by name."""
        return name in self._functions
    
    def __getitem__(self, name: str) -> Dict[str, str]:
        """Get function by name using [] operator."""
        return self.get(name)


class CFunctionExtractor:
    """
    Pipeline stage for extracting and validating C function implementations.
    
    Extracts functions from:
    1. Internal function library (built-in C files)
    2. External function files (specified per-YAML or project-specific)
    
    Validates all required functions have implementations before allowing
    C code generation.
    """
    
    def __init__(
        self,
        main_indexer: ValidatedFunctionIndexer,
        one_shot_indexer: ValidatedFunctionIndexer,
        boolean_indexer: ValidatedFunctionIndexer
    ):
        """
        Initialize function extractor.
        
        Args:
            main_indexer: Main function indexer
            one_shot_indexer: One-shot function indexer
            boolean_indexer: Boolean function indexer
        """
        self.main_indexer = main_indexer
        self.one_shot_indexer = one_shot_indexer
        self.boolean_indexer = boolean_indexer
        
        # C function registries
        self.internal_registry = CFunctionRegistry()
        self.external_registry = CFunctionRegistry()
        
        # Internal function library paths
        self.internal_function_paths: List[Path] = []
        
        # External function paths
        self.external_function_paths: List[Path] = []
    
    def set_internal_library(self, library_paths: List[Path]) -> None:
        """
        Set paths to internal function library files.
        
        Args:
            library_paths: List of paths to internal C source files
        """
        self.internal_function_paths = library_paths
    
    def add_external_sources(self, source_paths: List[Path]) -> None:
        """
        Add external source files for function extraction.
        
        Args:
            source_paths: List of paths to external C source files
        """
        self.external_function_paths.extend(source_paths)
    
    def extract_functions(self) -> None:
        """
        Extract functions from all registered source files.
        
        Parses both internal and external source files and builds
        function registries.
        """
        print("Extracting functions from source files...")
        
        # Parse internal library
        if self.internal_function_paths:
            print(f"  Internal library: {len(self.internal_function_paths)} file(s)")
            for path in self.internal_function_paths:
                if path.exists():
                    try:
                        self.internal_registry.parse_file(path)
                        print(f"    ✓ Parsed: {path.name}")
                    except (ValueError, FileNotFoundError) as e:
                        print(f"    ✗ Error in {path.name}: {e}")
                else:
                    print(f"    ⚠ Not found: {path}")
        
        # Parse external sources
        if self.external_function_paths:
            print(f"  External sources: {len(self.external_function_paths)} file(s)")
            for path in self.external_function_paths:
                if path.exists():
                    try:
                        self.external_registry.parse_file(path)
                        print(f"    ✓ Parsed: {path.name}")
                    except (ValueError, FileNotFoundError) as e:
                        print(f"    ✗ Error in {path.name}: {e}")
                else:
                    print(f"    ⚠ Not found: {path}")
        
        print(f"  Total functions extracted: {len(self.internal_registry) + len(self.external_registry)}")
    
    def validate_functions(self) -> None:
        """
        Validate all indexed functions against extracted implementations.
        
        Updates function indexers to mark functions as valid if their
        implementations were found in the registries.
        """
        print("\nValidating function implementations...")
        
        # Combine registries (external overrides internal)
        combined_functions = {}
        
        # Add internal functions first
        for name, func_data in self.internal_registry.items():
            combined_functions[name] = func_data
        
        # External functions override internal
        for name, func_data in self.external_registry.items():
            if name in combined_functions:
                print(f"  ℹ External function '{name}' overrides internal version")
            combined_functions[name] = func_data
        
        # Validate main functions
        self._validate_indexer(
            self.main_indexer,
            combined_functions,
            "Main"
        )
        
        # Validate one-shot functions
        self._validate_indexer(
            self.one_shot_indexer,
            combined_functions,
            "One-shot"
        )
        
        # Validate boolean functions
        self._validate_indexer(
            self.boolean_indexer,
            combined_functions,
            "Boolean"
        )
    
    def _validate_indexer(
        self,
        indexer: ValidatedFunctionIndexer,
        functions: Dict[str, Dict[str, str]],
        category: str
    ) -> None:
        """
        Validate a single function indexer.
        
        Args:
            indexer: Function indexer to validate
            functions: Dictionary of available function implementations
            category: Category name for logging
        """
        for func_name in indexer.get_all_functions():
            if func_name in functions:
                func_data = functions[func_name]
                indexer.set_function_valid(
                    func_name,
                    True,
                    func_data['prototype'],
                    func_data.get('source_file', 'unknown')
                )
    
    def all_functions_valid(self) -> bool:
        """
        Check if all required functions have valid implementations.
        
        Returns:
            True if all functions are valid, False otherwise
        """
        return (
            self.main_indexer.all_functions_valid() and
            self.one_shot_indexer.all_functions_valid() and
            self.boolean_indexer.all_functions_valid()
        )
    
    def get_validation_report(self) -> Dict[str, any]:
        """
        Get comprehensive validation report for all function types.
        
        Returns:
            Dictionary with validation status for each function type
        """
        return {
            'main_functions': self.main_indexer.get_validation_report(),
            'one_shot_functions': self.one_shot_indexer.get_validation_report(),
            'boolean_functions': self.boolean_indexer.get_validation_report(),
            'all_valid': self.all_functions_valid()
        }
    
    def get_function_code(self, function_name: str) -> Optional[str]:
        """
        Get function implementation code.
        
        Args:
            function_name: Name of function
            
        Returns:
            Function code or None if not found
        """
        # Check external first (higher priority)
        if function_name in self.external_registry:
            return self.external_registry.get_code(function_name)
        
        # Check internal
        if function_name in self.internal_registry:
            return self.internal_registry.get_code(function_name)
        
        return None
    
    def get_function_prototype(self, function_name: str) -> Optional[str]:
        """
        Get function prototype.
        
        Args:
            function_name: Name of function
            
        Returns:
            Function prototype or None if not found
        """
        # Check external first (higher priority)
        if function_name in self.external_registry:
            return self.external_registry.get_prototype(function_name)
        
        # Check internal
        if function_name in self.internal_registry:
            return self.internal_registry.get_prototype(function_name)
        
        return None
    
    def print_summary(self) -> None:
        """Print summary of function extraction and validation."""
        print("=" * 70)
        print("Function Extractor Summary")
        print("=" * 70)
        
        print(f"Internal library files: {len(self.internal_function_paths)}")
        print(f"External source files: {len(self.external_function_paths)}")
        print(f"Internal functions extracted: {len(self.internal_registry)}")
        print(f"External functions extracted: {len(self.external_registry)}")
        
        print("\nValidation Status:")
        self.main_indexer.print_summary()
        self.one_shot_indexer.print_summary()
        self.boolean_indexer.print_summary()
        
        if self.all_functions_valid():
            print("\n✓ All functions have valid implementations")
        else:
            print("\n⚠ Some functions are missing implementations")
            report = self.get_validation_report()
            
            all_invalid = set()
            all_invalid.update(report['main_functions']['invalid_list'])
            all_invalid.update(report['one_shot_functions']['invalid_list'])
            all_invalid.update(report['boolean_functions']['invalid_list'])
            
            print(f"\nMissing implementations ({len(all_invalid)}):")
            for func_name in sorted(all_invalid):
                print(f"  - {func_name}")


if __name__ == "__main__":
    print("Testing CFunctionExtractor")
    print("=" * 70)
    
    # Create indexers
    main_idx = ValidatedFunctionIndexer("main_functions")
    one_shot_idx = ValidatedFunctionIndexer("one_shot_functions")
    bool_idx = ValidatedFunctionIndexer("boolean_functions")
    
    # Add some functions
    main_idx.add_function("robot_main_loop")
    main_idx.add_function("nav_update")
    one_shot_idx.add_function("init_robot")
    one_shot_idx.add_function("shutdown_robot")
    bool_idx.add_function("check_robot_status")
    
    # Create extractor
    extractor = CFunctionExtractor(main_idx, one_shot_idx, bool_idx)
    
    # Would normally set paths here
    # extractor.set_internal_library([Path("internal_lib.c")])
    # extractor.add_external_sources([Path("user_funcs.c")])
    
    print("\nBefore extraction:")
    extractor.print_summary()
    
    print("\n" + "=" * 70)
    print("✓ Test completed!")
