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
    
    def output_files(
        self,
        function_names: List[str],
        c_file_path: Union[str, Path],
        h_file_path: Union[str, Path],
        h_includes: Optional[List[str]] = None,
        c_includes: Optional[List[str]] = None
    ) -> 'CFunctionRegistry':
        """
        Output selected functions to C and H files.
        
        Args:
            function_names: List of function names to output
            c_file_path: Path for output .c file
            h_file_path: Path for output .h file
            h_includes: List of include files for header (e.g., ['<stdio.h>', '"types.h"'])
            c_includes: List of include files for C file (e.g., ['<string.h>', '"utils.h"'])
            
        Returns:
            Self for method chaining
            
        Raises:
            KeyError: If any requested function is not in registry
        """
        # Validate all functions exist first
        missing_functions = [name for name in function_names if name not in self._functions]
        if missing_functions:
            raise KeyError(
                f"Requested function(s) not found in registry: {', '.join(missing_functions)}"
            )
        
        c_path = Path(c_file_path)
        h_path = Path(h_file_path)
        
        # Default to empty lists if None
        h_includes = h_includes or []
        c_includes = c_includes or []
        
        # Generate header file
        self._write_header_file(h_path, function_names, h_includes)
        
        # Generate C file
        self._write_c_file(c_path, h_path, function_names, c_includes)
        
        return self
    
    def _write_header_file(
        self,
        h_path: Path,
        function_names: List[str],
        includes: List[str]
    ) -> None:
        """
        Write header file with function prototypes.
        
        Args:
            h_path: Path to header file
            function_names: List of function names to include
            includes: List of include directives
        """
        # Generate header guard
        guard_name = h_path.stem.upper() + "_H"
        
        lines = []
        lines.append(f"#ifndef {guard_name}")
        lines.append(f"#define {guard_name}")
        lines.append("")
        
        # Add includes if any
        if includes:
            for include in includes:
                lines.append(f"#include {include}")
            lines.append("")
        
        lines.append("/* Function prototypes */")
        lines.append("")
        
        # Add prototypes
        for name in function_names:
            func = self._functions[name]
            prototype = func['prototype']
            lines.append(f"{prototype};")
        
        lines.append("")
        lines.append(f"#endif /* {guard_name} */")
        lines.append("")
        
        # Write file
        h_path.write_text('\n'.join(lines), encoding='utf-8')
    
    def _write_c_file(
        self,
        c_path: Path,
        h_path: Path,
        function_names: List[str],
        includes: List[str]
    ) -> None:
        """
        Write C file with function implementations.
        
        Args:
            c_path: Path to C file
            h_path: Path to corresponding header file
            function_names: List of function names to include
            includes: List of include directives
        """
        lines = []
        lines.append(f'#include "{h_path.name}"')
        lines.append("")
        
        # Add additional includes if any
        if includes:
            for include in includes:
                lines.append(f"#include {include}")
            lines.append("")
        
        # Add function implementations
        for i, name in enumerate(function_names):
            if i > 0:
                lines.append("")  # Blank line between functions
            
            func = self._functions[name]
            code = func['code']
            
            # Strip BEGIN and END markers
            clean_code = self._strip_markers(code)
            lines.append(clean_code.rstrip())
        
        lines.append("")
        
        # Write file
        c_path.write_text('\n'.join(lines), encoding='utf-8')
    
    def _strip_markers(self, code: str) -> str:
        """
        Remove BEGIN and END marker lines from code block.
        
        Args:
            code: Code block with markers
            
        Returns:
            Code without markers
        """
        lines = code.splitlines(keepends=False)
        
        # Remove first line if it's a BEGIN marker
        if lines and self._BEGIN_PATTERN.match(lines[0]):
            lines = lines[1:]
        
        # Remove last line if it's an END marker
        if lines and self._END_PATTERN.match(lines[-1]):
            lines = lines[:-1]
        
        return '\n'.join(lines)
    
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
                    'prototype': current_proto
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
            Dictionary with 'code' and 'prototype' keys
            
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
            Tuple of (function_name, {'code': str, 'prototype': str})
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
    
    def __repr__(self) -> str:
        """String representation of registry."""
        paths_str = ", ".join(str(p.name) for p in self.paths) if self.paths else "none"
        return f"CFunctionRegistry(paths=[{paths_str}], functions={len(self)})"


def main():
    """Example usage."""
    
    # Create registry and parse files
    registry = CFunctionRegistry()
    registry.parse_files(["file1.c", "file2.c"])
    
    print(f"{registry}\n")
    print(f"Functions found: {registry.names}\n")
    
    # Output selected functions to new files with includes
    try:
        registry.output_files(
            function_names=["foo", "bar", "baz"],
            c_file_path="output.c",
            h_file_path="output.h",
            h_includes=["<stdio.h>", "<stdint.h>", '"types.h"'],
            c_includes=["<string.h>", "<stdlib.h>", '"utils.h"']
        )
        print("Files generated successfully!")
    except KeyError as e:
        print(f"Error: {e}")
    
    # Access by function name
    if "my_function" in registry:
        func = registry["my_function"]
        print(f"\nFunction: {func['prototype']}")
        print(f"Code:\n{func['code']}")


if __name__ == "__main__":
    main()