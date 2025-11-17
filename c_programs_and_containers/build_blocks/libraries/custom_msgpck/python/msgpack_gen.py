#!/usr/bin/env python3
"""
MessagePack Code Generator for Arena Allocator

Can be used as:
  1. Command-line tool: python msgpack_gen.py config.json
  2. Python library: from msgpack_gen import MsgPackCodeGenerator
  3. Production preprocessor: Generate multiple configs, shared runtime

Example production preprocessor usage:
    from msgpack_gen import MsgPackCodeGenerator, RuntimeGenerator
    
    # Generate multiple data structures
    configs = {
        "device_config": {...},
        "sensor_config": {...},
        "network_config": {...}
    }
    
    runtime = RuntimeGenerator()
    
    for name, data in configs.items():
        gen = MsgPackCodeGenerator(name)
        gen.load_dict(data)
        gen.generate_data_only("output/")
        runtime.merge(gen)  # Collect all hashes
    
    # Generate single shared runtime
    runtime.generate("output/", "msgpack_runtime")
"""

import sys
import json
from typing import Any, Dict, List, Tuple, Optional, Union, Set
from dataclasses import dataclass
from pathlib import Path
from io import StringIO

# Try to import msgpack
try:
    import msgpack as msgpack_lib
    HAVE_MSGPACK = True
except ImportError:
    HAVE_MSGPACK = False

# FNV-1a 64-bit hash constants
FNV_OFFSET_BASIS_64 = 14695981039346656037
FNV_PRIME_64 = 1099511628211

def hash_string_64(s: str) -> int:
    """
    FNV-1a 64-bit hash (matches C implementation)
    
    Args:
        s: String to hash
        
    Returns:
        64-bit hash value
    """
    hash_val = FNV_OFFSET_BASIS_64
    for byte in s.encode('utf-8'):
        hash_val ^= byte
        hash_val = (hash_val * FNV_PRIME_64) & 0xFFFFFFFFFFFFFFFF
    return hash_val

@dataclass
class NodeInfo:
    """Information about a node in the tree"""
    offset: int
    type_name: str
    data_offset: Optional[int] = None
    child_offset: Optional[int] = None
    value: Any = None

@dataclass
class GenerationStats:
    """Statistics about generated code"""
    node_count: int
    node_bytes: int
    string_bytes: int
    total_bytes: int
    unique_strings: int
    hashed_keys: int
    
    def __str__(self) -> str:
        return (f"Nodes: {self.node_count}, "
                f"Node area: {self.node_bytes} bytes, "
                f"String pool: {self.string_bytes} bytes, "
                f"Total: {self.total_bytes} bytes, "
                f"Unique strings: {self.unique_strings}, "
                f"Hashed keys: {self.hashed_keys}")

class RuntimeGenerator:
    """
    Shared runtime generator for multiple MessagePack data structures
    
    Collects hashes from multiple MsgPackCodeGenerator instances and
    generates a single shared runtime with all hash mappings.
    
    Example:
        runtime = RuntimeGenerator()
        
        # Add multiple configs
        for name, data in configs.items():
            gen = MsgPackCodeGenerator(name)
            gen.load_dict(data)
            gen.generate_data_only("output/")
            runtime.merge(gen)
        
        # Generate single shared runtime
        runtime.generate("output/", "msgpack_runtime")
    """
    
    def __init__(self):
        """Initialize runtime generator"""
        self.hash_map: Dict[str, int] = {}
        self.generator_names: Set[str] = set()
    
    def merge(self, generator: 'MsgPackCodeGenerator'):
        """
        Merge hashes from a generator
        
        Args:
            generator: MsgPackCodeGenerator instance
        """
        self.hash_map.update(generator.hash_map)
        self.generator_names.add(generator.var_name)
    
    def add_hash(self, key: str, hash_value: Optional[int] = None):
        """
        Manually add a hash mapping
        
        Args:
            key: String key
            hash_value: Hash value (computed if None)
        """
        if hash_value is None:
            hash_value = hash_string_64(key)
        self.hash_map[key] = hash_value
    
    def get_hash_count(self) -> int:
        """Get number of unique hashes"""
        return len(self.hash_map)
    
    def generate_header(self, output: Union[str, StringIO] = None, 
                       runtime_name: str = "msgpack_runtime") -> str:
        """
        Generate shared runtime header
        
        Args:
            output: File path or StringIO. If None, returns string.
            runtime_name: Base name for runtime files
            
        Returns:
            Generated code
        """
        guard_name = f"{runtime_name.upper()}_H"
        
        lines = []
        lines.append("// Auto-generated MessagePack shared runtime")
        lines.append("// DO NOT EDIT MANUALLY")
        lines.append("// This runtime is shared across multiple data structures\n")
        
        if self.generator_names:
            lines.append(f"// Used by: {', '.join(sorted(self.generator_names))}\n")
        
        lines.append(f"#ifndef {guard_name}")
        lines.append(f"#define {guard_name}\n")
        lines.append("#include <stdint.h>")
        lines.append("#include <stddef.h>\n")
        
        if self.hash_map:
            lines.append("// ========== Hash Value Macros ==========")
            lines.append("// Compile-time hash constants for all known keys\n")
            for string, hash_val in sorted(self.hash_map.items()):
                safe_name = string.upper().replace(' ', '_').replace('-', '_')
                safe_name = ''.join(c if c.isalnum() or c == '_' else '_' for c in safe_name)
                lines.append(f"#define HASH_{safe_name} 0x{hash_val:016X}ULL")
            lines.append("")
        
        lines.append("// Hash-to-string mapping for debugging")
        lines.append("typedef struct {")
        lines.append("    uint64_t hash;")
        lines.append("    const char* str;")
        lines.append("} HashMapping;\n")
        
        lines.append("// All known hash mappings")
        lines.append("extern const HashMapping all_known_hashes[];")
        lines.append("extern const size_t all_known_hashes_count;\n")
        
        lines.append("// Reverse lookup: hash -> string")
        lines.append("const char* msgpack_hash_to_string(uint64_t hash);\n")
        
        lines.append("// Statistics")
        lines.append(f"#define MSGPACK_RUNTIME_HASH_COUNT {len(self.hash_map)}")
        lines.append(f"#define MSGPACK_RUNTIME_CONFIG_COUNT {len(self.generator_names)}\n")
        
        lines.append(f"#endif // {guard_name}")
        
        code = '\n'.join(lines)
        
        if isinstance(output, str):
            with open(output, 'w') as f:
                f.write(code)
        elif output is not None:
            output.write(code)
        
        return code
    
    def generate_source(self, output: Union[str, StringIO] = None,
                       runtime_name: str = "msgpack_runtime") -> str:
        """
        Generate shared runtime source
        
        Args:
            output: File path or StringIO. If None, returns string.
            runtime_name: Base name for runtime files
            
        Returns:
            Generated code
        """
        lines = []
        lines.append("// Auto-generated MessagePack shared runtime")
        lines.append("// DO NOT EDIT MANUALLY\n")
        lines.append("#include <stdint.h>")
        lines.append("#include <stddef.h>")
        lines.append(f'#include "{runtime_name}.h"\n')
        
        if self.hash_map:
            lines.append("// All known string hashes from all configurations")
            lines.append("const HashMapping all_known_hashes[] = {")
            for string, hash_val in sorted(self.hash_map.items()):
                escaped = string.replace('\\', '\\\\').replace('"', '\\"')
                lines.append(f'    {{0x{hash_val:016X}ULL, "{escaped}"}},')
            lines.append("};\n")
            
            lines.append("const size_t all_known_hashes_count = "
                        "sizeof(all_known_hashes) / sizeof(all_known_hashes[0]);\n")
            
            lines.append("const char* msgpack_hash_to_string(uint64_t hash) {")
            lines.append("    for (size_t i = 0; i < all_known_hashes_count; i++) {")
            lines.append("        if (all_known_hashes[i].hash == hash) {")
            lines.append("            return all_known_hashes[i].str;")
            lines.append("        }")
            lines.append("    }")
            lines.append("    return NULL;")
            lines.append("}")
        else:
            lines.append("const HashMapping all_known_hashes[] = {{0}};")
            lines.append("const size_t all_known_hashes_count = 0;\n")
            lines.append("const char* msgpack_hash_to_string(uint64_t hash) {")
            lines.append("    (void)hash;")
            lines.append("    return NULL;")
            lines.append("}")
        
        code = '\n'.join(lines)
        
        if isinstance(output, str):
            with open(output, 'w') as f:
                f.write(code)
        elif output is not None:
            output.write(code)
        
        return code
    
    def generate(self, output_dir: str = ".", 
                runtime_name: str = "msgpack_runtime") -> Dict[str, str]:
        """
        Generate both runtime files
        
        Args:
            output_dir: Output directory
            runtime_name: Base name for runtime files
            
        Returns:
            Dictionary mapping file type to file path
        """
        output_path = Path(output_dir)
        output_path.mkdir(parents=True, exist_ok=True)
        
        files = {
            'runtime_header': str(output_path / f"{runtime_name}.h"),
            'runtime_source': str(output_path / f"{runtime_name}.c"),
        }
        
        self.generate_header(files['runtime_header'], runtime_name)
        self.generate_source(files['runtime_source'], runtime_name)
        
        return files


class MsgPackCodeGenerator:
    """
    MessagePack code generator for arena allocator
    
    Generates C code from Python data structures with string hashing.
    Supports JSON, MessagePack binary, and Python dict/list.
    
    For production preprocessors, use generate_data_only() and merge
    into a shared RuntimeGenerator.
    
    Example:
        >>> gen = MsgPackCodeGenerator("config")
        >>> gen.load_json("config.json")
        >>> gen.generate_data_only("output/")  # Data files only
    """
    
    NODE_SIZE = 20  # sizeof(MsgPackNode)
    
    def __init__(self, var_name: str = "msgpack_data", verbose: bool = False):
        """
        Initialize generator
        
        Args:
            var_name: Variable name prefix for generated code
            verbose: Enable verbose output
        """
        self.var_name = var_name
        self.verbose = verbose
        self.nodes: List[NodeInfo] = []
        self.string_data = bytearray()
        self.string_map: Dict[str, int] = {}
        self.hash_map: Dict[str, int] = {}
        self._log_messages: List[str] = []
        
    def _log(self, message: str):
        """Log message if verbose mode enabled"""
        self._log_messages.append(message)
        if self.verbose:
            print(message)
    
    def get_log_messages(self) -> List[str]:
        """Get all log messages"""
        return self._log_messages.copy()
    
    def clear_log(self):
        """Clear log messages"""
        self._log_messages.clear()
    
    def reset(self):
        """Reset generator state"""
        self.nodes.clear()
        self.string_data.clear()
        self.string_map.clear()
        self.hash_map.clear()
        self._log_messages.clear()
    
    # ========== Data Loading Methods ==========
    
    def load_json(self, filepath: str) -> 'MsgPackCodeGenerator':
        """Load data from JSON file"""
        self._log(f"Loading JSON from {filepath}")
        with open(filepath, 'r') as f:
            data = json.load(f)
        self.build_tree(data)
        return self
    
    def load_json_string(self, json_str: str) -> 'MsgPackCodeGenerator':
        """Load data from JSON string"""
        self._log("Loading JSON from string")
        data = json.loads(json_str)
        self.build_tree(data)
        return self
    
    def load_msgpack(self, filepath: str) -> 'MsgPackCodeGenerator':
        """Load data from MessagePack file"""
        if not HAVE_MSGPACK:
            raise RuntimeError("msgpack module not available. Install with: pip install msgpack")
        
        self._log(f"Loading MessagePack from {filepath}")
        with open(filepath, 'rb') as f:
            data = msgpack_lib.unpackb(f.read(), raw=False)
        self.build_tree(data)
        return self
    
    def load_msgpack_bytes(self, data: bytes) -> 'MsgPackCodeGenerator':
        """Load data from MessagePack bytes"""
        if not HAVE_MSGPACK:
            raise RuntimeError("msgpack module not available. Install with: pip install msgpack")
        
        self._log("Loading MessagePack from bytes")
        decoded = msgpack_lib.unpackb(data, raw=False)
        self.build_tree(decoded)
        return self
    
    def load_dict(self, data: Union[dict, list]) -> 'MsgPackCodeGenerator':
        """Load data from Python dict or list"""
        self._log("Loading from Python dict/list")
        self.build_tree(data)
        return self
    
    def load_auto(self, filepath: str) -> 'MsgPackCodeGenerator':
        """Auto-detect format and load"""
        ext = Path(filepath).suffix.lower()
        
        if ext == '.json':
            return self.load_json(filepath)
        elif ext in ['.msgpack', '.mp', '.mpk']:
            return self.load_msgpack(filepath)
        else:
            try:
                return self.load_json(filepath)
            except:
                pass
            
            if HAVE_MSGPACK:
                try:
                    return self.load_msgpack(filepath)
                except:
                    pass
            
            raise RuntimeError(f"Could not load {filepath} as JSON or MessagePack")
    
    # ========== Tree Building Methods ==========
    
    def add_string(self, s: str) -> int:
        """Add string to pool, return offset"""
        if s in self.string_map:
            return self.string_map[s]
        
        offset = len(self.string_data)
        self.string_map[s] = offset
        self.string_data.extend(s.encode('utf-8'))
        self.string_data.append(0)
        return offset
    
    def hash_key(self, key: str) -> int:
        """Hash a key and store mapping"""
        if key not in self.hash_map:
            self.hash_map[key] = hash_string_64(key)
        return self.hash_map[key]
    
    def build_tree(self, data: Any) -> int:
        """Build node tree from Python data"""
        self._log("Building tree structure")
        return self._build_node(data)
    
    def _build_node(self, data: Any) -> int:
        """Recursively build nodes"""
        node_offset = len(self.nodes) * self.NODE_SIZE
        
        if data is None:
            node = NodeInfo(offset=node_offset, type_name="MSGPACK_TYPE_NIL")
            self.nodes.append(node)
            
        elif isinstance(data, bool):
            node = NodeInfo(offset=node_offset, type_name="MSGPACK_TYPE_BOOL", value=1 if data else 0)
            self.nodes.append(node)
            
        elif isinstance(data, int):
            if data < 0:
                node = NodeInfo(offset=node_offset, type_name="MSGPACK_TYPE_INT", value=data)
            else:
                node = NodeInfo(offset=node_offset, type_name="MSGPACK_TYPE_UINT", value=data)
            self.nodes.append(node)
            
        elif isinstance(data, float):
            node = NodeInfo(offset=node_offset, type_name="MSGPACK_TYPE_DOUBLE", value=data)
            self.nodes.append(node)
            
        elif isinstance(data, str):
            str_offset = self.add_string(data)
            node = NodeInfo(offset=node_offset, type_name="MSGPACK_TYPE_STR", 
                          data_offset=str_offset, value=len(data))
            self.nodes.append(node)
            
        elif isinstance(data, (bytes, bytearray)):
            str_offset = len(self.string_data)
            self.string_data.extend(data)
            node = NodeInfo(offset=node_offset, type_name="MSGPACK_TYPE_BIN",
                          data_offset=str_offset, value=len(data))
            self.nodes.append(node)
            
        elif isinstance(data, list):
            node = NodeInfo(offset=node_offset, type_name="MSGPACK_TYPE_ARRAY", value=len(data))
            node_idx = len(self.nodes)
            self.nodes.append(node)
            
            first_child_offset = None
            for item in data:
                child_offset = self._build_node(item)
                if first_child_offset is None:
                    first_child_offset = child_offset
            
            self.nodes[node_idx].child_offset = first_child_offset
            
        elif isinstance(data, dict):
            node = NodeInfo(offset=node_offset, type_name="MSGPACK_TYPE_MAP", value=len(data))
            node_idx = len(self.nodes)
            self.nodes.append(node)
            
            first_child_offset = None
            for key, value in data.items():
                key_hash = self.hash_key(str(key))
                key_node_offset = len(self.nodes) * self.NODE_SIZE
                key_node = NodeInfo(offset=key_node_offset, type_name="MSGPACK_TYPE_UINT", value=key_hash)
                self.nodes.append(key_node)
                
                if first_child_offset is None:
                    first_child_offset = key_node_offset
                
                self._build_node(value)
            
            self.nodes[node_idx].child_offset = first_child_offset
            
        else:
            raise ValueError(f"Unsupported type: {type(data)}")
        
        return node_offset
    
    # ========== Statistics ==========
    
    def get_stats(self) -> GenerationStats:
        """Get generation statistics"""
        node_bytes = len(self.nodes) * self.NODE_SIZE
        string_bytes = len(self.string_data)
        
        return GenerationStats(
            node_count=len(self.nodes),
            node_bytes=node_bytes,
            string_bytes=string_bytes,
            total_bytes=node_bytes + string_bytes,
            unique_strings=len(self.string_map),
            hashed_keys=len(self.hash_map)
        )
    
    def print_stats(self):
        """Print generation statistics"""
        stats = self.get_stats()
        
        print(f"\nGenerated MessagePack structure '{self.var_name}':")
        print(f"  Nodes:          {stats.node_count}")
        print(f"  Node area:      {stats.node_bytes} bytes")
        print(f"  String pool:    {stats.string_bytes} bytes")
        print(f"  Total:          {stats.total_bytes} bytes")
        print(f"  Unique strings: {stats.unique_strings}")
        print(f"  Hashed keys:    {stats.hashed_keys}")
        if stats.total_bytes > 0:
            print(f"  Efficiency:     {100 * stats.node_bytes / stats.total_bytes:.1f}% nodes, "
                  f"{100 * stats.string_bytes / stats.total_bytes:.1f}% strings")
    
    # ========== Code Generation ==========
    
    def generate_data_header(self, output: Union[str, StringIO] = None) -> str:
        """Generate data header file"""
        self._log(f"Generating data header for '{self.var_name}'")
        
        if isinstance(output, str):
            base_name = Path(output).stem
        else:
            base_name = f"{self.var_name}_data"
        
        guard_name = f"{base_name.upper()}_H"
        
        lines = []
        lines.append("// Auto-generated MessagePack data structure")
        lines.append(f"// Variable name: {self.var_name}")
        lines.append("// DO NOT EDIT MANUALLY\n")
        lines.append(f"#ifndef {guard_name}")
        lines.append(f"#define {guard_name}\n")
        lines.append("#include <stdint.h>")
        lines.append("#include <stddef.h>")
        lines.append("#include <stdbool.h>")
        lines.append('#include "msgpack_arena.h"\n')
        
        stats = self.get_stats()
        lines.append(f"// Statistics:")
        lines.append(f"//   Total size: {stats.total_bytes} bytes")
        lines.append(f"//   Nodes: {stats.node_count} ({stats.node_bytes} bytes)")
        lines.append(f"//   Strings: {stats.string_bytes} bytes")
        lines.append(f"//   Unique strings: {stats.unique_strings}")
        lines.append(f"//   Hashed keys: {stats.hashed_keys}\n")
        
        lines.append("// Data buffer (in flash/ROM)")
        lines.append(f"extern const uint8_t {self.var_name}_buffer[];")
        lines.append(f"extern const size_t {self.var_name}_buffer_size;\n")
        
        lines.append("// Arena instance")
        lines.append(f"extern MsgPackArena {self.var_name}_arena;\n")
        
        lines.append("// Initialization and access")
        lines.append(f"bool {self.var_name}_init(void);")
        lines.append(f"const MsgPackNode* {self.var_name}_root(void);\n")
        
        lines.append("// Size constants")
        lines.append(f"#define {self.var_name.upper()}_SIZE {stats.total_bytes}")
        lines.append(f"#define {self.var_name.upper()}_NODE_COUNT {stats.node_count}")
        lines.append(f"#define {self.var_name.upper()}_STRING_SIZE {stats.string_bytes}\n")
        
        lines.append(f"#endif // {guard_name}")
        
        code = '\n'.join(lines)
        
        if isinstance(output, str):
            with open(output, 'w') as f:
                f.write(code)
        elif output is not None:
            output.write(code)
        
        return code
    
    def generate_data_source(self, output: Union[str, StringIO] = None) -> str:
        """Generate data source file"""
        self._log(f"Generating data source for '{self.var_name}'")
        
        lines = []
        lines.append("// Auto-generated MessagePack data structure")
        lines.append(f"// Variable name: {self.var_name}")
        lines.append("// DO NOT EDIT MANUALLY\n")
        lines.append("#include <stdint.h>")
        
        if isinstance(output, str):
            header_name = Path(output).stem.replace("_data", "") + "_data.h"
        else:
            header_name = f"{self.var_name}_data.h"
        
        lines.append(f'#include "{header_name}"\n')
        
        node_area_size = len(self.nodes) * self.NODE_SIZE
        string_area_offset = node_area_size
        
        lines.append(f"const uint8_t {self.var_name}_buffer[] __attribute__((section(\".rodata\"))) = {{")
        lines.append("    // ========== Nodes ==========")
        
        for i, node in enumerate(self.nodes):
            lines.append(f"    // Node {i}: {node.type_name}")
            
            type_byte = self._get_type_enum(node.type_name)
            flags = 0
            element_count = node.value if node.type_name in ["MSGPACK_TYPE_ARRAY", "MSGPACK_TYPE_MAP"] else 0
            
            data_offset = 0
            if node.data_offset is not None:
                data_offset = string_area_offset + node.data_offset
            
            child_offset = node.child_offset if node.child_offset is not None else 0
            
            node_bytes = []
            node_bytes.append(f"{type_byte}, {flags}, ")
            node_bytes.append(f"{element_count & 0xFF}, {(element_count >> 8) & 0xFF}, ")
            node_bytes.append(f"{data_offset & 0xFF}, {(data_offset >> 8) & 0xFF}, ")
            node_bytes.append(f"{(data_offset >> 16) & 0xFF}, {(data_offset >> 24) & 0xFF}, ")
            node_bytes.append(f"{child_offset & 0xFF}, {(child_offset >> 8) & 0xFF}, ")
            node_bytes.append(f"{(child_offset >> 16) & 0xFF}, {(child_offset >> 24) & 0xFF}, ")
            
            if node.type_name in ["MSGPACK_TYPE_INT", "MSGPACK_TYPE_UINT"]:
                val = node.value if node.value is not None else 0
                if node.type_name == "MSGPACK_TYPE_INT" and val < 0:
                    val_bytes = val.to_bytes(8, 'little', signed=True)
                else:
                    val_bytes = val.to_bytes(8, 'little', signed=False)
                node_bytes.append(", ".join(f"{b}" for b in val_bytes))
            elif node.type_name in ["MSGPACK_TYPE_DOUBLE", "MSGPACK_TYPE_FLOAT"]:
                import struct
                val = node.value if node.value is not None else 0.0
                val_bytes = struct.pack('<d', val)
                node_bytes.append(", ".join(f"{b}" for b in val_bytes))
            elif node.type_name in ["MSGPACK_TYPE_STR", "MSGPACK_TYPE_BIN"]:
                size = node.value if node.value is not None else 0
                node_bytes.append(f"{size & 0xFF}, {(size >> 8) & 0xFF}, {(size >> 16) & 0xFF}, {(size >> 24) & 0xFF}, ")
                node_bytes.append("0, 0, 0, 0")
            else:
                node_bytes.append("0, 0, 0, 0, 0, 0, 0, 0")
            
            lines.append("    " + "".join(node_bytes) + ",")
        
        if self.string_data:
            lines.append("\n    // ========== String Pool ==========")
            for i in range(0, len(self.string_data), 16):
                chunk = self.string_data[i:i+16]
                line = "    " + ", ".join(f"{b}" for b in chunk)
                if i + 16 < len(self.string_data):
                    line += ","
                lines.append(line)
        
        lines.append("};\n")
        lines.append(f"const size_t {self.var_name}_buffer_size = sizeof({self.var_name}_buffer);\n")
        lines.append(f"MsgPackArena {self.var_name}_arena;\n")
        
        lines.append(f"bool {self.var_name}_init(void) {{")
        lines.append(f"    return msgpack_arena_init(&{self.var_name}_arena,")
        lines.append(f"                              {self.var_name}_buffer,")
        lines.append(f"                              {self.var_name}_buffer_size);")
        lines.append("}\n")
        
        lines.append(f"const MsgPackNode* {self.var_name}_root(void) {{")
        lines.append(f"    return msgpack_arena_root(&{self.var_name}_arena);")
        lines.append("}")
        
        code = '\n'.join(lines)
        
        if isinstance(output, str):
            with open(output, 'w') as f:
                f.write(code)
        elif output is not None:
            output.write(code)
        
        return code
    
    def generate_data_only(self, output_dir: str = ".") -> Dict[str, str]:
        """
        Generate only data files (no runtime)
        
        Use this in production preprocessors with shared runtime.
        
        Args:
            output_dir: Output directory
            
        Returns:
            Dictionary mapping file type to file path
        """
        output_path = Path(output_dir)
        output_path.mkdir(parents=True, exist_ok=True)
        
        files = {
            'data_header': str(output_path / f"{self.var_name}_data.h"),
            'data_source': str(output_path / f"{self.var_name}_data.c"),
        }
        
        self.generate_data_header(files['data_header'])
        self.generate_data_source(files['data_source'])
        
        self._log(f"Generated data-only files for '{self.var_name}' in {output_dir}")
        
        return files
    
    def generate_runtime_only(self, output_dir: str = ".", 
                              runtime_name: str = None) -> Dict[str, str]:
        """
        Generate only runtime files (DEPRECATED - use RuntimeGenerator instead)
        
        Args:
            output_dir: Output directory
            runtime_name: Runtime file name (default: {var_name}_runtime)
            
        Returns:
            Dictionary mapping file type to file path
        """
        if runtime_name is None:
            runtime_name = f"{self.var_name}_runtime"
        
        runtime = RuntimeGenerator()
        runtime.merge(self)
        return runtime.generate(output_dir, runtime_name)
    
    def generate_all(self, output_dir: str = ".") -> Dict[str, str]:
        """
        Generate all files (data + runtime)
        
        For single-config use. For multiple configs, use generate_data_only()
        with shared RuntimeGenerator.
        
        Args:
            output_dir: Output directory
            
        Returns:
            Dictionary mapping file type to file path
        """
        # Generate data files
        files = self.generate_data_only(output_dir)
        
        # Generate runtime
        runtime_files = self.generate_runtime_only(output_dir, 
                                                   f"{self.var_name}_runtime")
        files.update(runtime_files)
        
        self._log(f"Generated all files for '{self.var_name}' in {output_dir}")
        
        return files
    
    def _get_type_enum(self, type_name: str) -> int:
        """Convert type name to enum value"""
        types = {
            "MSGPACK_TYPE_NIL": 0,
            "MSGPACK_TYPE_BOOL": 1,
            "MSGPACK_TYPE_INT": 2,
            "MSGPACK_TYPE_UINT": 3,
            "MSGPACK_TYPE_FLOAT": 4,
            "MSGPACK_TYPE_DOUBLE": 5,
            "MSGPACK_TYPE_STR": 6,
            "MSGPACK_TYPE_BIN": 7,
            "MSGPACK_TYPE_ARRAY": 8,
            "MSGPACK_TYPE_MAP": 9,
            "MSGPACK_TYPE_EXT": 10
        }
        return types.get(type_name, 0)


# ========== Command-line Interface ==========

def main():
    """Command-line interface"""
    import argparse
    
    parser = argparse.ArgumentParser(
        description='Generate C code from Python data structures for MessagePack arena allocator',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog='''
Examples:
  # Single config (generates data + runtime)
  %(prog)s config.json
  
  # Multiple configs (data only, shared runtime)
  %(prog)s config1.json -n config1 --data-only -o output/
  %(prog)s config2.json -n config2 --data-only -o output/
  %(prog)s --runtime-only -o output/ --runtime-name shared_runtime
  
  # From Python dict
  %(prog)s -d '{"key": "value"}' -n my_config
        '''
    )
    
    parser.add_argument('input', nargs='?',
                       help='Input file or Python dict with -d')
    parser.add_argument('-d', '--dict', action='store_true',
                       help='Treat input as Python dict/list string')
    parser.add_argument('-f', '--format', choices=['json', 'msgpack', 'auto'],
                       default='auto',
                       help='Input format (default: auto-detect)')
    parser.add_argument('-n', '--name', default='msgpack_data',
                       help='Variable name prefix (default: msgpack_data)')
    parser.add_argument('-o', '--output', default='.',
                       help='Output directory (default: current directory)')
    parser.add_argument('--data-only', action='store_true',
                       help='Generate only data files (no runtime)')
    parser.add_argument('--runtime-only', action='store_true',
                       help='Generate only runtime files (no data)')
    parser.add_argument('--runtime-name', default='msgpack_runtime',
                       help='Runtime file name (default: msgpack_runtime)')
    parser.add_argument('-v', '--verbose', action='store_true',
                       help='Verbose output')
    
    args = parser.parse_args()
    
    if args.runtime_only:
        # Generate empty runtime (user should provide config files to merge)
        print("Generating standalone runtime...")
        runtime = RuntimeGenerator()
        runtime.generate(args.output, args.runtime_name)
        print(f"Generated runtime: {args.runtime_name}.h, {args.runtime_name}.c")
        return
    
    if not args.input:
        parser.print_help()
        sys.exit(1)
    
    # Create generator
    gen = MsgPackCodeGenerator(args.name, verbose=args.verbose)
    
    # Load data
    try:
        if args.dict:
            import ast
            data = ast.literal_eval(args.input)
            gen.load_dict(data)
        elif args.format == 'json':
            gen.load_json(args.input)
        elif args.format == 'msgpack':
            gen.load_msgpack(args.input)
        else:
            gen.load_auto(args.input)
    except Exception as e:
        print(f"Error loading data: {e}", file=sys.stderr)
        sys.exit(1)
    
    # Generate files
    try:
        if args.data_only:
            files = gen.generate_data_only(args.output)
        else:
            files = gen.generate_all(args.output)
        
        gen.print_stats()
        
        print(f"\nGenerated files:")
        for file_type, filepath in files.items():
            print(f"  {filepath}")
        
        if not args.data_only:
            print(f"\nTo use in your code:")
            print(f'  #include "{args.name}_data.h"')
            print(f'  #include "{args.name}_runtime.h"')
            print(f"  {args.name}_init();")
        else:
            print(f"\nData files only. Generate shared runtime with:")
            print(f"  {sys.argv[0]} --runtime-only --runtime-name shared_runtime")
        
    except Exception as e:
        print(f"Error generating code: {e}", file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    main()

