#!/usr/bin/env python3
"""
json_record_encoder.py

Encode JSON structures into flat record arrays suitable for embedded C.
Generates .h files with const arrays that can be parsed at runtime.

Features:
  - String deduplication
  - Pre-order tree flattening
  - Multiple JSON objects in one file
  - ROM-friendly output (const arrays)

Usage:
  # As module:
  from json_record_encoder import JsonRecordEncoder
  
  encoder = JsonRecordEncoder()
  encoder.load('{"type": "sequence", "name": "root"}')
  encoder.load('{"type": "action", "name": "child"}')
  
  with open('tree_data.h', 'w') as f:
      f.write(encoder.generate_c_code())

  # As CLI:
  python json_record_encoder.py input.json -o output.h
  python json_record_encoder.py tree1.json tree2.json -o combined.h
"""

import json
import struct
import argparse
from typing import Any, Dict, List, Tuple


class JsonRecordEncoder:
    """
    Core JSON record encoder - converts JSON to flat record array.
    Maintains a shared string table for deduplication.
    """
    
    def __init__(self):
        self.init()
    
    def init(self) -> None:
        """Clear all structures."""
        self.string_table: Dict[str, int] = {}
        self.string_data: List[str] = []
        self.next_offset: int = 0
        self.records: List[Tuple[int, int]] = []
        self.record_controls: List[Dict[str, int]] = []
        self.node_count: int = 0  # Count of ChainTree nodes (dicts with "type" key)
    
    def add_string(self, s: str) -> int:
        """Add string to table, return offset. Deduplicates automatically."""
        if s in self.string_table:
            return self.string_table[s]
        
        offset = self.next_offset
        self.string_table[s] = offset
        self.string_data.append(s)
        self.next_offset += len(s.encode('utf-8')) + 1
        return offset
    
    def encode_value(self, value: Any) -> None:
        """Recursively encode a JSON value into the records list."""
        if value is None:
            self.records.append((3, 0))  # JSON_TYPE_NULL
        
        elif isinstance(value, bool):
            # MUST check bool before int (bool is subclass of int in Python)
            self.records.append((4, 1 if value else 0))  # JSON_TYPE_BOOL
        
        elif isinstance(value, str):
            offset = self.add_string(value)
            self.records.append((0, offset))  # JSON_TYPE_STRING
        
        elif isinstance(value, int):
            clamped = max(-2147483648, min(2147483647, value))
            self.records.append((1, clamped & 0xFFFFFFFF))  # JSON_TYPE_INT32
        
        elif isinstance(value, float):
            packed = struct.pack('f', value)
            uint_val = struct.unpack('I', packed)[0]
            self.records.append((2, uint_val))  # JSON_TYPE_FLOAT32
        
        elif isinstance(value, list):
            self.records.append((5, len(value)))  # JSON_TYPE_ARRAY
            for item in value:
                self.encode_value(item)
        
        elif isinstance(value, dict):
            # Count ChainTree nodes (dicts with "type" key)
            if "type" in value:
                self.node_count += 1
            
            # container_count = total children (keys + values) = 2 * N
            self.records.append((6, len(value) * 2))  # JSON_TYPE_OBJECT
            for key, val in value.items():
                key_offset = self.add_string(str(key))
                self.records.append((0, key_offset))
                self.encode_value(val)
        
        else:
            raise ValueError(f"Unsupported JSON type: {type(value)} with value: {repr(value)}")
        
    def load(self, json_string: str) -> int:
        """
        Load a JSON string and encode it. Returns record control index.
        Can be called multiple times to accumulate records with shared string table.
        """
        start_position = len(self.records)
        
        try:
            obj = json.loads(json_string)
            self.encode_value(obj)
        except json.JSONDecodeError as e:
            print(f"Warning: Skipping invalid JSON: {e}")
            return -1
        
        num_records = len(self.records) - start_position
        
        control = {
            'start_position': start_position,
            'num_records': num_records
        }
        self.record_controls.append(control)
        
        return len(self.record_controls) - 1
    
    def load_dict(self, obj: Dict) -> int:
        """
        Load a Python dict directly (skip JSON parsing). Returns record control index.
        """
        start_position = len(self.records)
        
        self.encode_value(obj)
        
        num_records = len(self.records) - start_position
        
        control = {
            'start_position': start_position,
            'num_records': num_records
        }
        self.record_controls.append(control)
        
        return len(self.record_controls) - 1
    
    def load_file(self, filepath: str) -> int:
        """Load JSON from file. Returns record control index."""
        with open(filepath, 'r') as f:
            content = f.read()
        return self.load(content)
    
    def generate_c_code(self, array_name: str = "json_records", 
                        string_table_name: str = "string_table",
                        control_name: str = "record_controls",
                        include_guard: str = None,
                        include_types: bool = False) -> str:
        """Generate C code for the record array, string table, and controls.
        
        Args:
            array_name: Name for the record array
            string_table_name: Name for the string table
            control_name: Name for the control array
            include_guard: Custom include guard (auto-generated if None)
            include_types: If True, include type definitions; if False, 
                          assume json_record_reader.h is included
        """
        
        if include_guard is None:
            include_guard = f"{array_name.upper()}_DATA_H"
        
        lines = []
        
        # Include guard and headers
        lines.append(f"#ifndef {include_guard}")
        lines.append(f"#define {include_guard}")
        lines.append("")
        lines.append("/* Auto-generated by json_record_encoder.py */")
        lines.append("/* Include json_record_reader.h before this file */")
        lines.append("")
        
        if include_types:
            # Generate type definitions (standalone mode)
            lines.append("#include <stdint.h>")
            lines.append("")
            lines.append("typedef enum {")
            lines.append("    JSON_TYPE_STRING = 0,")
            lines.append("    JSON_TYPE_INT32 = 1,")
            lines.append("    JSON_TYPE_FLOAT32 = 2,")
            lines.append("    JSON_TYPE_NULL = 3,")
            lines.append("    JSON_TYPE_BOOL = 4,")
            lines.append("    JSON_TYPE_ARRAY = 5,")
            lines.append("    JSON_TYPE_OBJECT = 6")
            lines.append("} json_type_t;")
            lines.append("")
            
            lines.append("typedef struct {")
            lines.append("    json_type_t object_type;")
            lines.append("    union {")
            lines.append("        uint32_t string_offset;")
            lines.append("        int32_t i32_value;")
            lines.append("        float f32_value;")
            lines.append("        uint8_t bool_value;")
            lines.append("        uint32_t container_count;")
            lines.append("    } value;")
            lines.append("} json_record_t;")
            lines.append("")
            
            lines.append("typedef struct {")
            lines.append("    uint32_t start_position;")
            lines.append("    uint32_t num_records;")
            lines.append("} record_control_t;")
            lines.append("")
        
        # Generate string table
        lines.append(f"/* String table: {len(self.string_data)} strings, {self.next_offset} bytes */")
        lines.append(f"static const char {string_table_name}[] = {{")
        string_bytes = []
        for s in self.string_data:
            encoded = s.encode('utf-8')
            for byte in encoded:
                string_bytes.append(f"0x{byte:02x}")
            string_bytes.append("0x00")
        
        for i in range(0, len(string_bytes), 16):
            chunk = string_bytes[i:i+16]
            line = "    " + ", ".join(chunk)
            if i + 16 < len(string_bytes):
                line += ","
            lines.append(line)
        lines.append("};")
        lines.append("")
        
        # Generate record array
        lines.append(f"/* Record array: {len(self.records)} records */")
        lines.append(f"static const json_record_t {array_name}[{len(self.records)}] = {{")
        
        type_names = ['JSON_TYPE_STRING', 'JSON_TYPE_INT32', 'JSON_TYPE_FLOAT32', 
                     'JSON_TYPE_NULL', 'JSON_TYPE_BOOL', 'JSON_TYPE_ARRAY', 'JSON_TYPE_OBJECT']
        
        for i, (type_tag, value) in enumerate(self.records):
            comment = ""
            if type_tag == 0:  # STRING
                # Find string for comment
                for s, off in self.string_table.items():
                    if off == value:
                        comment = f'  /* "{s[:20]}{"..." if len(s) > 20 else ""}" */'
                        break
                line = f"    {{.object_type = {type_names[type_tag]}, .value.string_offset = {value}}}"
            elif type_tag == 1:  # INT32
                signed_val = struct.unpack('i', struct.pack('I', value))[0]
                line = f"    {{.object_type = {type_names[type_tag]}, .value.i32_value = {signed_val}}}"
            elif type_tag == 2:  # FLOAT32
                float_val = struct.unpack('f', struct.pack('I', value))[0]
                line = f"    {{.object_type = {type_names[type_tag]}, .value.f32_value = {float_val}f}}"
            elif type_tag == 3:  # NULL
                line = f"    {{.object_type = {type_names[type_tag]}, .value.string_offset = 0}}"
            elif type_tag == 4:  # BOOL
                comment = f"  /* {'true' if value else 'false'} */"
                line = f"    {{.object_type = {type_names[type_tag]}, .value.bool_value = {value}}}"
            elif type_tag in [5, 6]:  # ARRAY/OBJECT
                count = value if type_tag == 5 else value // 2
                comment = f"  /* {count} {'elements' if type_tag == 5 else 'keys'} */"
                line = f"    {{.object_type = {type_names[type_tag]}, .value.container_count = {value}}}"
            
            if i < len(self.records) - 1:
                line += ","
            line += comment
            lines.append(line)
        lines.append("};")
        lines.append("")
        
        # Generate record controls (for sub-object access if multiple loaded)
        if len(self.record_controls) > 1:
            lines.append(f"/* Record controls: {len(self.record_controls)} objects */")
            lines.append(f"static const record_control_t {control_name}[{len(self.record_controls)}] = {{")
            for i, ctrl in enumerate(self.record_controls):
                line = f"    {{.start_position = {ctrl['start_position']}, .num_records = {ctrl['num_records']}}}"
                if i < len(self.record_controls) - 1:
                    line += ","
                lines.append(line)
            lines.append("};")
            lines.append("")
        
        # Generate unified data descriptor
        lines.append(f"/* Data descriptor - pass this to json_reader_init_from_data() */")
        # Use singular name (strip _controls suffix if present)
        data_name = array_name.replace('_records', '_data')
        lines.append(f"static const json_data_t {data_name} = {{")
        lines.append(f"    .records = {array_name},")
        lines.append(f"    .strings = {string_table_name},")
        lines.append(f"    .record_count = {len(self.records)},")
        lines.append(f"    .string_size = {self.next_offset},")
        lines.append(f"    .node_count = {self.node_count}")
        lines.append("};")
        lines.append("")
        
        # Generate size constants (keep for backward compatibility)
        lines.append(f"#define {array_name.upper()}_COUNT {len(self.records)}")
        lines.append(f"#define {string_table_name.upper()}_SIZE {self.next_offset}")
        lines.append(f"#define {data_name.upper()}_NODE_COUNT {self.node_count}")
        if len(self.record_controls) > 1:
            lines.append(f"#define {control_name.upper()}_COUNT {len(self.record_controls)}")
        lines.append("")
        lines.append(f"#endif /* {include_guard} */")
        
        return "\n".join(lines)
    
    def format_record_value(self, type_tag: int, value: int) -> str:
        """Format a record value for display based on its type."""
        type_names = ['STRING', 'INT32', 'FLOAT32', 'NULL', 'BOOL', 'ARRAY', 'OBJECT']
        
        if type_tag == 0:  # STRING
            for s, off in self.string_table.items():
                if off == value:
                    return f'{type_names[type_tag]:7s} = "{s}"'
            return f'{type_names[type_tag]:7s} = @{value}'
        elif type_tag == 1:  # INT32
            signed_val = struct.unpack('i', struct.pack('I', value))[0]
            return f'{type_names[type_tag]:7s} = {signed_val}'
        elif type_tag == 2:  # FLOAT32
            float_val = struct.unpack('f', struct.pack('I', value))[0]
            return f'{type_names[type_tag]:7s} = {float_val}'
        elif type_tag == 4:  # BOOL
            return f'{type_names[type_tag]:7s} = {bool(value)}'
        elif type_tag in [5, 6]:  # ARRAY/OBJECT
            return f'{type_names[type_tag]:7s} = count:{value}'
        else:  # NULL
            return f'{type_names[type_tag]:7s} = null'
    
    def dump_records(self) -> None:
        """Print records for debugging."""
        print(f"Records: {len(self.records)}")
        print("-" * 60)
        for i, (type_tag, value) in enumerate(self.records):
            print(f"  [{i:4d}] {self.format_record_value(type_tag, value)}")
        print("-" * 60)
        print(f"Strings: {len(self.string_data)}")
        for s, off in self.string_table.items():
            print(f"  @{off}: \"{s}\"")
    
    def get_stats(self) -> Dict[str, Any]:
        """Return encoding statistics."""
        return {
            'total_records': len(self.records),
            'total_objects_loaded': len(self.record_controls),
            'unique_strings': len(self.string_table),
            'string_table_bytes': self.next_offset,
            'node_count': self.node_count
        }
    
    def generate_binary(self) -> bytes:
        """Generate binary file format for runtime loading.
        
        Binary format (little-endian):
          Header (24 bytes):
            uint32_t magic          = 0x4A534F4E ("JSON")
            uint32_t version        = 1
            uint32_t record_count
            uint32_t string_size
            uint32_t control_count
            uint32_t node_count
          String table:
            char[string_size]       null-terminated strings
          Records:
            json_record_t[record_count]  (8 bytes each: type + value)
          Controls:
            record_control_t[control_count]  (8 bytes each: start + count)
        """
        import struct
        
        parts = []
        
        # Header
        magic = 0x4A534F4E  # "JSON"
        version = 1
        header = struct.pack('<IIIIII', 
                            magic, 
                            version,
                            len(self.records),
                            self.next_offset,
                            len(self.record_controls),
                            self.node_count)
        parts.append(header)
        
        # String table
        string_bytes = bytearray()
        for s in self.string_data:
            string_bytes.extend(s.encode('utf-8'))
            string_bytes.append(0)  # null terminator
        parts.append(bytes(string_bytes))
        
        # Records (each is 8 bytes: uint32 type + uint32 value)
        for type_tag, value in self.records:
            parts.append(struct.pack('<II', type_tag, value))
        
        # Controls (each is 8 bytes: uint32 start + uint32 count)
        for ctrl in self.record_controls:
            parts.append(struct.pack('<II', ctrl['start_position'], ctrl['num_records']))
        
        return b''.join(parts)
    
    def save_binary(self, filepath: str) -> None:
        """Save binary format to file."""
        data = self.generate_binary()
        with open(filepath, 'wb') as f:
            f.write(data)
        print(f"Saved binary: {filepath} ({len(data)} bytes)")


def main():
    parser = argparse.ArgumentParser(
        description='Encode JSON files into C header with flat record arrays.',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  %(prog)s tree.json -o tree_data.h
  %(prog)s tree1.json tree2.json -o combined.h --name my_tree
  %(prog)s data.json --dump  # Debug output only
  
Names are derived from input filename by default:
  example_tree.json -> example_tree, example_tree_strings, example_tree_controls
        """
    )
    parser.add_argument('inputs', nargs='+', help='Input JSON file(s)')
    parser.add_argument('-o', '--output', help='Output .h file (stdout if not specified)')
    parser.add_argument('-n', '--name', default=None, 
                        help='Base name for generated arrays (default: derived from first input filename)')
    parser.add_argument('--dump', action='store_true', 
                        help='Dump records to stdout instead of generating C')
    parser.add_argument('--stats', action='store_true',
                        help='Print encoding statistics')
    parser.add_argument('--include-types', action='store_true',
                        help='Include type definitions (for standalone use without json_record_reader.h)')
    parser.add_argument('--binary', '-b', metavar='FILE',
                        help='Output binary file for runtime loading (instead of or in addition to .h)')
    
    args = parser.parse_args()
    
    encoder = JsonRecordEncoder()
    
    # Derive base name from first input filename if not specified
    if args.name is None:
        import os
        base = os.path.basename(args.inputs[0])  # "example_tree.json"
        base = os.path.splitext(base)[0]          # "example_tree"
        # Sanitize: replace non-alphanumeric with underscore
        args.name = ''.join(c if c.isalnum() else '_' for c in base)
    
    for filepath in args.inputs:
        print(f"Loading: {filepath}")
        idx = encoder.load_file(filepath)
        if idx < 0:
            print(f"  Warning: Failed to load {filepath}")
        else:
            stats = encoder.get_stats()
            print(f"  Loaded object {idx}: {stats['total_records']} records")
    
    if args.stats:
        stats = encoder.get_stats()
        print("\nStatistics:")
        print(f"  Total records:     {stats['total_records']}")
        print(f"  Objects loaded:    {stats['total_objects_loaded']}")
        print(f"  Unique strings:    {stats['unique_strings']}")
        print(f"  String table size: {stats['string_table_bytes']} bytes")
    
    if args.dump:
        print("\nRecord dump:")
        encoder.dump_records()
        return
    
    # Generate binary file if requested
    if args.binary:
        encoder.save_binary(args.binary)
    
    # Generate C code (if output specified)
    if args.output:
        guard = f"{args.name.upper()}_DATA_H"
        c_code = encoder.generate_c_code(
            array_name=f"{args.name}_records",
            string_table_name=f"{args.name}_strings",
            control_name=f"{args.name}_controls",
            include_guard=guard,
            include_types=args.include_types
        )
        
        with open(args.output, 'w') as f:
            f.write(c_code)
        stats = encoder.get_stats()
        print(f"\nGenerated: {args.output}")
        print(f"  {stats['total_records']} records, {stats['string_table_bytes']} string bytes")
    elif not args.binary:
        # Output to stdout only if no binary and no output file
        guard = f"{args.name.upper()}_DATA_H"
        c_code = encoder.generate_c_code(
            array_name=f"{args.name}_records",
            string_table_name=f"{args.name}_strings",
            control_name=f"{args.name}_controls",
            include_guard=guard,
            include_types=args.include_types
        )
        print("\n" + c_code)


if __name__ == "__main__":
    main()