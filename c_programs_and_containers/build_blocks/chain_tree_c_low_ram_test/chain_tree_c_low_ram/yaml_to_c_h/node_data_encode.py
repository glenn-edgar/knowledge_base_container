"""
Node Data Encoder for ChainTree Pipeline

This module encodes node data from ChainTree YAML into compact JSON record format
suitable for embedded systems. Integrates with the header file generation pipeline.
"""

import json
import struct
from pathlib import Path
from typing import List, Dict, Any, Tuple, Optional


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
            # CRITICAL FIX: container_count must be total children (keys + values)
            # For N key-value pairs, we have N keys + N values = 2*N children
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
    
    def generate_c_code(self, array_name: str = "json_records", 
                        string_table_name: str = "string_table",
                        control_name: str = "record_controls") -> str:
        """Generate C code segments for the record array, string table, and controls."""
        
        lines = []
        
        # Generate header comment
        lines.append("/* Auto-generated JSON record structures */")
        lines.append("")
        
        # Generate type definitions
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
        
        # Generate record array
        lines.append(f"const json_record_t {array_name}[{len(self.records)}] = {{")
        for i, (type_tag, value) in enumerate(self.records):
            type_names = ['JSON_TYPE_STRING', 'JSON_TYPE_INT32', 'JSON_TYPE_FLOAT32', 
                         'JSON_TYPE_NULL', 'JSON_TYPE_BOOL', 'JSON_TYPE_ARRAY', 'JSON_TYPE_OBJECT']
            
            if type_tag == 0:  # STRING
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
                line = f"    {{.object_type = {type_names[type_tag]}, .value.bool_value = {value}}}"
            elif type_tag in [5, 6]:  # ARRAY/OBJECT
                line = f"    {{.object_type = {type_names[type_tag]}, .value.container_count = {value}}}"
            
            if i < len(self.records) - 1:
                line += ","
            lines.append(line)
        lines.append("};")
        lines.append("")
        
        # Generate string table
        lines.append(f"const char {string_table_name}[] = {{")
        string_bytes = []
        for s in self.string_data:
            # Convert to C string literal bytes
            encoded = s.encode('utf-8')
            for byte in encoded:
                string_bytes.append(f"0x{byte:02x}")
            string_bytes.append("0x00")  # null terminator
        
        # Format as rows of 12 bytes
        for i in range(0, len(string_bytes), 12):
            chunk = string_bytes[i:i+12]
            line = "    " + ", ".join(chunk)
            if i + 12 < len(string_bytes):
                line += ","
            lines.append(line)
        lines.append("};")
        lines.append("")
        
        # Generate record controls
        lines.append(f"const record_control_t {control_name}[{len(self.record_controls)}] = {{")
        for i, ctrl in enumerate(self.record_controls):
            line = f"    {{.start_position = {ctrl['start_position']}, .num_records = {ctrl['num_records']}}}"
            if i < len(self.record_controls) - 1:
                line += ","
            lines.append(line)
        lines.append("};")
        lines.append("")
        
        # Generate size constants
        lines.append(f"#define {array_name.upper()}_COUNT {len(self.records)}")
        lines.append(f"#define {string_table_name.upper()}_SIZE {self.next_offset}")
        lines.append(f"#define {control_name.upper()}_COUNT {len(self.record_controls)}")
        
        return "\n".join(lines)
    
    def format_record_value(self, type_tag: int, value: int) -> str:
        """Format a record value for display based on its type."""
        type_names = ['STRING', 'INT32', 'FLOAT32', 'NULL', 'BOOL', 'ARRAY', 'OBJECT']
        
        if type_tag == 0:  # STRING
            str_val = self.string_data[value] if value < len(self.string_data) else f"@{value}"
            return f'{type_names[type_tag]:7s} = "{str_val}"'
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
    
    def get_stats(self) -> Dict[str, Any]:
        """Return encoding statistics."""
        return {
            'total_records': len(self.records),
            'total_objects_loaded': len(self.record_controls),
            'unique_strings': len(self.string_table),
            'string_table_bytes': self.next_offset
        }


class NodeDataEncoder:
    """
    Pipeline Stage 6: Encode node data from ChainTree YAML.
    
    Extracts node data from YAML and encodes it into JSON record format
    for efficient storage and access in embedded systems.
    """
    
    def __init__(self, handle, node_builder, function_builder):
        """
        Initialize NodeDataEncoder.
        
        Args:
            handle: ChainTreeYamlHandle instance
            node_builder: NodeIndexBuilder instance
            function_builder: FunctionIndexBuilder instance (for function resolution)
        """
        self.handle = handle
        self.node_builder = node_builder
        self.function_builder = function_builder
        
        # JSON record encoder
        self.encoder = JsonRecordEncoder()
        
        # Mapping from ltree_name to data_id
        self.node_data_ids: Dict[str, int] = {}
        
        # Configure which fields should be resolved to function IDs
        # Format: {field_name: function_type}
        # function_type can be: 'one_shot', 'main', 'boolean'
        self.function_fields = {
            'error_function': 'one_shot',
            'boolean_function': 'boolean',
            'finalize_function': 'one_shot',
           
    }
        
    def _process_node_reference_fields(self, data_dict: Dict) -> Dict:
        """
        Process fields that contain node references (ltree paths) and convert them to indices.
        
        Fields that should be converted:
        - sm_node_id: State machine node reference
        - target_node_id: Target node for transitions
        - Any field ending in '_node_id', '_node_ref', or '_node_name'
        
        Args:
            data_dict: Dictionary that may contain node reference fields
            
        Returns:
            Modified dictionary with ltree paths converted to node indices
        """
        result = data_dict.copy()
        
        # List of field name patterns that contain node references
        node_ref_patterns = [
            'sm_node_id',
            'target_node_id',
            'parent_node_id',
            'parent_node_name',  # Add this explicitly
            'next_node_id',
            'prev_node_id',
        ]
        
        for key, value in data_dict.items():
            # Check if this field contains a node reference
            # Added '_node_name' to the suffix checks
            if (key in node_ref_patterns or 
                key.endswith('_node_id') or 
                key.endswith('_node_ref') or
                key.endswith('_node_name')):
                
                # Value should be an ltree path string
                if isinstance(value, str) and value.startswith('kb.'):
                    # Look up the node index
                    if value in self.node_builder.ltree_to_final_index:
                        node_index = self.node_builder.get_node_final_index(value)
                        result[key] = node_index
                        print(f"    Resolved {key}: '{value}' -> node_index={node_index}")
                    else:
                        # Node not found or was filtered
                        print(f"    WARNING: {key} references unknown/filtered node: '{value}'")
                        result[key] = 0xFFFF  # Invalid node reference
            
            # Recursively process nested dictionaries
            elif isinstance(value, dict):
                result[key] = self._process_node_reference_fields(value)
            
            # Process lists that might contain dicts
            elif isinstance(value, list):
                result[key] = [
                    self._process_node_reference_fields(item) if isinstance(item, dict) else item
                    for item in value
                ]
        
        return result
            
    def generate_c_arrays(self, lines: List[str], unique_id: str) -> None:
        """
        Generate C array definitions with unique_id prefix.
        
        Args:
            lines: List to append generated lines to
            unique_id: Unique identifier for name mangling
        """
        
        # Generate records array
        if self.encoder.records:
            lines.append("/* JSON records array */")
            lines.append(f"const json_record_t {unique_id}_node_data_records[{len(self.encoder.records)}] = {{")
            
            for i, (type_tag, value) in enumerate(self.encoder.records):
                type_names = ['JSON_TYPE_STRING', 'JSON_TYPE_INT32', 'JSON_TYPE_FLOAT32', 
                            'JSON_TYPE_NULL', 'JSON_TYPE_BOOL', 'JSON_TYPE_ARRAY', 'JSON_TYPE_OBJECT']
                
                if type_tag == 0:  # STRING
                    line = f"    {{ .object_type = {type_names[type_tag]}, .value = {{ .string_offset = {value} }} }}"
                elif type_tag == 1:  # INT32
                    signed_val = struct.unpack('i', struct.pack('I', value))[0]
                    line = f"    {{ .object_type = {type_names[type_tag]}, .value = {{ .i32_value = {signed_val} }} }}"
                elif type_tag == 2:  # FLOAT32
                    float_val = struct.unpack('f', struct.pack('I', value))[0]
                    line = f"    {{ .object_type = {type_names[type_tag]}, .value = {{ .f32_value = {float_val}f }} }}"
                elif type_tag == 3:  # NULL
                    line = f"    {{ .object_type = {type_names[type_tag]}, .value = {{ .i32_value = 0 }} }}"
                elif type_tag == 4:  # BOOL
                    line = f"    {{ .object_type = {type_names[type_tag]}, .value = {{ .bool_value = {value} }} }}"
                elif type_tag in [5, 6]:  # ARRAY/OBJECT
                    line = f"    {{ .object_type = {type_names[type_tag]}, .value = {{ .container_count = {value} }} }}"
                
                if i < len(self.encoder.records) - 1:
                    line += ","
                lines.append(line)
            
            lines.append("};")
            lines.append("")
        
        # Generate strings array
        if self.encoder.string_data:
            # Build the string buffer
            string_bytes = []
            for s in self.encoder.string_data:
                encoded = s.encode('utf-8')
                for byte in encoded:
                    string_bytes.append(byte)
                string_bytes.append(0)  # null terminator
            
            lines.append("/* String data buffer */")
            lines.append(f"const char {unique_id}_node_data_strings[{len(string_bytes)}] = {{")
            
            # Format string buffer in chunks for readability
            chunk_size = 16
            for i in range(0, len(string_bytes), chunk_size):
                chunk = string_bytes[i:i+chunk_size]
                hex_values = ', '.join(f'0x{b:02x}' for b in chunk)
                
                if i + chunk_size < len(string_bytes):
                    lines.append(f"    {hex_values},")
                else:
                    lines.append(f"    {hex_values}")
            
            lines.append("};")
            lines.append("")
        
        # Generate controls array
        if self.encoder.record_controls:
            lines.append("/* Record control array (maps node data IDs to record ranges) */")
            lines.append(f"const record_control_t {unique_id}_node_data_controls[{len(self.encoder.record_controls)}] = {{")
            
            for i, control in enumerate(self.encoder.record_controls):
                start_pos = control['start_position']
                num_records = control['num_records']
                
                if i < len(self.encoder.record_controls) - 1:
                    lines.append(f"    {{ .start_position = {start_pos}, .num_records = {num_records} }},")
                else:
                    lines.append(f"    {{ .start_position = {start_pos}, .num_records = {num_records} }}")
            
            lines.append("};")
            lines.append("")
        
    def _has_meaningful_data(self, data: Any) -> bool:
        """
        Check if data contains meaningful (non-null) values.
        
        Returns False if data is:
        - None
        - Empty dict/list
        - Dict with only null values
        - List with only null values
        """
        if data is None:
            return False
        if isinstance(data, dict):
            # Check if all values are null/None or if dict is empty
            if not data:
                return False
            return any(self._has_meaningful_data(v) for v in data.values())
        if isinstance(data, (list, tuple)):
            # Check if list is empty or all elements are null
            if not data:
                return False
            return any(self._has_meaningful_data(v) for v in data)
        # String, number, bool - these are meaningful
        return True
    
    def _resolve_function_field(self, func_name: str, func_type: str) -> Optional[int]:
        """
        Resolve a function name to its index, adding it if not present.
        
        Args:
            func_name: Function name (e.g., "Handle_Error")
            func_type: Function type: 'main', 'one_shot', or 'boolean'
            
        Returns:
            Function index, or None if invalid type
        """
        if func_type == 'one_shot':
            indexer = self.function_builder.one_shot_indexer
            # Try to get existing index, add if not found
            try:
                return indexer.get_index(func_name)
            except KeyError:
                indexer.add_function(func_name)
                return indexer.get_index(func_name)
                
        elif func_type == 'main':
            indexer = self.function_builder.main_indexer
            try:
                return indexer.get_index(func_name)
            except KeyError:
                indexer.add_function(func_name)
                return indexer.get_index(func_name)
                
        elif func_type == 'boolean':
            indexer = self.function_builder.boolean_indexer
            try:
                return indexer.get_index(func_name)
            except KeyError:
                indexer.add_function(func_name)
                return indexer.get_index(func_name)
        else:
            print(f"  Warning: Unknown function type '{func_type}' for field resolution")
            return None
    
    def _process_function_fields(self, data_dict: Dict[str, Any]) -> Dict[str, Any]:
        """
        Process a dictionary to resolve function name fields to function IDs.
        
        Args:
            data_dict: Dictionary that may contain function name fields
            
        Returns:
            Modified dictionary with function names replaced by IDs
        """
        result = {}
        
        for key, value in data_dict.items():
            # Check if this field should be resolved to a function ID
            if key in self.function_fields and isinstance(value, str):
                func_type = self.function_fields[key]
                func_id = self._resolve_function_field(value, func_type)
                
                if func_id is not None:
                    # Store as "{field_name}_id" with integer value
                    result[f"{key}_id"] = func_id
                    print(f"    Resolved {key} '{value}' -> ID {func_id}")
                else:
                    # Failed to resolve, keep original
                    result[key] = value
            # Recursively process nested dictionaries
            elif isinstance(value, dict):
                result[key] = self._process_function_fields(value)
            # Process lists that might contain dicts
            elif isinstance(value, list):
                result[key] = [
                    self._process_function_fields(item) if isinstance(item, dict) else item
                    for item in value
                ]
            else:
                # Not a function field, keep as-is
                result[key] = value
        
        return result
    
    def encode_node_data(self) -> None:
        """
        Encode data for all nodes.
        
        Only encodes operational runtime data, NOT metadata:
        - Metadata (node_name, node_type) is excluded - not used at runtime
        - auto_start is excluded (already encoded in link_count bit 15)
        - Function name fields are resolved to function IDs
        - Node reference fields (sm_node_id, etc.) are resolved to node indices
        - Only encode node_dict if it has meaningful data after filtering
        - Only encode other operational fields (timeout, priority, config, etc.) if present
        """
        
        nodes_with_data = 0
        nodes_skipped = 0
        
        for ltree_name in self.node_builder.ltree_to_final_index.keys():
            # Skip filtered metadata nodes (defensive check - shouldn't be in final list)
            if ltree_name in self.node_builder.filtered_nodes:
                continue
            
            # Get node data from handle
            node_data = self.handle.get_node_data(ltree_name)
            
            if not node_data:
                # No data for this node, assign invalid data ID
                self.node_data_ids[ltree_name] = 0xFFFF
                nodes_skipped += 1
                continue
            
            # Extract ONLY operational runtime fields (exclude metadata)
            encode_data = {}
            
            # Include custom data fields (if operationally used)
            if 'data' in node_data and self._has_meaningful_data(node_data['data']):
                encode_data['data'] = node_data['data']
            
            # Handle node_dict specially - exclude auto_start (already in link_count)
            # and resolve function name fields to IDs and node references to indices
            if 'node_dict' in node_data:
                node_dict = node_data['node_dict']
                if node_dict and isinstance(node_dict, dict):
                    # Create a copy without auto_start
                    filtered_dict = {k: v for k, v in node_dict.items() if k != 'auto_start'}
                    
                    # Resolve function name fields to IDs
                    if filtered_dict:
                        filtered_dict = self._process_function_fields(filtered_dict)
                    
                    # Resolve node reference fields to indices
                    if filtered_dict:
                        filtered_dict = self._process_node_reference_fields(filtered_dict)
                    
                    # Only include if there's meaningful data remaining
                    if self._has_meaningful_data(filtered_dict):
                        encode_data['node_dict'] = filtered_dict
            
            # Include other operational fields (if present and meaningful)
            for key in ['timeout', 'priority', 'config', 'parameters']:
                if key in node_data and self._has_meaningful_data(node_data[key]):
                    # Also process node references in these fields
                    field_data = node_data[key]
                    if isinstance(field_data, dict):
                        field_data = self._process_node_reference_fields(field_data)
                    encode_data[key] = field_data
            
            # Encode the data and get the record control index
            if encode_data:
                data_id = self.encoder.load_dict(encode_data)
                self.node_data_ids[ltree_name] = data_id
                nodes_with_data += 1
                # Debug: show first few nodes with data
                if nodes_with_data <= 5:
                    node_name = node_data.get('node_name', ltree_name)
                    print(f"  Encoding node [{nodes_with_data}] '{node_name}': {list(encode_data.keys())}")
                    for key, val in encode_data.items():
                        print(f"    {key}: {val}")
            else:
                # No operational data, use invalid ID
                self.node_data_ids[ltree_name] = 0xFFFF
                nodes_skipped += 1
                # Debug: show first few skipped nodes
                if nodes_skipped <= 5:
                    node_name = node_data.get('node_name', ltree_name)
                    print(f"  Skipped node '{node_name}': no operational data")
        
        print(f"\n  Summary: {nodes_with_data} nodes with data, {nodes_skipped} nodes skipped")

            
    def get_node_data_id(self, ltree_name: str) -> int:
        """Get the data ID for a node."""
        return self.node_data_ids.get(ltree_name, 0xFFFF)
    
    def get_records_count(self) -> int:
        """Get total number of JSON records."""
        return len(self.encoder.records)
    
    def get_strings_size(self) -> int:
        """Get total size of string table in bytes."""
        return self.encoder.next_offset
    
    def get_controls_count(self) -> int:
        """Get total number of record controls."""
        return len(self.encoder.record_controls)
    
    def print_summary(self) -> None:
        """Print summary of node data encoding."""
        stats = self.encoder.get_stats()
        
        print("=" * 70)
        print("Node Data Encoder Summary")
        print("=" * 70)
        print(f"Nodes with data: {len([d for d in self.node_data_ids.values() if d != 0xFFFF])}")
        print(f"Total JSON records: {stats['total_records']}")
        print(f"Unique strings: {stats['unique_strings']}")
        print(f"String table size: {stats['string_table_bytes']} bytes")
    
    def generate_c_header(self, output_file: Path) -> None:
        """Generate C header file with node data structures."""
        
        c_code = self.encoder.generate_c_code(
            array_name="node_data_records",
            string_table_name="node_data_strings",
            control_name="node_data_controls"
        )
        
        # Wrap in header guards
        lines = [
            "/* Auto-generated by ChainTree Pipeline */",
            "#ifndef CHAINTREE_NODE_DATA_H",
            "#define CHAINTREE_NODE_DATA_H",
            "",
            "#include <stdint.h>",
            "#include <stdbool.h>",
            "",
            c_code,
            "",
            "#endif /* CHAINTREE_NODE_DATA_H */"
        ]
        
        with open(output_file, 'w') as f:
            f.write("\n".join(lines))


# Standalone test
if __name__ == '__main__':
    print("Testing JsonRecordEncoder standalone...")
    
    encoder = JsonRecordEncoder()
    encoder.init()
    
    # Load JSON objects incrementally
    print(f"Object 0: {encoder.load('{\"temperature\": 23.5, \"sensors\": [1, 2, 3]}')}")
    print(f"Object 1: {encoder.load('{\"config\": {\"timeout\": 30, \"retry\": true}}')}")
    print(f"Object 2: {encoder.load('{\"data\": [{\"id\": 1, \"name\": \"sensor_a\"}, {\"id\": 2, \"name\": \"sensor_b\"}]}')}")
    print(f"Object 3: {encoder.load('{\"temperature\": 24.1}')}")  # "temperature" string reused
    print(f"Object 4: {encoder.load('{\"matrix\": [[1, 2], [3, 4]]}')}")
    
    print(f"\nStats: {encoder.get_stats()}")
    
    # Show record controls
    print(f"\nRecord Controls:")
    for i, ctrl in enumerate(encoder.record_controls):
        print(f"  [{i}] start={ctrl['start_position']}, count={ctrl['num_records']}")
    
    # Show records for first object
    print(f"\nFirst object records (indices {encoder.record_controls[0]['start_position']} to "
          f"{encoder.record_controls[0]['start_position'] + encoder.record_controls[0]['num_records'] - 1}):")
    start = encoder.record_controls[0]['start_position']
    end = start + encoder.record_controls[0]['num_records']
    for i in range(start, end):
        type_tag, value = encoder.records[i]
        formatted = encoder.format_record_value(type_tag, value)
        print(f"  [{i:2d}] {formatted}")
    
    # Generate C code
    print("\n" + "="*70)
    print("GENERATED C CODE:")
    print("="*70)
    c_code = encoder.generate_c_code()
    print(c_code)
    
    # Optionally write to file
    with open('json_records.h', 'w') as f:
        f.write(c_code)
    print("\nC code written to json_records.h")