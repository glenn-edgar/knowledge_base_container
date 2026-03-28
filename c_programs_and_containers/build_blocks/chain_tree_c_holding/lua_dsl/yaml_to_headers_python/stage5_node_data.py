"""
Stage 5: Node Data Encoder

Encodes node data from ChainTree YAML into compact JSON record format
suitable for embedded systems. Uses JsonRecordEncoder for the low-level
encoding and provides high-level node processing with:
  - Function field resolution (name -> index)
  - Node reference resolution (ltree path -> node index)
  - Metadata filtering (exclude non-runtime fields)
"""

import json
import struct
from pathlib import Path
from typing import List, Dict, Any, Tuple, Optional

from .stage1_handle import ChainTreeHandle
from .stage2_node_index import NodeIndexBuilder
from .stage3_function_index import FunctionIndexBuilder


class JsonRecordEncoder:
    """
    Core JSON record encoder - converts JSON to flat record array.
    Maintains a shared string table for deduplication.
    
    Type tags:
      0 = STRING, 1 = INT32, 2 = FLOAT32, 3 = NULL, 4 = BOOL, 5 = ARRAY, 6 = OBJECT
    """
    
    def __init__(self):
        self.init()
    
    def init(self) -> None:
        self.string_table: Dict[str, int] = {}
        self.string_data: List[str] = []
        self.next_offset: int = 0
        self.records: List[Tuple[int, int]] = []
        self.record_controls: List[Dict[str, int]] = []
    
    def add_string(self, s: str) -> int:
        if s in self.string_table:
            return self.string_table[s]
        offset = self.next_offset
        self.string_table[s] = offset
        self.string_data.append(s)
        self.next_offset += len(s.encode('utf-8')) + 1
        return offset
    
    def encode_value(self, value: Any) -> None:
        if value is None:
            self.records.append((3, 0))
        elif isinstance(value, bool):
            self.records.append((4, 1 if value else 0))
        elif isinstance(value, str):
            offset = self.add_string(value)
            self.records.append((0, offset))
        elif isinstance(value, int):
            clamped = max(-2147483648, min(2147483647, value))
            self.records.append((1, clamped & 0xFFFFFFFF))
        elif isinstance(value, float):
            packed = struct.pack('f', value)
            uint_val = struct.unpack('I', packed)[0]
            self.records.append((2, uint_val))
        elif isinstance(value, list):
            self.records.append((5, len(value)))
            for item in value:
                self.encode_value(item)
        elif isinstance(value, dict):
            self.records.append((6, len(value) * 2))
            for key, val in value.items():
                key_offset = self.add_string(str(key))
                self.records.append((0, key_offset))
                self.encode_value(val)
        else:
            raise ValueError(f"Unsupported JSON type: {type(value)} with value: {repr(value)}")
    
    def load(self, json_string: str) -> int:
        """Load a JSON string and encode it. Returns record control index."""
        start_position = len(self.records)
        try:
            obj = json.loads(json_string)
            self.encode_value(obj)
        except json.JSONDecodeError as e:
            print(f"Warning: Skipping invalid JSON: {e}")
            return -1
        
        num_records = len(self.records) - start_position
        control = {'start_position': start_position, 'num_records': num_records}
        self.record_controls.append(control)
        return len(self.record_controls) - 1
    
    def load_dict(self, obj: Dict) -> int:
        """Load a Python dict directly. Returns record control index."""
        start_position = len(self.records)
        self.encode_value(obj)
        num_records = len(self.records) - start_position
        control = {'start_position': start_position, 'num_records': num_records}
        self.record_controls.append(control)
        return len(self.record_controls) - 1
    
    def get_stats(self) -> Dict[str, Any]:
        return {
            'total_records': len(self.records),
            'total_objects_loaded': len(self.record_controls),
            'unique_strings': len(self.string_table),
            'string_table_bytes': self.next_offset
        }


class NodeDataEncoder:
    """
    Stage 5: Encode node data from ChainTree YAML.
    
    Extracts operational runtime data, resolves function/node references,
    encodes into JSON record format for embedded systems.
    """
    
    def __init__(self, handle: ChainTreeHandle, node_builder: NodeIndexBuilder,
                 function_builder: FunctionIndexBuilder):
        self.handle = handle
        self.node_builder = node_builder
        self.function_builder = function_builder
        
        self.encoder = JsonRecordEncoder()
        self.node_data_ids: Dict[str, int] = {}
        
        # Fields that should be resolved to function IDs: {field_name: function_type}
        self.function_fields = {
            'error_function': 'one_shot',
            'boolean_function': 'boolean',
            'finalize_function': 'one_shot',
            'initialize_function': 'one_shot',
            'wd_fn': 'one_shot',
            'logging_function': 'one_shot',
            'user_aux_function': 'boolean',
        }
    
    # =========================================================================
    # Internal Processing
    # =========================================================================
    
    def _has_meaningful_data(self, data: Any) -> bool:
        if data is None:
            return False
        if isinstance(data, dict):
            if not data:
                return False
            return any(self._has_meaningful_data(v) for v in data.values())
        if isinstance(data, (list, tuple)):
            if not data:
                return False
            return any(self._has_meaningful_data(v) for v in data)
        return True
    
    def _resolve_function_field(self, func_name: str, func_type: str) -> Optional[int]:
        indexer_map = {
            'one_shot': self.function_builder.one_shot_indexer,
            'main': self.function_builder.main_indexer,
            'boolean': self.function_builder.boolean_indexer,
        }
        indexer = indexer_map.get(func_type)
        if not indexer:
            print(f"  Warning: Unknown function type '{func_type}'")
            return None
        
        try:
            return indexer.get_index(func_name)
        except KeyError:
            indexer.add_function(func_name)
            return indexer.get_index(func_name)
    
    def _process_function_fields(self, data_dict: Dict[str, Any], depth: int = 0) -> Dict[str, Any]:
        """Resolve function name fields to function IDs."""
        indent = "  " * depth
        print(f"{indent}_process_function_fields called with keys: {list(data_dict.keys())}")
        
        result = {}
        for key, value in data_dict.items():
            if key in self.function_fields and isinstance(value, str):
                func_type = self.function_fields[key]
                print(f"{indent}  FOUND function field: {key} = '{value}' (type: {func_type})")
                func_id = self._resolve_function_field(value, func_type)
                if func_id is not None:
                    result[f"{key}_id"] = func_id
                    print(f"{indent}    Resolved {key} '{value}' -> ID {func_id}")
                else:
                    result[key] = value
            elif isinstance(value, dict):
                print(f"{indent}  Recursing into: {key}")
                result[key] = self._process_function_fields(value, depth + 1)
            elif isinstance(value, list):
                result[key] = [
                    self._process_function_fields(item, depth + 1) if isinstance(item, dict) else item
                    for item in value
                ]
            else:
                result[key] = value
        
        return result
    
    def _process_node_reference_fields(self, data_dict: Dict) -> Dict:
        """Resolve ltree path fields to node indices."""
        result = data_dict.copy()
        
        node_ref_patterns = [
            'sm_node_id', 'target_node_id', 'parent_node_id',
            'parent_node_name', 'next_node_id', 'prev_node_id',
        ]
        
        for key, value in data_dict.items():
            if (key in node_ref_patterns or
                key.endswith('_node_id') or
                key.endswith('_node_ref') or
                key.endswith('_node_name')):
                
                if isinstance(value, str) and value.startswith('kb.'):
                    if value in self.node_builder.ltree_to_final_index:
                        node_index = self.node_builder.get_node_final_index(value)
                        result[key] = node_index
                        print(f"    Resolved {key}: '{value}' -> node_index={node_index}")
                    else:
                        print(f"    WARNING: {key} references unknown/filtered node: '{value}'")
                        result[key] = 0xFFFF
            
            elif isinstance(value, dict):
                result[key] = self._process_node_reference_fields(value)
            elif isinstance(value, list):
                result[key] = [
                    self._process_node_reference_fields(item) if isinstance(item, dict) else item
                    for item in value
                ]
        
        return result
    
    # =========================================================================
    # Main Build Method
    # =========================================================================
    
    def build(self) -> None:
        """Encode data for all operational nodes."""
        nodes_with_data = 0
        nodes_skipped = 0
        
        for ltree_name in self.node_builder.ltree_to_final_index.keys():
            if ltree_name in self.node_builder.filtered_nodes:
                continue
            
            node_data = self.handle.get_node_data(ltree_name)
            
            if not node_data:
                self.node_data_ids[ltree_name] = 0xFFFF
                nodes_skipped += 1
                continue
            
            # Extract ONLY operational runtime fields
            encode_data = {}
            
            if 'data' in node_data and self._has_meaningful_data(node_data['data']):
                encode_data['data'] = node_data['data']
            
            if 'node_dict' in node_data:
                node_dict = node_data['node_dict']
                if node_dict and isinstance(node_dict, dict):
                    filtered_dict = {k: v for k, v in node_dict.items() if k != 'auto_start'}
                    if filtered_dict:
                        filtered_dict = self._process_function_fields(filtered_dict)
                    if filtered_dict:
                        filtered_dict = self._process_node_reference_fields(filtered_dict)
                    if self._has_meaningful_data(filtered_dict):
                        encode_data['node_dict'] = filtered_dict
            
            for key in ['timeout', 'priority', 'config', 'parameters']:
                if key in node_data and self._has_meaningful_data(node_data[key]):
                    field_data = node_data[key]
                    if isinstance(field_data, dict):
                        field_data = self._process_node_reference_fields(field_data)
                    encode_data[key] = field_data
            
            if encode_data:
                data_id = self.encoder.load_dict(encode_data)
                self.node_data_ids[ltree_name] = data_id
                nodes_with_data += 1
                if nodes_with_data <= 5:
                    node_name = node_data.get('node_name', ltree_name)
                    print(f"  Encoding node [{nodes_with_data}] '{node_name}': {list(encode_data.keys())}")
                    for key, val in encode_data.items():
                        print(f"    {key}: {val}")
            else:
                self.node_data_ids[ltree_name] = 0xFFFF
                nodes_skipped += 1
                if nodes_skipped <= 5:
                    node_name = node_data.get('node_name', ltree_name)
                    print(f"  Skipped node '{node_name}': no operational data")
        
        print(f"\n  Summary: {nodes_with_data} nodes with data, {nodes_skipped} nodes skipped")
    
    # =========================================================================
    # C Code Generation (used by Stage 6)
    # =========================================================================
    
    def generate_c_arrays(self, lines: List[str], unique_id: str) -> None:
        """Generate C array definitions with unique_id prefix."""
        
        # Records array
        if self.encoder.records:
            lines.append("/* JSON records array */")
            lines.append(f"const json_record_t {unique_id}_node_data_records[{len(self.encoder.records)}] = {{")
            
            type_names = ['JSON_TYPE_STRING', 'JSON_TYPE_INT32', 'JSON_TYPE_FLOAT32',
                          'JSON_TYPE_NULL', 'JSON_TYPE_BOOL', 'JSON_TYPE_ARRAY', 'JSON_TYPE_OBJECT']
            
            for i, (type_tag, value) in enumerate(self.encoder.records):
                if type_tag == 0:
                    line = f"    {{ .object_type = {type_names[type_tag]}, .value = {{ .string_offset = {value} }} }}"
                elif type_tag == 1:
                    signed_val = struct.unpack('i', struct.pack('I', value))[0]
                    line = f"    {{ .object_type = {type_names[type_tag]}, .value = {{ .i32_value = {signed_val} }} }}"
                elif type_tag == 2:
                    float_val = struct.unpack('f', struct.pack('I', value))[0]
                    line = f"    {{ .object_type = {type_names[type_tag]}, .value = {{ .f32_value = {float_val}f }} }}"
                elif type_tag == 3:
                    line = f"    {{ .object_type = {type_names[type_tag]}, .value = {{ .i32_value = 0 }} }}"
                elif type_tag == 4:
                    line = f"    {{ .object_type = {type_names[type_tag]}, .value = {{ .bool_value = {value} }} }}"
                elif type_tag in [5, 6]:
                    line = f"    {{ .object_type = {type_names[type_tag]}, .value = {{ .container_count = {value} }} }}"
                
                if i < len(self.encoder.records) - 1:
                    line += ","
                lines.append(line)
            
            lines.append("};")
            lines.append("")
        
        # String table
        if self.encoder.string_data:
            string_bytes = []
            for s in self.encoder.string_data:
                for byte in s.encode('utf-8'):
                    string_bytes.append(byte)
                string_bytes.append(0)
            
            lines.append("/* String data buffer */")
            lines.append(f"const char {unique_id}_node_data_strings[{len(string_bytes)}] = {{")
            
            chunk_size = 16
            for i in range(0, len(string_bytes), chunk_size):
                chunk = string_bytes[i:i + chunk_size]
                hex_values = ', '.join(f'0x{b:02x}' for b in chunk)
                if i + chunk_size < len(string_bytes):
                    lines.append(f"    {hex_values},")
                else:
                    lines.append(f"    {hex_values}")
            
            lines.append("};")
            lines.append("")
        
        # Controls array
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
    
    # =========================================================================
    # Accessors
    # =========================================================================
    
    def get_node_data_id(self, ltree_name: str) -> int:
        return self.node_data_ids.get(ltree_name, 0xFFFF)
    
    def get_records_count(self) -> int:
        return len(self.encoder.records)
    
    def get_strings_size(self) -> int:
        return self.encoder.next_offset
    
    def get_controls_count(self) -> int:
        return len(self.encoder.record_controls)
    
    def print_summary(self) -> None:
        stats = self.encoder.get_stats()
        print("=" * 70)
        print("Stage 5: Node Data Encoder Summary")
        print("=" * 70)
        print(f"Nodes with data: {len([d for d in self.node_data_ids.values() if d != 0xFFFF])}")
        print(f"Total JSON records: {stats['total_records']}")
        print(f"Unique strings: {stats['unique_strings']}")
        print(f"String table size: {stats['string_table_bytes']} bytes")