import json
import struct
from typing import List, Dict, Any, Tuple

class JsonRecordEncoder:
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
            self.records.append((6, len(value)))  # JSON_TYPE_OBJECT
            for key, val in value.items():
                key_offset = self.add_string(str(key))
                self.records.append((0, key_offset))
                self.encode_value(val)
        
        else:
            raise ValueError(f"Unsupported JSON type: {type(value)}")
    
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


# Usage example
if __name__ == '__main__':
    encoder = JsonRecordEncoder()
    encoder.init()
    
    # Load JSON objects incrementally
    print(encoder.load('{"temperature": 23.5, "sensors": [1, 2, 3]}'))
    print(encoder.load('{"config": {"timeout": 30, "retry": true}}'))
    print(encoder.load('{"data": [{"id": 1, "name": "sensor_a"}, {"id": 2, "name": "sensor_b"}]}'))
    print(encoder.load('{"temperature": 24.1}'))  # "temperature" string reused
    print(encoder.load('{"matrix": [[1, 2], [3, 4]]}'))
    
    print(f"Stats: {encoder.get_stats()}")
    
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