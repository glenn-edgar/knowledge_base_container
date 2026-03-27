"""
Stage 6: Binary Image Emitter (Python)

Replaces stage6_codegen.py when generating .ctb binary images.
Consumes identical stage 1-5 output.

Usage from orchestrator:
    from .stage6_binary import BinaryImageEmitter
    emitter = BinaryImageEmitter(...)
    emitter.emit()
"""

import struct
import zlib
from pathlib import Path
from typing import Dict, List, Optional, Tuple

from .stage1_handle import ChainTreeHandle
from .stage2_node_index import NodeIndexBuilder
from .stage3_function_index import FunctionIndexBuilder
from .stage4_link_table import LinkTableBuilder
from .stage5_node_data import NodeDataEncoder


# =========================================================================
# FNV-1a 32-bit (pure Python)
# =========================================================================

FNV1A_OFFSET_BASIS = 2166136261
FNV1A_PRIME = 16777619
FNV1A_MASK = 0xFFFFFFFF


def fnv1a_32(s: str) -> int:
    h = FNV1A_OFFSET_BASIS
    for b in s.encode('utf-8'):
        h ^= b
        h = (h * FNV1A_PRIME) & FNV1A_MASK
    return h


# =========================================================================
# Section type constants
# =========================================================================

SECT_NODE = 0x0001
SECT_LINK = 0x0002
SECT_MFHT = 0x0003
SECT_OSHT = 0x0004
SECT_BFHT = 0x0005
SECT_FSTR = 0x0006
SECT_JREC = 0x0007
SECT_JCTL = 0x0008
SECT_JSTR = 0x0009
SECT_EVNT = 0x000A
SECT_BMSK = 0x000B
SECT_KBIN = 0x000C
SECT_KBAL = 0x000D
SECT_GSTR = 0x000E

CTB_MAGIC = 0x43544231  # "CTB1"


# =========================================================================
# String Pool (deduplicated)
# =========================================================================

class StringPool:
    def __init__(self):
        self.strings: List[str] = []
        self.offsets: Dict[str, int] = {}
        self.next_offset = 0

    def add(self, s: str) -> int:
        if s in self.offsets:
            return self.offsets[s]
        offset = self.next_offset
        self.offsets[s] = offset
        self.strings.append(s)
        self.next_offset = offset + len(s.encode('utf-8')) + 1
        return offset

    def to_binary(self) -> bytes:
        parts = []
        for s in self.strings:
            parts.append(s.encode('utf-8') + b'\x00')
        return b''.join(parts)

    def size(self) -> int:
        return self.next_offset


# =========================================================================
# Helper functions
# =========================================================================

def align4(offset: int) -> int:
    return (offset + 3) & ~3


def pad_to_4(data: bytes) -> bytes:
    rem = len(data) % 4
    if rem == 0:
        return data
    return data + b'\x00' * (4 - rem)


# =========================================================================
# Binary Image Emitter
# =========================================================================

class BinaryImageEmitter:

    def __init__(
        self,
        output_dir: Path,
        handle_name: str,
        handle: ChainTreeHandle,
        node_builder: NodeIndexBuilder,
        function_builder: FunctionIndexBuilder,
        link_builder: LinkTableBuilder,
        data_encoder: Optional[NodeDataEncoder],
        main_function_usage: Dict[int, int],
        emit_c_header: bool = False,
    ):
        self.output_dir = Path(output_dir)
        self.handle_name = handle_name
        self.handle = handle
        self.node_builder = node_builder
        self.function_builder = function_builder
        self.link_builder = link_builder
        self.data_encoder = data_encoder
        self.main_function_usage = main_function_usage
        self.emit_c_header = emit_c_header

    # =================================================================
    # Hash table building
    # =================================================================

    def _build_hash_table(self, indexer) -> Tuple[list, Dict[int, int]]:
        entries = []
        for i, name in enumerate(indexer.get_all_functions()):
            h = fnv1a_32(name)
            entries.append({'hash': h, 'orig_index': i, 'name': name})

        # Check collisions
        hash_set = {}
        for e in entries:
            if e['hash'] in hash_set:
                raise RuntimeError(
                    f"FNV-1a collision in {indexer.name}: "
                    f"'{hash_set[e['hash']]}' and '{e['name']}' "
                    f"both hash to 0x{e['hash']:08X}")
            hash_set[e['hash']] = e['name']

        # Sort by hash
        entries.sort(key=lambda e: e['hash'])

        # Build remap: orig_index -> sorted position
        remap = {}
        for sorted_pos, e in enumerate(entries):
            remap[e['orig_index']] = sorted_pos

        return entries, remap

    # =================================================================
    # Node array with remapped indices
    # =================================================================

    def _build_node_array(self, main_remap, os_remap, bool_remap) -> bytes:
        array_size = self.node_builder.get_array_size()
        parts = []

        for i in range(array_size):
            ltree_name = self.node_builder.get_node_by_index(i)

            if ltree_name is None:
                parts.append(struct.pack('<10H',
                    i, 0xFFFF, 0, 0, 0,
                    main_remap.get(0, 0), os_remap.get(0, 0),
                    bool_remap.get(0, 0), os_remap.get(0, 0), 0xFFFF))
            else:
                node_data = self.handle.get_node_data(ltree_name)
                functions = self.handle.get_node_functions(ltree_name)

                main_orig = self.function_builder.main_indexer.get_index(functions['main'])
                init_orig = self.function_builder.one_shot_indexer.get_index(functions['init'])
                aux_orig = self.function_builder.boolean_indexer.get_index(functions['aux'])
                term_orig = self.function_builder.one_shot_indexer.get_index(functions['term'])

                main_idx = main_remap.get(main_orig, 0)
                init_idx = os_remap.get(init_orig, 0)
                aux_idx = bool_remap.get(aux_orig, 0)
                term_idx = os_remap.get(term_orig, 0)

                link_info = self.link_builder.get_node_link_info(ltree_name)
                link_count = link_info['link_count']

                node_dict = node_data.get('node_dict', {})
                auto_start = node_dict.get('auto_start', False) if isinstance(node_dict, dict) else False

                packed_lc = link_count & 0x7FFF
                if auto_start:
                    packed_lc |= 0x8000

                parent_ltree = self.handle.get_node_parent(ltree_name)
                parent_idx = 0xFFFF
                if parent_ltree and parent_ltree in self.node_builder.ltree_to_final_index:
                    parent_idx = self.node_builder.get_node_final_index(parent_ltree)

                depth = self.node_builder.get_node_depth(ltree_name)
                data_id = self.data_encoder.get_node_data_id(ltree_name) if self.data_encoder else 0xFFFF

                parts.append(struct.pack('<10H',
                    i, parent_idx, depth, link_info['link_start'], packed_lc,
                    main_idx, init_idx, aux_idx, term_idx, data_id))

        return b''.join(parts)

    # =================================================================
    # Link table
    # =================================================================

    def _build_link_table(self) -> bytes:
        parts = []
        for child_index in self.link_builder.link_table:
            parts.append(struct.pack('<H', child_index))
        return b''.join(parts)

    # =================================================================
    # Hash table binary
    # =================================================================

    @staticmethod
    def _hash_table_to_binary(entries) -> bytes:
        return b''.join(struct.pack('<I', e['hash']) for e in entries)

    @staticmethod
    def _func_names_to_binary(main_entries, os_entries, bool_entries) -> bytes:
        parts = []
        for e in main_entries:
            parts.append(e['name'].encode('utf-8') + b'\x00')
        for e in os_entries:
            parts.append(e['name'].encode('utf-8') + b'\x00')
        for e in bool_entries:
            parts.append(e['name'].encode('utf-8') + b'\x00')
        return b''.join(parts)

    # =================================================================
    # JSON data sections
    # =================================================================

    def _build_json_sections(self) -> Tuple[bytes, bytes, bytes]:
        if not self.data_encoder:
            return b'', b'', b''

        enc = self.data_encoder.encoder

        # Records: 8 bytes each
        rec_parts = []
        for rec in enc.records:
            rec_parts.append(struct.pack('<II', rec[0], rec[1]))

        # Controls: 8 bytes each
        ctrl_parts = []
        for ctrl in enc.record_controls:
            ctrl_parts.append(struct.pack('<II', ctrl['start_position'], ctrl['num_records']))

        # Strings: packed null-terminated
        str_parts = []
        for s in enc.string_data:
            str_parts.append(s.encode('utf-8') + b'\x00')

        return b''.join(rec_parts), b''.join(ctrl_parts), b''.join(str_parts)

    # =================================================================
    # Event / Bitmask / KB sections
    # =================================================================

    def _build_event_section(self, pool: StringPool) -> Tuple[bytes, int]:
        events = self.handle.get_event_string_table()
        if not events:
            return b'', 0

        sorted_events = sorted(events.items(), key=lambda x: x[1])
        parts = []
        for name, _ in sorted_events:
            offset = pool.add(name)
            parts.append(struct.pack('<I', offset))
        return b''.join(parts), len(sorted_events)

    def _build_bitmask_section(self, pool: StringPool) -> Tuple[bytes, int]:
        bitmasks = self.handle.get_bitmask_table()
        if not bitmasks:
            return b'', 0

        sorted_bm = sorted(bitmasks.items(), key=lambda x: x[1])
        parts = []
        for name, bit_pos in sorted_bm:
            offset = pool.add(name)
            parts.append(struct.pack('<IBxxx', offset, bit_pos))
        return b''.join(parts), len(sorted_bm)

    def _filter_executable_kbs(self, names):
        return [kb for kb in names
                if not kb.endswith('_test_functions') and kb != 'complete_functions_kb']

    def _build_kb_sections(self, pool: StringPool) -> Tuple[bytes, bytes, int, int]:
        kb_names = self._filter_executable_kbs(self.handle.get_kb_names())

        kb_parts = []
        alias_parts = []
        total_aliases = 0

        for kb_name in kb_names:
            name_offset = pool.add(kb_name)
            start_idx, end_idx = self.node_builder.get_kb_range(kb_name)
            node_count = end_idx - start_idx

            max_depth = 0
            for j in range(start_idx, end_idx):
                lt = self.node_builder.final_index_to_ltree.get(j)
                if lt:
                    max_depth = max(max_depth, self.node_builder.get_node_depth(lt))

            memory_factor = self.handle.get_kb_metadata_value(kb_name, "node_memory_factor", 10)

            aliases = self.handle.get_kb_node_aliases(kb_name)
            alias_count = len(aliases)
            alias_start = total_aliases

            kb_parts.append(struct.pack('<IHHHHHHH6x',
                name_offset, start_idx, start_idx, node_count,
                max_depth, memory_factor, alias_start, alias_count))

            if alias_count > 0:
                for aname, aindex in sorted(aliases.items()):
                    aoffset = pool.add(aname)
                    alias_parts.append(struct.pack('<IHH', aoffset, aindex, 0))
                total_aliases += alias_count

        return b''.join(kb_parts), b''.join(alias_parts), len(kb_names), total_aliases

    # =================================================================
    # Main emit
    # =================================================================

    def emit(self) -> int:
        print("\n  Building hash tables...")
        main_entries, main_remap = self._build_hash_table(self.function_builder.main_indexer)
        os_entries, os_remap = self._build_hash_table(self.function_builder.one_shot_indexer)
        bool_entries, bool_remap = self._build_hash_table(self.function_builder.boolean_indexer)

        print(f"    Main: {len(main_entries)} functions")
        print(f"    One-shot: {len(os_entries)} functions")
        print(f"    Boolean: {len(bool_entries)} functions")

        print("  Building sections...")
        node_bin = self._build_node_array(main_remap, os_remap, bool_remap)
        link_bin = self._build_link_table()
        main_hash_bin = self._hash_table_to_binary(main_entries)
        os_hash_bin = self._hash_table_to_binary(os_entries)
        bool_hash_bin = self._hash_table_to_binary(bool_entries)
        func_names_bin = self._func_names_to_binary(main_entries, os_entries, bool_entries)
        json_rec_bin, json_ctrl_bin, json_str_bin = self._build_json_sections()

        pool = StringPool()
        event_bin, event_count = self._build_event_section(pool)
        bitmask_bin, bitmask_count = self._build_bitmask_section(pool)
        kb_info_bin, kb_alias_bin, kb_count, alias_count = self._build_kb_sections(pool)
        pool_bin = pool.to_binary()

        # Section list
        json_rec_count = len(self.data_encoder.encoder.records) if self.data_encoder else 0
        json_ctrl_count = len(self.data_encoder.encoder.record_controls) if self.data_encoder else 0

        sections = [
            (SECT_NODE, node_bin, self.node_builder.get_array_size(), 20),
            (SECT_LINK, link_bin, self.link_builder.get_link_table_size(), 2),
            (SECT_MFHT, main_hash_bin, len(main_entries), 4),
            (SECT_OSHT, os_hash_bin, len(os_entries), 4),
            (SECT_BFHT, bool_hash_bin, len(bool_entries), 4),
            (SECT_FSTR, func_names_bin, len(main_entries) + len(os_entries) + len(bool_entries), 0),
            (SECT_JREC, json_rec_bin, json_rec_count, 8),
            (SECT_JCTL, json_ctrl_bin, json_ctrl_count, 8),
            (SECT_JSTR, json_str_bin, 0, 0),
            (SECT_EVNT, event_bin, event_count, 4),
            (SECT_BMSK, bitmask_bin, bitmask_count, 8),
            (SECT_KBIN, kb_info_bin, kb_count, 24),
            (SECT_KBAL, kb_alias_bin, alias_count, 8),
            (SECT_GSTR, pool_bin, 0, 0),
        ]

        section_count = len(sections)
        header_size = 64
        dir_size = section_count * 16
        data_start = align4(header_size + dir_size)

        # Compute offsets
        current_offset = data_start
        section_info = []
        for stype, sdata, scount, sesize in sections:
            section_info.append({
                'type': stype, 'data': sdata,
                'offset': current_offset, 'size': len(sdata),
                'count': scount, 'esize': sesize,
            })
            current_offset = align4(current_offset + len(sdata))

        total_size = current_offset

        # Flags
        flags = 0
        if len(json_rec_bin) > 0:
            flags |= 1
        if event_count > 0:
            flags |= 2
        if bitmask_count > 0:
            flags |= 4

        # Header
        header = struct.pack('<IHHIIIHHHHHHHHHHHHI16x',
            CTB_MAGIC,                              # magic
            1, 0,                                   # version
            flags,                                  # flags
            total_size,                             # total_image_size
            0,                                      # checksum (patch later)
            section_count,                          # section_count
            self.node_builder.get_array_size(),     # node_count
            self.node_builder.get_total_nodes(),    # node_active_count
            self.link_builder.get_link_table_size(),# link_table_size
            len(main_entries),                      # main_func_count
            len(os_entries),                        # one_shot_func_count
            len(bool_entries),                      # boolean_func_count
            event_count,                            # event_count
            bitmask_count,                          # bitmask_count
            kb_count,                               # kb_count
            json_rec_count,                         # json_records_count
            json_ctrl_count,                        # json_controls_count
            len(json_str_bin),                      # json_strings_size
        )
        assert len(header) == 64, f"Header size: {len(header)}"

        # Directory
        dir_parts = []
        for si in section_info:
            dir_parts.append(struct.pack('<IIIHH',
                si['type'], si['offset'], si['size'],
                si['count'], si['esize']))
        dir_bin = b''.join(dir_parts)

        # Assemble
        image_parts = [header, dir_bin]

        after_dir = header_size + dir_size
        if after_dir < data_start:
            image_parts.append(b'\x00' * (data_start - after_dir))

        for idx, si in enumerate(section_info):
            image_parts.append(si['data'])
            next_off = section_info[idx + 1]['offset'] if idx + 1 < len(section_info) else total_size
            end_data = si['offset'] + si['size']
            if end_data < next_off:
                image_parts.append(b'\x00' * (next_off - end_data))

        image = b''.join(image_parts)
        assert len(image) == total_size, f"Size mismatch: {len(image)} vs {total_size}"

        # CRC32
        crc = zlib.crc32(image) & 0xFFFFFFFF
        image = image[:16] + struct.pack('<I', crc) + image[20:]

        # Write
        self.output_dir.mkdir(parents=True, exist_ok=True)

        ctb_path = self.output_dir / f"{self.handle_name}.ctb"
        with open(ctb_path, 'wb') as f:
            f.write(image)
        print(f"  Generated: {ctb_path} ({len(image)} bytes)")

        if self.emit_c_header:
            self._write_c_header(image)

        return len(image)

    def _write_c_header(self, image_data: bytes) -> None:
        path = self.output_dir / f"{self.handle_name}_image.h"
        guard = f"{self.handle_name.upper()}_IMAGE_H"

        lines = [
            "/* Auto-generated ChainTree binary image */",
            f"#ifndef {guard}",
            f"#define {guard}",
            "",
            "#include <stdint.h>",
            "",
            f"#define {self.handle_name.upper()}_IMAGE_SIZE {len(image_data)}",
            "",
            f"const uint8_t {self.handle_name}_image[{len(image_data)}] = {{",
        ]

        for i in range(0, len(image_data), 16):
            chunk = image_data[i:i+16]
            hex_vals = ", ".join(f"0x{b:02x}" for b in chunk)
            line = f"    {hex_vals}"
            if i + 16 < len(image_data):
                line += ","
            lines.append(line)

        lines.extend(["};", "", f"#endif /* {guard} */", ""])

        with open(path, 'w') as f:
            f.write("\n".join(lines))
        print(f"  Generated: {path}")