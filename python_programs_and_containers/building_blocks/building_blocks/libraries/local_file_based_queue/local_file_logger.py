#!/usr/bin/env python3
"""
Test module for QueueDataReader class
"""

import json
import struct
import fcntl
import time
import tempfile
import shutil
from pathlib import Path
from datetime import datetime
from typing import Dict, Any, Optional

# Import the QueueDataReader class here
# from queue_reader import QueueDataReader

class QueueDataReader:
    """Power-failure-resistant class to read queue data"""
    
    def __init__(self, file_directory: str):
        self.file_directory = Path(file_directory)
        self._rotation_lock_file = self.file_directory / ".rotation.lock"
    
    def _calculate_checksum(self, data: str) -> str:
        """Calculate CRC32 checksum for data integrity"""
        import zlib
        return format(zlib.crc32(data.encode('utf-8')) & 0xffffffff, '08x')
    
    def _parse_line_with_checksum(self, line: str) -> tuple:
        """Parse a line that may have a checksum"""
        line = line.strip()
        if not line:
            return None, False
        
        if '|' in line:
            json_part, checksum = line.rsplit('|', 1)
            # Verify checksum
            if self._calculate_checksum(json_part) == checksum:
                return json.loads(json_part), True
            else:
                print(f"Checksum mismatch, skipping line")
                return None, False
        else:
            # Legacy format without checksum
            try:
                return json.loads(line), True
            except json.JSONDecodeError:
                return None, False
    
    def _acquire_file_lock(self, file_handle, exclusive=True):
        """Acquire file lock with retry logic"""
        max_retries = 10
        retry_delay = 0.1
        
        for attempt in range(max_retries):
            try:
                lock_type = fcntl.LOCK_EX if exclusive else fcntl.LOCK_SH
                fcntl.flock(file_handle.fileno(), lock_type | fcntl.LOCK_NB)
                return True
            except IOError:
                if attempt < max_retries - 1:
                    time.sleep(retry_delay)
                else:
                    return False
        return False
    
    def _release_file_lock(self, file_handle):
        """Release file lock"""
        try:
            fcntl.flock(file_handle.fileno(), fcntl.LOCK_UN)
        except:
            pass
    
    def _read_file_safely(self, file_path):
        """Read a file with verification and locking"""
        records = []
        corrupted_count = 0
        
        if file_path.name == "queue_active.jsonl":
            try:
                with open(file_path, 'r') as f:
                    for line in f:
                        record, valid = self._parse_line_with_checksum(line)
                        if valid and record:
                            records.append(record)
                        elif line.strip():
                            corrupted_count += 1
            except:
                pass
        else:
            file_lock_path = file_path.with_suffix('.lock')
            file_lock = open(file_lock_path, 'w')
            
            try:
                if self._acquire_file_lock(file_lock, exclusive=False):
                    with open(file_path, 'r') as f:
                        for line in f:
                            record, valid = self._parse_line_with_checksum(line)
                            if valid and record:
                                records.append(record)
                            elif line.strip():
                                corrupted_count += 1
            finally:
                self._release_file_lock(file_lock)
                file_lock.close()
        
        if corrupted_count > 0:
            print(f"Skipped {corrupted_count} corrupted lines in {file_path.name}")
        
        return records
    
    def _read_all_records(self, uploaded_filter: Optional[bool] = None):
        """Read all records with integrity checking"""
        all_records = []
        
        # Create rotation lock file if it doesn't exist
        self._rotation_lock_file.parent.mkdir(parents=True, exist_ok=True)
        rotation_lock = open(self._rotation_lock_file, 'w')
        try:
            if not self._acquire_file_lock(rotation_lock, exclusive=False):
                return []
            
            queue_files = list(self.file_directory.glob("queue_*.jsonl"))
            active_file = self.file_directory / "queue_active.jsonl"
            if active_file.exists() and active_file not in queue_files:
                queue_files.append(active_file)
            
            for file_path in queue_files:
                file_records = self._read_file_safely(file_path)
                
                for record in file_records:
                    if uploaded_filter is not None:
                        if record.get("uploaded", False) != uploaded_filter:
                            continue
                    
                    all_records.append(record)
            
        finally:
            self._release_file_lock(rotation_lock)
            rotation_lock.close()
        
        # Sort by sequence number first, then timestamp
        all_records.sort(key=lambda x: (x.get("time_stamp", 0), x.get("sequence", 0)))
        
        return all_records
    
    def output_data(self, uploaded_files: bool, line_separator: str = ",") -> str:
        """Output sorted data based on upload status"""
        if uploaded_files:
            records = self._read_all_records(uploaded_filter=False)
        else:
            records = self._read_all_records(uploaded_filter=None)
        
        output_lines = []
        for record in records:
            formatted_record = {
                "uploaded": record.get("uploaded", False),
                "time_stamp": record.get("time_stamp"),
                "timestamp_readable": datetime.fromtimestamp(record.get("time_stamp", 0)).isoformat(),
                "table_name": record.get("table_name"),
                "table_data": record.get("table_data")
            }
            output_lines.append(json.dumps(formatted_record, ensure_ascii=False))
        
        return line_separator.join(output_lines)
    
    def get_summary(self) -> Dict[str, Any]:
        """Get summary statistics with integrity information"""
        all_records = self._read_all_records()
        uploaded_count = sum(1 for r in all_records if r.get("uploaded", False))
        pending_count = len(all_records) - uploaded_count
        
        tables = {}
        for record in all_records:
            table_name = record.get("table_name", "unknown")
            if table_name not in tables:
                tables[table_name] = {"total": 0, "uploaded": 0, "pending": 0}
            
            tables[table_name]["total"] += 1
            if record.get("uploaded", False):
                tables[table_name]["uploaded"] += 1
            else:
                tables[table_name]["pending"] += 1
        
        # Check for corrupted files
        corrupted_files = list(self.file_directory.glob("*.corrupted"))
        
        return {
            "total_records": len(all_records),
            "uploaded_records": uploaded_count,
            "pending_records": pending_count,
            "tables": tables,
            "oldest_timestamp": min((r.get("time_stamp", 0) for r in all_records), default=None),
            "newest_timestamp": max((r.get("time_stamp", 0) for r in all_records), default=None),
            "corrupted_files": len(corrupted_files),
            "has_journal": (self.file_directory / "queue_active.journal").exists()
        }


if __name__ == "__main__":
    def create_test_data(test_dir: Path):
        """Create test data files with various formats"""
        import zlib
        
        def calculate_checksum(data: str) -> str:
            """Calculate CRC32 checksum"""
            return format(zlib.crc32(data.encode('utf-8')) & 0xffffffff, '08x')
        
        # Create test directory
        test_dir.mkdir(parents=True, exist_ok=True)
        
        # Test data records
        test_records = [
            {
                "time_stamp": 1700000000,
                "sequence": 1,
                "table_name": "users",
                "table_data": {"id": 1, "name": "Alice"},
                "uploaded": False
            },
            {
                "time_stamp": 1700000100,
                "sequence": 2,
                "table_name": "orders",
                "table_data": {"order_id": 101, "amount": 99.99},
                "uploaded": True
            },
            {
                "time_stamp": 1700000200,
                "sequence": 3,
                "table_name": "users",
                "table_data": {"id": 2, "name": "Bob"},
                "uploaded": False
            },
            {
                "time_stamp": 1700000300,
                "sequence": 4,
                "table_name": "products",
                "table_data": {"product_id": 500, "name": "Widget"},
                "uploaded": True
            }
        ]
        
        # Write queue_active.jsonl with mixed formats
        active_file = test_dir / "queue_active.jsonl"
        with open(active_file, 'w') as f:
            # Write first record with checksum
            json_str = json.dumps(test_records[0])
            checksum = calculate_checksum(json_str)
            f.write(f"{json_str}|{checksum}\n")
            
            # Write second record without checksum (legacy format)
            f.write(json.dumps(test_records[1]) + "\n")
            
            # Write a corrupted line
            f.write("corrupted_data_here\n")
        
        # Write queue_001.jsonl with checksums
        queue_file = test_dir / "queue_001.jsonl"
        with open(queue_file, 'w') as f:
            for record in test_records[2:]:
                json_str = json.dumps(record)
                checksum = calculate_checksum(json_str)
                f.write(f"{json_str}|{checksum}\n")
        
        # Create a corrupted file marker
        corrupted_file = test_dir / "queue_002.jsonl.corrupted"
        corrupted_file.touch()
        
        # Create a journal file
        journal_file = test_dir / "queue_active.journal"
        journal_file.touch()
        
        print(f"Test data created in: {test_dir}")
        print(f"Files created:")
        for file in test_dir.iterdir():
            print(f"  - {file.name}")

    def run_tests(test_dir: Path):
        """Run comprehensive tests on QueueDataReader"""
        print("\n" + "="*60)
        print("Running QueueDataReader Tests")
        print("="*60)
        
        # Initialize reader
        reader = QueueDataReader(str(test_dir))
        
        # Test 1: Get Summary
        print("\n1. Testing get_summary():")
        print("-" * 40)
        summary = reader.get_summary()
        print(f"Total records: {summary['total_records']}")
        print(f"Uploaded records: {summary['uploaded_records']}")
        print(f"Pending records: {summary['pending_records']}")
        print(f"Has journal: {summary['has_journal']}")
        print(f"Corrupted files: {summary['corrupted_files']}")
        
        if summary['tables']:
            print("\nTable breakdown:")
            for table_name, stats in summary['tables'].items():
                print(f"  {table_name}: total={stats['total']}, "
                      f"uploaded={stats['uploaded']}, pending={stats['pending']}")
        
        if summary['oldest_timestamp']:
            oldest_time = datetime.fromtimestamp(summary['oldest_timestamp'])
            newest_time = datetime.fromtimestamp(summary['newest_timestamp'])
            print(f"\nTime range: {oldest_time} to {newest_time}")
        
        # Test 2: Output all data
        print("\n2. Testing output_data() - All records:")
        print("-" * 40)
        all_data = reader.output_data(uploaded_files=False, line_separator="\n")
        if all_data:
            lines = all_data.split("\n")
            print(f"Found {len(lines)} records")
            for i, line in enumerate(lines[:2], 1):  # Show first 2 records
                record = json.loads(line)
                print(f"\nRecord {i}:")
                print(f"  Table: {record['table_name']}")
                print(f"  Uploaded: {record['uploaded']}")
                print(f"  Timestamp: {record['timestamp_readable']}")
                print(f"  Data: {record['table_data']}")
        
        # Test 3: Output only pending (not uploaded) data
        print("\n3. Testing output_data() - Pending records only:")
        print("-" * 40)
        pending_data = reader.output_data(uploaded_files=True, line_separator="\n")
        if pending_data:
            lines = pending_data.split("\n")
            print(f"Found {len(lines)} pending records")
            for line in lines:
                record = json.loads(line)
                print(f"  - {record['table_name']}: {record['table_data']}")
        else:
            print("No pending records found")
        
        # Test 4: Test checksum validation
        print("\n4. Testing checksum validation:")
        print("-" * 40)
        test_line_good = '{"test": "data"}|7c1a2f5e'
        test_line_bad = '{"test": "data"}|wrongchk'
        
        result_good, valid_good = reader._parse_line_with_checksum(test_line_good)
        result_bad, valid_bad = reader._parse_line_with_checksum(test_line_bad)
        
        print(f"Valid checksum line: {valid_good} - {result_good}")
        print(f"Invalid checksum line: {valid_bad} - {result_bad}")
        
        # Test 5: Test file locking
        print("\n5. Testing file locking mechanism:")
        print("-" * 40)
        lock_test_file = test_dir / "lock_test.txt"
        with open(lock_test_file, 'w') as f:
            f.write("test")
        
        with open(lock_test_file, 'r') as f:
            acquired = reader._acquire_file_lock(f, exclusive=False)
            print(f"Acquired shared lock: {acquired}")
            if acquired:
                reader._release_file_lock(f)
                print("Released lock successfully")
        
        # Clean up lock test file
        lock_test_file.unlink()
        
        print("\n" + "="*60)
        print("Tests completed successfully!")
        print("="*60)

    def cleanup_test_dir(test_dir: Path):
        """Clean up test directory"""
        if test_dir.exists():
            shutil.rmtree(test_dir)
            print(f"\nCleaned up test directory: {test_dir}")
    
    # Create temporary test directory
    test_dir = Path(tempfile.mkdtemp(prefix="queue_reader_test_"))
    
    try:
        # Create test data
        create_test_data(test_dir)
        
        # Run tests
        run_tests(test_dir)
        
        # Optional: Interactive testing
        print("\n" + "="*60)
        print("Interactive Testing (optional)")
        print("="*60)
        print(f"\nTest directory: {test_dir}")
        print("\nYou can now:")
        print("1. Inspect the test files manually")
        print("2. Run additional tests")
        print("3. Press Enter to clean up and exit")
        
        input("\nPress Enter to clean up and exit...")
        
    except Exception as e:
        print(f"\nError during testing: {e}")
        import traceback
        traceback.print_exc()
    
    finally:
        # Clean up
        cleanup_test_dir(test_dir)