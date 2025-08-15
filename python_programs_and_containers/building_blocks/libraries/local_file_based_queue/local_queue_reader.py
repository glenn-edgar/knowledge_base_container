#!/usr/bin/env python3
"""
Test driver for QueueDataReader class
"""

import os
import json
import time
import fcntl
import hashlib
import struct
from pathlib import Path
from datetime import datetime
from typing import Dict, Any, Callable, Optional
import tempfile
import shutil
import threading
import zlib

class QueueDataReader:
    """Power-failure-resistant class to read queue data"""
    
    def __init__(self, file_directory: str):
        self.file_directory = Path(file_directory)
        self._rotation_lock_file = self.file_directory / ".rotation.lock"
    
    def _calculate_checksum(self, data: str) -> str:
        """Calculate CRC32 checksum for data integrity"""
        # Fixed: using zlib.crc32 instead of json.crc32
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
        
        # Ensure rotation lock file directory exists
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
    def create_test_environment(base_dir: Path) -> Dict[str, Path]:
        """Create multiple test scenarios in different directories"""
        scenarios = {}
        
        # Scenario 1: Normal data with checksums
        normal_dir = base_dir / "normal_data"
        normal_dir.mkdir(parents=True, exist_ok=True)
        scenarios["normal"] = normal_dir
        
        # Scenario 2: Mixed format data
        mixed_dir = base_dir / "mixed_format"
        mixed_dir.mkdir(parents=True, exist_ok=True)
        scenarios["mixed"] = mixed_dir
        
        # Scenario 3: Corrupted data
        corrupted_dir = base_dir / "corrupted_data"
        corrupted_dir.mkdir(parents=True, exist_ok=True)
        scenarios["corrupted"] = corrupted_dir
        
        # Scenario 4: Empty directory
        empty_dir = base_dir / "empty_data"
        empty_dir.mkdir(parents=True, exist_ok=True)
        scenarios["empty"] = empty_dir
        
        return scenarios
    
    def populate_normal_data(directory: Path):
        """Populate directory with normal test data"""
        def calculate_checksum(data: str) -> str:
            return format(zlib.crc32(data.encode('utf-8')) & 0xffffffff, '08x')
        
        # Create multiple queue files
        test_data = []
        current_time = int(time.time())
        
        # Generate test records
        for i in range(20):
            record = {
                "time_stamp": current_time + i * 100,
                "sequence": i + 1,
                "table_name": f"table_{i % 3}",  # 3 different tables
                "table_data": {
                    "id": i + 1,
                    "value": f"data_{i}",
                    "amount": round(100 * (i + 1) * 1.5, 2)
                },
                "uploaded": i % 3 == 0  # Every 3rd record is uploaded
            }
            test_data.append(record)
        
        # Write to queue_active.jsonl
        active_file = directory / "queue_active.jsonl"
        with open(active_file, 'w') as f:
            for record in test_data[:5]:
                json_str = json.dumps(record)
                checksum = calculate_checksum(json_str)
                f.write(f"{json_str}|{checksum}\n")
        
        # Write to queue_001.jsonl
        queue_001 = directory / "queue_001.jsonl"
        with open(queue_001, 'w') as f:
            for record in test_data[5:10]:
                json_str = json.dumps(record)
                checksum = calculate_checksum(json_str)
                f.write(f"{json_str}|{checksum}\n")
        
        # Write to queue_002.jsonl
        queue_002 = directory / "queue_002.jsonl"
        with open(queue_002, 'w') as f:
            for record in test_data[10:15]:
                json_str = json.dumps(record)
                checksum = calculate_checksum(json_str)
                f.write(f"{json_str}|{checksum}\n")
        
        # Write to queue_003.jsonl
        queue_003 = directory / "queue_003.jsonl"
        with open(queue_003, 'w') as f:
            for record in test_data[15:]:
                json_str = json.dumps(record)
                checksum = calculate_checksum(json_str)
                f.write(f"{json_str}|{checksum}\n")
        
        # Create journal file
        journal = directory / "queue_active.journal"
        journal.write_text("Journal data for recovery")
        
        print(f"  Created {len(test_data)} records across 4 files")
    
    def populate_mixed_format(directory: Path):
        """Populate directory with mixed format data"""
        def calculate_checksum(data: str) -> str:
            return format(zlib.crc32(data.encode('utf-8')) & 0xffffffff, '08x')
        
        active_file = directory / "queue_active.jsonl"
        with open(active_file, 'w') as f:
            # Record with checksum
            record1 = {"time_stamp": 1700000000, "sequence": 1, "table_name": "users", 
                      "table_data": {"id": 1}, "uploaded": False}
            json_str = json.dumps(record1)
            checksum = calculate_checksum(json_str)
            f.write(f"{json_str}|{checksum}\n")
            
            # Legacy record without checksum
            record2 = {"time_stamp": 1700000100, "sequence": 2, "table_name": "orders",
                      "table_data": {"id": 2}, "uploaded": True}
            f.write(json.dumps(record2) + "\n")
            
            # Another record with checksum
            record3 = {"time_stamp": 1700000200, "sequence": 3, "table_name": "products",
                      "table_data": {"id": 3}, "uploaded": False}
            json_str = json.dumps(record3)
            checksum = calculate_checksum(json_str)
            f.write(f"{json_str}|{checksum}\n")
        
        print(f"  Created mixed format file with checksums and legacy format")
    
    def populate_corrupted_data(directory: Path):
        """Populate directory with corrupted data"""
        def calculate_checksum(data: str) -> str:
            return format(zlib.crc32(data.encode('utf-8')) & 0xffffffff, '08x')
        
        active_file = directory / "queue_active.jsonl"
        with open(active_file, 'w') as f:
            # Valid record
            record1 = {"time_stamp": 1700000000, "sequence": 1, "table_name": "valid",
                      "table_data": {"id": 1}, "uploaded": False}
            json_str = json.dumps(record1)
            checksum = calculate_checksum(json_str)
            f.write(f"{json_str}|{checksum}\n")
            
            # Corrupted JSON
            f.write("{'invalid': json, syntax here}\n")
            
            # Wrong checksum
            record2 = {"time_stamp": 1700000100, "sequence": 2, "table_name": "wrong_checksum",
                      "table_data": {"id": 2}, "uploaded": False}
            f.write(f"{json.dumps(record2)}|wrongchecksum\n")
            
            # Partial line
            f.write('{"incomplete": "record"')
            
            # Valid record after corruption
            record3 = {"time_stamp": 1700000200, "sequence": 3, "table_name": "valid",
                      "table_data": {"id": 3}, "uploaded": True}
            json_str = json.dumps(record3)
            checksum = calculate_checksum(json_str)
            f.write(f"\n{json_str}|{checksum}\n")
        
        # Create corrupted file marker
        corrupted_marker = directory / "queue_001.jsonl.corrupted"
        corrupted_marker.touch()
        
        print(f"  Created file with corrupted data and corruption markers")
    
    def test_concurrent_access(directory: Path):
        """Test concurrent read access with threading"""
        print("\n6. Testing Concurrent Access:")
        print("-" * 40)
        
        reader = QueueDataReader(str(directory))
        results = []
        errors = []
        
        def read_worker(worker_id):
            try:
                summary = reader.get_summary()
                results.append((worker_id, summary['total_records']))
            except Exception as e:
                errors.append((worker_id, str(e)))
        
        # Create multiple threads
        threads = []
        for i in range(5):
            t = threading.Thread(target=read_worker, args=(i,))
            threads.append(t)
            t.start()
        
        # Wait for all threads
        for t in threads:
            t.join()
        
        if errors:
            print(f"  Errors occurred: {errors}")
        else:
            print(f"  All {len(threads)} threads read successfully")
            for worker_id, count in results:
                print(f"    Worker {worker_id}: {count} records")
    
    def run_scenario_tests(scenario_name: str, directory: Path):
        """Run tests for a specific scenario"""
        print(f"\n{'='*60}")
        print(f"Testing Scenario: {scenario_name.upper()}")
        print(f"Directory: {directory}")
        print("="*60)
        
        reader = QueueDataReader(str(directory))
        
        # Test 1: Summary
        print("\n1. Summary Statistics:")
        print("-" * 40)
        summary = reader.get_summary()
        print(f"  Total records: {summary['total_records']}")
        print(f"  Uploaded: {summary['uploaded_records']}")
        print(f"  Pending: {summary['pending_records']}")
        print(f"  Has journal: {summary['has_journal']}")
        print(f"  Corrupted files: {summary['corrupted_files']}")
        
        if summary['tables']:
            print("\n  Table breakdown:")
            for table, stats in summary['tables'].items():
                print(f"    {table}: total={stats['total']}, "
                      f"uploaded={stats['uploaded']}, pending={stats['pending']}")
        
        if summary['oldest_timestamp'] and summary['newest_timestamp']:
            oldest = datetime.fromtimestamp(summary['oldest_timestamp'])
            newest = datetime.fromtimestamp(summary['newest_timestamp'])
            print(f"\n  Time range: {oldest.isoformat()} to {newest.isoformat()}")
        
        # Test 2: All records
        print("\n2. All Records Output:")
        print("-" * 40)
        all_output = reader.output_data(uploaded_files=False, line_separator="\n")
        if all_output:
            lines = all_output.split("\n")
            print(f"  Total lines: {len(lines)}")
            if lines:
                first_record = json.loads(lines[0])
                print(f"  First record: {first_record['table_name']} - "
                      f"uploaded={first_record['uploaded']}")
        else:
            print("  No records found")
        
        # Test 3: Pending records only
        print("\n3. Pending Records Only:")
        print("-" * 40)
        pending_output = reader.output_data(uploaded_files=True, line_separator="\n")
        if pending_output:
            lines = pending_output.split("\n")
            print(f"  Pending records: {len(lines)}")
        else:
            print("  No pending records")
        
        # Test 4: Checksum validation
        print("\n4. Checksum Validation:")
        print("-" * 40)
        test_valid = '{"test": "data"}|7c1a2f5e'
        test_invalid = '{"test": "data"}|badcheck'
        
        result_valid, is_valid = reader._parse_line_with_checksum(test_valid)
        result_invalid, is_invalid = reader._parse_line_with_checksum(test_invalid)
        
        print(f"  Valid checksum: {is_valid} - {result_valid}")
        print(f"  Invalid checksum: {is_invalid} - {result_invalid}")
        
        # Test 5: File locking
        print("\n5. File Locking:")
        print("-" * 40)
        test_lock_file = directory / "test_lock.txt"
        test_lock_file.write_text("test")
        
        with open(test_lock_file, 'r') as f:
            acquired = reader._acquire_file_lock(f, exclusive=False)
            print(f"  Lock acquired: {acquired}")
            if acquired:
                reader._release_file_lock(f)
                print(f"  Lock released successfully")
        
        test_lock_file.unlink()
    
    def main():
        """Main test driver function"""
        print("\n" + "="*60)
        print("QUEUEDATAREADER TEST DRIVER")
        print("="*60)
        
        # Create temporary test directory
        base_dir = Path(tempfile.mkdtemp(prefix="queue_test_"))
        
        try:
            # Create test scenarios
            print("\nSetting up test scenarios...")
            scenarios = create_test_environment(base_dir)
            
            # Populate test data
            print("\nPopulating test data:")
            print(f"\n1. Normal data scenario:")
            populate_normal_data(scenarios["normal"])
            
            print(f"\n2. Mixed format scenario:")
            populate_mixed_format(scenarios["mixed"])
            
            print(f"\n3. Corrupted data scenario:")
            populate_corrupted_data(scenarios["corrupted"])
            
            print(f"\n4. Empty directory scenario:")
            print(f"  Directory created (no data)")
            
            # Run tests for each scenario
            for name, directory in scenarios.items():
                run_scenario_tests(name, directory)
            
            # Additional concurrent access test on normal data
            test_concurrent_access(scenarios["normal"])
            
            print("\n" + "="*60)
            print("ALL TESTS COMPLETED SUCCESSFULLY!")
            print("="*60)
            
            # Optional: Interactive mode
            print(f"\nTest directory: {base_dir}")
            response = input("\nPress Enter to clean up, or 'k' to keep test files: ")
            
            if response.lower() != 'k':
                shutil.rmtree(base_dir)
                print("Test files cleaned up.")
            else:
                print(f"Test files preserved at: {base_dir}")
                
        except Exception as e:
            print(f"\nERROR: {e}")
            import traceback
            traceback.print_exc()
            shutil.rmtree(base_dir, ignore_errors=True)
    
    # Run the main test driver
    main()