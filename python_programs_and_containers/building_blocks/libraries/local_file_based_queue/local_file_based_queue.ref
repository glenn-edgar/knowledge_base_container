import os
import json
import time
import fcntl
import hashlib
import struct
from pathlib import Path
from datetime import datetime
from typing import Dict, Any, Callable, Optional
import psycopg2
from psycopg2.extras import Json
import threading


class QueueFileWriter:
    """Power-failure-resistant class to append data to queue files"""
    
    def __init__(self, file_directory: str):
        self.file_directory = Path(file_directory)
        self.file_directory.mkdir(parents=True, exist_ok=True)
        self.active_file_path = self.file_directory / "queue_active.jsonl"
        self.journal_path = self.file_directory / "queue_active.journal"
        self.max_file_size = 100 * 1024 * 1024  # 100MB
        self.max_file_age = 3600  # 1 hour
        self._file_handle = None
        self._journal_handle = None
        self._file_created_at = None
        self._lock = threading.Lock()
        self._rotation_lock_file = self.file_directory / ".rotation.lock"
        
        # Recovery on startup
        self._recover_from_journal()
        self._validate_and_repair_active_file()
        self._open_or_create_file()
    
    def _calculate_checksum(self, data: str) -> str:
        """Calculate CRC32 checksum for data integrity"""
        return format(struct.unpack('>I', struct.pack('>I', 
            json.crc32(data.encode('utf-8')) & 0xffffffff))[0], '08x')
    
    def _write_journal_entry(self, record: dict):
        """Write to journal before actual write for recovery"""
        journal_entry = {
            "timestamp": time.time(),
            "record": record,
            "checksum": self._calculate_checksum(json.dumps(record))
        }
        
        if not self._journal_handle:
            self._journal_handle = open(self.journal_path, 'a', encoding='utf-8')
        
        self._journal_handle.write(json.dumps(journal_entry) + '\n')
        self._journal_handle.flush()
        os.fsync(self._journal_handle.fileno())
    
    def _clear_journal(self):
        """Clear journal after successful write"""
        if self._journal_handle:
            self._journal_handle.close()
            self._journal_handle = None
        
        if self.journal_path.exists():
            self.journal_path.unlink()
    
    def _recover_from_journal(self):
        """Recover incomplete writes from journal on startup"""
        if not self.journal_path.exists():
            return
        
        print(f"Found journal file, recovering incomplete writes...")
        recovered = 0
        
        try:
            with open(self.journal_path, 'r') as journal:
                for line in journal:
                    try:
                        entry = json.loads(line.strip())
                        record = entry["record"]
                        
                        # Verify checksum
                        if self._calculate_checksum(json.dumps(record)) != entry["checksum"]:
                            print(f"Skipping corrupted journal entry")
                            continue
                        
                        # Rewrite to active file
                        with open(self.active_file_path, 'a', encoding='utf-8') as f:
                            # Format with checksum for validation
                            line_data = json.dumps(record)
                            checksum = self._calculate_checksum(line_data)
                            full_line = f"{line_data}|{checksum}\n"
                            
                            f.write(full_line)
                            f.flush()
                            os.fsync(f.fileno())
                        
                        recovered += 1
                        
                    except (json.JSONDecodeError, KeyError):
                        continue
            
            print(f"Recovered {recovered} records from journal")
            
        finally:
            # Clear journal after recovery
            self.journal_path.unlink()
    
    def _validate_and_repair_active_file(self):
        """Validate and repair active file after power failure"""
        if not self.active_file_path.exists():
            return
        
        temp_path = self.active_file_path.with_suffix('.repair')
        corrupted_path = self.active_file_path.with_suffix('.corrupted')
        
        valid_lines = []
        corrupted_lines = []
        
        try:
            with open(self.active_file_path, 'r', encoding='utf-8') as f:
                for line_num, line in enumerate(f, 1):
                    line = line.strip()
                    if not line:
                        continue
                    
                    try:
                        # Check if line has checksum
                        if '|' in line:
                            json_part, checksum = line.rsplit('|', 1)
                            
                            # Verify checksum
                            if self._calculate_checksum(json_part) == checksum:
                                # Valid line with checksum
                                record = json.loads(json_part)
                                valid_lines.append(line + '\n')
                            else:
                                print(f"Line {line_num}: Checksum mismatch")
                                corrupted_lines.append(line + '\n')
                        else:
                            # Legacy format without checksum, try to parse
                            record = json.loads(line)
                            # Add checksum for future
                            checksum = self._calculate_checksum(line)
                            valid_lines.append(f"{line}|{checksum}\n")
                            
                    except json.JSONDecodeError:
                        print(f"Line {line_num}: Invalid JSON")
                        corrupted_lines.append(line + '\n')
                    except Exception as e:
                        print(f"Line {line_num}: {str(e)}")
                        corrupted_lines.append(line + '\n')
            
            if corrupted_lines:
                # Save corrupted lines for manual inspection
                with open(corrupted_path, 'a') as f:
                    f.writelines(corrupted_lines)
                print(f"Saved {len(corrupted_lines)} corrupted lines to {corrupted_path}")
            
            # Write valid lines to temp file
            if valid_lines:
                with open(temp_path, 'w') as f:
                    f.writelines(valid_lines)
                    f.flush()
                    os.fsync(f.fileno())
                
                # Atomic replace
                os.rename(temp_path, self.active_file_path)
                print(f"Repaired active file: {len(valid_lines)} valid lines retained")
                
        except Exception as e:
            print(f"Error during file repair: {str(e)}")
    
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
                    raise Exception("Could not acquire file lock after retries")
        return False
    
    def _release_file_lock(self, file_handle):
        """Release file lock"""
        try:
            fcntl.flock(file_handle.fileno(), fcntl.LOCK_UN)
        except:
            pass
    
    def _open_or_create_file(self):
        """Open the active file for appending with proper locking"""
        if self._file_handle:
            self._release_file_lock(self._file_handle)
            self._file_handle.close()
        
        self._file_handle = open(self.active_file_path, 'a', encoding='utf-8')
        self._acquire_file_lock(self._file_handle, exclusive=True)
        
        # Track file creation time for rotation
        if self.active_file_path.stat().st_size == 0:
            self._file_created_at = time.time()
        else:
            self._file_created_at = self.active_file_path.stat().st_mtime
    
    def _rotate_if_needed(self):
        """Rotate file if it exceeds size or age limits with proper locking"""
        file_size = self._file_handle.tell()
        file_age = time.time() - self._file_created_at
        
        if file_size >= self.max_file_size or file_age >= self.max_file_age:
            rotation_lock = open(self._rotation_lock_file, 'w')
            try:
                self._acquire_file_lock(rotation_lock, exclusive=True)
                
                # Ensure all data is written
                self._file_handle.flush()
                os.fsync(self._file_handle.fileno())
                
                # Also sync the directory to ensure metadata is written
                dir_fd = os.open(self.file_directory, os.O_RDONLY)
                os.fsync(dir_fd)
                os.close(dir_fd)
                
                # Close and release lock on active file
                self._release_file_lock(self._file_handle)
                self._file_handle.close()
                
                # Generate unique rotated filename
                timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
                counter = 1
                while True:
                    rotated_name = f"queue_{timestamp}_{counter:03d}.jsonl"
                    rotated_path = self.file_directory / rotated_name
                    if not rotated_path.exists():
                        break
                    counter += 1
                
                # Atomic rename
                os.rename(self.active_file_path, rotated_path)
                
                # Sync directory again after rename
                dir_fd = os.open(self.file_directory, os.O_RDONLY)
                os.fsync(dir_fd)
                os.close(dir_fd)
                
                # Create new active file
                self._open_or_create_file()
                
                # Clear any journal entries
                self._clear_journal()
                
            finally:
                self._release_file_lock(rotation_lock)
                rotation_lock.close()
    
    def store_data(self, table_name: str, table_data: Dict[str, Any]):
        """
        Store data with power failure protection
        
        Args:
            table_name: Name of the target table
            table_data: Dictionary containing the data to store
            
        Raises:
            Exception: If there's an error writing to file
        """
        with self._lock:
            try:
                record = {
                    "uploaded": False,
                    "time_stamp": int(time.time()),
                    "table_name": table_name,
                    "table_data": table_data,
                    "sequence": int(time.time() * 1000000)  # Microsecond precision sequence
                }
                
                # Write to journal first (for recovery)
                self._write_journal_entry(record)
                
                # Prepare line with checksum
                line_data = json.dumps(record, ensure_ascii=False)
                checksum = self._calculate_checksum(line_data)
                full_line = f"{line_data}|{checksum}\n"
                
                # Write to actual file
                self._file_handle.write(full_line)
                self._file_handle.flush()
                
                # Force write to disk (both data and metadata)
                os.fsync(self._file_handle.fileno())
                
                # Sync directory to ensure file metadata is persisted
                dir_fd = os.open(self.file_directory, os.O_RDONLY)
                os.fsync(dir_fd)
                os.close(dir_fd)
                
                # Clear journal after successful write
                self._clear_journal()
                
                # Check if rotation is needed
                self._rotate_if_needed()
                
            except Exception as e:
                raise Exception(f"Failed to store data to queue file: {str(e)}")
    
    def __del__(self):
        """Cleanup file handles on deletion"""
        if self._journal_handle and not self._journal_handle.closed:
            try:
                self._journal_handle.close()
            except:
                pass
        
        if self._file_handle and not self._file_handle.closed:
            try:
                self._release_file_lock(self._file_handle)
                self._file_handle.close()
            except:
                pass


class PostgresQueueProcessor:
    """Power-failure-resistant class to process queue files"""
    
    def __init__(self, postgres_connect: psycopg2.extensions.connection, file_directory: str):
        self.postgres_conn = postgres_connect
        self.file_directory = Path(file_directory)
        self.table_handlers: Dict[str, Callable] = {}
        self.checkpoint_file = self.file_directory / "checkpoint.json"
        self.checkpoint_backup = self.file_directory / "checkpoint.json.bak"
        self.checkpoint_lock_file = self.file_directory / ".checkpoint.lock"
        self._rotation_lock_file = self.file_directory / ".rotation.lock"
        self.processing_journal = self.file_directory / "processing.journal"
        self._load_checkpoint()
        self._recover_processing_state()
    
    def _recover_processing_state(self):
        """Recover from interrupted processing after power failure"""
        if not self.processing_journal.exists():
            return
        
        print("Found processing journal, recovering state...")
        
        try:
            with open(self.processing_journal, 'r') as f:
                for line in f:
                    try:
                        entry = json.loads(line.strip())
                        if entry["action"] == "processing_file":
                            # File was being processed, reset its state
                            file_name = entry["file"]
                            if file_name in self.checkpoint.get("processed_files", []):
                                self.checkpoint["processed_files"].remove(file_name)
                            self.checkpoint["current_file"] = None
                            self.checkpoint["position"] = 0
                            print(f"Reset processing state for {file_name}")
                    except:
                        continue
            
            # Save recovered checkpoint
            self._save_checkpoint()
            
        finally:
            # Clear journal
            self.processing_journal.unlink()
    
    def _write_processing_journal(self, action: str, data: dict):
        """Write processing state to journal"""
        entry = {
            "timestamp": time.time(),
            "action": action,
            **data
        }
        
        with open(self.processing_journal, 'a') as f:
            f.write(json.dumps(entry) + '\n')
            f.flush()
            os.fsync(f.fileno())
    
    def _clear_processing_journal(self):
        """Clear processing journal"""
        if self.processing_journal.exists():
            self.processing_journal.unlink()
    
    def _calculate_checksum(self, data: str) -> str:
        """Calculate CRC32 checksum for data integrity"""
        return format(struct.unpack('>I', struct.pack('>I', 
            json.crc32(data.encode('utf-8')) & 0xffffffff))[0], '08x')
    
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
    
    def _load_checkpoint(self):
        """Load processing checkpoint with backup recovery"""
        checkpoint_lock = open(self.checkpoint_lock_file, 'w')
        try:
            self._acquire_file_lock(checkpoint_lock, exclusive=False)
            
            # Try main checkpoint file
            if self.checkpoint_file.exists():
                try:
                    with open(self.checkpoint_file, 'r') as f:
                        content = f.read()
                        self.checkpoint = json.loads(content)
                        
                        # Verify checkpoint integrity
                        if "checksum" in self.checkpoint:
                            data = {k: v for k, v in self.checkpoint.items() if k != "checksum"}
                            expected = self.checkpoint["checksum"]
                            actual = self._calculate_checksum(json.dumps(data, sort_keys=True))
                            if expected != actual:
                                raise ValueError("Checkpoint checksum mismatch")
                        
                        return
                except Exception as e:
                    print(f"Main checkpoint corrupted: {e}")
            
            # Try backup checkpoint
            if self.checkpoint_backup.exists():
                try:
                    with open(self.checkpoint_backup, 'r') as f:
                        content = f.read()
                        self.checkpoint = json.loads(content)
                        print("Recovered from backup checkpoint")
                        return
                except Exception as e:
                    print(f"Backup checkpoint also corrupted: {e}")
            
            # Initialize new checkpoint
            self.checkpoint = {"processed_files": [], "current_file": None, "position": 0}
            
        finally:
            self._release_file_lock(checkpoint_lock)
            checkpoint_lock.close()
    
    def _save_checkpoint(self):
        """Save checkpoint with backup and atomic write"""
        checkpoint_lock = open(self.checkpoint_lock_file, 'w')
        try:
            self._acquire_file_lock(checkpoint_lock, exclusive=True)
            
            # Add checksum for integrity
            data = {k: v for k, v in self.checkpoint.items() if k != "checksum"}
            checksum = self._calculate_checksum(json.dumps(data, sort_keys=True))
            self.checkpoint["checksum"] = checksum
            
            # Backup current checkpoint
            if self.checkpoint_file.exists():
                try:
                    with open(self.checkpoint_file, 'rb') as src:
                        with open(self.checkpoint_backup, 'wb') as dst:
                            dst.write(src.read())
                            dst.flush()
                            os.fsync(dst.fileno())
                except:
                    pass
            
            # Write new checkpoint atomically
            temp_checkpoint = self.checkpoint_file.with_suffix('.tmp')
            with open(temp_checkpoint, 'w') as f:
                json.dump(self.checkpoint, f, indent=2)
                f.flush()
                os.fsync(f.fileno())
            
            # Atomic rename
            os.rename(temp_checkpoint, self.checkpoint_file)
            
            # Sync directory
            dir_fd = os.open(self.file_directory, os.O_RDONLY)
            os.fsync(dir_fd)
            os.close(dir_fd)
            
        finally:
            self._release_file_lock(checkpoint_lock)
            checkpoint_lock.close()
    
    def store_table_handler(self, table_name: str, table_handler: Callable):
        """
        Register a handler for a specific table
        
        Args:
            table_name: Name of the table
            table_handler: Function with signature (postgres_conn, table_name, time_stamp, table_data)
        """
        self.table_handlers[table_name] = table_handler
    
    def _get_queue_files(self):
        """Get list of queue files to process"""
        rotation_lock = open(self._rotation_lock_file, 'w')
        try:
            if not self._acquire_file_lock(rotation_lock, exclusive=False):
                return []
            
            files = []
            for file_path in sorted(self.file_directory.glob("queue_*.jsonl")):
                if file_path.name not in self.checkpoint["processed_files"]:
                    files.append(file_path)
            
            # Don't process the active file
            active_file = self.file_directory / "queue_active.jsonl"
            if active_file in files:
                files.remove(active_file)
            
            return files
            
        finally:
            self._release_file_lock(rotation_lock)
            rotation_lock.close()
    
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
                return None, False
        else:
            # Legacy format without checksum
            try:
                return json.loads(line), True
            except json.JSONDecodeError:
                return None, False
    
    def _update_file_uploaded_status(self, file_path: Path, line_positions: list):
        """Update uploaded status with power failure protection"""
        lock_file_path = file_path.with_suffix('.lock')
        lock_file = open(lock_file_path, 'w')
        
        try:
            self._acquire_file_lock(lock_file, exclusive=True)
            
            # Read all lines
            with open(file_path, 'r') as f:
                lines = f.readlines()
            
            # Update uploaded status
            updated_lines = []
            for i, line in enumerate(lines):
                if i in line_positions:
                    record, valid = self._parse_line_with_checksum(line)
                    if valid and record:
                        record["uploaded"] = True
                        line_data = json.dumps(record)
                        checksum = self._calculate_checksum(line_data)
                        updated_lines.append(f"{line_data}|{checksum}\n")
                    else:
                        updated_lines.append(line)  # Keep original if invalid
                else:
                    updated_lines.append(line)
            
            # Write atomically
            temp_path = file_path.with_suffix('.tmp')
            with open(temp_path, 'w') as f:
                f.writelines(updated_lines)
                f.flush()
                os.fsync(f.fileno())
            
            os.rename(temp_path, file_path)
            
            # Sync directory
            dir_fd = os.open(self.file_directory, os.O_RDONLY)
            os.fsync(dir_fd)
            os.close(dir_fd)
            
        finally:
            self._release_file_lock(lock_file)
            lock_file.close()
            try:
                os.unlink(lock_file_path)
            except:
                pass
    
    def store_file_data_to_postgres(self):
        """
        Process queue files with power failure protection
        
        Raises:
            Exception: If table handler is not specified or database error occurs
        """
        try:
            queue_files = self._get_queue_files()
            
            for file_path in queue_files:
                # Journal that we're processing this file
                self._write_processing_journal("processing_file", {"file": file_path.name})
                
                processed_positions = []
                file_lock_path = file_path.with_suffix('.lock')
                file_lock = open(file_lock_path, 'w')
                
                try:
                    if not self._acquire_file_lock(file_lock, exclusive=False):
                        continue
                    
                    with open(file_path, 'r') as f:
                        # Resume from checkpoint
                        if str(file_path) == self.checkpoint.get("current_file"):
                            f.seek(self.checkpoint["position"])
                        
                        for line_num, line in enumerate(f):
                            record, valid = self._parse_line_with_checksum(line)
                            
                            if not valid or not record:
                                continue
                            
                            table_name = record["table_name"]
                            
                            if table_name not in self.table_handlers:
                                raise Exception(f"No handler registered for table: {table_name}")
                            
                            if record.get("uploaded", False):
                                continue
                            
                            # Execute handler
                            handler = self.table_handlers[table_name]
                            handler(
                                self.postgres_conn,
                                table_name,
                                record["time_stamp"],
                                record["table_data"]
                            )
                            
                            processed_positions.append(line_num)
                            
                            # Checkpoint frequently for recovery
                            if len(processed_positions) % 10 == 0:
                                self.checkpoint["current_file"] = str(file_path)
                                self.checkpoint["position"] = f.tell()
                                self._save_checkpoint()
                                # Commit to database
                                self.postgres_conn.commit()
                    
                finally:
                    self._release_file_lock(file_lock)
                    file_lock.close()
                
                # Final commit for this file
                self.postgres_conn.commit()
                
                # Update uploaded status
                if processed_positions:
                    self._update_file_uploaded_status(file_path, processed_positions)
                
                # Mark file as processed
                self.checkpoint["processed_files"].append(file_path.name)
                self.checkpoint["current_file"] = None
                self.checkpoint["position"] = 0
                self._save_checkpoint()
                
                # Clear processing journal
                self._clear_processing_journal()
                
        except Exception as e:
            self.postgres_conn.rollback()
            raise Exception(f"Failed to store data to PostgreSQL: {str(e)}")


class QueueDataReader:
    """Power-failure-resistant class to read queue data"""
    
    def __init__(self, file_directory: str):
        self.file_directory = Path(file_directory)
        self._rotation_lock_file = self.file_directory / ".rotation.lock"
    
    def _calculate_checksum(self, data: str) -> str:
        """Calculate CRC32 checksum for data integrity"""
        return format(struct.unpack('>I', struct.pack('>I', 
            json.crc32(data.encode('utf-8')) & 0xffffffff))[0], '08x')
    
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
