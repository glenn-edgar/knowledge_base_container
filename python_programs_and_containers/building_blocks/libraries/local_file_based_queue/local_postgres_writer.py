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
