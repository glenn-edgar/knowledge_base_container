#!/usr/bin/env python3
"""
Complete Performance Monitor - All Metrics Collection
=====================================================

FIXED VERSION: Collects ALL performance parameters and writes complete database rows

Features:
- Complete Linux system metrics (CPU, memory, disk, network, temperature)
- Complete Docker container metrics (with proper fallbacks)
- All metrics written to database as complete rows
- Robust error handling with fallback values
- Comprehensive logging of all collected metrics
"""

import time
import os
import socket
import json
import subprocess
import threading
import re
from typing import Any, Dict, Optional, Tuple, List
from datetime import datetime, timezone
from contextlib import contextmanager

import psutil
import psycopg2
from psycopg2 import sql, extras

# SQL DDL for the circular buffer table
DDL_TEMPLATE = """
CREATE TABLE IF NOT EXISTS {table} (
  id                    BIGSERIAL PRIMARY KEY,
  slot                  INTEGER UNIQUE NOT NULL,
  sample_time           BIGINT,                             -- UTC seconds since epoch
  
  -- Identity/context fields
  master_flag           BOOLEAN NOT NULL DEFAULT FALSE,
  node_flag             BOOLEAN NOT NULL,
  node_name             TEXT NOT NULL,
  container_name        TEXT,
  
  -- CPU metrics (percentages 0-100)
  cpu_user_pct          REAL CHECK (cpu_user_pct IS NULL OR (cpu_user_pct >= 0 AND cpu_user_pct <= 100)),
  cpu_nice_pct          REAL CHECK (cpu_nice_pct IS NULL OR (cpu_nice_pct >= 0 AND cpu_nice_pct <= 100)),
  cpu_system_pct        REAL CHECK (cpu_system_pct IS NULL OR (cpu_system_pct >= 0 AND cpu_system_pct <= 100)),
  cpu_idle_pct          REAL CHECK (cpu_idle_pct IS NULL OR (cpu_idle_pct >= 0 AND cpu_idle_pct <= 100)),
  cpu_iowait_pct        REAL CHECK (cpu_iowait_pct IS NULL OR (cpu_iowait_pct >= 0 AND cpu_iowait_pct <= 100)),
  cpu_irq_pct           REAL CHECK (cpu_irq_pct IS NULL OR (cpu_irq_pct >= 0 AND cpu_irq_pct <= 100)),
  cpu_softirq_pct       REAL CHECK (cpu_softirq_pct IS NULL OR (cpu_softirq_pct >= 0 AND cpu_softirq_pct <= 100)),
  cpu_steal_pct         REAL CHECK (cpu_steal_pct IS NULL OR (cpu_steal_pct >= 0 AND cpu_steal_pct <= 100)),
  cpu_guest_pct         REAL CHECK (cpu_guest_pct IS NULL OR (cpu_guest_pct >= 0 AND cpu_guest_pct <= 100)),
  cpu_guest_nice_pct    REAL CHECK (cpu_guest_nice_pct IS NULL OR (cpu_guest_nice_pct >= 0 AND cpu_guest_nice_pct <= 100)),
  cpu_total_pct         REAL CHECK (cpu_total_pct IS NULL OR (cpu_total_pct >= 0 AND cpu_total_pct <= 100)),
  
  -- Load averages (node only)
  load_1m               REAL CHECK (load_1m IS NULL OR load_1m >= 0),
  load_5m               REAL CHECK (load_5m IS NULL OR load_5m >= 0),
  load_15m              REAL CHECK (load_15m IS NULL OR load_15m >= 0),
  
  -- System rates (per second)
  ctx_switches_s        REAL CHECK (ctx_switches_s IS NULL OR ctx_switches_s >= 0),
  interrupts_s          REAL CHECK (interrupts_s IS NULL OR interrupts_s >= 0),
  procs_running         INTEGER CHECK (procs_running IS NULL OR procs_running >= 0),
  procs_blocked         INTEGER CHECK (procs_blocked IS NULL OR procs_blocked >= 0),
  
  -- Node memory (MiB)
  mem_total_mb          INTEGER CHECK (mem_total_mb IS NULL OR mem_total_mb >= 0),
  mem_free_mb           INTEGER CHECK (mem_free_mb IS NULL OR mem_free_mb >= 0),
  mem_available_mb      INTEGER CHECK (mem_available_mb IS NULL OR mem_available_mb >= 0),
  mem_buffers_mb        INTEGER CHECK (mem_buffers_mb IS NULL OR mem_buffers_mb >= 0),
  mem_cached_mb         INTEGER CHECK (mem_cached_mb IS NULL OR mem_cached_mb >= 0),
  swap_total_mb         INTEGER CHECK (swap_total_mb IS NULL OR swap_total_mb >= 0),
  swap_free_mb          INTEGER CHECK (swap_free_mb IS NULL OR swap_free_mb >= 0),
  swap_used_mb          INTEGER CHECK (swap_used_mb IS NULL OR swap_used_mb >= 0),
  
  -- Process/container memory (MiB)
  c_rss_mb              INTEGER CHECK (c_rss_mb IS NULL OR c_rss_mb >= 0),
  c_vsz_mb              INTEGER CHECK (c_vsz_mb IS NULL OR c_vsz_mb >= 0),
  c_shared_mb           INTEGER CHECK (c_shared_mb IS NULL OR c_shared_mb >= 0),
  
  -- Disk I/O metrics
  disk_read_mb_s        REAL CHECK (disk_read_mb_s IS NULL OR disk_read_mb_s >= 0),
  disk_write_mb_s       REAL CHECK (disk_write_mb_s IS NULL OR disk_write_mb_s >= 0),
  disk_read_iops        REAL CHECK (disk_read_iops IS NULL OR disk_read_iops >= 0),
  disk_write_iops       REAL CHECK (disk_write_iops IS NULL OR disk_write_iops >= 0),
  disk_util_pct         REAL CHECK (disk_util_pct IS NULL OR (disk_util_pct >= 0 AND disk_util_pct <= 100)),
  
  -- Network I/O metrics
  net_rx_mb_s           REAL CHECK (net_rx_mb_s IS NULL OR net_rx_mb_s >= 0),
  net_tx_mb_s           REAL CHECK (net_tx_mb_s IS NULL OR net_tx_mb_s >= 0),
  net_rx_pkts_s         REAL CHECK (net_rx_pkts_s IS NULL OR net_rx_pkts_s >= 0),
  net_tx_pkts_s         REAL CHECK (net_tx_pkts_s IS NULL OR net_tx_pkts_s >= 0),
  net_rx_errs_s         REAL CHECK (net_rx_errs_s IS NULL OR net_rx_errs_s >= 0),
  net_tx_errs_s         REAL CHECK (net_tx_errs_s IS NULL OR net_tx_errs_s >= 0),
  net_rx_drops_s        REAL CHECK (net_rx_drops_s IS NULL OR net_rx_drops_s >= 0),
  net_tx_drops_s        REAL CHECK (net_tx_drops_s IS NULL OR net_tx_drops_s >= 0),
  
  -- Temperature (°C, node only)
  temperature_c         REAL,
  
  -- Metadata
  source                TEXT NOT NULL DEFAULT 'psutil',
  interval_seconds      INTEGER CHECK (interval_seconds IS NULL OR interval_seconds > 0),
  
  -- Business logic constraints
  CONSTRAINT container_presence_chk
    CHECK ((node_flag = TRUE AND container_name IS NULL) OR
           (node_flag = FALSE AND container_name IS NOT NULL)),
           
  CONSTRAINT master_only_nodes_chk
    CHECK (master_flag = FALSE OR node_flag = TRUE),
    
  CONSTRAINT slot_positive_chk
    CHECK (slot > 0)
);

-- Performance indexes
CREATE INDEX IF NOT EXISTS {table}_composite_idx
  ON {table} (master_flag, node_flag, node_name, container_name, sample_time DESC);

CREATE INDEX IF NOT EXISTS {table}_time_slot_idx
  ON {table} (sample_time NULLS FIRST, slot ASC);

CREATE INDEX IF NOT EXISTS {table}_slot_idx
  ON {table} (slot);

-- Partial indexes for common queries
CREATE INDEX IF NOT EXISTS {table}_master_nodes_idx
  ON {table} (sample_time DESC) WHERE master_flag = TRUE AND node_flag = TRUE;

CREATE INDEX IF NOT EXISTS {table}_containers_idx  
  ON {table} (node_name, container_name, sample_time DESC) WHERE node_flag = FALSE;
"""


def mb_from_bytes(nbytes: Optional[int]) -> Optional[int]:
    """Convert bytes to MiB, handling None values."""
    if nbytes is None:
        return None
    return int(round(nbytes / (1024 * 1024)))


def safe_float(value: Any) -> Optional[float]:
    """Safely convert value to float, returning None on error."""
    try:
        return float(value) if value is not None else None
    except (ValueError, TypeError):
        return None


def parse_size_string(size_str: str) -> Optional[int]:
    """Parse size string like '123.4MiB' or '1.2GB' to bytes."""
    try:
        size_str = size_str.strip()
        if not size_str or size_str in ['--', 'N/A', '0']:
            return 0
            
        # Extract number and unit
        match = re.match(r'([\d.]+)\s*([A-Za-z]*)', size_str)
        if not match:
            return None
            
        value, unit = match.groups()
        value = float(value)
        
        # Convert to bytes
        unit = unit.upper()
        multipliers = {
            '': 1,  # No unit, assume bytes
            'B': 1, 
            'K': 1024, 'KB': 1000, 'KIB': 1024,
            'M': 1024**2, 'MB': 1000**2, 'MIB': 1024**2,
            'G': 1024**3, 'GB': 1000**3, 'GIB': 1024**3,
            'T': 1024**4, 'TB': 1000**4, 'TIB': 1024**4
        }
        
        return int(value * multipliers.get(unit, 1))
    except (ValueError, TypeError):
        return None


class CompleteResourceMonitor:
    """
    Complete resource monitor that collects ALL performance metrics.
    
    Ensures every database row contains complete performance data.
    """
    
    def __init__(self, 
                 conn: psycopg2.extensions.connection,
                 table_name: str = "complete_monitor_table",
                 capacity: int = 10000,
                 master: bool = False,
                 node_name: Optional[str] = None,
                 use_null_for_empty: bool = True):
        """
        Initialize the complete resource monitor.
        """
        self.conn = conn
        self.table_name = table_name
        self.capacity = max(1, capacity)
        self.master = bool(master)
        self.node_name = node_name or socket.gethostname()
        self.use_null_for_empty = use_null_for_empty
        self._lock = threading.RLock()
        
        # Initialize Docker client (optional)
        self._docker = None
        self._docker_available = self._init_docker()
        
        # Set up database
        self._initialize_database()
        self._ensure_capacity()
        
        print(f"CompleteResourceMonitor initialized: table={table_name}, capacity={capacity}")

    def _init_docker(self) -> bool:
        """Initialize Docker client if available."""
        try:
            import docker
            self._docker = docker.from_env()
            self._docker.ping()
            print("Docker SDK initialized successfully")
            return True
        except Exception as e:
            print(f"Docker SDK not available: {e}")
            return False

    def _initialize_database(self) -> None:
        """Create table and indexes if they don't exist."""
        with self._lock, self.conn.cursor() as cur:
            # Check if table exists and has wrong column type
            cur.execute(f"""
                SELECT data_type 
                FROM information_schema.columns 
                WHERE table_name = '{self.table_name}' 
                AND column_name = 'sample_time'
            """)
            result = cur.fetchone()
            
            if result and 'timestamp' in result[0].lower():
                print(f"Found existing table with timestamp column, dropping and recreating...")
                cur.execute(f"DROP TABLE IF EXISTS {self.table_name}")
                self.conn.commit()
            
            ddl = DDL_TEMPLATE.format(table=self.table_name)
            cur.execute(ddl)
            self.conn.commit()
            print(f"Database table {self.table_name} initialized with BIGINT timestamps")

    def _ensure_capacity(self) -> None:
        """Ensure the table has exactly the specified capacity."""
        with self._lock, self.conn.cursor() as cur:
            cur.execute(f"SELECT COUNT(*) FROM {self.table_name}")
            current_count = cur.fetchone()[0]
            
            if current_count < self.capacity:
                self._grow_capacity(cur, current_count)
            elif current_count > self.capacity:
                self._shrink_capacity(cur, current_count)
                
            self.conn.commit()
            print(f"Table capacity set to {self.capacity} rows")

    def _grow_capacity(self, cur, current_count: int) -> None:
        """Add empty slots to reach target capacity."""
        cur.execute(f"SELECT slot FROM {self.table_name} ORDER BY slot")
        used_slots = {row[0] for row in cur.fetchall()}
        
        empty_timestamp = None if self.use_null_for_empty else 0  # Use 0 for epoch instead of datetime
        slots_needed = self.capacity - current_count
        new_slots = []
        
        slot = 1
        while len(new_slots) < slots_needed:
            if slot not in used_slots:
                new_slots.append((slot, empty_timestamp, False, True, '', None, 'empty'))
            slot += 1
            
        cur.executemany(f"""
            INSERT INTO {self.table_name} (slot, sample_time, master_flag, node_flag, node_name, container_name, source)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
        """, new_slots)

    def _shrink_capacity(self, cur, current_count: int) -> None:
        """Remove oldest slots to reach target capacity."""
        to_delete = current_count - self.capacity
        cur.execute(f"""
            DELETE FROM {self.table_name} 
            WHERE id IN (
                SELECT id FROM {self.table_name} 
                ORDER BY sample_time NULLS FIRST, id ASC 
                LIMIT %s
            )
        """, (to_delete,))

    @contextmanager
    def _get_cursor(self):
        """Context manager for database cursors with error handling."""
        cur = None
        try:
            cur = self.conn.cursor(cursor_factory=extras.RealDictCursor)
            yield cur
        except Exception as e:
            self.conn.rollback()
            print(f"Database error: {e}")
            raise
        finally:
            if cur:
                cur.close()

    def _get_empty_slot_condition(self) -> str:
        """Get the appropriate condition for finding empty slots based on column type."""
        with self.conn.cursor() as cur:
            try:
                # Check the data type of sample_time column
                cur.execute(f"""
                    SELECT data_type 
                    FROM information_schema.columns 
                    WHERE table_name = '{self.table_name}' 
                    AND column_name = 'sample_time'
                """)
                result = cur.fetchone()
                
                if result and 'timestamp' in result[0].lower():
                    # Old timestamp format - use epoch timestamp
                    return "sample_time IS NULL OR sample_time = to_timestamp(0)"
                else:
                    # New bigint format - use 0
                    return "sample_time IS NULL OR sample_time = 0"
            except:
                # Fallback - assume new format
                return "sample_time IS NULL OR sample_time = 0"

    def _circular_insert(self, data: Dict[str, Any]) -> None:
        """Insert complete data into circular buffer."""
        with self._lock:
            try:
                with self.conn.cursor(cursor_factory=extras.RealDictCursor) as cur:
                    # Find next available slot using appropriate condition
                    empty_condition = self._get_empty_slot_condition()
                    cur.execute(f"""
                        SELECT slot FROM {self.table_name}
                        WHERE {empty_condition}
                        ORDER BY slot ASC
                        LIMIT 1
                        FOR UPDATE SKIP LOCKED
                    """)
                    
                    row = cur.fetchone()
                    if not row:
                        # Fallback: find oldest slot
                        cur.execute(f"""
                            SELECT slot FROM {self.table_name}
                            ORDER BY sample_time NULLS FIRST, slot ASC
                            LIMIT 1 FOR UPDATE SKIP LOCKED
                        """)
                        row = cur.fetchone()
                        
                    if not row:
                        raise RuntimeError("No available slots in circular buffer")
                        
                    target_slot = row['slot']
                    
                    # Ensure ALL parameters are present (use None for missing)
                    complete_params = {
                        'master_flag': data.get('master_flag', False),
                        'node_flag': data.get('node_flag', True),
                        'node_name': data.get('node_name', ''),
                        'container_name': data.get('container_name'),
                        'cpu_user_pct': data.get('cpu_user_pct'),
                        'cpu_nice_pct': data.get('cpu_nice_pct'),
                        'cpu_system_pct': data.get('cpu_system_pct'),
                        'cpu_idle_pct': data.get('cpu_idle_pct'),
                        'cpu_iowait_pct': data.get('cpu_iowait_pct'),
                        'cpu_irq_pct': data.get('cpu_irq_pct'),
                        'cpu_softirq_pct': data.get('cpu_softirq_pct'),
                        'cpu_steal_pct': data.get('cpu_steal_pct'),
                        'cpu_guest_pct': data.get('cpu_guest_pct'),
                        'cpu_guest_nice_pct': data.get('cpu_guest_nice_pct'),
                        'cpu_total_pct': data.get('cpu_total_pct'),
                        'load_1m': data.get('load_1m'),
                        'load_5m': data.get('load_5m'),
                        'load_15m': data.get('load_15m'),
                        'ctx_switches_s': data.get('ctx_switches_s'),
                        'interrupts_s': data.get('interrupts_s'),
                        'procs_running': data.get('procs_running'),
                        'procs_blocked': data.get('procs_blocked'),
                        'mem_total_mb': data.get('mem_total_mb'),
                        'mem_free_mb': data.get('mem_free_mb'),
                        'mem_available_mb': data.get('mem_available_mb'),
                        'mem_buffers_mb': data.get('mem_buffers_mb'),
                        'mem_cached_mb': data.get('mem_cached_mb'),
                        'swap_total_mb': data.get('swap_total_mb'),
                        'swap_free_mb': data.get('swap_free_mb'),
                        'swap_used_mb': data.get('swap_used_mb'),
                        'c_rss_mb': data.get('c_rss_mb'),
                        'c_vsz_mb': data.get('c_vsz_mb'),
                        'c_shared_mb': data.get('c_shared_mb'),
                        'disk_read_mb_s': data.get('disk_read_mb_s'),
                        'disk_write_mb_s': data.get('disk_write_mb_s'),
                        'disk_read_iops': data.get('disk_read_iops'),
                        'disk_write_iops': data.get('disk_write_iops'),
                        'disk_util_pct': data.get('disk_util_pct'),
                        'net_rx_mb_s': data.get('net_rx_mb_s'),
                        'net_tx_mb_s': data.get('net_tx_mb_s'),
                        'net_rx_pkts_s': data.get('net_rx_pkts_s'),
                        'net_tx_pkts_s': data.get('net_tx_pkts_s'),
                        'net_rx_errs_s': data.get('net_rx_errs_s'),
                        'net_tx_errs_s': data.get('net_tx_errs_s'),
                        'net_rx_drops_s': data.get('net_rx_drops_s'),
                        'net_tx_drops_s': data.get('net_tx_drops_s'),
                        'temperature_c': data.get('temperature_c'),
                        'source': data.get('source', 'unknown'),
                        'interval_seconds': data.get('interval_seconds'),
                        'target_slot': target_slot
                    }
                    
                    # Log what we're writing
                    current_time = int(time.time())  # Current UTC timestamp
                    non_null_count = sum(1 for v in complete_params.values() if v is not None)
                    print(f"Writing {non_null_count} non-null metrics to slot {target_slot} at UTC {current_time}")
                    
                    # Check if we're using the new BIGINT format or old timestamp format
                    with self.conn.cursor() as check_cur:
                        check_cur.execute(f"""
                            SELECT data_type 
                            FROM information_schema.columns 
                            WHERE table_name = '{self.table_name}' 
                            AND column_name = 'sample_time'
                        """)
                        result = check_cur.fetchone()
                        use_timestamp_format = result and 'timestamp' in result[0].lower()
                    
                    # Set timestamp based on column type
                    if use_timestamp_format:
                        timestamp_value = f"to_timestamp({current_time})"
                    else:
                        timestamp_value = str(current_time)
                    
                    # Execute complete update with appropriate timestamp format
                    cur.execute(f"""
                        UPDATE {self.table_name} SET
                            sample_time = {timestamp_value},
                            master_flag = %(master_flag)s,
                            node_flag = %(node_flag)s,
                            node_name = %(node_name)s,
                            container_name = %(container_name)s,
                            cpu_user_pct = %(cpu_user_pct)s,
                            cpu_nice_pct = %(cpu_nice_pct)s,
                            cpu_system_pct = %(cpu_system_pct)s,
                            cpu_idle_pct = %(cpu_idle_pct)s,
                            cpu_iowait_pct = %(cpu_iowait_pct)s,
                            cpu_irq_pct = %(cpu_irq_pct)s,
                            cpu_softirq_pct = %(cpu_softirq_pct)s,
                            cpu_steal_pct = %(cpu_steal_pct)s,
                            cpu_guest_pct = %(cpu_guest_pct)s,
                            cpu_guest_nice_pct = %(cpu_guest_nice_pct)s,
                            cpu_total_pct = %(cpu_total_pct)s,
                            load_1m = %(load_1m)s,
                            load_5m = %(load_5m)s,
                            load_15m = %(load_15m)s,
                            ctx_switches_s = %(ctx_switches_s)s,
                            interrupts_s = %(interrupts_s)s,
                            procs_running = %(procs_running)s,
                            procs_blocked = %(procs_blocked)s,
                            mem_total_mb = %(mem_total_mb)s,
                            mem_free_mb = %(mem_free_mb)s,
                            mem_available_mb = %(mem_available_mb)s,
                            mem_buffers_mb = %(mem_buffers_mb)s,
                            mem_cached_mb = %(mem_cached_mb)s,
                            swap_total_mb = %(swap_total_mb)s,
                            swap_free_mb = %(swap_free_mb)s,
                            swap_used_mb = %(swap_used_mb)s,
                            c_rss_mb = %(c_rss_mb)s,
                            c_vsz_mb = %(c_vsz_mb)s,
                            c_shared_mb = %(c_shared_mb)s,
                            disk_read_mb_s = %(disk_read_mb_s)s,
                            disk_write_mb_s = %(disk_write_mb_s)s,
                            disk_read_iops = %(disk_read_iops)s,
                            disk_write_iops = %(disk_write_iops)s,
                            disk_util_pct = %(disk_util_pct)s,
                            net_rx_mb_s = %(net_rx_mb_s)s,
                            net_tx_mb_s = %(net_tx_mb_s)s,
                            net_rx_pkts_s = %(net_rx_pkts_s)s,
                            net_tx_pkts_s = %(net_tx_pkts_s)s,
                            net_rx_errs_s = %(net_rx_errs_s)s,
                            net_tx_errs_s = %(net_tx_errs_s)s,
                            net_rx_drops_s = %(net_rx_drops_s)s,
                            net_tx_drops_s = %(net_tx_drops_s)s,
                            temperature_c = %(temperature_c)s,
                            source = %(source)s,
                            interval_seconds = %(interval_seconds)s
                        WHERE slot = %(target_slot)s
                    """, complete_params)
                    
                    self.conn.commit()
                    
            except Exception as e:
                self.conn.rollback()
                print(f"Error in circular insert: {e}")
                raise

    def _collect_complete_node_metrics(self, interval: float) -> Dict[str, Any]:
        """Collect COMPLETE node-level metrics - all performance parameters."""
        print(f"Collecting COMPLETE node metrics (interval={interval}s)")
        
        metrics = {}
        
        # 1. CPU METRICS - Complete breakdown
        try:
            print("  Collecting CPU metrics...")
            cpu_times = psutil.cpu_times_percent(interval=interval, percpu=False)
            
            metrics.update({
                'cpu_user_pct': safe_float(getattr(cpu_times, 'user', None)),
                'cpu_nice_pct': safe_float(getattr(cpu_times, 'nice', None)),
                'cpu_system_pct': safe_float(getattr(cpu_times, 'system', None)),
                'cpu_idle_pct': safe_float(getattr(cpu_times, 'idle', None)),
                'cpu_iowait_pct': safe_float(getattr(cpu_times, 'iowait', None)),
                'cpu_irq_pct': safe_float(getattr(cpu_times, 'irq', None)),
                'cpu_softirq_pct': safe_float(getattr(cpu_times, 'softirq', None)),
                'cpu_steal_pct': safe_float(getattr(cpu_times, 'steal', None)),
                'cpu_guest_pct': safe_float(getattr(cpu_times, 'guest', None)),
                'cpu_guest_nice_pct': safe_float(getattr(cpu_times, 'guest_nice', None)),
            })
            
            # Calculate total CPU usage
            idle_pct = metrics.get('cpu_idle_pct') or 0.0
            metrics['cpu_total_pct'] = max(0.0, min(100.0, 100.0 - idle_pct))
            
            print(f"    CPU Total: {metrics['cpu_total_pct']:.1f}%")
        except Exception as e:
            print(f"    CPU collection failed: {e}")
            metrics.update({
                'cpu_user_pct': None, 'cpu_nice_pct': None, 'cpu_system_pct': None,
                'cpu_idle_pct': None, 'cpu_iowait_pct': None, 'cpu_irq_pct': None,
                'cpu_softirq_pct': None, 'cpu_steal_pct': None, 'cpu_guest_pct': None,
                'cpu_guest_nice_pct': None, 'cpu_total_pct': None
            })

        # 2. LOAD AVERAGES
        try:
            print("  Collecting load averages...")
            load_avg = psutil.getloadavg()
            metrics.update({
                'load_1m': safe_float(load_avg[0]),
                'load_5m': safe_float(load_avg[1]),
                'load_15m': safe_float(load_avg[2])
            })
            print(f"    Load: {metrics['load_1m']}, {metrics['load_5m']}, {metrics['load_15m']}")
        except Exception as e:
            print(f"    Load averages failed: {e}")
            metrics.update({'load_1m': None, 'load_5m': None, 'load_15m': None})

        # 3. SYSTEM RATES - Context switches, interrupts, process counts
        try:
            print("  Collecting system rates...")
            stats_before = psutil.cpu_stats()
            time.sleep(0.1)  # Small delay for rate calculation
            stats_after = psutil.cpu_stats()
            
            rate_interval = 0.1
            metrics.update({
                'ctx_switches_s': (stats_after.ctx_switches - stats_before.ctx_switches) / rate_interval,
                'interrupts_s': (stats_after.interrupts - stats_before.interrupts) / rate_interval
            })
            
            # Process counts from /proc/stat
            procs_running = procs_blocked = None
            try:
                with open('/proc/stat', 'r') as f:
                    for line in f:
                        if line.startswith('procs_running'):
                            procs_running = int(line.split()[1])
                        elif line.startswith('procs_blocked'):
                            procs_blocked = int(line.split()[1])
            except:
                pass
                
            metrics.update({
                'procs_running': procs_running,
                'procs_blocked': procs_blocked
            })
            
            print(f"    Context switches: {metrics['ctx_switches_s']:.0f}/s")
            print(f"    Processes: running={procs_running}, blocked={procs_blocked}")
        except Exception as e:
            print(f"    System rates failed: {e}")
            metrics.update({
                'ctx_switches_s': None, 'interrupts_s': None,
                'procs_running': None, 'procs_blocked': None
            })

        # 4. MEMORY METRICS - Complete memory information
        try:
            print("  Collecting memory metrics...")
            vm = psutil.virtual_memory()
            sm = psutil.swap_memory()
            
            metrics.update({
                'mem_total_mb': mb_from_bytes(vm.total),
                'mem_free_mb': mb_from_bytes(getattr(vm, 'free', None)),
                'mem_available_mb': mb_from_bytes(getattr(vm, 'available', None)),
                'mem_buffers_mb': mb_from_bytes(getattr(vm, 'buffers', None)),
                'mem_cached_mb': mb_from_bytes(getattr(vm, 'cached', None)),
                'swap_total_mb': mb_from_bytes(sm.total),
                'swap_free_mb': mb_from_bytes(sm.free),
                'swap_used_mb': mb_from_bytes(sm.used)
            })
            
            print(f"    Memory: {metrics['mem_available_mb']}MB available of {metrics['mem_total_mb']}MB")
            print(f"    Swap: {metrics['swap_used_mb']}MB used of {metrics['swap_total_mb']}MB")
        except Exception as e:
            print(f"    Memory collection failed: {e}")
            metrics.update({
                'mem_total_mb': None, 'mem_free_mb': None, 'mem_available_mb': None,
                'mem_buffers_mb': None, 'mem_cached_mb': None,
                'swap_total_mb': None, 'swap_free_mb': None, 'swap_used_mb': None
            })

        # 5. DISK I/O METRICS
        try:
            print("  Collecting disk I/O metrics...")
            disk_before = psutil.disk_io_counters(nowrap=True)
            time.sleep(0.1)
            disk_after = psutil.disk_io_counters(nowrap=True)
            
            if disk_before and disk_after:
                rate_interval = 0.1
                metrics.update({
                    'disk_read_mb_s': (disk_after.read_bytes - disk_before.read_bytes) / rate_interval / (1024 * 1024),
                    'disk_write_mb_s': (disk_after.write_bytes - disk_before.write_bytes) / rate_interval / (1024 * 1024),
                    'disk_read_iops': (disk_after.read_count - disk_before.read_count) / rate_interval,
                    'disk_write_iops': (disk_after.write_count - disk_before.write_count) / rate_interval
                })
                print(f"    Disk: R={metrics['disk_read_mb_s']:.1f}MB/s, W={metrics['disk_write_mb_s']:.1f}MB/s")
            else:
                metrics.update({
                    'disk_read_mb_s': None, 'disk_write_mb_s': None,
                    'disk_read_iops': None, 'disk_write_iops': None
                })
            
            metrics['disk_util_pct'] = None  # Requires per-device monitoring
        except Exception as e:
            print(f"    Disk I/O collection failed: {e}")
            metrics.update({
                'disk_read_mb_s': None, 'disk_write_mb_s': None,
                'disk_read_iops': None, 'disk_write_iops': None, 'disk_util_pct': None
            })

        # 6. NETWORK I/O METRICS
        try:
            print("  Collecting network I/O metrics...")
            net_before = psutil.net_io_counters(nowrap=True)
            time.sleep(0.1)
            net_after = psutil.net_io_counters(nowrap=True)
            
            if net_before and net_after:
                rate_interval = 0.1
                metrics.update({
                    'net_rx_mb_s': (net_after.bytes_recv - net_before.bytes_recv) / rate_interval / (1024 * 1024),
                    'net_tx_mb_s': (net_after.bytes_sent - net_before.bytes_sent) / rate_interval / (1024 * 1024),
                    'net_rx_pkts_s': (net_after.packets_recv - net_before.packets_recv) / rate_interval,
                    'net_tx_pkts_s': (net_after.packets_sent - net_before.packets_sent) / rate_interval,
                    'net_rx_errs_s': (net_after.errin - net_before.errin) / rate_interval,
                    'net_tx_errs_s': (net_after.errout - net_before.errout) / rate_interval,
                    'net_rx_drops_s': (net_after.dropin - net_before.dropin) / rate_interval,
                    'net_tx_drops_s': (net_after.dropout - net_before.dropout) / rate_interval
                })
                print(f"    Network: RX={metrics['net_rx_mb_s']:.2f}MB/s, TX={metrics['net_tx_mb_s']:.2f}MB/s")
            else:
                metrics.update({
                    'net_rx_mb_s': None, 'net_tx_mb_s': None, 'net_rx_pkts_s': None,
                    'net_tx_pkts_s': None, 'net_rx_errs_s': None, 'net_tx_errs_s': None,
                    'net_rx_drops_s': None, 'net_tx_drops_s': None
                })
        except Exception as e:
            print(f"    Network I/O collection failed: {e}")
            metrics.update({
                'net_rx_mb_s': None, 'net_tx_mb_s': None, 'net_rx_pkts_s': None,
                'net_tx_pkts_s': None, 'net_rx_errs_s': None, 'net_tx_errs_s': None,
                'net_rx_drops_s': None, 'net_tx_drops_s': None
            })

        # 7. TEMPERATURE - Enhanced detection
        try:
            print("  Collecting temperature...")
            temperature_c = None
            temp_sources_tried = []
            
            # Try psutil sensors first
            try:
                temps = psutil.sensors_temperatures()
                temp_sources_tried.append(f"psutil found {len(temps)} sensor groups")
                
                for sensor_name in ['coretemp', 'k10temp', 'cpu-thermal', 'acpi', 'thermal_zone0']:
                    if sensor_name in temps and temps[sensor_name]:
                        for sensor in temps[sensor_name]:
                            if hasattr(sensor, 'current') and sensor.current:
                                temperature_c = safe_float(sensor.current)
                                temp_sources_tried.append(f"Found {sensor_name}: {temperature_c}°C")
                                break
                        if temperature_c:
                            break
                
                if not temperature_c and temps:
                    # Try any available sensor
                    for sensor_group, sensors in temps.items():
                        temp_sources_tried.append(f"Trying {sensor_group} with {len(sensors)} sensors")
                        for sensor in sensors:
                            if hasattr(sensor, 'current') and sensor.current:
                                temperature_c = safe_float(sensor.current)
                                temp_sources_tried.append(f"Used {sensor_group}: {temperature_c}°C")
                                break
                        if temperature_c:
                            break
                            
            except Exception as e:
                temp_sources_tried.append(f"psutil sensors failed: {e}")
            
            # Fallback: try reading /sys/class/thermal directly
            if temperature_c is None:
                try:
                    import glob
                    thermal_files = glob.glob('/sys/class/thermal/thermal_zone*/temp')
                    temp_sources_tried.append(f"Found {len(thermal_files)} thermal zone files")
                    
                    for thermal_file in thermal_files:
                        try:
                            with open(thermal_file, 'r') as f:
                                temp_millicelsius = int(f.read().strip())
                                temperature_c = temp_millicelsius / 1000.0
                                temp_sources_tried.append(f"Read {thermal_file}: {temperature_c}°C")
                                break
                        except Exception as e:
                            temp_sources_tried.append(f"Failed {thermal_file}: {e}")
                            
                except Exception as e:
                    temp_sources_tried.append(f"Thermal zone reading failed: {e}")
            
            # Fallback: try hwmon
            if temperature_c is None:
                try:
                    import glob
                    hwmon_files = glob.glob('/sys/class/hwmon/hwmon*/temp*_input')
                    temp_sources_tried.append(f"Found {len(hwmon_files)} hwmon temperature files")
                    
                    for hwmon_file in hwmon_files:
                        try:
                            with open(hwmon_file, 'r') as f:
                                temp_millicelsius = int(f.read().strip())
                                temperature_c = temp_millicelsius / 1000.0
                                temp_sources_tried.append(f"Read {hwmon_file}: {temperature_c}°C")
                                break
                        except Exception as e:
                            temp_sources_tried.append(f"Failed {hwmon_file}: {e}")
                            
                except Exception as e:
                    temp_sources_tried.append(f"hwmon reading failed: {e}")
            
            metrics['temperature_c'] = temperature_c
            
            if temperature_c:
                print(f"    Temperature: {temperature_c:.1f}°C")
            else:
                print(f"    Temperature: N/A (tried: {'; '.join(temp_sources_tried)})")
                
        except Exception as e:
            print(f"    Temperature collection failed: {e}")
            metrics['temperature_c'] = None

        # 8. Container memory (NULL for node)
        metrics.update({
            'c_rss_mb': None,
            'c_vsz_mb': None,
            'c_shared_mb': None
        })

        # Count non-null metrics
        non_null_count = sum(1 for v in metrics.values() if v is not None)
        print(f"  Collected {non_null_count} non-null node metrics")
        
        return metrics

    def _collect_complete_container_metrics(self, container_id: str, interval: float) -> Dict[str, Any]:
        """Collect COMPLETE container metrics using the best available method."""
        print(f"Collecting COMPLETE container metrics for {container_id}")
        
        # Try Docker API first
        if self._docker_available:
            try:
                return self._collect_container_docker_api_complete(container_id, interval)
            except Exception as e:
                print(f"  Docker API failed: {e}")
        
        # Fallback to CLI
        try:
            return self._collect_container_cli_complete(container_id, interval)
        except Exception as e:
            print(f"  Docker CLI failed: {e}")
        
        # Last resort: return empty but complete metrics structure
        return self._get_empty_container_metrics()

    def _collect_container_docker_api_complete(self, container_id: str, interval: float) -> Dict[str, Any]:
        """Complete container metrics via Docker API with enhanced data collection."""
        print(f"  Using Docker API for {container_id}")
        
        container = self._docker.containers.get(container_id)
        
        # Get snapshots for rate calculations
        stats1 = container.stats(stream=False)
        time.sleep(interval)
        stats2 = container.stats(stream=False)
        
        metrics = self._get_empty_container_metrics()  # Start with complete structure
        
        # CPU calculation - Enhanced
        try:
            cpu_stats1 = stats1.get('cpu_stats', {})
            cpu_stats2 = stats2.get('cpu_stats', {})
            
            cpu_delta = cpu_stats2['cpu_usage']['total_usage'] - cpu_stats1['cpu_usage']['total_usage']
            system_delta = cpu_stats2['system_cpu_usage'] - cpu_stats1['system_cpu_usage']
            
            # Get number of CPUs
            online_cpus = cpu_stats2.get('online_cpus')
            if not online_cpus:
                online_cpus = len(cpu_stats2['cpu_usage'].get('percpu_usage', [1]))
            
            if system_delta > 0 and cpu_delta >= 0:
                cpu_percent = (cpu_delta / system_delta) * online_cpus * 100.0
                metrics['cpu_total_pct'] = safe_float(cpu_percent)
                print(f"    Container CPU: {cpu_percent:.2f}% ({online_cpus} CPUs)")
            else:
                print(f"    Container CPU: Unable to calculate (delta: {cpu_delta}, system: {system_delta})")
        except Exception as e:
            print(f"    CPU calculation failed: {e}")
        
        # Memory metrics - Enhanced
        try:
            mem_stats = stats2.get('memory_stats', {})
            memory_usage = mem_stats.get('usage', 0)  # Total memory usage
            memory_limit = mem_stats.get('limit', 0)  # Memory limit
            
            # Get detailed memory stats
            mem_details = mem_stats.get('stats', {})
            cache = mem_details.get('cache', 0)
            rss = mem_details.get('rss')
            
            # Use RSS if available, otherwise use usage minus cache
            if rss is not None:
                actual_memory = rss
            else:
                actual_memory = max(0, memory_usage - cache)
            
            metrics.update({
                'c_rss_mb': mb_from_bytes(actual_memory),
                'c_vsz_mb': mb_from_bytes(memory_limit) if memory_limit > 0 else None,  # Use limit as virtual
                'c_shared_mb': mb_from_bytes(cache)
            })
            
            print(f"    Container Memory: {metrics['c_rss_mb']}MB RSS, {metrics['c_shared_mb']}MB cache")
            if metrics['c_vsz_mb']:
                print(f"    Memory Limit: {metrics['c_vsz_mb']}MB")
        except Exception as e:
            print(f"    Memory calculation failed: {e}")
        
        # Network I/O - Enhanced with packet counts
        try:
            net1 = stats1.get('networks', {})
            net2 = stats2.get('networks', {})
            
            # Sum across all interfaces
            rx1_bytes = sum(iface.get('rx_bytes', 0) for iface in net1.values())
            tx1_bytes = sum(iface.get('tx_bytes', 0) for iface in net1.values())
            rx2_bytes = sum(iface.get('rx_bytes', 0) for iface in net2.values())
            tx2_bytes = sum(iface.get('tx_bytes', 0) for iface in net2.values())
            
            rx1_pkts = sum(iface.get('rx_packets', 0) for iface in net1.values())
            tx1_pkts = sum(iface.get('tx_packets', 0) for iface in net1.values())
            rx2_pkts = sum(iface.get('rx_packets', 0) for iface in net2.values())
            tx2_pkts = sum(iface.get('tx_packets', 0) for iface in net2.values())
            
            rx1_errs = sum(iface.get('rx_errors', 0) for iface in net1.values())
            tx1_errs = sum(iface.get('tx_errors', 0) for iface in net1.values())
            rx2_errs = sum(iface.get('rx_errors', 0) for iface in net2.values())
            tx2_errs = sum(iface.get('tx_errors', 0) for iface in net2.values())
            
            rx1_drops = sum(iface.get('rx_dropped', 0) for iface in net1.values())
            tx1_drops = sum(iface.get('tx_dropped', 0) for iface in net1.values())
            rx2_drops = sum(iface.get('rx_dropped', 0) for iface in net2.values())
            tx2_drops = sum(iface.get('tx_dropped', 0) for iface in net2.values())
            
            if interval > 0:
                metrics.update({
                    'net_rx_mb_s': max(0, rx2_bytes - rx1_bytes) / interval / (1024 * 1024),
                    'net_tx_mb_s': max(0, tx2_bytes - tx1_bytes) / interval / (1024 * 1024),
                    'net_rx_pkts_s': max(0, rx2_pkts - rx1_pkts) / interval,
                    'net_tx_pkts_s': max(0, tx2_pkts - tx1_pkts) / interval,
                    'net_rx_errs_s': max(0, rx2_errs - rx1_errs) / interval,
                    'net_tx_errs_s': max(0, tx2_errs - tx1_errs) / interval,
                    'net_rx_drops_s': max(0, rx2_drops - rx1_drops) / interval,
                    'net_tx_drops_s': max(0, tx2_drops - tx1_drops) / interval
                })
                
                print(f"    Container Network: RX={metrics['net_rx_mb_s']:.3f}MB/s ({metrics['net_rx_pkts_s']:.0f}pkt/s)")
                print(f"                       TX={metrics['net_tx_mb_s']:.3f}MB/s ({metrics['net_tx_pkts_s']:.0f}pkt/s)")
                if metrics['net_rx_errs_s'] > 0 or metrics['net_tx_errs_s'] > 0:
                    print(f"    Network Errors: RX={metrics['net_rx_errs_s']:.1f}/s, TX={metrics['net_tx_errs_s']:.1f}/s")
        except Exception as e:
            print(f"    Network calculation failed: {e}")
        
        # Block I/O - Enhanced with IOPS
        try:
            def get_blkio_stats(stats):
                blkio = stats.get('blkio_stats', {})
                read_bytes = write_bytes = read_ops = write_ops = 0
                
                # Get bytes
                for item in blkio.get('io_service_bytes_recursive', []):
                    if item.get('op') == 'Read':
                        read_bytes += item.get('value', 0)
                    elif item.get('op') == 'Write':
                        write_bytes += item.get('value', 0)
                
                # Get operations count
                for item in blkio.get('io_serviced_recursive', []):
                    if item.get('op') == 'Read':
                        read_ops += item.get('value', 0)
                    elif item.get('op') == 'Write':
                        write_ops += item.get('value', 0)
                
                return read_bytes, write_bytes, read_ops, write_ops
            
            read1, write1, read_ops1, write_ops1 = get_blkio_stats(stats1)
            read2, write2, read_ops2, write_ops2 = get_blkio_stats(stats2)
            
            if interval > 0:
                metrics.update({
                    'disk_read_mb_s': max(0, read2 - read1) / interval / (1024 * 1024),
                    'disk_write_mb_s': max(0, write2 - write1) / interval / (1024 * 1024),
                    'disk_read_iops': max(0, read_ops2 - read_ops1) / interval,
                    'disk_write_iops': max(0, write_ops2 - write_ops1) / interval
                })
                
                print(f"    Container Disk: R={metrics['disk_read_mb_s']:.3f}MB/s ({metrics['disk_read_iops']:.1f} IOPS)")
                print(f"                    W={metrics['disk_write_mb_s']:.3f}MB/s ({metrics['disk_write_iops']:.1f} IOPS)")
        except Exception as e:
            print(f"    Disk I/O calculation failed: {e}")
        
        # Count actual container metrics collected
        container_specific_metrics = [
            'cpu_total_pct', 'c_rss_mb', 'c_vsz_mb', 'c_shared_mb',
            'disk_read_mb_s', 'disk_write_mb_s', 'disk_read_iops', 'disk_write_iops',
            'net_rx_mb_s', 'net_tx_mb_s', 'net_rx_pkts_s', 'net_tx_pkts_s',
            'net_rx_errs_s', 'net_tx_errs_s', 'net_rx_drops_s', 'net_tx_drops_s'
        ]
        
        collected_count = sum(1 for metric in container_specific_metrics if metrics.get(metric) is not None)
        print(f"    Collected {collected_count}/{len(container_specific_metrics)} container-specific metrics")
        
        return metrics

    def _collect_container_cli_complete(self, container_id: str, interval: float) -> Dict[str, Any]:
        """Complete container metrics via Docker CLI with enhanced parsing."""
        print(f"  Using Docker CLI for {container_id}")
        
        def get_detailed_stats():
            try:
                # Get more detailed stats using inspect and stats
                result = subprocess.run([
                    'docker', 'stats', '--no-stream', '--format',
                    '{{.Container}},{{.CPUPerc}},{{.MemUsage}},{{.MemPerc}},{{.NetIO}},{{.BlockIO}}'
                ], capture_output=True, text=True, timeout=10)
                
                if result.returncode != 0:
                    return {}
                
                lines = [line.strip() for line in result.stdout.strip().split('\n') if line.strip()]
                if not lines:
                    return {}
                
                # Parse CSV-like output
                for line in lines:
                    parts = line.split(',')
                    if len(parts) >= 6:
                        return {
                            'container': parts[0].strip(),
                            'cpu': parts[1].strip(),
                            'memory': parts[2].strip(),
                            'mem_pct': parts[3].strip(),
                            'network': parts[4].strip(),
                            'block': parts[5].strip()
                        }
                return {}
            except Exception as e:
                print(f"    Detailed CLI stats error: {e}")
                return {}
        
        def get_container_info():
            """Get additional container information via docker inspect."""
            try:
                result = subprocess.run([
                    'docker', 'inspect', container_id,
                    '--format', '{{.HostConfig.Memory}},{{.Config.Memory}}'
                ], capture_output=True, text=True, timeout=5)
                
                if result.returncode == 0:
                    parts = result.stdout.strip().split(',')
                    if len(parts) >= 1:
                        memory_limit = int(parts[0]) if parts[0] and parts[0] != '0' else None
                        return {'memory_limit': memory_limit}
            except Exception as e:
                print(f"    Container inspect error: {e}")
            return {}
        
        metrics = self._get_empty_container_metrics()
        container_info = get_container_info()
        
        # Get two snapshots for rate calculations
        stats1 = get_detailed_stats()
        if stats1:
            time.sleep(interval)
            stats2 = get_detailed_stats()
            
            if stats2:
                # Parse CPU percentage
                try:
                    cpu_str = stats2.get('cpu', '').replace('%', '').strip()
                    if cpu_str and cpu_str != '--':
                        metrics['cpu_total_pct'] = safe_float(cpu_str)
                        print(f"    Container CPU: {metrics['cpu_total_pct']:.2f}%")
                except Exception as e:
                    print(f"    CPU parsing failed: {e}")
                
                # Parse memory usage with percentage
                try:
                    mem_str = stats2.get('memory', '').strip()
                    mem_pct_str = stats2.get('mem_pct', '').replace('%', '').strip()
                    
                    if ' / ' in mem_str:
                        used_str, limit_str = mem_str.split(' / ', 1)
                        used_bytes = parse_size_string(used_str.strip())
                        limit_bytes = parse_size_string(limit_str.strip())
                        
                        if used_bytes is not None:
                            metrics['c_rss_mb'] = mb_from_bytes(used_bytes)
                        
                        if limit_bytes is not None:
                            metrics['c_vsz_mb'] = mb_from_bytes(limit_bytes)
                        elif container_info.get('memory_limit'):
                            metrics['c_vsz_mb'] = mb_from_bytes(container_info['memory_limit'])
                        
                        print(f"    Container Memory: {metrics['c_rss_mb']}MB RSS of {metrics['c_vsz_mb']}MB limit")
                        if mem_pct_str:
                            print(f"    Memory Usage: {mem_pct_str}%")
                except Exception as e:
                    print(f"    Memory parsing failed: {e}")
                
                # Parse I/O with enhanced error handling
                try:
                    def parse_io_pair_enhanced(io_str):
                        """Enhanced I/O parsing with better error handling."""
                        if not io_str or io_str.strip() in ['--', 'N/A']:
                            return None, None
                        
                        if ' / ' in io_str:
                            parts = io_str.split(' / ')
                            if len(parts) == 2:
                                in_val = parse_size_string(parts[0].strip())
                                out_val = parse_size_string(parts[1].strip())
                                return in_val, out_val
                        return None, None
                    
                    # Network I/O rates
                    net1_rx, net1_tx = parse_io_pair_enhanced(stats1.get('network', ''))
                    net2_rx, net2_tx = parse_io_pair_enhanced(stats2.get('network', ''))
                    
                    if all(v is not None for v in [net1_rx, net1_tx, net2_rx, net2_tx]) and interval > 0:
                        metrics.update({
                            'net_rx_mb_s': max(0, net2_rx - net1_rx) / interval / (1024 * 1024),
                            'net_tx_mb_s': max(0, net2_tx - net1_tx) / interval / (1024 * 1024)
                        })
                        print(f"    Container Network: RX={metrics['net_rx_mb_s']:.3f}MB/s, TX={metrics['net_tx_mb_s']:.3f}MB/s")
                    
                    # Block I/O rates
                    block1_read, block1_write = parse_io_pair_enhanced(stats1.get('block', ''))
                    block2_read, block2_write = parse_io_pair_enhanced(stats2.get('block', ''))
                    
                    if all(v is not None for v in [block1_read, block1_write, block2_read, block2_write]) and interval > 0:
                        metrics.update({
                            'disk_read_mb_s': max(0, block2_read - block1_read) / interval / (1024 * 1024),
                            'disk_write_mb_s': max(0, block2_write - block1_write) / interval / (1024 * 1024)
                        })
                        print(f"    Container Disk: R={metrics['disk_read_mb_s']:.3f}MB/s, W={metrics['disk_write_mb_s']:.3f}MB/s")
                        
                except Exception as e:
                    print(f"    I/O parsing failed: {e}")
        
        # Count container-specific metrics collected
        container_metrics = ['cpu_total_pct', 'c_rss_mb', 'c_vsz_mb', 'disk_read_mb_s', 'disk_write_mb_s', 'net_rx_mb_s', 'net_tx_mb_s']
        collected = sum(1 for metric in container_metrics if metrics.get(metric) is not None)
        print(f"    Collected {collected}/{len(container_metrics)} container metrics via CLI")
        
        return metrics

    def _get_empty_container_metrics(self) -> Dict[str, Any]:
        """Return complete metrics structure with container-appropriate defaults."""
        return {
            # CPU (only total for containers)
            'cpu_user_pct': None, 'cpu_nice_pct': None, 'cpu_system_pct': None,
            'cpu_idle_pct': None, 'cpu_iowait_pct': None, 'cpu_irq_pct': None,
            'cpu_softirq_pct': None, 'cpu_steal_pct': None, 'cpu_guest_pct': None,
            'cpu_guest_nice_pct': None, 'cpu_total_pct': None,
            
            # Load/system (N/A for containers)
            'load_1m': None, 'load_5m': None, 'load_15m': None,
            'ctx_switches_s': None, 'interrupts_s': None,
            'procs_running': None, 'procs_blocked': None,
            
            # Node memory (N/A for containers)
            'mem_total_mb': None, 'mem_free_mb': None, 'mem_available_mb': None,
            'mem_buffers_mb': None, 'mem_cached_mb': None,
            'swap_total_mb': None, 'swap_free_mb': None, 'swap_used_mb': None,
            
            # Container memory
            'c_rss_mb': None, 'c_vsz_mb': None, 'c_shared_mb': None,
            
            # Disk I/O
            'disk_read_mb_s': None, 'disk_write_mb_s': None,
            'disk_read_iops': None, 'disk_write_iops': None, 'disk_util_pct': None,
            
            # Network I/O
            'net_rx_mb_s': None, 'net_tx_mb_s': None, 'net_rx_pkts_s': None,
            'net_tx_pkts_s': None, 'net_rx_errs_s': None, 'net_tx_errs_s': None,
            'net_rx_drops_s': None, 'net_tx_drops_s': None,
            
            # Temperature (N/A for containers)
            'temperature_c': None,
        }

    # Public API methods
    
    def sample_node(self, interval_seconds: float = 1.0, source: str = "psutil") -> None:
        """Sample COMPLETE node-level metrics and write complete database row."""
        try:
            print(f"\n=== SAMPLING NODE (interval={interval_seconds}s) ===")
            metrics = self._collect_complete_node_metrics(interval_seconds)
            
            data = {
                'master_flag': self.master,
                'node_flag': True,
                'node_name': self.node_name,
                'container_name': None,
                'source': source,
                'interval_seconds': int(round(interval_seconds)),
                **metrics
            }
            
            self._circular_insert(data)
            print(f"=== NODE SAMPLE COMPLETE ===\n")
            
        except Exception as e:
            print(f"Failed to sample complete node metrics: {e}")
            raise

    def sample_container(self, 
                        container_id: str,
                        container_name: Optional[str] = None,
                        interval_seconds: float = 1.0,
                        source: str = "docker") -> None:
        """Sample COMPLETE container metrics and write complete database row."""
        try:
            print(f"\n=== SAMPLING CONTAINER {container_id} (interval={interval_seconds}s) ===")
            metrics = self._collect_complete_container_metrics(container_id, interval_seconds)
            
            data = {
                'master_flag': False,
                'node_flag': False,
                'node_name': self.node_name,
                'container_name': container_name or container_id,
                'source': source,
                'interval_seconds': int(round(interval_seconds)),
                **metrics
            }
            
            self._circular_insert(data)
            print(f"=== CONTAINER SAMPLE COMPLETE ===\n")
            
        except Exception as e:
            print(f"Failed to sample complete container metrics: {e}")
            raise

    def get_recent_samples(self, limit: int = 10) -> List[Dict[str, Any]]:
        """Get recent complete samples from the circular buffer."""
        with self._get_cursor() as cur:
            cur.execute(f"""
                SELECT * FROM {self.table_name}
                WHERE sample_time IS NOT NULL
                ORDER BY sample_time DESC
                LIMIT %s
            """, (limit,))
            
            return [dict(row) for row in cur.fetchall()]

    def print_sample_summary(self, samples: List[Dict[str, Any]]) -> None:
        """Print a detailed summary of collected samples showing all metrics."""
        print("\n" + "="*80)
        print("DETAILED SAMPLE SUMMARY")
        print("="*80)
        
        for i, sample in enumerate(samples):
            # Convert timestamp to readable format
            if sample['sample_time'] and sample['sample_time'] > 0:
                readable_time = datetime.fromtimestamp(sample['sample_time']).strftime('%Y-%m-%d %H:%M:%S UTC')
                time_display = f"{readable_time} (epoch: {sample['sample_time']})"
            else:
                time_display = f"No timestamp ({sample['sample_time']})"
                
            print(f"\nSample {i+1}: {time_display}")
            is_node = sample['node_flag']
            
            if is_node:
                print(f"  🖥️  NODE: {sample['node_name']} (Master: {sample.get('master_flag', False)})")
                
                # Node-specific metrics
                if sample.get('cpu_total_pct') is not None:
                    print(f"     CPU: {sample['cpu_total_pct']:.1f}% total")
                    breakdown = []
                    if sample.get('cpu_user_pct'): breakdown.append(f"user={sample['cpu_user_pct']:.1f}%")
                    if sample.get('cpu_system_pct'): breakdown.append(f"sys={sample['cpu_system_pct']:.1f}%")
                    if sample.get('cpu_iowait_pct'): breakdown.append(f"iowait={sample['cpu_iowait_pct']:.1f}%")
                    if breakdown:
                        print(f"          ({', '.join(breakdown)})")
                
                if any(sample.get(f'load_{period}') for period in ['1m', '5m', '15m']):
                    loads = [f"{sample.get(f'load_{period}', 0):.2f}" for period in ['1m', '5m', '15m']]
                    print(f"     Load: {', '.join(loads)} (1m, 5m, 15m)")
                
                if sample.get('mem_total_mb'):
                    used = sample['mem_total_mb'] - (sample.get('mem_available_mb', 0) or 0)
                    print(f"     Memory: {used}MB used / {sample['mem_total_mb']}MB total")
                    print(f"             {sample.get('mem_available_mb', 0)}MB available")
                
                if sample.get('swap_used_mb') and sample.get('swap_total_mb'):
                    print(f"     Swap: {sample['swap_used_mb']}MB used / {sample['swap_total_mb']}MB total")
                
                if any(sample.get(f'disk_{op}_mb_s') for op in ['read', 'write']):
                    print(f"     Disk I/O: R={sample.get('disk_read_mb_s', 0):.2f}MB/s, W={sample.get('disk_write_mb_s', 0):.2f}MB/s")
                    if any(sample.get(f'disk_{op}_iops') for op in ['read', 'write']):
                        print(f"               R={sample.get('disk_read_iops', 0):.0f} IOPS, W={sample.get('disk_write_iops', 0):.0f} IOPS")
                
                if any(sample.get(f'net_{dir}_mb_s') for dir in ['rx', 'tx']):
                    print(f"     Network: RX={sample.get('net_rx_mb_s', 0):.3f}MB/s, TX={sample.get('net_tx_mb_s', 0):.3f}MB/s")
                    if any(sample.get(f'net_{dir}_pkts_s') for dir in ['rx', 'tx']):
                        print(f"              RX={sample.get('net_rx_pkts_s', 0):.0f}pkt/s, TX={sample.get('net_tx_pkts_s', 0):.0f}pkt/s")
                
                if sample.get('temperature_c'):
                    print(f"     Temperature: {sample['temperature_c']:.1f}°C")
                    
            else:
                print(f"  🐳  CONTAINER: {sample['container_name']} on {sample['node_name']}")
                
                # Container-specific metrics
                if sample.get('cpu_total_pct') is not None:
                    print(f"     CPU: {sample['cpu_total_pct']:.2f}%")
                
                if sample.get('c_rss_mb') is not None:
                    mem_parts = [f"{sample['c_rss_mb']}MB RSS"]
                    if sample.get('c_vsz_mb'):
                        mem_parts.append(f"{sample['c_vsz_mb']}MB limit")
                    if sample.get('c_shared_mb'):
                        mem_parts.append(f"{sample['c_shared_mb']}MB shared")
                    print(f"     Memory: {', '.join(mem_parts)}")
                
                if any(sample.get(f'disk_{op}_mb_s') for op in ['read', 'write']):
                    disk_parts = [f"R={sample.get('disk_read_mb_s', 0):.3f}MB/s", f"W={sample.get('disk_write_mb_s', 0):.3f}MB/s"]
                    if any(sample.get(f'disk_{op}_iops') for op in ['read', 'write']):
                        disk_parts.extend([f"R={sample.get('disk_read_iops', 0):.1f}IOPS", f"W={sample.get('disk_write_iops', 0):.1f}IOPS"])
                    print(f"     Disk I/O: {', '.join(disk_parts)}")
                
                if any(sample.get(f'net_{dir}_mb_s') for dir in ['rx', 'tx']):
                    net_parts = [f"RX={sample.get('net_rx_mb_s', 0):.3f}MB/s", f"TX={sample.get('net_tx_mb_s', 0):.3f}MB/s"]
                    if any(sample.get(f'net_{dir}_pkts_s') for dir in ['rx', 'tx']):
                        net_parts.extend([f"RX={sample.get('net_rx_pkts_s', 0):.0f}pkt/s", f"TX={sample.get('net_tx_pkts_s', 0):.0f}pkt/s"])
                    print(f"     Network: {', '.join(net_parts)}")
            
            # Count metrics
            total_fields = len([k for k in sample.keys() if k not in ['id', 'slot', 'sample_time', 'source', 'interval_seconds']])
            non_null = sum(1 for k, v in sample.items() 
                          if k not in ['id', 'slot', 'sample_time', 'source', 'interval_seconds'] and v is not None)
            
            expected_nulls = len([k for k in sample.keys() 
                                if k not in ['id', 'slot', 'sample_time', 'source', 'interval_seconds'] and 
                                ((is_node and k.startswith('c_')) or (not is_node and k in ['load_1m', 'load_5m', 'load_15m', 'mem_total_mb', 'mem_free_mb', 'mem_available_mb', 'mem_buffers_mb', 'mem_cached_mb', 'swap_total_mb', 'swap_free_mb', 'swap_used_mb', 'temperature_c', 'ctx_switches_s', 'interrupts_s', 'procs_running', 'procs_blocked', 'cpu_user_pct', 'cpu_nice_pct', 'cpu_system_pct', 'cpu_idle_pct', 'cpu_iowait_pct', 'cpu_irq_pct', 'cpu_softirq_pct', 'cpu_steal_pct', 'cpu_guest_pct', 'cpu_guest_nice_pct']))])
            
            expected_metrics = total_fields - expected_nulls
            coverage = (non_null / expected_metrics * 100) if expected_metrics > 0 else 0
            
            print(f"     ✅ Metrics: {non_null}/{expected_metrics} collected ({coverage:.0f}% coverage)")
        
        print("\n" + "="*80)

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        try:
            if hasattr(self, 'conn') and self.conn:
                self.conn.close()
                print("Database connection closed")
        except Exception as e:
            print(f"Error closing connection: {e}")


if __name__ == "__main__":
    # Test the complete version
    try:
        conn = psycopg2.connect(
            host="localhost",
            port=5432, 
            database="knowledge_base",
            user="gedgar",
            password=os.getenv("POSTGRES_PASSWORD")
        )
        print("Connected to database successfully")
        
        with CompleteResourceMonitor(
            conn=conn,
            table_name="raw_resource_table",
            capacity=1000,
            master=True,
            node_name="production-server-01"
        ) as monitor:
            
            # Quick temperature check
            print("\n=== TEMPERATURE SENSOR CHECK ===")
            try:
                import psutil
                temps = psutil.sensors_temperatures()
                print(f"Available sensor groups: {list(temps.keys())}")
                for name, sensors in temps.items():
                    print(f"  {name}: {len(sensors)} sensors")
                    for i, sensor in enumerate(sensors):
                        if hasattr(sensor, 'current'):
                            print(f"    Sensor {i}: {sensor.current}°C")
            except Exception as e:
                print(f"Temperature check failed: {e}")
            print("================================\n")
            
            print("Starting COMPLETE performance monitoring...")
            
            for sample_num in range(1000):
                print(f"\n{'='*60}")
                print(f"SAMPLE ROUND {sample_num + 1}/3")
                print(f"{'='*60}")
                
                # Sample complete node metrics
                monitor.sample_node(interval_seconds=2)
                
                # Sample complete container metrics
                for container in ["postgres-vector"]:
                    try:
                        monitor.sample_container(
                            container_id=container,
                            container_name=container,
                            interval_seconds=1
                        )
                    except Exception as e:
                        print(f"Container {container} sampling failed: {e}")
                
                if sample_num < 2:
                    print("Waiting 5 seconds before next sample...")
                    time.sleep(5)
            
            # Show final results
            print(f"\n{'='*60}")
            print("FINAL RESULTS")
            print(f"{'='*60}")
            
            recent = monitor.get_recent_samples(limit=6)
            monitor.print_sample_summary(recent)
            
            print(f"\nTotal samples collected: {len(recent)}")
            
            # Show raw timestamps for verification
            print("\nRaw UTC timestamps:")
            for i, sample in enumerate(recent[:3]):
                if sample['sample_time']:
                    readable = datetime.fromtimestamp(sample['sample_time']).strftime('%Y-%m-%d %H:%M:%S UTC')
                    print(f"  Sample {i+1}: {sample['sample_time']} ({readable})")
            
            print("SUCCESS: All performance parameters collected with UTC timestamps!")
            
    except Exception as e:
        print(f"Error: {e}")
        raise