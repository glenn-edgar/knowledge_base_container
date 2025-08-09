# Complete Performance Monitor

A comprehensive system and container performance monitoring solution with circular buffer design for PostgreSQL storage.

## Overview

The Complete Performance Monitor collects detailed system metrics (CPU, memory, disk, network, temperature) and Docker container metrics, storing them in a fixed-size PostgreSQL circular buffer. This ensures constant memory usage while maintaining high-resolution recent performance data.

## Features

### 🖥️ **System Monitoring**
- **Complete CPU breakdown**: user, nice, system, idle, iowait, irq, softirq, steal, guest
- **Memory metrics**: total, free, available, buffers, cached, swap usage
- **Load averages**: 1m, 5m, 15m
- **System rates**: context switches/sec, interrupts/sec, process counts
- **Disk I/O**: read/write throughput (MB/s) and IOPS
- **Network I/O**: throughput, packet rates, errors, drops
- **Temperature**: CPU temperature from multiple sensor sources

### 🐳 **Container Monitoring**
- **Docker integration**: API and CLI fallback support
- **Container CPU**: usage percentage
- **Container memory**: RSS, virtual memory limits, shared memory
- **Container I/O**: disk and network throughput with rates
- **Enhanced metrics**: packet counts, error rates, IOPS

### 🔄 **Circular Buffer Design**
- **Fixed table size**: automatically manages capacity
- **Automatic recycling**: overwrites oldest data when full
- **Thread-safe operations**: concurrent access support
- **UTC timestamps**: numeric epoch timestamps for easy querying

### 📊 **Database Features**
- **PostgreSQL storage**: optimized with proper indexes
- **Complete data rows**: every insert contains all available metrics
- **Flexible capacity**: adjustable buffer size
- **Query-friendly**: easy filtering and time-based queries

## Installation

### Prerequisites
```bash
# Python dependencies
pip install psutil psycopg2-binary docker

# Optional: Docker SDK (CLI fallback available)
pip install docker
```

### PostgreSQL Setup
```sql
-- Create database and user
CREATE DATABASE monitoring;
CREATE USER monitor_user WITH PASSWORD 'secure_password';
GRANT ALL PRIVILEGES ON DATABASE monitoring TO monitor_user;
```

## Quick Start

### Basic Usage
```python
import psycopg2
from complete_performance_monitor import CompleteResourceMonitor

# Database connection
conn = psycopg2.connect(
    host="localhost",
    database="monitoring", 
    user="monitor_user",
    password="secure_password"
)

# Initialize monitor with 1000-sample circular buffer
with CompleteResourceMonitor(
    conn=conn,
    table_name="performance_metrics",
    capacity=1000,
    master=True,
    node_name="server-01"
) as monitor:
    
    # Sample system metrics
    monitor.sample_node(interval_seconds=2.0)
    
    # Sample container metrics
    monitor.sample_container(
        container_id="my-app",
        container_name="web-server",
        interval_seconds=1.0
    )
    
    # Get recent samples
    recent = monitor.get_recent_samples(limit=10)
    monitor.print_sample_summary(recent)
```

### Command Line Usage
```bash
# Basic monitoring
python resource_monitor.py \
    --host localhost \
    --database monitoring \
    --user monitor_user \
    --password secure_password \
    --capacity 5000 \
    --master

# Monitor specific containers
python resource_monitor.py \
    --host localhost \
    --database monitoring \
    --user monitor_user \
    --password secure_password \
    --container web-server \
    --container database \
    --samples 10 \
    --interval 5
```

## Configuration

### Constructor Parameters

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `conn` | psycopg2.connection | Required | Active PostgreSQL connection |
| `table_name` | str | `"monitor_samples_ring"` | Table name for metrics storage |
| `capacity` | int | `10000` | Maximum rows in circular buffer |
| `master` | bool | `False` | Mark this node as master |
| `node_name` | str | hostname | Node identifier |
| `use_null_for_empty` | bool | `True` | Use NULL vs 0 for empty slots |

### Environment Variables
```bash
# PostgreSQL password (alternative to --password)
export POSTGRES_PASSWORD=secure_password

# Docker configuration
export DOCKER_HOST=unix:///var/run/docker.sock
```

## Database Schema

### Table Structure
```sql
CREATE TABLE monitor_samples_ring (
  id                    BIGSERIAL PRIMARY KEY,
  slot                  INTEGER UNIQUE NOT NULL,        -- Circular buffer slot
  sample_time           BIGINT,                          -- UTC epoch timestamp
  
  -- Identity
  master_flag           BOOLEAN NOT NULL DEFAULT FALSE,
  node_flag             BOOLEAN NOT NULL,                -- true=node, false=container
  node_name             TEXT NOT NULL,
  container_name        TEXT,
  
  -- CPU metrics (percentages 0-100)
  cpu_user_pct          REAL,
  cpu_nice_pct          REAL,
  cpu_system_pct        REAL,
  cpu_idle_pct          REAL,
  cpu_iowait_pct        REAL,
  cpu_irq_pct           REAL,
  cpu_softirq_pct       REAL,
  cpu_steal_pct         REAL,
  cpu_guest_pct         REAL,
  cpu_guest_nice_pct    REAL,
  cpu_total_pct         REAL,
  
  -- Load and system rates
  load_1m               REAL,
  load_5m               REAL,
  load_15m              REAL,
  ctx_switches_s        REAL,
  interrupts_s          REAL,
  procs_running         INTEGER,
  procs_blocked         INTEGER,
  
  -- Node memory (MiB)
  mem_total_mb          INTEGER,
  mem_free_mb           INTEGER,
  mem_available_mb      INTEGER,
  mem_buffers_mb        INTEGER,
  mem_cached_mb         INTEGER,
  swap_total_mb         INTEGER,
  swap_free_mb          INTEGER,
  swap_used_mb          INTEGER,
  
  -- Container memory (MiB)
  c_rss_mb              INTEGER,                          -- Resident set size
  c_vsz_mb              INTEGER,                          -- Virtual memory
  c_shared_mb           INTEGER,                          -- Shared memory
  
  -- Disk I/O
  disk_read_mb_s        REAL,
  disk_write_mb_s       REAL,
  disk_read_iops        REAL,
  disk_write_iops       REAL,
  disk_util_pct         REAL,
  
  -- Network I/O
  net_rx_mb_s           REAL,
  net_tx_mb_s           REAL,
  net_rx_pkts_s         REAL,
  net_tx_pkts_s         REAL,
  net_rx_errs_s         REAL,
  net_tx_errs_s         REAL,
  net_rx_drops_s        REAL,
  net_tx_drops_s        REAL,
  
  -- Temperature
  temperature_c         REAL,
  
  -- Metadata
  source                TEXT NOT NULL DEFAULT 'psutil',
  interval_seconds      INTEGER
);
```

### Indexes
```sql
-- Composite index for time-based queries
CREATE INDEX monitor_samples_ring_composite_idx
  ON monitor_samples_ring (master_flag, node_flag, node_name, container_name, sample_time DESC);

-- Time-slot index for circular buffer management
CREATE INDEX monitor_samples_ring_time_slot_idx
  ON monitor_samples_ring (sample_time NULLS FIRST, slot ASC);

-- Partial indexes for common queries
CREATE INDEX monitor_samples_ring_master_nodes_idx
  ON monitor_samples_ring (sample_time DESC) 
  WHERE master_flag = TRUE AND node_flag = TRUE;

CREATE INDEX monitor_samples_ring_containers_idx  
  ON monitor_samples_ring (node_name, container_name, sample_time DESC) 
  WHERE node_flag = FALSE;
```

## Query Examples

### Recent System Metrics
```sql
-- Last hour of node metrics
SELECT sample_time, cpu_total_pct, mem_available_mb, temperature_c
FROM monitor_samples_ring 
WHERE node_flag = TRUE 
  AND sample_time > (EXTRACT(epoch FROM NOW()) - 3600)
ORDER BY sample_time DESC;
```

### Container Performance
```sql
-- Container memory usage over time
SELECT 
  sample_time,
  container_name,
  c_rss_mb as memory_mb,
  cpu_total_pct,
  net_rx_mb_s + net_tx_mb_s as network_mb_s
FROM monitor_samples_ring 
WHERE node_flag = FALSE 
  AND container_name = 'web-server'
ORDER BY sample_time DESC 
LIMIT 100;
```

### System Overview
```sql
-- Latest metrics from all nodes
SELECT DISTINCT ON (node_name, container_name)
  node_name,
  container_name,
  cpu_total_pct,
  CASE 
    WHEN node_flag THEN mem_available_mb 
    ELSE c_rss_mb 
  END as memory_mb,
  temperature_c
FROM monitor_samples_ring 
WHERE sample_time IS NOT NULL
ORDER BY node_name, container_name, sample_time DESC;
```

### Time Range Analysis
```sql
-- Average CPU usage in the last 6 hours
SELECT 
  node_name,
  AVG(cpu_total_pct) as avg_cpu,
  MAX(cpu_total_pct) as max_cpu,
  COUNT(*) as samples
FROM monitor_samples_ring 
WHERE node_flag = TRUE 
  AND sample_time > (EXTRACT(epoch FROM NOW()) - 21600)
GROUP BY node_name;
```

## API Reference

### Core Methods

#### `sample_node(interval_seconds=1.0, source="psutil")`
Collect complete system metrics and store in database.

**Parameters:**
- `interval_seconds` (float): Sampling interval for rate calculations
- `source` (str): Source identifier for this sample

**Example:**
```python
monitor.sample_node(interval_seconds=2.0)
```

#### `sample_container(container_id, container_name=None, interval_seconds=1.0, source="docker")`
Collect complete container metrics and store in database.

**Parameters:**
- `container_id` (str): Docker container ID or name
- `container_name` (str, optional): Display name for the container
- `interval_seconds` (float): Sampling interval for rate calculations  
- `source` (str): Source identifier for this sample

**Example:**
```python
monitor.sample_container(
    container_id="nginx-web",
    container_name="web-server",
    interval_seconds=1.0
)
```

#### `get_recent_samples(limit=100, node_filter=None, container_filter=None)`
Retrieve recent samples from the circular buffer.

**Parameters:**
- `limit` (int): Maximum number of samples to return
- `node_filter` (str, optional): Filter by node name
- `container_filter` (str, optional): Filter by container name

**Returns:** List of sample dictionaries

**Example:**
```python
# Get last 50 samples from specific node
recent = monitor.get_recent_samples(
    limit=50, 
    node_filter="server-01"
)
```

#### `set_capacity(new_capacity)`
Change the circular buffer capacity.

**Parameters:**
- `new_capacity` (int): New capacity (must be > 0)

**Example:**
```python
monitor.set_capacity(5000)  # Resize to 5000 samples
```

#### `cleanup_old_samples(keep_hours=24)`
Clean up samples older than specified hours.

**Parameters:**
- `keep_hours` (int): Hours of data to keep

**Returns:** Number of samples cleaned up

**Example:**
```python
cleaned = monitor.cleanup_old_samples(keep_hours=12)
print(f"Cleaned {cleaned} old samples")
```

### Utility Methods

#### `print_sample_summary(samples)`
Print detailed summary of collected samples.

#### `debug_table_state()`
Show current circular buffer state and statistics.

## Docker Integration

### Docker API (Preferred)
When Docker SDK is available, the monitor uses the Docker API for comprehensive metrics:
- Accurate CPU usage calculation with multi-core normalization
- Detailed memory breakdown (RSS, cache, limits)
- Complete network I/O with packet counts and error rates
- Block I/O with both throughput and IOPS

### CLI Fallback
When Docker SDK is unavailable, falls back to `docker stats` CLI:
- Basic CPU and memory metrics
- Network and disk I/O throughput
- Memory limits via `docker inspect`

### Container Detection
```bash
# List running containers
docker ps --format "table {{.Names}}\t{{.Image}}\t{{.Status}}"

# Monitor specific containers
monitor.sample_container("nginx-web")
monitor.sample_container("postgres-db") 
monitor.sample_container("redis-cache")
```

## Performance Considerations

### Sampling Intervals
- **Node metrics**: 1-5 seconds for system monitoring
- **Container metrics**: 1-3 seconds for application monitoring
- **High-frequency**: Sub-second for performance debugging

### Buffer Sizing
- **Small systems**: 1,000-5,000 samples (~1-5 hours at 1-second intervals)
- **Production systems**: 10,000-50,000 samples (1-2 days of data)
- **Large deployments**: 100,000+ samples with multiple monitors

### Database Optimization
```sql
-- Analyze table periodically
ANALYZE monitor_samples_ring;

-- Consider partitioning for very large datasets
-- Consider UNLOGGED tables for maximum performance (no crash recovery)
```

## Troubleshooting

### Common Issues

#### Temperature Sensors Not Working
```bash
# Check available sensors
ls /sys/class/thermal/thermal_zone*/temp
ls /sys/class/hwmon/hwmon*/temp*_input

# Test sensor access
cat /sys/class/thermal/thermal_zone0/temp
```

**Solutions:**
- Install `lm-sensors`: `sudo apt install lm-sensors`
- Run sensor detection: `sudo sensors-detect`
- Check permissions: `sudo chmod 644 /sys/class/thermal/thermal_zone*/temp`

#### Docker Permission Issues
```bash
# Add user to docker group
sudo usermod -aG docker $USER
# Logout and login again

# Or use sudo for docker commands
```

#### PostgreSQL Connection Issues
```bash
# Check PostgreSQL service
sudo systemctl status postgresql

# Test connection
psql -h localhost -U monitor_user -d monitoring

# Check pg_hba.conf for authentication
sudo nano /etc/postgresql/*/main/pg_hba.conf
```

#### Missing Dependencies
```bash
# Install all Python dependencies
pip install psutil psycopg2-binary docker

# For Ubuntu/Debian
sudo apt install python3-psutil python3-psycopg2

# For RHEL/CentOS
sudo yum install python3-psutil python3-psycopg2
```

### Performance Issues

#### High CPU Usage
- Increase sampling intervals
- Reduce number of monitored containers
- Use CLI fallback instead of Docker API

#### Memory Issues
- Reduce circular buffer capacity
- Use UNLOGGED tables for better performance
- Consider table partitioning

#### Disk I/O Issues
- Move database to faster storage
- Tune PostgreSQL settings
- Batch multiple samples in transactions

### Debugging

#### Enable Verbose Logging
```python
import logging
logging.basicConfig(level=logging.DEBUG)

# Or add debug prints
monitor.debug_table_state()
```

#### Check Database Performance
```sql
-- Check table size
SELECT pg_size_pretty(pg_total_relation_size('monitor_samples_ring'));

-- Check index usage
SELECT schemaname, tablename, indexname, idx_scan 
FROM pg_stat_user_indexes 
WHERE tablename = 'monitor_samples_ring';

-- Monitor query performance
EXPLAIN ANALYZE SELECT * FROM monitor_samples_ring 
WHERE sample_time > (EXTRACT(epoch FROM NOW()) - 3600);
```

## Production Deployment

### Systemd Service
```ini
[Unit]
Description=Performance Monitor
After=network.target postgresql.service docker.service

[Service]
Type=simple
User=monitor
WorkingDirectory=/opt/performance-monitor
ExecStart=/usr/bin/python3 resource_monitor.py --config /etc/performance-monitor/config.yaml
Restart=always
RestartSec=10

[Install]
WantedBy=multi-user.target
```

### Configuration File
```yaml
# /etc/performance-monitor/config.yaml
database:
  host: localhost
  port: 5432
  database: monitoring
  user: monitor_user
  password_env: POSTGRES_PASSWORD

monitoring:
  table_name: performance_metrics
  capacity: 50000
  node_name: "{{ ansible_hostname }}"
  master: false
  
  intervals:
    node_seconds: 5
    container_seconds: 2
    
  containers:
    - web-server
    - database
    - cache

logging:
  level: INFO
  file: /var/log/performance-monitor.log
```

### Monitoring Multiple Hosts
```python
# Multi-host setup with central database
hosts = [
    {"host": "app-server-01", "master": True},
    {"host": "app-server-02", "master": False},
    {"host": "db-server-01", "master": False}
]

for host_config in hosts:
    monitor = CompleteResourceMonitor(
        conn=get_connection(),
        node_name=host_config["host"],
        master=host_config["master"]
    )
    # Deploy monitor to each host
```

### High Availability
- Deploy monitors on multiple nodes
- Use PostgreSQL replication for database HA
- Implement monitoring of the monitors
- Set up alerting for monitor failures

## License

This project is licensed under the MIT License - see the LICENSE file for details.

## Contributing

1. Fork the repository
2. Create a feature branch (`git checkout -b feature/amazing-feature`)
3. Commit your changes (`git commit -m 'Add some amazing feature'`)
4. Push to the branch (`git push origin feature/amazing-feature`)
5. Open a Pull Request

## Support

For issues and questions:
- Check the troubleshooting section above
- Review PostgreSQL and Docker logs
- Test with minimal configuration first
- Provide system information and error logs when reporting issues

