# Performance Analysis Expert System

A production-grade Python library for comprehensive system performance monitoring, analysis, and capacity planning. Built with SRE best practices for real-time incident response and strategic infrastructure planning.

## 🚀 Features

### Real-Time Monitoring
- **Emergency Health Checks** - Immediate detection of critical system issues
- **Anomaly Detection** - Statistical analysis to identify performance outliers
- **Resource Contention Analysis** - Identify conflicts between containers and system processes
- **Correlation Analysis** - Discover relationships between performance metrics

### Reporting & Planning
- **Daily Health Reports** - Comprehensive system status summaries
- **Weekly Performance Summaries** - Strategic analysis with trends
- **Capacity Planning** - Forecast resource exhaustion with growth trends
- **Workload Pattern Analysis** - Identify predictable performance cycles

### Alert Management
- **Dynamic Alert Configuration** - Generate thresholds based on historical data
- **Multi-Level Alerting** - P1-Critical, P2-Warning, P3-Advisory classifications
- **SLI Compliance Tracking** - Monitor availability, performance, and capacity SLIs

## 📋 Prerequisites

- Python 3.6+
- PostgreSQL database with performance metrics table
- Required Python packages:
  ```bash
  pip install psycopg2-binary
  ```

## 🛠️ Installation

```bash
# Clone or download the performance_analyzer.py file
wget https://your-repo/performance_analyzer.py

# Install dependencies
pip install psycopg2-binary

# Or use with your existing project
from performance_analyzer import PerformanceAnalyzer
```

## 📊 Database Schema

The analyzer expects a PostgreSQL table with the following structure:

```sql
CREATE TABLE monitor_samples_ring (
    sample_time INTEGER,           -- Unix timestamp
    node_name VARCHAR(255),        -- Hostname/node identifier
    container_name VARCHAR(255),    -- Container name (NULL for node metrics)
    node_flag BOOLEAN,             -- TRUE for node, FALSE for container
    
    -- CPU Metrics
    cpu_total_pct FLOAT,           -- Total CPU usage percentage
    cpu_iowait_pct FLOAT,          -- I/O wait percentage
    
    -- Memory Metrics  
    mem_available_mb FLOAT,        -- Available memory in MB
    mem_total_mb FLOAT,            -- Total memory in MB
    swap_used_mb FLOAT,            -- Swap usage in MB
    c_rss_mb FLOAT,               -- Container RSS memory in MB
    
    -- System Metrics
    load_1m FLOAT,                 -- 1-minute load average
    temperature_c FLOAT,           -- System temperature in Celsius
    ctx_switches_s FLOAT,          -- Context switches per second
    
    -- I/O Metrics
    disk_read_mb_s FLOAT,          -- Disk read throughput
    disk_write_mb_s FLOAT,         -- Disk write throughput
    
    -- Network Metrics
    net_rx_mb_s FLOAT,             -- Network receive throughput
    net_tx_mb_s FLOAT,             -- Network transmit throughput
    net_rx_errs_s FLOAT,           -- Network receive errors/sec
    net_tx_errs_s FLOAT,           -- Network transmit errors/sec
    net_rx_drops_s FLOAT,          -- Network receive drops/sec
    net_tx_drops_s FLOAT           -- Network transmit drops/sec
);
```

## 🔧 Quick Start

### Basic Usage

```python
import psycopg2
from performance_analyzer import PerformanceAnalyzer

# Connect to your database
conn = psycopg2.connect(
    host="localhost",
    database="monitoring_db",
    user="your_user",
    password="your_password"
)

# Initialize the analyzer
analyzer = PerformanceAnalyzer(
    connection=conn,
    table_name="monitor_samples_ring",
    cpu_count=8  # Optional: auto-detected if not specified
)

# Run emergency health check
emergency_status = analyzer.emergency_health_check()

# Generate daily report
daily_report = analyzer.daily_health_report()

# Close connection when done
conn.close()
```

### Emergency Response

```python
# For incident response - immediate system health check
emergency_results = analyzer.emergency_health_check()

if emergency_results['status'] == 'CRITICAL':
    print("⚠️ CRITICAL ISSUES DETECTED!")
    for action in emergency_results['immediate_actions']:
        print(f"Action required: {action}")
```

### Capacity Planning

```python
# Analyze capacity trends and forecast resource exhaustion
capacity_analysis = analyzer.capacity_planning_analysis(days=30)

for forecast in capacity_analysis['forecasts']:
    if forecast['days_to_exhaustion'] and forecast['days_to_exhaustion'] < 30:
        print(f"Warning: {forecast['node_name']} {forecast['resource_type']} "
              f"exhaustion in {forecast['days_to_exhaustion']} days")
```

## 📈 Available Methods

### Emergency & Immediate Response

| Method | Description | Use Case |
|--------|-------------|----------|
| `emergency_health_check()` | Detect critical issues in last 5 minutes | Incident response |
| `system_availability_check(hours)` | Check SLI compliance over period | Availability monitoring |

### Anomaly Detection

| Method | Description | Use Case |
|--------|-------------|----------|
| `detect_anomalies(hours)` | Statistical anomaly detection | Identify unusual behavior |
| `resource_contention_analysis()` | Analyze resource competition | Performance optimization |
| `correlation_analysis(hours)` | Find metric correlations | Root cause analysis |

### Capacity Planning

| Method | Description | Use Case |
|--------|-------------|----------|
| `capacity_planning_analysis(days)` | Forecast resource exhaustion | Infrastructure planning |
| `workload_pattern_analysis(days)` | Identify usage patterns | Scheduling optimization |

### Reporting

| Method | Description | Use Case |
|--------|-------------|----------|
| `daily_health_report()` | Comprehensive daily summary | Daily operations review |
| `weekly_performance_summary()` | Weekly trends and analysis | Strategic planning |
| `generate_alert_config()` | Dynamic threshold generation | Alert tuning |
| `export_metrics_summary(hours)` | Export metrics for external systems | Integration |

## 🎯 Alert Levels

The system uses four alert levels based on impact and urgency:

- **P1-CRITICAL**: Immediate action required (system at risk)
- **P2-WARNING**: Attention needed (degraded performance)
- **P3-ADVISORY**: Proactive notification (potential issues)
- **INFO**: Informational only

## 📊 System Status Grades

Nodes are graded based on three SLIs:

- **EXCELLENT**: All SLIs > 99%
- **GOOD**: All SLIs > 95%
- **ACCEPTABLE**: All SLIs > 90%
- **POOR**: One or more SLIs < 90%
- **CRITICAL**: System at risk of failure

## 🔍 Example Output

### Emergency Health Check
```
🚨 EMERGENCY HEALTH CHECK - Last 5 minutes
============================================================
⚠️  CRITICAL: 3 immediate threats detected!
   • node-01: CPU usage 96.5% - system unresponsive risk
   • node-02: Memory 3.2% free - OOM risk
   • node-03: Temperature 87.3°C - thermal throttling

🔧 IMMEDIATE ACTIONS REQUIRED:
   🔥 node-01: Kill high-CPU processes immediately
   🔥 node-02: Free memory immediately or restart services
   🌡️ node-03: Check cooling systems immediately
```

### Capacity Planning Analysis
```
📈 CAPACITY PLANNING ANALYSIS - 30 day forecast
=======================================================
📊 node-01:
   Daily Peak CPU: 78.3% (trend: +0.82%/day)
   Min Free Memory: 2048MB (trend: -15.3MB/day)
   ⚠️ CPU exhaustion in ~21 days
   
💡 Recommendation: Plan scaling - High usage with increasing trend
```

## 🛡️ Best Practices

1. **Regular Health Checks**: Run `emergency_health_check()` every 5 minutes
2. **Daily Reports**: Schedule `daily_health_report()` for operational reviews
3. **Capacity Planning**: Run `capacity_planning_analysis()` weekly
4. **Alert Tuning**: Update thresholds monthly using `generate_alert_config()`
5. **Data Retention**: Keep at least 30 days of metrics for accurate analysis

## 🔧 Configuration

### CPU Count Detection
The analyzer auto-detects CPU count from load average patterns. Override if needed:

```python
analyzer = PerformanceAnalyzer(conn, "metrics_table", cpu_count=16)
```

### Custom Thresholds
Modify alert thresholds in the class constants or override methods:

```python
class CustomAnalyzer(PerformanceAnalyzer):
    def _get_cpu_recommendation(self, current_avg, trend):
        if current_avg > 70:  # Custom threshold
            return "Scale immediately"
        return super()._get_cpu_recommendation(current_avg, trend)
```

## 📝 Data Requirements

- **Minimum Data**: 5 minutes for emergency checks
- **Recommended**: 24 hours for meaningful analysis
- **Optimal**: 7-30 days for capacity planning
- **Sample Rate**: 1-minute intervals recommended

## 🤝 Contributing

Contributions are welcome! Areas for enhancement:

- Machine learning for anomaly detection
- Additional metric correlations
- Custom visualization exports
- Integration with alerting systems
- Support for additional database backends

## 📄 License

This project is provided as-is for monitoring and analysis purposes. Modify and distribute as needed for your infrastructure monitoring needs.

## 🆘 Support

For issues, questions, or contributions:
- Review the method docstrings for detailed usage
- Check the example usage in the main block
- Ensure your database schema matches requirements
- Verify sufficient historical data exists

## 🚨 Important Notes

- **Performance Impact**: Analysis queries may be resource-intensive on large datasets
- **Time Zones**: All timestamps are in Unix epoch format
- **Database Connection**: Ensure connection pooling for production use
- **Error Handling**: Wrap critical calls in try-except blocks for production

