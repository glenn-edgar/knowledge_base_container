#!/usr/bin/env python3
"""
Performance Analysis Expert System
==================================

Industrial-grade performance analysis class that implements expert troubleshooting
methodologies as executable Python methods. Based on SRE and observability best practices.

Usage:
    analyzer = PerformanceAnalyzer(connection, table_name)
    analyzer.emergency_health_check()
    analyzer.daily_health_report()
    analyzer.capacity_planning_analysis()
"""

import psycopg2
from psycopg2 import extras
import time
import statistics
from datetime import datetime, timedelta
from typing import Dict, List, Any, Optional, Tuple
from dataclasses import dataclass
from enum import Enum
import json

class AlertLevel(Enum):
    P1_CRITICAL = "P1-CRITICAL"
    P2_WARNING = "P2-WARNING" 
    P3_ADVISORY = "P3-ADVISORY"
    INFO = "INFO"

class SystemStatus(Enum):
    EXCELLENT = "EXCELLENT"
    GOOD = "GOOD"
    ACCEPTABLE = "ACCEPTABLE"
    POOR = "POOR"
    CRITICAL = "CRITICAL"

@dataclass
class PerformanceAlert:
    level: AlertLevel
    timestamp: int
    node_name: str
    container_name: Optional[str]
    metric: str
    value: float
    threshold: float
    message: str
    impact: str

@dataclass
class SystemHealthScore:
    node_name: str
    availability_pct: float
    performance_pct: float  
    capacity_pct: float
    overall_grade: SystemStatus
    total_samples: int

@dataclass
class CapacityForecast:
    node_name: str
    resource_type: str
    current_avg: float
    current_peak: float
    growth_rate_per_day: float
    days_to_exhaustion: Optional[int]
    recommendation: str

class PerformanceAnalyzer:
    """
    Expert-level performance analysis system implementing industrial best practices.
    
    Provides methods for immediate incident response, trend analysis, capacity planning,
    and performance optimization based on SRE methodologies.
    """
    
    def __init__(self, 
                 connection: psycopg2.extensions.connection,
                 table_name: str = "monitor_samples_ring",
                 cpu_count: int = None):
        """
        Initialize the performance analyzer.
        
        Args:
            connection: Active PostgreSQL connection
            table_name: Performance monitoring table name  
            cpu_count: Number of CPUs (auto-detected if None)
        """
        self.conn = connection
        self.table_name = table_name
        self.cpu_count = cpu_count or self._detect_cpu_count()
        
    def _detect_cpu_count(self) -> int:
        """Detect CPU count from the most recent sample."""
        try:
            with self.conn.cursor() as cur:
                # Estimate CPU count from load average patterns
                cur.execute(f"""
                    SELECT AVG(load_1m / NULLIF(cpu_total_pct, 0) * 100) as estimated_cpus
                    FROM {self.table_name}
                    WHERE node_flag = TRUE 
                        AND cpu_total_pct > 10 
                        AND load_1m > 0
                        AND sample_time > (EXTRACT(epoch FROM NOW()) - 3600)
                """)
                result = cur.fetchone()
                if result and result[0]:
                    return max(1, int(result[0]))
        except:
            pass
        return 4  # Default fallback

    def _execute_query(self, query: str, params: tuple = ()) -> List[Dict[str, Any]]:
        """Execute query and return results as list of dictionaries."""
        with self.conn.cursor(cursor_factory=extras.RealDictCursor) as cur:
            cur.execute(query, params)
            return [dict(row) for row in cur.fetchall()]

    def _format_timestamp(self, timestamp: int) -> str:
        """Convert epoch timestamp to readable format."""
        return datetime.fromtimestamp(timestamp).strftime('%Y-%m-%d %H:%M:%S')

    # ==========================================
    # EMERGENCY & IMMEDIATE RESPONSE METHODS
    # ==========================================

    def emergency_health_check(self) -> Dict[str, Any]:
        """
        EMERGENCY: Immediate system health check for incident response.
        Returns critical issues requiring immediate attention.
        
        Returns:
            Dictionary with immediate threats and recommended actions
        """
        print("🚨 EMERGENCY HEALTH CHECK - Last 5 minutes")
        print("=" * 60)
        
        current_time = int(time.time())
        five_min_ago = current_time - 300
        
        # Critical system alerts - FIXED: moved net_errors_s calculation to WHERE clause
        critical_query = f"""
            SELECT 
                sample_time,
                node_name,
                container_name,
                cpu_total_pct,
                mem_available_mb,
                mem_total_mb,
                temperature_c,
                load_1m,
                swap_used_mb,
                COALESCE(net_rx_errs_s, 0) + COALESCE(net_tx_errs_s, 0) + 
                COALESCE(net_rx_drops_s, 0) + COALESCE(net_tx_drops_s, 0) as net_errors_s
            FROM {self.table_name}
            WHERE sample_time > %s
                AND (
                    (node_flag = TRUE AND (
                        cpu_total_pct > 95 
                        OR mem_available_mb::float / NULLIF(mem_total_mb, 0) < 0.05 
                        OR temperature_c > 85
                        OR load_1m > %s
                        OR swap_used_mb > 100
                    ))
                    OR (node_flag = FALSE AND cpu_total_pct > 90)
                    OR (COALESCE(net_rx_errs_s, 0) + COALESCE(net_tx_errs_s, 0) + 
                        COALESCE(net_rx_drops_s, 0) + COALESCE(net_tx_drops_s, 0)) > 10
                )
            ORDER BY sample_time DESC
        """
        
        critical_issues = self._execute_query(critical_query, (five_min_ago, self.cpu_count * 2))
        
        alerts = []
        recommendations = []
        
        for issue in critical_issues:
            node = issue['node_name']
            container = issue['container_name']
            timestamp = issue['sample_time']
            
            # CPU Critical
            if issue['cpu_total_pct'] and issue['cpu_total_pct'] > 95:
                alerts.append(PerformanceAlert(
                    level=AlertLevel.P1_CRITICAL,
                    timestamp=timestamp,
                    node_name=node,
                    container_name=container,
                    metric="CPU",
                    value=issue['cpu_total_pct'],
                    threshold=95.0,
                    message=f"CPU usage {issue['cpu_total_pct']:.1f}% - system unresponsive risk",
                    impact="System may become unresponsive"
                ))
                recommendations.append(f"🔥 {node}: Kill high-CPU processes immediately")
            
            # Memory Critical  
            if issue['mem_available_mb'] and issue['mem_total_mb']:
                mem_free_pct = issue['mem_available_mb'] / issue['mem_total_mb'] * 100
                if mem_free_pct < 5:
                    alerts.append(PerformanceAlert(
                        level=AlertLevel.P1_CRITICAL,
                        timestamp=timestamp,
                        node_name=node,
                        container_name=container,
                        metric="Memory",
                        value=mem_free_pct,
                        threshold=5.0,
                        message=f"Memory {mem_free_pct:.1f}% free - OOM risk",
                        impact="Out of memory killer may activate"
                    ))
                    recommendations.append(f"🔥 {node}: Free memory immediately or restart services")
            
            # Temperature Critical
            if issue['temperature_c'] and issue['temperature_c'] > 85:
                alerts.append(PerformanceAlert(
                    level=AlertLevel.P1_CRITICAL,
                    timestamp=timestamp,
                    node_name=node,
                    container_name=container,
                    metric="Temperature",
                    value=issue['temperature_c'],
                    threshold=85.0,
                    message=f"Temperature {issue['temperature_c']:.1f}°C - thermal throttling",
                    impact="CPU performance will be reduced"
                ))
                recommendations.append(f"🌡️ {node}: Check cooling systems immediately")
            
            # Network Errors
            if issue['net_errors_s'] and issue['net_errors_s'] > 10:
                alerts.append(PerformanceAlert(
                    level=AlertLevel.P2_WARNING,
                    timestamp=timestamp,
                    node_name=node,
                    container_name=container,
                    metric="Network",
                    value=issue['net_errors_s'],
                    threshold=10.0,
                    message=f"Network errors {issue['net_errors_s']:.1f}/s - packet loss occurring",
                    impact="Network performance degraded"
                ))
                recommendations.append(f"🌐 {node}: Check network interface and cable connections")
        
        # Print immediate summary
        if alerts:
            print(f"⚠️  CRITICAL: {len(alerts)} immediate threats detected!")
            for alert in alerts[:5]:  # Show top 5
                print(f"   • {alert.node_name}: {alert.message}")
            print("\n🔧 IMMEDIATE ACTIONS REQUIRED:")
            for rec in recommendations[:3]:  # Top 3 actions
                print(f"   {rec}")
        else:
            print("✅ No critical issues detected in last 5 minutes")
        
        return {
            'status': 'CRITICAL' if alerts else 'HEALTHY',
            'critical_alerts': len(alerts),
            'alerts': [alert.__dict__ for alert in alerts],
            'immediate_actions': recommendations,
            'check_time': current_time
        }

    def system_availability_check(self, hours: int = 24) -> Dict[str, Any]:
        """
        Check system availability and performance SLIs over specified period.
        
        Args:
            hours: Time period to analyze (default 24 hours)
            
        Returns:
            Availability metrics and SLI compliance
        """
        print(f"📊 SYSTEM AVAILABILITY CHECK - Last {hours} hours")
        print("=" * 50)
        
        start_time = int(time.time()) - (hours * 3600)
        
        sli_query = f"""
            WITH performance_sli AS (
                SELECT 
                    node_name,
                    -- Availability SLI (system responsive)
                    AVG(CASE WHEN cpu_total_pct < 90 AND load_1m < %s THEN 1 ELSE 0 END) * 100 as availability_pct,
                    -- Performance SLI (low latency proxy)  
                    AVG(CASE WHEN cpu_iowait_pct < 10 THEN 1 ELSE 0 END) * 100 as performance_pct,
                    -- Capacity SLI (resource headroom)
                    AVG(CASE WHEN mem_available_mb::float / NULLIF(mem_total_mb, 0) > 0.15 THEN 1 ELSE 0 END) * 100 as capacity_pct,
                    COUNT(*) as total_samples,
                    AVG(cpu_total_pct) as avg_cpu,
                    MIN(mem_available_mb::float / NULLIF(mem_total_mb, 0) * 100) as min_mem_free_pct
                FROM {self.table_name}
                WHERE node_flag = TRUE 
                    AND sample_time > %s
                GROUP BY node_name
            )
            SELECT 
                node_name,
                availability_pct,
                performance_pct,
                capacity_pct,
                total_samples,
                avg_cpu,
                min_mem_free_pct,
                CASE 
                    WHEN availability_pct >= 99.9 AND performance_pct >= 99.0 AND capacity_pct >= 95.0 THEN 'EXCELLENT'
                    WHEN availability_pct >= 99.5 AND performance_pct >= 95.0 AND capacity_pct >= 90.0 THEN 'GOOD'
                    WHEN availability_pct >= 99.0 AND performance_pct >= 90.0 AND capacity_pct >= 80.0 THEN 'ACCEPTABLE'
                    ELSE 'POOR'
                END as overall_grade
            FROM performance_sli
            ORDER BY availability_pct DESC
        """
        
        results = self._execute_query(sli_query, (self.cpu_count * 1.5, start_time))
        
        health_scores = []
        for row in results:
            health_scores.append(SystemHealthScore(
                node_name=row['node_name'],
                availability_pct=row['availability_pct'],
                performance_pct=row['performance_pct'],
                capacity_pct=row['capacity_pct'],
                overall_grade=SystemStatus(row['overall_grade']),
                total_samples=row['total_samples']
            ))
            
            # Print node status
            status_emoji = {
                'EXCELLENT': '🟢',
                'GOOD': '🔵', 
                'ACCEPTABLE': '🟡',
                'POOR': '🔴'
            }
            print(f"{status_emoji[row['overall_grade']]} {row['node_name']}:")
            print(f"   Availability: {row['availability_pct']:.2f}% | Performance: {row['performance_pct']:.2f}% | Capacity: {row['capacity_pct']:.2f}%")
            print(f"   Avg CPU: {row['avg_cpu']:.1f}% | Min Memory Free: {row['min_mem_free_pct']:.1f}%")
        
        return {
            'health_scores': [score.__dict__ for score in health_scores],
            'sli_compliance': {
                'excellent_nodes': len([s for s in health_scores if s.overall_grade == SystemStatus.EXCELLENT]),
                'poor_nodes': len([s for s in health_scores if s.overall_grade == SystemStatus.POOR])
            }
        }

    # ==========================================
    # ANOMALY DETECTION & PATTERN ANALYSIS  
    # ==========================================

    def detect_anomalies(self, hours: int = 4) -> Dict[str, Any]:
        """
        Detect performance anomalies and outliers using statistical methods.
        
        Args:
            hours: Time period to analyze for anomalies
            
        Returns:
            Detected anomalies with statistical context
        """
        print(f"🔍 ANOMALY DETECTION - Last {hours} hours")
        print("=" * 45)
        
        start_time = int(time.time()) - (hours * 3600)
        
        # Statistical anomaly detection
        anomaly_query = f"""
            WITH stats AS (
                SELECT 
                    node_name,
                    AVG(cpu_total_pct) as avg_cpu,
                    STDDEV(cpu_total_pct) as stddev_cpu,
                    AVG(mem_available_mb::float / NULLIF(mem_total_mb, 0) * 100) as avg_mem_free,
                    STDDEV(mem_available_mb::float / NULLIF(mem_total_mb, 0) * 100) as stddev_mem_free,
                    AVG(load_1m) as avg_load,
                    STDDEV(load_1m) as stddev_load
                FROM {self.table_name}
                WHERE node_flag = TRUE 
                    AND sample_time > %s
                GROUP BY node_name
            ),
            anomalies AS (
                SELECT 
                    m.sample_time,
                    m.node_name,
                    m.cpu_total_pct,
                    m.load_1m,
                    m.mem_available_mb::float / NULLIF(m.mem_total_mb, 0) * 100 as mem_free_pct,
                    m.temperature_c,
                    s.avg_cpu,
                    s.stddev_cpu,
                    -- Z-score anomaly detection (>2 standard deviations)
                    CASE WHEN s.stddev_cpu > 0 AND ABS(m.cpu_total_pct - s.avg_cpu) > 2 * s.stddev_cpu 
                         THEN 'CPU_ANOMALY' END as cpu_anomaly,
                    CASE WHEN s.stddev_load > 0 AND ABS(m.load_1m - s.avg_load) > 2 * s.stddev_load 
                         THEN 'LOAD_ANOMALY' END as load_anomaly,
                    CASE WHEN m.cpu_iowait_pct > 20 THEN 'IO_WAIT_SPIKE' END as io_anomaly,
                    CASE WHEN m.ctx_switches_s > 50000 THEN 'HIGH_CONTEXT_SWITCHING' END as ctx_anomaly
                FROM {self.table_name} m
                JOIN stats s ON m.node_name = s.node_name
                WHERE m.node_flag = TRUE 
                    AND m.sample_time > %s
                    AND (
                        (s.stddev_cpu > 0 AND ABS(m.cpu_total_pct - s.avg_cpu) > 2 * s.stddev_cpu)
                        OR (s.stddev_load > 0 AND ABS(m.load_1m - s.avg_load) > 2 * s.stddev_load)
                        OR m.cpu_iowait_pct > 20
                        OR m.ctx_switches_s > 50000
                    )
            )
            SELECT * FROM anomalies 
            ORDER BY sample_time DESC
            LIMIT 20
        """
        
        anomalies = self._execute_query(anomaly_query, (start_time, start_time))
        
        detected_anomalies = []
        for anomaly in anomalies:
            anomaly_types = []
            if anomaly['cpu_anomaly']:
                anomaly_types.append(f"CPU spike: {anomaly['cpu_total_pct']:.1f}% (avg: {anomaly['avg_cpu']:.1f}%)")
            if anomaly['load_anomaly']:  
                anomaly_types.append(f"Load spike: {anomaly['load_1m']:.2f}")
            if anomaly['io_anomaly']:
                anomaly_types.append("High I/O wait")
            if anomaly['ctx_anomaly']:
                anomaly_types.append("Excessive context switching")
            
            if anomaly_types:
                detected_anomalies.append({
                    'timestamp': anomaly['sample_time'],
                    'node': anomaly['node_name'],
                    'anomalies': anomaly_types,
                    'time_str': self._format_timestamp(anomaly['sample_time'])
                })
                
                print(f"🚨 {anomaly['node_name']} at {self._format_timestamp(anomaly['sample_time'])}")
                for anom_type in anomaly_types:
                    print(f"   • {anom_type}")
        
        if not detected_anomalies:
            print("✅ No significant anomalies detected")
        
        return {
            'anomalies_detected': len(detected_anomalies),
            'anomalies': detected_anomalies,
            'analysis_period_hours': hours
        }

    def resource_contention_analysis(self) -> Dict[str, Any]:
        """
        Analyze resource contention between containers and system processes.
        
        Returns:
            Analysis of resource competition and recommendations
        """
        print("⚔️ RESOURCE CONTENTION ANALYSIS")
        print("=" * 35)
        
        current_time = int(time.time())
        one_hour_ago = current_time - 3600
        
        contention_query = f"""
            WITH container_usage AS (
                SELECT 
                    sample_time,
                    node_name,
                    SUM(cpu_total_pct) as total_container_cpu,
                    SUM(c_rss_mb) as total_container_memory,
                    COUNT(*) as container_count
                FROM {self.table_name}
                WHERE node_flag = FALSE 
                    AND sample_time > %s
                GROUP BY sample_time, node_name
            ),
            system_usage AS (
                SELECT 
                    sample_time,
                    node_name,
                    cpu_total_pct as system_cpu,
                    mem_total_mb - mem_available_mb as system_memory_used_mb,
                    load_1m
                FROM {self.table_name}
                WHERE node_flag = TRUE 
                    AND sample_time > %s
            )
            SELECT 
                s.sample_time,
                s.node_name,
                s.system_cpu,
                s.system_memory_used_mb,
                s.load_1m,
                COALESCE(c.total_container_cpu, 0) as container_cpu_sum,
                COALESCE(c.total_container_memory, 0) as container_memory_mb,
                COALESCE(c.container_count, 0) as container_count,
                -- Contention indicators
                CASE WHEN s.system_cpu > 80 AND c.total_container_cpu > 60 
                     THEN 'CPU_CONTENTION' END as cpu_contention,
                CASE WHEN s.load_1m > %s AND c.container_count > 3 
                     THEN 'SCHEDULING_CONTENTION' END as sched_contention
            FROM system_usage s
            LEFT JOIN container_usage c ON s.sample_time = c.sample_time AND s.node_name = c.node_name
            WHERE s.system_cpu > 50 OR c.total_container_cpu > 40
            ORDER BY s.sample_time DESC
            LIMIT 20
        """
        
        contention_data = self._execute_query(contention_query, (one_hour_ago, one_hour_ago, self.cpu_count))
        
        contentions = []
        for row in contention_data:
            if row['cpu_contention'] or row['sched_contention']:
                contentions.append({
                    'timestamp': row['sample_time'],
                    'node': row['node_name'],
                    'system_cpu': row['system_cpu'],
                    'container_cpu_sum': row['container_cpu_sum'],
                    'container_count': row['container_count'],
                    'load': row['load_1m'],
                    'contention_type': row['cpu_contention'] or row['sched_contention']
                })
                
                print(f"⚠️ {row['node_name']} - {row['contention_type']}")
                print(f"   System CPU: {row['system_cpu']:.1f}% | Container Total: {row['container_cpu_sum']:.1f}%")
                print(f"   Load: {row['load_1m']:.2f} | Containers: {row['container_count']}")
        
        if not contentions:
            print("✅ No resource contention detected")
        
        return {
            'contention_events': len(contentions),
            'contentions': contentions
        }

    # ==========================================
    # CAPACITY PLANNING & FORECASTING
    # ==========================================

    def capacity_planning_analysis(self, days: int = 30) -> Dict[str, Any]:
        """
        Comprehensive capacity planning analysis with growth forecasting.
        
        Args:
            days: Historical period for trend analysis
            
        Returns:
            Capacity forecasts and scaling recommendations
        """
        print(f"📈 CAPACITY PLANNING ANALYSIS - {days} day forecast")
        print("=" * 55)
        
        start_time = int(time.time()) - (days * 86400)
        
        # Growth trend analysis
        capacity_query = f"""
            WITH daily_peaks AS (
                SELECT 
                    DATE(to_timestamp(sample_time)) as date,
                    node_name,
                    MAX(cpu_total_pct) as peak_cpu,
                    MIN(mem_available_mb) as min_mem_free,
                    MAX(disk_read_mb_s + disk_write_mb_s) as peak_disk_io,
                    AVG(temperature_c) as avg_temp
                FROM {self.table_name}
                WHERE node_flag = TRUE 
                    AND sample_time > %s
                GROUP BY DATE(to_timestamp(sample_time)), node_name
            ),
            trends AS (
                SELECT 
                    node_name,
                    AVG(peak_cpu) as avg_daily_peak_cpu,
                    AVG(min_mem_free) as avg_daily_min_mem,
                    AVG(peak_disk_io) as avg_daily_peak_io,
                    -- Linear regression for growth trends
                    REGR_SLOPE(peak_cpu, EXTRACT(epoch FROM date)) * 86400 as cpu_trend_per_day,
                    REGR_SLOPE(min_mem_free, EXTRACT(epoch FROM date)) * 86400 as mem_trend_per_day,
                    COUNT(*) as data_points
                FROM daily_peaks 
                GROUP BY node_name
                HAVING COUNT(*) > 7  -- At least a week of data
            )
            SELECT 
                node_name,
                avg_daily_peak_cpu,
                avg_daily_min_mem,
                avg_daily_peak_io,
                cpu_trend_per_day,
                mem_trend_per_day,
                data_points,
                -- Days until resource exhaustion
                CASE 
                    WHEN cpu_trend_per_day > 0 THEN 
                        GREATEST(0, (95 - avg_daily_peak_cpu) / NULLIF(cpu_trend_per_day, 0))
                END as days_until_cpu_exhaustion,
                CASE 
                    WHEN mem_trend_per_day < 0 THEN 
                        GREATEST(0, avg_daily_min_mem / NULLIF(-mem_trend_per_day, 0))
                END as days_until_mem_exhaustion
            FROM trends
            ORDER BY 
                LEAST(
                    COALESCE(days_until_cpu_exhaustion, 999),
                    COALESCE(days_until_mem_exhaustion, 999)
                ) ASC
        """
        
        capacity_data = self._execute_query(capacity_query, (start_time,))
        
        forecasts = []
        for row in capacity_data:
            node = row['node_name']
            
            # CPU forecast
            if row['cpu_trend_per_day'] and row['cpu_trend_per_day'] > 0.1:
                cpu_forecast = CapacityForecast(
                    node_name=node,
                    resource_type="CPU",
                    current_avg=row['avg_daily_peak_cpu'],
                    current_peak=row['avg_daily_peak_cpu'],
                    growth_rate_per_day=row['cpu_trend_per_day'],
                    days_to_exhaustion=int(row['days_until_cpu_exhaustion']) if row['days_until_cpu_exhaustion'] else None,
                    recommendation=self._get_cpu_recommendation(row['avg_daily_peak_cpu'], row['cpu_trend_per_day'])
                )
                forecasts.append(cpu_forecast)
            
            # Memory forecast  
            if row['mem_trend_per_day'] and row['mem_trend_per_day'] < -10:
                mem_forecast = CapacityForecast(
                    node_name=node,
                    resource_type="Memory",
                    current_avg=row['avg_daily_min_mem'],
                    current_peak=row['avg_daily_min_mem'],
                    growth_rate_per_day=row['mem_trend_per_day'],
                    days_to_exhaustion=int(row['days_until_mem_exhaustion']) if row['days_until_mem_exhaustion'] else None,
                    recommendation=self._get_memory_recommendation(row['avg_daily_min_mem'], row['mem_trend_per_day'])
                )
                forecasts.append(mem_forecast)
            
            print(f"📊 {node}:")
            print(f"   Daily Peak CPU: {row['avg_daily_peak_cpu']:.1f}% (trend: {row['cpu_trend_per_day']:.2f}%/day)")
            print(f"   Min Free Memory: {row['avg_daily_min_mem']:.0f}MB (trend: {row['mem_trend_per_day']:.1f}MB/day)")
            
            if row['days_until_cpu_exhaustion'] and row['days_until_cpu_exhaustion'] < 60:
                print(f"   ⚠️ CPU exhaustion in ~{int(row['days_until_cpu_exhaustion'])} days")
            if row['days_until_mem_exhaustion'] and row['days_until_mem_exhaustion'] < 60:
                print(f"   ⚠️ Memory exhaustion in ~{int(row['days_until_mem_exhaustion'])} days")
        
        return {
            'forecasts': [f.__dict__ for f in forecasts],
            'critical_forecasts': len([f for f in forecasts if f.days_to_exhaustion and f.days_to_exhaustion < 30]),
            'analysis_period_days': days
        }

    def _get_cpu_recommendation(self, current_avg: float, trend: float) -> str:
        """Generate CPU scaling recommendation based on usage and trend."""
        if current_avg > 80:
            return "Scale immediately - CPU critically high"
        elif current_avg > 60 and trend > 1.0:
            return "Plan scaling - High usage with increasing trend"  
        elif trend > 0.5:
            return "Monitor closely - Growing CPU usage trend"
        else:
            return "No immediate action needed"

    def _get_memory_recommendation(self, current_min: float, trend: float) -> str:
        """Generate memory scaling recommendation based on usage and trend."""
        if current_min < 500:
            return "Add memory immediately - Critically low"
        elif current_min < 1000 and trend < -50:
            return "Plan memory upgrade - Declining available memory"
        elif trend < -20:
            return "Monitor memory usage - Downward trend detected"
        else:
            return "Memory levels stable"

    # ==========================================
    # PERFORMANCE PATTERN ANALYSIS
    # ==========================================

    def workload_pattern_analysis(self, days: int = 7) -> Dict[str, Any]:
        """
        Analyze workload patterns to identify predictable performance cycles.
        
        Args:
            days: Number of days to analyze for patterns
            
        Returns:
            Workload patterns and optimization opportunities
        """
        print(f"📅 WORKLOAD PATTERN ANALYSIS - {days} days")
        print("=" * 40)
        
        start_time = int(time.time()) - (days * 86400)
        
        pattern_query = f"""
            WITH hourly_patterns AS (
                SELECT 
                    node_name,
                    EXTRACT(hour FROM to_timestamp(sample_time)) as hour,
                    EXTRACT(dow FROM to_timestamp(sample_time)) as day_of_week,
                    AVG(cpu_total_pct) as avg_cpu,
                    AVG(mem_available_mb::float / NULLIF(mem_total_mb, 0) * 100) as avg_mem_free,
                    AVG(disk_read_mb_s + disk_write_mb_s) as avg_disk_io,
                    AVG(net_rx_mb_s + net_tx_mb_s) as avg_net_io,
                    COUNT(*) as samples
                FROM {self.table_name}
                WHERE node_flag = TRUE 
                    AND sample_time > %s
                GROUP BY node_name, 
                         EXTRACT(hour FROM to_timestamp(sample_time)), 
                         EXTRACT(dow FROM to_timestamp(sample_time))
                HAVING COUNT(*) > 2  -- Minimum samples per hour
            )
            SELECT 
                node_name,
                hour,
                AVG(CASE WHEN day_of_week IN (1,2,3,4,5) THEN avg_cpu END) as weekday_avg_cpu,
                AVG(CASE WHEN day_of_week IN (0,6) THEN avg_cpu END) as weekend_avg_cpu,
                AVG(CASE WHEN day_of_week IN (1,2,3,4,5) THEN avg_mem_free END) as weekday_avg_mem,
                AVG(CASE WHEN day_of_week IN (0,6) THEN avg_mem_free END) as weekend_avg_mem,
                AVG(avg_disk_io) as avg_disk_io,
                AVG(avg_net_io) as avg_net_io
            FROM hourly_patterns 
            GROUP BY node_name, hour 
            ORDER BY node_name, hour
        """
        
        patterns = self._execute_query(pattern_query, (start_time,))
        
        # Analyze patterns by node
        node_patterns = {}
        for row in patterns:
            node = row['node_name']
            if node not in node_patterns:
                node_patterns[node] = {'hours': [], 'peak_hours': [], 'low_hours': []}
            
            node_patterns[node]['hours'].append({
                'hour': row['hour'],
                'weekday_cpu': row['weekday_avg_cpu'],
                'weekend_cpu': row['weekend_avg_cpu'],
                'weekday_mem': row['weekday_avg_mem'],
                'weekend_mem': row['weekend_avg_mem']
            })
            
            # Identify peak and low usage hours
            weekday_cpu = row['weekday_avg_cpu'] or 0
            if weekday_cpu > 60:
                node_patterns[node]['peak_hours'].append(row['hour'])
            elif weekday_cpu < 20:
                node_patterns[node]['low_hours'].append(row['hour'])
        
        # Print analysis
        for node, data in node_patterns.items():
            print(f"\n📊 {node} Workload Patterns:")
            
            if data['peak_hours']:
                peak_hours_str = ', '.join([f"{h:02d}:00" for h in sorted(data['peak_hours'])])
                print(f"   🔴 Peak Hours: {peak_hours_str}")
            
            if data['low_hours']:
                low_hours_str = ', '.join([f"{h:02d}:00" for h in sorted(data['low_hours'])])
                print(f"   🟢 Low Usage: {low_hours_str}")
            
            # Calculate peak vs off-peak difference
            weekday_cpus = [h['weekday_cpu'] for h in data['hours'] if h['weekday_cpu']]
            if len(weekday_cpus) > 12:  # Enough data points
                peak_cpu = max(weekday_cpus)
                min_cpu = min(weekday_cpus)
                variance = peak_cpu - min_cpu
                
                if variance > 40:
                    print(f"   📈 High Variance: {variance:.1f}% (Peak: {peak_cpu:.1f}%, Low: {min_cpu:.1f}%)")
                    print(f"      💡 Recommendation: Consider auto-scaling or scheduled resource adjustment")
        
        return {
            'node_patterns': node_patterns,
            'analysis_period_days': days
        }

    # ==========================================  
    # DAILY/WEEKLY REPORTING METHODS
    # ==========================================

    def daily_health_report(self) -> Dict[str, Any]:
        """
        Generate comprehensive daily system health report.
        
        Returns:
            Complete daily health summary with recommendations
        """
        print("📋 DAILY HEALTH REPORT")
        print("=" * 25)
        print(f"Report Date: {datetime.now().strftime('%Y-%m-%d %H:%M')}")
        print("=" * 50)
        
        report = {}
        
        # 1. Emergency health check
        emergency_status = self.emergency_health_check()
        report['emergency_status'] = emergency_status
        
        print("\n")
        
        # 2. System availability  
        availability = self.system_availability_check(hours=24)
        report['availability'] = availability
        
        print("\n")
        
        # 3. Anomaly detection
        anomalies = self.detect_anomalies(hours=24)
        report['anomalies'] = anomalies
        
        print("\n")
        
        # 4. Resource contention
        contention = self.resource_contention_analysis()
        report['contention'] = contention
        
        # 5. Overall health score
        health_scores = availability['health_scores']
        if health_scores:
            excellent_count = len([s for s in health_scores if s['overall_grade'] == 'EXCELLENT'])
            poor_count = len([s for s in health_scores if s['overall_grade'] == 'POOR'])
            total_nodes = len(health_scores)
            
            overall_health = "EXCELLENT" if excellent_count == total_nodes else \
                           "POOR" if poor_count > 0 else \
                           "GOOD"
        else:
            overall_health = "UNKNOWN"
        
        print(f"\n🎯 OVERALL SYSTEM HEALTH: {overall_health}")
        print(f"📊 Report Summary:")
        print(f"   • Critical Alerts: {emergency_status['critical_alerts']}")
        print(f"   • Anomalies Detected: {anomalies['anomalies_detected']}")
        print(f"   • Resource Contention Events: {contention['contention_events']}")
        print(f"   • Nodes in Excellent Health: {excellent_count if 'excellent_count' in locals() else 0}")
        
        report['overall_health'] = overall_health
        report['report_timestamp'] = int(time.time())
        
        return report

    def weekly_performance_summary(self) -> Dict[str, Any]:
        """
        Generate weekly performance summary with trends and capacity planning.
        
        Returns:
            Weekly performance analysis and strategic recommendations
        """
        print("📈 WEEKLY PERFORMANCE SUMMARY")
        print("=" * 35)
        
        # Capacity planning
        capacity_analysis = self.capacity_planning_analysis(days=7)
        
        print("\n")
        
        # Workload patterns
        workload_patterns = self.workload_pattern_analysis(days=7)
        
        # Strategic recommendations
        recommendations = self._generate_weekly_recommendations(capacity_analysis, workload_patterns)
        
        return {
            'capacity_analysis': capacity_analysis,
            'workload_patterns': workload_patterns,
            'strategic_recommendations': recommendations,
            'report_period': '7 days',
            'report_timestamp': int(time.time())
        }

    def _generate_weekly_recommendations(self, capacity_analysis: Dict, workload_patterns: Dict) -> List[str]:
        """Generate strategic recommendations based on weekly analysis."""
        recommendations = []
        
        # Capacity-based recommendations
        critical_forecasts = capacity_analysis.get('critical_forecasts', 0)
        if critical_forecasts > 0:
            recommendations.append(f"🚨 URGENT: {critical_forecasts} resources approaching exhaustion - plan scaling immediately")
        
        # Pattern-based recommendations
        for node, patterns in workload_patterns.get('node_patterns', {}).items():
            peak_hours = len(patterns.get('peak_hours', []))
            low_hours = len(patterns.get('low_hours', []))
            
            if peak_hours > 0 and low_hours > 0:
                recommendations.append(f"💡 {node}: Implement scheduled scaling - {peak_hours}h peak, {low_hours}h low usage")
        
        if not recommendations:
            recommendations.append("✅ No critical issues identified - maintain current monitoring")
        
        return recommendations

    # ==========================================
    # CORRELATION & ADVANCED ANALYSIS  
    # ==========================================

    def correlation_analysis(self, hours: int = 24) -> Dict[str, Any]:
        """
        Analyze correlations between different performance metrics.
        
        Args:
            hours: Time period for correlation analysis
            
        Returns:
            Correlation coefficients and insights
        """
        print(f"🔗 CORRELATION ANALYSIS - {hours} hours")
        print("=" * 40)
        
        start_time = int(time.time()) - (hours * 3600)
        
        correlation_query = f"""
            SELECT 
                CORR(cpu_total_pct, temperature_c) as cpu_temp_correlation,
                CORR(cpu_total_pct, load_1m) as cpu_load_correlation,
                CORR(mem_available_mb, disk_read_mb_s + disk_write_mb_s) as mem_disk_correlation,
                CORR(load_1m, ctx_switches_s) as load_context_correlation,
                CORR(cpu_iowait_pct, disk_read_mb_s + disk_write_mb_s) as iowait_disk_correlation,
                COUNT(*) as sample_count
            FROM {self.table_name}
            WHERE node_flag = TRUE 
                AND sample_time > %s
                AND cpu_total_pct IS NOT NULL
                AND temperature_c IS NOT NULL
        """
        
        correlations = self._execute_query(correlation_query, (start_time,))
        
        if correlations:
            corr_data = correlations[0]
            
            print("📊 Metric Correlations:")
            correlations_list = [
                ("CPU vs Temperature", corr_data['cpu_temp_correlation']),
                ("CPU vs Load", corr_data['cpu_load_correlation']),
                ("Memory vs Disk I/O", corr_data['mem_disk_correlation']),
                ("Load vs Context Switches", corr_data['load_context_correlation']),
                ("I/O Wait vs Disk I/O", corr_data['iowait_disk_correlation'])
            ]
            
            insights = []
            for name, corr in correlations_list:
                if corr is not None:
                    strength = "Strong" if abs(corr) > 0.7 else "Moderate" if abs(corr) > 0.4 else "Weak"
                    direction = "positive" if corr > 0 else "negative"
                    
                    print(f"   • {name}: {corr:.3f} ({strength} {direction})")
                    
                    if abs(corr) > 0.7:
                        insights.append(f"Strong {direction} correlation between {name.lower()}")
            
            print(f"\n💡 Key Insights:")
            for insight in insights[:3]:  # Top 3 insights
                print(f"   • {insight}")
            
            return {
                'correlations': {
                    'cpu_temperature': corr_data['cpu_temp_correlation'],
                    'cpu_load': corr_data['cpu_load_correlation'],
                    'memory_disk': corr_data['mem_disk_correlation'],
                    'load_context_switches': corr_data['load_context_correlation'],
                    'iowait_disk': corr_data['iowait_disk_correlation']
                },
                'insights': insights,
                'sample_count': corr_data['sample_count']
            }
        
        return {'error': 'Insufficient data for correlation analysis'}

    # ==========================================
    # UTILITY METHODS
    # ==========================================

    def generate_alert_config(self) -> Dict[str, Any]:
        """
        Generate recommended alert thresholds based on historical data.
        
        Returns:
            Recommended alert configuration
        """
        print("⚙️ GENERATING ALERT CONFIGURATION")
        print("=" * 40)
        
        # Analyze historical data to set dynamic thresholds
        current_time = int(time.time())
        week_ago = current_time - (7 * 86400)
        
        threshold_query = f"""
            SELECT 
                node_name,
                PERCENTILE_CONT(0.95) WITHIN GROUP (ORDER BY cpu_total_pct) as p95_cpu,
                PERCENTILE_CONT(0.90) WITHIN GROUP (ORDER BY cpu_total_pct) as p90_cpu,
                PERCENTILE_CONT(0.05) WITHIN GROUP (ORDER BY mem_available_mb::float / NULLIF(mem_total_mb, 0) * 100) as p5_mem_free,
                PERCENTILE_CONT(0.10) WITHIN GROUP (ORDER BY mem_available_mb::float / NULLIF(mem_total_mb, 0) * 100) as p10_mem_free,
                MAX(temperature_c) as max_temp,
                PERCENTILE_CONT(0.95) WITHIN GROUP (ORDER BY load_1m) as p95_load
            FROM {self.table_name}
            WHERE node_flag = TRUE 
                AND sample_time > %s
            GROUP BY node_name
        """
        
        threshold_data = self._execute_query(threshold_query, (week_ago,))
        
        alert_configs = {}
        for row in threshold_data:
            node = row['node_name']
            
            # Set conservative thresholds based on historical patterns
            alert_configs[node] = {
                'cpu_warning': min(80, row['p90_cpu'] * 1.1) if row['p90_cpu'] else 80,
                'cpu_critical': min(95, row['p95_cpu'] * 1.05) if row['p95_cpu'] else 95,
                'memory_warning': max(10, row['p10_mem_free'] * 0.8) if row['p10_mem_free'] else 15,
                'memory_critical': max(5, row['p5_mem_free'] * 0.5) if row['p5_mem_free'] else 5,
                'load_warning': min(self.cpu_count * 1.5, row['p95_load'] * 1.2) if row['p95_load'] else self.cpu_count,
                'load_critical': min(self.cpu_count * 2.0, row['p95_load'] * 1.5) if row['p95_load'] else self.cpu_count * 1.5,
                'temperature_warning': min(75, row['max_temp'] * 0.9) if row['max_temp'] else 70,
                'temperature_critical': min(85, row['max_temp'] * 0.95) if row['max_temp'] else 80
            }
            
            print(f"📋 {node} Alert Thresholds:")
            print(f"   CPU: {alert_configs[node]['cpu_warning']:.0f}% (warn) | {alert_configs[node]['cpu_critical']:.0f}% (crit)")
            print(f"   Memory: {alert_configs[node]['memory_warning']:.0f}% free (warn) | {alert_configs[node]['memory_critical']:.0f}% free (crit)")
            print(f"   Load: {alert_configs[node]['load_warning']:.1f} (warn) | {alert_configs[node]['load_critical']:.1f} (crit)")
            print(f"   Temperature: {alert_configs[node]['temperature_warning']:.0f}°C (warn) | {alert_configs[node]['temperature_critical']:.0f}°C (crit)")
        
        return {
            'alert_configurations': alert_configs,
            'based_on_days': 7,
            'generated_at': current_time
        }

    def export_metrics_summary(self, hours: int = 24) -> Dict[str, Any]:
        """
        Export key metrics summary for external monitoring systems.
        
        Args:
            hours: Time period to summarize
            
        Returns:
            JSON-exportable metrics summary
        """
        current_time = int(time.time())
        start_time = current_time - (hours * 3600)
        
        summary_query = f"""
            SELECT 
                node_name,
                COUNT(*) as sample_count,
                AVG(cpu_total_pct) as avg_cpu,
                MAX(cpu_total_pct) as max_cpu,
                AVG(mem_available_mb::float / NULLIF(mem_total_mb, 0) * 100) as avg_mem_free_pct,
                MIN(mem_available_mb::float / NULLIF(mem_total_mb, 0) * 100) as min_mem_free_pct,
                AVG(load_1m) as avg_load,
                MAX(load_1m) as max_load,
                AVG(temperature_c) as avg_temp,
                MAX(temperature_c) as max_temp,
                AVG(disk_read_mb_s + disk_write_mb_s) as avg_disk_io,
                AVG(net_rx_mb_s + net_tx_mb_s) as avg_net_io
            FROM {self.table_name}
            WHERE node_flag = TRUE 
                AND sample_time > %s
            GROUP BY node_name
        """
        
        node_summaries = self._execute_query(summary_query, (start_time,))
        
        # Container summary
        container_summary_query = f"""
            SELECT 
                node_name,
                container_name,
                AVG(cpu_total_pct) as avg_cpu,
                MAX(cpu_total_pct) as max_cpu,
                AVG(c_rss_mb) as avg_memory_mb,
                MAX(c_rss_mb) as max_memory_mb
            FROM {self.table_name}
            WHERE node_flag = FALSE 
                AND sample_time > %s
            GROUP BY node_name, container_name
        """
        
        container_summaries = self._execute_query(container_summary_query, (start_time,))
        
        return {
            'export_timestamp': current_time,
            'period_hours': hours,
            'node_metrics': node_summaries,
            'container_metrics': container_summaries,
            'summary_stats': {
                'total_nodes': len(node_summaries),
                'total_containers': len(container_summaries),
                'avg_system_cpu': statistics.mean([n['avg_cpu'] for n in node_summaries if n['avg_cpu']]),
                'max_system_cpu': max([n['max_cpu'] for n in node_summaries if n['max_cpu']], default=0)
            }
        }


# Example usage and testing
if __name__ == "__main__":
    import psycopg2
    import os
    
    # Example usage
    try:
        conn = psycopg2.connect(
            host="localhost",
            database="knowledge_base",
            user="gedgar", 
            password=os.getenv("POSTGRES_PASSWORD")
        )
        
        analyzer = PerformanceAnalyzer(conn, "raw_resource_table")
        
        print("🔍 PERFORMANCE ANALYSIS EXPERT SYSTEM")
        print("=" * 50)
        
        # Emergency health check
        emergency_results = analyzer.emergency_health_check()
        
        print("\n" + "="*50)
        
        # Daily health report
        daily_report = analyzer.daily_health_report()
        
        # Generate alert configuration
        alert_config = analyzer.generate_alert_config()
        
        conn.close()
        
    except Exception as e:
        print(f"Error: {e}")
